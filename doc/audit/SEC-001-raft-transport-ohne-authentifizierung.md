# SEC-001 — Der Raft-gRPC-Endpunkt ist vollständig unauthentifiziert

- **Schweregrad:** Kritisch
- **Kategorie:** Fehlende Authentifizierung / Spoofing / Manipulation des Konsens
- **Ort:** `src/Blun.MultiRaft.Grpc/RaftProtocolService.cs`, `src/Blun.MultiRaft.Grpc/RaftKestrelExtensions.cs`, `src/Blun.MultiRaft.Grpc/RaftFrameCodec.cs`

## Befund

`RaftProtocolService.Session` nimmt jeden eingehenden bidirektionalen Stream an. Es gibt an keiner Stelle
eine Authentifizierung, eine Autorisierungs-Policy oder eine Prüfung, ob der Aufrufer überhaupt ein
Cluster-Mitglied ist:

```csharp
public static IEndpointRouteBuilder MapRaftProtocol(this IEndpointRouteBuilder endpoints)
{
    endpoints.MapGrpcService<RaftProtocolService>();   // kein RequireAuthorization()
    return endpoints;
}
```

Die Identität des Absenders wird ausschließlich aus dem *Nachrichteninhalt* gelesen. In
`RaftFrameCodec.ToDomain` wird `message.Leader` bzw. `message.Candidate` direkt in eine `NodeId`
umgewandelt — ohne Abgleich gegen eine TLS-Identität, gegen die konfigurierte Peer-Liste
(`GrpcRaftTransportOptions.Peers`) oder gegen die Absenderadresse:

```csharp
public static AppendEntriesRequest ToDomain(RaftGroupId group, AppendEntries message)
    => new(group, message.Term, new NodeId(message.Leader), ...);
```

Ein Absender behauptet also frei, welcher Knoten er ist. `RaftStreamSession` hat kein Feld für die
Gegenstelle — die Session weiß strukturell nicht, mit wem sie spricht.

## Auswirkung

Wer den Raft-Port erreichen kann, kontrolliert den Cluster vollständig. Ohne jede Vorbedingung:

| Frame | Wirkung |
|---|---|
| `AppendEntries` mit hohem `Term` | Jeder Knoten tritt zurück, akzeptiert den Absender als Leader, kürzt seinen Log (`TruncateTailAsync`) und übernimmt beliebige Einträge. **Beliebiges Überschreiben committeter Historie.** |
| `AppendEntries` mit `Kind = Membership` | Beliebige Änderung der Voter-Menge — inklusive der Cluster-Gruppe (`RaftGroupId.Cluster`), also der administrativen Ebene. |
| `InstallSnapshot` | Ersetzt den kompletten State und die im Snapshot transportierte Konfiguration (`ResetToSnapshotAsync` löscht *alle* Segmente). |
| `Vote` mit hohem `Term` | Term-Inflation; erzwingt Neuwahlen im gesamten Cluster. |
| `TimeoutNow` | Erzwungener Leader-Wechsel. |
| `LeaderTarget` mit `Execute = true` | Erzwungene Leadership-Übergabe auf einen gewählten Knoten. |

Zusätzlich sind alle Nutzdaten lesbar bzw. fälschbar: `AppendEntries`-Frames enthalten die vollständigen
Payloads der Queue-Nachrichten.

Der einzige heutige Schutz ist, dass die Demo an Loopback bindet.
`RaftKestrelExtensions.ConfigureRaftEndpoint` nimmt jede `IPAddress` entgegen, und das ist der
dokumentierte Weg, den Bibliotheksnutzer für einen echten Cluster gehen.

## Warum die vorhandene Dokumentation nicht ausreicht

`ConfigureRaftEndpoint` verweist über den `configureListen`-Parameter auf `listen.UseHttps(...)` und
überlässt TLS bewusst dem Host. Das adressiert aber nur Vertraulichkeit, nicht Autorisierung: auch mit
Server-TLS kann jeder Client, der die Verbindung aufbauen darf, jede beliebige `NodeId` behaupten. Und
selbst mit mTLS fehlt die Bindung „dieses Zertifikat gehört zu genau dieser `NodeId`".

## Empfehlung

1. **Peer-Authentifizierung verpflichtend machen.** mTLS mit Client-Zertifikaten, und in
   `RaftProtocolService.Session` die Zertifikatsidentität aus `ServerCallContext` gegen die konfigurierte
   Peer-Menge auflösen.
2. **Behauptete Identität gegen authentifizierte Identität prüfen.** Die aus dem Frame gelesene `NodeId`
   (`Leader`, `Candidate`, `NodeLoad.Node`) muss gegen die für die Verbindung festgestellte Peer-Identität
   validiert werden; bei Abweichung Frame verwerfen und protokollieren. Dafür braucht `RaftStreamSession`
   ein `PeerId`-Feld.
3. **`MapRaftProtocol` muss eine Autorisierungs-Policy erzwingen** (`.RequireAuthorization(...)`), statt
   den Endpunkt anonym zu mappen.
4. Wo mTLS nicht praktikabel ist: ein vorab geteiltes Cluster-Secret pro Verbindung prüfen. Schwächer,
   aber kategorisch besser als „gar nichts".
5. Solange nichts davon existiert, gehört ein deutlicher Warnhinweis in `README.md` und in die XML-Doku
   von `ConfigureRaftEndpoint`: *dieser Endpunkt darf nur in einem vertrauenswürdigen Netzsegment
   erreichbar sein.*

## Umsetzungsstand

Empfehlung 2 und 4 sind umgesetzt: `IRaftPeerAuthenticator` (`src/Blun.MultiRaft.Grpc/RaftPeerAuthenticator.cs`)
ist die Erweiterungsstelle, an der ein Client eine Identität behauptet und ein Server sie prüft, bevor er eine
Session überhaupt annimmt. `SharedSecretRaftPeerAuthenticator` bindet einen geteilten Schlüssel per
HMAC-SHA256 an die behauptete `NodeId` — wer den Schlüssel kennt, beweist Cluster-Mitgliedschaft, nicht eine
bestimmte Identität. `RaftStreamSession` trägt jetzt ein `PeerId`-Feld, und `HandleRequestAsync` verwirft jeden
`AppendEntries`-, `Vote`-, `InstallSnapshot`- oder `NodeLoad`-Frame, dessen behaupteter Absender nicht mit der
authentifizierten `PeerId` übereinstimmt (protokolliert, nicht nur verworfen) — das ist Empfehlung 2. Ist kein
`IRaftPeerAuthenticator` konfiguriert, bleibt der Endpunkt wie zuvor offen; `ConfigureRaftEndpoint`s XML-Doku
weist jetzt ausdrücklich darauf hin.

Schwächer als mTLS (Empfehlung 1) bleibt es: ein kompromittierter Schlüssel gilt für den ganzen Cluster, es
gibt keine Bindung an ein TLS-Zertifikat, und Schlüsselrotation ist Sache der Implementierung. Das Interface
ist genau deshalb schmal gehalten — eine mTLS-Variante, die die Zertifikatsidentität aus
`ServerCallContext.AuthContext` liest, lässt sich als weiterer `IRaftPeerAuthenticator` nachreichen, ohne einen
einzigen Aufrufer anzufassen.

## Verwandt

- [SEC-002](SEC-002-klartext-transport-als-default.md) — Vertraulichkeit/Integrität auf dem Transport
- [SEC-007](SEC-007-entry-kind-truncation.md) — was ein einzelner Frame zusätzlich anrichten kann
- [SEC-008](SEC-008-batch-append-umgeht-payload-limit.md) — Datenverlust über einen einzigen Frame
