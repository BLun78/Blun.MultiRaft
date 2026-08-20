# SEC-003 — Unbegrenzte Snapshot-Kanäle ohne Backpressure (Speicher-DoS)

- **Schweregrad:** Hoch
- **Kategorie:** Unkontrollierter Ressourcenverbrauch (CWE-400 / CWE-770)
- **Ort:** `src/Blun.MultiRaft.Grpc/RaftStreamSession.cs` — `HandleRequestAsync` (Fall `InstallSnapshot`), `HandleSnapshotChunk`

## Befund

Für jede eingehende `InstallSnapshot`-Anfrage wird ein **unbegrenzter** Channel angelegt:

```csharp
var body = Channel.CreateUnbounded<ReadOnlyMemory<byte>>(new UnboundedChannelOptions
{
    SingleReader = true,
    SingleWriter = true,
});
_inboundSnapshots[frame.CorrelationId] = body;
```

Gefüllt wird er aus dem Reader-Loop heraus mit `TryWrite`, also ohne jede Rückstauwirkung auf den Absender:

```csharp
channel.Writer.TryWrite(frame.SnapshotChunk.Data.ToByteArray());
```

`TryWrite` auf einem unbounded Channel schlägt nie fehl und wartet nie. Der Reader-Loop liest also so
schnell vom Socket, wie die Gegenstelle sendet, und legt jeden Chunk als **frisch allokiertes Byte-Array**
(`ToByteArray()`) im Speicher ab — unabhängig davon, wie schnell der Konsument (`RestoreAsync` der
State Machine, der typischerweise auf Platte schreibt) sie abholt.

Ein zweiter Pfad verschärft das: Die Größe eines Snapshots ist laut Design bewusst unbegrenzt
(„a snapshot is the one payload in this system with no size bound"), und `GrpcRaftTransport.InstallSnapshotAsync`
setzt aus demselben Grund absichtlich **keine Deadline**.

## Auswirkung

- Ein Peer, der schneller sendet als der Empfänger schreibt, treibt den Empfänger in den
  `OutOfMemoryException` — auch ohne böse Absicht, allein durch ein Geschwindigkeitsgefälle zwischen Netz
  und Platte. Das ist der wahrscheinlichere der beiden Fälle.
- Ein bösartiger Peer (siehe [SEC-001](SEC-001-raft-transport-ohne-authentifizierung.md): jeder kann
  Peer sein) sendet einen `InstallSnapshot`-Frame und danach beliebig viele Chunks. Ein einziger Frame
  genügt, um den Prozess zu beenden.
- Verschärfend: Lehnt die Gruppe den Snapshot früh ab (veralteter Term), kehrt der Handler zurück und
  entfernt den Channel-Eintrag — bis dahin wurden aber bereits alle eingetroffenen Chunks allokiert.

## Empfehlung

1. **Bounded Channel** mit `BoundedChannelFullMode.Wait` statt `CreateUnbounded`, und schreiben über
   `await WriteAsync(...)` statt `TryWrite`. Damit stoppt der Reader-Loop, und HTTP/2-Flow-Control setzt
   den Rückstau bis zum Absender fort — genau das, wofür sie da ist.
   *Achtung:* Der Reader-Loop bedient alle Gruppen dieser Verbindung; blockierendes Warten dort stoppt auch
   sie. Sauberer ist deshalb ein bounded Channel **plus** Auslagerung des Wartens, oder ein
   Gesamtbudget (siehe 2.).
2. **Ein konfigurierbares Byte-Budget pro Snapshot und pro Session.** Beim Überschreiten die Anfrage mit
   `InstallSnapshotResponse(Success: false)` beantworten und die Chunks verwerfen.
3. Chunks nicht einzeln als `byte[]` allokieren, sondern in einen gepoolten Puffer oder direkt in die
   Ziel-Datei streamen.
4. Eine Obergrenze für gleichzeitig offene `_inboundSnapshots` pro Session.

## Verwandt

- [SEC-004](SEC-004-unbegrenztes-request-fanout.md) — derselbe fehlende Rückstau auf dem Request-Pfad
- [SEC-001](SEC-001-raft-transport-ohne-authentifizierung.md)
