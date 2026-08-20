# SEC-002 — Klartext (h2c) ist der Default des Transports

- **Schweregrad:** Hoch
- **Kategorie:** Fehlende Vertraulichkeit und Integrität auf dem Transport
- **Ort:** `src/Blun.MultiRaft.Grpc/RaftGrpcProtocol.cs`, `src/Blun.MultiRaft.Grpc/RaftKestrelExtensions.cs`, `src/Blun.MultiRaft.Grpc/GrpcRaftTransport.cs`

## Befund

Der Default-Wert von `GrpcRaftTransportOptions.Protocol` ist `RaftGrpcProtocol.Http2`, und dessen eigene
Dokumentation beschreibt ihn korrekt als *cleartext (h2c) unless the peer address is `https://`*.

Serverseitig ruft `ConfigureRaftEndpoint` **kein** `UseHttps` auf; das ist an den Aufrufer delegiert:

```csharp
listen.Protocols = protocol == RaftGrpcProtocol.Http3 ? HttpProtocols.Http3 : HttpProtocols.Http2;
configureListen?.Invoke(listen);   // hier müsste der Host UseHttps(...) einhängen
```

Clientseitig setzt `PeerConnection.OpenAsync` weder ein Zertifikat noch eine Validierung; ob TLS zum
Einsatz kommt, hängt allein davon ab, ob die konfigurierte Peer-`Uri` mit `https://` beginnt.

Das heißt: die *sichere* Variante erfordert drei zusammenpassende, voneinander unabhängige Entscheidungen
(Schema in `Peers`, `configureListen` auf dem Server, Zertifikatsverwaltung), die *unsichere* Variante
erfordert keine.

## Auswirkung

Ohne TLS ist der gesamte Replikationsverkehr im Klartext:

- **Vertraulichkeit:** Jede Queue-Nachricht, die durch `AppendEntries` repliziert wird, ist auf dem Netz
  mitlesbar. Auch Snapshots — also der komplette State — gehen ungeschützt über die Leitung.
- **Integrität:** Ein On-Path-Angreifer kann Frames verändern. Die WAL-Prüfsumme schützt hier nicht: sie
  wird erst *nach* der Übernahme lokal über den Datensatz berechnet und existiert laut eigener
  Dokumentation ausdrücklich nur, um einen Crash-Abriss zu erkennen — nicht als Transportsicherung.
- Zusammen mit [SEC-001](SEC-001-raft-transport-ohne-authentifizierung.md) genügt Netzzugang, um den
  Cluster zu übernehmen; TLS würde zumindest den passiven Angreifer aussperren.

## Bewertung des Trade-offs

Die Begründung im Code („no certificates required to stand up a local or trusted-network cluster") ist für
eine Demo nachvollziehbar. Für eine Bibliothek ist der Default aber die Voreinstellung, die die meisten
Deployments erben werden — und ein Konsensprotokoll, dessen Nachrichten den Zustand jedes Knotens
bestimmen, ist genau die Sorte Verkehr, die nicht ungeschützt laufen sollte.

## Empfehlung

1. Den Default auf TLS umstellen und Klartext zu einer *ausdrücklichen* Entscheidung machen — etwa
   `RaftGrpcProtocol.Http2Cleartext` als eigener, benannter Wert, statt Klartext als Nebeneffekt des
   Default-Enums.
2. `ConfigureRaftEndpoint` sollte bei einer Nicht-Loopback-Adresse ohne konfiguriertes `configureListen`
   mindestens warnen (oder eine explizite `allowCleartext: true`-Zusage verlangen).
3. Client- und Serverseite konsistent koppeln: Wenn `Peers` `http://`-Adressen enthält, während der Host
   TLS erwartet, scheitert das heute erst zur Laufzeit und sieht aus wie „das Cluster wählt nicht" — ein
   Startup-Check würde das sofort sichtbar machen.
4. Für HTTP/3 ist TLS ohnehin zwingend; das ist im Code korrekt beschrieben und kann als Vorlage dienen.

## Verwandt

- [SEC-001](SEC-001-raft-transport-ohne-authentifizierung.md)
