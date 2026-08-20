# SEC-004 — Unbegrenztes Request-Fan-out und unbegrenzte Sendewarteschlange

- **Schweregrad:** Mittel–Hoch
- **Kategorie:** Unkontrollierter Ressourcenverbrauch (CWE-400 / CWE-770)
- **Ort:** `src/Blun.MultiRaft.Grpc/RaftStreamSession.cs` — `PumpInboundAsync`, `_outbound`

## Befund

**(a) Jeder eingehende Request-Frame startet einen ungezählten Task.**

```csharp
default:
    // Handled off the read loop: a request may take an fsync to answer ...
    _ = HandleRequestAsync(frame);
    break;
```

Die Begründung (den Reader-Loop nicht hinter einem fsync blockieren) ist richtig, aber es gibt keine
Obergrenze: kein Semaphore, kein Zähler, keine Warteschlange. Der Reader-Loop liest so schnell vom Socket,
wie die Gegenstelle sendet, und erzeugt pro Frame einen Task samt der daran hängenden `RaftFrame`-Objekte.

**(b) Der Outbound-Channel ist unbegrenzt.**

```csharp
_outbound = Channel.CreateUnbounded<RaftFrame>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
```

Antworten und ausgehende Requests werden hier ohne Obergrenze abgelegt. Solange der Writer-Loop
langsamer schreibt, als produziert wird, wächst die Warteschlange unbeschränkt.

**(c) Es gibt keine Obergrenze für gleichzeitige Sessions.** `RaftProtocolService` legt pro eingehendem
Stream eine `RaftStreamSession` mit zwei Tasks, zwei Dictionaries und einem Channel an. Weder
`AddRaftProtocol` noch `ConfigureRaftEndpoint` begrenzen die Anzahl gleichzeitiger Verbindungen
(`KestrelServerOptions.Limits.MaxConcurrentConnections` bleibt unangetastet); `MaxStreamsPerConnection`
wird sogar bewusst von 100 auf 4096 angehoben.

## Auswirkung

- Ein Peer, der Frames schneller sendet, als der Knoten sie beantworten kann, treibt Speicher- und
  Thread-Pool-Verbrauch unbegrenzt nach oben. Da eine Antwort einen fsync enthalten kann, ist dieses
  Gefälle der Normalfall unter Last, nicht der Ausnahmefall.
- In Kombination mit [SEC-001](SEC-001-raft-transport-ohne-authentifizierung.md) ist das ein
  Ein-Paket-DoS von jedem, der den Port erreicht: 4096 Streams × unbegrenzte Frames.
- **(b) ist zusätzlich ein echtes Speicherleck**, siehe
  [BUG-002](PROBLEME-src-speicher-lecks-und-locks.md#bug-002): fällt der Writer-Loop aus,
  wird der Channel nie geschlossen und füllt sich bis zum Prozessende weiter.

## Empfehlung

1. Ein `SemaphoreSlim` mit konfigurierbarer Obergrenze um `HandleRequestAsync`; bei Erschöpfung den
   Reader-Loop *warten* lassen (Rückstau über HTTP/2-Flow-Control) statt weiter anzunehmen.
2. `_outbound` als `BoundedChannel` mit `BoundedChannelFullMode.Wait` anlegen. Die Kapazität sollte
   großzügig sein (Ziel ist Schutz, nicht Latenz), aber endlich.
3. `KestrelServerOptions.Limits.MaxConcurrentConnections` in `ConfigureRaftEndpoint` auf einen
   sinnvollen Default setzen bzw. als Parameter anbieten — die Methode nimmt bereits
   `maxStreamsPerConnection` entgegen, die Symmetrie fehlt.
4. `MaxReceiveMessageSize` explizit setzen (`AddGrpc(o => o.MaxReceiveMessageSize = ...)` und in den
   `GrpcChannelOptions`), statt sich auf den 4-MB-Default zu verlassen — die Bibliothek hat mit
   `SegmentedRaftWalOptions.MaxMessageBytes` bereits eine eigene, kleinere Vorstellung davon.

## Verwandt

- [SEC-003](SEC-003-unbegrenzte-snapshot-kanaele.md)
- [BUG-002](PROBLEME-src-speicher-lecks-und-locks.md#bug-002)
