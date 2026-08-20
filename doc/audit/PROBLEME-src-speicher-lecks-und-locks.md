# `src/` — Probleme, Speicherlecks und Locks

Ergebnis des zweiten Durchgangs: eine vollständige Lesung von `src/Blun.MultiRaft`,
`src/Blun.MultiRaft.Wal` und `src/Blun.MultiRaft.Grpc` (rund 6.900 Zeilen echter Quelltext, ohne `obj/`)
mit Blick auf Ressourcenlecks, Sperren, Races und stille Fehlerpfade.

Reine Sicherheitsbefunde stehen als eigene `SEC-*.md` daneben und werden hier nur referenziert, wo sie
denselben Code betreffen.

**Zusammenfassung nach Schweregrad**

| ID | Schwere | Kurzfassung |
|---|---|---|
| [BUG-001](#bug-001) | Niedrig | `ArrayPool`-Double-Return, wenn `Rent` wirft (4 Stellen) |
| [BUG-002](#bug-002) | **Hoch** | Outbound-Channel wächst unbegrenzt, wenn der Writer-Loop stirbt |
| [BUG-003](#bug-003) | Mittel | `RaftStreamSession.DisposeAsync` entsorgt die CTS unter laufenden Loops |
| [BUG-004](#bug-004) | Mittel | Tote `PeerConnection` wird nie entsorgt — Socket-/Handle-Leck |
| [BUG-005](#bug-005) | **Hoch** | Eine unerwartete Exception beendet den Tick-Loop des gesamten Knotens |
| [BUG-006](#bug-006) | Mittel | `RaftMembership.Deserialize`: Integer-Overflow umgeht die Längenprüfung |
| [BUG-007](#bug-007) | **Hoch** | `MembershipChange.Read` ohne Längenprüfung — Gruppe startet nie wieder |
| [BUG-008](#bug-008) | Mittel | Verlorenes Wakeup in `ApplyCommittedAsync` |
| [BUG-009](#bug-009) | Mittel | Fire-and-forget `ValueTask` und verschluckte State-Machine-Fehler |
| [BUG-010](#bug-010) | **Hoch** | `_stateGate` wird über die gesamte Snapshot-Übertragung gehalten |
| [BUG-011](#bug-011) | **Hoch** | Windows: Löschen/Verschieben offener Dateien — halb ausgeführte Kompaktierung |
| [BUG-012](#bug-012) | Mittel | `_load` im Cluster-Koordinator wächst unbegrenzt |
| [BUG-013](#bug-013) | Mittel | Snapshots werden bei `RemoveGroupAsync` nie gelöscht |
| [BUG-014](#bug-014) | Mittel | Gleichzeitige Snapshot-Schreibvorgänge teilen einen Staging-Pfad |
| [BUG-015](#bug-015) | Mittel | Leere `wal.cfg` macht den Log dauerhaft unbeschreibbar |
| [BUG-016](#bug-016) | Niedrig | `ConcurrentDictionary.Values` in den heißesten Schleifen |
| [BUG-017](#bug-017) | Niedrig | `Task.Run` mit bereits abgebrochenem Token lässt In-Flight-Flags gesetzt |
| [BUG-018](#bug-018) | Niedrig | `MultiRaftHost.DisposeAsync` ist nicht idempotent |
| [BUG-019](#bug-019) | Niedrig | `AppendAsync` kann ohne Timeout unbegrenzt hängen |
| [BUG-020](#bug-020) | Niedrig | `SegmentedRaftWal.DisposeAsync` entsorgt das Write-Gate unter Wartenden |
| [BUG-021](#bug-021) | **Hoch** | Eine beschädigte Meta-Datei setzt den Wahlzustand still auf Term 0 zurück |

---

<a id="bug-001"></a>
## BUG-001 — `ArrayPool`-Double-Return, wenn `Rent` wirft

**Schwere:** Niedrig · **Ort:** `WalSegment.cs:143`, `SegmentedRaftWal.cs:307`, `RaftGroupInstance.cs:1133`
(und strukturgleich `SegmentedRaftWal.cs:208`)

Alle vier Stellen haben dieselbe Form:

```csharp
if (size > buffer.Length)
{
    ArrayPool<byte>.Shared.Return(buffer);
    buffer = ArrayPool<byte>.Shared.Rent(size);
}
```

und darunter ein `finally { ArrayPool<byte>.Shared.Return(buffer); }`.

Wirft `Rent` (praktisch: `OutOfMemoryException` bei einer sehr großen Anforderung — und die Größe kommt
teilweise von der Gegenstelle, siehe [SEC-008](SEC-008-batch-append-umgeht-payload-limit.md)), dann zeigt
`buffer` noch auf das **bereits zurückgegebene** Array, und das `finally` gibt es ein zweites Mal zurück.

Ein doppelt zurückgegebenes Array wird von `ArrayPool.Shared` an zwei Mieter gleichzeitig ausgegeben. Das
Ergebnis ist kein Absturz, sondern zwei Codepfade, die sich gegenseitig den Puffer überschreiben — also
genau die Klasse von stillem Datenfehler, die dieses Projekt schon einmal Tage gekostet hat
(`doc/open-issue-seed-visibility.md`).

**Behebung:** Erst mieten, dann zurückgeben:

```csharp
if (size > buffer.Length)
{
    byte[] larger = ArrayPool<byte>.Shared.Rent(size);
    ArrayPool<byte>.Shared.Return(buffer);
    buffer = larger;
}
```

---

<a id="bug-002"></a>
## BUG-002 — Der Outbound-Channel wächst unbegrenzt, wenn der Writer-Loop stirbt

**Schwere:** Hoch · **Ort:** `RaftStreamSession.cs:57`, `PumpOutboundAsync`, `GrpcRaftTransport.cs:290`

`PumpOutboundAsync` endet bei einem Transportfehler so:

```csharp
catch (Exception ex) when (ex is RpcException or InvalidOperationException or IOException)
{
    FailPending(ex);
}
```

Es gibt **kein** `finally { _outbound.Writer.TryComplete(); }`. Der Reader-Loop hat eines, der
Writer-Loop nicht. Der Channel bleibt also offen und unbegrenzt (`Channel.CreateUnbounded`), während
niemand mehr daraus liest.

Verschärfend prüft der Transport die Lebendigkeit einer Verbindung ausschließlich am *Reader*:

```csharp
public bool IsAlive => !Session.ReaderLoop.IsCompleted;
```

Solange der Reader noch läuft — bei einer halboffenen Verbindung, oder wenn der Schreibfehler ein
`InvalidOperationException` („response stream already completed") war — gilt die Session weiter als
lebendig. Jeder Heartbeat jeder Gruppe auf diesem Knotenpaar, jede Antwort aus `HandleRequestAsync`,
jeder Vote wird weiter in einen Channel geschrieben, den niemand leert.

**Auswirkung:** Stetig wachsender Speicherverbrauch bis zum Prozessende, und gleichzeitig hängt jeder
Aufrufer in `CallAsync` bis zu seinem Timeout — die Frames werden ja angenommen, nur nie gesendet. Bei
tausenden Gruppen pro Knotenpaar ist das die schnellste Art, den Prozess zu füllen.

**Behebung:**

1. `finally { _outbound.Writer.TryComplete(); }` in `PumpOutboundAsync`, symmetrisch zu `PumpInboundAsync`.
2. `IsAlive` gegen **beide** Loops prüfen:
   `!Session.ReaderLoop.IsCompleted && !Session.WriterLoop.IsCompleted`.
3. Den Channel begrenzen, siehe [SEC-004](SEC-004-unbegrenztes-request-fanout.md).

---

<a id="bug-003"></a>
## BUG-003 — `RaftStreamSession.DisposeAsync` entsorgt die CTS unter laufenden Loops

**Schwere:** Mittel · **Ort:** `RaftStreamSession.cs:241`

```csharp
public async ValueTask DisposeAsync()
{
    await _shutdown.CancelAsync().ConfigureAwait(false);
    _outbound.Writer.TryComplete();
    …
    _shutdown.Dispose();          // <- WriterLoop und ReaderLoop laufen möglicherweise noch
}
```

Die beiden Pump-Tasks werden nirgends abgewartet. Beide greifen laufend auf `_shutdown.Token` zu
(`_reader.MoveNext(_shutdown.Token)`, `_outbound.Reader.ReadAllAsync(_shutdown.Token)`), und jeder
`HandleRequestAsync`, der gerade in Arbeit ist, reicht `_shutdown.Token` an den Listener weiter.

`CancellationTokenSource.Token` wirft nach `Dispose()` eine `ObjectDisposedException`. Diese steht in
**keiner** der beiden Catch-Listen (`RpcException or InvalidOperationException or IOException`). Sie
verlässt also den Pump-Task, und `ReaderLoop` faultet — was `RaftProtocolService.Session` mit
`await session.ReaderLoop` direkt weiterreicht.

**Auswirkung:** Sporadische `ObjectDisposedException` beim Herunterfahren, die wie ein Transportfehler
aussehen. Zusätzlich laufen die Tasks nach dem Dispose noch weiter, was bei vielen Verbindungen den
Shutdown verschleiert.

**Behebung:** Vor `_shutdown.Dispose()` beide Loops abwarten:

```csharp
try
{
    await Task.WhenAll(ReaderLoop, WriterLoop).ConfigureAwait(false);
}
catch (Exception ex) when (ex is OperationCanceledException or RpcException or IOException or InvalidOperationException)
{
    // Beide Loops enden auf diesem Weg; das ist der normale Abschluss.
}

_shutdown.Dispose();
```

---

<a id="bug-004"></a>
## BUG-004 — Eine tote `PeerConnection` wird nie entsorgt

**Schwere:** Mittel (echtes Handle-/Socket-Leck) · **Ort:** `GrpcRaftTransport.cs:236–258`

```csharp
if (_peers.TryGetValue(target, out PeerConnection? existing) && existing.IsAlive)
{
    return existing.Session;
}
…
PeerConnection winner = _peers.AddOrUpdate(
    target,
    connection,
    (_, current) => current.IsAlive ? current : connection);

if (!ReferenceEquals(winner, connection))
{
    await connection.DisposeAsync().ConfigureAwait(false);
}
```

Ist die vorhandene Verbindung **tot**, gewinnt die neue: `AddOrUpdate` ersetzt den Eintrag, `winner` ist
die neue Verbindung, und der Zweig `!ReferenceEquals(...)` greift nicht. Die alte, tote `PeerConnection`
wird damit stillschweigend fallen gelassen — **ohne `DisposeAsync`**.

An ihr hängen ein `GrpcChannel`, ein `HttpClient`, ein `SocketsHttpHandler` und eine
`AsyncDuplexStreamingCall`. Der Finalizer räumt davon nichts vollständig auf.

`Drop(target)` macht es richtig (`_ = connection.DisposeAsync().AsTask()`), aber `Drop` wird nur nach einer
gefangenen Exception aufgerufen. Der hier beschriebene Pfad ist der andere: eine Verbindung, die von selbst
gestorben ist und beim nächsten `ConnectAsync` ersetzt wird.

**Auswirkung:** Bei einem Peer, der wiederholt neu startet oder flappt, akkumulieren sich Sockets und
Dateideskriptoren über die Lebensdauer des Prozesses.

**Behebung:** Den Verlierer in beide Richtungen entsorgen:

```csharp
PeerConnection? loser = null;
PeerConnection winner = _peers.AddOrUpdate(
    target,
    connection,
    (_, current) =>
    {
        if (current.IsAlive) { return current; }
        loser = current;
        return connection;
    });

if (loser is not null) { await loser.DisposeAsync().ConfigureAwait(false); }
if (!ReferenceEquals(winner, connection)) { await connection.DisposeAsync().ConfigureAwait(false); }
```

*Nebenbefund:* `PeerConnection.OpenAsync` nimmt einen `cancellationToken` entgegen und benutzt ihn
nirgends — `client.Session(cancellationToken: default)` ignoriert ihn ausdrücklich. Entweder durchreichen
oder den Parameter entfernen.

---

<a id="bug-005"></a>
## BUG-005 — Eine unerwartete Exception beendet den Tick-Loop des gesamten Knotens

**Schwere:** Hoch · **Ort:** `MultiRaftHost.cs:361`, `TickLoopAsync`

Die Absicht steht im Kommentar und ist richtig:

> One sick group must never stop the clock for the thousands beside it.

Die Umsetzung deckt sie aber nicht:

```csharp
catch (Exception ex) when (ex is IOException or InvalidOperationException)
{
    HostLog.TickFailed(_logger, ex, instance.Group.Value);
    return true;
}
```

Alles andere fliegt aus `TickOneAsync` heraus in `TickLoopAsync`, dessen `try` nur
`OperationCanceledException` fängt — der Loop endet. Damit steht die Uhr für **jede** Gruppe des Knotens:
keine Heartbeats, keine Wahlen, keine Kompaktierung. Nichts startet ihn neu, und nichts protokolliert es;
der Fehler wird erst sichtbar, wenn `DisposeAsync` das `_ticker`-Task abwartet und ihn dann weiterwirft.

Erreichbare Exceptions, die nicht in der Liste stehen:

- `ArgumentOutOfRangeException` aus `MemoryMappedSegmentDevice.WriteAsync` („Write runs past the end of the
  segment") — erreichbar über [SEC-008](SEC-008-batch-append-umgeht-payload-limit.md).
- `IndexOutOfRangeException` / `ArgumentOutOfRangeException` aus `MembershipChange.Read`, siehe
  [BUG-007](#bug-007).
- `ObjectDisposedException`, wenn eine Gruppe parallel entsorgt wird.
- Alles, was eine Host-`IRaftStateMachine` wirft und bis hierher durchschlägt.

**Behebung:** In `TickOneAsync` alles außer `OperationCanceledException` fangen und protokollieren
(`catch (Exception ex)` mit `#pragma warning disable CA1031`, wie es `MessageSender` bereits vormacht).
Zusätzlich sollte `TickLoopAsync` einen unerwarteten Abbruch laut protokollieren, statt still zu enden.

Dasselbe Muster steht an drei weiteren Stellen und verdient dieselbe Behandlung:
`RaftGroupInstance.cs:791` (Wahlkampf), `RaftGroupInstance.cs:1916` (Auto-Kompaktierung),
`ClusterCoordinator.cs:601` (Koordinationsschleife). Bei den letzten beiden ist die Folge ebenfalls ein
dauerhaft toter Hintergrund-Loop.

---

<a id="bug-006"></a>
## BUG-006 — `RaftMembership.Deserialize`: Integer-Overflow umgeht die Längenprüfung

**Schwere:** Mittel · **Ort:** `RaftMembership.cs:133`

```csharp
int voterCount = BinaryPrimitives.ReadInt32LittleEndian(source);
int learnerCount = BinaryPrimitives.ReadInt32LittleEndian(source[4..]);
if (voterCount < 0 || learnerCount < 0 || source.Length < 8 + ((voterCount + learnerCount) * 8))
{
    return Empty;
}

var voters = ImmutableArray.CreateBuilder<NodeId>(voterCount);
```

Die Prüfung ist gut gemeint, rechnet aber in `int`. Mit `voterCount = 0x10000000` und
`learnerCount = 0x10000000` ergibt `(0x20000000) * 8` genau `0x100000000`, was als `int` zu **0**
überläuft. Die Bedingung `source.Length < 8` ist damit falsch, die Prüfung greift nicht, und
`CreateBuilder<NodeId>(0x10000000)` fordert 2 GB an.

Die Eingabe ist nicht lokal: `Deserialize` wird in `RaftGroupInstance.OnInstallSnapshotAsync` auf
`request.Configuration` angewendet — also auf Bytes, die direkt von der Leitung kommen. 16 gezielt
gewählte Bytes genügen.

**Reproduziert**, nicht nur hergeleitet. Die Prüfbedingung aus Zeile 133 wörtlich übernommen und mit
`voterCount = learnerCount = 0x10000000` gegen eine 16-Byte-Eingabe laufen lassen:

```
payload length      : 16
voterCount+learners : 536870912
(sum) * 8 as int    : 0            <- Überlauf
guard bound         : 8
guard passes        : True   (voters=268435456, learners=268435456)
allocated builder capacity 268435456 (2048 MB backing array)
```

Bemerkenswert am Rand: Schreibt man dieselbe Rechnung mit *Konstanten* hin, lehnt der Compiler sie mit
`CS0220` („Vorgangsüberlauf während der Kompilierzeit im aktivierten Modus") ab. Mit Variablen — also so,
wie sie hier steht — wickelt sie still um. Der Compiler kennt das Problem; er kann es an dieser Stelle nur
nicht sehen.

**Behebung:** In `long` rechnen:

```csharp
long total = (long)voterCount + learnerCount;
if (voterCount < 0 || learnerCount < 0 || source.Length < 8 + (total * 8))
{
    return Empty;
}
```

Sinnvoll ist zusätzlich eine harte Obergrenze für die Mitgliederzahl — eine Raft-Gruppe mit Millionen
Votern ist ohnehin kein gültiger Zustand.

---

<a id="bug-007"></a>
## BUG-007 — `MembershipChange.Read` ohne Längenprüfung: die Gruppe startet nie wieder

**Schwere:** Hoch · **Ort:** `RaftMembership.cs:44`, Aufrufer `RaftGroupInstance.cs:1059, 1174, 1966`

```csharp
public static MembershipChange Read(ReadOnlySpan<byte> source)
    => new((MembershipChangeKind)source[0], new NodeId(BinaryPrimitives.ReadUInt64LittleEndian(source[1..])));
```

Keine Prüfung gegen `PayloadSize` (9). Ein Membership-Eintrag mit kürzerer Payload wirft.

Der gefährliche Aufrufer ist `FlushBatchAsync` (`RaftGroupInstance.cs:1174`), und die **Reihenfolge** ist
das Problem:

```csharp
RaftLogEntry[] items = [.. batch];
await _wal.AppendAsync(items, cancellationToken).ConfigureAwait(false);   // 1. persistiert

foreach (RaftLogEntry entry in items)
{
    if (entry.Kind == RaftEntryKind.Membership)
    {
        ApplyMembership(MembershipChange.Read(entry.Payload.Span));       // 2. wirft
    }
}
```

Der Eintrag ist zu diesem Zeitpunkt bereits **auf der Platte**. Die Exception verlässt
`OnAppendEntriesAsync`, wird in `RaftStreamSession.HandleRequestAsync` nicht gefangen (sie ist weder
`RpcException` noch `InvalidOperationException` noch `IOException`) und verschwindet in einem
unbeobachteten Task.

Beim nächsten Start läuft `ReplayMembershipAsync` (`RaftGroupInstance.cs:1966`) über denselben Eintrag und
wirft erneut — diesmal aus `StartAsync` heraus. **Die Gruppe lässt sich nicht mehr starten**, und der
einzige Weg zurück ist, den Log von Hand zu löschen.

Erreichbar ist das über einen Peer (siehe [SEC-001](SEC-001-raft-transport-ohne-authentifizierung.md))
oder — ganz ohne Angreifer — über die Kind-Truncation aus
[SEC-007](SEC-007-entry-kind-truncation.md): ein gewöhnliches `Command` mit `Kind = 258` und weniger als
neun Byte Payload wird beim Empfänger zu einem Membership-Eintrag und bricht die Gruppe dauerhaft.

**Behebung:**

1. Längenprüfung in `Read`:
   ```csharp
   if (source.Length < PayloadSize)
   {
       throw new InvalidOperationException("A membership entry payload must be " + PayloadSize + " bytes.");
   }
   ```
   `InvalidOperationException` ist hier der richtige Typ (Integritätsverletzung, wird oben gefangen und
   protokolliert statt still verschluckt).
2. Wichtiger noch: **validieren, bevor geschrieben wird.** In `AbsorbEntriesAsync` jeden
   Membership-Eintrag prüfen, solange er noch im Batch steht, und den ganzen `AppendEntries` mit
   `Success: false` ablehnen. Ein Eintrag, den dieser Knoten nicht anwenden kann, darf gar nicht erst in
   seinen Log.

---

<a id="bug-008"></a>
## BUG-008 — Verlorenes Wakeup in `ApplyCommittedAsync`

**Schwere:** Mittel · **Ort:** `RaftGroupInstance.cs:1834`

```csharp
if (!await _applyGate.WaitAsync(0).ConfigureAwait(false))
{
    // Another apply pass is already running; it will pick up whatever this one would have.
    return;
}
```

Die Annahme im Kommentar stimmt nicht. Der laufende Durchgang hat seinen Zielwert bereits gelesen:

```csharp
long committed = Volatile.Read(ref _commitIndex);
long applied   = Volatile.Read(ref _lastApplied);
```

Schiebt sich `_commitIndex` **nach** dieser Lesung weiter und ruft der Anstoßer `ApplyCommittedAsync`
auf, während das Gate noch belegt ist, kehrt der neue Aufruf sofort zurück — und der laufende Durchgang
wendet nur bis zu seinem alten `committed` an. Danach stößt nichts mehr an.

**Auswirkung:** `_lastApplied` bleibt hinter `_commitIndex` zurück, bis zufällig ein weiterer Commit oder
ein weiteres `AppendEntries` eintrifft. Auf einer Gruppe, die genau in diesem Moment still wird, bleibt der
Rückstand dauerhaft: committete Einträge erreichen die State Machine nie.

`WaitForAppliedAsync` pollt im Millisekundentakt und übertüncht das für linearisierbare Lesevorgänge — um
den Preis einer Warteschleife, die genau dann heiß läuft, wenn sie es nicht sollte.

**Behebung:** Nach dem Freigeben erneut prüfen, statt sich auf den anderen Durchgang zu verlassen:

```csharp
do
{
    // … anwenden …
}
while (Volatile.Read(ref _lastApplied) < Volatile.Read(ref _commitIndex));
```

innerhalb des Gates, oder ein Flag `_applyRequested`, das der abgewiesene Aufrufer setzt und der laufende
Durchgang vor dem Release prüft.

---

<a id="bug-009"></a>
## BUG-009 — Fire-and-forget `ValueTask` und verschluckte State-Machine-Fehler

**Schwere:** Mittel · **Ort:** `RaftGroupInstance.cs:914, 1785`

```csharp
finally
{
    _stateGate.Release();
    _ = ApplyCommittedAsync();        // OnAppendEntriesAsync
}
…
Volatile.Write(ref _commitIndex, candidate);
ReleaseCommitWaiters(candidate);
_ = ApplyCommittedAsync();            // AdvanceCommitIndexAsync
```

Zwei getrennte Probleme:

**1. Ein `ValueTask` darf nicht verworfen werden.** Das ist keine Stilfrage: `ValueTask` ist ausdrücklich
dokumentiert als „darf genau einmal konsumiert werden". Ist das Pooling von
`AsyncValueTaskMethodBuilder` aktiv (Laufzeitschalter `System.Threading.Tasks.ValueTaskPoolingEnabled`),
wird das zugrunde liegende `IValueTaskSource` recycelt, während der verworfene Aufruf noch läuft — mit
undefiniertem Ergebnis. Korrekt wäre mindestens `_ = ApplyCommittedAsync().AsTask();`.

**2. Fehler der State Machine verschwinden spurlos.** `ApplyCommittedAsync` fängt ausschließlich
`OperationCanceledException`. Wirft `_stateMachine.ApplyAsync` irgendetwas anderes — und das ist
Host-Code, über den diese Bibliothek per Definition nichts weiß —, dann:

- landet die Exception in einem verworfenen `ValueTask` und wird nie beobachtet,
- bleibt `_lastApplied` für immer stehen,
- gibt es **keine einzige Logzeile** darüber.

Die Gruppe repliziert und committet danach weiter fröhlich, wendet aber nichts mehr an. Von außen sieht
das aus wie eine gesunde Gruppe. Das ist exakt die Fehlerklasse, die dieses Projekt an anderer Stelle
ausdrücklich bekämpft hat („Swallowing that without a word made a whole class of follower-side failure
invisible from both ends of the wire", `RaftStreamSession`).

**Behebung:** Einen `catch (Exception ex)` mit einer eigenen `[LoggerMessage]`-Zeile ergänzen (EventId im
Bereich 1000–1016), und den Aufruf als `.AsTask()` mit angehängtem Fehler-Logging starten.

---

<a id="bug-010"></a>
## BUG-010 — `_stateGate` wird über die gesamte Snapshot-Übertragung gehalten

**Schwere:** Hoch · **Ort:** `RaftGroupInstance.OnInstallSnapshotAsync` (ab `RaftGroupInstance.cs:1493`)

Die Methode nimmt `_stateGate` **ganz am Anfang** und hält es bis zum Schluss — dazwischen liegt:

```csharp
await _snapshots.WriteAsync(Group, metadata, body, cancellationToken).ConfigureAwait(false);
await snapshotable.RestoreAsync(Group, _snapshots.ReadAsync(Group, cancellationToken), cancellationToken)…
await _wal.ResetToSnapshotAsync(…)…
```

`body` ist der **Netzwerkstrom**. `WriteAsync` konsumiert ihn vollständig, Chunk für Chunk, und ein
Snapshot ist laut Design „the one payload in this system with no size bound". Absenderseitig setzt
`GrpcRaftTransport.InstallSnapshotAsync` aus demselben Grund bewusst **keine Deadline**.

Solange das läuft, ist `_stateGate` belegt. Das blockiert auf derselben Gruppe:

- `OnAppendEntriesAsync` — der Knoten nimmt keine Replikation mehr an,
- `OnRequestVoteAsync` — er kann keine Stimme abgeben, was fremde Wahlen ausbremst,
- `CampaignAsync`, `StepAsideAsync`, `BecomeLeaderAsync`, `AppendCoreAsync`.

Das Gate ist ausdrücklich asynchron gebaut, damit ein Gruppen-Lock nie einen Pool-Thread hält
(„a group holding an OS lock while its fsync completes would block a pool thread that thousands of
sibling groups need"). Über eine unbegrenzte Netzwerkübertragung gehalten, ist es aber genau das, was es
vermeiden sollte — nur ohne Thread-Blockade.

**Sicherheitsrelevanz:** Ein Absender, der einen `InstallSnapshot`-Frame schickt und dann langsam oder gar
nicht weitersendet, hält das `_stateGate` einer Gruppe beliebig lange. Zusammen mit
[SEC-001](SEC-001-raft-transport-ohne-authentifizierung.md) ist das ein gezielter Ein-Frame-DoS gegen eine
einzelne Gruppe auf einem einzelnen Knoten — und zusammen mit
[SEC-003](SEC-003-unbegrenzte-snapshot-kanaele.md) gleichzeitig ein Speicher-DoS.

**Behebung:** Die Übertragung aus dem Gate herausziehen. Vorschlag:

1. Unter dem Gate nur die Term-Prüfung, `StepDownAsync` und die Entscheidung „annehmen oder verwerfen".
2. Gate freigeben, den Body in die Staging-Datei streamen (`_snapshots.WriteAsync`).
3. Gate erneut nehmen und die Zustandsübernahme (`RestoreAsync`, `ResetToSnapshotAsync`,
   `_commitIndex`/`_lastApplied`/`_membership`) unter dem Gate abschließen — mit erneuter Term-Prüfung,
   weil sich in der Zwischenzeit etwas geändert haben kann.
3. Ein Snapshot-Empfang pro Gruppe gleichzeitig (eigenes `Interlocked`-Flag, wie `_compactionInFlight`).
4. Einen Inaktivitäts-Timeout auf dem Chunk-Strom, damit ein stiller Absender nicht unbegrenzt hält.

---

<a id="bug-011"></a>
## BUG-011 — Windows: Löschen und Verschieben offener Dateien

**Schwere:** Hoch (plattformspezifisch) · **Ort:** `WalSegment.cs:206, 298`, `RaftSnapshot.cs:140, 179, 226`

`CLAUDE.md` hält ausdrücklich fest, dass Windows, Linux und macOS gleichrangige Ziele sind und dass
`FileShare` „enforced on Windows, advisory on Unix" ist. Genau an dieser Kante liegen zwei Fehler.

**(a) Kompaktierung gegen einen lesenden Follower.**

`WalSegment.OpenRead` (Zeile 206) öffnet ohne `FileShare.Delete`:

```csharp
public SafeFileHandle OpenRead()
    => File.OpenHandle(Path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, FileOptions.Asynchronous);
```

`SegmentedRaftWal.ReadFromAsync` hält so ein Handle offen, während es einem Follower Einträge liefert —
und nimmt dafür bewusst **nicht** das Write-Gate („Reads do not take the gate at all"). Läuft parallel
`TruncateHeadAsync`, ruft das `WalSegment.Delete` auf:

```csharp
try { File.Delete(Path); }
catch (FileNotFoundException) { … }
catch (DirectoryNotFoundException) { … }
```

Unter Windows scheitert `File.Delete` bei einem offenen Handle ohne `FILE_SHARE_DELETE` mit einer
`IOException` („being used by another process"). Die steht in keinem der beiden `catch`.

Die Folge ist schlimmer als der Abbruch selbst: `TruncateHeadAsync` hat zu diesem Zeitpunkt bereits einen
Teil der `doomed`-Segmente gelöscht, aktualisiert aber `_segments`, `_firstIndex` und die Base-Datei
**erst danach**. Der Log bleibt in einem Zustand zurück, in dem gelöschte Segmente noch in `_segments`
stehen — jeder Lesezugriff darauf endet in `FileNotFoundException`.

Unter Linux tritt nichts davon auf: der `unlink` gelingt, der Leser liest die entkoppelte Inode zu Ende.
Der Fehler ist also nur auf Windows sichtbar und dort auch nur unter Last.

**(b) Snapshot schreiben, während einer gesendet wird.**

`FileRaftSnapshotStore.ReadAsync` (Zeile 179) benutzt `File.OpenRead`, also `FileShare.Read` — kein
`Delete`, kein `Write`. Genau dieser Strom wird in `SendSnapshotAsync` an einen Peer geschickt und kann
Minuten offen bleiben. Läuft parallel `TakeSnapshotAsync`, endet es in

```csharp
File.Move(staging, path, overwrite: true);     // Zeile 226
```

was unter Windows an demselben offenen Handle mit `IOException` scheitert. Zurück bleibt eine
`.tmp`-Datei, und die Kompaktierung in `TakeSnapshotAsync` läuft danach nicht mehr — der Log wächst weiter.

**Behebung:**

1. In `WalSegment.OpenRead` und in `SegmentedRaftWal`s übrigen Lesepfaden
   `FileShare.ReadWrite | FileShare.Delete` verwenden. Das ist der Windows-Weg, POSIX-Semantik zu bekommen.
2. `FileRaftSnapshotStore.ReadAsync` und `ReadMetadataAsync` mit
   `new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete)` öffnen
   statt mit `File.OpenRead`.
3. `WalSegment.Delete` zusätzlich `IOException` fangen und das Segment für einen späteren Versuch
   vormerken, statt die Kompaktierung halb ausgeführt zu verlassen.
4. In `TruncateHeadAsync` die Reihenfolge umdrehen: erst `_segments`/`_firstIndex`/Base umstellen, dann
   die Dateien löschen. Ein nicht gelöschtes Segment kostet Platz; ein gelöschtes Segment in `_segments`
   kostet Korrektheit.

---

<a id="bug-012"></a>
## BUG-012 — `_load` im Cluster-Koordinator wächst unbegrenzt

**Schwere:** Mittel · **Ort:** `ClusterCoordinator.cs:345`, `OnLoadReportAsync`

```csharp
public ValueTask OnLoadReportAsync(NodeLoadReport report, CancellationToken cancellationToken = default)
{
    long now = _time.GetTimestamp();
    _load.AddOrUpdate(report.Node, …);
    return ValueTask.CompletedTask;
}
```

Zwei Lücken:

1. **`report.Node` wird nicht geprüft.** Es gibt keinen Abgleich gegen `_options.EffectiveNodes` oder
   gegen die Mitgliedschaft der Cluster-Gruppe. Jede beliebige `NodeId` legt einen Eintrag an.
2. **Es wird nie etwas entfernt.** `GetLoad()` und `OrderCandidates` *filtern* zwar nach
   `LoadReportTtl`, aber ausschließlich beim Lesen. `_load` selbst schrumpft nie.

Der Klassenkommentar begründet die weichen, TTL-gealterten Zustandsdaten überzeugend („Load reports are
pushed, never logged … Soft state, held in memory on the cluster leader, aged out by TTL") — das Altern
findet aber nur in der Ausgabe statt, nicht im Speicher.

**Auswirkung:** Ein unauthentifizierter Absender (siehe
[SEC-001](SEC-001-raft-transport-ohne-authentifizierung.md)) kann mit Load-Reports über zufällige Node-IDs
den Speicher des Cluster-Leaders unbegrenzt füllen. Auch ohne Angreifer bleiben ausgemusterte Knoten für
immer stehen, und `GetLoad()` iteriert bei jedem Aufruf über die gesamte Menge.

**Behebung:**

1. Reports von Knoten ablehnen, die weder in `_options.EffectiveNodes` noch in
   `_group.Membership.AllMembers` stehen.
2. Abgelaufene Einträge im Koordinationsdurchgang tatsächlich entfernen — die Schleife läuft ohnehin
   regelmäßig:
   ```csharp
   foreach (LoadEntry entry in _load.Values)
   {
       if (ElapsedSince(entry.Timestamp, now) > _options.LoadReportTtl)
       {
           _load.TryRemove(entry.Node, out _);
       }
   }
   ```

---

<a id="bug-013"></a>
## BUG-013 — Snapshots werden bei `RemoveGroupAsync` nie gelöscht

**Schwere:** Mittel · **Ort:** `MultiRaftHost.cs`, `RemoveGroupAsync`

```csharp
await instance.DisposeAsync().ConfigureAwait(false);
if (deleteData)
{
    await _walFactory.DeleteAsync(group, cancellationToken).ConfigureAwait(false);
    await _metaStore.DeleteAsync(group, cancellationToken).ConfigureAwait(false);
}
```

`_snapshots` fehlt. `IRaftSnapshotStore.DeleteAsync` ist im gesamten `src/`-Baum **kein einziges Mal**
aufgerufen — die Methode existiert, wird implementiert und ist toter Code.

Zwei Folgen:

1. **Platzleck.** Der Sinn von `deleteData` ist laut Kommentar „the queue it backed was deleted, so keeping
   either would only waste the disk". Ausgerechnet der potenziell größte Bestandteil bleibt liegen.
2. **Korrektheitsrisiko bei Wiederverwendung einer Gruppen-ID.** Wird später eine Gruppe mit derselben ID
   angelegt, findet `SendSnapshotAsync` bzw. `ReadMetadataAsync` den Snapshot der *gelöschten* Gruppe. Er
   trägt einen `LastIncludedIndex` und eine Konfiguration aus einem völlig anderen Leben — und wird als
   gültiger Snapshot dieser Gruppe behandelt.

**Behebung:** `await _snapshots?.DeleteAsync(group, cancellationToken)` im `deleteData`-Zweig ergänzen.

---

<a id="bug-014"></a>
## BUG-014 — Gleichzeitige Snapshot-Schreibvorgänge teilen einen Staging-Pfad

**Schwere:** Mittel · **Ort:** `RaftSnapshot.cs:203`

```csharp
string path = PathFor(group);
string staging = path + ".tmp";

await using (FileStream stream = File.Create(staging))
```

Der Staging-Name ist pro Gruppe fest. Zwei gleichzeitige `WriteAsync` für dieselbe Gruppe schreiben in
**dieselbe Datei**, und beide verschieben sie anschließend über den Zielpfad.

Dass das passieren kann, ist nicht theoretisch. `MaybeStartAutoCompaction` schützt sich mit
`_compactionInFlight`, dieses Flag deckt aber nur die automatische Kompaktierung ab. Ungeschützt sind:

- ein direkter Aufruf des **öffentlichen** `TakeSnapshotAsync` neben der automatischen Kompaktierung,
- `OnInstallSnapshotAsync`, das ebenfalls `_snapshots.WriteAsync` für dieselbe Gruppe aufruft — ein
  Follower kann also gleichzeitig einen Snapshot empfangen und einen eigenen erzeugen.

Ergebnis ist eine Datei mit ineinander geschriebenen Bytes, die anschließend als gültiger Snapshot an den
Zielpfad verschoben wird. Der Magic-Header stimmt, die Prüfung in `ReadMetadataAsync` greift nicht, und
`RestoreAsync` bekommt Müll. Für einen Store, dessen erklärter Zweck der atomare Austausch ist
(„The move is the commit point"), ist das die eine Lücke, die den Zweck aufhebt.

**Behebung:**

1. Eindeutigen Staging-Namen verwenden: `path + "." + Guid.NewGuid().ToString("N") + ".tmp"`, und
   verwaiste `.tmp`-Dateien beim Öffnen aufräumen.
2. Zusätzlich in `RaftGroupInstance` sicherstellen, dass pro Gruppe nur ein Snapshot-Schreibvorgang läuft
   — dasselbe `Interlocked`-Flag für `TakeSnapshotAsync` *und* `OnInstallSnapshotAsync`.

---

<a id="bug-015"></a>
## BUG-015 — Eine leere `wal.cfg` macht den Log dauerhaft unbeschreibbar

**Schwere:** Mittel · **Ort:** `SegmentedRaftWal.LoadConfigAsync` / `SaveConfig`

```csharp
private async ValueTask LoadConfigAsync(CancellationToken cancellationToken)
{
    if (!File.Exists(ConfigPath)) { return; }
    …
    int read = await RandomAccess.ReadAsync(handle, buffer, 0, cancellationToken).ConfigureAwait(false);
    if (read < 1) { return; }          // <- _baseFileWritten bleibt false
    …
    _baseFileWritten = true;
}

private void SaveConfig()
{
    if (_baseFileWritten) { return; }
    …
    using SafeFileHandle handle = File.OpenHandle(ConfigPath, FileMode.CreateNew, …);   // wirft
```

Existiert `wal.cfg` mit null Byte Länge — der Zustand nach einem Absturz zwischen Anlegen und Schreiben —,
dann bleibt `_baseFileWritten` auf `false`, und `SaveConfig` versucht die Datei mit `FileMode.CreateNew`
neu anzulegen. Das wirft eine `IOException`, weil sie schon da ist.

`SaveConfig` wird von **beiden** Append-Überladungen bei jedem Aufruf ausgeführt. Der Log ist damit
dauerhaft unbeschreibbar.

Und die Art der Exception macht es still: Laut Konvention des Projekts ist `IOException` „an ordinary
retryable condition", die der Replikationspfad endlos wiederholt. Die Gruppe dreht sich also lautlos im
Kreis — exakt das Verhalten, vor dem `CLAUDE.md` unter „Code conventions" warnt, nur mit vertauschten
Rollen.

**Behebung:**

- `FileMode.Create` statt `FileMode.CreateNew` (die Datei ist ein einzelnes Byte, das Überschreiben ist
  harmlos), **oder**
- eine leere/zu kurze `wal.cfg` in `LoadConfigAsync` als „noch nicht geschrieben" behandeln und
  ausdrücklich neu erzeugen.

Zusätzlich sollte ein Fehler beim Schreiben der Konfiguration als `InvalidOperationException`
weitergereicht werden, nicht als `IOException` — es ist kein Transportproblem, sondern ein
Integritätsproblem.

---

<a id="bug-016"></a>
## BUG-016 — `ConcurrentDictionary.Values` in den heißesten Schleifen

**Schwere:** Niedrig (Allokationsdruck, kein Fehlverhalten) · **Ort:**
`MultiRaftHost.cs:64, 78, 299, 325`, `RaftGroupInstance.cs:1316`, `ClusterCoordinator.cs:345`

`ConcurrentDictionary<K,V>.Values` ist **keine** Sicht, sondern eine Kopie: die Eigenschaft nimmt alle
Bucket-Locks und baut eine neue `ReadOnlyCollection<TValue>` auf.

Betroffen sind ausgerechnet die Pfade, für die die Architektur ausdrücklich gebaut wurde:

```csharp
foreach (RaftGroupInstance instance in _groups.Values)   // TickLoopAsync, jeder Tick
foreach (PeerReplicationState peer in _peers.Values)     // PushToAllPeers, jeder Heartbeat
public IReadOnlyCollection<RaftGroupInstance> Groups => [.. _groups.Values];   // doppelte Kopie
```

Bei den angepeilten zehntausend Gruppen und 25 ms Tick-Intervall sind das 40 Listen mit je 10.000
Einträgen **pro Sekunde**, allein für den Tick-Loop. Der Kommentar an `LeaderCount` argumentiert, der
Scan koste „microseconds" — das stimmt für die Schleife, unterschlägt aber die Kopie davor.

Das läuft dem erklärten Ziel des Designs direkt zuwider („A per-group timer is a queue entry + callback +
allocation per group per tick, which dominates CPU at ten-thousand-group scale").

**Behebung:** Über das Dictionary selbst iterieren — das ist lazy und allokationsfrei:

```csharp
foreach (KeyValuePair<RaftGroupId, RaftGroupInstance> pair in _groups)
{
    RaftGroupInstance instance = pair.Value;
    …
}
```

Für `Groups` (öffentliche API) ist die Kopie in Ordnung; für `LeaderCount` und die Tick-/Heartbeat-Pfade
nicht.

---

<a id="bug-017"></a>
## BUG-017 — `Task.Run` mit bereits abgebrochenem Token lässt In-Flight-Flags gesetzt

**Schwere:** Niedrig · **Ort:** `RaftGroupInstance.cs` — `StartCampaign`, `PushToAllPeers`,
`MaybeStartAutoCompaction`

Alle drei folgen demselben Muster:

```csharp
if (Interlocked.CompareExchange(ref _campaignInFlight, 1, 0) != 0) { return; }

_ = Task.Run(async () => { try { … } finally { Volatile.Write(ref _campaignInFlight, 0); } },
             _shutdown.Token);
```

Das Flag wird **vor** `Task.Run` gesetzt, zurückgesetzt wird es im `finally` **innerhalb** des Delegaten.
Ist `_shutdown.Token` bereits abgebrochen, wenn `Task.Run` aufgerufen wird, läuft der Delegat nie — und
das Flag bleibt für immer auf 1.

Praktisch trifft das nur den Shutdown, weil `_shutdown` ausschließlich in `DisposeAsync` abgebrochen wird;
die Gruppe wird also ohnehin gerade abgebaut. Es ist trotzdem eine Falle: Sobald jemand `_shutdown` für
etwas anderes als „endgültig" verwendet, latcht die Gruppe stumm — sie kann nie wieder kandidieren, nie
wieder replizieren, nie wieder kompaktieren, und nichts sagt es.

**Behebung:** Das Flag im Delegaten setzen, oder `Task.Run(…)` ohne Token aufrufen (die Abbruchprüfung
steht im Rumpf ohnehin schon) und das `finally` die Arbeit machen lassen.

---

<a id="bug-018"></a>
## BUG-018 — `MultiRaftHost.DisposeAsync` ist nicht idempotent

**Schwere:** Niedrig · **Ort:** `MultiRaftHost.cs:299 ff.`

```csharp
public async ValueTask DisposeAsync()
{
    await _shutdown.CancelAsync().ConfigureAwait(false);
    …
    _shutdown.Dispose();
}
```

Kein `_disposed`-Flag. Ein zweiter Aufruf ruft `CancelAsync()` auf einer bereits entsorgten
`CancellationTokenSource` und wirft `ObjectDisposedException`.

`IAsyncDisposable.DisposeAsync` muss laut Kontrakt mehrfach aufrufbar sein, und die Konstellation ist
naheliegend: `RaftGroupInstance`, `ClusterCoordinator` und `SegmentedRaftWal` haben dieses Flag alle —
`MultiRaftHost` als einziger nicht.

Zusätzlich fängt der `await _ticker`-Block nur `OperationCanceledException`; ist der Tick-Loop mit einer
anderen Exception gestorben (siehe [BUG-005](#bug-005)), wirft `DisposeAsync` sie beim Herunterfahren
weiter.

**Behebung:** `if (_disposed) { return; } _disposed = true;` an den Anfang, und den `await _ticker`-Block
gegen alle Exceptions absichern.

---

<a id="bug-019"></a>
## BUG-019 — `AppendAsync` kann ohne Timeout unbegrenzt hängen

**Schwere:** Niedrig (Designentscheidung, aber ohne Warnhinweis) · **Ort:**
`RaftGroupInstance.AppendCoreAsync`, `WaitForCommitAsync`

Mit `DurabilityLevel.Quorum` — dem **Default** — endet ein Append in:

```csharp
await WaitForCommitAsync(index, cancellationToken).ConfigureAwait(false);
```

`WaitForCommitAsync` wartet auf ein `TaskCompletionSource`, das nur durch einen Commit oder durch den
übergebenen Token freigegeben wird. Da `cancellationToken` einen Default-Wert hat (`default`), ist der
gängigste Aufruf `AppendAsync(payload)` — und der hängt **für immer**, wenn die Gruppe ihr Quorum verliert.

Das ist verteidigungsfähig (der Aufrufer soll selbst entscheiden, wie lange er wartet), aber:

- die XML-Dokumentation von `AppendAsync` erwähnt es nicht,
- `DisposeAsync` bricht die Waiter ab, ein hängender Aufrufer bekommt also eine
  `TaskCanceledException` statt einer aussagekräftigen Meldung,
- die Waiter bleiben bis dahin in `_commitWaiters` stehen, einer pro hängendem Aufruf.

**Empfehlung:** Entweder eine optionale `AppendTimeout` in `RaftGroupOptions`, oder wenigstens ein
deutlicher Hinweis in der Dokumentation, dass bei `Quorum` ohne Token unbegrenzt gewartet wird.

---

<a id="bug-020"></a>
## BUG-020 — `SegmentedRaftWal.DisposeAsync` entsorgt das Write-Gate unter Wartenden

**Schwere:** Niedrig · **Ort:** `SegmentedRaftWal.DisposeAsync`

```csharp
public async ValueTask DisposeAsync()
{
    if (_disposed) { return; }

    await _writeGate.WaitAsync().ConfigureAwait(false);
    try { _disposed = true; … }
    finally
    {
        _writeGate.Release();
        _writeGate.Dispose();
    }
}
```

Jede Append-Methode prüft `ObjectDisposedException.ThrowIf(_disposed, this)` **vor** dem
`await _writeGate.WaitAsync(...)`. Ein Aufrufer, der die Prüfung passiert, während `DisposeAsync` noch
nicht `_disposed` gesetzt hat, wartet anschließend auf ein Semaphor, das gleich entsorgt wird — und
bekommt eine `ObjectDisposedException` aus `WaitAsync`.

Das ist bei geordnetem Herunterfahren unwahrscheinlich (`RaftGroupInstance` entsorgt den Log erst nach
`_shutdown.Cancel()`), aber die Prüfung liegt strukturell auf der falschen Seite der Sperre.

Dieselbe Form in `RaftGroupInstance.DisposeAsync` (`_stateGate.Dispose()` bei `RaftGroupInstance.cs:1015`,
ohne die laufenden Anfragen abzuwarten) und in
[BUG-003](#bug-003).

**Behebung:** `_disposed` unter dem Gate prüfen, oder das Gate gar nicht entsorgen —
`SemaphoreSlim.Dispose` ist nur nötig, wenn `AvailableWaitHandle` benutzt wurde, was hier nicht der Fall
ist. Letzteres ist die einfachste sichere Variante und in .NET-Bibliotheken üblich.

---

<a id="bug-021"></a>
## BUG-021 — Eine beschädigte Meta-Datei setzt den Wahlzustand still auf Term 0 zurück

**Schwere:** Hoch · **Ort:** `RaftMetaStore.cs` — `FileRaftMetaStore.ReadAsync`

```csharp
int read = await RandomAccess.ReadAsync(handle, buffer, 0, cancellationToken).ConfigureAwait(false);
if (read < RecordSize || BinaryPrimitives.ReadUInt64LittleEndian(buffer) != Magic)
{
    return RaftMeta.Initial;      // = new(0, null)
}
```

Eine zu kurze oder mit falschem Magic behaftete Datei wird wie eine **fehlende** behandelt. Der
Rückgabewert `RaftMeta.Initial` bedeutet aber „Term 0, noch nie gewählt" — der Knoten vergisst also
stillschweigend seinen Term und seine abgegebene Stimme.

Die Schnittstelle benennt die Gefahr in ihrer eigenen Dokumentation:

> it must be fsynced *before* a vote is granted — losing it is how a node ends up voting twice in one term
> and electing two leaders.

Genau das passiert hier: Nach einem Neustart mit beschädigter Meta-Datei ist `_votedFor` leer, und der
Knoten kann in einem Term, in dem er bereits gewählt hat, ein zweites Mal wählen. Zwei Leader in einem
Term ist die eine Eigenschaft, die Raft nicht verlieren darf.

Der Unterschied zwischen „diese Datei gibt es nicht" (legitim: ein neuer Knoten) und „diese Datei ist
kaputt" (nicht legitim) wird nicht gemacht. Über den 24-Byte-Datensatz gibt es zudem keine Prüfsumme; das
Magic deckt nur Byte 0–7 ab, `Term` und `VotedFor` liegen ungeschützt dahinter.

Ein umgekipptes Bit im Term-Feld ist noch subtiler: Magic und Länge stimmen, der Datensatz wird
akzeptiert, und der Knoten startet mit einem willkürlichen Term — nach oben verfälscht stört er die ganze
Gruppe, nach unten verfälscht wählt er doppelt.

**Behebung:**

1. Einen beschädigten Datensatz als Fehler behandeln, nicht als fehlenden:
   ```csharp
   if (read < RecordSize || BinaryPrimitives.ReadUInt64LittleEndian(buffer) != Magic)
   {
       throw new InvalidOperationException(
           "The Raft metadata for group " + group + " is unreadable. Refusing to start rather than "
           + "silently resetting term and vote, which would allow a second vote in a term already voted in.");
   }
   ```
   Nur der Fall „Datei existiert nicht" darf `RaftMeta.Initial` liefern — und den prüft die Methode
   bereits eine Zeile darüber.
2. Eine Prüfsumme über den Datensatz aufnehmen. `IWalChecksumStrategy` liegt im Nachbarprojekt und tut
   genau das für die Log-Datensätze.
3. Denselben Blick auf `ClusterModeStore.ReadAsync` werfen, siehe
   [SEC-011](SEC-011-cluster-mode-marker-ohne-integritaetspruefung.md) — dort ist es dieselbe Lücke mit
   derselben Ursache.

---

## Was ich geprüft und *nicht* beanstandet habe

Der Vollständigkeit halber, damit ein späterer Durchgang diese Stellen nicht erneut aufrollt:

- **`_stateGate`/`_applyGate`-Verschachtelung.** Es gibt keinen Pfad, der `_applyGate` hält und dann
  `_stateGate` nimmt. `ApplyCommittedAsync` wird ausschließlich nach der Freigabe von `_stateGate`
  angestoßen. Kein Deadlock.
- **Der `Task.Run`-Boundary in `PushToAllPeers`.** Vorhanden und korrekt, entsprechend der Warnung in
  `CLAUDE.md`.
- **Korrelations-IDs in `RaftStreamSession`.** Beide Seiten zählen unabhängig ab 1, Kollisionen sind aber
  unschädlich, weil Anfrage- und Antwort-Frames disjunkte `PayloadCase`-Werte haben und
  `_inboundSnapshots` nur eingehende Anfragen indiziert.
- **Der Puffertausch in `AbsorbEntriesAsync`.** Der Batch wird vor jedem Tausch geleert, die bereits
  eingereihten Einträge zeigen also nie auf einen zurückgegebenen Puffer. (Der Tausch selbst hat trotzdem
  das Problem aus [BUG-001](#bug-001).)
- **`RaftMembership` als Ganzes.** Immutable, atomar per `Volatile.Write` ausgetauscht; `Apply` ist
  idempotent und behandelt unbekannte `MembershipChangeKind`-Werte mit `_ => this`, also sicher.
- **`InMemoryRaftCluster.Detach` / `DetachChunks`.** Halten den Zero-Copy-Kontrakt korrekt ein.
- **`SingleNodeRaftTransport`.** Wirft `InvalidOperationException` statt `IOException`, wie in
  `CLAUDE.md` gefordert.
- **Die `Volatile`-Zugriffe auf `_currentTerm`, `_role`, `_commitIndex`, `_lastApplied`.** Konsistent
  verwendet; die Schreibvorgänge auf `_currentTerm`/`_votedFor` stehen alle unter `_stateGate`.
