# SEC-008 — Der Batch-Append umgeht `MaxPayloadBytes`; Recovery bestraft das mit Datenverlust

- **Schweregrad:** Hoch
- **Kategorie:** Fehlende Eingabevalidierung (CWE-20) → Datenverlust, inkonsistente Grenzwertprüfung
- **Ort:** `src/Blun.MultiRaft.Wal/SegmentedRaftWal.cs` — `AppendAsync(ReadOnlyMemory<RaftLogEntry>, …)`, `RollIfNeeded`; `src/Blun.MultiRaft.Wal/WalSegment.cs` — `RecoverAsync`

## Befund

Es gibt zwei Append-Überladungen. Die **Einzel**-Variante prüft die Payload-Größe:

```csharp
public async ValueTask<long> AppendAsync(RaftEntryHeader header, ReadOnlyMemory<byte> payload, …)
{
    ObjectDisposedException.ThrowIf(_disposed, this);
    if (payload.Length != header.PayloadLength) { throw new ArgumentException(…); }
    if (payload.Length > _options.MaxPayloadBytes)
    {
        throw new ArgumentOutOfRangeException(nameof(payload), payload.Length, "Payload exceeds MaxPayloadBytes.");
    }
    …
```

Die **Batch**-Variante prüft sie nicht — weder `MaxPayloadBytes` noch die Header/Payload-Konsistenz:

```csharp
public async ValueTask<long> AppendAsync(ReadOnlyMemory<RaftLogEntry> entries, …)
{
    ObjectDisposedException.ThrowIf(_disposed, this);
    if (entries.IsEmpty) { return LastIndex; }

    await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
    try
    {
        SaveConfig();
        …
        int size = RaftWalRecord.SizeOf(remaining.Span[count].Payload.Length, _checksum.ChecksumSize);
```

Das ist ausgerechnet der Pfad, über den **fremde** Daten in den Log gelangen: Einträge, die per
`AppendEntries` von einem Peer kommen. Der Einzel-Pfad, der geprüft wird, ist der lokale.

Die Recovery hingegen kennt die Grenze und behandelt ihre Verletzung als Beschädigung
(`WalSegment.RecoverAsync`):

```csharp
// A corrupt length field must never be trusted into an allocation, so it is bounded by the
// same limit the write path enforces before it is used for anything.
if (header.PayloadLength < 0
    || header.PayloadLength > options.MaxPayloadBytes
    || header.Index != expected
    || header.Term <= 0)
{
    torn = true;
    break;
}
```

Der Kommentar sagt „the same limit the write path enforces" — genau das stimmt für den Batch-Pfad nicht.

## Auswirkung

**1. Stiller, dauerhafter Datenverlust beim nächsten Neustart.**
Ein Eintrag mit `PayloadLength > MaxPayloadBytes` wird zur Laufzeit anstandslos geschrieben. Beim nächsten
Öffnen des Logs stuft die Recovery ihn als Riss ein, setzt `torn = true`, **kürzt die Datei auf den letzten
guten Offset** (`RandomAccess.SetLength(trim, good)`) und löscht in `SegmentedRaftWal.RecoverAsync` alle
nachfolgenden Segmente:

```csharp
if (torn)
{
    // Everything after the first torn record is unreachable: the log must stay dense.
    File.Delete(file);
    continue;
}
```

Der Knoten verliert also alles ab diesem Eintrag — auch committete Einträge. Aus Sicht von Raft ist das
ein Replikat, das committete Historie verloren hat.

**2. Überlauf der gemappten Segmentgrenze.**
`SegmentedRaftWalOptions.Validate()` garantiert `SegmentSizeBytes >= SizeOf(MaxPayloadBytes)` — diese
Invariante trägt aber nur, solange die Grenze eingehalten wird. `RollIfNeeded` nimmt einen beliebig
großen Datensatz an, sobald das aktive Segment leer ist:

```csharp
if (_active is { } active && (active.Count == 0 || active.Length + recordSize <= _options.SegmentSizeBytes))
{
    return active;
}
```

Mit `SegmentAccess = MemoryMapped` schlägt der Schreibvorgang dann in
`MemoryMappedSegmentDevice.WriteAsync` fehl:

```csharp
if (offset < 0 || offset + data.Length > _capacity)
{
    throw new ArgumentOutOfRangeException(nameof(offset), offset, "Write runs past the end of the segment.");
}
```

Diese `ArgumentOutOfRangeException` ist weder `IOException` noch `InvalidOperationException` und wird in
`RaftStreamSession.HandleRequestAsync` **nicht** gefangen — sie landet in einem unbeobachteten Task und
verschwindet. Der Leader sieht nur ein Timeout und wiederholt endlos.

**3. Speicherdruck.** `ArrayPool<byte>.Shared.Rent(bytes)` mit unbegrenztem `bytes` allokiert außerhalb
des Pools (Arrays > 1 MB werden vom Shared Pool nicht mehr gecached) — eine Allokation in Größe der
gesamten Batch, gesteuert von der Gegenstelle.

## Empfehlung

1. **Dieselbe Prüfung in die Batch-Überladung ziehen**, vor dem `_writeGate` und für jeden Eintrag:
   ```csharp
   foreach (ref readonly RaftLogEntry entry in entries.Span)
   {
       if (entry.Payload.Length > _options.MaxPayloadBytes)
       {
           throw new ArgumentOutOfRangeException(nameof(entries), entry.Payload.Length, "Payload exceeds MaxPayloadBytes.");
       }

       if (entry.Payload.Length != entry.Header.PayloadLength)
       {
           throw new ArgumentException("Header payload length does not match the payload.", nameof(entries));
       }
   }
   ```
   Wichtig: **vor** dem ersten Schreiben prüfen, nicht mittendrin — ein halb geschriebener Batch ist
   schlimmer als ein abgelehnter.
2. Die Prüfung zusätzlich weiter vorne einziehen, in `RaftFrameCodec.ToDomain(LogEntry)` bzw. beim
   Empfang von `AppendEntries`, damit ein überlanger Eintrag gar nicht erst bis zum Log durchgereicht wird.
   Dort ist die Antwort `AppendEntriesResponse(Success: false)` — eine saubere Ablehnung statt einer
   Exception.
3. Den Kommentar in `WalSegment.RecoverAsync` erst dann stehen lassen, wenn er wieder stimmt.
4. Einen Contract-Test in `WalTests.cs` ergänzen, der beide Überladungen gegen dieselbe Grenze prüft —
   die Datei existiert genau dafür („sharing one contract test suite … this has already caught real
   divergence between them").

## Verwandt

- [SEC-001](SEC-001-raft-transport-ohne-authentifizierung.md) — wer diesen Pfad erreichen kann
- [SEC-007](SEC-007-entry-kind-truncation.md) — dieselbe fehlende Validierung eine Ebene höher
- [BUG-001](PROBLEME-src-speicher-lecks-und-locks.md#bug-001)
