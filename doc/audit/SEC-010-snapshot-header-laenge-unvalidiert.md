# SEC-010 — Unvalidierte Längenangabe im Snapshot-Header führt direkt in eine Allokation

- **Schweregrad:** Mittel
- **Kategorie:** Unkontrollierte Speicherallokation aus untrusted Länge (CWE-789 / CWE-20)
- **Ort:** `src/Blun.MultiRaft.Wal/RaftSnapshot.cs` — `FileRaftSnapshotStore.ReadMetadataAsync`

## Befund

```csharp
int configLength = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(24));

byte[] configuration = new byte[configLength];
if (configLength > 0)
{
    await stream.ReadExactlyAsync(configuration, cancellationToken).ConfigureAwait(false);
}
```

`configLength` kommt roh aus der Datei und geht ungeprüft in `new byte[configLength]`. Es gibt weder eine
Obergrenze noch eine Prüfung gegen die tatsächliche Dateigröße. Das Magic am Dateianfang deckt nur die
ersten acht Bytes ab; über den Rest des Headers gibt es keine Prüfsumme.

Das ist bemerkenswert, weil der Write-Ahead-Log dieselbe Situation ausdrücklich richtig behandelt und den
Grund sogar auskommentiert (`WalSegment.RecoverAsync`):

> A corrupt length field must never be trusted into an allocation, so it is bounded by the same limit the
> write path enforces before it is used for anything.

Der Snapshot-Store übernimmt diese Regel nicht.

## Auswirkung

- **Negativer Wert** (`configLength < 0`): `OverflowException` bzw. `ArgumentOutOfRangeException` beim
  Anlegen des Arrays.
- **Sehr großer Wert** (bis `int.MaxValue`): sofortige Allokation von bis zu 2 GB, in aller Regel
  `OutOfMemoryException`.
- **Abgeschnittene Datei**: `ReadExactlyAsync` wirft `EndOfStreamException`.

In allen drei Fällen fliegt die Exception aus `ReadMetadataAsync` heraus. Das ist eine Methode, die beim
Start einer Gruppe aufgerufen wird — der Knoten startet also nicht, statt den unbrauchbaren Snapshot wie
einen fehlenden zu behandeln (`return null`), was der Rest der Methode für jeden anderen Defekt tut:

```csharp
if (… < header.Length || BinaryPrimitives.ReadUInt64LittleEndian(header) != RaftSnapshotMetadata.Magic)
{
    return null;      // für ein falsches Magic: sauberes "kein Snapshot"
}
```

Ein einzelnes gekipptes Bit im Längenfeld hat also eine andere, drastischere Wirkung als ein gekipptes Bit
im Magic — obwohl beides derselbe Defekt ist.

Angriffsseitig ist der Pfad indirekt: Der Inhalt stammt aus einem `InstallSnapshot`, dessen
`Configuration` über die Leitung kommt (siehe
[SEC-001](SEC-001-raft-transport-ohne-authentifizierung.md)) und beim Empfänger unverändert in diese
Datei geschrieben wird. Wer den Snapshot-Pfad oder das Datenverzeichnis erreicht, kann den Start des
Knotens verhindern.

## Empfehlung

1. Länge gegen die tatsächliche Dateigröße und gegen eine explizite Obergrenze prüfen, bevor allokiert
   wird:
   ```csharp
   if (configLength < 0 || RaftSnapshotMetadata.HeaderSize + (long)configLength > stream.Length)
   {
       return null;
   }
   ```
2. `ReadExactlyAsync` in denselben `return null`-Pfad überführen (bzw. `EndOfStreamException` fangen),
   damit ein beschädigter Snapshot wie ein fehlender wirkt statt wie ein Startfehler.
3. Eine Prüfsumme über den Header aufnehmen — die WAL-Datensätze haben eine, und `IWalChecksumStrategy`
   liegt bereits im selben Projekt.

## Verwandt

- [SEC-011](SEC-011-cluster-mode-marker-ohne-integritaetspruefung.md) — dieselbe Lücke im Mode-Marker
- [SEC-008](SEC-008-batch-append-umgeht-payload-limit.md)
