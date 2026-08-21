# RaftPayloadCompression: LZ4-Kompression für WAL-Payloads

Implementierungs-Prompt, überarbeitet nach den Messergebnissen aus `doc/compression-level-benchmark.md`.
Ursprünglicher Entwurf sah ein Enum mit mehreren LZ4-Leveln und eine statische Per-Gruppe-Konfiguration vor;
die Benchmark-Daten haben beides revidiert (siehe "Warum diese Entscheidungen" unten).

## Ziel

Payload-Kompression für Raft-Command-Einträge, sodass große Payloads komprimiert im WAL liegen und
komprimiert über gRPC repliziert werden — ohne dass WAL oder gRPC-Transport davon wissen (beide behandeln
`RaftLogEntry.Payload` bereits als opake Bytes, siehe `RaftEntryHeader`/`IRaftWal` in
`src/Blun.MultiRaft.Wal`). Kompression und Dekompression passieren ausschließlich im Core-Projekt
(`src/Blun.MultiRaft`), an der Grenze zwischen Anwendung und Log.

## Warum diese Entscheidungen (aus den Messdaten)

- **Nur `None` und ein einziges "schnelles" Level, keine Levelauswahl.** `L06_HC` und `L12_MAX` waren im
  Append+Flush-Pfad in **jedem** gemessenen Fall gleich oder schlechter als `None` — bei 1 MiB/Random kostet
  `L06_HC` das Fünffache von `None` (21,6 ms vs. 4,16 ms), `L12_MAX` bei 1 MiB/Repetitive sogar das Achtfache
  (35,2 ms vs. 4,13 ms). Ein Level-Enum mit mehreren Stufen würde eine Wahlmöglichkeit suggerieren, die die
  Daten für den Append-Pfad nicht stützen. Nur `L00_FAST` (K4os `LZ4Level.L00_FAST`) bleibt über den
  gesamten gemessenen Bereich nah an oder unter `None`.
- **Adaptiv statt statischer Schwelle: pro Append komprimieren versuchen, nur behalten wenn kleiner.**
  Kompressibilität hängt stark vom *Inhalt* ab, nicht nur von der Größe — `Random` und `Repetitive` divergieren
  bei größeren Payloads um den Faktor 6+ (siehe `doc/compression-level-benchmark.md`, Kernbefund 1). Eine
  reine Größenschwelle ("> 256 KB komprimieren") hätte bei inkompressiblen großen Payloads (z.B. bereits
  komprimierte oder verschlüsselte Message-Bodies) unnötig Zeit gekostet. `L00_FAST`s eigener Overhead ist
  dabei klein genug (schlechtester gemessener Fall: ~13% bei 1 MiB/Random), dass der Versuch selbst bei
  jedem Append vertretbar ist.
- **Roher LZ4-Block-Codec mit eigenem 4-Byte-Längenpräfix — nicht das Frame-Format.** Ursprünglich war
  `LZ4Frame` vorgesehen (`Stream` allokiert bei jeder Größe ~1,5× so viel wie `Frame`, weil
  `MemoryStream.ToArray()` intern noch einmal kopiert; zeitlich Unentschieden). **Bei der Umsetzung stellte
  sich heraus, dass das nicht trägt:** `LZ4EncoderSettings.ContentLength` — genau das Feld, das den Decoder
  seinen Zielpuffer exakt dimensionieren lassen würde — ist für Span-Ziele in K4os schlicht nicht
  implementiert und wirft `NotImplementedException` aus `ByteSpanLZ4FrameWriter`. Damit verliert das
  Frame-Format seinen einzigen hier nutzbaren Vorteil. Der übrige Nutzen (Selbstbeschreibung, Magic Number,
  Checksums) ist in diesem Kontext wertlos: `RaftEntryHeader.Compression` sagt bereits, ob ein Eintrag
  komprimiert ist, das WAL hat seine eigene Prüfsumme, und beide Enden sind dieselbe Bibliotheksversion.
  `LZ4Codec.Encode`/`Decode` mit vorangestellter Originallänge kostet 4 Byte statt Frame-Header plus
  End-Mark, dekomprimiert in einem einzigen Aufruf in einen exakt dimensionierten Puffer und behält das
  Allokationsprofil (ein gepoolter Puffer). Damit wird auch nur das Basispaket `K4os.Compression.LZ4`
  gebraucht, nicht `.Streams`.
- **Feature bleibt drin, trotz durchschnittlich ~1 KB Payload-Größe.** Im für euch typischen Bereich
  (2 KB–16 KB) zeigte der Benchmark kein klares Signal (Unterschiede im Bereich des Messrauschens) — das
  Feature schadet dort also nicht, hilft aber auch nicht spürbar. Der klare Nutzen (bis zu ~50% kleinere
  Zeit bei 1 MiB/Repetitive) zeigt sich erst nahe der 1024-KB-Nachrichtengrenze. Der adaptive Ansatz
  (Punkt 2) stellt sicher, dass der Normalfall (~1 KB) nicht durch das Feature verschlechtert wird.

## Scope

### 1. `RaftPayloadCompression`-Enum (`src/Blun.MultiRaft.Wal`, Teil des Wire-Formats)

```csharp
public enum RaftPayloadCompression : byte
{
    None = 0,
    Lz4Fast = 1,   // K4os LZ4Level.L00_FAST
}
```

Bewusst nur zwei Werte. Falls später ein Snapshot-Kompressions-Pfad andere Level braucht (dort zählt
einmalige Größe, nicht Latenz pro Append — andere Kostenfunktion, siehe Benchmark-Kernbefund 2 zu
`L12_MAX`s Optimal-Parsing-Kosten), sollte das ein eigenes Enum sein, nicht dieses hier erweitert.

### 2. `RaftEntryHeader` (`RaftLogEntry.cs`)

Eines der beiden `_reserved`-Bytes wird zu `public readonly RaftPayloadCompression Compression`. Header
bleibt 32 Byte, kein Breaking Change am Layout. `PayloadLength` bleibt die Länge der tatsächlich
gespeicherten (ggf. komprimierten) Bytes — unverändert gegenüber dem ursprünglichen Entwurf.

### 3. NuGet-Abhängigkeiten

`K4os.Compression.LZ4` (Basispaket, enthält `LZ4Codec` und `PinnedMemory`) zu `src/Blun.MultiRaft` (Core)
hinzufügen — nicht zu `Blun.MultiRaft.Wal`, das bleibt storage-only und referenziert keine
Kompressionslogik. `.Streams` wird **nicht** gebraucht (siehe Block-Codec-Begründung oben); das
Benchmark-Projekt referenziert es weiterhin, weil es die Frame-APIs mitmisst. Zentral in
`Directory.Packages.props` auf `1.3.8` gepinnt.

### 4. `RaftGroupOptions`: `PayloadCompression`-Schalter (Enum, nicht Bool)

```csharp
/// <summary>
/// Whether AppendAsync tries LZ4 compression on each command payload, keeping the compressed form only
/// when it is smaller than the original. <see cref="RaftPayloadCompression.None"/> (the default) turns the
/// attempt off entirely. See doc/raft-payload-compression.md for why the actual per-entry choice is
/// adaptive rather than a size threshold, and why there is only one level to switch on.
/// </summary>
public RaftPayloadCompression PayloadCompression { get; init; } = RaftPayloadCompression.None;
```

Dieselbe Enum wie im Header (Schritt 1) — kein zweiter Typ, kein Bool. `RaftGroupOptions.PayloadCompression`
ist der An/Aus-Schalter pro Gruppe (`None` = Feature aus, `Lz4Fast` = Feature an); `RaftEntryHeader.Compression`
trägt den tatsächlichen Wire-Zustand *jedes einzelnen Eintrags*, nachdem die adaptive Entscheidung
(Schritt 5) gefallen ist — bei ausgeschaltetem Feature ist das immer `None`, bei eingeschaltetem Feature
`Lz4Fast` nur für die Einträge, bei denen sich Kompression tatsächlich gelohnt hat, sonst ebenfalls `None`.
Beide Stellen nutzen dieselbe Enum, weil es dieselbe Bedeutung ist ("welche Kompression liegt hier vor"),
nur an zwei verschiedenen Punkten im Datenfluss (Konfigurationsabsicht vs. tatsächliches Ergebnis).

### 5. Kompression: adaptiv in `RaftGroupInstance.AppendAsync`

Implementiert in `RaftPayloadCodec.Compress` (`src/Blun.MultiRaft/Core/RaftPayloadCodec.cs`), aufgerufen aus
`AppendCoreAsync` vor dem WAL-Append:

1. Übersprungen, wenn `PayloadCompression == None`, wenn der Eintrag kein `RaftEntryKind.Command` ist, oder
   wenn das Payload leer ist. **`Membership` bleibt ausdrücklich unkomprimiert** — nicht nur "klein und
   selten", sondern zwingend: der Replay, der beim Start die Konfiguration rekonstruiert, liest diese
   Einträge direkt aus dem Log, bevor überhaupt eine State Machine existiert, die dekomprimieren könnte.
2. `LZ4Codec.Encode(payload.Span, buffer.AsSpan(4), LZ4Level.L00_FAST)` in einen aus `ArrayPool<byte>`
   geliehenen Puffer der Größe `4 + LZ4Codec.MaximumOutputSize(payload.Length)`; die Originallänge kommt als
   Little-Endian-`int` in die ersten 4 Byte.
3. Nur behalten, wenn `4 + written < payload.Length` — das Präfix zählt mit. Dann
   `Header.Compression = Lz4Fast`; sonst `Header.Compression = None` und das unveränderte Original ins WAL.
4. Der geliehene Puffer wird als `out byte[]? rented` an den Aufrufer zurückgegeben, der ihn nach dem Append
   in einem `finally` zurückgibt — er muss über den `Compress`-Aufruf hinaus gültig bleiben.

### 6. Dekompression: einmalig in `IRaftStateMachine.ApplyAsync`-Auslieferung

`RaftPayloadCodec.Expand`, aufgerufen in der Apply-Schleife: bei `Compression == Lz4Fast` die Originallänge
aus dem 4-Byte-Präfix lesen, ein Array dieser Größe anlegen, `LZ4Codec.Decode` in einem Aufruf hineinschreiben
und die dekodierte Länge gegen die deklarierte prüfen (`InvalidOperationException` bei Abweichung — ein
Integritätsproblem, kein Transportfehler, siehe die Exception-Konvention in CLAUDE.md). Das Ergebnis ist ein
frisches Array, kein Slice des recycelten WAL-Puffers, und erfüllt damit die Buffer-Lifetime-Regel
automatisch.

### 7. Pinned-Memory-Sizing für K4os

`PinnedMemory.MaxPooledSize = Mem.M1 + 128` (1 MiB + 128 Byte) statt K4os-Default oder komplett
deaktiviertem Pooling — deckt das 1024-KB-Message-Cap der Bibliothek plus Marge ab, ohne für den Normalfall
(~1 KB) über-zu-dimensionieren. Begründung: `Blun.MultiRaft` ist ein Dauerbetrieb-Prozess, nicht das
kurzlebige "in-and-out"-Szenario, für das K4os' Pinning-Pooling-Default gedacht ist — Pinned Memory blockiert
GC-Kompaktierung, was sich über Tage/Wochen mit tausenden Gruppen zu Fragmentierung summieren kann.

Gesetzt im **statischen Konstruktor von `RaftPayloadCodec`**, nicht in `MultiRaftHost`: so greift es auch,
wenn eine `RaftGroupInstance` direkt ohne Host benutzt wird, und erst dann, wenn tatsächlich etwas
komprimiert wird. Die Einstellung ist prozessglobal — das ist eine bewusste Nebenwirkung, im Code als solche
kommentiert.

### 8. gRPC: `Compression` muss über die Leitung — nicht im ursprünglichen Entwurf, aber zwingend

Der Entwurf ging davon aus, der gRPC-Transport bleibe unverändert, weil er Payloads "nur durchreicht". Das
stimmt für das Payload, **nicht für den Header**: `RaftFrameCodec.ToProto`/`ToDomain` baut die Header-Felder
einzeln auf, und was dort nicht steht, existiert für den Empfänger nicht. Ohne `compression` im Proto
speichert ein Follower die komprimierten Bytes mit `Compression = None` und reicht beim Apply einen
LZ4-Block an seine State Machine, als wäre er das Kommando.

Das ist genau die Fehlerklasse aus `doc/open-issue-seed-visibility.md` und den Remarks in `RaftFrameCodec`:
Term, Index und Prüfsumme stimmen alle, die Konsistenzprüfung besteht, **nur der Inhalt ist falsch** — und
für keinen Test über `InMemoryRaftTransport` sichtbar, weil der Header dort per Wert kopiert wird und das
Flag ohnehin trägt. Deshalb:

- `uint32 compression = 7;` in `message LogEntry` (`Protos/raft.proto`).
- `ToProto` schreibt es, `ToDomain` liest es — und lehnt unbekannte Werte mit
  `InvalidOperationException` ab, statt sie zu truncaten (dieselbe Begründung wie beim bestehenden
  `Kind`-Check: ein Schema, das dieser Knoten nicht dekodieren kann, ist ein Protokoll-Mismatch).
- Abgedeckt durch `GrpcTransportTests.CompressionFlagSurvivesTheWireRoundTrip`, dem Gegenstück zum schon
  vorhandenen `ApplicationTagSurvivesTheWireRoundTrip`.

## Nicht im Scope dieser Änderung

- Snapshot-Pfad (`CaptureAsync`/`RestoreAsync`) — andere Kostenfunktion (einmalige Größe statt
  Append-Latenz), eigene Entscheidung wert.
- `RaftEntryKind.Membership`-Einträge — bleiben unkomprimiert (siehe Schritt 5.1: der Konfigurations-Replay
  beim Start liest sie ohne State Machine).

## Tests

Alle in `test/Blun.MultiRaft.Tests/PayloadCompressionTests.cs`, bis auf den Wire-Test:

- `ACompressedPayloadReachesTheStateMachineByteForByte` (64 B / 4 KB / 256 KB) — prüft **jede Replik**, nicht
  nur den Leader: der Follower speichert, was er bekommt, und dekomprimiert selbst.
- `AnIncompressiblePayloadIsStoredUncompressedAndStillArrivesIntact` — Zufallsbytes landen mit
  `Compression = None` und Originallänge im Log, nicht als größerer Blob.
- `ACompressiblePayloadIsActuallySmallerOnDisk` — die gespeicherte Länge ist tatsächlich kleiner.
- `CompressionIsOffByDefaultAndLeavesTheEntryUntouched` — Default-Verhalten unverändert.
- `MembershipEntriesAreNeverCompressed` — auch bei eingeschalteter Kompression.
- `GrpcTransportTests.CompressionFlagSurvivesTheWireRoundTrip` — siehe Schritt 8.

## Referenzen

- Messdaten und Rohbegründung: `doc/compression-level-benchmark.md`.
- Benchmark-Code der verglichenen Kompressions-APIs:
  `benchmark/Blun.MultiRaft.Benchmarks/CompressionLevelBenchmarks.cs`.
