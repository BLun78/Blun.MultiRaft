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
- **`LZ4Frame`, nicht `LZ4Stream` oder `LZ4Pickler`.** Einziges konsistentes Ergebnis über die gesamte
  Matrix: `Stream` allokiert bei jeder Größe **~1,5× so viel wie `Frame`** (Grund: `MemoryStream.ToArray()`
  kopiert intern noch einmal). Zeitlich ein Unentschieden, aber bei potenziell zehntausenden Gruppen mit
  kontinuierlicher Append-Last (siehe CLAUDE.md, "ten-thousand-group scale") ist weniger GC-Druck der
  Tiebreaker. `Pickler` wurde in dieser Runde nicht gegen `Frame` gemessen (siehe Benchmark-Doku), scheidet
  aber ohnehin aus, weil `Frame` das für Append benötigte Span-zu-Span-Verhalten ohne Zwischenkopie bietet.
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

`K4os.Compression.LZ4.Streams` (für die `LZ4Frame`-API) zu `src/Blun.MultiRaft` (Core) hinzufügen — nicht
zu `Blun.MultiRaft.Wal`, das bleibt storage-only und referenziert keine Kompressionslogik. Zentral in
`Directory.Packages.props` versionieren (bereits als `1.3.8` im Benchmark-Projekt gepinnt, dieselbe Version
für Core übernehmen).

### 4. `RaftGroupOptions`: `PayloadCompression`-Flag

```csharp
/// <summary>
/// Whether AppendAsync tries LZ4 compression on each command payload, keeping the compressed form only
/// when it is smaller than the original. Off by default. See doc/raft-payload-compression.md for why this
/// is adaptive per entry rather than a size threshold, and why there is only one level to turn on.
/// </summary>
public bool PayloadCompression { get; init; }
```

Bool statt Enum an dieser Stelle — es gibt nur eine Kompressionsvariante, ein Enum mit zwei Werten (`None`/
`Lz4Fast`) wäre hier reine Indirektion. Das `RaftPayloadCompression`-Enum aus Schritt 1 lebt im Header, wo es
den Wire-Zustand *jedes einzelnen Eintrags* trägt (weil die Entscheidung pro Eintrag adaptiv fällt, nicht
pro Gruppe fix ist) — das ist ein anderer Zweck als diese Gruppen-Option, die nur an/aus schaltet.

### 5. Kompression: adaptiv in `RaftGroupInstance.AppendAsync`

Vor dem WAL-Append, wenn `PayloadCompression == true`:

1. `LZ4Frame.Encode(payload.Span, buffer.Span, LZ4Level.L00_FAST, extraMemory: 0)` in einen Puffer der
   Größe `LZ4Codec.MaximumOutputSize(payload.Length) + 64` (Frame-Header/Footer-Marge, siehe
   `CompressWithFrame` im Benchmark-Code als Referenzimplementierung,
   `benchmark/Blun.MultiRaft.Benchmarks/CompressionLevelBenchmarks.cs`).
2. **Encoder muss `LZ4EncoderSettings.ContentLength = payload.Length` setzen.** Ohne das ist die
   Originallänge beim Decode nicht direkt aus dem Frame-Descriptor lesbar (`ILZ4FrameReader.GetFrameLength()`
   liefert dann `null`), und der Decoder müsste in einer wachsenden Puffer-Schleife lesen statt den
   Zielpuffer exakt vorzudimensionieren. Das war im Benchmark irrelevant (dort wurde nie dekomprimiert),
   ist hier aber ein echter Korrektheits-/Effizienzpunkt.
3. Ist das Ergebnis kürzer als `payload.Length`: `Header.Compression = Lz4Fast`, komprimierte Bytes ins WAL.
   Sonst: `Header.Compression = None`, Original-Payload unverändert ins WAL (wie heute).

### 6. Dekompression: einmalig in `IRaftStateMachine.ApplyAsync`-Auslieferung

Wenn `entry.Header.Compression == Lz4Fast`: Zielpuffer über `reader.GetFrameLength()` (dank gesetztem
`ContentLength`, siehe Schritt 5.2) exakt dimensionieren, `LZ4Frame.Decode(entry.Payload, extraMemory: 0)`
öffnen, per `ReadManyBytes` befüllen. Ergebnis ist ein neuer, eigener Array — respektiert automatisch die
Buffer-Lifetime-Regel aus CLAUDE.md (recycelter Puffer darf nicht über die Auslieferung hinaus gehalten
werden), weil das ohnehin ein frisches Array ist, keine Slice des recycelten Puffers.

### 7. Pinned-Memory-Sizing für K4os

`PinnedMemory.MaxPooledSize = Mem.M1 + 128` (1 MiB + 128 Byte) statt K4os-Default oder komplett
deaktiviertem Pooling setzen — deckt das 1024-KB-Message-Cap der Bibliothek plus Marge für
Frame-Format-Overhead ab, ohne für den Normalfall (~1 KB) über-zu-dimensionieren. Begründung: `Blun.MultiRaft`
ist ein Dauerbetrieb-Prozess (`MultiRaftHost`), nicht das kurzlebige "in-and-out"-Szenario, für das K4os'
eigenes Pinning-Pooling-Default gedacht ist — Pinned Memory blockiert GC-Kompaktierung, was sich über Tage/
Wochen mit tausenden Gruppen zu Fragmentierung summieren kann. Diese Einstellung beim Start setzen
(z.B. in `MultiRaftHost`-Initialisierung oder wo die Library sonst einmalig konfiguriert wird).

## Nicht im Scope dieser Änderung

- Snapshot-Pfad (`CaptureAsync`/`RestoreAsync`) — andere Kostenfunktion (einmalige Größe statt
  Append-Latenz), eigene Entscheidung wert.
- `RaftEntryKind.Membership`-Einträge — bleiben unkomprimiert, sind klein und selten.
- gRPC-Transport — unverändert, da er `RaftLogEntry.Payload` ohnehin nur durchreicht; Kompression läuft
  bereits vor dem Transport, sodass das Frame automatisch komprimiert repliziert wird.

## Tests

- Roundtrip: `Compress → WAL-Append → Read → Decompress` liefert das Original-Payload bit-identisch, für
  beide Inhaltstypen (zufällig, wiederholend) und mehrere Größen (klein, wo `None` gewinnt; groß, wo
  `Lz4Fast` gewinnt).
- Adaptiver Fallback: ein Payload, für den `LZ4Frame.Encode` kein kleineres Ergebnis liefert (z.B.
  vorkomprimierte/zufällige Bytes knapp über der Kompressionsschwelle), landet mit `Compression = None`
  bit-identisch im WAL — nicht mit einem größeren komprimierten Blob.
- `PayloadCompression = false` (Default) verhält sich exakt wie vor dieser Änderung — Regressionstest gegen
  bestehende `WalTests.cs`/`RaftGroupInstance`-Tests.
- Contract-Test-Erweiterung in `WalTests.cs`, falls dort der Header direkt geprüft wird (`Compression`-Feld
  muss für bestehende Tests weiterhin `None` sein).

## Referenzen

- Messdaten und Rohbegründung: `doc/compression-level-benchmark.md`.
- Referenzimplementierung der Kompressions-APIs (Stream/Frame/Pickler) als Benchmark-Code:
  `benchmark/Blun.MultiRaft.Benchmarks/CompressionLevelBenchmarks.cs` (`CompressionHelpers.CompressWithFrame`).
