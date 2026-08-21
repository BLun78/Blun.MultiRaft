# LZ4-Kompressionslevel-Matrix-Benchmark gegen SegmentedRaftWal (mit Flush)

## Prompt

Baue einen BenchmarkDotNet-Benchmark in `benchmark/Blun.MultiRaft.Benchmarks/` (neue Datei, z.B.
`CompressionLevelBenchmarks.cs`), der misst, ob und ab welcher Payload-Größe welches LZ4-Level die beste
Kombination aus Kompressionszeit + WAL-Append + Flush-to-Disk liefert.

**Wichtiger Hinweis zur Matrixgröße:** `[Params]` bildet ein kartesisches Produkt aller Achsen. 100-Byte-
Schritte von 100 B bis 1 MiB (~10.480 Werte) × 12 Level (`None` + 11 LZ4-Level) × 2 Inhaltstypen wären
~250.000 Benchmark-Cases — mit BenchmarkDotNet (das pro Case einen eigenen Prozess/mehrere Iterationen mit
Warmup fährt) praktisch nicht durchführbar. Deshalb:

- Payload-Größen als **`[Params]`-Stützstellen statt durchgehender 100-Byte-Schritte**:
  `100, 500, 1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 128_000, 256_000, 512_000, 1_048_576`
  (13 Werte, log-artig verteilt — deckt die Größenordnung ab, ohne die Kurve durch reine Redundanz zwischen
  benachbarten 100-B-Schritten aufzublähen).
- Wenn eine feinere Auflösung in einem bestimmten Bereich später nötig ist (z.B. um exakt den Umschlagpunkt
  zu finden), kann dieser Bereich gezielt nachverdichtet werden — nicht die ganze Spanne.

**Benchmark-Parameter (`[Params]`):**

- `PayloadBytes`: die 13 Stützstellen oben.
- `Compression`: `None, L00_FAST, L03_HC, L04_HC, L05_HC, L06_HC, L07_HC, L08_HC, L09_HC, L10_OPT, L11_OPT,
  L12_MAX` (K4os `LZ4Level`, plus `None` als unkomprimierter Vergleichswert).
- `ContentType`: `Random` (inkompressibel, Worst Case) und `Repetitive` (JSON/Text-ähnlich mit
  Wiederholungsmustern, Best-Case-Näherung für Queue-Commands).

**Was der Benchmark-Body macht (End-to-End, weil "Performance geht über alles beim WAL"):**

1. `LZ4Pickler.Pickle(payload, level)` (übersprungen bei `None`).
2. `IRaftWal.AppendAsync(header, compressedPayload)`.
3. `IRaftWal.FlushAsync()` — **`FlushToDisk = true`** im `SegmentedRaftWalOptions`-Setup, im Unterschied zum
   bestehenden `WalAppendBenchmarks`, der Flush bewusst ausschaltet (dort ist der Kommentar explizit: fsync
   kostet 1-2 Größenordnungen mehr als der Rest und würde den eigentlichen Vergleich begraben — hier ist
   Flush aber genau das, was gemessen werden soll).

**Setup (`[GlobalSetup]`):**

- `SegmentedRaftWalFactory` mit `WalSegmentAccess.RandomAccess`, großem `SegmentSizeBytes` (kein Rollover
  während der Messung), temp-Verzeichnis pro Lauf, `[GlobalCleanup]` räumt es weg — analog zum bestehenden
  `WalAppendBenchmarks`.
- `[MemoryDiagnoser]` beibehalten, damit Allokationen pro Kompressionslevel sichtbar werden — relevant, weil
  `Pickle` bei jedem Aufruf ein neues Array erzeugt.
- Report zusätzlich als CSV exportieren, damit die Auswertung "Größe → bestes Level" danach skriptbar ist.

**Nicht im Scope:** noch keine Änderung an `RaftEntryHeader`, `RaftPayloadCompression`-Enum in der Library
oder `RaftGroupOptions` — dieser Benchmark liefert nur die Datenbasis für die spätere Größen-zu-Level-
Heuristik.

**Nach dem Lauf:** aus dem BenchmarkDotNet-Report für jede Größenklasse das Level mit bester Gesamtzeit
(Compress + Append + Flush) bei akzeptabler Kompressionsrate ablesen und daraus eine Tabelle
"Payload-Größenbereich → empfohlenes Level" ableiten.

## Ergänzung: LZ4Stream vs. LZ4Frame vs. LZ4Pickler

Zusätzliche Achse `CompressionApi` (`Pickler, Stream, Frame`) über dieselbe Größen-/Level-/Inhaltsmatrix, um
zu klären, ab welcher Payload-Größe (falls überhaupt) `LZ4Stream` oder `LZ4Frame` gegenüber dem bisher
verwendeten `LZ4Pickler` (One-Shot-Block-Format) einen Unterschied machen — und welche der drei API-Varianten
gewinnt.

- **`Pickler`** (`LZ4Pickler.Pickle`) — bisherige Baseline, ein Aufruf, eine Allokation, bespoke Block-Format.
- **`Stream`** (`LZ4Stream.Encode(Stream, level, extraMemory, leaveOpen)`) — Chunked-Encoder über einen
  `MemoryStream`; zahlt die `Stream`-Abstraktion (virtuelle Aufrufe, internes Buffering) mit. Die API, die
  gebraucht würde, wenn ein Payload nicht mehr in einen zusammenhängenden Span passt.
- **`Frame`** (`LZ4Frame.Encode(Span<byte>, Span<byte>, level, extraMemory)`) — Span-zu-Span, kein `Stream`
  im Pfad, gleiches LZ4-Frame-Format (selbstbeschreibend, mit Checksums) wie `Stream`. Direktester Fit für
  einen WAL-Eintrag, der schon als ein zusammenhängender Span vorliegt.

`None` läuft der Einfachheit halber unter allen drei API-Werten identisch mit (kein Kompressionsaufruf) —
das ist redundant, aber günstig, und dient nebenbei als Sanity-Check: alle drei sollten innerhalb der
Messtoleranz gleich schnell sein.

**Scope-Hinweis:** Gemessen wird nur der Schreibpfad (Compress → Append → Flush), kein Round-Trip/Decode —
konsistent mit dem ursprünglichen Benchmark-Ziel ("Performance geht über alles beim WAL", also der Pfad, den
ein Aufrufer tatsächlich abwartet).

## Ergebnis

_(wird nach dem Benchmark-Lauf ergänzt)_
