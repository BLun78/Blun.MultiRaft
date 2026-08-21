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

**Status: Lauf abgebrochen, unvollständig.** Die volle Matrix (13 Größen × 12 Level × 2 Inhaltstypen × 3
APIs = 936 Fälle × 2 Runtimes ≈ 1.873 BenchmarkDotNet-Cases) hätte laut BenchmarkDotNet-eigener Schätzung
~21 Stunden gebraucht (jeder Fall fährt Warmup/Pilot/Workload mit echtem `FlushAsync`). Der Lauf wurde nach
439 abgeschlossenen Fällen (~23%, Größen 100 B–2.000 B) manuell gestoppt. Rohdaten der abgeschlossenen Fälle:
`compression-bench-partial.csv` (im Repo-Root, nicht committet — Artefakt eines lokalen Laufs).

**Was die vorliegenden Teildaten zeigen (100 B–2.000 B):**

| Größe | `None` (Baseline) | Beste Kombo | Schlechteste Kombo |
|---|---|---|---|
| 100 B | keine `None`-Messung in diesem Teillauf gelaufen | `L08_HC` + Stream, 1,956 ms | `L05_HC` + Stream, 2,018 ms |
| 500 B | 1,959 ms | `L00_FAST` + Frame, 1,947 ms | `L12_MAX` + Pickler, 1,991 ms |
| 1.000 B | 1,959 ms | `L00_FAST` + Stream, 1,942 ms | `L07_HC` + Frame, 2,107 ms |
| 2.000 B | 1,984 ms | `L00_FAST` + Stream, 1,982 ms | `L05_HC` + Pickler, 2,050 ms |

API-Durchschnitt über alle komprimierten Fälle in diesem Bereich (n≈134-135 je API): Frame 1,993 ms,
Pickler 1,992 ms, Stream 1,990 ms — praktisch gleichauf. Inhaltstyp: Random 1,996 ms vs. Repetitive
1,987 ms — ebenfalls kein nennenswerter Unterschied.

**Einordnung — kein Ergebnis, sondern ein Zwischenstand ohne Aussagekraft für die eigentliche Fragestellung:**
Bei 100 B–2.000 B liegt praktisch jede Kombination innerhalb von ±3% der `None`-Baseline (~1,94–2,11 ms).
Das ist erwartbar: `FlushAsync` (echter fsync) dominiert die Gesamtzeit vollständig, Kompressionszeit für
so kleine Payloads liegt im Mikrosekundenbereich und geht im Flush-Rauschen unter. **Die eigentlich
interessante Frage — ab welcher Payload-Größe sich Kompression lohnt und ob Stream/Frame gegen Pickler
gewinnen — bleibt unbeantwortet**, weil die dafür relevanten großen Größenklassen (16 KB–1 MiB, wo
Kompressionszeit und -rate signifikant gegenüber Flush werden) in diesem Teillauf nicht erreicht wurden.

**Nächster Schritt, falls die Frage weiterverfolgt wird:** entweder den vollen Lauf über Nacht/mehrere
Stunden laufen lassen, oder die Matrix gezielt verkleinern (z.B. nur die oberen Größenstufen ab 16 KB, wo
das Signal erwartbar ist, statt der kompletten 13-Punkte-Sweep von vorne).

## Verkleinerte Matrix: `CompressionLevelLargePayloadBenchmarks`

Eigene, separate Benchmark-Klasse (die volle 936-Fälle-Matrix in `CompressionLevelBenchmarks` bleibt
unverändert für einen späteren vollständigen Lauf erhalten). Ziel: ~200 Fälle, die in Minuten statt Stunden
laufen, und die Größen unter 2.000 B auslassen — die haben im abgebrochenen vollen Lauf bereits gezeigt,
dass dort Flush dominiert und kein Signal zu erwarten ist.

**Reduzierte Achsen:**
- **Größen** (ab 2.000 B): `2_000, 8_000, 16_000, 32_000, 64_000, 128_000, 256_000, 1_048_576` — 8
  log-verteilte Stützstellen von 2 KB bis 1 MiB.
- **Level**: `None, L00_FAST, L06_HC, L12_MAX` — Baseline, schnellster, ein repräsentativer Mittel-HC, und
  maximale Ratio. Die feinen HC-Zwischenstufen bewegen sich laut K4os-Doku nahezu linear mit dem Regler,
  die Auflösung wird für diesen Kurzlauf nicht gebraucht.
- **Inhaltstyp**: `Random, Repetitive` — unverändert, größter Einflussfaktor auf die Kompressionsrate.
- **API**: `Pickler, Stream, Frame` — unverändert, das ist der eigentliche Kern dieser Runde.

8 × 4 × 2 × 3 = **192 Fälle**.

**Abweichung von der Projektregel** ("Benchmarks müssen immer net10.0 und net11.0 gemeinsam laufen", siehe
CLAUDE.md): dieser Lauf läuft bewusst **nur unter `net11.0`** — ein expliziter, einmaliger Kurzlauf für eine
schnelle Exploration, kein Zahlenwert, der als Vergleichsergebnis in einen Report ginge.

Aufruf:

```bash
dotnet run --project benchmark/Blun.MultiRaft.Benchmarks/Blun.MultiRaft.Benchmarks.csproj -c Release -f net11.0 -- --runtimes net11.0 --filter "*CompressionLevelLargePayloadBenchmarks*"
```

### Ergebnis (verkleinerte Matrix)

_(wird nach dem Lauf ergänzt)_
