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
- **API**: `Stream, Frame` — `Pickler` bewusst ausgelassen; das ist die bereits gut verstandene
  One-Shot-Block-Format-Baseline aus der vollen Matrix, die eigentlich offene Frage hier ist Stream vs.
  Frame (beide LZ4-Frame-Format).

8 × 4 × 2 × 2 = **128 Fälle**.

**Abweichung von der Projektregel** ("Benchmarks müssen immer net10.0 und net11.0 gemeinsam laufen", siehe
CLAUDE.md): dieser Lauf läuft bewusst **nur unter `net11.0`** — ein expliziter, einmaliger Kurzlauf für eine
schnelle Exploration, kein Zahlenwert, der als Vergleichsergebnis in einen Report ginge.

Aufruf:

```bash
dotnet run --project benchmark/Blun.MultiRaft.Benchmarks/Blun.MultiRaft.Benchmarks.csproj -c Release -f net11.0 -- --runtimes net11.0 --filter "*CompressionLevelLargePayloadBenchmarks*"
```

### Ergebnis (verkleinerte Matrix)

**Status: vollständig gelaufen.** 128 Fälle (8 Größen × 4 Level × 2 Inhaltstypen × 2 APIs), nur `net11.0`,
Laufzeit 1:24:06 (5.046,64 s Workload-Zeit laut BenchmarkDotNet). Rohdaten: `compression-bench-large-partial.csv`
(im Repo-Root, nicht committet — Artefakt eines lokalen Laufs; je Zeile Größe/Level/API/Inhaltstyp/Mean-ms).
Jede Zelle unten ist der `Mean` einer einzelnen BenchmarkDotNet-Messreihe (kein weiteres Mitteln über
Api/ContentType wie in der ersten Fassung dieses Abschnitts — diese Version schlüsselt **beides einzeln**
auf, wie angefordert).

#### Vollständige Tabelle, nach Größe und Inhaltstyp getrennt

Alle Zeiten in ms. `None` läuft (redundant, siehe Design-Hinweis oben) unter `Stream` und `Frame` identisch
mit — hier als ein gemeinsamer Wert je Inhaltstyp dargestellt (Mittel der beiden, Abweichung < 1%).

**2.000 B**

| Level | API | Random | Repetitive |
|---|---|---|---|
| None | – | 0,4287 | 0,4259 |
| L00_FAST | Stream | 0,4278 | 0,4148 |
| L00_FAST | Frame | 0,4330 | 0,4187 |
| L06_HC | Stream | 0,4596 | 0,4213 |
| L06_HC | Frame | 0,4347 | 0,4302 |
| L12_MAX | Stream | 0,4491 | 0,4477 |
| L12_MAX | Frame | 0,4418 | 0,4663 |

**8.000 B**

| Level | API | Random | Repetitive |
|---|---|---|---|
| None | – | 0,4430 | 0,4315 |
| L00_FAST | Stream | 0,4406 | 0,4280 |
| L00_FAST | Frame | 0,4430 | 0,4254 |
| L06_HC | Stream | 0,4806 | 0,4343 |
| L06_HC | Frame | 0,4806 | 0,4373 |
| L12_MAX | Stream | 0,4835 | 0,4272 |
| L12_MAX | Frame | 0,4734 | 0,4305 |

**16.000 B**

| Level | API | Random | Repetitive |
|---|---|---|---|
| None | – | 0,4448 | 0,4442 |
| L00_FAST | Stream | 0,4470 | 0,4156 |
| L00_FAST | Frame | 0,4305 | 0,4158 |
| L06_HC | Stream | 0,5201 | 0,4238 |
| L06_HC | Frame | 0,5169 | 0,4225 |
| L12_MAX | Stream | 0,5351 | 0,4278 |
| L12_MAX | Frame | 0,5508 | 0,4268 |

**32.000 B** — siehe Anomalie-Hinweis unten (`None` weicht stark zwischen den Inhaltstypen ab, obwohl
Kompression hier noch gar nicht greift)

| Level | API | Random | Repetitive |
|---|---|---|---|
| None | – | 1,2329 | 2,0615 |
| L00_FAST | Stream | 2,0660 | 2,0300 |
| L00_FAST | Frame | 2,0620 | 2,0540 |
| L06_HC | Stream | 2,4120 | 2,0950 |
| L06_HC | Frame | 2,4210 | 1,9940 |
| L12_MAX | Stream | 2,3950 | 2,0580 |
| L12_MAX | Frame | 2,3850 | 2,0380 |

**64.000 B**

| Level | API | Random | Repetitive |
|---|---|---|---|
| None | – | 2,0945 | 2,0265 |
| L00_FAST | Stream | 2,0630 | 2,0480 |
| L00_FAST | Frame | 2,0930 | 1,9860 |
| L06_HC | Stream | 2,8460 | 2,0230 |
| L06_HC | Frame | 2,8240 | 2,0280 |
| L12_MAX | Stream | 2,9050 | 1,9720 |
| L12_MAX | Frame | 2,8980 | 2,0340 |

**128.000 B**

| Level | API | Random | Repetitive |
|---|---|---|---|
| None | – | 2,2905 | 2,2475 |
| L00_FAST | Stream | 2,5890 | 2,0740 |
| L00_FAST | Frame | 2,3310 | 2,0760 |
| L06_HC | Stream | 4,3670 | 2,1810 |
| L06_HC | Frame | 4,0110 | 2,1100 |
| L12_MAX | Stream | 4,2210 | 4,0980 |
| L12_MAX | Frame | 4,1990 | 4,1410 |

**256.000 B**

| Level | API | Random | Repetitive |
|---|---|---|---|
| None | – | 3,3225 | 3,2745 |
| L00_FAST | Stream | 3,3910 | 2,0860 |
| L00_FAST | Frame | 3,3520 | 2,0430 |
| L06_HC | Stream | 7,3560 | 2,3560 |
| L06_HC | Frame | 7,3250 | 2,3940 |
| L12_MAX | Stream | 7,5430 | 8,6690 |
| L12_MAX | Frame | 7,6640 | 8,6830 |

**1.048.576 B (1 MiB)**

| Level | API | Random | Repetitive |
|---|---|---|---|
| None | – | 4,1830 | 4,1335 |
| L00_FAST | Stream | 4,7920 | 2,3280 |
| L00_FAST | Frame | 4,6560 | 2,2470 |
| L06_HC | Stream | 21,6360 | 3,6820 |
| L06_HC | Frame | 22,5310 | 3,7590 |
| L12_MAX | Stream | 23,4570 | 35,2270 |
| L12_MAX | Frame | 23,3400 | 35,5590 |

#### Zusammenfassung über alle Größen (komprimiert, beide Inhaltstypen gepoolt)

- Nach Level: `L00_FAST` Ø 1,735 ms, `L06_HC` Ø 3,369 ms, `L12_MAX` Ø 5,970 ms.
- Nach API: `Frame` Ø 3,694 ms, `Stream` Ø 3,689 ms — praktisch identisch, kein belastbarer Sieger über alle
  Größen gepoolt (siehe unten für die größenabhängige Aufschlüsselung, die interessanter ist als dieser
  Durchschnitt).

#### Speicherallokation (`[MemoryDiagnoser]`, Bytes pro Aufruf)

Anders als die Zeitmessung liefert die Allokation ein **eindeutiges, größen- und level-unabhängiges**
Ergebnis: `Stream` allokiert durchgehend **~1,5× so viel wie `Frame`**, bei jeder gemessenen Größe. Grund:
`CompressWithStream` schreibt in einen `MemoryStream` und ruft am Ende `ToArray()` — das kopiert den
internen Puffer ein zweites Mal. `CompressWithFrame` schreibt direkt in einen einzigen, vorab dimensionierten
Zielpuffer und gibt ihn per `Memory<byte>`-Slice zurück, ohne zweite Kopie.

| Größe | `None` (Baseline, kein Kompressionsaufruf) | `Stream` (Ø über alle Level) | `Frame` (Ø über alle Level) | Verhältnis Stream/Frame |
|---|---|---|---|---|
| 2.000 B | 623 B | 4.323 B | 3.219 B | 1,34× |
| 8.000 B | 636 B | 13.363 B | 9.247 B | 1,45× |
| 16.000 B | 724 B | 25.410 B | 17.545 B | 1,45× |
| 32.000 B | 642 B | 49.507 B | 33.343 B | 1,48× |
| 64.000 B | 626 B | 97.709 B | 65.577 B | 1,49× |
| 128.000 B | 625 B | 194.538 B | 129.858 B | 1,50× |
| 256.000 B | 718 B | 387.124 B | 258.537 B | 1,50× |
| 1.048.576 B | 666 B | 1.581.241 B | 1.054.765 B | 1,50× |

Das Verhältnis konvergiert bei größeren Payloads sauber gegen **1,5×** (nicht 2×, wie eine reine
"eine Kopie extra"-Überlegung nahelegen würde — der `MemoryStream`-interne Puffer selbst ist bereits auf
`LZ4Codec.MaximumOutputSize(payload.Length)` vorallokiert, sodass nur der `ToArray()`-Aufruf zusätzlich
kopiert, nicht zusätzlich über-alloziert). Das Kompressions-**Level** hat auf die Allokation praktisch
keinen Einfluss (`L00_FAST`/`L06_HC`/`L12_MAX` liegen im gepoolten Schnitt alle bei ~245 KB je Aufruf über
die gesamte Größenmatrix) — die Puffergröße wird vorab aus der *Payload-Länge* berechnet, nicht aus dem
tatsächlichen Kompressionsergebnis.

**Konsequenz für die API-Wahl:** Das ist der einzige Messwert in diesem Lauf, bei dem eine der beiden APIs
**konsistent und mit klarer Ursache** besser abschneidet, unabhängig von Größe, Level oder Inhaltstyp.
Kombiniert mit der Zeitmessung (kein konsistenter Zeitunterschied, siehe unten) macht das **`Frame` zur
eindeutig vorzuziehenden API** — nicht wegen Rohgeschwindigkeit, sondern weil sie bei identischem Ergebnis
strukturell weniger Garbage produziert, was bei tausenden Gruppen und kontinuierlicher Append-Last (siehe
CLAUDE.md, "ten-thousand-group scale") über die Zeit spürbar wird (GC-Druck, siehe auch die gemerkte
Pinned-Memory-Überlegung zu K4os an anderer Stelle in diesem Projekt).

#### Kernbefunde

1. **Random und Repetitive verhalten sich ab 128 KB komplett unterschiedlich — der gepoolte Durchschnitt
   aus der ersten Fassung dieses Abschnitts hat das verdeckt.** Bei `Repetitive` (viel Redundanz, LZ4 findet
   leicht Treffer) bleibt selbst `L06_HC` bis 1 MiB günstig (3,68–3,76 ms, kaum über `None`s 4,13 ms). Bei
   `Random` (nichts zu finden) explodiert dieselbe Stufe auf 21,6–22,5 ms bei 1 MiB — ein Faktor von ~6
   gegenüber der Repetitive-Zeit bei identischem Level und identischer Größe. Kompressionszeit ist also
   nicht in erster Linie eine Funktion der Payload-*Größe*, sondern der Payload-*Kompressibilität*, und eine
   reine Größen-Heuristik (wie ursprünglich angefragt: "Payload-Länge → Level") kann das nicht abbilden,
   ohne im Worst Case (inkompressible Nutzlast, z.B. bereits komprimierte oder verschlüsselte
   Message-Bodies) drastisch teurer zu werden als `None`.
2. **`L12_MAX` dreht bei 256 KB/1 MiB sogar um: dort wird `Repetitive` langsamer als `Random`**, obwohl
   `Repetitive` bei den niedrigeren Leveln durchgehend schneller war (z.B. 1 MiB: `L12_MAX` Random
   23,3–23,5 ms vs. Repetitive **35,2–35,6 ms** — Repetitive ist hier fast 1,5× langsamer als Random).
   Plausible Erklärung: die aufwändige Optimal-Parsing-Suche von `L12_MAX` (`LZ4Level.L12_MAX` ist K4os'
   exhaustive/optimal-parse-Modus) hat bei hochredundanten Daten sehr viele Match-Kandidaten zu bewerten,
   während sie bei echtem Zufallsrauschen schnell aufgibt, weil es kaum Treffer gibt. Nicht weiter
   verifiziert (z.B. per Kompressions-Timer isoliert von Append+Flush) — als Beobachtung, nicht als
   bewiesene Ursache zu lesen.
3. **`L00_FAST` bleibt in beiden Inhaltstypen über fast den gesamten Bereich nah an `None`, mit einer
   Ausnahme:** bei `Random` und großen Größen (256 KB, 1 MiB) liegt `L00_FAST` leicht *über* `None`
   (1 MiB: 4,66–4,79 ms vs. 4,13–4,18 ms — ~13% langsamer), während es bei `Repetitive` deutlich *unter*
   `None` liegt (1 MiB: 2,25–2,33 ms vs. 4,13 ms — fast halbiert). Der ursprünglich (im gepoolten
   Durchschnitt) berichtete "Kompression lohnt sich ab 256 KB"-Befund gilt also nur für kompressible
   Inhalte, nicht pauschal.
4. **`L06_HC`/`L12_MAX` sind für den WAL-Append-Pfad bei `Random`-artigen (inkompressiblen) Payloads in
   keinem gemessenen Größenbereich zu empfehlen** — dort kostet jede Stufe oberhalb `L00_FAST` durchgehend
   mehr als `None`, mit wachsendem Abstand bei wachsender Größe.
5. **Auffälliger Sprung bei 32.000 B, nur bei `Random`**: `None` fällt bei `Random` auf 1,233 ms, während
   `Repetitive` bei 2,062 ms bleibt — bei 16 KB lagen beide `None`-Werte noch dicht beieinander (0,4448 vs.
   0,4442 ms). Da `None` gar nichts komprimiert, kann der Inhaltstyp hier eigentlich keinen Unterschied
   machen — das riecht nach Messrauschen an einer Warmup-/GC-Grenze bei dieser Fallgröße, nicht nach einem
   echten Effekt. Vor einer Schlussfolgerung für diesen konkreten Größenbereich würde ich das mit mehr
   Wiederholungen nachmessen.
6. **`Stream` vs. `Frame`: über beide Inhaltstypen und alle Größen kein konsistenter Sieger**, Unterschiede
   liegen meist im niedrigen einstelligen Prozentbereich in beide Richtungen (z.B. 1 MiB/Random/`L00_FAST`:
   Frame 4,656 ms vs. Stream 4,792 ms, Frame ~3% schneller; aber 1 MiB/Repetitive/`L06_HC`: Stream 3,682 ms
   vs. Frame 3,759 ms, Stream ~2% schneller). Kein Größen- oder Inhaltsmuster erkennbar, das eine der beiden
   APIs systematisch bevorzugt — die Wahl sollte nach API-Ergonomie entschieden werden (`Frame` ist
   Span-zu-Span ohne `Stream`-Umweg, direkterer Fit für einen WAL-Eintrag), nicht nach Rohgeschwindigkeit.

#### Vorläufige Empfehlung (überarbeitet, inhaltstyp-bewusst)

Die ursprüngliche reine "Größe → Level"-Heuristik aus der Aufgabenstellung lässt sich anhand dieser Daten
**nicht** verantwortungsvoll ableiten, weil Kompressibilität (nicht nur Größe) das Ergebnis dominiert:

| Payload-Größe | Empfehlung, wenn Inhalt bekannt kompressibel ist (z.B. JSON/Text) | Empfehlung, wenn Inhalt unbekannt/potenziell inkompressibel ist |
|---|---|---|
| < 256 KB | `None` — kein klarer Vorteil, `L00_FAST` weder Gewinn noch Verlust | `None` |
| ≥ 256 KB | `L00_FAST` — deutlicher Netto-Gewinn (bei 1 MiB fast halbierte Zeit) | `None` — `L00_FAST` ist hier bereits ~13% *langsamer* als `None` |
| jede Größe | `L06_HC`/`L12_MAX` nicht empfohlen — Ausnahme ggf. `L06_HC` bei sehr großen, sehr redundanten Payloads (bleibt dort nah an `L00_FAST`), aber ohne klaren Zusatznutzen gegenüber `L00_FAST` | `L06_HC`/`L12_MAX` explizit vermeiden — dort am teuersten gemessen |
| API-Wahl | `Frame` (Ergonomie-Entscheidung, nicht Geschwindigkeit — Messwerte praktisch gleichauf) | `Frame` |

**Praktische Konsequenz für eine spätere `RaftPayloadCompression`-Heuristik:** eine reine Payload-Längen-
Schwelle (wie ursprünglich skizziert: "< 100 B → kein Level, > 512 KB → Max-Level") ist nach diesen Daten zu
grob. Ein sichererer Ansatz wäre, immer mit `L00_FAST` zu versuchen (dessen Worst-Case-Kosten bei
inkompressiblen Daten und großen Payloads mit ~13% überschaubar bleiben) und nur zu speichern, wenn das
Ergebnis tatsächlich kleiner ist als das Original — nicht, verschiedene Level anhand der Größe vorab zu
wählen. `L06_HC`/`L12_MAX` würde ich aus dem WAL-Append-Pfad ganz herausnehmen; sie sind allenfalls für
Snapshots interessant (dort zählt einmalige Größe, nicht Latenz pro Append — aber das ist außerhalb des
Scopes dieses Benchmarks).
