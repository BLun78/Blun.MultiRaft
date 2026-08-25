# Wirkt `[MethodImpl(MethodImplOptions.AggressiveInlining)]` in diesem Code?

## Frage

Der WAL-Code trug an 19 Stellen `[MethodImpl(MethodImplOptions.AggressiveInlining)]`. Die Behauptung war,
dass ein Teil davon wirkungslos ist. Behauptungen über den JIT sind billig — dieser Benchmark misst nach.

## Warum das nicht direkt messbar ist

Das Attribut wird zur Compile-Zeit in die Metadaten geschrieben. Es gibt keinen Schalter, mit dem sich
dieselbe Methode einmal mit und einmal ohne messen ließe. `InliningBenchmarks.cs` misst deshalb **Kopien der
Methodenformen**, nicht die ausgelieferten Methoden. Wer die Bodies eines Tripletts auseinanderlaufen lässt,
misst ab dann etwas anderes.

Zwei Entwurfsentscheidungen tragen das Ergebnis:

- **Der `Never`-Arm (`NoInlining`) ist das Lineal.** Aus zwei Zahlen allein lässt sich nicht ablesen, ob der
  JIT im Basisfall schon inlinet hat oder ob beide Arme gleich langsam sind. Der erzwungene Call liefert den
  Preis eines echten Calls, und erst davor werden die anderen beiden lesbar.
- **Jede Schleife ist eine serielle Abhängigkeitskette**, kein Summieren unabhängiger Calls. Unabhängige
  Iterationen erlauben dem JIT, den geinlineten Arm zu vektorisieren oder aus der Schleife zu ziehen, während
  der `Never`-Arm skalar bleibt — das erzeugt einen Unterschied, der nichts mit dem Call zu tun hat.

Gemessen wird Latenz pro Call, in Nanosekunden. Für die Frage „*wurde* inlined?" ist Timing ohnehin das
schwächere Instrument; `--disasm --disasmDepth 3` beantwortet sie als Tatsache statt als Messung.

## Lauf

```bash
dotnet run --project benchmark/Blun.MultiRaft.Benchmarks/Blun.MultiRaft.Benchmarks.csproj -c Release \
  -f net11.0 -- --runtimes net10.0 net11.0 --filter "*InliningBenchmarks*" --job short
```

BenchmarkDotNet v0.16.0-preview.1, Windows 11 26200, 12th Gen Intel Core i9-12900K, .NET 10.0.11 /
.NET 11.0.0-preview.6. `--job short` (WarmupCount=3, IterationCount=3, LaunchCount=1) — die Fehlerbalken sind
entsprechend grob, aber die Abstände hier sind größer als die Streuung.

| Form | Methode | .NET 10 | .NET 11 |
|---|---|---:|---:|
| Triviale Arithmetik | `SizeOf_JitDecides` | 0,593 ns | 0,598 ns |
| (`RaftWalRecord.SizeOf`) | `SizeOf_Aggressive` | 0,593 ns | 0,595 ns |
| | `SizeOf_Never` | 0,836 ns | 1,044 ns |
| Switch mit `throw` | `ChecksumSize_JitDecides` | 1,302 ns | 1,453 ns |
| (`ChecksumSizeFor`) | `ChecksumSize_Aggressive` | **1,035 ns** | **1,035 ns** |
| | `ChecksumSize_Never` | 1,641 ns | 1,464 ns |
| Interface-Dispatch | `Dispatch_Interface_Plain` | 0,823 ns | 0,820 ns |
| | `Dispatch_Interface_Aggressive` | 0,811 ns | 0,817 ns |
| | `Dispatch_Direct` | 0,811 ns | 0,809 ns |
| Interface + echter CRC-32 | `Checksum_Interface_Plain` | 12,331 ns | 12,326 ns |
| (`Crc32ChecksumStrategy`) | `Checksum_Interface_Aggressive` | 12,321 ns | 12,385 ns |

## Auswertung

**Triviale Arithmetik: das Attribut ist ein No-op.** Mit und ohne liegen auf derselben Zahl (0,593 vs.
0,593 ns), und beide deutlich unter dem erzwungenen Call. Der JIT inlinet diese Form von sich aus; das
Attribut schreibt hin, was ohnehin passiert. Die verbliebenen Hinweise auf `RaftEntryHeader.Read/Write`,
`RaftWalRecord.SizeOf`, `Advance` und `ToTicks` haben damit dokumentarischen, keinen messbaren Wert.

**Switch mit `throw`: das Attribut wirkt — hier war die Vorhersage nur halb richtig.** Auf .NET 11 liegt der
Normalfall (1,453 ns) praktisch auf dem erzwungenen Call (1,464 ns): der JIT lehnt diese Form ab. Das
Attribut drückt sie auf 1,035 ns, also **rund 29 % schneller** auf .NET 11 und 20 % auf .NET 10. Auf dieser
Methodenform ist `AggressiveInlining` kein Placebo.

Für `WalChecksumStrategy.ChecksumSizeFor` ändert das trotzdem nichts: die Methode wird im gesamten Code
**zweimal** aufgerufen, beide Male kalt — `SegmentedRaftWalOptions`-Validierung und der `WalSegment`-
Konstruktor, der das Ergebnis in `_checksumSize` festhält, gerade damit pro Entry nicht neu geschaltet wird.
0,4 ns zweimal pro Segmenterzeugung sind nicht messbar. Der Grund für die Entfernung war die kalte
Aufrufstelle, nicht fehlende Wirkung — und wenn diese Methode je in einen heißen Pfad wandert, ist das
Attribut dort **messbar** gerechtfertigt.

**Interface-Dispatch: das Attribut wirkt nicht.** Mit Attribut (0,811 ns), ohne (0,823 ns) und der direkte
Aufruf über den konkreten Typ (0,811 ns) sind dieselbe Zahl. Bemerkenswert ist die zweite Hälfte davon: der
Interface-Aufruf kostet nichts gegenüber dem direkten, die Guarded Devirtualisation aus dynamischem PGO
greift also. Genau deshalb ist das Attribut wirkungslos — devirtualisiert wird ohnehin geinlinet, und ohne
Devirtualisation kommt es gar nicht zum Zug. *Vorbehalt:* Der Benchmark hat pro Aufrufstelle genau eine
Implementierung, was es der GDV leicht macht. In der Bibliothek nutzt jede WAL-Instanz konsistent eine
Strategie, der Fall ist also vergleichbar; eine Aufrufstelle, die zwischen mehreren Strategien wechselt, wäre
es nicht.

**Interface + echter CRC-32: der Call verschwindet in der Arbeit.** 12,33 ns gegen 12,32 ns. Der Call-Frame
kostet nach der Messung oben rund 0,24 ns, also unter 2 % der Hash-Zeit über 64 Byte — und hier nicht einmal
das, weil er weginlinet wird. Das ist der eigentliche Grund, warum die sechs Strategie-Implementierungen ihre
Attribute verloren haben.

## Konsequenz

Die Messung stützt das Aufräumen in `Checksum/`, aber mit einer Korrektur an der Begründung: die
Switch-Form profitiert real, sie tut es nur an keiner Stelle, an der dieser Code sie aufruft.

Und die Größenordnung insgesamt einordnen: das Beste, was hier zu gewinnen war, sind 0,4 ns pro Aufruf. Ein
`RandomAccess.FlushToDisk` auf dem Append-Pfad kostet je nach Laufwerk das Zehntausend- bis Millionenfache.
Inlining ist in dieser Bibliothek kein Hebel; Batching, Puffer-Wiederverwendung und die Frage, wie oft
überhaupt fsync fällt, sind es.
