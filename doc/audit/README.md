# Audit — Blun.MultiRaft

Vollständige Lesung des Repositories in drei Durchgängen, Stand `0ba36df` (20. August 2026), zuzüglich der
zu diesem Zeitpunkt noch nicht committeten Teile (`demo/Blun.MultiRaft.Observer`,
`demo/Blun.MultiRaft.Observer.Ui`, `demo/chaos`, `ControlPlane.cs`, `MessageSender.cs`).

Gelesen wurden rund 15.000 Zeilen echter Quelltext (ohne `obj/`), dazu Projektdateien, Paketverwaltung,
`.gitignore` und die CI-Definition.

## Durchgang 1 — Security

Ein Dokument pro Befund.

| ID | Schwere | Titel |
|---|---|---|
| [SEC-001](SEC-001-raft-transport-ohne-authentifizierung.md) | **Kritisch** | Der Raft-gRPC-Endpunkt ist vollständig unauthentifiziert |
| [SEC-002](SEC-002-klartext-transport-als-default.md) | **Hoch** | Klartext (h2c) ist der Default des Transports |
| [SEC-003](SEC-003-unbegrenzte-snapshot-kanaele.md) | **Hoch** | Unbegrenzte Snapshot-Kanäle ohne Backpressure |
| [SEC-004](SEC-004-unbegrenztes-request-fanout.md) | Mittel–Hoch | Unbegrenztes Request-Fan-out und unbegrenzte Sendewarteschlange |
| [SEC-005](SEC-005-csrf-gegen-loopback-endpunkte.md) | Mittel | CSRF gegen die Loopback-HTTP-Endpunkte |
| [SEC-006](SEC-006-ungepruefte-traffic-parameter.md) | Mittel | Ungeprüfte `size`/`count`-Parameter: OOM und dauerhafte Blockade |
| [SEC-007](SEC-007-entry-kind-truncation.md) | Mittel | `RaftEntryKind`/`ApplicationTag` werden abgeschnitten statt validiert |
| [SEC-008](SEC-008-batch-append-umgeht-payload-limit.md) | **Hoch** | Batch-Append umgeht `MaxPayloadBytes`; Recovery bestraft das mit Datenverlust |
| [SEC-009](SEC-009-uri-interpolation-im-observer-proxy.md) | Niedrig | Ungeprüfte Routenwerte unkodiert in Proxy-URIs |
| [SEC-010](SEC-010-snapshot-header-laenge-unvalidiert.md) | Mittel | Unvalidierte Längenangabe im Snapshot-Header |
| [SEC-011](SEC-011-cluster-mode-marker-ohne-integritaetspruefung.md) | Mittel | Ein gekipptes Bit im Cluster-Mode-Marker hebelt die Sicherheitsregel aus |

## Durchgang 2 — `src/`: Probleme, Speicherlecks, Locks

→ **[PROBLEME-src-speicher-lecks-und-locks.md](PROBLEME-src-speicher-lecks-und-locks.md)** — 21 Befunde
(BUG-001 bis BUG-021), plus eine Liste der Stellen, die geprüft und für in Ordnung befunden wurden.

Die schwersten: der Tick-Loop des gesamten Knotens stirbt an einer unerwarteten Exception
([BUG-005](PROBLEME-src-speicher-lecks-und-locks.md#bug-005)), ein Membership-Eintrag ohne Längenprüfung
macht eine Gruppe dauerhaft unstartbar
([BUG-007](PROBLEME-src-speicher-lecks-und-locks.md#bug-007)), das `_stateGate` wird über die gesamte
Snapshot-Übertragung gehalten ([BUG-010](PROBLEME-src-speicher-lecks-und-locks.md#bug-010)), unter Windows
zerlegt eine parallel laufende Kompaktierung den Log-Zustand
([BUG-011](PROBLEME-src-speicher-lecks-und-locks.md#bug-011)), und eine beschädigte Meta-Datei setzt den
Wahlzustand still zurück ([BUG-021](PROBLEME-src-speicher-lecks-und-locks.md#bug-021)).

## Durchgang 3 — Demo, Tests, Benchmarks, Build, CI

→ **[PROBLEME-demo-und-uebriges.md](PROBLEME-demo-und-uebriges.md)** — 11 Befunde (D-001 bis D-011), plus
dieselbe Gegenliste.

Die schwersten: `.gitignore` deckt `node_modules/` und `dist/` nicht ab
([D-001](PROBLEME-demo-und-uebriges.md#d-001)), und `net11.0` wird nirgends getestet, obwohl dort
`runtime-async` aktiv ist ([D-002](PROBLEME-demo-und-uebriges.md#d-002)).

---

## Wenn nur drei Dinge gemacht werden

1. **[SEC-001](SEC-001-raft-transport-ohne-authentifizierung.md)** — Solange der Raft-Port
   unauthentifiziert ist, ist jeder andere Befund auf dem Transportpfad nur eine Variante desselben
   Problems. Bis eine Peer-Authentifizierung existiert, gehört ein deutlicher Warnhinweis in `README.md`
   und in die XML-Doku von `ConfigureRaftEndpoint`.
2. **[SEC-008](SEC-008-batch-append-umgeht-payload-limit.md) + [BUG-007](PROBLEME-src-speicher-lecks-und-locks.md#bug-007)**
   — Beides sind fehlende Validierungen auf dem Empfangspfad, beide enden in dauerhaftem Datenverlust
   bzw. einer Gruppe, die sich nicht mehr starten lässt, und beide sind wenige Zeilen. Der gemeinsame
   Grundsatz: *nichts in den Log schreiben, was dieser Knoten nicht wieder lesen und anwenden kann.*
3. **[BUG-005](PROBLEME-src-speicher-lecks-und-locks.md#bug-005)** — Die Absicht („one sick group must
   never stop the clock for the thousands beside it") ist richtig formuliert und von der Catch-Liste nicht
   gedeckt. Ein `catch (Exception)` an vier Stellen schließt die Lücke.

## Zum Charakter des Codes

Der Vollständigkeit halber, weil eine Befundliste ein schiefes Bild gibt: Dieser Code ist ungewöhnlich gut
dokumentiert, und zwar an der einzigen Stelle, an der es zählt — die Kommentare erklären durchgehend, *warum*
eine Entscheidung so und nicht anders getroffen wurde, inklusive der Fälle, in denen sich eine ursprüngliche
Annahme nicht bestätigt hat (die Multiplexing-Latenz im gRPC-Benchmark, die als widerlegt im Repository
stehen bleibt).

Ein großer Teil der Befunde oben ist genau deshalb überhaupt formulierbar: Die Absicht ist jeweils
aufgeschrieben, und der Befund ist die Lücke zwischen dieser Absicht und der Umsetzung — nicht ein fehlendes
Konzept. Das ist die angenehmere Sorte Audit-Ergebnis.
