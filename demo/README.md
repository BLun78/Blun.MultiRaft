# Demo

Aspire-orchestrierter Fünf-Node-Cluster mit echtem gRPC-Transport, plus ein Observer, der alle fünf Nodes
beobachtet und darf sie starten/stoppen.

## Warum fünf Nodes

Quorum ist bei fünf Nodes drei. Das macht die interessanten Zustände von Hand erreichbar: ein Node down
ändert nichts, zwei down funktioniert noch, der dritte down stoppt Writes. Bei drei Nodes gibt es nur den
ersten und den letzten dieser Zustände, nicht den mittleren.

## Starten

```bash
dotnet run --project demo/Blun.MultiRaft.AppHost/Blun.MultiRaft.AppHost.csproj
```

Status-Endpoints der Nodes: `http://localhost:8101` bis `8105`. Control Plane (start/stop von Prozessen):
`8200`. Observer-API: `8300`. Observer-UI (falls Node.js auf dem PATH liegt): `4200`.

Für einen kalten Cluster vorher `demo/Blun.MultiRaft.AppHost/data/` löschen — sonst startet der Cluster mit
dem Log-Stand des letzten Laufs weiter.

## Was pro Node auf der SSD landet

Jeder Node bekommt sein eigenes Datenverzeichnis unter `demo/Blun.MultiRaft.AppHost/data/node-<id>/` — fünf
Nodes heißt fünfmal dieselbe Struktur parallel auf der Platte, komplett unabhängig voneinander.

```
data/node-<id>/
└── wal/
    └── g<GroupId als 20-stellige Zahl>/      # ein Verzeichnis pro Raft-Gruppe
        ├── wal.cfg                            # Log-Metadaten: Segmentgröße, Checksum-Algorithmus, Layout-Version
        ├── log.base                           # Basiszustand, ab dem die Segmente aufsetzen (nach Snapshot-Kompaktierung)
        └── <ErsterIndex als 20-stellige Zahl>.seg   # Segment-Dateien, append-only
```

**Ein Verzeichnis pro Gruppe, nicht ein gemeinsames Log für alle Queues.** Das ist Absicht: eine leergelaufene
Queue kann ihr ganzes Log wegwerfen, ohne andere Gruppen zu berühren. Die administrative Cluster-Gruppe
(`RaftGroupId.Cluster = ulong.MaxValue`) bekommt genau dasselbe Layout wie jede Queue-Gruppe.

**Segmente rollen nach Größe** (`SegmentSizeBytes`, Default 32 MiB) — nicht nach Zeit oder Eintragsanzahl.
Segmente sind die Einheit für Head-Truncation: nach einem Snapshot werden ganze Segmentdateien gelöscht, nie
innerhalb einer Datei geschnitten. Der Dateiname kodiert den ersten enthaltenen Log-Index, damit die
Wiederherstellung beim Öffnen ohne Indexdatei die Reihenfolge kennt.

**Jeder committete Append ist ein echtes fsync**, kein gepuffertes Schreiben — `FlushToDisk` ist per Default
an, sonst wäre die Raft-Haltbarkeitsgarantie nur behauptet, nicht wahr. Das ist auch der Grund, warum die
Demo-Traffic-Generierung mit ~36-42 Writes/s pro Node eher durch die Platte begrenzt ist als durch das
konfigurierte 10-ms-Intervall.

**Memory-Mapping ist aus** (`SegmentAccess = RandomAccess`, Default). Ein gemapptes Segment reserviert
Adressraum/Page-Table-Einträge pro Gruppe, solange geschrieben wird — bei fünf Nodes mit mehreren Gruppen
pro Node summiert sich das schnell. In der Demo ist kein Grund, das anzuschalten.

Außerhalb von `wal/`, aber ebenfalls pro Gruppe:

```
data/node-<id>/
└── meta/
    └── g<GroupId als 20-stellige Zahl>.meta   # aktueller Term + votedFor
```

Bewusst getrennt vom Log: das `.meta`-File wird bei jeder Wahl geschrieben, nicht bei jedem Append, und
folgt damit einem ganz anderen Schreibmuster als der WAL.

## Die Control Plane

Läuft im AppHost-Prozess, weil nur der die DCP-Ressourcen (Prozesse) direkt anfassen kann. Bindet an
Loopback und kennt nur die Ressourcennamen, mit denen sie konstruiert wurde.

Zwei verschiedene Arten von "raus", die nicht verwechselt werden dürfen:
- **Ressource stoppen** (über Aspire/Control Plane) beendet den Prozess.
- **`DELETE /cluster/nodes/{id}`** lässt den Prozess laufen, nimmt ihn aber aus der Konfiguration der
  Cluster-Gruppe. Das ist eine Single-Server-Membership-Change und läuft nur über den Cluster-Leader — der
  Observer routet diesen Request deshalb gezielt dorthin. Der entfernte Node wird vom Reconcile-Pass des
  Coordinators wieder aufgenommen — das ist korrektes Verhalten, kein Bug.

## Der Observer

Ein gewöhnlicher Client der `/status`-Endpoints der Nodes — keine privilegierten Zugriffe, alles was er
zeigt, könnte auch jeder andere abfragen. Nur Start/Stop von Prozessen geht über die Control Plane.

Die UI hält genau **einen** Log-Stream für alle fünf Nodes offen, nicht fünf einzelne. Grund: ein Browser
erlaubt sechs gleichzeitige Verbindungen pro Origin über HTTP/1.1. Fünf Node-Streams plus ein
Cluster-Stream sind genau sechs — jeder weitere Request (jeder Button-Klick) würde dahinter verhungern.

Der Cluster-Leader in der UI ist eine Zusammenfassung dessen, was jeder Node glaubt, keine feststehende
Tatsache — während einer Wahl sind sich die Nodes uneinig, und die UI zeigt das als "disputed" statt einen
davon zu bevorzugen.
