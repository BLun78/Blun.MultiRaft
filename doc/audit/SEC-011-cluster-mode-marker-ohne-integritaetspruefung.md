# SEC-011 — Ein gekipptes Bit im Cluster-Mode-Marker hebelt die Sicherheitsregel aus, für die er existiert

- **Schweregrad:** Mittel
- **Kategorie:** Fehlende Validierung eines persistierten Enums (CWE-20), Umgehung einer Sicherheitsprüfung
- **Ort:** `src/Blun.MultiRaft/Cluster/ClusterModeStore.cs` — `ReadAsync`, `ValidateTransition`

## Befund

`ClusterModeStore` existiert für genau eine Regel, die im Klassenkommentar sorgfältig begründet ist:
**replicated → single-node muss abgelehnt werden**, weil der herausgelöste Knoten sonst zu einer zweiten
Autorität über dieselben Gruppen-IDs würde.

Der Marker wird ungeprüft aus der Datei gecastet:

```csharp
return new ClusterModeMarker(
    (ClusterMode)BinaryPrimitives.ReadUInt64LittleEndian(buffer.AsSpan(8)),
    new NodeId(BinaryPrimitives.ReadUInt64LittleEndian(buffer.AsSpan(16))));
```

Und die Prüfung ist als Positivvergleich formuliert:

```csharp
if (marker.Mode == ClusterMode.Replicated && intended == ClusterMode.SingleNode)
{
    throw new InvalidOperationException(…);
}

return marker.Mode == ClusterMode.SingleNode && intended == ClusterMode.Replicated;
```

Ein `Mode`-Wert, der weder `SingleNode` noch `Replicated` ist, fällt durch **beide** Vergleiche hindurch:
`ValidateTransition` wirft nicht und gibt `false` zurück — der Start wird also erlaubt.

Über die 24 Byte des Datensatzes gibt es keine Prüfsumme; das Magic deckt nur Byte 0–7 ab. Die
Mode-Bytes liegen bei Offset 8–15 und sind damit ungeschützt.

## Auswirkung

Ein einzelnes beschädigtes Byte in `cluster.mode` — ein Bitfehler, ein halb geschriebener Sektor, ein
manuell kopiertes Datenverzeichnis — lässt einen Knoten, der zuvor Teil eines replizierten Clusters war,
als Single-Node-Cluster starten. Genau das Szenario, das die Klasse verhindern soll, und genau mit den
Konsequenzen, die ihr eigener Kommentar beschreibt:

> Both sides would then have real, committed, divergent histories, and there is no rule that reconciles
> them — which is precisely why this has to be refused up front rather than resolved later.

Die Prüfung ist also nicht „fail-safe", sondern „fail-open": Bei einem unverständlichen Marker wird die
riskantere Option gewählt. Die zweite Prüfung in derselben Methode (`marker.Self != self`) ist demgegenüber
korrekt fail-safe formuliert.

## Empfehlung

1. **Unbekannte Modi als Fehler behandeln**, nicht als „keine Einschränkung":
   ```csharp
   if (!Enum.IsDefined(marker.Mode))
   {
       throw new InvalidOperationException(
           "The cluster mode marker for node " + self + " is unreadable. Refusing to start rather than "
           + "guessing which mode this node last ran in.");
   }
   ```
   `Enum.IsDefined` ist AOT-/trim-tauglich in der generischen Überladung (`Enum.IsDefined<ClusterMode>`),
   passt also zur Trim-Safe-Vorgabe des Projekts.
2. Genauso in `ReadAsync`: Ein Datensatz mit unbekanntem Modus sollte nicht als gültiger Marker
   zurückkommen. Achtung — hier ist `null` die *falsche* Antwort, denn `null` bedeutet „hat noch nie
   gelaufen" und erlaubt damit ebenfalls jeden Modus. Der Unterschied zwischen „kein Marker" und
   „unlesbarer Marker" muss erhalten bleiben.
3. Eine Prüfsumme über den 24-Byte-Datensatz aufnehmen, analog zu den WAL-Datensätzen.
4. Denselben Blick auf `FileRaftMetaStore.ReadAsync` werfen: Dort wird bei einem falschen Magic
   `RaftMeta.Initial` zurückgegeben — also Term 0, keine Stimme. Das ist eine *stille Rücksetzung* des
   Wahlzustands und kann dazu führen, dass ein Knoten in einem Term zweimal wählt. Der Kommentar der
   Schnittstelle benennt diese Gefahr selbst („losing it is how a node ends up voting twice in one term and
   electing two leaders"), die Implementierung behandelt einen beschädigten Datensatz aber wie einen
   fehlenden. Siehe [BUG-021](PROBLEME-src-speicher-lecks-und-locks.md#bug-021).

## Verwandt

- [SEC-010](SEC-010-snapshot-header-laenge-unvalidiert.md)
- [BUG-021](PROBLEME-src-speicher-lecks-und-locks.md#bug-021)
