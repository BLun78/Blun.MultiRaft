# SEC-009 — Ungeprüfte Routenwerte werden unkodiert in Proxy-URIs interpoliert

- **Schweregrad:** Niedrig
- **Kategorie:** Unzureichende Ausgabekodierung (CWE-116), Request-Manipulation
- **Ort:** `demo/Blun.MultiRaft.Observer/Program.cs` — `POST /api/resources/{name}/{command}`

## Befund

Der Observer reicht Ressourcen-Kommandos an die Control Plane weiter und baut die Ziel-URI durch
String-Interpolation:

```csharp
app.MapPost("/api/resources/{name}/{command}", async (string name, string command, …) =>
{
    using HttpResponseMessage response = await clients
        .CreateClient("node")
        .PostAsync(new Uri(settings.ControlPlane, $"/api/resources/{name}/{command}"), null, token);
```

`name` und `command` kommen ungeprüft und **unkodiert** aus der Route. Routenwerte sind zu diesem
Zeitpunkt bereits URL-dekodiert, ein `%2F` im eingehenden Pfad wird also zu einem echten `/`, und
`?`/`#` werden als Query- bzw. Fragment-Trenner wirksam.

Zum Vergleich: `Traffic.SendAsync` interpoliert ebenfalls, prüft den Gruppennamen aber vorher gegen die
bekannten Gruppen (`current.Groups.FirstOrDefault(g => string.Equals(g.Group, group, StringComparison.Ordinal))`)
und ist damit effektiv durch eine Allowlist gedeckt. `Membership.ChangeAsync` interpoliert eine bereits
als `ulong` geparste Zahl. Nur dieser eine Endpunkt reicht rohe Strings durch.

## Auswirkung

Begrenzt, aber real:

- Die Ziel-URI kann auf einen anderen Pfad der Control Plane umgelenkt werden. Da diese nur drei Routen
  anbietet und `Resolve(name)` dort eine Allowlist erzwingt, ist der erreichbare Schaden heute klein.
- Query-/Fragment-Injektion in den weitergereichten Request.
- Die Schutzwirkung hängt vollständig an der Allowlist der *Gegenstelle*. Das ist eine Kopplung, die
  niemand sieht, wenn später ein Endpunkt zur Control Plane hinzukommt — genau die Art von latenter
  Annahme, die bei einer Erweiterung kippt.

Ich stufe das bewusst als niedrig ein: Es ist heute keine ausnutzbare Lücke, sondern eine fehlende
Absicherung an einer Stelle, an der zwei andere Stellen im selben Projekt es richtig machen.

## Empfehlung

1. Beide Segmente kodieren:
   ```csharp
   new Uri(settings.ControlPlane,
       $"/api/resources/{Uri.EscapeDataString(name)}/{Uri.EscapeDataString(command)}")
   ```
2. Besser zusätzlich: `command` im Observer gegen `start`/`stop`/`restart` prüfen, statt beliebige
   Strings weiterzureichen. Die Control Plane tut das bereits — dieselbe Prüfung am Eingang spart den
   Roundtrip und macht die Annahme sichtbar.
3. `name` gegen die dem Observer bekannten Knoten (`ObserverOptions.Nodes`) validieren.

## Verwandt

- [SEC-005](SEC-005-csrf-gegen-loopback-endpunkte.md)
