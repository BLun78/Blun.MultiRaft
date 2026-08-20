# SEC-005 — CSRF gegen die Loopback-HTTP-Endpunkte (Loopback ist keine Autorisierungsgrenze)

- **Schweregrad:** Mittel (Demo-Umgebung), Hoch bei Übernahme des Musters
- **Kategorie:** Cross-Site Request Forgery (CWE-352), fehlende Origin-Prüfung
- **Ort:** `demo/Blun.MultiRaft.AppHost/ControlPlane.cs`, `demo/Blun.MultiRaft.Observer/Program.cs`, `demo/Blun.MultiRaft.Node/Program.cs`

## Befund

Drei HTTP-Oberflächen der Demo binden an Loopback und behandeln das als ausreichenden Schutz. Der
Kommentar in `ControlPlane.cs` sagt das ausdrücklich:

> It binds to loopback only, and it will only command resources it was told about at construction. It can
> stop processes; it has no business being reachable from the network.

Die Begrenzung auf bekannte Ressourcennamen ist gut. Die Annahme, Loopback sei eine Grenze, gilt aber
gegenüber dem Netzwerk — **nicht gegenüber dem Browser**. Jede Webseite, die der Entwickler geöffnet hat,
kann Requests an `http://127.0.0.1:<port>/...` absetzen. Bei „einfachen" Requests (POST/DELETE ohne
Custom-Header, ohne JSON-Content-Type) gibt es keinen Preflight; die *Antwort* bleibt dem Angreifer
verborgen, die **Nebenwirkung tritt trotzdem ein**.

Alle betroffenen Endpunkte sind zustandsverändernd und benötigen weder Body noch Header:

| Endpunkt | Port | Wirkung |
|---|---|---|
| `POST /api/resources/{name}/stop` | 8200 (Control Plane) | Beendet einen Raft-Knotenprozess |
| `POST /api/resources/{name}/{command}` | 8300 (Observer, proxyt weiter) | dito |
| `DELETE /cluster/nodes/{node}` | 8101–8105 (Node) | Nimmt einen Knoten aus der Cluster-Konfiguration |
| `POST /groups/{group}/messages?size=…` | 8101–8105 | Startet Traffic-Last, siehe [SEC-006](SEC-006-ungepruefte-traffic-parameter.md) |
| `POST /cluster/groups/{group}/leader` | 8101–8105 | Erzwingt Leader-Wechsel |

Es gibt in keinem der drei Projekte eine CORS-Policy, eine Origin-/Referer-Prüfung, einen
Anti-Forgery-Token oder einen `Sec-Fetch-Site`-Check.

Ein Proof of Concept ist eine Zeile auf einer beliebigen Webseite:

```html
<img src="x" onerror="fetch('http://127.0.0.1:8101/cluster/nodes/2',{method:'DELETE',mode:'no-cors'})">
```

Hinzu kommt DNS-Rebinding: Ein Angreifer-Domain, die auf `127.0.0.1` auflöst, umgeht auch die
Same-Origin-Beschränkung beim *Lesen* der Antworten, weil keiner der Endpunkte den `Host`-Header prüft.

## Auswirkung

Auf einer Entwicklermaschine, auf der die Demo läuft: Fernsteuerung des Clusters durch jede besuchte
Webseite — Knoten stoppen, Mitgliedschaft ändern, Last erzeugen. Der Schaden bleibt auf die Demo begrenzt;
das Muster ist aber genau das, was jemand für einen echten Admin-Endpunkt kopieren würde.

## Empfehlung

1. **`Host`-Header validieren** (nur `127.0.0.1:<port>` / `localhost:<port>` akzeptieren) — die
   wirksamste Einzelmaßnahme gegen DNS-Rebinding, und in ASP.NET Core über `HostFiltering` schon vorhanden.
2. **Origin-Prüfung für zustandsverändernde Methoden.** Fehlender oder fremder `Origin`/`Sec-Fetch-Site`
   ⇒ 403. Das kostet drei Zeilen Middleware und stoppt den einfachen CSRF-Fall vollständig.
3. Eine explizite, restriktive CORS-Policy für die eine erlaubte UI-Origin (`http://localhost:4200`)
   statt gar keiner.
4. Ein beim Start erzeugtes Shared Secret, das die UI mitschickt — das macht den Kanal auch dann sicher,
   wenn die Ports später einmal nicht mehr Loopback-only sind.
5. Den Kommentar in `ControlPlane.cs` korrigieren: Loopback schützt vor dem Netz, nicht vor dem Browser.

## Verwandt

- [SEC-006](SEC-006-ungepruefte-traffic-parameter.md) — was ein einzelner CSRF-POST anrichten kann
- [SEC-009](SEC-009-uri-interpolation-im-observer-proxy.md)
