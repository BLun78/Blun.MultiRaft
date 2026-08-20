# Demo, Tests, Benchmarks, Build und CI — Probleme

Dritter Durchgang: alles außerhalb von `src/`. Gelesen wurden `demo/Blun.MultiRaft.Node`,
`demo/Blun.MultiRaft.AppHost`, `demo/Blun.MultiRaft.Observer`, `demo/Blun.MultiRaft.Observer.Ui`
(Konfiguration), `demo/chaos`, `benchmark/`, `test/`, die Projektdateien, `Directory.Build.props`,
`Directory.Packages.props`, `.gitignore` und `.github/workflows/ci.yml`.

Sicherheitsbefunde der Demo stehen separat als [SEC-005](SEC-005-csrf-gegen-loopback-endpunkte.md),
[SEC-006](SEC-006-ungepruefte-traffic-parameter.md) und
[SEC-009](SEC-009-uri-interpolation-im-observer-proxy.md).

| ID | Schwere | Kurzfassung |
|---|---|---|
| [D-001](#d-001) | **Hoch** | `.gitignore` deckt `node_modules/` und `dist/` nicht ab |
| [D-002](#d-002) | **Hoch** | `net11.0` wird nirgends getestet — und trägt dort einen experimentellen Schalter |
| [D-003](#d-003) | Mittel | `MessageSender`: CTS wird unter dem laufenden Task entsorgt |
| [D-004](#d-004) | Mittel | Die Tests hängen an der Wanduhr, obwohl die Bibliothek einen `TimeProvider` anbietet |
| [D-005](#d-005) | Mittel | `LogStream.PumpAsync` fängt `JsonException` nicht — der SSE-Endpunkt bricht ab |
| [D-006](#d-006) | Niedrig | `ClusterWatcher.PollAsync` wirft bei doppelter Node-ID |
| [D-007](#d-007) | Mittel | CI: keine `permissions`, keine gepinnten Actions |
| [D-008](#d-008) | Niedrig | `Blun.MultiRaft.Grpc` ist als einziges `src/`-Projekt nicht trim-/AOT-geprüft |
| [D-009](#d-009) | Niedrig | Ungeprüfte Konfigurationswerte beim Start |
| [D-010](#d-010) | Niedrig | `.mcp.json` startet `npx -y …@latest` |
| [D-011](#d-011) | Niedrig | Observer-Proxy-Endpunkte antworten mit 500 statt mit einer Aussage |

---

<a id="d-001"></a>
## D-001 — `.gitignore` deckt `node_modules/` und `dist/` nicht ab

**Schwere:** Hoch (Repository-Hygiene) · **Ort:** `.gitignore`

Die Datei kennt `bin/`, `obj/`, `TestResults/`, `BenchmarkDotNet.Artifacts/`, IDE- und OS-Dateien sowie
`demo/**/data/`. Sie kennt **nicht**:

```
node_modules/
dist/
```

`demo/Blun.MultiRaft.Observer.Ui/` steht derzeit als untracked im Arbeitsverzeichnis und enthält beides —
`node_modules` mit 238 Paketen der obersten Ebene und ein gebautes `dist/observer-ui`. Ein
`git add demo/` würde das alles aufnehmen.

Das ist nicht nur unschön:

- Zehntausende Dateien im Verlauf, die sich nie sauber wieder entfernen lassen.
- Committete `node_modules` sind ein reales Lieferketten-Risiko: Was einmal drin liegt, wird bei jedem
  Checkout ausgeliefert und bei einem `npm audit`-Durchlauf nicht mehr erfasst; `package-lock.json`
  existiert bereits und erfüllt den Zweck korrekt.
- `dist/` ist Build-Ausgabe und gehört aus demselben Grund nicht ins Repository wie `bin/`.

Positiv: `demo/chaos/` bringt eine eigene `.gitignore` mit. Die Regel fehlt also nur oben.

**Behebung:** Ergänzen:

```gitignore
## Node
node_modules/
dist/
npm-debug.log*
.angular/
```

`.angular/` ist der Cache des Angular-Builds und wächst ebenfalls stark.

---

<a id="d-002"></a>
## D-002 — `net11.0` wird nirgends getestet, trägt dort aber einen experimentellen Schalter

**Schwere:** Hoch · **Ort:** `test/Blun.MultiRaft.Tests/Blun.MultiRaft.Tests.csproj`,
`.github/workflows/ci.yml`, `src/Blun.MultiRaft/Blun.MultiRaft.csproj`

Alle drei Bibliotheken zielen auf zwei Frameworks:

```xml
<TargetFrameworks>net10.0;net11.0</TargetFrameworks>
```

Das Testprojekt auf eines:

```xml
<TargetFramework>net10.0</TargetFramework>
```

und die CI führt konsequenterweise nur das aus:

```yaml
run: dotnet exec test/Blun.MultiRaft.Tests/bin/Release/net10.0/Blun.MultiRaft.Tests.dll -result-trx TestResults/results.trx
```

`net11.0` wird also gebaut und nie ausgeführt — auf keiner der drei Plattformen.

Das wäre für sich schon eine Lücke. Entscheidend ist aber, was dort zusätzlich aktiv ist:

```xml
<!-- net11-only switches live here, never unconditionally. -->
<PropertyGroup Condition="'$(TargetFramework)' == 'net11.0'">
  <Features>$(Features);runtime-async=on</Features>
</PropertyGroup>
```

`runtime-async` ändert, wie asynchrone Methoden von der Laufzeit ausgeführt werden. Genau das Ziel, dessen
Async-Verhalten von einem experimentellen Schalter abhängt, ist das ungetestete. Für eine Bibliothek, deren
Fehlerklasse laut `CLAUDE.md` „intermittent rather than deterministic" ist und die durchgehend auf
`SemaphoreSlim`, `Channel`, `ValueTask` und Fire-and-forget-Tasks aufbaut, ist das die riskanteste Lücke
im Testaufbau.

Auch die Benchmarks bestätigen die Absicht, beide Laufzeiten zu vergleichen (`WalBenchmarks` zielt auf
beide; `CLAUDE.md` schreibt sogar vor, immer beide in einem Lauf zu messen) — nur die Tests bleiben außen
vor.

**Behebung:**

1. `<TargetFrameworks>net10.0;net11.0</TargetFrameworks>` im Testprojekt.
2. Die CI-Testschritte für beide Ausgaben ausführen:
   ```yaml
   - name: Test (net10.0)
     run: dotnet exec test/Blun.MultiRaft.Tests/bin/Release/net10.0/Blun.MultiRaft.Tests.dll -result-trx TestResults/net10.trx
   - name: Test (net11.0)
     run: dotnet exec test/Blun.MultiRaft.Tests/bin/Release/net11.0/Blun.MultiRaft.Tests.dll -result-trx TestResults/net11.trx
   ```
3. Falls `net11.0` bewusst noch nicht getestet werden soll, gehört diese Entscheidung samt Begründung in
   `CLAUDE.md` — dort steht sie heute nicht, und die Multi-Targeting-Zeile suggeriert das Gegenteil.

---

<a id="d-003"></a>
## D-003 — `MessageSender`: die CTS wird unter dem laufenden Task entsorgt

**Schwere:** Mittel · **Ort:** `demo/Blun.MultiRaft.Node/MessageSender.cs`

```csharp
_ = Task.Run(() => RunAsync(instance, job), CancellationToken.None);
```

Der Task wird nirgends festgehalten. `DisposeAsync` bricht ab und entsorgt sofort:

```csharp
foreach (SendJob job in _jobs.Values)
{
    await job.Cancellation.CancelAsync().ConfigureAwait(false);
    job.Cancellation.Dispose();          // der Lauf kann noch mittendrin sein
}
```

`RunAsync` greift danach weiter auf `job.Cancellation.Token` zu. Nach `Dispose()` wirft diese Eigenschaft
`ObjectDisposedException`. Die Stelle entscheidet, was passiert:

```csharp
try { await instance.AppendAsync(payload, 1, job.Cancellation.Token) … }   // innerer try: gefangen
catch (Exception ex) { job.Fail(); job.Error = ex.Message; break; }

await timer.WaitForNextTickAsync(job.Cancellation.Token).ConfigureAwait(false);   // außerhalb: nicht gefangen
```

Der äußere `try` fängt nur `OperationCanceledException`. Die `ObjectDisposedException` verlässt also
`RunAsync` und landet in einem unbeobachteten Task.

Zusätzlich hebelt es eine Zusage auf, die `RaftNodeHost.DisposeAsync` ausdrücklich macht:

> Before the host: a run still appending into a group being torn down would spend its last moments logging
> failures about it.

Die Reihenfolge stimmt, aber ohne `await` auf den Lauf-Task ist sie wirkungslos — der Generator kann noch
in die Gruppe schreiben, während der Host sie abbaut.

*Nebenbefund:* `Start` überschreibt `_jobs[group] = job`, ohne die `CancellationTokenSource` des
vorherigen Laufs zu entsorgen. Ein CTS ohne Registrierungen ist zwar GC-fähig, aber die Symmetrie fehlt.

**Behebung:**

1. Den Task im `SendJob` mitführen (`public Task? Runner { get; set; }`) und in `DisposeAsync` **erst**
   abwarten, **dann** entsorgen.
2. `WaitForNextTickAsync` in den inneren `try` ziehen oder den äußeren `catch` auf
   `ObjectDisposedException` erweitern.
3. Die Allokation `new byte[job.Size]` in den `try` ziehen, siehe
   [SEC-006](SEC-006-ungepruefte-traffic-parameter.md).

---

<a id="d-004"></a>
## D-004 — Die Tests hängen an der Wanduhr, obwohl die Bibliothek einen `TimeProvider` anbietet

**Schwere:** Mittel · **Ort:** `test/Blun.MultiRaft.Tests/*`

`RaftGroupInstance`, `MultiRaftHost` und `ClusterCoordinator` nehmen alle einen `TimeProvider` entgegen und
benutzen ihn konsequent (`_time.GetTimestamp()`, `new PeriodicTimer(_tickInterval, _time)`). Die Bibliothek
hat sich also die Voraussetzung für deterministische Zeit-Tests selbst geschaffen.

Im Testprojekt kommt `TimeProvider` **kein einziges Mal** vor. Stattdessen echte Wartezeiten:

```csharp
await Task.Delay(500);                       // AutoCompactionTests.cs:81, :115
await Task.Delay(TimeSpan.FromSeconds(5));   // ClusterCoordinatorTests.cs:254
await Task.Delay(10);                        // Polling-Schleifen in TestCluster/ClusterTestCluster
ElectionTimeout = TimeSpan.FromMilliseconds(120)
```

Zwei Konsequenzen:

1. **Flakiness.** Eine Wahl mit 120 ms Timeout gegen eine ausgelastete CI-VM (die Matrix fährt drei
   Betriebssysteme parallel) ist ein Rennen. `CLAUDE.md` hält selbst fest, dass die Fehler dieses Projekts
   „tend to be intermittent rather than deterministic failures" — die Testbasis verstärkt genau das.
2. **Laufzeit.** Ein einzelnes `Task.Delay(5s)` plus die 500-ms-Wartezeiten summieren sich, obwohl die
   Bibliothek mit `FakeTimeProvider` in Millisekunden durch dieselben Szenarien liefe.

`Microsoft.Extensions.TimeProvider.Testing` bringt `FakeTimeProvider` mit; das Paket fehlt in
`Directory.Packages.props`.

**Behebung:** `FakeTimeProvider` in `TestCluster`/`ClusterTestCluster` einziehen und die Wartezeiten durch
`Advance(...)` ersetzen. Wo echte Nebenläufigkeit gebraucht wird (der gRPC-Transport-Test etwa), sind
reale Wartezeiten in Ordnung — aber dann als Timeout mit Abbruchbedingung, nicht als fester Schlaf.

---

<a id="d-005"></a>
## D-005 — `LogStream.PumpAsync` fängt `JsonException` nicht

**Schwere:** Mittel · **Ort:** `demo/Blun.MultiRaft.Observer/LogStream.cs`

```csharp
LogLine? parsed = JsonSerializer.Deserialize(line[6..], ObserverJson.Default.LogLine);
```

Die Catch-Liste der Schleife deckt `OperationCanceledException`, `HttpRequestException` und `IOException`
ab — `JsonException` nicht.

Ein abgeschnittener SSE-Frame genügt: Bricht die Verbindung zur Control Plane mitten in einer Zeile ab,
liefert `ReadLineAsync` am Stromende den unvollständigen Rest als Zeile zurück. Beginnt der mit `data: `,
läuft er in `Deserialize` und wirft.

Der Pump-Task faultet dann, und `MergeAsync` reißt ihn im `finally` wieder hoch:

```csharp
try { await Task.WhenAll(pumps).ConfigureAwait(false); }
catch (OperationCanceledException) { … }      // JsonException nicht abgedeckt
```

Die Exception verlässt also den SSE-Endpunkt — und zwar aus dem `finally` heraus, wodurch sie eine
eventuell darunterliegende, eigentliche Ursache überdeckt. Für den Benutzer sieht das aus wie ein
Log-Fenster, das ohne Grund stehen bleibt.

Dass ausgerechnet dieser eine Stream ausfällt, ist besonders unangenehm: Er trägt laut eigener
Dokumentation die Konsolen **aller fünf Knoten**, weil eine Verbindung pro Knoten die Sechs-Verbindungs-
Grenze des Browsers sprengen würde.

**Behebung:** `catch (JsonException ex)` mit derselben Behandlung wie `HttpRequestException` ergänzen
(`ObserverLog.LogStreamDropped`, dann Reconnect), und im `finally` von `MergeAsync` breiter fangen.

---

<a id="d-006"></a>
## D-006 — `ClusterWatcher.PollAsync` wirft bei doppelter Node-ID

**Schwere:** Niedrig · **Ort:** `demo/Blun.MultiRaft.Observer/ClusterWatcher.cs`

```csharp
Dictionary<ulong, ResourceState> states = resources.Result.ToDictionary(r => r.Node);
```

`ToDictionary` wirft `ArgumentException`, sobald zwei Einträge dieselbe `Node` melden. Die Control Plane
baut ihre Antwort heute aus `options.Nodes`, also aus einer Liste ohne Duplikate — die Annahme ist aber
nirgends festgehalten und wird beim ersten Konfigurationsfehler verletzt.

Immerhin: Der äußere `catch (Exception ex)` in `ExecuteAsync` fängt es ab und protokolliert es, der
Watcher überlebt also. Sichtbar wird es als Cluster-Ansicht, die stehen bleibt und deren Grund nur im Log
steht.

**Behebung:** `ToDictionary(r => r.Node)` durch eine duplikatstolerante Variante ersetzen:

```csharp
Dictionary<ulong, ResourceState> states = [];
foreach (ResourceState state in resources.Result)
{
    states[state.Node] = state;
}
```

---

<a id="d-007"></a>
## D-007 — CI: keine `permissions`, keine gepinnten Actions

**Schwere:** Mittel (Lieferkette) · **Ort:** `.github/workflows/ci.yml`

Zwei Härtungslücken:

**(a) Kein `permissions`-Block.** Ohne ihn erbt `GITHUB_TOKEN` die Repository-Voreinstellung, die je nach
Organisation Schreibrechte einschließt. Der Workflow braucht nur Lesezugriff.

```yaml
permissions:
  contents: read
```

**(b) Actions nicht auf Commit-SHA gepinnt.**

```yaml
- uses: actions/checkout@v4
- uses: actions/setup-dotnet@v4
- uses: actions/upload-artifact@v4
```

Ein Tag ist verschiebbar. Wird ein Action-Repository kompromittiert und der Tag umgesetzt, führt dieser
Workflow den neuen Code aus. Für Workflows, die auf `pull_request` laufen, ist das der klassische Einstieg.

```yaml
- uses: actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683 # v4.2.2
```

**(c) Ergänzend:** Es gibt keine NuGet-Lock-Dateien und kein `--locked-mode` beim Restore. Die zentrale
Paketverwaltung (`Directory.Packages.props`) fixiert die direkten Versionen, aber
`CentralPackageTransitivePinningEnabled` deckt nur, was auch aufgelistet ist. Der Umgang mit dem
`MessagePack`-Advisory zeigt, dass hier bewusst gearbeitet wird — ein `packages.lock.json` wäre der
nächste Schritt.

**Positiv anzumerken:** Die Matrix über alle drei Betriebssysteme, die begründete Umgehung des
`dotnet test`-VSTest-Bridge und die Kommentare zum `MessagePack`-Pin sind vorbildlich und sollten so
bleiben.

---

<a id="d-008"></a>
## D-008 — `Blun.MultiRaft.Grpc` ist als einziges `src/`-Projekt nicht trim-/AOT-geprüft

**Schwere:** Niedrig · **Ort:** `src/Blun.MultiRaft.Grpc/Blun.MultiRaft.Grpc.csproj`

`CLAUDE.md` formuliert die Regel ohne Einschränkung:

> Code must be trim-safe and AOT-compatible.

`src/Blun.MultiRaft` und `src/Blun.MultiRaft.Wal` setzen entsprechend:

```xml
<IsAotCompatible>true</IsAotCompatible>
<IsTrimmable>true</IsTrimmable>
```

`src/Blun.MultiRaft.Grpc` setzt beides **nicht**. Damit laufen dort weder der Trim- noch der
AOT-Analyzer, und ein `RequiresUnreferencedCode`-Verstoß fällt beim Bauen nicht auf — obwohl
`TreatWarningsAsErrors` sonst überall greift.

Bemerkenswert ist, dass `demo/Blun.MultiRaft.Observer` es setzt und dabei sogar kommentiert, warum die
Minimal-API-Bindung sonst per Reflection liefe. Das Bewusstsein ist also da; ausgerechnet die
Transport-Bibliothek fehlt.

**Behebung:** Beide Eigenschaften ergänzen und den Build sprechen lassen. Falls Grpc.AspNetCore Warnungen
erzeugt, die nicht auflösbar sind, gehört das als begründetes `NoWarn` mit Kommentar dokumentiert —
so, wie es dieses Repository an anderen Stellen tut.

---

<a id="d-009"></a>
## D-009 — Ungeprüfte Konfigurationswerte beim Start

**Schwere:** Niedrig · **Ort:** `demo/Blun.MultiRaft.Node/Program.cs`,
`demo/Blun.MultiRaft.Observer/Program.cs`

```csharp
var self = new NodeId(ulong.Parse(builder.Configuration["RAFT_NODE_ID"] ?? "1", …));
int raftPort   = int.Parse(builder.Configuration["RAFT_PORT"] ?? "7101", …);
int statusPort = int.Parse(builder.Configuration["RAFT_STATUS_PORT"] ?? "8101", …);
peers[new NodeId(id)] = new Uri(setting.Value);
int port = int.Parse(builder.Configuration["OBSERVER_PORT"] ?? "8300", …);
var controlPlane = new Uri(builder.Configuration["OBSERVER_CONTROL_PLANE"] ?? "http://127.0.0.1:8200");
```

Jeder dieser Aufrufe wirft bei einem Tippfehler eine `FormatException` bzw. `UriFormatException` aus dem
Top-Level-Programm — ohne Kontext, welche Variable gemeint war. Ein Port außerhalb 1–65535 fällt erst
Kestrel auf.

Das steht im Kontrast zum Rest der Demo: `ObservedNode.Parse` behandelt genau diese Frage ausdrücklich
und begründet die Entscheidung sauber:

> Anything malformed is dropped rather than thrown on: the observer's job is to report, and a demo that
> refuses to start because one entry was mistyped reports nothing at all.

Hier ist Abbrechen die richtige Wahl (ein Knoten mit falscher ID darf nicht starten) — aber dann mit einer
Meldung, die den Variablennamen nennt.

**Behebung:** `TryParse` mit einer eigenen Fehlermeldung, oder `builder.Configuration.GetValue<int>` mit
anschließender Bereichsprüfung. Vier Zeilen, die einen Konfigurationsfehler von einer Stack-Trace in einen
Satz verwandeln.

---

<a id="d-010"></a>
## D-010 — `.mcp.json` startet `npx -y …@latest`

**Schwere:** Niedrig (Lieferkette, Entwicklungsumgebung) · **Ort:** `.mcp.json`

```json
{
  "mcpServers": {
    "playwright": {
      "command": "npx",
      "args": ["-y", "@playwright/mcp@latest"]
    }
  }
}
```

Die Datei ist derzeit untracked. Wird sie eingecheckt, führt jede Entwicklungsumgebung, die sie liest, beim
Start automatisch `npx -y @playwright/mcp@latest` aus — also: neueste Version auflösen, ohne Rückfrage
installieren, ausführen. Kein Pin, keine Integritätsprüfung, keine Bestätigung.

Dasselbe Muster steht in `demo/chaos/package.json` ordentlicher da (`"playwright": "^1.58.0"` als
`devDependency` mit `package-lock.json` daneben).

**Behebung:** Entweder auf eine konkrete Version pinnen (`@playwright/mcp@1.58.0`), oder die Datei in
`.gitignore` aufnehmen, wenn sie eine rein persönliche Werkzeugkonfiguration bleiben soll. Beides ist
vertretbar — der Zwischenzustand „eingecheckt und `@latest`" ist es nicht.

---

<a id="d-011"></a>
## D-011 — Observer-Proxy-Endpunkte antworten mit 500 statt mit einer Aussage

**Schwere:** Niedrig · **Ort:** `demo/Blun.MultiRaft.Observer/Program.cs`

```csharp
app.MapGet("/api/resources", async (IHttpClientFactory clients, ObserverOptions settings, CancellationToken token) =>
{
    ResourceState[] states = await clients
        .CreateClient("node")
        .GetFromJsonAsync(new Uri(settings.ControlPlane, "/api/resources"), ObserverJson.Default.ResourceStateArray, token)
        .ConfigureAwait(false) ?? [];

    return Results.Json(states, ObserverJson.Default.ResourceStateArray);
});
```

Ist die Control Plane nicht erreichbar — sie startet im selben Prozess wie der App-Host und ist beim ersten
Aufruf der UI eventuell noch nicht so weit —, wirft `GetFromJsonAsync` eine `HttpRequestException`, und der
Endpunkt antwortet mit 500 und einer leeren Seite.

`ClusterWatcher.PollResourcesAsync` behandelt exakt denselben Fall exakt richtig, mit Begründung:

> The app host is the one process the observer cannot demand anything of: it may still be starting, and the
> cluster view does not depend on it.

Der proxyende Endpunkt hat diese Behandlung nicht bekommen. Dasselbe gilt für `POST /api/resources/{…}`,
dessen `PostAsync` ebenfalls ungeschützt ist.

**Behebung:** Denselben `catch (HttpRequestException)` einziehen und mit `503` plus einer kurzen Meldung
antworten, statt mit einem nackten 500 — die UI zeigt dann „der App-Host antwortet nicht" statt gar nichts.

---

## Was ich geprüft und *nicht* beanstandet habe

- **`AppHost.cs`** — feste Ports mit begründetem Verzicht auf Service Discovery, sauberer
  `NodeJsIsAvailable`-Test ohne Prozessstart, korrekte Trennung der Datenverzeichnisse pro Knoten.
- **`LogStream`s Ein-Verbindungs-Entwurf** — die Sechs-Verbindungs-Grenze des Browsers ist real, die
  Pumps werden ordentlich verfolgt und im `finally` abgewartet, und der `Reset`-Frame beim Reconnect
  verhindert doppelte Historie.
- **`ClusterWatcher.WatchAsync`** — ein Ein-Element-Mailbox pro Abonnent mit `DropOldest`; ein langsamer
  Browser fällt in die Gegenwart zurück statt in einen Rückstau. Korrekt, und die Registrierung wird im
  `finally` wieder entfernt.
- **`Sse.Begin` + Flush vor dem ersten Payload** — an beiden Stellen (`Sse.cs`, `ControlPlane.cs`)
  vorhanden, was den „hängender Request statt offener Stream"-Fehler ausschließt.
- **Gruppen-IDs als Strings zum Browser** — durchgehend eingehalten, `ulong.MaxValue` überlebt die
  JavaScript-Zahl.
- **`ControlPlaneService.Resolve`** — echte Allowlist über die bei der Konstruktion übergebenen Namen.
- **Resource-Kommandos ohne Request-Token** (`CancellationToken.None`) — richtig, und begründet.
- **`Directory.Packages.props`** — zentrale Versionsverwaltung, transitives Pinning aktiv, und der
  `MessagePack`-Vorwärts-Pin ist genau die richtige Reaktion auf ein Advisory.
- **`demo/chaos/`** — eigenständige `.gitignore`, gepinnte `devDependency`, `package-lock.json` vorhanden.
- **Der gRPC-Benchmark** — misst ehrlich, schließt den Verbindungsaufbau begründet aus und dokumentiert,
  dass das Ergebnis die ursprüngliche Annahme *nicht* bestätigt. Dass das so im Repository steht, ist ein
  Qualitätsmerkmal.
