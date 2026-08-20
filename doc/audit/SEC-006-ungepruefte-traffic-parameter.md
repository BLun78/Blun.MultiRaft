# SEC-006 — Ungeprüfte `size`/`count`-Parameter: OOM und dauerhafte Blockade des Generators

- **Schweregrad:** Mittel
- **Kategorie:** Fehlende Eingabevalidierung (CWE-20) → unkontrollierte Speicherallokation (CWE-789)
- **Ort:** `demo/Blun.MultiRaft.Node/Program.cs`, `demo/Blun.MultiRaft.Node/RaftNodeHost.cs` (`StartSending`), `demo/Blun.MultiRaft.Node/MessageSender.cs` (`Start`, `RunAsync`)

## Befund

Die Route nimmt drei Zahlen ungeprüft entgegen:

```csharp
app.MapPost(
    "/groups/{group}/messages",
    (RaftNodeHost host, ulong group, int? count, int? intervalMs, int? size)
        => Results.Json(host.StartSending(group, count ?? 100, intervalMs ?? 10, size ?? 256)));
```

`StartSending` begrenzt nur nach **unten**:

```csharp
=> _sender.Start(group, count, TimeSpan.FromMilliseconds(Math.Max(1, intervalMs)), Math.Max(16, size));
```

Und `MessageSender.RunAsync` allokiert damit direkt:

```csharp
private async Task RunAsync(RaftGroupInstance instance, SendJob job)
{
    byte[] payload = new byte[job.Size];      // <- außerhalb jedes try
    ...
```

Es gibt keine Obergrenze für `size` und keine für `count`. `SegmentedRaftWalOptions.MaxPayloadBytes`
(≈ 1 MB + 16 KB) wird hier nicht konsultiert.

## Auswirkung

**1. Sofortiger OOM und *dauerhaft* blockierter Generator.**
`POST /groups/1/messages?size=2000000000` allokiert ein 2-GB-Array. Die Allokation steht **vor** dem
`try`-Block, die Methode läuft in einem Fire-and-Forget-Task (`_ = Task.Run(() => RunAsync(...))`).
Folge:

- Die `OutOfMemoryException` landet in einem unbeobachteten Task und verschwindet still.
- `job.Finish()` im `finally` wird nie erreicht, weil die Exception vor dem `try` fliegt → `job.Running`
  bleibt für immer `true`.
- Jeder weitere Start-Request für diese Gruppe wird ab jetzt mit
  `"a run is already in progress"` abgelehnt — **permanent, bis der Prozess neu startet**.

Ein einzelner Request legt den Traffic-Generator einer Gruppe also dauerhaft lahm, und der Grund ist
nirgends sichtbar.

**2. Payload jenseits des WAL-Limits.** Ein `size` zwischen `MaxPayloadBytes` und dem verfügbaren Speicher
allokiert erfolgreich und geht dann in `AppendAsync`. Der Einzel-Append-Pfad wirft dort
`ArgumentOutOfRangeException` — die wird von `catch (Exception ex)` gefangen und als `job.Error`
gemeldet, ist also verkraftbar. Über den Replikationspfad ist dieselbe Größe jedoch *nicht* geprüft, siehe
[SEC-008](SEC-008-batch-append-umgeht-payload-limit.md).

**3. `count` unbegrenzt.** `count=int.MaxValue` bei `intervalMs=1` läuft ~24 Tage. Es gibt einen
Stop-Endpunkt, also eher Ärgernis als Risiko.

Erreichbar ist das ohne Authentifizierung von jeder Webseite aus, siehe
[SEC-005](SEC-005-csrf-gegen-loopback-endpunkte.md).

## Empfehlung

1. `size` gegen `SegmentedRaftWalOptions.MaxPayloadBytes` klemmen, `count` gegen eine sinnvolle
   Obergrenze — beides in `StartSending`, wo `Math.Max` schon steht:
   ```csharp
   Math.Clamp(size, 16, SegmentedRaftWalOptions.MaxMessageBytes)
   ```
2. Bei Überschreitung mit `400 Bad Request` und einer Begründung antworten, statt still zu klemmen —
   die Demo lebt davon, dass man ihr ansieht, was sie tut.
3. Die Allokation in `RunAsync` **in** den `try`-Block ziehen, damit `job.Finish()` im `finally` in jedem
   Fall läuft und der Fehler in `job.Error` sichtbar wird.
4. Den Task-Handle behalten statt `_ =`, siehe
   [D-003](PROBLEME-demo-und-uebriges.md#d-003).

## Verwandt

- [SEC-005](SEC-005-csrf-gegen-loopback-endpunkte.md)
- [SEC-008](SEC-008-batch-append-umgeht-payload-limit.md)
- [D-003](PROBLEME-demo-und-uebriges.md#d-003)
