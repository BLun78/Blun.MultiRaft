# SEC-007 — `RaftEntryKind` und `ApplicationTag` werden abgeschnitten statt validiert

- **Schweregrad:** Mittel
- **Kategorie:** Fehlende Eingabevalidierung (CWE-20), unsichere Typkonvertierung (CWE-704)
- **Ort:** `src/Blun.MultiRaft.Grpc/RaftFrameCodec.cs` — `ToDomain(LogEntry)`

## Befund

Auf der Leitung sind `Kind` und `ApplicationTag` `uint`. Im Domänenmodell ist `RaftEntryKind` ein
`byte`-Enum mit genau drei gültigen Werten (`Command = 0`, `NoOp = 1`, `Membership = 2`), und
`ApplicationTag` ist ein `byte`. Der Codec konvertiert per unchecked Cast:

```csharp
public static RaftLogEntry ToDomain(LogEntry entry)
{
    byte[] payload = entry.Payload.ToByteArray();
    return new RaftLogEntry(
        new RaftEntryHeader(
            entry.Term,
            entry.Index,
            (RaftEntryKind)entry.Kind,          // uint -> byte-Enum, schneidet ab
            payload.Length,
            entry.TimestampTicks,
            (byte)entry.ApplicationTag),        // uint -> byte, schneidet ab
        payload);
}
```

Beide Casts sind verlustbehaftet und ungeprüft. Es gibt keine `Enum.IsDefined`-Prüfung und keine
Bereichsprüfung.

## Auswirkung

**1. Ein `Command` kann sich in eine `Membership`-Änderung verwandeln.**
`Kind = 258` ergibt `258 & 0xFF = 2` = `RaftEntryKind.Membership`. Ein Eintrag, den der Absender als
Anwendungskommando gemeint hat, wird beim Empfänger als Konfigurationsänderung interpretiert und mit
seiner Payload als `MembershipChangePayload` geparst. Das trifft die Voter-Menge — inklusive der
Cluster-Gruppe.

Das ist auch ohne Angreifer relevant: Es macht einen Protokoll-Versionsfehler (ein neuerer Peer sendet
einen neuen `Kind`-Wert) zu einer stillen Zustandsbeschädigung statt zu einer klaren Ablehnung. Genau
diese Sorte „stiller Fehler mit korrekten Headern" hat dieses Projekt laut `CLAUDE.md` schon einmal
Tage gekostet.

**2. Undefinierte Enum-Werte gelangen in den Log.** `Kind = 3..255` erzeugt einen `RaftEntryKind`, den
kein `switch` kennt. Der Eintrag wird persistiert und bei jedem Replay erneut ausgewertet.

**3. `ApplicationTag` verliert stillschweigend Information.** Wert 256 wird zu 0. Das Feld ist laut
Dokumentation ein Prioritäts- bzw. Fälligkeits-Bucket, aus dem ein Index rekonstruiert wird — eine
Fehlzuordnung ist hier kein kosmetischer Schaden.

## Bewertung

Für sich genommen ist das ein Robustheitsproblem; in Kombination mit
[SEC-001](SEC-001-raft-transport-ohne-authentifizierung.md) ist es ein zusätzlicher Angriffspfad. Ich
führe es getrennt, weil es auch nach der Behebung von SEC-001 bestehen bleibt: ein *authentifizierter*
Peer mit abweichender Version löst es genauso aus.

## Empfehlung

```csharp
if (entry.Kind > (uint)RaftEntryKind.Membership)
{
    throw new InvalidOperationException($"Unknown log entry kind {entry.Kind}.");
}

if (entry.ApplicationTag > byte.MaxValue)
{
    throw new InvalidOperationException($"Application tag {entry.ApplicationTag} does not fit a byte.");
}
```

`InvalidOperationException` ist hier der richtige Typ: Laut Konvention des Projekts steht er für eine
Integritätsverletzung, und genau das ist ein Eintrag, dessen Art nicht bekannt ist. Der Aufrufer in
`RaftStreamSession.HandleRequestAsync` fängt ihn bereits und protokolliert ihn über
`GrpcLog.RequestFailed`, statt die Session abzureißen — das gewünschte Verhalten.

Zusätzlich sollte erwogen werden, `Kind` in der `.proto` als `enum` statt `uint32` zu deklarieren; das
verschiebt zwar nur die Verantwortung, dokumentiert aber den Wertebereich auf der Leitung.

## Verwandt

- [SEC-001](SEC-001-raft-transport-ohne-authentifizierung.md)
- [SEC-008](SEC-008-batch-append-umgeht-payload-limit.md)
