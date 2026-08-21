# Using `Blun.MultiRaft.Wal`

This is the write-ahead log on its own — `IRaftWal` and its two implementations. It has no dependency on
consensus and no dependency on gRPC; you can reference `Blun.MultiRaft.Wal` by itself if all you want is a
segmented, checksummed, crash-safe append log. (`RaftGroupInstance` — see [raft-usage.md](raft-usage.md) —
is the only thing in this repository that drives it as part of something bigger.)

```
dotnet add package Blun.MultiRaft.Wal
```

## The interface

```csharp
using Blun.MultiRaft;
using Blun.MultiRaft.Wal;

IRaftWal wal = await SegmentedRaftWal.OpenAsync("./data/queue-42/wal");

var header = new RaftEntryHeader(
    term: 1,
    index: wal.LastIndex + 1,
    kind: RaftEntryKind.Command,
    payloadLength: payload.Length,
    timestampTicks: DateTime.UtcNow.Ticks);

long index = await wal.AppendAsync(header, payload);
await wal.FlushAsync(); // fsync -- see "Durability" below

await foreach (RaftLogEntry entry in wal.ReadFromAsync(fromIndex: 1, toIndexInclusive: wal.LastIndex))
{
    Consume(entry.Payload.Span); // valid only until the next MoveNextAsync -- copy if you need to keep it
}

await wal.DisposeAsync();
```

Two things about the shape of this API that are easy to miss:

- **The index space is dense and 1-based.** `AppendAsync(RaftEntryHeader, ...)` requires `header.Index` to be
  exactly `LastIndex + 1`; anything else throws `InvalidOperationException`. There is no gap-filling and no
  auto-numbering — the caller always knows and states the index it is writing.
- **`ReadFromAsync`'s payloads alias a buffer the log owns and recycles.** They are only valid until the next
  `MoveNextAsync()`. If you need one past that point, call `entry.ToOwned()` to get a copy, or copy the span
  yourself.

## `ApplicationTag`, and rebuilding an index without the payload

`RaftEntryHeader` carries one byte, `ApplicationTag`, that this library never reads or interprets — it exists
for whatever a caller layers on top of a plain command log. The motivating case is a message queue: a
priority-ordered queue needs a bucket index (which offsets are priority 7, which are priority 2) rebuilt on
every restart and every leader takeover, and a due-time-ordered scheduled-message queue needs the equivalent
keyed by a due-time bucket instead. Neither actually needs the payload to do that — just the tag and the
index it belongs to.

```csharp
var header = new RaftEntryHeader(
    term: 1,
    index: wal.LastIndex + 1,
    kind: RaftEntryKind.Command,
    payloadLength: payload.Length,
    timestampTicks: DateTime.UtcNow.Ticks,
    applicationTag: priorityBucket); // e.g. 0-9; opaque to this library either way

long index = await wal.AppendAsync(header, payload);
```

`ReadHeadersFromAsync` reads just the fixed 32-byte header per entry — not the payload or the checksum that
follows it on disk — which is what makes an index rebuild proportional to entry count rather than to the
average payload size:

```csharp
var priorityBuckets = new Dictionary<byte, List<long>>();
await foreach (RaftEntryHeader header in wal.ReadHeadersFromAsync(wal.FirstIndex, wal.LastIndex))
{
    (priorityBuckets.TryGetValue(header.ApplicationTag, out List<long>? bucket)
        ? bucket
        : priorityBuckets[header.ApplicationTag] = []).Add(header.Index);
}
```

The trade-off this makes: a header-only read cannot verify the checksum, which covers the payload too, so a
header handed out here may belong to an entry whose payload turns out to be corrupt. That's fine for building
an index — the corruption still surfaces the moment something actually tries to deliver that entry through
`ReadFromAsync`, which verifies the whole record before yielding it. Building an index isn't delivering a
message, so it doesn't need the same guarantee.

If a group is replicated, `ApplicationTag` travels with `AppendEntries` like the rest of the header — a
follower's own index rebuild sees the same tag the leader wrote, not just whatever it can infer from the
payload on its own.

## Choosing an implementation

| | `InMemoryRaftWal` | `SegmentedRaftWal` |
|---|---|---|
| Survives a process restart | No | Yes |
| Where state lives | An immutable snapshot (array + offset/count), swapped on write | Segment files on disk |
| When to use it | Tests, transient/consumer-local groups | Anything whose durability you'd actually claim |

`InMemoryRaftWal`'s reads (`ReadFromAsync`, `ReadHeadersFromAsync`, `TermAtAsync`, `LastIndex`, `LastTerm`,
`FirstIndex`) never take a lock. Writers (appends and truncations) are serialized against each other by an
internal gate — Raft only ever has one writer in flight per group anyway — and each write builds a new,
fully-populated snapshot before publishing it with a single volatile write. A concurrent reader therefore
either sees the snapshot from just before the write or just after, never a torn mix, without contending with
the writer for a lock. Appending is O(1) amortized (new entries land in unused capacity of the same backing
array before the snapshot publishing the higher count goes out); head truncation is O(1) too (it just moves
the snapshot's offset/first-index forward instead of shifting elements).

```csharp
IRaftWal volatileLog = new InMemoryRaftWal();

IRaftWal durableLog = await SegmentedRaftWal.OpenAsync(
    "./data/queue-42/wal",
    new SegmentedRaftWalOptions { /* see below */ });
```

`SegmentedRaftWal.OpenAsync` creates the directory if it doesn't exist, and replays whatever is already
there — segments, the compaction boundary, the checksum algorithm marker — if it does. Re-opening the same
directory resumes exactly where the log left off; there is no separate "recover" step to call.

## Options that matter

```csharp
var options = new SegmentedRaftWalOptions
{
    SegmentSizeBytes = 32 * 1024 * 1024,          // default: a few dozen max-size messages per segment
    FlushToDisk = true,                            // false only for tests/benchmarks -- see "Durability"
    SegmentAccess = WalSegmentAccess.RandomAccess,  // or .MemoryMapped for hot queues -- see below
    ChecksumAlgorithm = WalChecksumAlgorithm.XxHash3,
    MaxPayloadBytes = SegmentedRaftWalOptions.MaxMessageBytes, // default; the envelope headroom is added on top
    ScratchBufferBytes = 8 * 1024,
};
```

- **`ChecksumAlgorithm`** is fixed for the life of a log directory. It's written to a small `wal.cfg` file
  the first time anything is appended and checked on every subsequent open; opening an existing log with a
  *different* algorithm throws `InvalidOperationException` rather than silently misreading old records.
  Pick once, per queue, and leave it — there's no in-place re-checksum.
- **`SegmentAccess`**: `RandomAccess` (default) issues one write syscall per append or batch.
  `MemoryMapped` preallocates the active segment and appends by copying into a mapped view — dramatically
  cheaper per append (see the benchmark numbers in the top-level README), but it reserves address space and
  page-table entries for as long as that segment is being written. With one log per queue, that cost is
  charged per group — turn it on for the specific queues that are actually hot, not as a blanket default.
- **`MaxPayloadBytes`** defaults to 1024 KB (`MaxMessageBytes`) — that's the net message size a caller can
  hand to `AppendAsync`. The actual on-disk record cap (`EffectiveMaxPayloadBytes`) is `MaxPayloadBytes` plus
  `EnvelopeHeadroomBytes` (16 KB), room for whatever wraps the message on its way into a log record. That
  effective cap is a hard ceiling — `AppendAsync` throws `ArgumentOutOfRangeException` above it — and it also
  bounds how large a buffer the recovery scan will ever trust a corrupted length field to ask for.
- **`ScratchBufferBytes`** is *not* sized to `MaxPayloadBytes`. It's the inline buffer used for ordinary
  appends (default 8 KB); anything larger transparently borrows from `ArrayPool<byte>.Shared` instead. Don't
  raise this to match your largest expected message — that cost is paid once per log, and with thousands of
  queues it adds up fast for a case that's rare by construction.

## Batching

```csharp
RaftLogEntry[] batch =
[
    new RaftLogEntry(term: 1, index: 10, RaftEntryKind.Command, payloadA),
    new RaftLogEntry(term: 1, index: 11, RaftEntryKind.Command, payloadB),
];

long lastIndex = await wal.AppendAsync(batch);
```

A batch of entries that fit in one segment is written as a single I/O operation. If a batch would straddle a
segment boundary, it's split there transparently — you don't need to pre-chunk it yourself. Batching is worth
roughly an order of magnitude in the unmapped (`RandomAccess`) mode; with `MemoryMapped` the gap mostly closes
(see the README's Measurements section), because the syscall batching amortizes was the whole benefit.

## Reconciling a diverging log

```csharp
await wal.TruncateTailAsync(fromIndex: 15); // discards index 15 and everything after it
```

This is the operation a Raft follower needs when its log diverges from the leader's (Raft §5.3) — an
append-only store that can only truncate its *head* can't do this, which is why `IRaftWal` has both
directions. The freed indices are immediately reusable: appending at index 15 again afterward is not an
error.

## Compaction

```csharp
await wal.TruncateHeadAsync(uptoIndexInclusive: 900); // discards everything at or below 900
```

Whole segments only — a partial head truncation would mean rewriting a file, which is exactly the write
amplification a segmented log exists to avoid. `FirstIndex` moves forward to the start of the oldest segment
that survives.

## Snapshots: replacing the log wholesale

`TruncateHeadAsync` throws away a *prefix* the log already has entries for. `ResetToSnapshotAsync` is for the
other case — a replica that's installing a snapshot has no entries at all for the range the snapshot covers,
and needs the log to simply resume just past it:

```csharp
await wal.ResetToSnapshotAsync(lastIncludedIndex: 900, lastIncludedTerm: 4);
// wal.LastIndex == 900, wal.LastTerm == 4, wal.FirstIndex == 901
```

Both operations persist the boundary index and term durably (`SegmentedRaftWal` writes it to a small
`log.base` file, fsynced). That boundary has to survive a restart: the very next `AppendEntries` a replica
receives runs its consistency check against exactly that index, and a log that had forgotten the term at its
own starting point would reject every entry after it, permanently.

`IRaftSnapshotStore` (also in this package — `InMemoryRaftSnapshotStore` and `FileRaftSnapshotStore`) is
where the snapshot's *contents* live; it's a separate concern from the log, covered in
[raft-usage.md](raft-usage.md#snapshots-and-compaction) since it's `RaftGroupInstance` that actually drives
capture-and-truncate together.

## Durability

`FlushAsync` is the fsync boundary — nothing before it is guaranteed to survive a crash. `FlushToDisk = false`
in the options disables the actual fsync syscall entirely; that's for tests and benchmarks measuring the
framing/write path, never for anything you'd call durable. With it off, `AppendAsync` returning success means
"in this segment's write buffer," not "on disk."

What a flush actually guarantees is a property of the OS and the drive underneath it, not of this code —
Windows, Linux and macOS are all first-class targets here, but "committed" is only ever as durable as the
platform's own fsync semantics.

## Multiple groups: `IRaftWalFactory`

If you're hosting more than one log — one per queue is the design point of this whole library — use the
factory rather than constructing `SegmentedRaftWal` instances directly:

```csharp
IRaftWalFactory factory = new SegmentedRaftWalFactory("./data/wal-root", options);

IRaftWal queueA = await factory.OpenAsync(new RaftGroupId(1));
IRaftWal queueB = await factory.OpenAsync(new RaftGroupId(2));

await factory.DeleteAsync(new RaftGroupId(1)); // the queue was deleted -- removes its whole directory
```

Each group gets its own subdirectory (`g00000000000000000001/`, zero-padded, so filenames stay short and
unambiguous across platforms) and its own file handle for the active segment — nothing is shared between
groups, which is what makes it reasonable to have thousands of them open in one process. `InMemoryRaftWalFactory`
is the equivalent for the volatile implementation, keyed the same way.
