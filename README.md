# Blun.MultiRaft

Multi-Raft consensus for .NET, built for a message queue rather than a database: **one Raft group per
queue**, leader-driven asymmetric replication, and a write-ahead log behind a swappable interface.

```
dotnet build Blun.MultiRaft.slnx
dotnet test test/Blun.MultiRaft.Tests/Blun.MultiRaft.Tests.csproj
dotnet run -c Release --project benchmark/Blun.MultiRaft.Benchmarks/Blun.MultiRaft.Benchmarks.csproj -- --filter "*WalAppend*"
```

Targets `net10.0;net11.0`. MIT licensed.

**Usage docs** (the "how do I call this" companion to the design notes below): [doc/wal-usage.md](doc/wal-usage.md),
[doc/grpc-usage.md](doc/grpc-usage.md), [doc/raft-usage.md](doc/raft-usage.md).

## Platform support

**Windows, Linux and macOS are all first-class targets.** The library is required to build, pass its test
suite and behave identically on all three; a change that works on only one of them is not finished. There is
no supported-platform attribute anywhere in the public surface, and none should appear.

Nothing in the design is OS-specific: the storage layer is `System.IO.RandomAccess` over `SafeFileHandle`,
the transport contract is a plain interface, and the timing goes through `TimeProvider`. Three places are
nonetheless where a platform difference would first show up, and they are worth knowing about:

- **`RandomAccess.FlushToDisk` is the durability boundary**, and what a flush guarantees is a property of the
  operating system and the drive, not of this code. Treat "committed" as exactly as durable as the platform's
  fsync, and do not assume the three are equivalent when writing durability tests.
- **`FileShare` is enforced on Windows and advisory on Unix.** The segmented log is written by a single
  process and serializes its own writers through an async gate, so it does not depend on the OS enforcing
  anything — but a future change that starts relying on `FileShare` for correctness would pass on Windows and
  silently fail elsewhere.
- **Path length and case sensitivity.** Segment and metadata filenames are generated from zero-padded
  numbers, so they are short and case-unambiguous by construction. Keep it that way.

CI is expected to run the test suite on all three; anything that cannot be verified on a platform belongs in
this list rather than in an assumption.

**Status: the workflow exists (`.github/workflows/ci.yml`) but has never actually run.** This repository has
no `git remote` in this environment, so nothing has pushed to trigger Actions — the cross-platform claim above
is backed by running the suite locally on Windows only, not by the matrix. One real bug in the workflow was
found and fixed anyway, without needing a remote: its test step used `dotnet test --logger trx`, which fails
outright on the .NET 10+ SDK for an MTP/xUnit v3 project ("Testing with VSTest target is no longer
supported") — confirmed by running that exact command locally, not assumed. Fixed to `dotnet exec` the built
test DLL with MTP's own `-result-trx` flag, which was verified locally to produce the right exit code (0 on a
clean pass, non-zero on a failure) and still write the `.trx` the upload step looks for. Whether the workflow
runs cleanly on Linux and macOS specifically remains unverified until something pushes to a real remote.

## What it does

- **Multi-Raft.** Every group is independent — its own term, log, election and leader. A node is leader for
  some groups and follower for others at the same time, which is the point: it spreads leader CPU across the
  cluster instead of concentrating it.
- **Leader-driven asymmetric replication.** Followers never talk to each other. The leader appends locally
  and returns as soon as the group's `DurabilityLevel` allows, pushing to followers on its own schedule.
  Commit is still real Raft: a majority of voters must report a matching index. The difference is that the
  leader learns this from replies as they land, not by blocking a request behind the slowest peer.
- **Membership changes.** Single-server changes (Raft §4.1), not joint consensus. A joining node is added as
  a non-voting learner, catches up, and is promoted only once it is within a configured distance of the
  leader. Removal is one entry.
- **Pre-vote.** On by default (Raft §9.6). A node that was partitioned away cannot force a healthy group
  through a needless election when it comes back.

## Layout

Two libraries, and the dependency runs one way only.

| Project | What lives there |
|---|---|
| `src/Blun.MultiRaft.Wal` | `RaftGroupId`, `RaftLogEntry`, `IRaftWal` and its implementations |
| `src/Blun.MultiRaft` | `NodeId`, `RaftGroupInstance`, membership, transport contracts, `MultiRaftHost` |

`Blun.MultiRaft.Wal` references nothing from the consensus core — it is a leaf, and it stays one. That is
what makes the storage seam real rather than decorative: the log can be referenced, tested, benchmarked or
replaced without dragging elections and transports along, and a change that would make the log depend on
consensus fails to compile instead of quietly passing review. `RaftGroupId` lives there rather than in the
core for the same reason: a log is keyed by group, and `IRaftWalFactory` has to say so without knowing what
a group *does*.

Inside the core, `Core/` holds the group instance and its state, `Transport/` the wire contracts and an
in-process cluster, `Hosting/` the per-node host and its shared clock.

## Design decisions worth knowing

**One WAL per group, not one shared log.** A shared log is what databases do, and it is wrong here. A queue
that drains wants to throw its entire log away; in a shared log the slowest group pins the segments of every
other one, and queues run empty constantly. The cost of the per-group choice is per-instance footprint, so
`SegmentedRaftWal` keeps it to one file handle for the active segment plus 16 bytes of index map per retained
entry — no page cache, no background thread.

**A custom segmented log rather than an embedded store.** FASTER is archived and has no .NET 10 assembly. Its
successor, Tsavorite, is not published as a standalone package — it ships only inside `Microsoft.Garnet`,
which drags the whole Garnet server along. Beyond packaging, a Raft log needs *tail* truncation when a
follower's log diverges, and append-only stores that can only truncate their head make that a workaround.
`IRaftWal` keeps the door open: a Tsavorite backend is a class, not a rewrite.

**Memory-mapped segments are an option, not the default.** `SegmentAccess = MemoryMapped` preallocates the
active segment and appends by copying into a mapped view — no syscall per write at all. It fits the format
rather than fighting it, because segments are already fixed-size and the zero-filled tail of a preallocated
file is recognised by recovery as a clean ending rather than a torn write. It is off by default because the
cost is charged per group: a mapping reserves a segment's worth of address space and page-table entries for
as long as it is being written, and ten thousand groups times a 4 MB segment works directly against the
per-group footprint the whole design is built around. Turn it on for the queues that are hot. Two further
details worth knowing: only the segment being written stays preallocated — rolling a segment trims it back to
its contents — and durability takes two steps, because `FlushViewOfFile` does not flush the drive's cache and
the handle is fsynced afterwards.

**Message size is capped at 1024 KB**, and the log is dimensioned for it: `MaxPayloadBytes` defaults to that
plus envelope headroom, and a segment is validated at open to hold at least one maximum-size record. The
framing buffer is deliberately *not* sized to the cap — it covers ordinary commands and borrows from the
array pool above that. Sizing it to the maximum would charge every group a megabyte for a message size
almost none of them will see, which across thousands of queues is the footprint this design exists to avoid.

**Two WAL implementations from the start.** `InMemoryRaftWal` is not only for tests. An interface with one
implementation is a wrapper class; the shared contract suite in `WalTests.cs` runs against both, and it has
already caught a divergence between them.

**One timer for all groups.** A `System.Threading.Timer` per group is a queue entry, a callback and an
allocation per group per tick. At ten thousand groups that dominates a node's CPU long before consensus does.
`MultiRaftHost` ticks the whole set from one loop and a group's election deadline is a timestamp it compares
against.

**gRPC, but multiplexed by hand — and the latency argument for it did not hold up when measured.**
`Blun.MultiRaft.Grpc` puts *one* bidirectional stream between each pair of nodes and carries every group over
it, with the group id in the frame and a correlation id pairing replies to requests. The reasoning for this
over a stream per group was that each of thousands of separate HTTP/2 streams carries its own flow-control
window and HPACK state, and `benchmark/Blun.MultiRaft.Grpc.Benchmarks` was built to check that reasoning
against the alternative — the same gRPC stack, but one dedicated stream per group — with a listener that
answers instantly, so what is measured is transport overhead alone.

It does not confirm the latency claim. Concurrent round trips across many groups, steady-state (connection
setup measured and excluded):

| Groups | Multiplexed (steady) | Per-group setup | Per-group (steady) | Ratio |
|---|---|---|---|---|
| 10 | 19.0 ms | 0.2 ms | 23.4 ms | 1.23x |
| 100 | 125.2 ms | 1.4 ms | 84.0 ms | 0.67x |
| 500 | 462.3 ms | 27.0 ms | 404.4 ms | 0.87x |
| 2000 | 3544.1 ms | 408.9 ms | 3315.3 ms | 0.94x |

Ratio is per-group-steady over multiplexed-steady; above 1 favors multiplexing. At every group count
measured here it is at or below 1 — the dedicated-stream arm was never slower in steady-state throughput, and
was sometimes faster. Localhost, .NET's gRPC client and server, an empty-payload heartbeat-shaped request —
this does not rule out the effect showing up on a real network, with real payloads, or well past 2000 groups,
but it means the HPACK/flow-control argument is not a demonstrated fact the way the WAL numbers above are; it
is exactly what it was called then — an argument — and should be described that way, not as settled.

Two things this benchmark does show, and that motivate keeping the multiplexed design regardless: connection
*setup* cost is real and scales with group count (409 ms to open 2000 channels, versus 3.5 s of steady-state
work — not dominant here, but not free, and it recurs every time a per-group connection needs re-establishing,
where the multiplexed session amortizes it once per node pair). And this benchmark does not measure the
separate, likely more important claim at real scale: whether a node can hold thousands of concurrent TCP
connections and HTTP/2 stream objects open at all without hitting OS-level resource limits — a question about
survivability under load, not about the latency of any single round trip. That question remains unmeasured
here too. `IRaftProtocolTransport` keeps a custom `System.IO.Pipelines` transport as an option; given this
result, its case rests on that resource-footprint question, not on the framing-overhead one.

**Linearizable reads without a log entry.** `ReadIndexAsync` implements Raft §6.4: the leader confirms it
still leads by exchanging heartbeats with a quorum, then hands back its commit index; the reader waits until
it has applied that index. No append, no fsync. The confirmation is the load-bearing part — a partitioned
leader still believes it leads and its commit index still looks plausible, so answering from it would return
a stale read dressed up as a linearizable one, which is worse than an honestly stale one. For a queue that
distinction is not academic: a stale read is a duplicate delivery or a lost settlement, not slightly old data.

**Snapshots close the only unrecoverable state.** A replica that falls behind the leader's retained log used
to be terminal — the entries it needed existed nowhere. `InstallSnapshot` replaces the log wholesale, and the
group configuration travels inside the snapshot because the receiver is about to discard the log that
membership changes lived in. `TakeSnapshotAsync` then compacts the log, and the boundary index and term are
persisted so the replica can still answer the consistency check at its own starting point after a restart.

**Leadership transfer skips pre-vote on the receiving end, deliberately.** `TransferLeadershipAsync` (Raft
§3.10) blocks new client writes, waits for the target's log to actually match, sends `TimeoutNow`, then steps
aside. The target's resulting candidacy passes `skipPreVote: true` to its election — every other voter's
pre-vote check refuses a grant while it still believes a leader is alive, and that belief is driven by when it
last heard a heartbeat, which lags the outgoing leader's actual step-aside by up to a full election timeout.
Skipping pre-vote there is what makes a deliberate handoff resolve in one round instead of stalling for that
window despite every voter already knowing, from the transfer itself, that the new election is legitimate.
Found by a test intermittently timing out at 5 seconds rather than by inspection.

## Measurements

Append path, `--job short`, fsync disabled so the disk flush does not swamp everything else. Short-job
variance is high — the 4 KB batch figures in particular have a standard deviation in the tens of
microseconds — so read the ratios, not the absolutes.

| Payload | Access | Single | Batch of 32, per entry | Alloc/append |
|---|---|---|---|---|
| 64 B | RandomAccess | 10.2 µs | 0.43 µs | 613 B |
| 64 B | MemoryMapped | **0.14 µs** | 0.10 µs | 137 B |
| 4096 B | RandomAccess | 37.2 µs | 2.92 µs | 670 B |
| 4096 B | MemoryMapped | **3.0 µs** | 3.06 µs | 112 B |

Three things worth reading out of this.

**Mapping the segment is worth 70x on a small single append** (10.2 µs to 0.14 µs) and 12x on a 4 KB one.
That is the syscall, and nothing else: the work done per append is otherwise identical.

**Batching and mapping are substitutes, not complements.** Batching is worth roughly 24x on the unmapped
path, because it amortizes exactly the syscall that mapping removes outright. Once the segment is mapped the
benefit collapses — and at 4 KB it disappears entirely, the batch costing 3.06 µs per entry against 3.00 µs
for a single append. Which is to say: if you turn on `MemoryMapped` for a hot queue, do not also expect the
batching path to keep paying for itself there.

**Allocation per append does not grow with payload size** — 613 B at 64 bytes, 670 B at 4 KB — which is the
zero-copy write path showing up in the numbers: what is allocated is async machinery, never the payload.
`InMemoryRaftWal` allocates 4192 B for a 4 KB payload because it copies deliberately, having no buffer of
its own.

**Automatic compaction is opt-in, triggered by applied growth, never by a timer.** Setting
`RaftGroupOptions.AutoCompactionThreshold` makes a group call its own `TakeSnapshotAsync` once entries
applied since the log's retained start cross it. Checked after every apply pass rather than polled on a
schedule: compaction only ever becomes newly due when `LastApplied` moves, so a timer would either check
uselessly often on an idle group or lag behind a busy one. Queued through `Task.Run`, one at a time per
group (same debounce pattern as peer replication, for the same reason: a slow `CaptureAsync` must not hold
the tick loop hostage). No default threshold — how large a state machine's captured state is, and therefore
where the disk-versus-work trade lands, is something only the host knows.

## Not implemented yet

- **A custom TCP transport.** The gRPC one works and is what the Aspire scenario runs on. The framing-overhead
  argument for a custom transport was measured and did not hold up (see above); a custom transport's remaining
  case is the unmeasured resource-footprint question — thousands of concurrent connections/streams — not
  round-trip latency.

## Fixed: intermittent test hang (was pre-existing)

A small fraction of test runs (observed 3–8% across `RaftGroupTests` and `LeadershipTransferTests`, measured
with .NET's own `Start-Process`/`WaitForExit` rather than a shell wrapper, to rule out shell artifacts) used to
never complete — the process sat idle rather than failing an assertion.

Two dumps taken seconds apart on the same hang showed the active thread moving through *different* points
inside `ReplicateToPeerAsync` and `InMemoryRaftWal` each time, with SOS `threadpool` reporting one thread
running, several idle, well under the pool's minimum — not starvation, and not a fixed instruction pointer
either, so not a classic deadlock. That combination points at a livelock: `TestCluster.FastOptions` times
elections aggressively (120 ms timeout, 100% jitter, 10 ms tick, three hosts ticking independently) over an
in-memory transport with zero real latency, which can let repeated elections re-synchronize by chance and
never settle on a stable leader for a while.

The actual permanent-hang *mechanism*, though, was independent of whether that theory is the full story:
`PushToAllPeers` started each peer's replication as a fire-and-forget call directly on the tick loop's own
call stack (`TickLoopAsync → TickAsync → CampaignAsync → BecomeLeaderAsync → AppendCoreAsync →
PushToAllPeers → ReplicateToPeerAsync`), and since an in-memory transport has no real latency to force a
yield, that whole chain — however many replication rounds it took — could run to completion without ever
returning control to `TickLoopAsync`'s own loop. Until it did, that host's tick loop could not observe
`_shutdown.Token`, so `MultiRaftHost.DisposeAsync` calling `await _ticker` waited for a return that a
sufficiently unlucky run might delay far longer than any client was waiting on it for.

Fixed by queuing each peer's replication (`Task.Run`) instead of running it inline: the tick returns
immediately regardless of how replication goes, so shutdown is never held hostage by however long it takes.
Verified with 80 repeated runs of the previously-hanging `RaftGroupTests` test and 40 of the full
`LeadershipTransferTests` class (both via `Start-Process`/`WaitForExit`, not a shell timeout) — 0 hangs in
either, against a same-method baseline of roughly 1 in 12–25 before the fix.
