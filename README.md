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
[doc/grpc-usage.md](doc/grpc-usage.md), [doc/raft-usage.md](doc/raft-usage.md),
[doc/cluster-usage.md](doc/cluster-usage.md).

> **Note on the benchmark sources.** Both benchmark projects were lost to an accident and rewritten from
> their documented behaviour rather than recovered. Every measurement below has been re-run against the
> rewritten harness on one machine; the qualitative conclusions are unchanged, the absolute numbers are not
> comparable with any figure quoted before this note existed.

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
- **Cluster management, without a placement policy.** One reserved group holds the node set and whatever
  administration means to the host. It answers *is this node a legal target for that group's leadership* and
  *who is carrying least* — and moves a leader only when asked to. Nothing rebalances on a timer. See
  [doc/cluster-usage.md](doc/cluster-usage.md).
- **Optional LZ4 payload compression.** Off by default. Switched on per group with
  `RaftGroupOptions.PayloadCompression`, it compresses each command payload before the log sees it and
  expands it again on the way to the state machine, so the log, the wire and the follower all carry the
  smaller form. Each entry decides for itself: the compressed form is kept only when it is actually smaller,
  and the header records which happened.

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
in-process cluster, `Hosting/` the per-node host and its shared clock, `Cluster/` the cluster-management
group and the placement API built on it.

## The demo cluster

```bash
dotnet run --project demo/Blun.MultiRaft.AppHost/Blun.MultiRaft.AppHost.csproj
```

Five nodes over real gRPC, three queue groups plus the administrative one, and an observer that watches all
five and can stop and start them again. Five rather than three because the quorum is then three, which puts
the interesting states within reach by hand: kill one node and the cluster shrugs, kill two and it still
works, kill the third and writes stop.

| What | Where |
|---|---|
| Nodes 1–5, Raft protocol | `127.0.0.1:7101`–`7105` |
| Nodes 1–5, `/status`, `/cluster/load`, placement | `127.0.0.1:8101`–`8105` |
| App-host control plane — resource state, start/stop, console logs | `127.0.0.1:8200` |
| Observer API — the aggregated cluster view | `127.0.0.1:8300` |
| Observer UI (Angular, Tailwind) | `127.0.0.1:4200` |

The observer is an ordinary client of the nodes' `/status` endpoints, given no privileged access to the
cluster: everything it displays could have been fetched with curl. What it cannot do on its own is start and
stop processes, and that is the one thing it forwards to the app host, which is the only place where
starting a resource and reading its console output are possible without hunting for OS process handles.

Leadership is moved by **clicking a cell in the group matrix**: the cell is the pair of group and node the
request is about, so it is what you click. The node already leading a group, and any node that is not
answering, are not offered. The picker on the right of each row is the other half of the same idea — leaving
the target empty asks the group's own leader to choose the best-placed candidate, and the answer it gives
(`Valid`, `Lagging`, `NotResponding`) appears next to the button that asked.

**The write-ahead log has its own panel**, because a log nobody can see is a log nobody trusts. Per group and
per node it shows the bytes the filesystem actually holds — segments included, so the half-written segment
counts — alongside the entry count, the index range, the number of segments and the average bytes per entry.
The last column sends generated traffic into a group: 100, 1 000 or 10 000 commands, at most one every 10 ms,
routed to that group's leader because that is where an append goes. Every follower's copy grows with it and
ends up byte-identical, which is the clearest picture of replication the demo has.

The rate shown next to a run is measured, not the rate that was asked for. Each append waits for its commit,
so what the number reports is what a replicated round trip costs — around 36–42 writes a second here, not
the hundred a ten-millisecond interval would suggest. The contrast between the queue groups at roughly
285 bytes an entry (a 256-byte payload plus framing) and the cluster group at roughly 47 is worth a look too.

Both kinds of "out" are reachable from the node cards, and they are not the same thing. **Stop** ends the
process — the node is gone and its peers find out by not hearing from it. **Out of cluster** leaves it
running and takes it out of the cluster group's configuration, which is a single-server membership change on
the administrative plane. A removal does not stick while the node is still configured on the others: the
coordinator's reconcile pass adopts any configured node it finds missing and promotes it back, one per pass.
That is the mechanism working in plain sight, and it happens fast enough that the voter set is usually whole
again before the next poll.

Two things the UI deliberately keeps apart. **Aspire's resource state and whether a node answers** are
different claims — a node can be `Running` and silent, and watching a consensus cluster is largely about
being able to see that. And **the cluster leader is a tally, not a fact**: every node reports the leader it
believes in, the header shows what most of them say, and while they disagree it says so rather than picking
a winner. During an election they legitimately differ.

The UI is skipped with a note if Node.js is not on `PATH`; the five nodes and the observer API come up
regardless.

The demo persists to `demo/Blun.MultiRaft.AppHost/data/`. **Delete it before the first five-node run** — an
older directory holds a cluster whose voter set is three nodes.

`demo/chaos/` drives all of this from a script — kills, restarts, membership changes, the quorum edge — and
checks after every step that the cluster settled, then reports what the nodes logged while it happened. One
warning is expected and frequent there: `replication to node N failed`, carrying an `IOException`, which is
what the replication loop is supposed to say about a peer that is not answering.

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

**Compression has one level and no size threshold, and both of those are measured rather than chosen.** The
obvious design — a level per group, and a payload-size rule deciding when to bother — is the one the numbers
ruled out. Every LZ4 level above the fastest cost more on the append-and-flush path than it saved, at every
size measured, reaching eight times the uncompressed time at a megabyte: there is nothing to choose between,
so `RaftPayloadCompression` is `None` or `Lz4Fast` and that is all. And a size threshold would be guessing at
the wrong variable, because what decides whether compression pays is how compressible the payload is, not how
large: the same level at the same size differed sixfold between repetitive and random content. So the choice
is made per entry, by trying — cheap enough at the fastest level to be worth a correct answer — and the
compressed form is kept only when it is genuinely smaller. `RaftEntryHeader.Compression` carries the outcome,
which means a group with compression switched on still writes `None` for anything incompressible, and a
reader never needs to know how the group was configured. The encoding is the raw LZ4 block codec behind a
four-byte length prefix rather than the LZ4 frame format: the frame's self-description is redundant next to
that header field and the WAL's own checksum, and its one genuinely useful part — the content length that
would size a decode buffer — is not implemented by K4os for span targets at all. Numbers in
[Measurements](#measurements); the full reasoning in
[doc/raft-payload-compression.md](doc/raft-payload-compression.md).

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
window and HPACK state. A console harness was built to check that reasoning against the alternative — the
same gRPC stack, but one dedicated stream per group — with a listener that answers instantly, so what was
measured is transport overhead alone. That harness has since been deleted from the tree; the numbers below
are the record it left, and reproducing or extending them means building it again.

It does not confirm the latency claim. Concurrent round trips across many groups, steady-state (connection
setup measured and excluded):

| Groups | Multiplexed (steady) | Per-group setup | Per-group (steady) | Ratio |
|---|---|---|---|---|
| 10 | 1.4 ms | 149.6 ms | 4.4 ms | 3.08x |
| 100 | 10.2 ms | 289.3 ms | 23.6 ms | 2.31x |
| 500 | 72.9 ms | 1362.3 ms | 49.9 ms | 0.69x |
| 2000 | 227.8 ms | 4272.6 ms | 122.0 ms | 0.54x |

Ratio is per-group-steady over multiplexed-steady; above 1 favors multiplexing. It **crosses over**: at ten
and a hundred groups multiplexing is two to three times faster, and by five hundred the dedicated-stream arm
has overtaken it and stays ahead. So the honest summary is that neither arm wins outright, and the one that
wins depends on the group count — which is the opposite of the original claim, that multiplexing wins and
wins harder as groups multiply. Localhost, .NET's gRPC client and server, an empty-payload heartbeat-shaped
request: this does not rule out the effect showing up on a real network, with real payloads, or well past
2000 groups, but it means the HPACK/flow-control argument is not a demonstrated fact the way the WAL numbers
above are. It is exactly what it was called then — an argument — and should be described that way.

Two things the measurement does show, and that motivate keeping the multiplexed design regardless: connection
*setup* cost is real, scales with group count, and is now the dominant term rather than a footnote — 4.3 s to
open 2000 channels against 122 ms of steady-state work, so setup costs thirty-five times what the round trips
it enables do. It recurs every time a per-group connection needs re-establishing, where the multiplexed
session amortizes it once per node pair. And the measurement never covered the
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

**The cluster is a Raft group like any other, and its voter set is the node set.** One reserved id
(`RaftGroupId.Cluster`, `ulong.MaxValue` — not 0, because that is `default(RaftGroupId)` and an uninitialized
struct must not address the administrative plane) carries node membership as ordinary `Membership` entries
and whatever administration means to the host as `Command` entries on the same log. Adding a node to the
cluster is therefore adding a voter, through the single-server-change path that already exists, rather than
through a second membership mechanism that would have to be kept in agreement with the first.

**A fresh node starts with no configuration and waits to be adopted.** This is the one part of the bootstrap
that is not a convenience. Under the election restriction two empty logs are exactly as complete as each
other, so two fresh voters will elect one of themselves — and if some third node in that set actually held
data, the winner's empty log would truncate it away. Starting empty-handed removes that outcome instead of
making it unlikely. A cold cluster is seeded by the lowest node id, the one choice every node reaches without
being told, and the seed writes its own voter status into the log rather than only holding it in the
configuration it was started with: joining nodes rebuild membership by replaying entries, so a seed that
never logged itself is a seed they cannot see, and two views of who votes is two different quorums.

**Single-node to replicated is allowed and needs no mechanism; the reverse is refused.** The node that ran
alone holds the log and the nodes joining it hold nothing, so the election restriction settles which log wins
without being asked. Going the other way would make a node pulled out of a cluster a second authority on the
same group ids, with two real committed histories and no rule that reconciles them — so it is refused at
startup, from a marker the node writes next to its data, rather than discovered later.

**Placement is a question the library answers, not a decision it makes.** Nothing rebalances on a timer or a
threshold. The coordinator will tell you whether a node is a legal target for a group's leadership — a
judgement only that group's current leader can make, since it rests on per-peer match indices — and which
node is carrying the fewest leaderships, and it will carry out a handover when asked. Load reports are pushed
to the cluster leader and held in memory, never put through the log: leader counts change on every election
of every group, and replicating them would turn an advisory number into a write firehose on the one group
that has to stay responsive. The actionable event, `NodeBecameAvailable`, is raised on the cluster leader
alone — if every node raised it, every node's consumer would decide to re-place the same groups at the same
instant.

## Measurements

Append path, `--job short`, fsync disabled so the disk flush does not swamp everything else. Short-job
variance is high — the 4 KB batch figures in particular have a standard deviation in the tens of
microseconds — so read the ratios, not the absolutes.

.NET 10 and .NET 11 in one invocation, as the benchmark defaults to. Both runtimes agree within the noise
here, so one column each is shown rather than two.

| Payload | Access | Single (net10 / net11) | Batch of 32, per entry | Alloc/append |
|---|---|---|---|---|
| 64 B | RandomAccess | 19.7 / 23.4 µs | 0.86 / 0.83 µs | 58 B |
| 64 B | MemoryMapped | **0.25 / 0.23 µs** | 0.18 / 0.17 µs | 132 B |
| 4096 B | RandomAccess | 23.5 / 25.8 µs | 2.94 / 2.29 µs | 680 B |
| 4096 B | MemoryMapped | **4.17 / 4.17 µs** | 4.25 / 4.31 µs | 112 B |

Three things worth reading out of this.

**Mapping the segment is worth about 80x on a small single append** (19.7 µs to 0.25 µs) and roughly 6x on a
4 KB one. That is the syscall, and nothing else: the work done per append is otherwise identical. The gap
narrows with payload size because at 4 KB the copy starts to matter next to the call itself.

**Batching and mapping are substitutes, not complements.** Batching is worth roughly 23x on the unmapped
path at 64 bytes, because it amortizes exactly the syscall that mapping removes outright. Once the segment is
mapped the benefit collapses — and at 4 KB it inverts, the batch costing 4.25 µs per entry against 4.17 µs
for a single append. Which is to say: if you turn on `MemoryMapped` for a hot queue, do not also expect the
batching path to keep paying for itself there.

**Allocation per append is async machinery, not payload.** At 64 bytes and at 4 KB the mapped path allocates
132 B and 112 B — it does not track payload size, which is the zero-copy write path showing up in the
numbers. The unmapped 4 KB figure of 680 B is the one place a payload-sized buffer is briefly involved.
`InMemoryRaftWal`, by contrast, copies deliberately, having no buffer of its own.

### Compression

A separate sweep, and read separately from the table above: **fsync is on here**, deliberately, because the
question was what a caller actually waits for rather than what the code costs. Three caveats. It ran on
net11.0 only — a one-off exploratory pass, not a runtime comparison, so it does not follow the
both-runtimes rule the rest of the benchmarks do. The figures were taken through K4os's frame API, while the
implementation settled on the raw block codec, which does strictly less work per call — so treat these as an
upper bound on what the shipped path costs. And each cell is one `Mean` at `--job` defaults, not a repeated
measurement; the millisecond-scale differences are solid, the tens-of-microseconds ones are not.

Milliseconds per compress-append-flush, as *repetitive content / random content* — the two ends of
compressibility:

| Payload | `None` | `Lz4Fast` (`L00_FAST`) | `L06_HC` | `L12_MAX` |
|---|---|---|---|---|
| 2 KB | 0.43 / 0.43 | 0.42 / 0.43 | 0.43 / 0.43 | 0.47 / 0.44 |
| 64 KB | 2.03 / 2.09 | **1.99** / 2.09 | 2.03 / 2.82 | 2.03 / 2.90 |
| 256 KB | 3.27 / 3.32 | **2.04** / 3.35 | 2.39 / 7.33 | 8.68 / 7.66 |
| 1 MiB | 4.13 / 4.18 | **2.25** / 4.66 | 3.76 / 22.53 | 35.56 / 23.34 |

**No level above the fastest ever beats it.** Not in one cell of this table — neither size nor content type
produces a case where paying for a higher level comes out ahead of `Lz4Fast`. `L06_HC` on
random content at a megabyte costs 22.5 ms against 4.2 ms for storing it uncompressed, and `L12_MAX` on
*repetitive* content — the case it should be best at — is the worst cell in the table at 35.6 ms, apparently
because its exhaustive parse has the most candidates to weigh exactly where matches are plentiful. Whatever
compression buys in a shorter flush, these levels spend several times over on CPU first. That is what reduced
the enum to on and off.

**`Lz4Fast` is a no-op below about 256 KB.** At 2 KB and 64 KB it lands within a few percent of `None` in
either direction — fsync dominating a compression cost still measured in microseconds. It neither helps nor
hurts an ordinary 1 KB queue command. The case it exists for is the payload approaching the 1024 KB cap,
where compressible content nearly halves the wait.

**Compressibility, not size, decides.** At a megabyte `Lz4Fast` takes 2.25 ms on repetitive content and
4.66 ms on random — against an uncompressed baseline of about 4.15 ms either way. Same level, same size: a
win of nearly 2x on one, a loss of about 11% on the other. A size threshold would have switched compression
on for both. Trying per entry and keeping the result only when it shrank pays that 11% in the worst case and
is right in every case.

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

### The same shape again, in the election path

Replication was queued; the election was not. `TickAsync` awaited `CampaignAsync` inline, which puts the
whole campaign on the tick loop's stack — including `BecomeLeaderAsync`, which appends the term's no-op and,
at `Quorum` durability, waits for it to commit. A winner that could not reach a majority therefore stopped
the clock for *every* group on that node, and the loop never got back to observing shutdown.

This one was not diagnosed by inspection either. A run sat idle, and a dump named it outright:

```
TickLoopAsync -> TickOneAsync -> TickAsync -> CampaignAsync
              -> BecomeLeaderAsync -> AppendCoreAsync -> WaitForCommitAsync
```

with every pool thread idle and no other library frame anywhere in the process — not starvation, not a lock,
just an `await` on a commit that a partitioned node was never going to get. Elections are now queued with the
same one-in-flight debounce replication and compaction already use, and the election deadline is re-armed
before queuing so a tick arriving mid-campaign does not stack a second one behind it. The general rule this
is the third instance of: **work whose duration is set by the rest of the cluster does not belong on the
clock's stack.**

Two related bugs surfaced while chasing it, both on the catch-up path and both able to strand a replica
permanently rather than briefly. `ReplicateToPeerAsync` decided a peer needed a snapshot from `prevTerm < 0`
alone — but `TermAtAsync(0)` answers 0, "before the log began", which is exactly what a peer still at
`NextIndex` 1 asks about, so against a compacted log such a peer was sent an empty range and acknowledged at
match 0 forever. That is not a corner case: it is what every node joining a group old enough to have
compacted looks like. And after a snapshot *was* sent, the loop exited instead of iterating — `continue` in a
do-while jumps to the condition, and the pending flag had been cleared at the top of the body — so the
entries appended since the compaction boundary waited for the next heartbeat.
