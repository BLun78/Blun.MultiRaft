# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Multi-Raft consensus library for .NET, built for a message queue rather than a database: one Raft group per
queue, leader-driven asymmetric replication, and a write-ahead log behind a swappable interface. See
`README.md` for the full design rationale (multi-Raft, membership changes, pre-vote, read index, snapshots,
leadership transfer) — it is detailed and worth reading before touching `Core/`.

## Commands

```bash
# Build
dotnet build Blun.MultiRaft.slnx

# Test (whole suite)
dotnet test test/Blun.MultiRaft.Tests/Blun.MultiRaft.Tests.csproj

# Test a single test/class (MTP/xUnit v3 filter syntax)
dotnet test test/Blun.MultiRaft.Tests/Blun.MultiRaft.Tests.csproj --filter-method "*MethodName*"
dotnet test test/Blun.MultiRaft.Tests/Blun.MultiRaft.Tests.csproj --filter-class "*ClassName*"

# CI does NOT use `dotnet test` -- its VSTest bridge fails outright on the .NET 10+ SDK for this
# MTP/xUnit v3 project. It runs the built DLL directly:
dotnet exec test/Blun.MultiRaft.Tests/bin/Release/net10.0/Blun.MultiRaft.Tests.dll -result-trx TestResults/results.trx

# Benchmarks -- must always run both net10.0 and net11.0 in one invocation, to compare runtime-to-runtime
dotnet run --project benchmark/Blun.MultiRaft.Benchmarks/Blun.MultiRaft.Benchmarks.csproj -c Release -f net11.0 -- --runtimes net10.0 net11.0
dotnet run -c Release --project benchmark/Blun.MultiRaft.Benchmarks/Blun.MultiRaft.Benchmarks.csproj -- --filter "*WalAppend*"

# The gRPC multiplexing benchmark is a plain console harness, not BenchmarkDotNet
dotnet run -c Release --project benchmark/Blun.MultiRaft.Grpc.Benchmarks/Blun.MultiRaft.Grpc.Benchmarks.csproj

# Aspire demo: five nodes, real gRPC, /status on 8101-8105, observer UI on 4200
# (control plane 8200, observer API 8300; delete demo/Blun.MultiRaft.AppHost/data/ first)
dotnet run --project demo/Blun.MultiRaft.AppHost/Blun.MultiRaft.AppHost.csproj

# The observer UI on its own (the Aspire run starts it too, if Node.js is on PATH)
npm --prefix demo/Blun.MultiRaft.Observer.Ui start
```

## Shell safety

Never pass an unset or possibly-empty variable as a path to a command that writes or deletes. `git worktree
add "$VAR" HEAD` with an empty `VAR` resolves to the repository root and begins deleting the working tree —
this has already destroyed this repository's history once. Use literal paths, or verify with `test -n`.

## Platform support

Windows, Linux and macOS are all first-class targets — a change that only works on one is not finished.
There is no supported-platform attribute anywhere in the public surface, and none should appear. Three spots
are where a platform difference would first show up:

- `RandomAccess.FlushToDisk` is the durability boundary; "committed" is only as durable as the platform's
  fsync — don't assume Windows/Linux/macOS durability semantics are equivalent.
- `FileShare` is enforced on Windows, advisory on Unix. The segmented log already serializes its own writers
  through an async gate, so correctness must not come to depend on OS-level file locking.
- Segment/metadata filenames are zero-padded numbers — keep them short and case-unambiguous.

CI (`.github/workflows/ci.yml`) runs the full matrix on push/PR to `main`.

## Code conventions

- C# with `LangVersion=preview`; multi-targets `net10.0;net11.0` for core libraries.
- **Logging must use source-generated `[LoggerMessage]` partial methods** — `LogInformation()`,
  `LogWarning()`, etc. are prohibited. Pattern:

  ```csharp
  internal static partial class MyLog
  {
      [LoggerMessage(Level = LogLevel.Information, EventId = 1000, Message = "...")]
      public static partial void SomeEvent(ILogger logger, /* params */);
  }
  // call site:
  MyLog.SomeEvent(_logger, param1, param2);
  ```

  EventId ranges: 1000–1016 Core Raft (`RaftGroupInstance.Log`), 1100+ Host (`HostLog`), 1200+ Cluster
  coordinator (`ClusterLog`), 1300+ gRPC transport (`GrpcLog`), 2000+ Demo node (`NodeStartedLog`), 2100+
  Observer (`ObserverLog`), 2200+ App-host control plane (`ControlPlaneLog`).
- Code must be trim-safe and AOT-compatible.
- Throw `InvalidOperationException` for integrity violations; `IOException` for transport failures. This
  distinction is load-bearing: the replication loop treats `IOException` as an ordinary retryable condition,
  so throwing it for a real integrity problem makes a group spin silently forever.
- **Buffers handed out by `IRaftWal.ReadFromAsync` and `IRaftSnapshotStore.ReadAsync` are recycled on the
  next iteration.** Anything that keeps a payload past its `MoveNextAsync` — a transport batching a
  replication round into one message, anything queuing a write — must copy first (`RaftLogEntry.ToOwned()`,
  `ByteString.CopyFrom`). Ignoring this does not fail loudly: headers are copied by value, so indices and
  terms stay right, the follower's consistency check passes, and only the *contents* are wrong. It cost days
  once; see `doc/open-issue-seed-visibility.md`.

## Architecture

Two libraries, dependency runs one way only: `Blun.MultiRaft.Wal` references nothing from the consensus
core. This is what makes the storage seam real — the log can be referenced, tested, benchmarked or replaced
without dragging elections and transports along.

| Project | Contents |
|---|---|
| `src/Blun.MultiRaft.Wal` | `RaftGroupId`, `RaftLogEntry`, `IRaftWal` and its implementations (`InMemoryRaftWal`, `SegmentedRaftWal`) |
| `src/Blun.MultiRaft` | `NodeId`, `RaftGroupInstance`, membership, transport contracts, `MultiRaftHost`, `ClusterCoordinator` |
| `src/Blun.MultiRaft.Grpc` | gRPC transport implementation |
| `demo/Blun.MultiRaft.Node` + `demo/Blun.MultiRaft.AppHost` | Aspire-based demo cluster, five nodes |
| `demo/Blun.MultiRaft.Observer` + `demo/Blun.MultiRaft.Observer.Ui` | Aggregating watcher and its Angular/Tailwind UI |

`RaftGroupId` lives in the WAL project (not core) because a log is keyed by group, and `IRaftWalFactory` has
to say so without knowing what a group *does*.

Inside `src/Blun.MultiRaft`:
- `Core/` — the group instance and its state (`RaftGroupInstance`, `RaftMembership`, `RaftMetaStore`, `RaftRole`)
- `Transport/` — wire contracts and an in-process transport (`IRaftProtocolTransport`, `InMemoryRaftTransport`, `RaftMessages`)
- `Hosting/` — the per-node host and shared clock (`MultiRaftHost`)
- `Cluster/` — the cluster-management group and node placement (`ClusterCoordinator`, `ClusterOptions`)

### Design decisions worth knowing before changing this code

- **One WAL per group, not a shared log.** A shared log lets the slowest group pin every other one's
  segments; per-group means a drained queue can throw its whole log away. Cost is per-instance footprint —
  `SegmentedRaftWal` keeps one file handle for the active segment plus 16 bytes of index map per entry.
- **Custom segmented log, not an embedded store.** FASTER/Tsavorite were ruled out (packaging + no tail
  truncation for follower log divergence). `IRaftWal` keeps a different backend a class away, not a rewrite.
- **Memory-mapped segments (`SegmentAccess = MemoryMapped`) are opt-in, not default** — big single-append win
  (measured ~70x at 64B), but the mapping reserves address space/page-table entries per group for as long as
  it's being written, which fights the whole per-group-footprint design at scale. Turn on only for hot queues.
- **Message size capped at 1024 KB**; the framing buffer is deliberately *not* sized to the cap (covers
  ordinary commands, borrows from the array pool above that) to avoid charging every group a megabyte.
- **Two WAL implementations from the start** (`InMemoryRaftWal`, `SegmentedRaftWal`) sharing one contract
  test suite (`WalTests.cs`) — this has already caught real divergence between them.
- **One `System.Threading.Timer` for the whole host, not one per group** — a per-group timer is a queue
  entry + callback + allocation per group per tick, which dominates CPU at ten-thousand-group scale.
  `MultiRaftHost` ticks all groups from one loop; a group's election deadline is just a timestamp comparison.
  The cluster group is ticked first, ahead of the queue groups sharing that loop.
- **gRPC transport multiplexes by hand**: one bidirectional stream per node pair carries every group
  (group id + correlation id in the frame), instead of one HTTP/2 stream per group. NOTE: the latency
  argument for this (avoiding thousands of streams' flow-control/HPACK overhead) was benchmarked in
  `benchmark/Blun.MultiRaft.Grpc.Benchmarks` and **did not hold up** — per-group streams were at or faster
  than multiplexed in steady state at every scale measured. The design is kept anyway for connection-setup
  cost (real, scales with group count) and an unmeasured resource-footprint question (thousands of concurrent
  OS-level connections). Don't cite the latency argument as settled fact; see README.md for the numbers.
- **`ReadIndexAsync` (Raft §6.4)** gives linearizable reads without a log append: the leader confirms
  leadership via a heartbeat quorum first — skipping that confirmation would let a partitioned leader return
  stale data as if linearizable.
- **Snapshots (`InstallSnapshot`/`TakeSnapshotAsync`)** carry group configuration inside the snapshot, since
  the receiver discards the log that membership changes used to live in.
- **Leadership transfer skips pre-vote on the receiving end deliberately** (`skipPreVote: true` in
  `TransferLeadershipAsync`) so a deliberate handoff resolves in one round instead of stalling for a full
  election timeout while other voters still believe the outgoing leader is alive. Found via an intermittent
  5s test timeout, not by inspection — a reminder that Raft timing bugs here tend to be intermittent rather
  than deterministic failures.
- **Peer replication is queued via `Task.Run`, never run inline on the tick loop.** A prior bug had
  `PushToAllPeers` run fire-and-forget replication directly on the tick loop's call stack; over the
  zero-latency in-memory transport used in tests, that chain could run to completion without ever yielding
  back to `TickLoopAsync`, so `_shutdown.Token` was never observed and `DisposeAsync` could hang. If you touch
  the tick loop or replication path, preserve the `Task.Run` boundary — inlining it reintroduces the hang.

### Cluster management

- **One reserved group, `RaftGroupId.Cluster` = `ulong.MaxValue`.** Not 0, because `default(RaftGroupId)` is
  0 and an uninitialized struct must not address the administrative plane. Ids `>= ReservedFloor` are refused
  by `MultiRaftHost.AddGroupAsync`.
- **The cluster group's voter set *is* the cluster's node set.** Adding a node = adding a voter, via the
  existing single-server-change path. No separate membership mechanism.
- **The library decides nothing about placement on its own initiative — it only reacts.** Nothing
  rebalances on a timer or a threshold. The coordinator answers two questions (is this node a legal target?
  who is carrying least?) and performs a handover when asked. `ClusterEventKind.NodeBecameAvailable` fires on
  the **cluster leader only** so one reactor decides, not N.
- **`ClusterOptions.RebalanceOnFailover` (default `true`) is the one exception, and it is still a reaction,
  not a schedule.** Raft's election cannot be load-aware — it has to complete under partition without
  waiting on load reports that may never arrive — so an unplanned leader loss lands wherever the randomized
  election happens to put it, which is not necessarily the least-loaded legal target. Each `ReconcileAsync`
  pass on the cluster leader (`ClusterCoordinator.RebalanceAsync`) compares the leader of every
  locally-hosted group against what `OrderCandidates` would pick, and asks for exactly one transfer when
  they disagree — only on the pass where the leader actually changed (`_lastKnownLeader`), never as a
  standing preference that could fight a leader for reasons of its own. Evaluated only for groups the
  cluster leader itself hosts, because `OrderCandidates` needs a local instance to answer at all.
- **Two things keep that reaction from becoming a herd, and both are load-bearing.** Load reports arrive on
  their own interval (2 s against a 5 s pass by default), so a pass decides every group against numbers that
  predate it. Without `_projectedLeaders` — the transfers this pass handed out, counted on top of each
  target's reported number until a report numbered after the transfer retires the projection — "who is
  carrying least?" answers with the same node for every group that failed over and moves all of them onto
  it. And a transfer this pass performed is written back into `_lastKnownLeader` as the expected leader:
  without that, the leadership change it caused reads as a fresh failover on the next pass, which transfers
  again, forever. Symptom of either one missing is the same from outside — the demo's groups continuously
  re-gathering on one node. `FailoverRebalancingSpreadsTheGroupsInsteadOfPilingThemOnOneNode` pins both, and
  it only reproduces with load reports slower than the reconcile pass, which is why it configures its own.
- **"A legal target" is two separate checks, and both defaults are derived rather than picked.**
  `LeaderTargetLagThreshold` (null → `MaxEntriesPerAppend`, one replication round) and
  `LeaderTargetContactWindow` (null → 2× `ElectionTimeout`, the coordinator's own availability window).
  Deliberately *not* `PromotionCatchUpThreshold`: promotion getting it wrong costs a weaker quorum, a
  handover getting it wrong stops writes for up to `LeadershipTransferCatchUpTimeout`. Liveness is checked
  separately because a node that just died still has a current-looking match index.
- **`NotResponding` must not be collapsed into `Unreachable`.** `Unreachable` means the group's *leader*
  could not be asked, so `ClusterCoordinator.ResolveAsync` stops walking candidates. `NotResponding` is a
  verdict about one candidate and the walk continues. Merging them turns one dead replica into a refusal to
  place the group anywhere.
- **Load reports are pushed, never logged.** Leader counts change on every election of every group; putting
  them through the cluster group's log would be a write firehose on the one group that must stay responsive.
  Soft state, held in memory on the cluster leader, aged out by TTL.
- **A node with history is handed `RaftMembership.Empty` on purpose, and its *role* must come from the
  replay, not from what it was handed.** `RaftGroupInstance.StartAsync` replays membership entries over the
  configuration it is given; reading `membership.IsVoter(Self)` (the parameter) instead of
  `Membership.IsVoter(Self)` (the replayed state) made every restarted node a learner, and learners never
  campaign. A whole cluster coming back from disk then sat at its last term forever with nobody standing for
  election, while the queue groups — which are handed a real voter set — kept working perfectly, so nothing
  looked broken except an administrative plane stuck on `AdminSuspended`. Covered by
  `ClusterOnSegmentedLogTests.ARestartedClusterElectsAnAdministrativeLeaderAgain`.
- **Bootstrap rule (`ClusterCoordinator.ShouldSeed`) is safety-critical.** A fresh node must NOT start as a
  voter: two empty logs are equally complete under the election restriction, so two fresh voters will elect
  one of themselves and truncate the node that actually had data. Fresh nodes start with
  `RaftMembership.Empty`, wait to be adopted as learners, and are promoted once caught up. Only the
  lowest-id node seeds a cold cluster.
- **Single-node → replicated is allowed and needs no mechanism; replicated → single-node is refused.**
  `ClusterModeStore` enforces this across restarts. The reverse direction would make a node pulled out of a
  cluster a second authority on the same group ids, with two real committed histories and no rule to
  reconcile them.
- **`SingleNodeRaftTransport` throws `InvalidOperationException`, not `IOException`.** In single-node mode
  nothing should ever address a peer; `IOException` would be retried forever by the replication loop instead
  of reporting the problem.

### The demo and its observer

- **Five nodes, so the quorum is three.** The demo exists to make failure states reachable by hand: one node
  down changes nothing, two still work, the third stops writes. With three nodes only the first and last of
  those exist.
- **The control plane lives inside the app host because it has to.** `ResourceCommandService`,
  `ResourceLoggerService` and `ResourceNotificationService` exist only in that process; anything outside it
  would be hunting OS process handles, which is precisely the platform-specific mess avoided everywhere else.
  It binds to loopback and refuses any resource name it was not given at construction — it can stop processes.
- **`ResourceLoggerService` is keyed by the DCP instance id, not the resource's display name.**
  `WatchAsync("raft-node-1")` does not throw; it yields nothing, forever, and looks exactly like a resource
  that has not logged yet. Resolve the id with `ResourceNotificationService.TryGetCurrentState(name, out e)`
  and use `e.ResourceId`. Commands, unlike logs, do accept the display name. A stream opened this way does
  survive the resource being restarted — measured, not assumed: the console output of the new process keeps
  arriving on the connection that was already open.
- **WAL size in the demo is measured off the filesystem, not counted.** `RaftNodeHost.MeasureWal` walks
  `<data>/wal/g<group padded to 20>` and sums the file lengths, which couples the demo to
  `SegmentedRaftWalFactory`'s directory naming on purpose: a counter of appended bytes would miss the
  allocated-but-not-yet-full segment, and the number people want is what the disk is holding.
- **The generated traffic is paced by the commit, not by the interval.** `POST /groups/{id}/messages` runs on
  the group's leader and waits for each append's durability before the next, so a 10 ms interval yields
  roughly 36–42 writes a second on this machine. The UI reports the *measured* rate for that reason; do not
  relabel it as the requested one.
- **The demo offers two different kinds of "out", and they must not be conflated.** Stopping a resource ends
  the process; `DELETE /cluster/nodes/{id}` leaves it running and takes it out of the cluster group's
  configuration. The second is a single-server membership change and only the cluster leader can serve it,
  which is why the observer routes it to the leader instead of to any answering node the way it routes
  placement. The removal is then undone by the coordinator's own reconcile pass — that is correct, not a bug.
- **Resource commands must not take the request's cancellation token.** A client that disconnects mid-flight
  would otherwise leave the command cancelled somewhere in the middle (`Command 'stop' was canceled` in the
  resource's log) with the resource in whichever state it reached.
- **The UI gets *one* log stream for all five nodes, and that is load-bearing.** A browser allows six
  concurrent connections per origin over HTTP/1.1. Five per-node log streams plus the cluster stream is
  exactly six, so every subsequent request — every button on the page — queued behind connections that never
  end. Nothing failed: the POSTs were issued and simply never got a turn, while curl against the same URL
  answered in six milliseconds. `LogStream` fans the five in on the server and tags each line with its node,
  which is the same trade the gRPC transport makes one layer down. Adding a second long-lived stream to that
  page reintroduces the bug.
- **An SSE endpoint must flush before its first payload.** ASP.NET Core holds the headers until something is
  written, so a stream whose source is quiet never begins its response and the client sees a hung request
  rather than an open stream.
- **Group ids reach the browser as strings.** `RaftGroupId.Cluster` is `ulong.MaxValue`, which a JavaScript
  number cannot hold exactly; sent as a number it arrives rounded and the UI addresses a group that does not
  exist.
- **Aspire's resource state and "does the node answer" are separate facts, and the UI keeps them apart.** A
  node can be `Running` and silent. Collapsing the two would hide the failure mode the demo exists to show.
- **The cluster leader shown in the UI is a tally of what each node believes, not a fact.** Nodes disagree
  during an election; the UI says "disputed" rather than picking one.

## No known open defects

`doc/open-issue-seed-visibility.md` is kept as a post-mortem, not as an open item. The cluster group's voter
set diverging over gRPC turned out to be `RaftFrameCodec` wrapping a recycled WAL buffer instead of copying
it — see the buffer-lifetime rule under "Code conventions", which is the general form of it.

Note that the Aspire demo persists to `demo/Blun.MultiRaft.AppHost/data/`. Log entries written before that
fix carry the wrong payload permanently, so a demo run against a pre-existing data directory still shows the
old divergence. Delete it to test a cold cluster.
