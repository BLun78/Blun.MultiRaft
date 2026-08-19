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

# Aspire demo: three nodes, real gRPC, /status on 8101-8103
dotnet run --project demo/Blun.MultiRaft.AppHost/Blun.MultiRaft.AppHost.csproj
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

  EventId ranges: 1000–1015 Core Raft (`RaftGroupInstance.Log`), 1100+ Host (`HostLog`), 1200+ Cluster
  coordinator (`ClusterLog`), 2000+ Demo node (`NodeStartedLog`).
- Code must be trim-safe and AOT-compatible.
- Throw `InvalidOperationException` for integrity violations; `IOException` for transport failures. This
  distinction is load-bearing: the replication loop treats `IOException` as an ordinary retryable condition,
  so throwing it for a real integrity problem makes a group spin silently forever.

## Architecture

Two libraries, dependency runs one way only: `Blun.MultiRaft.Wal` references nothing from the consensus
core. This is what makes the storage seam real — the log can be referenced, tested, benchmarked or replaced
without dragging elections and transports along.

| Project | Contents |
|---|---|
| `src/Blun.MultiRaft.Wal` | `RaftGroupId`, `RaftLogEntry`, `IRaftWal` and its implementations (`InMemoryRaftWal`, `SegmentedRaftWal`) |
| `src/Blun.MultiRaft` | `NodeId`, `RaftGroupInstance`, membership, transport contracts, `MultiRaftHost`, `ClusterCoordinator` |
| `src/Blun.MultiRaft.Grpc` | gRPC transport implementation |
| `demo/Blun.MultiRaft.Node` + `demo/Blun.MultiRaft.AppHost` | Aspire-based demo cluster |

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
- **The library decides nothing about placement.** Nothing rebalances on a timer or a threshold. The
  coordinator answers two questions (is this node a legal target? who is carrying least?) and performs a
  handover when asked. `ClusterEventKind.NodeBecameAvailable` fires on the **cluster leader only** so one
  reactor decides, not N.
- **Load reports are pushed, never logged.** Leader counts change on every election of every group; putting
  them through the cluster group's log would be a write firehose on the one group that must stay responsive.
  Soft state, held in memory on the cluster leader, aged out by TTL.
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

## Known open defect

`doc/open-issue-seed-visibility.md` — over the gRPC transport, nodes joining a cold cluster do not see the
seed in the cluster group's voter set. Reproducible in the Aspire demo, **not** reproduced in-process on
either WAL implementation, so the transport is the prime suspect. Read that file before touching
`AbsorbEntriesAsync`, `RaftFrameCodec`, or `RaftStreamSession.HandleRequestAsync` — the last of these
swallows `InvalidOperationException`, which is what makes a follower-side append failure invisible.
