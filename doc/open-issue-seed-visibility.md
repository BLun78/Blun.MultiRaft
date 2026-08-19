# Open defect: joining nodes do not see the seed in the cluster voter set

**Status: reproducible in the Aspire demo, not reproduced in-process. Not fixed.**

## What is observed

After a cold three-node cluster forms over the gRPC transport, the nodes disagree about who votes in the
cluster-management group:

```
GET /status
node 1 (seed):  "voters":[1,3,2]
node 2:         "voters":[3,2]
node 3:         "voters":[3,2]
```

Node 1 is the cluster leader and is missing from the other two nodes' configuration. Two views of who votes
is two different quorums; it is silent until it decides an election.

## What the logs show

Membership changes are logged where they are applied (event 1004), on every node. The seed's log is exactly
what the design intends:

```
node 1: applied membership change PromoteToVoter for node 1; 1 voters
        applied membership change AddLearner    for node 3; 1 voters
        applied membership change AddLearner    for node 2; 1 voters
        applied membership change PromoteToVoter for node 3; 2 voters
        applied membership change PromoteToVoter for node 2; 3 voters
```

Node 2's does not match it at all:

```
node 2: Raft group ... started on node 2 at term 0, last index 0
        node 2 stepped down to term 1
        applied membership change PromoteToVoter for node 3; 1 voters   (x4)
        applied membership change PromoteToVoter for node 2; 2 voters
```

So node 2 starts empty, then applies only the *last two* membership entries — and applies one of them four
times. It never applies `PromoteToVoter(1)`, `AddLearner(3)` or `AddLearner(2)`.

## What has been ruled out

- **The seed not writing its own entry.** It does; the entry is in its log and it is logged. This was a real
  bug and is fixed — the seed's voter status used to exist only in the configuration handed to `StartAsync`,
  and the fix is retried from the reconciliation pass rather than attempted once, because the coordinator's
  opening campaign races the tick loop's own queued campaign for the same group.
- **The in-memory log.** `ClusterCoordinatorTests.AColdClusterIsSeededByTheLowestNodeIdAndAdoptsTheRest`
  asserts every node agrees on the voter set, and fails if the seed's entry is removed.
- **The segmented log.** `ClusterOnSegmentedLogTests.AColdClusterFormsAndEveryNodeAgreesOnTheVoterSet` runs
  the same scenario on file-backed stores and passes.
- **Replication failing outright.** The leader logs no replication failures (event 1007) for this group, so
  node 2 is answering successfully.
- **Snapshots.** No compaction threshold is configured, and no snapshot is sent or installed.

The remaining difference between the passing tests and the failing demo is the transport: the tests use the
in-process cluster, the demo uses gRPC.

## Where to look next

Node 2 applying an entry four times means it appended it four times, which means it was not recognised as
already present each time — either its `LastIndex` is not advancing, or the entry at that index keeps being
truncated and rewritten. Both point at the same question, which is the one to answer first:

**Why does node 2's consistency check pass for a `PrevLogIndex` its empty log cannot possibly satisfy?**

`TermAtAsync` answers `-1` for any index above `LastIndex` in both log implementations, so the check should
reject. Two things worth instrumenting:

1. Log `PrevLogIndex`/`PrevLogTerm` and the local answer on every rejected and accepted `AppendEntries`, and
   compare what the leader sent with what the follower decoded — `RaftFrameCodec` and the proto's default-value
   elision are the obvious place for a field to arrive as 0.
2. `RaftStreamSession.HandleRequestAsync` swallows `InvalidOperationException` so that one group's failure
   cannot tear down the shared stream. `SegmentedRaftWal.EnsureDense` throws exactly that for a
   non-contiguous append. A follower failing this way answers nothing and the caller only sees a timeout, so
   any such failure is currently invisible — worth logging before anything else is investigated.

## Reproducing

```bash
dotnet run --project demo/Blun.MultiRaft.AppHost/Blun.MultiRaft.AppHost.csproj
# wait ~30s, then
for p in 8101 8102 8103; do curl -s http://127.0.0.1:$p/status | grep -o '"voters":\[[0-9,]*\]'; done
```
