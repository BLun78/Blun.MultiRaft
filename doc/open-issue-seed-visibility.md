# Resolved: joining nodes did not see the seed in the cluster voter set

**Status: root cause found and fixed. Kept as a record — the way this hid is more instructive than the fix.**

## What was observed

After a cold three-node cluster formed over the gRPC transport, the nodes disagreed about who votes in the
cluster-management group:

```
GET /status
node 1 (seed):  "voters":[1,3,2]
node 2:         "voters":[3,2]
node 3:         "voters":[3,2]
```

Node 1 was the cluster leader and was missing from the other two nodes' configuration. Two views of who votes
are two different quorums; it is silent until it decides an election.

The seed's own membership log was exactly what the design intends:

```
node 1: applied membership change PromoteToVoter for node 1; 1 voters
        applied membership change AddLearner     for node 3; 1 voters
        applied membership change AddLearner     for node 2; 1 voters
        applied membership change PromoteToVoter for node 3; 2 voters
        applied membership change PromoteToVoter for node 2; 3 voters
```

Node 2's did not match it at all:

```
node 2: applied membership change PromoteToVoter for node 3; 1 voters   (x4)
        applied membership change PromoteToVoter for node 2; 2 voters
```

Five entries were replicated and five were applied. The count was right; four of the five carried the wrong
contents.

## The cause

`RaftFrameCodec.ToProto(in RaftLogEntry)` wrapped the entry's payload instead of copying it:

```csharp
Payload = UnsafeByteOperations.UnsafeWrap(entry.Payload),
```

`IRaftWal.ReadFromAsync` documents that the payload it yields is a slice of a pooled buffer the log owns and
**recycles on the next `MoveNextAsync`**. The replication loop hands that enumerable straight to the
transport, and `RaftStreamSession.AppendEntriesAsync` drains a whole round into one frame before sending it —
so every entry in a batch ended up pointing at the same recycled slot. The frame is then serialised later
still, on the outbound writer pump. By the time the bytes were read, they were the **last** entry's bytes.

Headers are copied by value, so terms and indices stayed correct. That is what made it silent: the follower's
consistency check passed, the entries appended at the right indices, the leader was acknowledged, and nothing
anywhere reported a failure. Only the contents were wrong. In the cluster group that reads as a follower
applying one membership change over and over and never seeing the others.

The same defect existed on the snapshot path: `FileRaftSnapshotStore.ReadAsync` refills one chunk array per
iteration, and `InstallSnapshotAsync` wrapped it while `SendAsync` merely queued the frame. Any snapshot
larger than one 64 KB chunk would have installed corrupt state on the receiver. Fixed in the same change,
before it was ever observed.

## Why every test passed

`InMemoryRaftTransport` detaches entries with `RaftLogEntry.ToOwned()` and chunks with `ToArray()`. The
in-process transport honoured the contract that the gRPC one broke — so no test on either WAL implementation
could see it, and the two implementations agreeing told us nothing. The bug lived exactly in the gap between
a documented lifetime contract and the one consumer that ignored it.

Regression tests are `GrpcTransportTests.EntriesReadFromTheLogArriveWithTheirOwnPayloads` and
`ASnapshotBodyLongerThanOneChunkArrivesIntact`. Both drive real `SegmentedRaftWal` / `FileRaftSnapshotStore`
output through a real Kestrel server, and both fail on the pre-fix code with the signature above: entry 1
arrives carrying entry 8's bytes.

## What was ruled out along the way, and cost time

- **The seed not writing its own entry.** It was a real bug, found first and fixed first: the seed's voter
  status used to exist only in the configuration handed to `StartAsync`. Fixing it did not fix the symptom,
  which is what made the next few hypotheses worse rather than better.
- **The in-memory log**, **the segmented log**, **replication failing outright**, **snapshots** — all
  eliminated by tests or by their absence from the logs, and all correctly eliminated. The mistake was
  concluding "so the transport is the prime suspect" and then looking at the transport's *framing and
  dispatch* rather than at what it does with the memory it is handed.

## The other half: a swallowed exception

`RaftStreamSession.HandleRequestAsync` caught `RpcException`, `InvalidOperationException` and `IOException`
and discarded them without a word, so that one group's failed request could not tear down the stream every
other group on the node pair shares. Not rethrowing is right. Saying nothing was not: `InvalidOperationException`
is this library's signal for an integrity violation — `SegmentedRaftWal.EnsureDense` raises exactly that for a
non-contiguous append — and a follower failing that way answers nothing at all, so the leader sees only a
timeout and retries forever.

That did not cause this defect, but it is why an entire class of follower-side failure was invisible from both
ends of the wire while it was being hunted. The handler now logs (`GrpcLog.RequestFailed`, event 1300) and
still does not rethrow. `GrpcRaftTransportOptions.Logger` and the DI-injected logger on `RaftProtocolService`
are how it reaches a sink.
