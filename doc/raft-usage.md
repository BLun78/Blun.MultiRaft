# Using `Blun.MultiRaft`

The consensus core: `RaftGroupInstance` (one Raft group — one queue) and `MultiRaftHost` (every group on one
node, sharing a clock). This document assumes you already have a transport — either
[`Blun.MultiRaft.Grpc`](grpc-usage.md) for a real cluster, or the in-process one below for getting something
running in five minutes — and a WAL, covered in [wal-usage.md](wal-usage.md).

```
dotnet add package Blun.MultiRaft
```

## The fastest way to see it work: no network at all

`InMemoryRaftCluster` (shipped in this package, not test-only) wires several nodes together in one process
with zero real I/O. This is the quickest way to see an election happen:

```csharp
using Blun.MultiRaft;
using Blun.MultiRaft.Hosting;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;

var network = new InMemoryRaftCluster();
var hosts = new Dictionary<NodeId, MultiRaftHost>();

foreach (ulong id in (ulong[])[1, 2, 3])
{
    var self = new NodeId(id);
    MultiRaftHost host = null!;
    IRaftProtocolTransport transport = network.Connect(self, new Listener(() => host)); // see below
    host = new MultiRaftHost(self, new InMemoryRaftWalFactory(), new InMemoryRaftMetaStore(), transport);
    hosts[self] = host;
    await host.StartAsync();
}

RaftMembership membership = RaftMembership.OfVoters(hosts.Keys.ToArray());
var group = new RaftGroupId(42);
foreach (MultiRaftHost host in hosts.Values)
{
    await host.AddGroupAsync(group, membership);
}

// Give the election a moment, then find the leader.
await Task.Delay(500);
RaftGroupInstance leader = hosts.Values
    .Select(h => h.TryGetGroup(group, out RaftGroupInstance? g) ? g : null)
    .Single(g => g?.IsLeader == true)!;

long index = await leader.AppendAsync("hello"u8.ToArray());
```

`Connect` needs the listener *before* the host that will implement it exists — `MultiRaftHost` needs the
transport at construction, and the transport needs somewhere to deliver inbound calls to. A small
indirection breaks the chicken-and-egg:

```csharp
sealed class Listener(Func<MultiRaftHost> resolve) : IRaftProtocolListener
{
    public ValueTask<AppendEntriesResponse> OnAppendEntriesAsync(AppendEntriesRequest r, IAsyncEnumerable<RaftLogEntry> e, CancellationToken ct = default)
        => resolve().OnAppendEntriesAsync(r, e, ct);
    public ValueTask<InstallSnapshotResponse> OnInstallSnapshotAsync(InstallSnapshotRequest r, IAsyncEnumerable<ReadOnlyMemory<byte>> b, CancellationToken ct = default)
        => resolve().OnInstallSnapshotAsync(r, b, ct);
    public ValueTask<ReadIndexResponse> OnReadIndexAsync(ReadIndexRequest r, CancellationToken ct = default)
        => resolve().OnReadIndexAsync(r, ct);
    public ValueTask<TimeoutNowResponse> OnTimeoutNowAsync(TimeoutNowRequest r, CancellationToken ct = default)
        => resolve().OnTimeoutNowAsync(r, ct);
    public ValueTask<VoteResponse> OnRequestVoteAsync(VoteRequest r, CancellationToken ct = default)
        => resolve().OnRequestVoteAsync(r, ct);
}
```

`MultiRaftHost` itself implements `IRaftProtocolListener`, which is exactly what this forwards to — so for a
real cluster, where the host already exists by the time you build the transport, you don't need this at all
(pass the host straight through, as [grpc-usage.md](grpc-usage.md) does).

## The two ways to run a group

**`MultiRaftHost`** — one per node, owns the shared tick loop and the storage factories, and is what you
should reach for. This is the only thing that scales to many groups per node: a `Timer` per `RaftGroupInstance`
is the cost this design exists to avoid, and `MultiRaftHost` is where that sharing lives.

```csharp
var host = new MultiRaftHost(
    self: new NodeId(1),
    walFactory: new SegmentedRaftWalFactory("./data/wal"),
    metaStore: new FileRaftMetaStore("./data/meta"),
    transport: grpcTransport,                      // any IRaftProtocolTransport
    snapshotStore: new FileRaftSnapshotStore("./data/snapshots"), // optional -- omit if you never compact
    tickInterval: TimeSpan.FromMilliseconds(25));   // default; one loop drives every group's clock

await host.StartAsync();

RaftGroupInstance group = await host.AddGroupAsync(
    new RaftGroupId(42),
    RaftMembership.OfVoters(new NodeId(1), new NodeId(2), new NodeId(3)),
    stateMachine: myStateMachine,   // see "The state machine" below; null is fine if you only care about the log
    options: new RaftGroupOptions { /* see raft-usage.md#tuning */ });
```

`AddGroupAsync` is idempotent per group id — calling it again for a group already hosted returns the existing
instance rather than creating a second one. `RemoveGroupAsync(group, deleteData: true)` stops a group and, if
asked, deletes its WAL/meta/snapshot state — call that when the *queue* the group backed has been deleted,
not when you're just draining a node (`TransferLeadershipAsync`, below, is for that).

**`RaftGroupInstance` directly** — construct one yourself only if you're not using `MultiRaftHost` at all
(e.g. embedding this in something with its own scheduling). You then own calling `TickAsync` periodically and
wiring `IRaftProtocolListener` to its `OnAppendEntriesAsync`/`OnRequestVoteAsync`/`OnInstallSnapshotAsync`/
`OnReadIndexAsync`/`OnTimeoutNowAsync` methods yourself — which is exactly what `MultiRaftHost` already does
for you, so there's rarely a reason to.

```csharp
var instance = new RaftGroupInstance(
    group: new RaftGroupId(42),
    self: new NodeId(1),
    wal: await new SegmentedRaftWalFactory("./data/wal").OpenAsync(new RaftGroupId(42)),
    metaStore: new FileRaftMetaStore("./data/meta"),
    transport: grpcTransport,
    stateMachine: myStateMachine,      // optional, default discards everything
    options: myOptions,                // optional, default RaftGroupOptions()
    timeProvider: null,                // optional, default TimeProvider.System
    logger: null,                      // optional, default NullLogger
    snapshotStore: mySnapshotStore);   // optional, needed only for TakeSnapshotAsync

await instance.StartAsync(RaftMembership.OfVoters(/* ... */));
```

## The state machine

`RaftGroupInstance` replicates bytes; deciding what they *mean* is yours:

```csharp
public sealed class QueueStateMachine : IRaftStateMachine
{
    public ValueTask ApplyAsync(RaftGroupId group, RaftLogEntry entry, CancellationToken cancellationToken = default)
    {
        // entry.Payload aliases a buffer the log recycles right after this call returns -- copy anything
        // you need to keep past it.
        HandleCommand(entry.Index, entry.Payload.Span);
        return ValueTask.CompletedTask;
    }
}
```

`ApplyAsync` is called once per committed entry, strictly in index order, never concurrently for the same
group. `entry.Kind` is always `RaftEntryKind.Command` for entries `ApplyAsync` sees — `NoOp` and `Membership`
entries are handled by the library itself and never reach your state machine.

If you also want [automatic or manual compaction](#snapshots-and-compaction), implement
`IRaftSnapshotableStateMachine` instead (it extends `IRaftStateMachine`, so `ApplyAsync` is the same method):

```csharp
public sealed class QueueStateMachine : IRaftSnapshotableStateMachine
{
    public ValueTask ApplyAsync(RaftGroupId group, RaftLogEntry entry, CancellationToken cancellationToken = default) { /* ... */ }

    public async IAsyncEnumerable<ReadOnlyMemory<byte>> CaptureAsync(
        RaftGroupId group, long lastAppliedIndex, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Stream your state out in chunks -- there's no size bound on a snapshot.
        yield return SerializeState();
    }

    public async ValueTask RestoreAsync(
        RaftGroupId group, IAsyncEnumerable<ReadOnlyMemory<byte>> body, CancellationToken cancellationToken = default)
    {
        // Wholesale replacement: discard whatever state you had first. A partial restore would leave
        // this replica agreeing with nobody.
        Reset();
        await foreach (ReadOnlyMemory<byte> chunk in body.WithCancellation(cancellationToken))
        {
            Ingest(chunk.Span);
        }
    }
}
```

If you pass `stateMachine: null`, you get `NullRaftStateMachine` — the log still replicates and commits
normally, it just discards every entry. Fine for groups whose value *is* the log itself.

## Writing

```csharp
long index = await group.AppendAsync(payload);
```

Only the leader can append; a follower throws `NotLeaderException`, which carries `Leader` — the node id
this instance currently believes leads, or `null` if it doesn't know — so a client can redirect instead of
retrying blindly:

```csharp
try
{
    await group.AppendAsync(payload);
}
catch (NotLeaderException ex) when (ex.Leader is { } leader)
{
    RedirectTo(leader);
}
```

`AppendAsync` returns once `RaftGroupOptions.Durability` is satisfied (default `Quorum` — a majority of
voters have the entry; the real Raft guarantee). `LeaderBuffered`/`LeaderDurable` return earlier and are
correspondingly weaker: a client that received a successful `AppendAsync` under one of those and later
observes the entry gone (leader crashed before fsync, or lost an election before replicating) isn't hitting a
bug — that's what the chosen level promised. Read `DurabilityLevel`'s doc comments in the source before
picking anything but `Quorum`.

`AppendAsync` also takes an optional `applicationTag` byte, defaulting to 0, that lands unread in
`RaftEntryHeader.ApplicationTag` — see [wal-usage.md](wal-usage.md#applicationtag-and-rebuilding-an-index-without-the-payload)
for what it's for and how to read it back cheaply on every replica.

## Reading: linearizable, without appending anything

A naive read from a follower's local state can be stale — for a queue, that's not "slightly old data," it's a
duplicate delivery or a lost settlement. `ReadIndexAsync` (Raft §6.4) gets you a linearizable read without
paying for a log entry:

```csharp
long readyIndex = await group.PrepareLinearizableReadAsync();
// After this returns, this replica has applied everything that was committed when the read began.
// Reading its local state now is linearizable.
ReadLocalState();
```

`PrepareLinearizableReadAsync` is `ReadIndexAsync` (get the index to wait for) plus `WaitForAppliedAsync`
(block until this replica has caught up to it) in one call. On the leader, `ReadIndexAsync` confirms it still
leads by exchanging heartbeats with a quorum before answering — a partitioned leader that skipped this check
would hand back a stale read dressed up as a linearizable one. On a follower, it asks the leader over the
transport; if there's no known leader yet, or the leader is unreachable, it throws.

## Membership: joining and leaving

Single-server changes only (Raft §4.1) — one node at a time, no joint consensus:

```csharp
await leader.AddLearnerAsync(newNodeId);          // non-voting; always safe, never moves the quorum

// Poll or wait until the learner's log has caught up -- PromoteToVoterAsync refuses otherwise:
bool promoted = await leader.PromoteToVoterAsync(newNodeId); // false if still too far behind

await leader.RemoveNodeAsync(departingNodeId);     // voter or learner, either way
```

`PromoteToVoterAsync` compares the learner's replicated index against
`RaftGroupOptions.PromotionCatchUpThreshold` (default 64 entries) and returns `false` without doing anything
if it's still too far behind — promoting a replica that can't yet serve reads just hands the quorum a member
that slows it down. Call it again once the learner's caught up.

## Draining a node: leadership transfer

```csharp
bool moved = await leader.TransferLeadershipAsync(targetNodeId); // or omit the target to auto-pick
```

This is for planned maintenance — get leadership off a node *before* you take it down, rather than waiting
for an election to notice it's gone. It blocks new writes for its duration
(`LeadershipTransferInProgressException` on anything that tries to append while it's in flight), waits for
the target's log to actually match, then hands over. Returns `false` — leaving the original leader in
charge — if the target couldn't be caught up in time
(`RaftGroupOptions.LeadershipTransferCatchUpTimeout`, default ten election timeouts) or was unreachable; your
code should treat that as "try again," not as an error.

## Snapshots and compaction

```csharp
bool compacted = await group.TakeSnapshotAsync();
```

Captures applied state via `IRaftSnapshotableStateMachine.CaptureAsync`, stores it durably, *then* truncates
the log up to that point — in that order, deliberately, so a crash between the two steps can never leave the
state in neither place. Returns `false` if there's no snapshotable state machine or nothing new to compact.

To do this automatically instead of calling it yourself:

```csharp
var options = new RaftGroupOptions { AutoCompactionThreshold = 10_000 };
```

The group checks after every batch of newly-applied entries whether it's crossed this many entries since the
log's retained start, and if so fires `TakeSnapshotAsync` in the background (never inline on the tick — a
slow `CaptureAsync` must not hold up the group's clock). There's no default threshold: how large your state
machine's captured state actually is, and so where the disk-versus-recompaction-work trade lands, is
something only you know.

A replica that falls behind the leader's *compacted* log gets caught up automatically via `InstallSnapshot`
the next time the leader tries to replicate to it and finds it needs entries that no longer exist — you don't
call anything for this to happen, it's part of the normal replication loop.

## Tuning: `RaftGroupOptions`

```csharp
var options = new RaftGroupOptions
{
    ElectionTimeout = TimeSpan.FromMilliseconds(300),  // base; actual timeout adds jitter on top
    ElectionJitter = 1.0,                              // fraction of ElectionTimeout added, randomized
    HeartbeatInterval = TimeSpan.FromMilliseconds(75),
    MaxEntriesPerAppend = 256,
    Durability = DurabilityLevel.Quorum,
    PreVote = true,
    PromotionCatchUpThreshold = 64,
    LeadershipTransferCatchUpTimeout = null,           // null = 10x ElectionTimeout
    AutoCompactionThreshold = null,                    // null = never compacts automatically
};
```

`ElectionJitter` matters more than it looks like it should: without it, followers that lost the same leader
at the same moment all time out together and split the vote — and with many groups sharing a node, they'd do
it in lockstep. Don't turn it down for a "faster" cluster; it's not there for looks.

`PreVote` (on by default, Raft §9.6) stops a node that was partitioned away and comes back with a
far-ahead term from forcing a healthy group through a needless election. Leave it on unless you have a
specific reason not to.

## Logging

Every log call in this library goes through source-generated `[LoggerMessage]` methods — pass an `ILogger`
via the `logger:` parameter on `RaftGroupInstance`/`MultiRaftHost` and you get structured, allocation-free
logging for elections, replication failures, snapshot events and membership changes for free. Passing `null`
uses `NullLogger` and costs nothing.
