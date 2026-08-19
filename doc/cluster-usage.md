# Cluster management

How a node joins a cluster, who leads it, and how a group's leadership is moved.

The short version: **the library decides nothing about placement.** Nothing rebalances on a timer, a
threshold, or a heuristic. Leadership changes when Raft itself decides one is needed — a leader failed, a
node was partitioned away — or when a caller explicitly asks for a handover. What this layer offers is the
two things a caller cannot work out for itself, and then it gets out of the way:

- Is this node actually a legal target for that group's leadership?
- Which node is carrying the least right now?

## The cluster group

There is exactly one, at `RaftGroupId.Cluster`. It is an ordinary `RaftGroupInstance` — same election, same
log, same replication — hosted alongside the queue groups and ticked ahead of them.

Its **voter set is the cluster's node set**. Adding a node to the cluster is adding a voter to this group,
which means it goes through the single-server change path (Raft §4.1) that already exists rather than through
a second membership mechanism that would have to be kept in agreement with the first.

Two kinds of entry share its log and cannot be confused for one another:

| Entry kind | Written by | Applied by |
|---|---|---|
| `Membership` | the library, for node membership | the group itself; never reaches a state machine |
| `Command` | the host, for whatever administration means to it — users, access changes, placement policy | the `AdminStateMachine` you supply |

So the library owns node membership and nothing else. Everything else on that group is yours.

```csharp
var host = new MultiRaftHost(self, walFactory, metaStore, transport, snapshotStore);
await host.StartAsync();

await using var cluster = new ClusterCoordinator(host, new ClusterOptions
{
    Mode = ClusterMode.Replicated,
    Self = self,
    Nodes = [new NodeId(1), new NodeId(2), new NodeId(3)],
    DataDirectory = "/var/lib/blun/cluster",
    AdminStateMachine = myAdminStateMachine,
});

await cluster.StartAsync();
```

`RaftGroupId.Cluster` is `ulong.MaxValue`, and ids at or above `RaftGroupId.ReservedFloor` are refused by
`MultiRaftHost.AddGroupAsync`. It is deliberately not `0`: that is `default(RaftGroupId)`, and a struct
nobody initialised must not end up addressing the administrative plane.

## Joining, and why a fresh node waits

A node that has never run before starts with **an empty configuration** and waits to be adopted. It does not
campaign, because it is not a voter as far as it knows.

That is not caution, it is the whole safety argument. Under the election restriction (Raft §5.4.1) two empty
logs are exactly as complete as each other, so two fresh voters will happily elect one of themselves — and if
a third node in that set actually held data, the winner's empty log would then truncate it away. Starting
empty-handed removes that outcome rather than making it unlikely.

Which leaves the question of who starts a cluster that is entirely new. The **lowest node id seeds it**: it
begins as its own single voter, wins an election nobody else takes part in, and then adopts the others as
learners and promotes them once they have caught up. It is the one choice every node reaches independently
without being told.

If the seed is down, a brand-new cluster does not form. That is correct — there is no data yet to be
unavailable, and it forms the moment the seed appears.

The seed also writes **its own voter status into the log** as an ordinary membership entry. Without that the
configuration would be asymmetric in a way that stays hidden for a while: the seed is a voter because its
startup configuration said so, and joining nodes rebuild the configuration by replaying membership entries —
so they would see every node the seed added and not the seed itself. Two views of who votes is two different
quorums, and the disagreement is silent right up until it decides an election.

## Single-node, and growing out of it

`ClusterMode.SingleNode` is a cluster of one. Everything works as it does in a replicated cluster; there is
simply nobody to agree with, and no wire:

```csharp
await using var cluster = new ClusterCoordinator(host, new ClusterOptions
{
    Mode = ClusterMode.SingleNode,
    Self = new NodeId(1),
    DataDirectory = "/var/lib/blun/cluster",
});
```

A single-voter group never touches a transport — the vote round returns before sending anything, pre-vote is
skipped, the read barrier answers from itself, and the peer set is empty. `SingleNodeRaftTransport` exists for
the constructor signature, and **throws on every call**, deliberately `InvalidOperationException` rather than
`IOException`: a call arriving there means the configuration grew past this node, and an `IOException` would
be retried forever by the replication loop instead of reported.

**The mode is fixed for the life of the process, and recorded across restarts** in `cluster.mode` under the
data directory. One direction is allowed and one is not:

- **Single-node → replicated is allowed**, implicitly: restart with a peer list and it happens. It needs no
  special mechanism because the node that ran alone holds the log and the nodes joining it are new and hold
  nothing, so the election restriction settles which log wins without being asked. The newcomers join as
  learners and are caught up from the existing log.
- **Replicated → single-node is refused.** A node pulled out of a cluster still holds entries that cluster
  committed; running it alone makes it a second authority on the same group ids, and there is no rule that
  reconciles two real committed histories afterwards. Better refused at startup than discovered later.

Without a `DataDirectory` the marker lives in memory only and the mode is not enforced across restarts. That
is fine for tests and wrong for a real node; the coordinator logs a warning saying so.

## Whether this node can serve administration

```csharp
if (cluster.State == ClusterServiceState.Available) { /* admin operations work */ }
```

`AdminSuspended` means no leader for the cluster group is reachable — this node is in a minority. **Queue
groups are untouched by that**: each has its own quorum, and losing the administrative one does not take them
with it. The state is trustworthy rather than assumed because the cluster group runs with `CheckQuorum` on: a
leader that stops hearing from a majority steps aside instead of continuing to believe it leads.

`CheckQuorum` is available for ordinary groups too, and off by default there:

```csharp
new RaftGroupOptions { CheckQuorum = true }
```

## Moving a group's leadership

Three calls, in increasing order of commitment. All of them are answered by the **group's current leader**,
wherever it is — routed there for you — because the judgement rests on per-peer match indices and no other
replica has them.

```csharp
// "Could node 7 take this group?" Changes nothing.
LeaderTargetResponse check = await cluster.CheckLeaderTargetAsync(group, new NodeId(7));

// "Node 7 if it can, otherwise the best alternative." Still changes nothing.
LeaderTargetResponse pick = await cluster.ResolveLeaderTargetAsync(group, preferred: new NodeId(7));

// The same choice, carried out.
LeaderTargetResponse moved = await cluster.RequestLeaderTransferAsync(group, preferred: new NodeId(7));
```

`Status` says why a node was refused — `NotAMember`, `NotAVoter`, `Lagging` (with `Lag` in entries),
`TransferInFlight`, `NoLeader`, `Unreachable` — or `Valid`, or `IsCurrentLeader`, which is not an error but
"there is nothing to do". `Transferred` says whether leadership actually moved, and is `false` for the two
query forms.

**The group is never left without a leader.** A target that turns out to be unsuitable, declines, or cannot
be reached simply does not receive leadership, and the node holding it keeps it. There is no step that gives
leadership up before a successor has accepted.

Omitting `preferred` lets the group's own leader choose the closest-logged voter. Passing one and having it
refused falls back to the ordering below.

### How candidates are ordered

Least group-leaderships first, **the cluster leader last**, node id breaking ties.

The cluster leader is sorted last rather than filtered out. As a filter it would leave a single-node cluster —
or any group whose only other voter is the cluster leader — with no legal target at all, turning a preference
into an outage. As a sort key it is simply never chosen while anyone else will do.

A node that has not reported recently ranks below every node that has. There is no evidence it is idle, and
preferring an unmeasured node over a measured empty one would be a guess dressed up as a decision.

### Where the numbers come from

Each node pushes `{ groups, leaders }` to the cluster leader every couple of seconds. **Never through the
log**: leader counts change on every election of every group, and replicating them would turn an advisory
number into a write firehose on the one group that has to stay responsive. It is soft state, held in memory
on the leader and aged out by `LoadReportTtl`, and rebuilt within one report interval after an election.

Push rather than pull, so a node that dies simply stops reporting; pulling would cost the leader one round
trip per node with timeouts that serialize.

```csharp
ClusterLoadSnapshot load = cluster.GetLoad();   // empty on any node that is not the cluster leader
```

Transports opt in by implementing `IRaftClusterTransport`, discovered with an `is` check — the same shape
`IRaftSnapshotableStateMachine` already uses. A transport that does not keeps compiling, and the coordinator
degrades honestly: no load model, so placement follows only what the caller names explicitly.

## Events

Two kinds, and the split is between acting and watching.

```csharp
await foreach (ClusterEvent evt in cluster.Events)
{
    if (evt.Kind == ClusterEventKind.NodeBecameAvailable)
    {
        // Your cue to re-place some group leaderships onto evt.Node, if you want to.
    }
}
```

| Event | Raised on | For |
|---|---|---|
| `NodeBecameAvailable` | **the cluster leader only** | acting on |
| `ClusterStateChanged` | every node | keeping your own bookkeeping straight |

`NodeBecameAvailable` is raised in exactly one place on purpose. A node coming back is the moment a host may
want to move work onto it, and if every node raised it, every node's consumer would decide that at the same
instant and fire the same handovers at the same groups.

It is a stream rather than a C# `event`, and not for taste: a handler invoked inline would run consumer code
of unknown duration on the caller's stack, and this library has been bitten by exactly that shape before —
work started on the tick loop's own stack kept the loop from ever observing shutdown. It is meant for one
consumer; events are handed out, not broadcast. The channel is bounded and drops the oldest, so a consumer
that stops reading cannot grow it without limit.

## Creating groups with a placement preference

```csharp
await cluster.CreateGroupAsync(
    new RaftGroupId(42),
    RaftMembership.OfVoters(node1, node2, node3),
    initialLeader: node1,
    stateMachine: queueStateMachine);
```

This is **local** — every node that should hold a replica calls it for itself. The library does not push group
creation across the cluster, because which queues exist is your catalogue, and you already have a replicated
place to keep it: the cluster group's log.

The `initialLeader` hint is honoured by campaigning immediately on the named node, which costs nothing — at
creation every replica's log is empty, so the election restriction cannot separate them and the first
candidate wins. Doing it this way rather than electing at random and transferring afterwards saves a handover
the group would otherwise pay for within seconds of existing.

This is the lever for producer affinity: an exchange lands on node A, and the queue bound to it is created
with `initialLeader: A`, so the queue's WAL — which *is* the queue's storage — is written on the node the
producer is already talking to.

## Demo

The Aspire scenario exposes all of it over HTTP:

```
GET  /status                          # role, term and leader per group, plus this node's cluster view
GET  /cluster/load                    # the leader's picture; empty on the other nodes
POST /cluster/groups/{group}/leader    # optional ?node=<id>; omit to let the group's leader choose
```

```bash
dotnet run --project demo/Blun.MultiRaft.AppHost/Blun.MultiRaft.AppHost.csproj
curl -s http://127.0.0.1:8103/cluster/load
curl -s -X POST "http://127.0.0.1:8101/cluster/groups/1/leader?node=3"
```
