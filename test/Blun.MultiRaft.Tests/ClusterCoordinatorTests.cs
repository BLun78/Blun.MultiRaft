// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Collections.Immutable;
using System.Text;
using Blun.MultiRaft.Cluster;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Tests;

/// <summary>
/// The cluster-management group: how it bootstraps, who it lets lead, and what it refuses.
/// </summary>
public sealed class ClusterCoordinatorTests
{
    private static readonly RaftGroupId Queue = new(4242);

    [Fact]
    public async Task ASingleNodeClusterLeadsItselfWithoutATransport()
    {
        // The claim being tested is that a single-voter group never touches the wire: the vote round returns
        // before sending, pre-vote is skipped, there are no peers to replicate to. SingleNodeRaftTransport
        // throws on every method, so any call at all fails the test rather than passing unnoticed.
        await using var cluster = new ClusterTestNode(1, ClusterMode.SingleNode, SingleNodeRaftTransport.Instance);
        await cluster.StartAsync();

        await ClusterTestNode.WaitUntilAsync(
            () => cluster.Coordinator.IsClusterLeader,
            "the lone node to lead its own cluster");

        Assert.Equal(ClusterServiceState.Available, cluster.Coordinator.State);
        Assert.Equal(new NodeId(1), cluster.Coordinator.ClusterLeader);

        // And it is a working Raft group, not a stub: an administrative append commits on itself alone.
        long index = await cluster.Coordinator.AppendAdminAsync(Encoding.UTF8.GetBytes("create-user:ada"));
        Assert.True(index > 0);
        Assert.True(cluster.Coordinator.Group!.CommitIndex >= index);
    }

    [Fact]
    public async Task TheClusterGroupIdIsReservedAgainstOrdinaryGroups()
    {
        await using var node = new ClusterTestNode(1, ClusterMode.SingleNode, SingleNodeRaftTransport.Instance);
        await node.StartAsync();

        // Hosting a queue on the cluster group's id would put two unrelated sets of entries in one log.
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await node.Host.AddGroupAsync(RaftGroupId.Cluster, RaftMembership.OfVoters(new NodeId(1))));
    }

    [Fact]
    public async Task AColdClusterIsSeededByTheLowestNodeIdAndAdoptsTheRest()
    {
        // The bootstrap rule in full. Every node is fresh, so only the lowest id may start as a voter; the
        // others must wait to be adopted rather than electing one of themselves, because two empty logs are
        // equally complete under the election restriction and the loser's data -- if it had any -- would go.
        await using var cluster = new ClusterTestCluster();
        // Node 3 starts first on purpose: the seed is decided by the id, never by who came up first.
        await cluster.AddNodesAsync(3, 1, 2);

        await ClusterTestCluster.WaitUntilAsync(
            () => cluster.Node(1).Coordinator.IsClusterLeader,
            "node 1 -- the lowest id -- to seed the cluster");

        // Nodes 2 and 3 never campaign: they start with an empty configuration and are pulled in as learners.
        await ClusterTestCluster.WaitUntilAsync(
            () => cluster.Node(1).Coordinator.Group!.Membership.Voters.Length == 3,
            "the seed to adopt and promote both other nodes",
            TimeSpan.FromSeconds(20));

        Assert.False(cluster.Node(2).Coordinator.IsClusterLeader);
        Assert.False(cluster.Node(3).Coordinator.IsClusterLeader);

        foreach (ulong id in (ulong[])[1, 2, 3])
        {
            Assert.Equal(ClusterServiceState.Available, cluster.Node(id).Coordinator.State);
        }

        // And every node has to agree on who votes, the seed included. The joining nodes rebuild the
        // configuration by replaying membership entries, so a seed whose own voter status exists only in the
        // configuration it was handed at startup -- and not in the log -- is invisible to them. That
        // disagreement is silent right up until it decides an election with the wrong quorum.
        await ClusterTestCluster.WaitUntilAsync(
            () => cluster.Nodes.All(n => n.Coordinator.Group!.Membership.Voters.Length == 3),
            "every node to see the whole voter set, seed included",
            TimeSpan.FromSeconds(20));

        foreach (ClusterTestNode node in cluster.Nodes)
        {
            ImmutableArray<NodeId> voters = node.Coordinator.Group!.Membership.Voters;
            Assert.Equal<NodeId>([new(1), new(2), new(3)], [.. voters.OrderBy(v => v)]);
        }
    }

    [Fact]
    public async Task AdministrativeCommandsReplicateToEveryNode()
    {
        await using var cluster = new ClusterTestCluster();
        var machines = new Dictionary<ulong, RecordingStateMachine>();
        await cluster.AddNodesAsync(id => machines[id] = new RecordingStateMachine(), 1, 2, 3);

        await ClusterTestCluster.WaitUntilAsync(
            () => cluster.Node(1).Coordinator.Group!.Membership.Voters.Length == 3,
            "the cluster to form",
            TimeSpan.FromSeconds(20));

        await cluster.Node(1).Coordinator.AppendAdminAsync(Encoding.UTF8.GetBytes("grant:ada:publish"));

        // Membership entries never reach a state machine and administrative commands always do, which is what
        // lets the library's own bookkeeping and the host's share one log without either reading the other.
        await ClusterTestCluster.WaitUntilAsync(
            () => machines.Values.All(m => m.Applied.Any(
                a => Encoding.UTF8.GetString(a.Payload) == "grant:ada:publish")),
            "the administrative command to reach every node",
            TimeSpan.FromSeconds(20));

        Assert.All(machines.Values, m => Assert.Single(m.Applied));
    }

    [Fact]
    public async Task LoadReportsReachTheClusterLeaderAndOrderPlacement()
    {
        await using var cluster = new ClusterTestCluster();
        await cluster.AddNodesAsync(1, 2, 3);
        await cluster.WaitForFormedAsync();

        // Give node 3 two group leaderships and node 2 none, then check the ordering reflects it.
        await cluster.CreateGroupEverywhereAsync(new RaftGroupId(100), initialLeader: 3);
        await cluster.CreateGroupEverywhereAsync(new RaftGroupId(101), initialLeader: 3);

        ClusterCoordinator leader = cluster.Nodes.First(n => n.Coordinator.IsClusterLeader).Coordinator;

        await ClusterTestCluster.WaitUntilAsync(
            () => leader.GetLoad().Nodes.Count == 3,
            "every node to have reported its load",
            TimeSpan.FromSeconds(20));

        IReadOnlyList<NodeLoad> ordered = leader.GetLoad().Nodes;
        Assert.Equal(3, ordered.Count);

        // One rule, so this is the same ordering a transfer target is picked by, asserted whole: the cluster
        // leader last regardless of what it is carrying, everyone else least-loaded first. Asserting only the
        // monotonic half would pass against a snapshot that ranks the cluster leader first — which is what it
        // used to do, because the snapshot and the placement path each had their own comparison.
        Assert.Equal(leader.Self, ordered[^1].Node);

        for (int i = 1; i < ordered.Count - 1; i++)
        {
            Assert.True(
                ordered[i - 1].LeaderCount <= ordered[i].LeaderCount,
                "load snapshot must be ordered least-loaded first ahead of the cluster leader");
        }
    }

    [Fact]
    public async Task AnIneligibleTargetIsRefusedAndAnAlternativeIsOffered()
    {
        await using var cluster = new ClusterTestCluster();
        await cluster.AddNodesAsync(1, 2, 3);
        await cluster.WaitForFormedAsync();

        RaftGroupId group = new(200);
        await cluster.CreateGroupEverywhereAsync(group);

        ClusterCoordinator coordinator = cluster.Nodes[0].Coordinator;

        // A node that is not in the group at all cannot be handed its leadership, and the library says so
        // rather than guessing.
        LeaderTargetResponse stranger = await coordinator.CheckLeaderTargetAsync(group, new NodeId(99));
        Assert.Equal(LeaderTargetStatus.NotAMember, stranger.Status);

        // Asked to resolve rather than to check, it falls back to a legal candidate instead of failing.
        LeaderTargetResponse resolved = await coordinator.ResolveLeaderTargetAsync(group, new NodeId(99));

        // Valid and IsCurrentLeader are both "this node can lead it"; which one comes back depends on where
        // leadership happens to sit, since the fallback orders by load and the incumbent is an ordinary
        // candidate in that ordering. Insisting on Valid would be asserting the election's outcome.
        Assert.Contains(resolved.Status, (LeaderTargetStatus[])[LeaderTargetStatus.Valid, LeaderTargetStatus.IsCurrentLeader]);
        Assert.NotEqual(new NodeId(99), resolved.Node);
        Assert.Contains(resolved.Node, cluster.Nodes.Select(n => n.Self));
        Assert.False(resolved.Transferred, "resolving must not move anything");
    }

    [Fact]
    public async Task ATransferRequestMovesLeadershipAndAlwaysLeavesALeader()
    {
        await using var cluster = new ClusterTestCluster();
        await cluster.AddNodesAsync(1, 2, 3);
        await cluster.WaitForFormedAsync();

        RaftGroupId group = new(300);
        await cluster.CreateGroupEverywhereAsync(group);

        RaftGroupInstance before = await cluster.WaitForGroupLeaderAsync(group);
        NodeId wanted = cluster.Nodes.Select(n => n.Self).First(n => n != before.Self);

        LeaderTargetResponse response = await cluster.Nodes[0].Coordinator
            .RequestLeaderTransferAsync(group, wanted);

        Assert.Equal(wanted, response.Node);
        Assert.True(response.Transferred, "the named target was eligible and should have taken over");

        RaftGroupInstance after = await cluster.WaitForGroupLeaderAsync(group);
        Assert.Equal(wanted, after.Self);
    }

    [Fact]
    public async Task ARefusedTransferLeavesTheExistingLeaderInPlace()
    {
        await using var cluster = new ClusterTestCluster();
        await cluster.AddNodesAsync(1, 2, 3);
        await cluster.WaitForFormedAsync();

        RaftGroupId group = new(301);
        await cluster.CreateGroupEverywhereAsync(group);

        RaftGroupInstance before = await cluster.WaitForGroupLeaderAsync(group);

        // Nobody by this name is in the group. The requirement is not that this succeeds -- it is that the
        // group still has the leader it started with afterwards.
        LeaderTargetResponse response = await cluster.Nodes[0].Coordinator
            .CheckLeaderTargetAsync(group, new NodeId(77));

        Assert.Equal(LeaderTargetStatus.NotAMember, response.Status);
        Assert.False(response.Transferred);
        Assert.True(before.IsLeader, "a refused target must never cost the group its leader");
    }

    [Fact]
    public async Task ANodeComingBackRaisesTheActionableEventOnTheClusterLeaderOnly()
    {
        await using var cluster = new ClusterTestCluster();
        await cluster.AddNodesAsync(1, 2, 3);
        await cluster.WaitForFormedAsync();

        ClusterTestNode leader = cluster.Nodes.First(n => n.Coordinator.IsClusterLeader);
        ClusterTestNode other = cluster.Nodes.First(n => n != leader);

        // Take a node away long enough for the leader to notice, then bring it back. Both directions, so the
        // absence is unambiguous rather than a one-way partition the node could still answer through.
        //
        // Comfortably longer than the harness's two-second LoadReportTtl, and that margin is the point. The
        // event fires on a transition from "expired" to "reporting again", so the leader has to actually
        // observe the node as gone first; a partition held for exactly the TTL leaves it to chance whether an
        // aging pass lands inside that window, and when it does not, nothing has changed and there is nothing
        // to announce. The test then fails for a missing precondition rather than for the behaviour it means
        // to assert -- which is what it did, intermittently, under a loaded run.
        cluster.Network.Cut(leader.Self, other.Self);
        cluster.Network.Cut(other.Self, leader.Self);
        await Task.Delay(TimeSpan.FromSeconds(5));

        await ClusterTestCluster.WaitUntilAsync(
            () => leader.Coordinator.GetLoad().Nodes.All(n => n.Node != other.Self),
            "the cluster leader to age the partitioned node out of its load model",
            TimeSpan.FromSeconds(15));

        cluster.Network.HealAll();

        await ClusterTestCluster.WaitUntilAsync(
            () => cluster.Nodes.Any(n => n.Events.Any(
                e => e.Kind == ClusterEventKind.NodeBecameAvailable && e.Node == other.Self)),
            "the returning node to be announced",
            TimeSpan.FromSeconds(25));

        // The contract is "on the cluster leader only", and that is asserted against whoever leads now rather
        // than whoever led before the partition -- cluster leadership is allowed to move while a node is
        // away, and pinning the assertion to the old leader would be testing the election, not the event.
        ClusterTestNode[] announcers =
        [
            .. cluster.Nodes.Where(n => n.Events.Any(e => e.Kind == ClusterEventKind.NodeBecameAvailable)),
        ];

        // One reactor, not three. If every node raised this, every consumer would decide to re-place the same
        // groups at the same instant.
        ClusterTestNode announcer = Assert.Single(announcers);
        Assert.True(
            announcer.Coordinator.IsClusterLeader,
            "the actionable event must be raised by the cluster leader, not by a follower");
    }

    [Fact]
    public async Task EveryNodeSeesTheObservationalEvent()
    {
        await using var cluster = new ClusterTestCluster();
        await cluster.AddNodesAsync(1, 2, 3);
        await cluster.WaitForFormedAsync();

        await ClusterTestCluster.WaitUntilAsync(
            () => cluster.Nodes.All(n => n.Events.Any(e => e.Kind == ClusterEventKind.ClusterStateChanged)),
            "all three nodes to publish their view of the cluster",
            TimeSpan.FromSeconds(20));
    }
}

/// <summary>The mode marker, which is what makes "the mode cannot change" outlast one process.</summary>
public sealed class ClusterModeStoreTests : IDisposable
{
    private readonly string _directory =
        Path.Combine(Path.GetTempPath(), "blun-mode-" + Guid.NewGuid().ToString("N"));

    [Fact]
    public async Task AFreshNodeHasNoMarkerAndIsNotATransition()
    {
        var store = new ClusterModeStore(_directory);
        Assert.Null(await store.ReadAsync());
        Assert.False(ClusterModeStore.ValidateTransition(null, ClusterMode.Replicated, new NodeId(1)));
    }

    [Fact]
    public async Task TheMarkerSurvivesAReopen()
    {
        var store = new ClusterModeStore(_directory);
        await store.WriteAsync(new ClusterModeMarker(ClusterMode.SingleNode, new NodeId(7)));

        ClusterModeMarker? read = await new ClusterModeStore(_directory).ReadAsync();
        Assert.Equal(new ClusterModeMarker(ClusterMode.SingleNode, new NodeId(7)), read);
    }

    [Fact]
    public void SingleNodeToReplicatedIsTheAllowedPromotion()
    {
        // The node that ran alone holds the log; the nodes joining it are new and hold nothing, so the
        // election restriction settles which log wins without being asked.
        Assert.True(ClusterModeStore.ValidateTransition(
            new ClusterModeMarker(ClusterMode.SingleNode, new NodeId(1)),
            ClusterMode.Replicated,
            new NodeId(1)));
    }

    [Fact]
    public void ReplicatedToSingleNodeIsRefused()
    {
        // A node pulled out of a cluster still holds entries that cluster committed. Running it alone makes
        // it a second authority on the same group ids, and no rule reconciles two real histories afterwards.
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => ClusterModeStore.ValidateTransition(
                new ClusterModeMarker(ClusterMode.Replicated, new NodeId(1)),
                ClusterMode.SingleNode,
                new NodeId(1)));

        Assert.Contains("second authority", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AMarkerFromAnotherNodeIsRefused()
    {
        Assert.Throws<InvalidOperationException>(
            () => ClusterModeStore.ValidateTransition(
                new ClusterModeMarker(ClusterMode.Replicated, new NodeId(2)),
                ClusterMode.Replicated,
                new NodeId(1)));
    }

    [Fact]
    public void StayingInTheSameModeIsNotATransition()
    {
        Assert.False(ClusterModeStore.ValidateTransition(
            new ClusterModeMarker(ClusterMode.Replicated, new NodeId(1)),
            ClusterMode.Replicated,
            new NodeId(1)));

        Assert.False(ClusterModeStore.ValidateTransition(
            new ClusterModeMarker(ClusterMode.SingleNode, new NodeId(1)),
            ClusterMode.SingleNode,
            new NodeId(1)));
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }
}

/// <summary>The dummy transport's contract: it never succeeds quietly.</summary>
public sealed class SingleNodeTransportTests
{
    [Fact]
    public async Task EveryCallThrowsAnIntegrityErrorRatherThanAnIoError()
    {
        // IOException would be wrong and not merely imprecise: the replication loop treats it as an ordinary
        // unreachable peer and retries forever, so a real configuration mistake would spin in silence.
        IRaftProtocolTransport transport = SingleNodeRaftTransport.Instance;
        var target = new NodeId(2);

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await transport.RequestVoteAsync(target, new VoteRequest(new RaftGroupId(1), 1, new NodeId(1), 0, 0, false)));

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await transport.TimeoutNowAsync(target, new TimeoutNowRequest(new RaftGroupId(1), 1)));

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await transport.ReadIndexAsync(target, new ReadIndexRequest(new RaftGroupId(1), 1)));
    }
}

/// <summary>Reserved ids, and the fact that ordinary ones are unaffected.</summary>
public sealed class ReservedGroupIdTests
{
    [Fact]
    public void OnlyTheTopBandIsReserved()
    {
        Assert.True(RaftGroupId.Cluster.IsReserved);
        Assert.True(new RaftGroupId(RaftGroupId.ReservedFloor).IsReserved);
        Assert.False(new RaftGroupId(RaftGroupId.ReservedFloor - 1).IsReserved);

        // Zero is deliberately NOT the cluster group: it is default(RaftGroupId), and an uninitialized struct
        // must not address the administrative plane.
        Assert.False(default(RaftGroupId).IsReserved);
        Assert.NotEqual(default, RaftGroupId.Cluster);
    }
}

/// <summary>
/// The same cluster, on the segmented log rather than the in-memory one.
/// </summary>
/// <remarks>
/// Not redundant with <see cref="ClusterCoordinatorTests"/>. The two logs are separate implementations of one
/// interface, and this repository already has a shared WAL contract suite precisely because a divergence
/// between them is easy to introduce and invisible from either side alone. A cluster is a heavier consumer of
/// that interface than the contract suite is — it drives truncation, replay and the consistency check through
/// real replication — so it is worth running against both.
/// </remarks>
public sealed class ClusterOnSegmentedLogTests
{
    /// <summary>
    /// The whole cluster stops and comes back. Nothing seeds a cluster that already exists, so every node is
    /// handed <see cref="RaftMembership.Empty"/> and rebuilds the voter set by replaying its own log — and
    /// the role it takes has to come from what the replay reconstructed, not from what it was handed. Taking
    /// it from the parameter makes every restarted node a learner, learners never campaign, and the
    /// administrative plane then sits at its last term forever with nobody standing for election. The queue
    /// groups keep working throughout, which is what makes it easy to miss.
    /// </summary>
    [Fact]
    public async Task ARestartedClusterElectsAnAdministrativeLeaderAgain()
    {
        await using var cluster = new ClusterTestCluster(onDisk: true);
        await cluster.AddNodesAsync(1, 2, 3);
        await cluster.WaitForFormedAsync();

        await cluster.RestartAllAsync();

        await ClusterTestCluster.WaitUntilAsync(
            () => cluster.Nodes.Any(n => n.Coordinator.IsClusterLeader),
            "the administrative plane to elect a leader after the whole cluster restarted",
            TimeSpan.FromSeconds(30));

        Assert.All(
            cluster.Nodes,
            node =>
            {
                Assert.NotEqual(RaftRole.Learner, node.Coordinator.Group!.Role);
                Assert.Equal(3, node.Coordinator.Group!.Membership.Voters.Length);
            });
    }

    [Fact]
    public async Task AColdClusterFormsAndEveryNodeAgreesOnTheVoterSet()
    {
        await using var cluster = new ClusterTestCluster(onDisk: true);
        await cluster.AddNodesAsync(3, 1, 2);

        await ClusterTestCluster.WaitUntilAsync(
            () => cluster.Nodes.All(n => n.Coordinator.Group!.Membership.Voters.Length == 3),
            "every node to see the whole voter set on a file-backed log",
            TimeSpan.FromSeconds(30));

        foreach (ClusterTestNode node in cluster.Nodes)
        {
            Assert.Equal<NodeId>(
                [new(1), new(2), new(3)],
                [.. node.Coordinator.Group!.Membership.Voters.OrderBy(v => v)]);
        }
    }
}
