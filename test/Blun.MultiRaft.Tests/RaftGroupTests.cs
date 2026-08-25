// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Text;
using Blun.MultiRaft.Core;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Tests;

public sealed class RaftGroupTests
{
    private static readonly RaftGroupId Queue = new(42);

    [Fact]
    public async Task SingleVoterElectsItselfAndCommits()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL]);

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);

        long index = await leader.AppendAsync(Encoding.UTF8.GetBytes("hello"));

        Assert.Equal(new NodeId(1), leader.Self);
        Assert.True(leader.CommitIndex >= index);
    }

    [Fact]
    public async Task ThreeVotersElectExactlyOneLeader()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL, 2UL, 3UL]);

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);

        Assert.Single(cluster.GroupsOf(Queue), g => g.IsLeader);
        Assert.All(
            cluster.GroupsOf(Queue),
            g => Assert.Equal(leader.CurrentTerm, g.CurrentTerm));
    }

    [Fact]
    public async Task CommittedEntriesReachEveryFollowerAndApplyInOrder()
    {
        Dictionary<NodeId, RecordingStateMachine> machines = [];
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(
            Queue,
            [1UL, 2UL, 3UL],
            node => machines[node] = new RecordingStateMachine());

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);

        for (int i = 1; i <= 20; i++)
        {
            await leader.AppendAsync(Encoding.UTF8.GetBytes("msg-" + i));
        }

        await TestCluster.WaitUntilAsync(
            () => cluster.GroupsOf(Queue).All(g => g.LastIndex == leader.LastIndex),
            TimeSpan.FromSeconds(5),
            "all replicas to hold the leader's log");

        await TestCluster.WaitUntilAsync(
            () => machines.Values.All(m => m.Applied.Count == 20),
            TimeSpan.FromSeconds(5),
            "every state machine to apply all 20 commands");

        foreach (RecordingStateMachine machine in machines.Values)
        {
            long[] indices = [.. machine.Applied.Select(a => a.Index)];
            Assert.Equal(indices.OrderBy(i => i), indices);

            string[] payloads = [.. machine.Applied.Select(a => Encoding.UTF8.GetString(a.Payload))];
            Assert.Equal([.. Enumerable.Range(1, 20).Select(i => "msg-" + i)], payloads);
        }
    }

    [Fact]
    public async Task FollowersRejectClientAppendsAndNameTheLeader()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL, 2UL, 3UL]);

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        Core.RaftGroupInstance follower = cluster.GroupsOf(Queue).First(g => !g.IsLeader);

        await TestCluster.WaitUntilAsync(
            () => follower.LeaderId is not null,
            TimeSpan.FromSeconds(5),
            "the follower to learn who leads");

        NotLeaderException error = await Assert.ThrowsAsync<NotLeaderException>(
            async () => await follower.AppendAsync(Encoding.UTF8.GetBytes("nope")));

        // Naming the leader is what turns a rejection into a redirect instead of a retry loop.
        Assert.Equal(leader.Self, error.Leader);
    }

    [Fact]
    public async Task LosingTheLeaderElectsANewOne()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL, 2UL, 3UL]);

        Core.RaftGroupInstance first = await cluster.WaitForLeaderAsync(Queue);
        await first.AppendAsync(Encoding.UTF8.GetBytes("before-failover"));

        // Cut the old leader off in both directions: it is not dead, it is partitioned — the harder case,
        // because it keeps believing it leads until it hears a higher term.
        foreach (NodeId other in cluster.Hosts.Keys.Where(n => n != first.Self))
        {
            cluster.Network.Cut(first.Self, other);
            cluster.Network.Cut(other, first.Self);
        }

        Core.RaftGroupInstance second = null!;
        await TestCluster.WaitUntilAsync(
            () =>
            {
                second = cluster.GroupsOf(Queue).FirstOrDefault(g => g.IsLeader && g.Self != first.Self)!;
                return second is not null;
            },
            TimeSpan.FromSeconds(10),
            "the surviving majority to elect a new leader");

        Assert.True(second.CurrentTerm > first.CurrentTerm);

        // The new leader must hold everything the old one committed — the election restriction of §5.4.1.
        Assert.True(second.LastIndex >= 1);
        long index = await second.AppendAsync(Encoding.UTF8.GetBytes("after-failover"));
        Assert.True(second.CommitIndex >= index);
    }

    [Fact]
    public async Task ALearnerJoinsCatchesUpAndIsPromoted()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3, 4);

        // Node 4 is hosted but starts outside the configuration: it is the node that will join.
        RaftMembership initial = RaftMembership.OfVoters(new NodeId(1), new NodeId(2), new NodeId(3));
        foreach ((NodeId _, Hosting.MultiRaftHost host) in cluster.Hosts)
        {
            await host.AddGroupAsync(Queue, initial, null, TestCluster.FastOptions);
        }

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        for (int i = 1; i <= 10; i++)
        {
            await leader.AppendAsync(Encoding.UTF8.GetBytes("pre-join-" + i));
        }

        var joiner = new NodeId(4);
        await leader.AddLearnerAsync(joiner);

        Assert.Contains(joiner, leader.Membership.Learners);
        Assert.DoesNotContain(joiner, leader.Membership.Voters);

        // Quorum is still 2 of 3: a learner must not move it, or adding one could stall the group.
        Assert.Equal(2, leader.Membership.QuorumSize);

        Core.RaftGroupInstance learner = cluster.Group(4, Queue);
        await TestCluster.WaitUntilAsync(
            () => learner.LastIndex == leader.LastIndex,
            TimeSpan.FromSeconds(5),
            "the learner to catch up");

        Assert.True(await leader.PromoteToVoterAsync(joiner));

        Assert.Contains(joiner, leader.Membership.Voters);
        Assert.Equal(3, leader.Membership.QuorumSize);

        // The group still commits with the wider quorum, which is the only thing promotion is for.
        long index = await leader.AppendAsync(Encoding.UTF8.GetBytes("after-promotion"));
        Assert.True(leader.CommitIndex >= index);
    }

    [Fact]
    public async Task PromotingANodeThatIsFarBehindIsRefused()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3, 4);
        RaftMembership initial = RaftMembership.OfVoters(new NodeId(1), new NodeId(2), new NodeId(3));
        foreach ((NodeId _, Hosting.MultiRaftHost host) in cluster.Hosts)
        {
            await host.AddGroupAsync(Queue, initial, null, TestCluster.FastOptions);
        }

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        var joiner = new NodeId(4);

        // Add the learner, then cut it off before it can catch up and pile on far more than the threshold.
        await leader.AddLearnerAsync(joiner);
        cluster.Network.Cut(leader.Self, joiner);

        for (int i = 0; i < 100; i++)
        {
            await leader.AppendAsync(Encoding.UTF8.GetBytes("bulk-" + i));
        }

        Assert.False(await leader.PromoteToVoterAsync(joiner));
        Assert.DoesNotContain(joiner, leader.Membership.Voters);
    }

    [Fact]
    public async Task RemovingANodeShrinksTheQuorum()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL, 2UL, 3UL]);

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        NodeId victim = cluster.GroupsOf(Queue).First(g => !g.IsLeader).Self;

        Assert.Equal(2, leader.Membership.QuorumSize);
        await leader.RemoveNodeAsync(victim);

        Assert.DoesNotContain(victim, leader.Membership.Voters);
        Assert.Equal(2, leader.Membership.Voters.Length);

        // With the node gone, the remaining two must still be able to commit on their own.
        cluster.Network.Cut(leader.Self, victim);
        long index = await leader.AppendAsync(Encoding.UTF8.GetBytes("after-removal"));
        Assert.True(leader.CommitIndex >= index);
    }

    [Fact]
    public async Task ADivergentFollowerIsReconciledToTheLeadersLog()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL, 2UL, 3UL]);

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        Core.RaftGroupInstance stale = cluster.GroupsOf(Queue).First(g => !g.IsLeader);

        // Partition one follower, let the leader move on without it, then let it back in.
        cluster.Network.Cut(leader.Self, stale.Self);
        for (int i = 0; i < 30; i++)
        {
            await leader.AppendAsync(Encoding.UTF8.GetBytes("while-partitioned-" + i));
        }

        cluster.Network.Heal(leader.Self, stale.Self);

        await TestCluster.WaitUntilAsync(
            () => stale.LastIndex == leader.LastIndex,
            TimeSpan.FromSeconds(10),
            "the rejoined follower to be brought back in line");

        Assert.Equal(leader.LastIndex, stale.LastIndex);
    }
}
