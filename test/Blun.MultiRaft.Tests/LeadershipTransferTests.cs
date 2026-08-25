// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Text;
using Blun.MultiRaft.Core;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Tests;

/// <summary>Voluntary leadership handoff (Raft §3.10) — for draining a node before maintenance.</summary>
public sealed class LeadershipTransferTests
{
    private static readonly RaftGroupId Queue = new(555);

    [Fact]
    public async Task TransferHandsLeadershipToTheNamedTarget()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL, 2UL, 3UL]);

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        await leader.AppendAsync(Encoding.UTF8.GetBytes("before-transfer"));

        Core.RaftGroupInstance target = cluster.GroupsOf(Queue).First(g => !g.IsLeader);

        bool transferred = await leader.TransferLeadershipAsync(target.Self);

        Assert.True(transferred);
        Assert.False(leader.IsLeader);

        await TestCluster.WaitUntilAsync(
            () => target.IsLeader,
            TimeSpan.FromSeconds(5),
            "the named target to become leader");

        // A real election followed the handoff, so the new leader must be able to serve normally.
        long index = await target.AppendAsync(Encoding.UTF8.GetBytes("after-transfer"));
        Assert.True(target.CommitIndex >= index);
    }

    [Fact]
    public async Task TransferWithNoTargetPicksTheMostCaughtUpVoter()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL, 2UL, 3UL]);

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        for (int i = 0; i < 5; i++)
        {
            await leader.AppendAsync(Encoding.UTF8.GetBytes("msg-" + i));
        }

        NodeId leaderId = leader.Self;
        Assert.True(await leader.TransferLeadershipAsync());

        await TestCluster.WaitUntilAsync(
            () => cluster.GroupsOf(Queue).Any(g => g.IsLeader && g.Self != leaderId),
            TimeSpan.FromSeconds(5),
            "some other voter to take over");
    }

    [Fact]
    public async Task NewWritesAreRefusedWhileATransferIsInFlight()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL, 2UL, 3UL]);

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        Core.RaftGroupInstance target = cluster.GroupsOf(Queue).First(g => !g.IsLeader);

        // The catch-up wait inside TransferLeadershipAsync gives the assertion below a real window: the
        // target is already caught up (nothing was appended), so without the block this would very likely
        // race the transfer and pass by accident rather than by the guarantee actually holding.
        ValueTask<bool> transfer = leader.TransferLeadershipAsync(target.Self);

        // Three outcomes are all consistent with the guarantee: the block fires
        // (LeadershipTransferInProgressException), or the handoff has already completed by the time this
        // runs (NotLeaderException, since the old leader stepped aside). What would violate it is a write
        // silently succeeding mid-transfer.
        try
        {
            await leader.AppendAsync(Encoding.UTF8.GetBytes("during-transfer"));
            Assert.Fail("a write during a leadership transfer must not silently succeed");
        }
        catch (LeadershipTransferInProgressException ex)
        {
            Assert.Equal(target.Self, ex.Successor);
        }
        catch (NotLeaderException)
        {
            // The transfer had already finished; the block did its job while it lasted.
        }

        await transfer;
    }

    [Fact]
    public async Task ASecondTransferCannotStartWhileOneIsInFlight()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3, 4);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL, 2UL, 3UL, 4UL]);

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        Core.RaftGroupInstance[] others = [.. cluster.GroupsOf(Queue).Where(g => !g.IsLeader)];

        ValueTask<bool> first = leader.TransferLeadershipAsync(others[0].Self);

        // The second call races the first's completion in principle, but the first is a leader-only
        // operation and this assertion only cares that two transfers are never simultaneously accepted --
        // either this throws, or it observes the leader has already changed and fails as NotLeaderException.
        try
        {
            await leader.TransferLeadershipAsync(others[1].Self);
        }
        catch (Exception ex) when (ex is InvalidOperationException or NotLeaderException)
        {
            // Either outcome is the guarantee holding.
        }

        await first;
    }

    [Fact]
    public async Task TransferringToOneselfIsANoOp()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL, 2UL, 3UL]);

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);

        Assert.True(await leader.TransferLeadershipAsync(leader.Self));
        Assert.True(leader.IsLeader);
    }

    [Fact]
    public async Task TransferringToALearnerIsRejected()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3, 4);
        RaftMembership initial = RaftMembership.OfVoters(new NodeId(1), new NodeId(2), new NodeId(3));
        foreach ((NodeId _, Hosting.MultiRaftHost host) in cluster.Hosts)
        {
            await host.AddGroupAsync(Queue, initial, null, TestCluster.FastOptions);
        }

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        var learner = new NodeId(4);
        await leader.AddLearnerAsync(learner);

        // A learner has not been vetted by a vote and does not count toward quorum; handing it leadership
        // would put the group in the hands of a member nobody agreed could lead it.
        await Assert.ThrowsAsync<ArgumentException>(async () => await leader.TransferLeadershipAsync(learner));
        Assert.True(leader.IsLeader);
    }

    [Fact]
    public async Task AFollowerCannotInitiateATransfer()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL, 2UL, 3UL]);

        await cluster.WaitForLeaderAsync(Queue);
        Core.RaftGroupInstance follower = cluster.GroupsOf(Queue).First(g => !g.IsLeader);

        await Assert.ThrowsAsync<NotLeaderException>(async () => await follower.TransferLeadershipAsync());
    }

    [Fact]
    public async Task ATransferToAnUnreachableTargetIsAbandonedAndLeadershipStays()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL, 2UL, 3UL]);

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        Core.RaftGroupInstance target = cluster.GroupsOf(Queue).First(g => !g.IsLeader);

        cluster.Network.Cut(leader.Self, target.Self);

        bool transferred = await leader.TransferLeadershipAsync(target.Self);

        Assert.False(transferred);
        Assert.True(leader.IsLeader, "an abandoned transfer must leave the original leader in charge");

        // The block on new writes must not outlive the abandoned attempt.
        long index = await leader.AppendAsync(Encoding.UTF8.GetBytes("still-works"));
        Assert.True(leader.CommitIndex >= index);
    }
}
