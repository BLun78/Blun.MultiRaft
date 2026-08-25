// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Text;
using Blun.MultiRaft.Core;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Tests;

/// <summary>
/// Linearizable reads via ReadIndex (Raft §6.4) — reads that are current without costing a log entry.
/// </summary>
public sealed class ReadIndexTests
{
    private static readonly RaftGroupId Queue = new(1234);

    [Fact]
    public async Task TheLeaderServesAReadIndexWithoutGrowingTheLog()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL, 2UL, 3UL]);

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        for (int i = 0; i < 5; i++)
        {
            await leader.AppendAsync(Encoding.UTF8.GetBytes("msg-" + i));
        }

        long before = leader.LastIndex;
        long readIndex = await leader.ReadIndexAsync();

        // No append, no fsync, no replication of anything new. That is the entire reason ReadIndex exists
        // rather than committing a barrier entry for every read.
        Assert.Equal(before, leader.LastIndex);
        Assert.Equal(leader.CommitIndex, readIndex);
    }

    [Fact]
    public async Task AFollowerObtainsAReadIndexFromTheLeaderAndCatchesUpToIt()
    {
        Dictionary<NodeId, RecordingStateMachine> machines = [];
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(
            Queue,
            [1UL, 2UL, 3UL],
            node => machines[node] = new RecordingStateMachine());

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        for (int i = 0; i < 10; i++)
        {
            await leader.AppendAsync(Encoding.UTF8.GetBytes("msg-" + i));
        }

        Core.RaftGroupInstance follower = cluster.GroupsOf(Queue).First(g => !g.IsLeader);

        long readIndex = await follower.PrepareLinearizableReadAsync();

        // After this returns, reading the follower's local state is linearizable: it has applied everything
        // that was committed when the read began. Serving reads off followers at all is the point — without
        // it every read goes to the leader, which is the bottleneck multi-Raft exists to spread.
        Assert.True(readIndex > 0);
        Assert.True(follower.LastApplied >= readIndex);
        Assert.True(machines[follower.Self].Applied.Count > 0);
    }

    [Fact]
    public async Task APartitionedLeaderRefusesToServeAReadIndex()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL, 2UL, 3UL]);

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        await leader.AppendAsync(Encoding.UTF8.GetBytes("before"));

        // This is the case that makes the leadership confirmation load-bearing rather than ceremonial. The
        // old leader still believes it leads and its commit index still looks fine — answering from it would
        // hand back a stale read dressed up as a linearizable one, which is worse than an honestly stale one.
        foreach (NodeId other in cluster.Hosts.Keys.Where(n => n != leader.Self))
        {
            cluster.Network.Cut(leader.Self, other);
            cluster.Network.Cut(other, leader.Self);
        }

        Assert.True(leader.IsLeader, "the partitioned node must still believe it leads for this test to mean anything");

        await Assert.ThrowsAsync<NotLeaderException>(async () => await leader.ReadIndexAsync());
    }

    [Fact]
    public async Task ASingleVoterGroupNeedsNoConfirmation()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL]);

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        await leader.AppendAsync(Encoding.UTF8.GetBytes("only"));

        // A single voter is its own quorum, so there is nobody who could have superseded it and no round
        // trip to make.
        long readIndex = await leader.ReadIndexAsync();
        Assert.Equal(leader.CommitIndex, readIndex);
    }

    [Fact]
    public async Task AFollowerWithoutAKnownLeaderReportsItRatherThanGuessing()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL, 2UL, 3UL]);

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        Core.RaftGroupInstance follower = cluster.GroupsOf(Queue).First(g => !g.IsLeader);

        cluster.Network.Cut(follower.Self, leader.Self);

        // Serving the read locally would be exactly the stale read this mechanism exists to prevent, so the
        // only correct answer is to say it cannot be done here.
        await Assert.ThrowsAnyAsync<Exception>(async () => await follower.ReadIndexAsync());
    }
}
