// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Text;
using Blun.MultiRaft.Core;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Tests;

/// <summary>Compaction triggered automatically by <see cref="RaftGroupOptions.AutoCompactionThreshold"/>.</summary>
public sealed class AutoCompactionTests
{
    private static readonly RaftGroupId Queue = new(4242);

    [Fact]
    public async Task CompactsOnItsOwnOnceTheThresholdIsCrossed()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        RaftGroupOptions options = new()
        {
            ElectionTimeout = TimeSpan.FromMilliseconds(120),
            ElectionJitter = 1.0,
            HeartbeatInterval = TimeSpan.FromMilliseconds(30),
            AutoCompactionThreshold = 10,
        };
        RaftMembership membership = RaftMembership.OfVoters(new NodeId(1), new NodeId(2), new NodeId(3));
        foreach ((NodeId _, Hosting.MultiRaftHost host) in cluster.Hosts)
        {
            await host.AddGroupAsync(Queue, membership, new RecordingStateMachine(), options);
        }

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        for (int i = 0; i < 25; i++)
        {
            await leader.AppendAsync(Encoding.UTF8.GetBytes("msg-" + i));
        }

        // Nobody called TakeSnapshotAsync directly. If the log's start ever moves past index 1 on its own,
        // that can only be the automatic trigger.
        await TestCluster.WaitUntilAsync(
            () => leader.LastIndex >= 1, // sanity: the group is actually alive
            TimeSpan.FromSeconds(2),
            "the group to be usable at all");

        Core.RaftGroupInstance? compacted = null;
        await TestCluster.WaitUntilAsync(
            () =>
            {
                compacted = cluster.GroupsOf(Queue).FirstOrDefault(g => g.FirstIndex > 1);
                return compacted is not null;
            },
            TimeSpan.FromSeconds(10),
            "some replica to compact its log on its own");

        Assert.NotNull(compacted);
    }

    [Fact]
    public async Task NeverCompactsWithoutAConfiguredThreshold()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(
            Queue,
            [1UL, 2UL, 3UL],
            _ => new RecordingStateMachine());

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        for (int i = 0; i < 25; i++)
        {
            await leader.AppendAsync(Encoding.UTF8.GetBytes("msg-" + i));
        }

        await TestCluster.WaitUntilAsync(
            () => leader.LastApplied >= leader.CommitIndex,
            TimeSpan.FromSeconds(5),
            "everything to be applied");

        // Give the (absent) auto-trigger every chance it would have had.
        await Task.Delay(500);

        Assert.All(cluster.GroupsOf(Queue), g => Assert.Equal(1, g.FirstIndex));
    }

    [Fact]
    public async Task NeverCompactsWithoutASnapshotableStateMachine()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        RaftGroupOptions options = new()
        {
            ElectionTimeout = TimeSpan.FromMilliseconds(120),
            ElectionJitter = 1.0,
            HeartbeatInterval = TimeSpan.FromMilliseconds(30),
            AutoCompactionThreshold = 5,
        };
        RaftMembership membership = RaftMembership.OfVoters(new NodeId(1), new NodeId(2), new NodeId(3));
        foreach ((NodeId _, Hosting.MultiRaftHost host) in cluster.Hosts)
        {
            // No state machine at all: NullRaftStateMachine, which does not implement
            // IRaftSnapshotableStateMachine, so there is nothing to capture into a snapshot.
            await host.AddGroupAsync(Queue, membership, null, options);
        }

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        for (int i = 0; i < 25; i++)
        {
            await leader.AppendAsync(Encoding.UTF8.GetBytes("msg-" + i));
        }

        await TestCluster.WaitUntilAsync(
            () => leader.LastApplied >= leader.CommitIndex,
            TimeSpan.FromSeconds(5),
            "everything to be applied");
        await Task.Delay(500);

        Assert.All(cluster.GroupsOf(Queue), g => Assert.Equal(1, g.FirstIndex));
    }

}
