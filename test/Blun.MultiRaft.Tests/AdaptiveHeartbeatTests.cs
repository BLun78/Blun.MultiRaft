// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Tests;

public sealed class AdaptiveHeartbeatTests
{
    private static readonly RaftGroupId Queue = new(77);

    [Fact]
    public async Task IdleHeartbeatIntervalAtOrAboveElectionTimeoutIsRejected()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1);
        var options = new RaftGroupOptions
        {
            ElectionTimeout = TimeSpan.FromMilliseconds(120),
            HeartbeatInterval = TimeSpan.FromMilliseconds(30),
            IdleHeartbeatInterval = TimeSpan.FromMilliseconds(120),
        };

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => cluster.AddGroupEverywhereAsync(Queue, [1UL], options: options).AsTask());
    }

    [Fact]
    public async Task IdleHeartbeatIntervalBelowHeartbeatIntervalIsRejected()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1);
        var options = new RaftGroupOptions
        {
            ElectionTimeout = TimeSpan.FromMilliseconds(120),
            HeartbeatInterval = TimeSpan.FromMilliseconds(30),
            IdleHeartbeatInterval = TimeSpan.FromMilliseconds(10),
        };

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => cluster.AddGroupEverywhereAsync(Queue, [1UL], options: options).AsTask());
    }

    /// <summary>
    /// An idle group must never spuriously re-elect just because its heartbeats slowed down. This runs the
    /// idle interval close to the election timeout -- the tightest, least forgiving margin the constructor
    /// still allows -- for several multiples of it with no writes at all, and checks the group never forgets
    /// its leader or bumps its term in the meantime.
    /// </summary>
    [Fact]
    public async Task IdleGroupKeepsItsLeaderAcrossSeveralIdleHeartbeatIntervals()
    {
        var options = new RaftGroupOptions
        {
            ElectionTimeout = TimeSpan.FromMilliseconds(200),
            ElectionJitter = 1.0,
            HeartbeatInterval = TimeSpan.FromMilliseconds(20),
            IdleHeartbeatInterval = TimeSpan.FromMilliseconds(150),
        };

        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL, 2UL, 3UL], options: options);

        RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        long termBeforeIdle = leader.CurrentTerm;
        NodeId leaderBeforeIdle = leader.Self;

        // Long enough to cross the idle threshold (one ElectionTimeout of silence) and then sit in the idle
        // cadence for several of its own intervals -- the window where a wrong margin would show up as a
        // follower timing out between two idle heartbeats.
        await Task.Delay(TimeSpan.FromMilliseconds(1200));

        Assert.All(
            cluster.GroupsOf(Queue),
            g => Assert.Equal(termBeforeIdle, g.CurrentTerm));
        Assert.True(leader.IsLeader);
        Assert.Equal(leaderBeforeIdle, leader.Self);
        Assert.Single(cluster.GroupsOf(Queue), g => g.IsLeader);
    }
}
