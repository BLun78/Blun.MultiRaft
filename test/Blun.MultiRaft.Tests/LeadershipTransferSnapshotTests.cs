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
/// Where leadership transfer (Raft §3.10) and log compaction meet — two features built independently, whose
/// interaction was never exercised by either feature's own tests.
/// </summary>
public sealed class LeadershipTransferSnapshotTests
{
    private static readonly RaftGroupId Queue = new(9001);

    [Fact]
    public async Task ATransferTargetThatNeedsASnapshotStillGetsCaughtUpAndTakesOver()
    {
        Dictionary<NodeId, RecordingStateMachine> machines = [];
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(
            Queue,
            [1UL, 2UL, 3UL],
            node => machines[node] = new RecordingStateMachine());

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        Core.RaftGroupInstance target = cluster.GroupsOf(Queue).First(g => !g.IsLeader);

        // Strand the target behind a compacted prefix, exactly as the plain snapshot tests do, so the
        // transfer has to go through InstallSnapshot rather than ordinary AppendEntries to catch it up.
        cluster.Network.Cut(leader.Self, target.Self);
        for (int i = 0; i < 40; i++)
        {
            await leader.AppendAsync(Encoding.UTF8.GetBytes("msg-" + i));
        }

        await TestCluster.WaitUntilAsync(
            () => leader.LastApplied >= leader.CommitIndex && leader.CommitIndex > 30,
            TimeSpan.FromSeconds(5),
            "the leader to apply what it committed");
        Assert.True(await leader.TakeSnapshotAsync());
        Assert.True(
            leader.LastIndex - target.LastIndex > 30,
            "the target must be behind the compacted prefix for this test to mean anything");

        cluster.Network.Heal(leader.Self, target.Self);

        // The catch-up wait inside TransferLeadershipAsync has to survive the target needing a snapshot
        // first and only then the remaining AppendEntries -- not just ordinary replication.
        bool transferred = await leader.TransferLeadershipAsync(target.Self);

        Assert.True(transferred, "a transfer to a snapshot-needing target must not be abandoned");
        Assert.False(leader.IsLeader);

        await TestCluster.WaitUntilAsync(
            () => target.IsLeader,
            TimeSpan.FromSeconds(10),
            "the snapshot-caught-up target to become leader");

        long index = await target.AppendAsync(Encoding.UTF8.GetBytes("after-transfer"));
        Assert.True(target.CommitIndex >= index);
    }

    [Fact]
    public async Task CompactingDuringAnInFlightTransferDoesNotDisruptIt()
    {
        Dictionary<NodeId, RecordingStateMachine> machines = [];
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(
            Queue,
            [1UL, 2UL, 3UL],
            node => machines[node] = new RecordingStateMachine());

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        Core.RaftGroupInstance target = cluster.GroupsOf(Queue).First(g => !g.IsLeader);

        for (int i = 0; i < 10; i++)
        {
            await leader.AppendAsync(Encoding.UTF8.GetBytes("msg-" + i));
        }

        await TestCluster.WaitUntilAsync(
            () => leader.LastApplied >= leader.CommitIndex,
            TimeSpan.FromSeconds(5),
            "the leader to apply what it committed");

        // The target is already caught up, so the transfer's own catch-up wait resolves almost immediately --
        // this is deliberately racing a compaction against it rather than serializing the two, since that
        // race is the actual scenario worth covering.
        ValueTask<bool> transfer = leader.TransferLeadershipAsync(target.Self);
        bool compacted = await leader.TakeSnapshotAsync();

        bool transferred = await transfer;

        // TakeSnapshotAsync only removes entries already applied on every voter, so it cannot invalidate a
        // transfer that was waiting on the log matching -- both operations should be able to succeed.
        Assert.True(transferred);
        Assert.True(compacted);

        await TestCluster.WaitUntilAsync(
            () => target.IsLeader,
            TimeSpan.FromSeconds(5),
            "the target to become leader despite the concurrent compaction");
    }

    [Fact]
    public async Task ATransferTargetInstallingASnapshotEventuallyLeadsOnceCaughtUp()
    {
        // A tighter variant of the snapshot-target test: rather than only checking the transfer's own
        // return value, this confirms the elected leader's state machine actually holds what the snapshot
        // plus the trailing entries were supposed to deliver -- the transfer succeeding is not enough if the
        // state it hands over is wrong.
        Dictionary<NodeId, RecordingStateMachine> machines = [];
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(
            Queue,
            [1UL, 2UL, 3UL],
            node => machines[node] = new RecordingStateMachine());

        Core.RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        Core.RaftGroupInstance target = cluster.GroupsOf(Queue).First(g => !g.IsLeader);

        cluster.Network.Cut(leader.Self, target.Self);
        for (int i = 0; i < 35; i++)
        {
            await leader.AppendAsync(Encoding.UTF8.GetBytes("bulk-" + i));
        }

        await TestCluster.WaitUntilAsync(
            () => leader.LastApplied >= leader.CommitIndex,
            TimeSpan.FromSeconds(5),
            "the leader to apply everything");
        await leader.TakeSnapshotAsync();

        cluster.Network.Heal(leader.Self, target.Self);
        Assert.True(await leader.TransferLeadershipAsync(target.Self));

        await TestCluster.WaitUntilAsync(
            () => target.IsLeader,
            TimeSpan.FromSeconds(10),
            "the target to become leader");

        RecordingStateMachine restored = machines[target.Self];
        Assert.Contains(restored.Applied, a => Encoding.UTF8.GetString(a.Payload) == "bulk-0");
        Assert.Contains(restored.Applied, a => Encoding.UTF8.GetString(a.Payload) == "bulk-34");
    }
}
