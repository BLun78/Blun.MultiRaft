// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Text;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Tests;

/// <summary>
/// Snapshot transfer, and specifically the state that used to be unrecoverable: a replica that was offline
/// while the leader compacted its log past the point the replica had reached.
/// </summary>
public sealed class SnapshotTests
{
    private static readonly RaftGroupId Queue = new(77);

    [Fact]
    public async Task CompactingTheLogKeepsTheBoundaryAnswerable()
    {
        await using var wal = new InMemoryRaftWal();
        for (int i = 1; i <= 20; i++)
        {
            await RaftWalContractTests.AppendAsync(wal, term: 3, index: i, "e" + i);
        }

        await wal.TruncateHeadAsync(10);

        // The entry at index 10 is gone, but its term must survive it: the leader's next AppendEntries
        // checks consistency against exactly that index, and answering -1 would deadlock replication.
        Assert.Equal(11, wal.FirstIndex);
        Assert.Equal(3, await wal.TermAtAsync(10));
        Assert.Equal(-1, await wal.TermAtAsync(9));
    }

    [Fact]
    public async Task ResetToSnapshotRestartsTheIndexSpace()
    {
        await using var wal = new InMemoryRaftWal();
        await RaftWalContractTests.AppendAsync(wal, term: 1, index: 1, "old");

        await wal.ResetToSnapshotAsync(lastIncludedIndex: 500, lastIncludedTerm: 9);

        Assert.Equal(500, wal.LastIndex);
        Assert.Equal(9, wal.LastTerm);
        Assert.Equal(501, wal.FirstIndex);
        Assert.Equal(9, await wal.TermAtAsync(500));

        // Appending continues past the snapshot rather than colliding with indices it already covers.
        await RaftWalContractTests.AppendAsync(wal, term: 9, index: 501, "after");
        Assert.Equal(501, wal.LastIndex);
    }

    [Fact]
    public async Task ASegmentedLogRemembersItsSnapshotBoundaryAcrossReopen()
    {
        string directory = Path.Combine(Path.GetTempPath(), "blun-mr-snap-" + Guid.CreateVersion7().ToString("N"));
        var options = new SegmentedRaftWalOptions { SegmentSizeBytes = 64 * 1024, MaxPayloadBytes = 4096 };
        try
        {
            await using (SegmentedRaftWal wal = await SegmentedRaftWal.OpenAsync(directory, options))
            {
                await RaftWalContractTests.AppendAsync(wal, term: 1, index: 1, "discarded");
                await wal.ResetToSnapshotAsync(lastIncludedIndex: 900, lastIncludedTerm: 4);
            }

            await using SegmentedRaftWal reopened = await SegmentedRaftWal.OpenAsync(directory, options);

            // Losing this across a restart would leave the replica unable to answer for its own starting
            // point, which is indistinguishable from a permanently divergent log.
            Assert.Equal(900, reopened.LastIndex);
            Assert.Equal(4, reopened.LastTerm);
            Assert.Equal(4, await reopened.TermAtAsync(900));
            Assert.Equal(901, reopened.FirstIndex);
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task AReplicaBehindTheCompactedLogIsCaughtUpBySnapshot()
    {
        Dictionary<NodeId, RecordingStateMachine> machines = [];
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(
            Queue,
            [1UL, 2UL, 3UL],
            node => machines[node] = new RecordingStateMachine());

        RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        RaftGroupInstance straggler = cluster.GroupsOf(Queue).First(g => !g.IsLeader);

        // Cut the replica off, then move the leader well past it and compact everything it is missing.
        cluster.Network.Cut(leader.Self, straggler.Self);
        for (int i = 1; i <= 40; i++)
        {
            await leader.AppendAsync(Encoding.UTF8.GetBytes("msg-" + i));
        }

        await TestCluster.WaitUntilAsync(
            () => leader.LastApplied >= leader.CommitIndex && leader.CommitIndex > 30,
            TimeSpan.FromSeconds(5),
            "the leader to apply what it committed");

        Assert.True(await leader.TakeSnapshotAsync());
        Assert.True(
            leader.LastIndex - straggler.LastIndex > 30,
            "the straggler must be behind the compacted prefix for this test to mean anything");

        // Before snapshot transfer existed, this was terminal: the entries the replica needed no longer
        // existed anywhere, and it would sit there logging a warning forever.
        cluster.Network.Heal(leader.Self, straggler.Self);

        await TestCluster.WaitUntilAsync(
            () => straggler.LastIndex == leader.LastIndex,
            TimeSpan.FromSeconds(15),
            "the straggler to be caught up by snapshot");

        Assert.Equal(leader.LastIndex, straggler.LastIndex);
        Assert.True(straggler.CommitIndex >= leader.CommitIndex - 1);

        // The state machine has to hold the snapshot's contents, not just the log position.
        RecordingStateMachine restored = machines[straggler.Self];
        Assert.Contains(restored.Applied, a => Encoding.UTF8.GetString(a.Payload) == "msg-1");
        Assert.Contains(restored.Applied, a => Encoding.UTF8.GetString(a.Payload) == "msg-40");
    }

    [Fact]
    public async Task AnInstalledSnapshotCarriesTheGroupConfiguration()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3, 4);
        RaftMembership initial = RaftMembership.OfVoters(new NodeId(1), new NodeId(2), new NodeId(3));
        foreach ((NodeId _, Hosting.MultiRaftHost host) in cluster.Hosts)
        {
            await host.AddGroupAsync(Queue, initial, new RecordingStateMachine(), TestCluster.FastOptions);
        }

        RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        var joiner = new NodeId(4);

        // The joiner is added while cut off, so the membership entry that added it is one of the entries it
        // will never receive — it can only learn its own membership from the snapshot.
        cluster.Network.Cut(leader.Self, joiner);
        await leader.AddLearnerAsync(joiner);
        for (int i = 0; i < 30; i++)
        {
            await leader.AppendAsync(Encoding.UTF8.GetBytes("bulk-" + i));
        }

        await TestCluster.WaitUntilAsync(
            () => leader.LastApplied >= leader.CommitIndex,
            TimeSpan.FromSeconds(5),
            "the leader to catch its own state machine up");
        Assert.True(await leader.TakeSnapshotAsync());

        cluster.Network.Heal(leader.Self, joiner);
        RaftGroupInstance learner = cluster.Group(4, Queue);

        await TestCluster.WaitUntilAsync(
            () => learner.Membership.Contains(joiner),
            TimeSpan.FromSeconds(15),
            "the joiner to learn its membership from the snapshot");

        Assert.Contains(joiner, learner.Membership.Learners);
        Assert.Equal(RaftRole.Learner, learner.Role);
    }

    [Fact]
    public void MembershipSurvivesASerializationRoundTrip()
    {
        var membership = new RaftMembership(
            [new NodeId(1), new NodeId(2), new NodeId(3)],
            [new NodeId(9)]);

        RaftMembership restored = RaftMembership.Deserialize(membership.Serialize());

        Assert.Equal(membership.Voters, restored.Voters);
        Assert.Equal(membership.Learners, restored.Learners);
        Assert.Equal(membership.QuorumSize, restored.QuorumSize);
    }

    [Fact]
    public async Task ASnapshotStoreKeepsTheOldSnapshotWhenAWriteFailsHalfway()
    {
        string root = Path.Combine(Path.GetTempPath(), "blun-mr-store-" + Guid.CreateVersion7().ToString("N"));
        try
        {
            var store = new FileRaftSnapshotStore(root);
            await store.WriteAsync(Queue, new RaftSnapshotMetadata(10, 1, ReadOnlyMemory<byte>.Empty), Chunks("good"));

            // A body that throws partway is exactly the crash-during-write case: the previous snapshot has to
            // still be there afterwards, because a half-written one would be installed as if it were whole.
            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await store.WriteAsync(Queue, new RaftSnapshotMetadata(20, 2, ReadOnlyMemory<byte>.Empty), FailingChunks()));

            RaftSnapshotMetadata? metadata = await store.ReadMetadataAsync(Queue);
            Assert.NotNull(metadata);
            Assert.Equal(10, metadata.Value.LastIncludedIndex);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> Chunks(string content)
    {
        yield return Encoding.UTF8.GetBytes(content);
        await ValueTask.CompletedTask;
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> FailingChunks()
    {
        yield return Encoding.UTF8.GetBytes("partial");
        await ValueTask.CompletedTask;
        throw new InvalidOperationException("state machine failed mid-capture");
    }
}
