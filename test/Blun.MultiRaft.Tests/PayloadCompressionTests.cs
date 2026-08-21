// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Text;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Tests;

/// <summary>
/// Compression is meant to be invisible above <see cref="IRaftStateMachine"/> and below it: what a caller
/// appends is what every replica applies, whether or not the bytes shrank on the way. These tests assert
/// that invisibility from both ends — the payload that comes back out, and the header that says how it was
/// stored.
/// </summary>
public sealed class PayloadCompressionTests
{
    private static readonly RaftGroupId Queue = new(77);

    /// <summary><see cref="TestCluster.FastOptions"/> with compression switched on.</summary>
    private static RaftGroupOptions Compressed => new()
    {
        ElectionTimeout = TimeSpan.FromMilliseconds(120),
        ElectionJitter = 1.0,
        HeartbeatInterval = TimeSpan.FromMilliseconds(30),
        PromotionCatchUpThreshold = 8,
        LeadershipTransferCatchUpTimeout = TimeSpan.FromSeconds(10),
        PayloadCompression = RaftPayloadCompression.Lz4Fast,
    };

    /// <summary>Highly repetitive, so LZ4 certainly shrinks it — the case compression exists for.</summary>
    private static byte[] CompressiblePayload(int length)
    {
        byte[] pattern = Encoding.UTF8.GetBytes("{\"type\":\"enqueue\",\"queue\":\"orders\",\"body\":\"");
        var buffer = new byte[length];
        for (int written = 0; written < length;)
        {
            int chunk = Math.Min(pattern.Length, length - written);
            pattern.AsSpan(0, chunk).CopyTo(buffer.AsSpan(written));
            written += chunk;
        }

        return buffer;
    }

    /// <summary>
    /// Random bytes, which LZ4 cannot shrink — the case the adaptive rule exists for. Seeded so a failure
    /// reproduces.
    /// </summary>
    private static byte[] IncompressiblePayload(int length)
    {
        var buffer = new byte[length];
        new Random(Seed: 20260822).NextBytes(buffer);
        return buffer;
    }

    [Theory]
    [InlineData(64)]
    [InlineData(4_096)]
    [InlineData(256_000)]
    public async Task ACompressedPayloadReachesTheStateMachineByteForByte(int length)
    {
        var machines = new Dictionary<NodeId, RecordingStateMachine>();
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(
            Queue,
            [1UL, 2UL, 3UL],
            node => machines[node] = new RecordingStateMachine(),
            Compressed);

        RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        byte[] payload = CompressiblePayload(length);

        long index = await leader.AppendAsync(payload);

        // Every replica, not just the leader: the follower stores what the leader sent it and decompresses
        // on its own, so a leader-only assertion would miss a receive-path mistake entirely.
        foreach ((NodeId node, RecordingStateMachine machine) in machines)
        {
            await TestCluster.WaitUntilAsync(
                () => machine.Applied.Any(a => a.Index == index),
                TimeSpan.FromSeconds(5),
                $"node {node.Value} to apply entry {index}");

            byte[] applied = machine.Applied.Single(a => a.Index == index).Payload;
            Assert.Equal(payload, applied);
        }
    }

    [Fact]
    public async Task AnIncompressiblePayloadIsStoredUncompressedAndStillArrivesIntact()
    {
        var machines = new Dictionary<NodeId, RecordingStateMachine>();
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1);
        await cluster.AddGroupEverywhereAsync(
            Queue,
            [1UL],
            node => machines[node] = new RecordingStateMachine(),
            Compressed);

        RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        byte[] payload = IncompressiblePayload(8_192);

        long index = await leader.AppendAsync(payload);

        await TestCluster.WaitUntilAsync(
            () => machines[leader.Self].Applied.Any(a => a.Index == index),
            TimeSpan.FromSeconds(5),
            $"entry {index} to be applied");

        Assert.Equal(payload, machines[leader.Self].Applied.Single(a => a.Index == index).Payload);

        // The point of the adaptive rule: a frame that did not shrink is not worth storing, so the entry is
        // marked uncompressed and holds exactly the caller's bytes and no more.
        RaftEntryHeader header = await HeaderAtAsync(cluster, leader, index);
        Assert.Equal(RaftPayloadCompression.None, header.Compression);
        Assert.Equal(payload.Length, header.PayloadLength);
    }

    [Fact]
    public async Task ACompressiblePayloadIsActuallySmallerOnDisk()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL], stateMachine: null, Compressed);

        RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        byte[] payload = CompressiblePayload(64_000);

        long index = await leader.AppendAsync(payload);

        RaftEntryHeader header = await HeaderAtAsync(cluster, leader, index);
        Assert.Equal(RaftPayloadCompression.Lz4Fast, header.Compression);
        Assert.True(
            header.PayloadLength < payload.Length,
            $"expected the stored payload to be smaller than {payload.Length} bytes, got {header.PayloadLength}");
    }

    [Fact]
    public async Task CompressionIsOffByDefaultAndLeavesTheEntryUntouched()
    {
        var machines = new Dictionary<NodeId, RecordingStateMachine>();
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1);

        // TestCluster.FastOptions, i.e. no PayloadCompression set at all.
        await cluster.AddGroupEverywhereAsync(Queue, [1UL], node => machines[node] = new RecordingStateMachine());

        RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        byte[] payload = CompressiblePayload(64_000);

        long index = await leader.AppendAsync(payload);

        await TestCluster.WaitUntilAsync(
            () => machines[leader.Self].Applied.Any(a => a.Index == index),
            TimeSpan.FromSeconds(5),
            $"entry {index} to be applied");

        RaftEntryHeader header = await HeaderAtAsync(cluster, leader, index);
        Assert.Equal(RaftPayloadCompression.None, header.Compression);
        Assert.Equal(payload.Length, header.PayloadLength);
        Assert.Equal(payload, machines[leader.Self].Applied.Single(a => a.Index == index).Payload);
    }

    /// <summary>
    /// Membership entries stay uncompressed even with compression switched on: the replay that rebuilds
    /// configuration at startup reads them straight off the log, with no state machine in the path to
    /// decompress them.
    /// </summary>
    [Fact]
    public async Task MembershipEntriesAreNeverCompressed()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2);
        await cluster.AddGroupEverywhereAsync(Queue, [1UL], stateMachine: null, Compressed);

        RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Queue);
        await leader.AddLearnerAsync(new NodeId(2));

        IRaftWal wal = await LogOfAsync(cluster, leader);
        bool sawMembership = false;
        await foreach (RaftEntryHeader header in wal.ReadHeadersFromAsync(wal.FirstIndex, wal.LastIndex))
        {
            if (header.Kind == RaftEntryKind.Membership)
            {
                sawMembership = true;
                Assert.Equal(RaftPayloadCompression.None, header.Compression);
            }
        }

        Assert.True(sawMembership, "expected the learner addition to have appended a membership entry");
    }

    private static async ValueTask<RaftEntryHeader> HeaderAtAsync(
        TestCluster cluster,
        RaftGroupInstance group,
        long index)
    {
        IRaftWal wal = await LogOfAsync(cluster, group);
        await foreach (RaftEntryHeader header in wal.ReadHeadersFromAsync(index, index))
        {
            return header;
        }

        Assert.Fail($"No entry at index {index}.");
        return default;
    }

    /// <summary>
    /// The group's own log, not a copy: <see cref="InMemoryRaftWalFactory"/> hands out one instance per
    /// group. Never disposed here for that reason — the group is still using it.
    /// </summary>
    private static ValueTask<IRaftWal> LogOfAsync(TestCluster cluster, RaftGroupInstance group)
        => cluster.Logs[group.Self].OpenAsync(group.Group);
}
