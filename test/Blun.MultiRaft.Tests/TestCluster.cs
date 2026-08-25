// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Blun.MultiRaft.Core;
using Blun.MultiRaft.Hosting;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Tests;

/// <summary>
/// An in-process cluster of hosts sharing one <see cref="InMemoryRaftCluster"/>.
/// Timings are compressed hard — a 120 ms election timeout instead of the production default — so a full
/// leader failover finishes inside a test run instead of dominating it.
/// </summary>
internal sealed class TestCluster : IAsyncDisposable
{
    private readonly Dictionary<NodeId, MultiRaftHost> _hosts = [];
    private readonly InMemoryRaftCluster _network = new();

    public Dictionary<NodeId, InMemoryRaftSnapshotStore> Snapshots { get; } = [];

    /// <summary>
    /// The log factories, so a test can reopen a group's log and read back what was actually stored — the
    /// factory hands out the same instance for a group, so this is the very log the group is writing to.
    /// </summary>
    public Dictionary<NodeId, InMemoryRaftWalFactory> Logs { get; } = [];

    public static RaftGroupOptions FastOptions => new()
    {
        ElectionTimeout = TimeSpan.FromMilliseconds(120),
        ElectionJitter = 1.0,
        HeartbeatInterval = TimeSpan.FromMilliseconds(30),
        PromotionCatchUpThreshold = 8,

        // Set explicitly rather than left to its default of ten election timeouts. Everything else here is
        // compressed on purpose, but that default would compress this along with it -- to 1.2s -- and how
        // long it takes to install a snapshot and replay the entries after it has nothing to do with how
        // fast this suite wants elections to happen. Under a parallel run the two would occasionally cross,
        // which is a property of the test configuration and not of the transfer.
        LeadershipTransferCatchUpTimeout = TimeSpan.FromSeconds(10),
    };

    public InMemoryRaftCluster Network => _network;

    public IReadOnlyDictionary<NodeId, MultiRaftHost> Hosts => _hosts;

    public async ValueTask<TestCluster> WithNodesAsync(params ulong[] nodes)
    {
        foreach (ulong raw in nodes)
        {
            var node = new NodeId(raw);
            MultiRaftHost host = null!;
            IRaftProtocolTransport transport = _network.Connect(node, new LazyListener(() => host));
            host = new MultiRaftHost(
                node,
                Logs[node] = new InMemoryRaftWalFactory(),
                new InMemoryRaftMetaStore(),
                transport,
                Snapshots[node] = new InMemoryRaftSnapshotStore(),
                tickInterval: TimeSpan.FromMilliseconds(10));
            _hosts[node] = host;
            await host.StartAsync();
        }

        return this;
    }

    public async ValueTask<RaftMembership> AddGroupEverywhereAsync(
        RaftGroupId group,
        IEnumerable<ulong> voters,
        Func<NodeId, IRaftStateMachine>? stateMachine = null,
        RaftGroupOptions? options = null)
    {
        RaftMembership membership = RaftMembership.OfVoters([.. voters.Select(v => new NodeId(v))]);
        foreach ((NodeId node, MultiRaftHost host) in _hosts)
        {
            await host.AddGroupAsync(group, membership, stateMachine?.Invoke(node), options ?? FastOptions);
        }

        return membership;
    }

    public Core.RaftGroupInstance Group(ulong node, RaftGroupId group)
    {
        Assert.True(_hosts[new NodeId(node)].TryGetGroup(group, out Core.RaftGroupInstance? instance));
        return instance!;
    }

    public IEnumerable<Core.RaftGroupInstance> GroupsOf(RaftGroupId group)
        => _hosts.Values
            .Select(host => host.TryGetGroup(group, out Core.RaftGroupInstance? g) ? g : null)
            .Where(g => g is not null)!;

    /// <summary>Waits for the group to settle on exactly one leader and returns it.</summary>
    public async ValueTask<Core.RaftGroupInstance> WaitForLeaderAsync(RaftGroupId group, TimeSpan? timeout = null)
    {
        Core.RaftGroupInstance? leader = null;
        await WaitUntilAsync(
            () =>
            {
                Core.RaftGroupInstance[] leaders = [.. GroupsOf(group).Where(g => g.IsLeader)];
                leader = leaders.Length == 1 ? leaders[0] : null;
                return leader is not null;
            },
            timeout ?? TimeSpan.FromSeconds(5),
            "a single leader");

        return leader!;
    }

    public static async ValueTask WaitUntilAsync(Func<bool> condition, TimeSpan timeout, string what)
    {
        long deadline = Stopwatch.GetTimestamp() + (long)(timeout.TotalSeconds * Stopwatch.Frequency);
        while (Stopwatch.GetTimestamp() < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(5);
        }

        Assert.Fail("Timed out waiting for " + what + ".");
    }

    public async ValueTask DisposeAsync()
    {
        foreach (MultiRaftHost host in _hosts.Values)
        {
            await host.DisposeAsync();
        }

        _hosts.Clear();
    }

    /// <summary>
    /// The host has to exist before it can be connected and has to be connected before it can be
    /// constructed. Indirecting through a factory breaks the cycle without leaking a setter onto the host.
    /// </summary>
    private sealed class LazyListener(Func<MultiRaftHost> resolve) : IRaftProtocolListener
    {
        public ValueTask<AppendEntriesResponse> OnAppendEntriesAsync(
            AppendEntriesRequest request,
            IAsyncEnumerable<RaftLogEntry> entries,
            CancellationToken cancellationToken = default)
            => resolve().OnAppendEntriesAsync(request, entries, cancellationToken);

        public ValueTask<InstallSnapshotResponse> OnInstallSnapshotAsync(
            InstallSnapshotRequest request,
            IAsyncEnumerable<ReadOnlyMemory<byte>> body,
            CancellationToken cancellationToken = default)
            => resolve().OnInstallSnapshotAsync(request, body, cancellationToken);

        public ValueTask<ReadIndexResponse> OnReadIndexAsync(
            ReadIndexRequest request,
            CancellationToken cancellationToken = default)
            => resolve().OnReadIndexAsync(request, cancellationToken);

        public ValueTask<TimeoutNowResponse> OnTimeoutNowAsync(
            TimeoutNowRequest request,
            CancellationToken cancellationToken = default)
            => resolve().OnTimeoutNowAsync(request, cancellationToken);

        public ValueTask<VoteResponse> OnRequestVoteAsync(
            VoteRequest request,
            CancellationToken cancellationToken = default)
            => resolve().OnRequestVoteAsync(request, cancellationToken);
    }
}

/// <summary>Records what was applied, so tests can assert on order and on exactly-once application.</summary>
internal sealed class RecordingStateMachine : IRaftSnapshotableStateMachine
{
    private readonly ConcurrentQueue<(long Index, byte[] Payload)> _applied = new();

    public IReadOnlyCollection<(long Index, byte[] Payload)> Applied => _applied;

    public ValueTask ApplyAsync(RaftGroupId group, RaftLogEntry entry, CancellationToken cancellationToken = default)
    {
        // Copied deliberately: the entry's payload aliases a buffer the log recycles on the next iteration.
        _applied.Enqueue((entry.Index, entry.Payload.ToArray()));
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Serializes the applied commands as length-prefixed records. Deliberately trivial: what the test is
    /// about is the transfer and the log surgery around it, not the encoding.
    /// </summary>
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> CaptureAsync(
        RaftGroupId group,
        long lastAppliedIndex,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach ((long index, byte[] payload) in _applied.Where(a => a.Index <= lastAppliedIndex).ToArray())
        {
            byte[] record = new byte[12 + payload.Length];
            BinaryPrimitives.WriteInt64LittleEndian(record, index);
            BinaryPrimitives.WriteInt32LittleEndian(record.AsSpan(8), payload.Length);
            payload.CopyTo(record.AsSpan(12));
            yield return record;
        }

        await ValueTask.CompletedTask.ConfigureAwait(false);
    }

    public async ValueTask RestoreAsync(
        RaftGroupId group,
        IAsyncEnumerable<ReadOnlyMemory<byte>> body,
        CancellationToken cancellationToken = default)
    {
        using var buffer = new MemoryStream();
        await foreach (ReadOnlyMemory<byte> chunk in body.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            await buffer.WriteAsync(chunk, cancellationToken).ConfigureAwait(false);
        }

        // Wholesale replacement, never a merge: a restore that kept prior state would leave the replica
        // agreeing with nobody.
        _applied.Clear();

        byte[] bytes = buffer.ToArray();
        int offset = 0;
        while (offset + 12 <= bytes.Length)
        {
            long index = BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(offset));
            int length = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset + 8));
            offset += 12;
            if (length < 0 || offset + length > bytes.Length)
            {
                break;
            }

            _applied.Enqueue((index, bytes.AsSpan(offset, length).ToArray()));
            offset += length;
        }
    }
}
