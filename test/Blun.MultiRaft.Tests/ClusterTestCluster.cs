// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using Blun.MultiRaft.Cluster;
using Blun.MultiRaft.Hosting;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Tests;

/// <summary>
/// One node with a host and a coordinator over it, plus a drained copy of its event stream.
/// </summary>
/// <remarks>
/// The events are drained into a list by a background reader rather than awaited by the test. The stream is
/// bounded and drops the oldest when nobody reads it, so a test that only looked at the end would be racing
/// the channel's own back-pressure policy rather than the behaviour it means to assert.
/// </remarks>
internal sealed class ClusterTestNode : IAsyncDisposable
{
    private readonly ConcurrentQueue<ClusterEvent> _events = new();
    private readonly CancellationTokenSource _draining = new();
    private readonly string? _dataDirectory;
    private Task? _drain;

    public ClusterTestNode(
        ulong id,
        ClusterMode mode,
        IRaftProtocolTransport transport,
        ImmutableArray<NodeId> nodes = default,
        IRaftStateMachine? adminStateMachine = null,
        string? dataDirectory = null)
    {
        Self = new NodeId(id);
        _dataDirectory = dataDirectory;

        // File-backed when a directory is given, in memory otherwise — and the file-backed path is not
        // decoration. The two logs are separate implementations behind one interface, and a divergence
        // between them is exactly the class of bug the shared WAL contract suite exists to catch; a cluster
        // exercised only in memory would not notice one.
        bool onDisk = dataDirectory is not null;

        Host = new MultiRaftHost(
            Self,
            onDisk
                ? new SegmentedRaftWalFactory(Path.Combine(dataDirectory!, "wal"))
                : new InMemoryRaftWalFactory(),
            onDisk
                ? new FileRaftMetaStore(Path.Combine(dataDirectory!, "meta"))
                : new InMemoryRaftMetaStore(),
            transport,
            onDisk
                ? new FileRaftSnapshotStore(Path.Combine(dataDirectory!, "snapshots"))
                : new InMemoryRaftSnapshotStore(),
            tickInterval: TimeSpan.FromMilliseconds(10));

        Coordinator = new ClusterCoordinator(
            Host,
            new ClusterOptions
            {
                Mode = mode,
                Self = Self,
                Nodes = nodes.IsDefault ? [Self] : nodes,
                AdminStateMachine = adminStateMachine,
                DataDirectory = dataDirectory,

                // Compressed hard, like TestCluster.FastOptions: a cluster that takes a second per pass to
                // converge would dominate the run rather than be measured by it.
                LoadReportInterval = TimeSpan.FromMilliseconds(100),
                LoadReportTtl = TimeSpan.FromSeconds(2),
                ReconcileInterval = TimeSpan.FromMilliseconds(200),
                GroupOptions = new RaftGroupOptions
                {
                    ElectionTimeout = TimeSpan.FromMilliseconds(150),
                    HeartbeatInterval = TimeSpan.FromMilliseconds(40),
                    PromotionCatchUpThreshold = 8,
                },
            });
    }

    public NodeId Self { get; }

    public MultiRaftHost Host { get; }

    public ClusterCoordinator Coordinator { get; }

    public IReadOnlyCollection<ClusterEvent> Events => _events;

    public async ValueTask StartAsync()
    {
        await Host.StartAsync();
        await Coordinator.StartAsync();
        _drain = Task.Run(DrainAsync, CancellationToken.None);
    }

    public static async ValueTask WaitUntilAsync(Func<bool> condition, string what, TimeSpan? timeout = null)
        => await TestCluster.WaitUntilAsync(condition, timeout ?? TimeSpan.FromSeconds(10), what);

    public async ValueTask DisposeAsync()
    {
        await _draining.CancelAsync();
        await Coordinator.DisposeAsync();
        await Host.DisposeAsync();

        if (_drain is not null)
        {
            try
            {
                await _drain;
            }
            catch (OperationCanceledException)
            {
                // Expected.
            }
        }

        _draining.Dispose();

        if (_dataDirectory is not null && Directory.Exists(_dataDirectory))
        {
            Directory.Delete(_dataDirectory, recursive: true);
        }
    }

    private async Task DrainAsync()
    {
        try
        {
            await foreach (ClusterEvent evt in Coordinator.Events.WithCancellation(_draining.Token))
            {
                _events.Enqueue(evt);
            }
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
    }
}

/// <summary>Several <see cref="ClusterTestNode"/> over one in-process network.</summary>
internal sealed class ClusterTestCluster : IAsyncDisposable
{
    private readonly List<ClusterTestNode> _nodes = [];

    public InMemoryRaftCluster Network { get; } = new();

    public IReadOnlyList<ClusterTestNode> Nodes => _nodes;

    public ClusterTestNode Node(ulong id) => _nodes.First(n => n.Self.Value == id);

    private readonly string? _root;

    /// <summary>In-memory stores.</summary>
    public ClusterTestCluster()
    {
    }

    /// <summary>File-backed stores under a temporary root, so the segmented log is the one under test.</summary>
    public ClusterTestCluster(bool onDisk)
        => _root = onDisk
            ? Path.Combine(Path.GetTempPath(), "blun-cluster-" + Guid.NewGuid().ToString("N"))
            : null;

    public ValueTask AddNodesAsync(params ulong[] ids) => AddNodesAsync(null, ids);

    /// <summary>
    /// Starts the nodes. Deliberately not in ascending id order in some tests: which node seeds the cluster
    /// must be decided by the id, not by who happened to start first.
    /// </summary>
    public async ValueTask AddNodesAsync(Func<ulong, IRaftStateMachine>? stateMachine, params ulong[] ids)
    {
        ImmutableArray<NodeId> all = [.. ids.Select(i => new NodeId(i)).OrderBy(n => n)];

        foreach (ulong id in ids)
        {
            ClusterTestNode node = null!;
            IRaftProtocolTransport transport = Network.Connect(new NodeId(id), new LazyClusterListener(() => node));
            node = new ClusterTestNode(
                id,
                ClusterMode.Replicated,
                transport,
                all,
                stateMachine?.Invoke(id),
                _root is null ? null : Path.Combine(_root, "node-" + id.ToString(CultureInfo.InvariantCulture)));
            _nodes.Add(node);
            await node.StartAsync();
        }
    }

    public async ValueTask WaitForFormedAsync(TimeSpan? timeout = null)
        => await WaitUntilAsync(
            () => _nodes.Any(n => n.Coordinator.IsClusterLeader
                                  && n.Coordinator.Group!.Membership.Voters.Length == _nodes.Count),
            "the cluster group to take in every node",
            timeout ?? TimeSpan.FromSeconds(25));

    /// <summary>Creates a group on every node, optionally asking one of them to lead it from the start.</summary>
    public async ValueTask CreateGroupEverywhereAsync(RaftGroupId group, ulong? initialLeader = null)
    {
        RaftMembership membership = RaftMembership.OfVoters([.. _nodes.Select(n => n.Self)]);
        NodeId? wanted = initialLeader is { } id ? new NodeId(id) : null;

        foreach (ClusterTestNode node in _nodes)
        {
            await node.Coordinator.CreateGroupAsync(
                group,
                membership,
                wanted,
                options: TestCluster.FastOptions);
        }

        await WaitForGroupLeaderAsync(group);
    }

    public async ValueTask<RaftGroupInstance> WaitForGroupLeaderAsync(RaftGroupId group, TimeSpan? timeout = null)
    {
        RaftGroupInstance? leader = null;
        await WaitUntilAsync(
            () =>
            {
                RaftGroupInstance[] leaders =
                [
                    .. _nodes
                        .Select(n => n.Host.TryGetGroup(group, out RaftGroupInstance? g) ? g : null)
                        .Where(g => g is { IsLeader: true })!,
                ];

                leader = leaders.Length == 1 ? leaders[0] : null;
                return leader is not null;
            },
            "a single leader for group " + group,
            timeout ?? TimeSpan.FromSeconds(15));

        return leader!;
    }

    public static async ValueTask WaitUntilAsync(Func<bool> condition, string what, TimeSpan? timeout = null)
    {
        long deadline = Stopwatch.GetTimestamp()
                        + (long)((timeout ?? TimeSpan.FromSeconds(15)).TotalSeconds * Stopwatch.Frequency);

        while (Stopwatch.GetTimestamp() < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(10);
        }

        Assert.Fail("Timed out waiting for " + what + ".");
    }

    public async ValueTask DisposeAsync()
    {
        foreach (ClusterTestNode node in _nodes)
        {
            await node.DisposeAsync();
        }

        _nodes.Clear();
    }

    /// <summary>
    /// The host must exist to be connected and be connected to be constructed; a factory breaks the cycle.
    /// This one also forwards the cluster half, which is what lets load reports and leadership questions
    /// travel over the in-process network exactly as they do over gRPC.
    /// </summary>
    private sealed class LazyClusterListener(Func<ClusterTestNode> resolve)
        : IRaftProtocolListener, IRaftClusterListener
    {
        public ValueTask<AppendEntriesResponse> OnAppendEntriesAsync(
            AppendEntriesRequest request,
            IAsyncEnumerable<RaftLogEntry> entries,
            CancellationToken cancellationToken = default)
            => resolve().Host.OnAppendEntriesAsync(request, entries, cancellationToken);

        public ValueTask<InstallSnapshotResponse> OnInstallSnapshotAsync(
            InstallSnapshotRequest request,
            IAsyncEnumerable<ReadOnlyMemory<byte>> body,
            CancellationToken cancellationToken = default)
            => resolve().Host.OnInstallSnapshotAsync(request, body, cancellationToken);

        public ValueTask<ReadIndexResponse> OnReadIndexAsync(
            ReadIndexRequest request,
            CancellationToken cancellationToken = default)
            => resolve().Host.OnReadIndexAsync(request, cancellationToken);

        public ValueTask<TimeoutNowResponse> OnTimeoutNowAsync(
            TimeoutNowRequest request,
            CancellationToken cancellationToken = default)
            => resolve().Host.OnTimeoutNowAsync(request, cancellationToken);

        public ValueTask<VoteResponse> OnRequestVoteAsync(
            VoteRequest request,
            CancellationToken cancellationToken = default)
            => resolve().Host.OnRequestVoteAsync(request, cancellationToken);

        public ValueTask OnLoadReportAsync(NodeLoadReport report, CancellationToken cancellationToken = default)
            => resolve().Host.OnLoadReportAsync(report, cancellationToken);

        public ValueTask<LeaderTargetResponse> OnLeaderTargetAsync(
            LeaderTargetRequest request,
            CancellationToken cancellationToken = default)
            => resolve().Host.OnLeaderTargetAsync(request, cancellationToken);
    }
}
