// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Collections.Immutable;
using System.Globalization;
using Blun.MultiRaft.Cluster;
using Blun.MultiRaft.Core;
using Blun.MultiRaft.Grpc;
using Blun.MultiRaft.Hosting;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Node;

/// <summary>
/// One node of the demo cluster: a <see cref="MultiRaftHost"/>, a gRPC transport pointed at its peers, and a
/// few groups to elect leaders for.
/// </summary>
/// <remarks>
/// It is also the <see cref="IRaftProtocolListener"/> the gRPC service dispatches into, which is why the
/// transport and the service can share one object without either knowing about the other.
/// </remarks>
public sealed class RaftNodeHost : IHostedService, IRaftProtocolListener, IRaftClusterListener, IAsyncDisposable
{
    /// <summary>
    /// Several groups rather than one, because a single group would demonstrate Raft, not <em>multi</em>-Raft.
    /// With three, leadership normally lands on different nodes for different groups — which is the property
    /// the whole design exists for, and it is visible on the dashboard within seconds.
    /// </summary>
    private static readonly RaftGroupId[] Groups = [new(1), new(2), new(3)];

    private readonly NodeId _self;
    private readonly IReadOnlyDictionary<NodeId, Uri> _peers;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<RaftNodeHost> _logger;
    private readonly string? _dataDirectory;
    private readonly RaftGrpcProtocol _protocol;

    private GrpcRaftTransport? _transport;
    private MultiRaftHost? _host;
    private ClusterCoordinator? _cluster;
    private MessageSender? _sender;
    private Task? _events;
    private CancellationTokenSource? _stopping;
    private IRaftSnapshotStore? _snapshots;

    /// <summary>The last few cluster events, so the dashboard can show that the plane is doing something.</summary>
    private readonly Queue<string> _recentEvents = new();

    public RaftNodeHost(
        NodeId self,
        IReadOnlyDictionary<NodeId, Uri> peers,
        string? dataDirectory,
        RaftGrpcProtocol protocol,
        ILoggerFactory loggerFactory)
    {
        _self = self;
        _peers = peers;
        _dataDirectory = dataDirectory;
        _protocol = protocol;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<RaftNodeHost>();
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _transport = new GrpcRaftTransport(
            new GrpcRaftTransportOptions
            {
                Peers = _peers,
                LocalNode = _self,
                Protocol = _protocol,
                Logger = _loggerFactory.CreateLogger<GrpcRaftTransport>(),
            },
            this);

        IRaftWalFactory wal = string.IsNullOrWhiteSpace(_dataDirectory)
            ? new InMemoryRaftWalFactory()
            : new SegmentedRaftWalFactory(Path.Combine(_dataDirectory, "wal"));

        IRaftMetaStore meta = string.IsNullOrWhiteSpace(_dataDirectory)
            ? new InMemoryRaftMetaStore()
            : new FileRaftMetaStore(Path.Combine(_dataDirectory, "meta"));

        IRaftSnapshotStore snapshots = string.IsNullOrWhiteSpace(_dataDirectory)
            ? new InMemoryRaftSnapshotStore()
            : new FileRaftSnapshotStore(Path.Combine(_dataDirectory, "snapshots"));
        _snapshots = snapshots;

        _host = new MultiRaftHost(
            _self,
            wal,
            meta,
            _transport,
            snapshots,
            tickInterval: TimeSpan.FromMilliseconds(50),
            logger: _loggerFactory.CreateLogger<MultiRaftHost>());

        await _host.StartAsync(cancellationToken).ConfigureAwait(false);

        // The cluster group comes up before any queue does. It is what decides whether this node's
        // administrative plane is usable at all, and the queues below do not depend on that answer -- each
        // has its own quorum -- but the node should know it before it starts claiming to serve anything.
        _cluster = new ClusterCoordinator(
            _host,
            new ClusterOptions
            {
                Mode = _peers.Count > 1 ? ClusterMode.Replicated : ClusterMode.SingleNode,
                Self = _self,
                Nodes = [.. _peers.Keys],
                DataDirectory = string.IsNullOrWhiteSpace(_dataDirectory)
                    ? null
                    : Path.Combine(_dataDirectory, "cluster"),
                GroupOptions = new RaftGroupOptions
                {
                    ElectionTimeout = TimeSpan.FromMilliseconds(1500),
                    HeartbeatInterval = TimeSpan.FromMilliseconds(300),
                },
            },
            logger: _loggerFactory.CreateLogger<ClusterCoordinator>());

        await _cluster.StartAsync(cancellationToken).ConfigureAwait(false);

        _stopping = new CancellationTokenSource();
        _events = Task.Run(PumpClusterEventsAsync, CancellationToken.None);

        RaftMembership membership = RaftMembership.OfVoters([.. _peers.Keys]);

        // Election timeouts are generous compared with the tests: this is a real network, and the nodes are
        // starting at the same moment, so a tight timeout would just produce split votes while the peers'
        // listeners are still coming up.
        var options = new RaftGroupOptions
        {
            ElectionTimeout = TimeSpan.FromMilliseconds(1500),
            HeartbeatInterval = TimeSpan.FromMilliseconds(300),
        };

        foreach (RaftGroupId group in Groups)
        {
            // A real host applies committed entries to actual state (Blun.MQ: an enqueue or a settlement).
            // This demo has none, so DemoStateMachine only counts what it has seen -- just enough of a state
            // machine for TakeSnapshotAsync to have something to capture and for the observer's "snapshot
            // now" button to do something real.
            await _host
                .AddGroupAsync(group, membership, new DemoStateMachine(), options, cancellationToken)
                .ConfigureAwait(false);
        }

        _sender = new MessageSender(_host, _loggerFactory.CreateLogger<MessageSender>());

        NodeStartedLog.Started(_logger, _self.Value, _peers.Count, Groups.Length);
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken) => await DisposeAsync().ConfigureAwait(false);

    /// <summary>
    /// Asks this node's cluster coordinator to move a group's leadership, so the placement API is reachable
    /// from the demo without a client library. Any node may be asked; the request is routed to the group's
    /// current leader wherever it is.
    /// </summary>
    public async Task<object> RequestLeaderAsync(ulong group, ulong? preferred)
    {
        if (_cluster is null)
        {
            return new { error = "the cluster coordinator is not running" };
        }

        LeaderTargetResponse response = await _cluster
            .RequestLeaderTransferAsync(
                new RaftGroupId(group),
                preferred is { } id ? new NodeId(id) : null)
            .ConfigureAwait(false);

        return new
        {
            group,
            node = response.Node.Value,
            status = response.Status.ToString(),
            lag = response.Lag,
            transferred = response.Transferred,
        };
    }

    /// <summary>
    /// Takes a node out of the cluster group's configuration, or puts it back in as a learner. Cluster
    /// leader only — a membership change is an append, and only a leader appends.
    /// </summary>
    /// <remarks>
    /// A removal does not stick while the node is still configured on this one: the coordinator's reconcile
    /// pass adopts any configured node missing from the membership and promotes it back one per pass. That
    /// is the mechanism working, not fighting the request, and watching a node go out and come back is the
    /// clearest demonstration of the single-server change rule there is.
    /// </remarks>
    public async Task<object> ChangeMembershipAsync(ulong node, bool remove)
    {
        if (_cluster?.Group is not { IsLeader: true } group)
        {
            return new
            {
                node,
                applied = false,
                error = "not the cluster leader",
                clusterLeader = _cluster?.ClusterLeader?.Value.ToString(CultureInfo.InvariantCulture),
            };
        }

        var target = new NodeId(node);

        try
        {
            return await ApplyMembershipAsync(group, target, node, remove).ConfigureAwait(false);
        }
        catch (NotLeaderException)
        {
            // Checked IsLeader a moment ago and it was true; leadership can move between the two. The race
            // is the cluster working, so it is reported as an answer rather than thrown at the caller.
            return new { node, applied = false, error = "leadership moved while the change was being applied" };
        }
    }

    private async Task<object> ApplyMembershipAsync(RaftGroupInstance group, NodeId target, ulong node, bool remove)
    {
        if (remove)
        {
            if (target == _self)
            {
                // Removing the leader from its own configuration is a leadership question, not a membership
                // one; it belongs on the transfer path where a successor is chosen first.
                return new { node, applied = false, error = "the cluster leader cannot remove itself" };
            }

            if (group.Membership.Voters.Length <= 3)
            {
                return new { node, applied = false, error = "refusing to shrink the voter set below three" };
            }

            await group.RemoveNodeAsync(target).ConfigureAwait(false);
        }
        else
        {
            await group.AddLearnerAsync(target).ConfigureAwait(false);
        }

        return new
        {
            node,
            applied = true,
            removed = remove,
            voters = group.Membership.Voters.Select(v => v.Value).ToArray(),
            learners = group.Membership.Learners.Select(v => v.Value).ToArray(),
        };
    }

    /// <summary>
    /// Captures this node's own applied state as a snapshot and compacts the log up to it. Any node may take
    /// one of its own state — unlike an append, this needs no quorum — so this is not routed to the leader
    /// the way membership changes are; the observer still points the button at the group's leader by
    /// convention, since that is the copy people are already looking at in the WAL panel.
    /// </summary>
    public async Task<object> TakeSnapshotAsync(ulong group)
    {
        var id = new RaftGroupId(group);

        if (_host is null || !_host.TryGetGroup(id, out RaftGroupInstance? instance) || instance is null)
        {
            return new { group, taken = false, error = "the node is not running that group" };
        }

        bool taken = await instance.TakeSnapshotAsync().ConfigureAwait(false);
        return new
        {
            group,
            taken,
            error = taken ? null : "nothing to snapshot yet -- no entries applied since the log's retained start",
        };
    }

    /// <summary>
    /// Starts a run of dummy traffic into a group, so its write-ahead log has something in it. Leader only —
    /// an append goes to the leader, and the observer routes the request there.
    /// </summary>
    public object StartSending(ulong group, int count, int intervalMs, int size)
        => _sender is null
            ? new { group, started = false, error = "the node is not running" }
            : _sender.Start(group, count, TimeSpan.FromMilliseconds(Math.Max(1, intervalMs)), Math.Max(16, size));

    /// <summary>Stops a run that is in progress.</summary>
    public object StopSending(ulong group)
    {
        _sender?.Stop(group);
        return new { group, stopping = true };
    }

    /// <summary>
    /// What this node's copy of a group's log costs on disk, and what is in it.
    /// </summary>
    /// <remarks>
    /// Measured by walking the directory rather than asked of the log, because the number that matters here
    /// is what the filesystem holds, segments and all — a counter of bytes appended would not include the
    /// segment that is allocated and not yet full. The layout is the factory's (<c>g</c> plus the group id,
    /// zero-padded), which is a coupling the demo accepts to be able to show real bytes.
    /// </remarks>
    private object DescribeWal(RaftGroupId group, RaftGroupInstance? instance)
    {
        (long bytes, int files) = MeasureWal(group);

        long first = instance?.FirstIndex ?? 0;
        long last = instance?.LastIndex ?? 0;
        long entries = last > 0 && last >= first ? last - first + 1 : 0;

        return new
        {
            sizeBytes = bytes,
            segments = files,
            firstIndex = first,
            lastIndex = last,
            entries,
            bytesPerEntry = entries > 0 ? bytes / entries : 0,
        };
    }

    /// <summary>
    /// What this node's stored snapshot for a group looks like, or <see langword="null"/> when it has none.
    /// Size is measured off the snapshot file the same way <see cref="MeasureWal"/> measures the log — the
    /// metadata itself carries no byte count, only the log position the snapshot stands for.
    /// </summary>
    private async ValueTask<object?> DescribeSnapshotAsync(RaftGroupId group)
    {
        if (_snapshots is null)
        {
            return null;
        }

        RaftSnapshotMetadata? metadata = await _snapshots.ReadMetadataAsync(group).ConfigureAwait(false);
        if (metadata is null)
        {
            return null;
        }

        return new
        {
            lastIncludedIndex = metadata.Value.LastIncludedIndex,
            lastIncludedTerm = metadata.Value.LastIncludedTerm,
            sizeBytes = MeasureSnapshotFile(group),
        };
    }

    private long MeasureSnapshotFile(RaftGroupId group)
    {
        if (string.IsNullOrWhiteSpace(_dataDirectory))
        {
            return 0;
        }

        string path = Path.Combine(
            _dataDirectory,
            "snapshots",
            "g" + group.Value.ToString("D20", CultureInfo.InvariantCulture) + ".snap");

        try
        {
            return File.Exists(path) ? new FileInfo(path).Length : 0;
        }
        catch (IOException)
        {
            // A snapshot being swapped in underneath the walk is not worth failing a status request over.
            return 0;
        }
    }

    private (long Bytes, int Files) MeasureWal(RaftGroupId group)
    {
        if (string.IsNullOrWhiteSpace(_dataDirectory))
        {
            return (0, 0);
        }

        string directory = Path.Combine(
            _dataDirectory,
            "wal",
            "g" + group.Value.ToString("D20", CultureInfo.InvariantCulture));

        if (!Directory.Exists(directory))
        {
            return (0, 0);
        }

        long bytes = 0;
        int files = 0;

        foreach (string file in Directory.EnumerateFiles(directory))
        {
            try
            {
                bytes += new FileInfo(file).Length;
                files++;
            }
            catch (IOException)
            {
                // A segment being rolled underneath the walk is not worth failing a status request over.
            }
            catch (UnauthorizedAccessException)
            {
                // Same.
            }
        }

        return (bytes, files);
    }

    private object? DescribeSend(ulong group)
    {
        SendJob? job = _sender?.Job(group);

        return job is null
            ? null
            : new
            {
                running = job.Running,
                sent = job.Sent,
                failed = job.Failed,
                total = job.Total,
                size = job.Size,
                ratePerSecond = Math.Round(job.RatePerSecond, 1),
                error = job.Error,
            };
    }

    /// <summary>What the cluster leader believes each node is carrying. Empty anywhere else.</summary>
    public object DescribeLoad()
    {
        if (_cluster is null)
        {
            return new { clusterLeader = (string?)null, nodes = Array.Empty<object>() };
        }

        ClusterLoadSnapshot load = _cluster.GetLoad();
        return new
        {
            clusterLeader = load.ClusterLeader?.Value.ToString(CultureInfo.InvariantCulture),
            nodes = load.Nodes.Select(n => new
            {
                node = n.Node.Value,
                groups = n.GroupCount,
                leaders = n.LeaderCount,
                ageMs = (long)n.Age.TotalMilliseconds,
            }).ToArray(),
        };
    }

    /// <summary>A snapshot of what this node believes, for the dashboard and for the scenario's assertions.</summary>
    public async ValueTask<object> DescribeAsync()
    {
        object?[] groupDescriptions = await Task.WhenAll(Groups.Select(DescribeGroupAsync)).ConfigureAwait(false);

        return new
        {
            node = _self.Value.ToString(CultureInfo.InvariantCulture),
            cluster = new
            {
                mode = _cluster?.Mode.ToString(),
                state = _cluster?.State.ToString(),
                isClusterLeader = _cluster?.IsClusterLeader ?? false,
                clusterLeader = _cluster?.ClusterLeader?.Value.ToString(CultureInfo.InvariantCulture),
                voters = _cluster?.Group?.Membership.Voters.Select(v => v.Value).ToArray() ?? [],
                learners = _cluster?.Group?.Membership.Learners.Select(v => v.Value).ToArray() ?? [],

                // The administrative group's own Raft state, shaped exactly like a queue group's entry
                // below. It is a Raft group like any other and reading it next to them is the point --
                // "the cluster is fine" and "the group that decides that is fine" are separate claims.
                group = await DescribeClusterGroupAsync().ConfigureAwait(false),
                recentEvents = RecentEvents(),
            },
            groups = groupDescriptions,
        };
    }

    private async Task<object> DescribeGroupAsync(RaftGroupId group)
    {
        object? snapshot = await DescribeSnapshotAsync(group).ConfigureAwait(false);

        if (_host is null || !_host.TryGetGroup(group, out RaftGroupInstance? instance) || instance is null)
        {
            // The log is still measured: a group this node has not opened may well have segments on
            // disk from before it was restarted, and that is worth seeing rather than hiding.
            return new
            {
                group = group.Value,
                role = "unknown",
                term = 0L,
                leader = (string?)null,
                commitIndex = 0L,
                wal = DescribeWal(group, null),
                snapshot,
                send = DescribeSend(group.Value),
            };
        }

        return new
        {
            group = group.Value,
            role = instance.Role.ToString(),
            term = instance.CurrentTerm,
            leader = instance.LeaderId?.Value.ToString(CultureInfo.InvariantCulture),
            commitIndex = instance.CommitIndex,
            wal = DescribeWal(group, instance),
            snapshot,
            send = DescribeSend(group.Value),
        };
    }

    private async ValueTask<object?> DescribeClusterGroupAsync()
    {
        RaftGroupInstance? instance = _cluster?.Group;
        if (instance is null)
        {
            return null;
        }

        object? snapshot = await DescribeSnapshotAsync(RaftGroupId.Cluster).ConfigureAwait(false);

        return new
            {
                group = RaftGroupId.Cluster.Value,
                role = instance.Role.ToString(),
                term = instance.CurrentTerm,
                leader = instance.LeaderId?.Value.ToString(CultureInfo.InvariantCulture),
                commitIndex = instance.CommitIndex,
                wal = DescribeWal(RaftGroupId.Cluster, instance),
                snapshot,
                send = DescribeSend(RaftGroupId.Cluster.Value),
            };
    }

    /// <inheritdoc />
    public ValueTask<AppendEntriesResponse> OnAppendEntriesAsync(
        AppendEntriesRequest request,
        IAsyncEnumerable<RaftLogEntry> entries,
        CancellationToken cancellationToken = default)
        => _host is null
            ? ValueTask.FromResult(new AppendEntriesResponse(0, false, 0, 0))
            : _host.OnAppendEntriesAsync(request, entries, cancellationToken);

    /// <inheritdoc />
    public ValueTask<InstallSnapshotResponse> OnInstallSnapshotAsync(
        InstallSnapshotRequest request,
        IAsyncEnumerable<ReadOnlyMemory<byte>> body,
        CancellationToken cancellationToken = default)
        => _host is null
            ? ValueTask.FromResult(new InstallSnapshotResponse(0, false))
            : _host.OnInstallSnapshotAsync(request, body, cancellationToken);

    /// <inheritdoc />
    public ValueTask<ReadIndexResponse> OnReadIndexAsync(
        ReadIndexRequest request,
        CancellationToken cancellationToken = default)
        => _host is null
            ? ValueTask.FromResult(new ReadIndexResponse(0, false, 0, 0))
            : _host.OnReadIndexAsync(request, cancellationToken);

    /// <inheritdoc />
    public ValueTask<TimeoutNowResponse> OnTimeoutNowAsync(
        TimeoutNowRequest request,
        CancellationToken cancellationToken = default)
        => _host is null
            ? ValueTask.FromResult(new TimeoutNowResponse(0, false))
            : _host.OnTimeoutNowAsync(request, cancellationToken);

    /// <inheritdoc />
    public ValueTask<VoteResponse> OnRequestVoteAsync(
        VoteRequest request,
        CancellationToken cancellationToken = default)
        => _host is null
            ? ValueTask.FromResult(new VoteResponse(0, false))
            : _host.OnRequestVoteAsync(request, cancellationToken);

    /// <inheritdoc />
    public ValueTask OnLoadReportAsync(NodeLoadReport report, CancellationToken cancellationToken = default)
        => _host is null ? ValueTask.CompletedTask : _host.OnLoadReportAsync(report, cancellationToken);

    /// <inheritdoc />
    public ValueTask<LeaderTargetResponse> OnLeaderTargetAsync(
        LeaderTargetRequest request,
        CancellationToken cancellationToken = default)
        => _host is null
            ? ValueTask.FromResult(new LeaderTargetResponse(_self, LeaderTargetStatus.Unreachable, 0, false))
            : _host.OnLeaderTargetAsync(request, cancellationToken);

    private string[] RecentEvents()
    {
        lock (_recentEvents)
        {
            return [.. _recentEvents];
        }
    }

    private async Task PumpClusterEventsAsync()
    {
        if (_cluster is null || _stopping is null)
        {
            return;
        }

        try
        {
            await foreach (ClusterEvent evt in _cluster.Events.WithCancellation(_stopping.Token).ConfigureAwait(false))
            {
                NodeStartedLog.ClusterEvent(_logger, evt.Kind.ToString(), evt.Node.Value, evt.State.ToString());

                lock (_recentEvents)
                {
                    _recentEvents.Enqueue(
                        evt.Kind + ":" + evt.Node.Value.ToString(CultureInfo.InvariantCulture) + ":" + evt.State);
                    while (_recentEvents.Count > 10)
                    {
                        _recentEvents.Dequeue();
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_stopping is not null)
        {
            await _stopping.CancelAsync().ConfigureAwait(false);
        }

        if (_events is not null)
        {
            try
            {
                await _events.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }

            _events = null;
        }

        if (_sender is not null)
        {
            // Before the host: a run still appending into a group being torn down would spend its last
            // moments logging failures about it.
            await _sender.DisposeAsync().ConfigureAwait(false);
            _sender = null;
        }

        if (_cluster is not null)
        {
            await _cluster.DisposeAsync().ConfigureAwait(false);
            _cluster = null;
        }

        _stopping?.Dispose();
        _stopping = null;

        if (_host is not null)
        {
            await _host.DisposeAsync().ConfigureAwait(false);
            _host = null;
        }

        if (_transport is not null)
        {
            await _transport.DisposeAsync().ConfigureAwait(false);
            _transport = null;
        }
    }
}

internal static partial class NodeStartedLog
{
    [LoggerMessage(
        EventId = 2000,
        Level = LogLevel.Information,
        Message = "Raft node {Node} started with {PeerCount} peers and {GroupCount} groups.")]
    public static partial void Started(ILogger logger, ulong node, int peerCount, int groupCount);

    [LoggerMessage(
        EventId = 2001,
        Level = LogLevel.Information,
        Message = "Cluster event {Kind} for node {Node}; this node's administrative plane is {State}.")]
    public static partial void ClusterEvent(ILogger logger, string kind, ulong node, string state);
}
