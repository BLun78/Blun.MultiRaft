// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Collections.Concurrent;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Blun.MultiRaft.Hosting;

/// <summary>
/// Every Raft group living on one node. Owns the two things a group must not own for itself: the transport,
/// and the clock.
/// </summary>
/// <remarks>
/// The clock is the point. A <see cref="System.Threading.Timer"/> per group is a queue entry, a callback and
/// a heap allocation per group per tick; at ten thousand groups that dominates the node's CPU long before
/// consensus does. One timer ticks the whole set instead, and a group's election deadline is just a
/// timestamp it compares against.
/// </remarks>
public sealed class MultiRaftHost : IRaftProtocolListener, IRaftClusterListener, IAsyncDisposable
{
    private readonly ConcurrentDictionary<RaftGroupId, RaftGroupInstance> _groups = new();
    private readonly IRaftWalFactory _walFactory;
    private readonly IRaftMetaStore _metaStore;
    private readonly IRaftSnapshotStore? _snapshots;
    private readonly IRaftProtocolTransport _transport;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly TimeSpan _tickInterval;

    private Task? _ticker;
    private bool _disposed;

    /// <summary>Creates a host. Nothing ticks until <see cref="StartAsync"/> is called.</summary>
    public MultiRaftHost(
        NodeId self,
        IRaftWalFactory walFactory,
        IRaftMetaStore metaStore,
        IRaftProtocolTransport transport,
        IRaftSnapshotStore? snapshotStore = null,
        TimeSpan? tickInterval = null,
        TimeProvider? timeProvider = null,
        ILogger<MultiRaftHost>? logger = null)
    {
        Self = self;
        _walFactory = walFactory ?? throw new ArgumentNullException(nameof(walFactory));
        _metaStore = metaStore ?? throw new ArgumentNullException(nameof(metaStore));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _snapshots = snapshotStore;
        _time = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger<MultiRaftHost>.Instance;
        _tickInterval = tickInterval ?? TimeSpan.FromMilliseconds(25);
    }

    /// <summary>Identity of this node.</summary>
    public NodeId Self { get; }

    /// <summary>Groups currently hosted here.</summary>
    public IReadOnlyCollection<RaftGroupInstance> Groups => [.. _groups.Values];

    /// <summary>How many of the hosted groups this node currently leads. The number placement is chosen by.</summary>
    /// <remarks>
    /// Recomputed by scanning rather than kept as a counter. The scan reads one <see cref="int"/> per group
    /// and is asked for on the order of once a second, so even at ten thousand groups it costs microseconds —
    /// where a counter would mean a role-change callback threaded through the hottest class in the library
    /// for no measurable gain.
    /// </remarks>
    public int LeaderCount
    {
        get
        {
            int count = 0;
            foreach (KeyValuePair<RaftGroupId, RaftGroupInstance> pair in _groups)
            {
                if (pair.Value.IsLeader)
                {
                    count++;
                }
            }

            return count;
        }
    }

    /// <summary>
    /// Where inbound load reports go. Set by <see cref="Cluster.ClusterCoordinator"/> when it starts; the
    /// host itself has no use for them and does not want to know what a cluster is.
    /// </summary>
    internal IRaftClusterListener? ClusterSink { get; set; }

    /// <summary>
    /// The transport every group here shares. Exposed so the cluster coordinator can discover whether it also
    /// speaks <see cref="IRaftClusterTransport"/> without being handed a second reference to the same object.
    /// </summary>
    internal IRaftProtocolTransport Transport => _transport;

    /// <summary>Starts the shared tick loop.</summary>
    public ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        _ticker ??= Task.Run(TickLoopAsync, CancellationToken.None);
        return ValueTask.CompletedTask;
    }

    /// <summary>Creates and starts a group on this node, opening its log.</summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The id falls in the band the library reserves for its own groups. Use
    /// <see cref="Cluster.ClusterCoordinator"/> for the cluster-management group; hosting a second group on
    /// its id would put two different sets of entries in one log.
    /// </exception>
    public ValueTask<RaftGroupInstance> AddGroupAsync(
        RaftGroupId group,
        RaftMembership membership,
        IRaftStateMachine? stateMachine = null,
        RaftGroupOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (group.IsReserved)
        {
            throw new ArgumentOutOfRangeException(
                nameof(group),
                group,
                "Group ids at or above " + RaftGroupId.ReservedFloor + " are reserved for the library.");
        }

        return AddGroupCoreAsync(group, membership, stateMachine, options, cancellationToken);
    }

    internal async ValueTask<RaftGroupInstance> AddGroupCoreAsync(
        RaftGroupId group,
        RaftMembership membership,
        IRaftStateMachine? stateMachine = null,
        RaftGroupOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (_groups.TryGetValue(group, out RaftGroupInstance? existing))
        {
            return existing;
        }

        IRaftWal wal = await _walFactory.OpenAsync(group, cancellationToken).ConfigureAwait(false);
        var instance = new RaftGroupInstance(
            group,
            Self,
            wal,
            _metaStore,
            _transport,
            stateMachine,
            options,
            _time,
            _logger,
            _snapshots);

        if (!_groups.TryAdd(group, instance))
        {
            await instance.DisposeAsync().ConfigureAwait(false);
            await wal.DisposeAsync().ConfigureAwait(false);
            return _groups[group];
        }

        await instance.StartAsync(membership, cancellationToken).ConfigureAwait(false);
        return instance;
    }

    /// <summary>Looks up a hosted group.</summary>
    public bool TryGetGroup(RaftGroupId group, out RaftGroupInstance? instance)
        => _groups.TryGetValue(group, out instance);

    /// <summary>
    /// Stops a group and, when <paramref name="deleteData"/> is set, discards its log and metadata —
    /// the queue it backed was deleted, so keeping either would only waste the disk.
    /// </summary>
    public async ValueTask RemoveGroupAsync(
        RaftGroupId group,
        bool deleteData = false,
        CancellationToken cancellationToken = default)
    {
        if (!_groups.TryRemove(group, out RaftGroupInstance? instance))
        {
            return;
        }

        await instance.DisposeAsync().ConfigureAwait(false);
        if (deleteData)
        {
            await _walFactory.DeleteAsync(group, cancellationToken).ConfigureAwait(false);
            await _metaStore.DeleteAsync(group, cancellationToken).ConfigureAwait(false);
            if (_snapshots is not null)
            {
                await _snapshots.DeleteAsync(group, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask<AppendEntriesResponse> OnAppendEntriesAsync(
        AppendEntriesRequest request,
        IAsyncEnumerable<RaftLogEntry> entries,
        CancellationToken cancellationToken = default)
    {
        if (!_groups.TryGetValue(request.Group, out RaftGroupInstance? instance))
        {
            // Term 0 tells the sender nothing it can act on, which is right: this node has no opinion about
            // a group it does not host, and must not be mistaken for a follower that fell behind.
            return new AppendEntriesResponse(0, Success: false, MatchIndex: 0, ConflictIndex: 0);
        }

        return await instance.OnAppendEntriesAsync(request, entries, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<InstallSnapshotResponse> OnInstallSnapshotAsync(
        InstallSnapshotRequest request,
        IAsyncEnumerable<ReadOnlyMemory<byte>> body,
        CancellationToken cancellationToken = default)
    {
        if (!_groups.TryGetValue(request.Group, out RaftGroupInstance? instance))
        {
            return new InstallSnapshotResponse(0, Success: false);
        }

        return await instance.OnInstallSnapshotAsync(request, body, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<ReadIndexResponse> OnReadIndexAsync(
        ReadIndexRequest request,
        CancellationToken cancellationToken = default)
    {
        if (!_groups.TryGetValue(request.Group, out RaftGroupInstance? instance))
        {
            return new ReadIndexResponse(0, Success: false, 0, 0);
        }

        return await instance.OnReadIndexAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<TimeoutNowResponse> OnTimeoutNowAsync(
        TimeoutNowRequest request,
        CancellationToken cancellationToken = default)
    {
        if (!_groups.TryGetValue(request.Group, out RaftGroupInstance? instance))
        {
            return new TimeoutNowResponse(0, Accepted: false);
        }

        return await instance.OnTimeoutNowAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<VoteResponse> OnRequestVoteAsync(
        VoteRequest request,
        CancellationToken cancellationToken = default)
    {
        if (!_groups.TryGetValue(request.Group, out RaftGroupInstance? instance))
        {
            return new VoteResponse(0, Granted: false);
        }

        return await instance.OnRequestVoteAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public ValueTask OnLoadReportAsync(NodeLoadReport report, CancellationToken cancellationToken = default)
        => ClusterSink?.OnLoadReportAsync(report, cancellationToken) ?? ValueTask.CompletedTask;

    /// <inheritdoc />
    public async ValueTask<LeaderTargetResponse> OnLeaderTargetAsync(
        LeaderTargetRequest request,
        CancellationToken cancellationToken = default)
    {
        if (!_groups.TryGetValue(request.Group, out RaftGroupInstance? instance))
        {
            // The group is not here at all, which is not the same as "here but not leading": the asker was
            // misdirected and should look elsewhere rather than conclude the group has no leader.
            return new LeaderTargetResponse(Self, LeaderTargetStatus.Unreachable, 0, Transferred: false);
        }

        return await instance.EvaluateLeaderTargetAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        await _shutdown.CancelAsync().ConfigureAwait(false);
        if (_ticker is not null)
        {
            try
            {
                await _ticker.ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Whatever ended the tick loop, shutdown must still proceed and dispose every group below.
            }
        }

        foreach (RaftGroupInstance instance in _groups.Values)
        {
            await instance.DisposeAsync().ConfigureAwait(false);
        }

        _groups.Clear();
        _shutdown.Dispose();
    }

    private async Task TickLoopAsync()
    {
        var ticker = new PeriodicTimer(_tickInterval, _time);
        try
        {
            while (await ticker.WaitForNextTickAsync(_shutdown.Token).ConfigureAwait(false))
            {
                // The cluster-management group goes first, ahead of however many queue groups are behind it.
                // The loop awaits each group in turn, so a node hosting ten thousand of them would otherwise
                // put the administrative plane's heartbeat at the mercy of queue traffic — the one group
                // whose responsiveness decides whether the node considers itself usable at all.
                if (_groups.TryGetValue(RaftGroupId.Cluster, out RaftGroupInstance? cluster)
                    && !await TickOneAsync(cluster).ConfigureAwait(false))
                {
                    return;
                }

                foreach (KeyValuePair<RaftGroupId, RaftGroupInstance> pair in _groups)
                {
                    if (pair.Key == RaftGroupId.Cluster)
                    {
                        continue;
                    }

                    if (!await TickOneAsync(pair.Value).ConfigureAwait(false))
                    {
                        return;
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
        finally
        {
            ticker.Dispose();
        }
    }

    /// <summary>Ticks one group. Returns <see langword="false"/> when the loop should stop entirely.</summary>
    private async ValueTask<bool> TickOneAsync(RaftGroupInstance instance)
    {
        try
        {
            await instance.TickAsync(_shutdown.Token).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch (Exception ex)
        {
            // One sick group must never stop the clock for the thousands beside it: catch everything
            // except cancellation, or a single bad tick kills the clock for every other group on this node.
            HostLog.TickFailed(_logger, ex, instance.Group.Value);
            return true;
        }
    }
}

internal static partial class HostLog
{
    [LoggerMessage(EventId = 1100, Level = LogLevel.Warning, Message = "Tick for Raft group {Group} failed.")]
    public static partial void TickFailed(ILogger logger, Exception exception, ulong group);
}
