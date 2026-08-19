// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Globalization;
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
public sealed class RaftNodeHost : IHostedService, IRaftProtocolListener, IAsyncDisposable
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
        _transport = new GrpcRaftTransport(new GrpcRaftTransportOptions { Peers = _peers, Protocol = _protocol }, this);

        IRaftWalFactory wal = string.IsNullOrWhiteSpace(_dataDirectory)
            ? new InMemoryRaftWalFactory()
            : new SegmentedRaftWalFactory(Path.Combine(_dataDirectory, "wal"));

        IRaftMetaStore meta = string.IsNullOrWhiteSpace(_dataDirectory)
            ? new InMemoryRaftMetaStore()
            : new FileRaftMetaStore(Path.Combine(_dataDirectory, "meta"));

        IRaftSnapshotStore snapshots = string.IsNullOrWhiteSpace(_dataDirectory)
            ? new InMemoryRaftSnapshotStore()
            : new FileRaftSnapshotStore(Path.Combine(_dataDirectory, "snapshots"));

        _host = new MultiRaftHost(
            _self,
            wal,
            meta,
            _transport,
            snapshots,
            tickInterval: TimeSpan.FromMilliseconds(50),
            logger: _loggerFactory.CreateLogger<MultiRaftHost>());

        await _host.StartAsync(cancellationToken).ConfigureAwait(false);

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
            await _host.AddGroupAsync(group, membership, null, options, cancellationToken).ConfigureAwait(false);
        }

        NodeStartedLog.Started(_logger, _self.Value, _peers.Count, Groups.Length);
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken) => await DisposeAsync().ConfigureAwait(false);

    /// <summary>A snapshot of what this node believes, for the dashboard and for the scenario's assertions.</summary>
    public object Describe()
        => new
        {
            node = _self.Value.ToString(CultureInfo.InvariantCulture),
            groups = Groups.Select(group =>
            {
                if (_host is null || !_host.TryGetGroup(group, out RaftGroupInstance? instance) || instance is null)
                {
                    return new { group = group.Value, role = "unknown", term = 0L, leader = (string?)null, commitIndex = 0L };
                }

                return new
                {
                    group = group.Value,
                    role = instance.Role.ToString(),
                    term = instance.CurrentTerm,
                    leader = instance.LeaderId?.Value.ToString(CultureInfo.InvariantCulture),
                    commitIndex = instance.CommitIndex,
                };
            }).ToArray(),
        };

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
    public async ValueTask DisposeAsync()
    {
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
}
