// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Collections.Concurrent;
using System.Net;
using System.Net.Http;
using Blun.MultiRaft.Grpc.Protocol;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;

namespace Blun.MultiRaft.Grpc;

/// <summary>Where the other nodes are. Resolved once at startup; membership changes reuse these addresses.</summary>
public sealed class GrpcRaftTransportOptions
{
    /// <summary>Address of each peer, by node id. The local node may be present and is simply never dialled.</summary>
    public required IReadOnlyDictionary<NodeId, Uri> Peers { get; init; }

    /// <summary>How long to wait for a peer's reply before treating it as unreachable.</summary>
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Which HTTP version to speak to every peer. See <see cref="RaftGrpcProtocol"/>.</summary>
    public RaftGrpcProtocol Protocol { get; init; } = RaftGrpcProtocol.Http2;

    /// <summary>
    /// Optional. Used only to report an inbound request this node could not answer — a session is otherwise
    /// silent, and that silence is what made a follower-side append failure invisible from both ends.
    /// </summary>
    public ILogger? Logger { get; init; }
}

/// <summary>
/// gRPC transport. One bidirectional stream per peer, shared by every group on this node.
/// </summary>
/// <remarks>
/// The session is established lazily on first use and re-established after a failure. A Raft group treats an
/// unreachable peer as an ordinary condition, so a dial failure surfaces as <see cref="IOException"/> — the
/// same thing the in-process cluster raises — rather than as a transport-specific exception the consensus
/// core would have to know about.
/// </remarks>
public sealed class GrpcRaftTransport : IRaftClusterTransport, IAsyncDisposable
{
    private readonly GrpcRaftTransportOptions _options;
    private readonly IRaftProtocolListener _listener;
    private readonly ConcurrentDictionary<NodeId, PeerConnection> _peers = new();
    private bool _disposed;

    public GrpcRaftTransport(GrpcRaftTransportOptions options, IRaftProtocolListener listener)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _listener = listener ?? throw new ArgumentNullException(nameof(listener));
    }

    /// <inheritdoc />
    public async ValueTask<AppendEntriesResponse> AppendEntriesAsync(
        NodeId target,
        AppendEntriesRequest request,
        IAsyncEnumerable<RaftLogEntry> entries,
        CancellationToken cancellationToken = default)
    {
        RaftStreamSession session = await ConnectAsync(target, cancellationToken).ConfigureAwait(false);
        using CancellationTokenSource timeout = Deadline(cancellationToken);
        try
        {
            return await session.AppendEntriesAsync(request, entries, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ShouldDrop(ex, cancellationToken))
        {
            Drop(target);
            throw new IOException("AppendEntries to node " + target + " failed.", ex);
        }
    }

    /// <inheritdoc />
    public async ValueTask<InstallSnapshotResponse> InstallSnapshotAsync(
        NodeId target,
        InstallSnapshotRequest request,
        IAsyncEnumerable<ReadOnlyMemory<byte>> body,
        CancellationToken cancellationToken = default)
    {
        RaftStreamSession session = await ConnectAsync(target, cancellationToken).ConfigureAwait(false);
        try
        {
            // No deadline: a snapshot is as large as the state machine is, and cutting it off partway would
            // guarantee the follower never catches up.
            return await session.InstallSnapshotAsync(request, body, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ShouldDrop(ex, cancellationToken))
        {
            Drop(target);
            throw new IOException("InstallSnapshot to node " + target + " failed.", ex);
        }
    }

    /// <inheritdoc />
    public async ValueTask<ReadIndexResponse> ReadIndexAsync(
        NodeId target,
        ReadIndexRequest request,
        CancellationToken cancellationToken = default)
    {
        RaftStreamSession session = await ConnectAsync(target, cancellationToken).ConfigureAwait(false);
        using CancellationTokenSource timeout = Deadline(cancellationToken);
        try
        {
            return await session.ReadIndexAsync(request, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ShouldDrop(ex, cancellationToken))
        {
            Drop(target);
            throw new IOException("ReadIndex to node " + target + " failed.", ex);
        }
    }

    /// <inheritdoc />
    public async ValueTask<TimeoutNowResponse> TimeoutNowAsync(
        NodeId target,
        TimeoutNowRequest request,
        CancellationToken cancellationToken = default)
    {
        RaftStreamSession session = await ConnectAsync(target, cancellationToken).ConfigureAwait(false);
        using CancellationTokenSource timeout = Deadline(cancellationToken);
        try
        {
            return await session.TimeoutNowAsync(request, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ShouldDrop(ex, cancellationToken))
        {
            Drop(target);
            throw new IOException("TimeoutNow to node " + target + " failed.", ex);
        }
    }

    /// <inheritdoc />
    public async ValueTask<VoteResponse> RequestVoteAsync(
        NodeId target,
        VoteRequest request,
        CancellationToken cancellationToken = default)
    {
        RaftStreamSession session = await ConnectAsync(target, cancellationToken).ConfigureAwait(false);
        using CancellationTokenSource timeout = Deadline(cancellationToken);
        try
        {
            return await session.RequestVoteAsync(request, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ShouldDrop(ex, cancellationToken))
        {
            Drop(target);
            throw new IOException("RequestVote to node " + target + " failed.", ex);
        }
    }

    /// <inheritdoc />
    public async ValueTask ReportLoadAsync(
        NodeId target,
        NodeLoadReport report,
        CancellationToken cancellationToken = default)
    {
        RaftStreamSession session = await ConnectAsync(target, cancellationToken).ConfigureAwait(false);
        using CancellationTokenSource timeout = Deadline(cancellationToken);
        try
        {
            await session.ReportLoadAsync(report, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ShouldDrop(ex, cancellationToken))
        {
            Drop(target);
            throw new IOException("Load report to node " + target + " failed.", ex);
        }
    }

    /// <inheritdoc />
    public async ValueTask<LeaderTargetResponse> LeaderTargetAsync(
        NodeId target,
        LeaderTargetRequest request,
        CancellationToken cancellationToken = default)
    {
        RaftStreamSession session = await ConnectAsync(target, cancellationToken).ConfigureAwait(false);

        // No deadline of its own when the call is meant to act: a handover waits for the successor's log to
        // catch up, which is bounded by the group's own transfer timeout and can legitimately outlast a
        // request timeout sized for heartbeats. A pure query gets the ordinary deadline.
        using CancellationTokenSource timeout = request.Execute
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : Deadline(cancellationToken);

        try
        {
            return await session.LeaderTargetAsync(request, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ShouldDrop(ex, cancellationToken))
        {
            Drop(target);
            throw new IOException("Leader-target request to node " + target + " failed.", ex);
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        foreach (PeerConnection peer in _peers.Values)
        {
            await peer.DisposeAsync().ConfigureAwait(false);
        }

        _peers.Clear();
    }

    private CancellationTokenSource Deadline(CancellationToken cancellationToken)
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        source.CancelAfter(_options.RequestTimeout);
        return source;
    }

    private static bool ShouldDrop(Exception ex, CancellationToken cancellationToken)
        => !cancellationToken.IsCancellationRequested
           && ex is RpcException or IOException or ObjectDisposedException or OperationCanceledException
               or InvalidOperationException;

    private async ValueTask<RaftStreamSession> ConnectAsync(NodeId target, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_peers.TryGetValue(target, out PeerConnection? existing) && existing.IsAlive)
        {
            return existing.Session;
        }

        if (!_options.Peers.TryGetValue(target, out Uri? address))
        {
            throw new IOException("No address configured for node " + target + ".");
        }

        PeerConnection connection = await PeerConnection
            .OpenAsync(address, _options.Protocol, _listener, _options.Logger, cancellationToken)
            .ConfigureAwait(false);

        // A concurrent caller may have won the race; keep whichever landed first and discard the loser
        // rather than leaving two streams to the same peer. When the existing entry is dead, the new
        // connection replaces it and the dead one must still be disposed -- AddOrUpdate only tells us the
        // winner, so the update delegate captures the discarded entry itself.
        PeerConnection? loser = null;
        PeerConnection winner = _peers.AddOrUpdate(
            target,
            connection,
            (_, current) =>
            {
                if (current.IsAlive)
                {
                    return current;
                }

                loser = current;
                return connection;
            });

        if (loser is not null)
        {
            await loser.DisposeAsync().ConfigureAwait(false);
        }

        if (!ReferenceEquals(winner, connection))
        {
            await connection.DisposeAsync().ConfigureAwait(false);
        }

        return winner.Session;
    }

    private void Drop(NodeId target)
    {
        if (_peers.TryRemove(target, out PeerConnection? connection))
        {
            _ = connection.DisposeAsync().AsTask();
        }
    }

    private sealed class PeerConnection : IAsyncDisposable
    {
        private readonly GrpcChannel _channel;
        private readonly AsyncDuplexStreamingCall<RaftFrame, RaftFrame> _call;

        private PeerConnection(
            GrpcChannel channel,
            AsyncDuplexStreamingCall<RaftFrame, RaftFrame> call,
            RaftStreamSession session)
        {
            _channel = channel;
            _call = call;
            Session = session;
        }

        public RaftStreamSession Session { get; }

        // A dead writer leaves the reader running against a half-open connection: every frame the reader
        // still receives is a reply to a call that will never be sent. Both loops must be alive.
        public bool IsAlive => !Session.ReaderLoop.IsCompleted && !Session.WriterLoop.IsCompleted;

        public static ValueTask<PeerConnection> OpenAsync(
            Uri address,
            RaftGrpcProtocol protocol,
            IRaftProtocolListener listener,
            ILogger? logger,
            CancellationToken cancellationToken)
        {
            // EnableMultipleHttp2Connections is the client-side half of "the 101st stream still works": if
            // this channel's single connection to a peer ever hits that peer's advertised concurrent-stream
            // limit, SocketsHttpHandler opens a second physical connection instead of queuing new streams
            // behind the ones already in flight. Set unconditionally -- it does nothing under HTTP/3, which
            // has no per-connection stream ceiling to begin with, and costs nothing when the limit is never
            // approached, which is the common case since this transport normally holds one stream per peer.
            var handler = new SocketsHttpHandler { EnableMultipleHttp2Connections = true };

            // DefaultRequestVersion/DefaultVersionPolicy live on HttpClient, not on the handler underneath
            // it -- wrapping the handler is what GrpcChannelOptions.HttpClient (rather than .HttpHandler)
            // expects, and disposing the channel disposes this client, which disposes the handler in turn.
            var httpClient = new HttpClient(handler);

            if (protocol == RaftGrpcProtocol.Http3)
            {
                // No cleartext mode exists for QUIC -- forcing the exact version rather than negotiating
                // means a peer that can't speak HTTP/3 fails the connection outright instead of silently
                // running the cluster on HTTP/2 while everyone believes it asked for HTTP/3.
                httpClient.DefaultRequestVersion = HttpVersion.Version30;
                httpClient.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact;
            }
            else
            {
                httpClient.DefaultRequestVersion = HttpVersion.Version20;
                httpClient.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact;
            }

            GrpcChannel channel = GrpcChannel.ForAddress(address, new GrpcChannelOptions { HttpClient = httpClient, DisposeHttpClient = true });
            var client = new RaftProtocol.RaftProtocolClient(channel);
            AsyncDuplexStreamingCall<RaftFrame, RaftFrame> call = client.Session(cancellationToken: default);
            var session = new RaftStreamSession(
                call.RequestStream,
                call.ResponseStream,
                listener,
                CancellationToken.None,
                logger);

            return ValueTask.FromResult(new PeerConnection(channel, call, session));
        }

        public async ValueTask DisposeAsync()
        {
            await Session.DisposeAsync().ConfigureAwait(false);
            _call.Dispose();
            _channel.Dispose();
        }
    }
}
