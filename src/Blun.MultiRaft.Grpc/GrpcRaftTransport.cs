// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Collections.Concurrent;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using Blun.MultiRaft.Core;
using Blun.MultiRaft.Grpc.Compression;
using Blun.MultiRaft.Grpc.Protocol;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using GzipCompressionProvider = Grpc.Net.Compression.GzipCompressionProvider;

namespace Blun.MultiRaft.Grpc;

/// <summary>Where the other nodes are. Resolved once at startup; membership changes reuse these addresses.</summary>
public sealed class GrpcRaftTransportOptions
{
    /// <summary>Address of each peer, by node id. The local node may be present and is simply never dialled.</summary>
    public required IReadOnlyDictionary<NodeId, Uri> Peers { get; init; }

    /// <summary>
    /// This node's own id, asserted to <see cref="Authenticator"/> when opening an outbound session. Required
    /// so a caller cannot forget to configure it and silently fall back to an unauthenticated identity.
    /// </summary>
    public required NodeId LocalNode { get; init; }

    /// <summary>How long to wait for a peer's reply before treating it as unreachable.</summary>
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Which HTTP version to speak to every peer. See <see cref="RaftGrpcProtocol"/>.</summary>
    public RaftGrpcProtocol Protocol { get; init; } = RaftGrpcProtocol.Http2;

    /// <summary>
    /// Optional. Used only to report an inbound request this node could not answer — a session is otherwise
    /// silent, and that silence is what made a follower-side append failure invisible from both ends.
    /// </summary>
    public ILogger? Logger { get; init; }

    /// <summary>
    /// Optional. When set, every outbound session carries an <c>x-raft-auth</c> header built from
    /// <see cref="LocalNode"/> for the peer to validate. See SEC-001 in <c>doc/audit</c>: without this, the
    /// endpoint on the other end has no way to tell this node apart from an arbitrary caller.
    /// </summary>
    public IRaftPeerAuthenticator? Authenticator { get; init; }

    /// <summary>
    /// Optional. Identifies the host application in the <c>User-Agent</c> sent on every peer connection,
    /// ahead of this library's own token. See <see cref="RaftUserAgent.ForApplication"/>.
    /// </summary>
    public string? UserAgent { get; init; }

    /// <summary>
    /// Off by default: an idle-cluster heartbeat or a small <c>AppendEntries</c> rarely has enough payload for
    /// compression to pay for its own CPU cost, so this is opt-in rather than assumed. When turned on, applies
    /// to this node's outbound requests only -- response compression is negotiated separately, via the peer's
    /// configured algorithm and the accept-encoding implied by <see cref="CompressionLevel"/>'s providers below.
    /// </summary>
    public bool EnableCompression { get; init; }

    /// <summary>
    /// Compression algorithm used once <see cref="EnableCompression"/> is on. Zstandard by default -- best
    /// ratio/speed trade-off of the three (.NET 11+ only; see <see cref="RaftGrpcCompression"/>). Matches
    /// <c>Blun.Mq.Client.MqClientOptions.Compression</c>'s options, minus the "off" state, which
    /// <see cref="EnableCompression"/> now owns.
    /// </summary>
    public RaftGrpcCompression Compression { get; init; } =
#if NET11_0_OR_GREATER
        RaftGrpcCompression.Zstd;
#else
        RaftGrpcCompression.Brotli;
#endif

    /// <summary>Compression effort for both directions. Only relevant when <see cref="Compression"/> is set.</summary>
    public CompressionLevel CompressionLevel { get; init; } = CompressionLevel.Fastest;

    /// <summary>
    /// See <c>SocketsHttpHandler.EnableMultipleHttp2Connections</c>: past a peer's advertised concurrent-stream
    /// limit, a second physical connection opens instead of queuing new streams behind ones already in flight.
    /// </summary>
    public bool EnableMultipleHttp2Connections { get; init; } = true;

    /// <summary>See <c>SocketsHttpHandler.EnableMultipleHttp3Connections</c>; only relevant under <see cref="RaftGrpcProtocol.Http3"/>.</summary>
    public bool EnableMultipleHttp3Connections { get; init; } = true;

    /// <summary>
    /// Interval between keep-alive pings on an otherwise-idle connection. A group's tick loop is normally
    /// enough traffic on its own, but a quiet cluster (a drained queue, an idle group) can otherwise sit
    /// silent long enough for the transport to reclaim the connection out from under it.
    /// </summary>
    public TimeSpan KeepAlivePingDelay { get; init; } = TimeSpan.FromSeconds(20);

    /// <summary>How long a keep-alive ping may go unanswered before the connection is considered dead.</summary>
    public TimeSpan KeepAlivePingTimeout { get; init; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Skips server-certificate validation entirely. Off by default; only meant for local/dev clusters
    /// running self-signed certificates. Matches <c>Blun.Mq.Client.MqClientOptions.AllowUntrustedServerCertificate</c>.
    /// </summary>
    public bool AllowUntrustedServerCertificate { get; init; }
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
            .OpenAsync(address, _options, _listener, cancellationToken)
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

        public static async ValueTask<PeerConnection> OpenAsync(
            Uri address,
            GrpcRaftTransportOptions options,
            IRaftProtocolListener listener,
            CancellationToken cancellationToken)
        {
            // EnableMultipleHttp2/3Connections is the client-side half of "the 101st stream still works": if
            // this channel's single connection to a peer ever hits that peer's advertised concurrent-stream
            // limit, SocketsHttpHandler opens a second physical connection instead of queuing new streams
            // behind the ones already in flight. Costs nothing when the limit is never approached, which is
            // the common case since this transport normally holds one stream per peer.
            var handler = new SocketsHttpHandler
            {
                PreAuthenticate = true,
                AllowAutoRedirect = false,
                MaxConnectionsPerServer = 10000,
                EnableMultipleHttp2Connections = options.EnableMultipleHttp2Connections,
                EnableMultipleHttp3Connections = options.EnableMultipleHttp3Connections,

                // Without this an idle connection is torn down after ~60s. A group between elections, or one
                // behind a drained queue, can sit quiet for minutes with its peer stream still open -- the
                // transport cannot tell that apart from an abandoned connection, so pings keep traffic on the
                // wire and stop it reclaiming the connection out from under a live session.
                KeepAlivePingDelay = options.KeepAlivePingDelay,
                KeepAlivePingTimeout = options.KeepAlivePingTimeout,
                KeepAlivePingPolicy = HttpKeepAlivePingPolicy.Always,

                // The pings above keep an *established* connection alive; this stops the pool retiring the
                // connection underneath them on its own idle schedule.
                PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
            };

            if (options.AllowUntrustedServerCertificate)
            {
                // Off by default -- see GrpcRaftTransportOptions.AllowUntrustedServerCertificate. Only the
                // validation callback is replaced; everything else keeps SocketsHttpHandler's own defaults.
                handler.SslOptions.RemoteCertificateValidationCallback = static (_, _, _, _) => true;
            }

            // UserAgentHandler sits between the socket handler and the HttpClient so it can rewrite the
            // request's User-Agent header after grpc-dotnet has already set its own token on it -- see
            // UserAgentHandler for why this cannot be done as gRPC metadata instead. DefaultRequestVersion/
            // DefaultVersionPolicy still have to live on HttpClient, not on either handler, and only one of
            // GrpcChannelOptions.HttpClient/.HttpHandler may be set, which is why the chain is wrapped into a
            // single HttpClient rather than passed as HttpHandler directly.
            var userAgentHandler = new UserAgentHandler(RaftUserAgent.ForApplication(options.UserAgent)) { InnerHandler = handler };

            // Disposing the channel disposes this client, which disposes userAgentHandler, which disposes the
            // socket handler in turn.
            var httpClient = new HttpClient(userAgentHandler);

            if (options.Protocol == RaftGrpcProtocol.Http3)
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

            // MaxReceiveMessageSize matches RaftProtocolServiceExtensions.DefaultMaxReceiveMessageBytes
            // (SEC-004): an explicit, generous-but-finite ceiling rather than gRPC's implicit 4 MB default.
            // CompressionProviders determines the grpc-accept-encoding this node advertises, and therefore
            // what a peer is allowed to compress its responses with.
            GrpcChannel channel = GrpcChannel.ForAddress(
                address,
                new GrpcChannelOptions
                {
                    HttpClient = httpClient,
                    DisposeHttpClient = true,
                    MaxReceiveMessageSize = RaftProtocolServiceExtensions.DefaultMaxReceiveMessageBytes,
                    CompressionProviders =
                    [
#if NET11_0_OR_GREATER
                        new ZstandardCompressionProvider(options.CompressionLevel),
#endif
                        new BrotliCompressionProvider(options.CompressionLevel),
                        new GzipCompressionProvider(options.CompressionLevel),
                    ],
                });
            var client = new RaftProtocol.RaftProtocolClient(channel);

            var metadata = new Metadata();
            if (options.Authenticator is not null)
            {
                // Built before the call opens: a session with no valid header is exactly the SEC-001 gap this
                // authenticator exists to close, so there is no "connect first, authenticate later" path here.
                string header = await options.Authenticator.CreateHeaderAsync(options.LocalNode, cancellationToken).ConfigureAwait(false);
                metadata.Add("x-raft-auth", header);
            }

            // Opts this call's *requests* into compression. Responses are negotiated separately, via the
            // peer's configured algorithm and the accept-encoding implied by CompressionProviders above.
            string? requestEncoding = options.EnableCompression
                ? options.Compression switch
                {
#if NET11_0_OR_GREATER
                    RaftGrpcCompression.Zstd => GrpcCompressionAlgorithms.Zstd,
#endif
                    RaftGrpcCompression.Brotli => GrpcCompressionAlgorithms.Brotli,
                    RaftGrpcCompression.GZip => GrpcCompressionAlgorithms.GZip,
                    _ => null,
                }
                : null;

            if (requestEncoding is not null)
            {
                metadata.Add("grpc-internal-encoding-request", requestEncoding);
            }

            var callOptions = new CallOptions().WithHeaders(metadata);
            AsyncDuplexStreamingCall<RaftFrame, RaftFrame> call = client.Session(callOptions);
            var session = new RaftStreamSession(
                call.RequestStream,
                call.ResponseStream,
                listener,
                CancellationToken.None,
                options.Logger);

            return new PeerConnection(channel, call, session);
        }

        public async ValueTask DisposeAsync()
        {
            await Session.DisposeAsync().ConfigureAwait(false);
            _call.Dispose();
            _channel.Dispose();
        }
    }
}
