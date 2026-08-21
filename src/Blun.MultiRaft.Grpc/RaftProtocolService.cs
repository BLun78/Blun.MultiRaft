// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using Blun.MultiRaft.Grpc.Protocol;
using Blun.MultiRaft.Transport;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Blun.MultiRaft.Grpc;

/// <summary>
/// The server half. Accepts an inbound session and hands its frames to the node's listener.
/// </summary>
/// <remarks>
/// There is exactly one method, and it is the only one there will be: everything the protocol does travels
/// as frames on the one stream. Adding an RPC per message type would put each of them on its own HTTP/2
/// stream and undo the multiplexing this transport exists for.
/// </remarks>
public sealed class RaftProtocolService : RaftProtocol.RaftProtocolBase
{
    private const string AuthHeaderName = "x-raft-auth";

    private readonly IRaftProtocolListener _listener;
    private readonly IRaftPeerAuthenticator? _authenticator;
    private readonly ILogger<RaftProtocolService>? _logger;

    public RaftProtocolService(
        IRaftProtocolListener listener,
        IRaftPeerAuthenticator? authenticator = null,
        ILogger<RaftProtocolService>? logger = null)
    {
        _listener = listener ?? throw new ArgumentNullException(nameof(listener));
        _authenticator = authenticator;
        _logger = logger;
    }

    /// <inheritdoc />
    public override async Task Session(
        IAsyncStreamReader<RaftFrame> requestStream,
        IServerStreamWriter<RaftFrame> responseStream,
        ServerCallContext context)
    {
        NodeId? peerId = null;
        if (_authenticator is not null)
        {
            string? header = context.RequestHeaders.GetValue(AuthHeaderName);
            peerId = await _authenticator.AuthenticateAsync(header, context.CancellationToken).ConfigureAwait(false);
            if (peerId is null)
            {
                if (_logger is not null)
                {
                    GrpcLog.PeerAuthenticationFailed(_logger);
                }

                throw new RpcException(new Status(StatusCode.Unauthenticated, "unknown or invalid peer"));
            }
        }

        await using var session = new RaftStreamSession(
            responseStream,
            requestStream,
            _listener,
            context.CancellationToken,
            _logger,
            peerId);

        // The call has to stay open as long as the peer keeps the stream: returning would close it and force
        // a reconnect on every group sharing it.
        await session.ReaderLoop.ConfigureAwait(false);
    }
}

/// <summary>Wiring helpers so a host can expose the transport in two lines.</summary>
public static class RaftProtocolServiceExtensions
{
    /// <summary>
    /// Explicit receive-size ceiling for the Raft gRPC service (SEC-004): left alone, gRPC's own 4 MB default
    /// applies, which is a coincidence rather than a decision this library made — an <c>AppendEntries</c>
    /// frame can legitimately carry <c>RaftGroupOptions.MaxEntriesPerAppend</c> (256 by default) entries, each
    /// up to <c>SegmentedRaftWalOptions.MaxMessageBytes</c> (1 MB), so a generous, explicit, and still finite
    /// ceiling replaces an implicit one rather than a tighter one.
    /// </summary>
    public const int DefaultMaxReceiveMessageBytes = 64 * 1024 * 1024;

    /// <summary>
    /// Registers the service and the listener it dispatches to. Pass <paramref name="authenticatorFactory"/> to
    /// require every inbound session to authenticate — see SEC-001 in <c>doc/audit</c> for what is at stake
    /// when it is left null.
    /// </summary>
    public static IServiceCollection AddRaftProtocol(
        this IServiceCollection services,
        Func<IServiceProvider, IRaftProtocolListener> listenerFactory,
        Func<IServiceProvider, IRaftPeerAuthenticator>? authenticatorFactory = null,
        int maxReceiveMessageBytes = DefaultMaxReceiveMessageBytes)
    {
        services.AddGrpc(options => options.MaxReceiveMessageSize = maxReceiveMessageBytes);
        services.AddSingleton(listenerFactory);
        if (authenticatorFactory is not null)
        {
            services.AddSingleton(authenticatorFactory);
        }

        return services;
    }

    /// <summary>Maps the single streaming endpoint.</summary>
    public static IEndpointRouteBuilder MapRaftProtocol(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGrpcService<RaftProtocolService>();
        return endpoints;
    }
}
