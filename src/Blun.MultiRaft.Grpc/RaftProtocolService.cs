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
    private readonly IRaftProtocolListener _listener;

    public RaftProtocolService(IRaftProtocolListener listener)
        => _listener = listener ?? throw new ArgumentNullException(nameof(listener));

    /// <inheritdoc />
    public override async Task Session(
        IAsyncStreamReader<RaftFrame> requestStream,
        IServerStreamWriter<RaftFrame> responseStream,
        ServerCallContext context)
    {
        await using var session = new RaftStreamSession(
            responseStream,
            requestStream,
            _listener,
            context.CancellationToken);

        // The call has to stay open as long as the peer keeps the stream: returning would close it and force
        // a reconnect on every group sharing it.
        await session.ReaderLoop.ConfigureAwait(false);
    }
}

/// <summary>Wiring helpers so a host can expose the transport in two lines.</summary>
public static class RaftProtocolServiceExtensions
{
    /// <summary>Registers the service and the listener it dispatches to.</summary>
    public static IServiceCollection AddRaftProtocol(
        this IServiceCollection services,
        Func<IServiceProvider, IRaftProtocolListener> listenerFactory)
    {
        services.AddGrpc();
        services.AddSingleton(listenerFactory);
        return services;
    }

    /// <summary>Maps the single streaming endpoint.</summary>
    public static IEndpointRouteBuilder MapRaftProtocol(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGrpcService<RaftProtocolService>();
        return endpoints;
    }
}
