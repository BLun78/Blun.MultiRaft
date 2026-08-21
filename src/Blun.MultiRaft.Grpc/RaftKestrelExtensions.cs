// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Net;
using System.Runtime.Versioning;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Transport.Quic;
using Microsoft.Extensions.DependencyInjection;

namespace Blun.MultiRaft.Grpc;

/// <summary>Server-side counterpart to <see cref="RaftGrpcProtocol"/> and <see cref="GrpcRaftTransportOptions"/>.</summary>
public static class RaftKestrelExtensions
{
    /// <summary>
    /// Kestrel's own default for <see cref="Http2Limits.MaxStreamsPerConnection"/>. This transport normally
    /// uses exactly one stream per peer connection — every group is multiplexed onto it as frames, not as
    /// separate streams — so the default is rarely the actual constraint in practice. It is raised anyway,
    /// because "the 101st concurrent thing still works" should hold regardless of how many streams a given
    /// deployment ends up opening, not just under this library's own current usage pattern.
    /// </summary>
    public const int DefaultMaxStreamsPerConnection = 4096;

    /// <summary>
    /// Default for <c>KestrelServerOptions.Limits.MaxConcurrentConnections</c> on a Raft endpoint
    /// (SEC-004): <see cref="DefaultMaxStreamsPerConnection"/> is raised well past Kestrel's own default with
    /// no matching cap on the number of *connections*, so the two together bounded neither side. A cluster
    /// normally holds one connection per peer, so this is generous headroom, not a tight budget.
    /// </summary>
    public const int DefaultMaxConcurrentConnections = 256;

    /// <summary>
    /// Kestrel's own default for <c>QuicTransportOptions.MaxUnidirectionalStreamCount</c> is 10. HTTP/3 spends
    /// unidirectional streams on control and QPACK streams rather than on requests, so this is raised only
    /// proportionally — it is not the request-carrying limit, which is
    /// <see cref="DefaultMaxStreamsPerConnection"/>'s HTTP/3 counterpart.
    /// </summary>
    public const int DefaultMaxUnidirectionalStreamCount = 64;

    /// <summary>
    /// Configures one Kestrel endpoint for the Raft protocol: the chosen HTTP version, and a per-connection
    /// concurrent-stream ceiling raised well past Kestrel's default of 100.
    /// </summary>
    /// <remarks>
    /// <paramref name="maxStreamsPerConnection"/> reaches HTTP/2 only. HTTP/3's equivalent ceiling lives on the
    /// QUIC transport (<c>QuicTransportOptions.MaxBidirectionalStreamCount</c>), which is not reachable from
    /// <see cref="KestrelServerOptions"/> at all — a host serving <see cref="RaftGrpcProtocol.Http3"/> must also
    /// call <see cref="ConfigureRaftQuicTransport"/>, or the endpoint silently keeps QUIC's default of 100
    /// concurrent request streams while HTTP/2 gets the raised value.
    /// </remarks>
    /// <remarks>
    /// This endpoint accepts any caller as any <c>NodeId</c> unless <c>RaftProtocolServiceExtensions.AddRaftProtocol</c>
    /// is given an <see cref="IRaftPeerAuthenticator"/>. Without one, whoever can reach this port controls the
    /// cluster — see SEC-001 in <c>doc/audit</c> for the full impact. Configure an authenticator (a shared
    /// secret at minimum, via <see cref="SharedSecretRaftPeerAuthenticator"/>, or mTLS via a custom
    /// implementation), or ensure this endpoint is reachable only from a trusted network segment.
    /// </remarks>
    /// <param name="configureListen">
    /// Anything further the endpoint needs — most commonly <c>listen.UseHttps(...)</c>. This library
    /// deliberately does not call that itself: HTTPS setup means a using directive on
    /// <c>Microsoft.AspNetCore.Server.Kestrel.Https</c> and a certificate the host, not this library, is
    /// responsible for. HTTP/3 has no cleartext mode at all — QUIC mandates TLS — so an endpoint configured
    /// for <see cref="RaftGrpcProtocol.Http3"/> without HTTPS set up here will accept no connections at all;
    /// that failure is visible immediately as "nothing connects," not as something this method could catch
    /// in advance without knowing what certificate setup the host intends to use.
    /// </param>
    /// <param name="allowCleartext">
    /// Must be set to accept an endpoint on a non-loopback address with no <paramref name="configureListen"/>
    /// (and therefore no TLS): otherwise this call throws. See SEC-002 in <c>doc/audit</c> — the replication
    /// stream carries every queue payload and the full state in a snapshot, and without TLS all of it is
    /// readable and alterable by anyone on the network path. Loopback addresses are exempt because there is no
    /// network path to protect against.
    /// </param>
    public static void ConfigureRaftEndpoint(
        this KestrelServerOptions options,
        IPAddress address,
        int port,
        RaftGrpcProtocol protocol = RaftGrpcProtocol.Http2,
        int maxStreamsPerConnection = DefaultMaxStreamsPerConnection,
        Action<ListenOptions>? configureListen = null,
        bool allowCleartext = false,
        int maxConcurrentConnections = DefaultMaxConcurrentConnections)
    {
        // HTTP/3 mandates TLS at the QUIC layer, so there is no cleartext case to guard there. For HTTP/2, an
        // endpoint with no configureListen callback has no way to have called UseHttps, so it is h2c -- the
        // one combination this check exists to make an explicit decision instead of a silent default.
        if (protocol != RaftGrpcProtocol.Http3
            && configureListen is null
            && !IPAddress.IsLoopback(address)
            && !allowCleartext)
        {
            throw new InvalidOperationException(
                $"ConfigureRaftEndpoint({address}:{port}) would accept unencrypted h2c Raft traffic on a "
                + "non-loopback address. Pass configureListen to call listen.UseHttps(...), or pass "
                + "allowCleartext: true to accept the risk explicitly. See SEC-002 in doc/audit.");
        }

        options.Limits.Http2.MaxStreamsPerConnection = maxStreamsPerConnection;
        options.Limits.MaxConcurrentConnections = maxConcurrentConnections;

        options.Listen(
            address,
            port,
            listen =>
            {
                listen.Protocols = protocol == RaftGrpcProtocol.Http3
                    ? HttpProtocols.Http3
                    : HttpProtocols.Http2;
                
                configureListen?.Invoke(listen);
            });
    }

    /// <summary>
    /// HTTP/3 counterpart to <see cref="ConfigureRaftEndpoint"/>'s <c>maxStreamsPerConnection</c>: raises the
    /// QUIC transport's per-connection concurrent-stream ceilings to match what the HTTP/2 path already gets.
    /// Call this alongside <see cref="ConfigureRaftEndpoint"/> whenever the endpoint speaks
    /// <see cref="RaftGrpcProtocol.Http3"/>; it is inert for an HTTP/2-only host.
    /// </summary>
    /// <remarks>
    /// Separate from <see cref="ConfigureRaftEndpoint"/> because the two ceilings live on different objects:
    /// HTTP/2's is a Kestrel limit, HTTP/3's is a transport option bound through DI, and nothing on
    /// <see cref="KestrelServerOptions"/> can reach it. <c>QuicTransportOptions</c> is still a .NET preview API,
    /// which is why this method carries <see cref="RequiresPreviewFeaturesAttribute"/> rather than the library
    /// opting every consumer in: a host that wants HTTP/3 sets <c>EnablePreviewFeatures</c> and accepts that the
    /// shape of these options may change, and a host on HTTP/2 never sees the requirement.
    /// </remarks>
    [RequiresPreviewFeatures("QuicTransportOptions is a .NET preview API and may change in a future release.")]
    public static IServiceCollection ConfigureRaftQuicTransport(
        this IServiceCollection services,
        int maxStreamsPerConnection = DefaultMaxStreamsPerConnection,
        int maxUnidirectionalStreamCount = DefaultMaxUnidirectionalStreamCount)
    {
        services.Configure<QuicTransportOptions>(quic =>
        {
            quic.MaxBidirectionalStreamCount = maxStreamsPerConnection;
            quic.MaxUnidirectionalStreamCount = maxUnidirectionalStreamCount;
        });

        return services;
    }
}
