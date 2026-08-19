// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Net;
using Microsoft.AspNetCore.Server.Kestrel.Core;

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
    /// Configures one Kestrel endpoint for the Raft protocol: the chosen HTTP version, and a per-connection
    /// concurrent-stream ceiling raised well past Kestrel's default of 100.
    /// </summary>
    /// <param name="configureListen">
    /// Anything further the endpoint needs — most commonly <c>listen.UseHttps(...)</c>. This library
    /// deliberately does not call that itself: HTTPS setup means a using directive on
    /// <c>Microsoft.AspNetCore.Server.Kestrel.Https</c> and a certificate the host, not this library, is
    /// responsible for. HTTP/3 has no cleartext mode at all — QUIC mandates TLS — so an endpoint configured
    /// for <see cref="RaftGrpcProtocol.Http3"/> without HTTPS set up here will accept no connections at all;
    /// that failure is visible immediately as "nothing connects," not as something this method could catch
    /// in advance without knowing what certificate setup the host intends to use.
    /// </param>
    public static void ConfigureRaftEndpoint(
        this KestrelServerOptions options,
        IPAddress address,
        int port,
        RaftGrpcProtocol protocol = RaftGrpcProtocol.Http2,
        int maxStreamsPerConnection = DefaultMaxStreamsPerConnection,
        Action<ListenOptions>? configureListen = null)
    {
        options.Limits.Http2.MaxStreamsPerConnection = maxStreamsPerConnection;

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
}
