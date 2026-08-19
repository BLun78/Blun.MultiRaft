// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

namespace Blun.MultiRaft.Grpc;

/// <summary>
/// Which HTTP version the Raft gRPC session runs over. Chosen once, the same on both ends of a node pair —
/// there is no negotiation, because a Raft session is long-lived and internal to the cluster, not a public
/// endpoint that needs to accommodate whatever a caller happens to speak.
/// </summary>
public enum RaftGrpcProtocol
{
    /// <summary>
    /// HTTP/2, cleartext (h2c) unless the peer address is <c>https://</c>. The default: no certificates
    /// required to stand up a local or trusted-network cluster, which is what
    /// <see cref="GrpcRaftTransportOptions"/>'s own defaults assume.
    /// </summary>
    Http2 = 0,

    /// <summary>
    /// HTTP/3 over QUIC. Unlike HTTP/2, QUIC has no cleartext mode at all — TLS is mandatory, not optional,
    /// so the peer address must be <c>https://</c> and the server must be listening with a certificate
    /// configured. Choosing this without TLS on both ends fails the connection outright rather than quietly
    /// falling back to HTTP/2, which is deliberate: a silent downgrade would make "I asked for HTTP/3" mean
    /// nothing.
    /// </summary>
    Http3 = 1,
}
