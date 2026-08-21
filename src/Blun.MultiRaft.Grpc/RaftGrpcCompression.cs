// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

namespace Blun.MultiRaft.Grpc;

/// <summary>Compression applied to this node's outbound requests. Mirrors <c>Blun.Mq.Client.MqCompression</c>.</summary>
public enum RaftGrpcCompression
{
    /// <summary>No compression. The default, matching the server's own default.</summary>
    None = 0,

    /// <summary>gzip — understood by every gRPC implementation.</summary>
    GZip = 1,

    /// <summary>Brotli — better ratio, negotiated only between this transport's own endpoints.</summary>
    Brotli = 2,

#if NET11_0_OR_GREATER
    /// <summary>Zstandard — best ratio/speed trade-off of the three, .NET 11+ only.</summary>
    Zstd = 3,
#endif
}
