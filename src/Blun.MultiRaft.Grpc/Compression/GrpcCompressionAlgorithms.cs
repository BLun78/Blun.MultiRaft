// Blun.MQ - a fast, pragmatic message queue system with a key-value store
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in this directory
// (or the repository root) for the full license text.

namespace Blun.MultiRaft.Grpc.Compression;

/// <summary>
/// Compression algorithm names as they appear on the wire, in grpc-encoding and
/// grpc-accept-encoding.
/// </summary>
/// <remarks>
/// Shared with <c>Blun.Mq.Client</c>, which links this file: the two ends negotiate on these
/// exact strings, so a typo on one side silently degrades to uncompressed rather than failing.
/// The <c>GrpcCompressionAlgorithm</c> enum next door stays server-side — it binds
/// configuration, which the client models with its own public option type. Named in plain
/// <c>&lt;c&gt;</c> rather than a cref on purpose: this file compiles into the client too, where
/// that type does not exist, and a cref would not resolve there.
/// </remarks>
internal static class GrpcCompressionAlgorithms
{
    /// <summary>The one algorithm every gRPC implementation understands.</summary>
    public const string GZip = "gzip";

    /// <summary>Brotli — see <see cref="BrotliCompressionProvider"/>.</summary>
    public const string Brotli = BrotliCompressionProvider.Encoding;

#if NET11_0_OR_GREATER
    /// <summary> Zstandard — see <see cref="ZstandardCompressionProvider"/>.</summary>
    public const string Zstd = ZstandardCompressionProvider.Encoding;
#endif

}
