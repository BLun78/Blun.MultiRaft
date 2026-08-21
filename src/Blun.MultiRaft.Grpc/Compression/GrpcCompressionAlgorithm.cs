// Blun.MQ - a fast, pragmatic message queue system with a key-value store
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in this directory
// (or the repository root) for the full license text.

namespace Blun.MultiRaft.Grpc.Compression;

/// <summary>
/// Message compression algorithm for the gRPC wire protocol ("mq:grpc:compression:algorithm").
/// </summary>
internal enum GrpcCompressionAlgorithm
{
    /// <summary>
    /// No compression (gRPC "identity"). The default — see Startup.ConfigureServices for why.
    /// </summary>
    None = 0,

    /// <summary>
    /// gzip: the one algorithm every gRPC implementation understands.
    /// </summary>
    GZip = 1,

    /// <summary>
    /// Brotli: better ratio at comparable cost, but negotiated only with clients that register
    /// <see cref="BrotliCompressionProvider"/> — see its remarks.
    /// </summary>
    Brotli = 2,

#if NET11_0_OR_GREATER
    /// <summary>
    /// Zstandard: better ratio at comparable cost, but negotiated only with clients that register
    /// <see cref="ZstandardCompressionProvider"/> — see its remarks.
    /// </summary>
    Zstandard = 3,
#endif

}
