// Blun.MQ - a fast, pragmatic message queue system with a key-value store
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in this directory
// (or the repository root) for the full license text.

using System.IO.Compression;
using Grpc.Net.Compression;

namespace Blun.MultiRaft.Grpc.Compression;

/// <summary>
/// Brotli message compression for the gRPC wire protocol. grpc-dotnet ships only gzip and
/// deflate providers, so "br" is supplied here via <see cref="ICompressionProvider"/>.
/// </summary>
/// <remarks>
/// <para>
/// "br" is not one of the encodings registered by the gRPC spec (identity/gzip/deflate/snappy),
/// so it is negotiated only with clients that register this same provider — our own
/// <c>Blun.Mq.Client</c>. Any other client simply omits "br" from grpc-accept-encoding and the
/// server falls back to sending uncompressed messages, so enabling it cannot break interop.
/// </para>
/// <para>
/// Both ends of the wire have to implement the algorithm identically, so this file is compiled
/// into <c>Blun.Mq.Client</c> too — linked from that project, the same way mq.proto is. Linked
/// as source rather than shared via a project reference, because the client package must not
/// depend on the engine assembly (doc/specs/10-client.md).
/// </para>
/// </remarks>
internal sealed class BrotliCompressionProvider : ICompressionProvider
{
    private readonly CompressionLevel _defaultCompressionLevel;


    /// <summary>
    /// Initializes a new instance of the <see cref="BrotliCompressionProvider"/> class with the specified <see cref="CompressionLevel"/>.
    /// </summary>
    /// <param name="defaultCompressionLevel">The default compression level to use when compressing data.</param>
    public BrotliCompressionProvider(CompressionLevel defaultCompressionLevel)
    {
        _defaultCompressionLevel = defaultCompressionLevel;
    }

    public const string Encoding = "br";

    public string EncodingName => Encoding;

    public Stream CreateCompressionStream(Stream stream, CompressionLevel? compressionLevel)
        => new BrotliStream(stream, compressionLevel ?? _defaultCompressionLevel, leaveOpen: true);

    public Stream CreateDecompressionStream(Stream stream)
        => new BrotliStream(stream, CompressionMode.Decompress, leaveOpen: true);
}
