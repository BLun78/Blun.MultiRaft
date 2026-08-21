// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Blun.MultiRaft.Observer;

/// <summary>Server-sent events, written by hand because that is all this needs.</summary>
internal static class Sse
{
    private static readonly byte[] DataPrefix = Encoding.UTF8.GetBytes("data: ");
    private static readonly byte[] Terminator = Encoding.UTF8.GetBytes("\n\n");

    public static void Begin(HttpContext context)
    {
        context.Response.Headers.ContentType = "text/event-stream";
        context.Response.Headers.CacheControl = "no-cache";
        context.Response.Headers["X-Accel-Buffering"] = "no";
    }

    public static async Task WriteAsync<T>(
        HttpContext context,
        T payload,
        JsonTypeInfo<T> typeInfo,
        CancellationToken cancellationToken)
    {
        // Serialised to bytes first and written through Response.Body only: interleaving Response.WriteAsync
        // with a serializer writing to the body stream puts two writers on one pipe.
        byte[] json = JsonSerializer.SerializeToUtf8Bytes(payload, typeInfo);

        await context.Response.Body.WriteAsync(DataPrefix, cancellationToken).ConfigureAwait(false);
        await context.Response.Body.WriteAsync(json, cancellationToken).ConfigureAwait(false);
        await context.Response.Body.WriteAsync(Terminator, cancellationToken).ConfigureAwait(false);
        await context.Response.Body.FlushAsync(cancellationToken).ConfigureAwait(false);
    }
}
