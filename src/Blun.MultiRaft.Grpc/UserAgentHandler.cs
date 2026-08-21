// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

namespace Blun.MultiRaft.Grpc;

/// <summary>
/// Puts this transport's <c>User-Agent</c> (<see cref="RaftUserAgent"/>) on every HTTP/2 request of a peer
/// connection, ahead of the <c>grpc-dotnet/x.y</c> token grpc-dotnet sets for itself.
/// </summary>
/// <remarks>
/// A delegating handler rather than a gRPC interceptor or metadata header, because <c>user-agent</c> is an HTTP
/// header, not gRPC metadata: adding it as metadata would put a second, competing value on the wire next to the
/// one grpc-dotnet already writes. Here the header is rewritten after grpc-dotnet has built the request, so
/// both tokens end up in one well-formed value.
/// </remarks>
internal sealed class UserAgentHandler(string userAgent) : DelegatingHandler
{
    private const string HeaderName = "User-Agent";

    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        // ToString() before Remove(): the collection is what is being read.
        string grpc = request.Headers.UserAgent.ToString();
        request.Headers.Remove(HeaderName);

        // TryAddWithoutValidation: the value is already sanitised (RaftUserAgent.Sanitize), and validation here
        // would reject the combined value for punctuation that is perfectly legal in a user agent a host chose
        // for itself.
        request.Headers.TryAddWithoutValidation(
            HeaderName,
            grpc.Length > 0 ? $"{userAgent} {grpc}" : userAgent);

        return base.SendAsync(request, cancellationToken);
    }
}
