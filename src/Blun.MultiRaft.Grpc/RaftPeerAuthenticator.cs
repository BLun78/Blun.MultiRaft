// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Security.Cryptography;
using System.Text;
using Blun.MultiRaft.Core;

namespace Blun.MultiRaft.Grpc;

/// <summary>
/// Authenticates the other end of a Raft session. Without one, <see cref="RaftProtocolService"/> accepts any
/// caller and trusts whatever <see cref="NodeId"/> a frame's payload claims — the gap described in
/// <c>doc/audit/SEC-001-raft-transport-ohne-authentifizierung.md</c>. Configuring an implementation closes it
/// two ways: the client attaches a header a listener can check before a session is even opened
/// (<see cref="CreateHeaderAsync"/>), and the server binds the connection to a <see cref="NodeId"/> it can
/// then compare against what individual frames claim (<see cref="AuthenticateAsync"/>), rather than trusting
/// the frame body outright.
/// </summary>
/// <remarks>
/// Deliberately narrow — two methods, no certificate handling, no negotiation — so a stronger scheme (mTLS,
/// reading the client certificate off <c>ServerCallContext.AuthContext</c>) can implement the same interface
/// later without touching any call site. This shape is the shared-secret path from the audit's recommendation
/// 4: weaker than mTLS, but categorically better than the unauthenticated default.
/// </remarks>
public interface IRaftPeerAuthenticator
{
    /// <summary>
    /// Builds the value the client attaches to an outbound session as the <c>x-raft-auth</c> metadata header,
    /// asserting <paramref name="localNode"/> as the caller's identity.
    /// </summary>
    ValueTask<string> CreateHeaderAsync(NodeId localNode, CancellationToken cancellationToken);

    /// <summary>
    /// Validates an inbound <c>x-raft-auth</c> header and returns the <see cref="NodeId"/> it authenticates
    /// for, or <c>null</c> if the header is missing, malformed, or fails validation — the caller must then
    /// refuse the session rather than proceed with an unauthenticated <see cref="NodeId"/>.
    /// </summary>
    ValueTask<NodeId?> AuthenticateAsync(string? headerValue, CancellationToken cancellationToken);
}

/// <summary>
/// Adapts two delegates to <see cref="IRaftPeerAuthenticator"/>, for callers who need neither shared state nor
/// a dedicated type — most uses of a single, unrotated secret. Reach for <see cref="SharedSecretRaftPeerAuthenticator"/>
/// or a hand-written implementation instead when the scheme needs state (key rotation, a revocation list).
/// </summary>
public sealed class DelegateRaftPeerAuthenticator(
    Func<NodeId, CancellationToken, ValueTask<string>> createHeader,
    Func<string?, CancellationToken, ValueTask<NodeId?>> authenticate) : IRaftPeerAuthenticator
{
    private readonly Func<NodeId, CancellationToken, ValueTask<string>> _createHeader =
        createHeader ?? throw new ArgumentNullException(nameof(createHeader));

    private readonly Func<string?, CancellationToken, ValueTask<NodeId?>> _authenticate =
        authenticate ?? throw new ArgumentNullException(nameof(authenticate));

    /// <inheritdoc />
    public ValueTask<string> CreateHeaderAsync(NodeId localNode, CancellationToken cancellationToken)
        => _createHeader(localNode, cancellationToken);

    /// <inheritdoc />
    public ValueTask<NodeId?> AuthenticateAsync(string? headerValue, CancellationToken cancellationToken)
        => _authenticate(headerValue, cancellationToken);
}

/// <summary>
/// A cluster-wide shared secret, HMAC-bound to the claimed <see cref="NodeId"/> rather than sent as a bare
/// token. A bare shared token would let any node that knows it speak for any other node in the cluster; binding
/// the MAC to the id means holding the secret proves membership, not a specific identity, so a peer still
/// cannot mint a header for a <see cref="NodeId"/> it is not.
/// </summary>
/// <remarks>
/// Header shape is <c>"{nodeId}.{base64(HMACSHA256(secret, nodeId))}"</c>. Comparison uses
/// <see cref="CryptographicOperations.FixedTimeEquals(ReadOnlySpan{byte}, ReadOnlySpan{byte})"/> rather than
/// <see cref="string.Equals(string)"/> so a byte-by-byte early exit cannot be timed by a network attacker to
/// recover the expected MAC one byte at a time.
/// </remarks>
public sealed class SharedSecretRaftPeerAuthenticator : IRaftPeerAuthenticator
{
    private readonly byte[] _secret;

    /// <summary>Creates an authenticator bound to <paramref name="secret"/>, shared out of band by every node in the cluster.</summary>
    public SharedSecretRaftPeerAuthenticator(byte[] secret)
    {
        ArgumentNullException.ThrowIfNull(secret);
        if (secret.Length == 0)
        {
            throw new ArgumentException("The shared secret must not be empty.", nameof(secret));
        }

        _secret = secret;
    }

    /// <inheritdoc />
    public ValueTask<string> CreateHeaderAsync(NodeId localNode, CancellationToken cancellationToken)
        => ValueTask.FromResult($"{localNode.Value}.{ComputeMac(localNode)}");

    /// <inheritdoc />
    public ValueTask<NodeId?> AuthenticateAsync(string? headerValue, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(headerValue))
        {
            return ValueTask.FromResult<NodeId?>(null);
        }

        int separator = headerValue.IndexOf('.');
        if (separator < 0 || !ulong.TryParse(headerValue.AsSpan(0, separator), out ulong raw))
        {
            return ValueTask.FromResult<NodeId?>(null);
        }

        var claimed = new NodeId(raw);
        byte[] expected = Encoding.ASCII.GetBytes(ComputeMac(claimed));
        byte[] actual = Encoding.ASCII.GetBytes(headerValue[(separator + 1)..]);

        // FixedTimeEquals requires equal lengths up front; a length mismatch is itself not a MAC and is safe
        // to reject immediately -- there is no secret-dependent branch being timed here.
        bool ok = expected.Length == actual.Length && CryptographicOperations.FixedTimeEquals(expected, actual);
        return ValueTask.FromResult<NodeId?>(ok ? claimed : null);
    }

    private string ComputeMac(NodeId node)
        => Convert.ToBase64String(HMACSHA256.HashData(_secret, BitConverter.GetBytes(node.Value)));
}
