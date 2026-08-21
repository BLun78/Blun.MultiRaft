// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Net;
using System.Net.Sockets;
using System.Text;
using Blun.MultiRaft.Grpc;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Blun.MultiRaft.Tests;

/// <summary>
/// SEC-001's shared-secret path: a header HMAC-bound to the claimed NodeId, and a session that refuses to
/// dispatch a frame whose claimed sender does not match the connection's authenticated identity.
/// </summary>
public sealed class RaftPeerAuthenticatorTests
{
    [Fact]
    public async Task SharedSecretRoundTripsForTheClaimedNode()
    {
        var authenticator = new SharedSecretRaftPeerAuthenticator("correct horse battery staple"u8.ToArray());
        var node = new NodeId(7);

        string header = await authenticator.CreateHeaderAsync(node, CancellationToken.None);
        NodeId? authenticated = await authenticator.AuthenticateAsync(header, CancellationToken.None);

        Assert.Equal(node, authenticated);
    }

    [Fact]
    public async Task SharedSecretRejectsATamperedHeader()
    {
        var authenticator = new SharedSecretRaftPeerAuthenticator("correct horse battery staple"u8.ToArray());
        string header = await authenticator.CreateHeaderAsync(new NodeId(7), CancellationToken.None);

        // Flip the claimed node without recomputing the MAC -- forging a different identity with a MAC that
        // was only ever valid for node 7.
        string tampered = "9" + header[1..];

        Assert.Null(await authenticator.AuthenticateAsync(tampered, CancellationToken.None));
    }

    [Fact]
    public async Task SharedSecretRejectsAHeaderFromTheWrongSecret()
    {
        var authenticator = new SharedSecretRaftPeerAuthenticator("correct horse battery staple"u8.ToArray());
        var impostor = new SharedSecretRaftPeerAuthenticator("wrong secret"u8.ToArray());
        string header = await impostor.CreateHeaderAsync(new NodeId(7), CancellationToken.None);

        Assert.Null(await authenticator.AuthenticateAsync(header, CancellationToken.None));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("not-a-valid-header")]
    [InlineData("7.")]
    public async Task SharedSecretRejectsMalformedInput(string? header)
    {
        var authenticator = new SharedSecretRaftPeerAuthenticator("correct horse battery staple"u8.ToArray());
        Assert.Null(await authenticator.AuthenticateAsync(header, CancellationToken.None));
    }

    [Fact]
    public async Task DelegateAuthenticatorForwardsToBothDelegates()
    {
        var authenticator = new DelegateRaftPeerAuthenticator(
            (node, _) => ValueTask.FromResult($"header-for-{node.Value}"),
            (header, _) => ValueTask.FromResult<NodeId?>(header == "header-for-3" ? new NodeId(3) : null));

        Assert.Equal("header-for-3", await authenticator.CreateHeaderAsync(new NodeId(3), CancellationToken.None));
        Assert.Equal(new NodeId(3), await authenticator.AuthenticateAsync("header-for-3", CancellationToken.None));
        Assert.Null(await authenticator.AuthenticateAsync("header-for-4", CancellationToken.None));
    }

    [Fact]
    public async Task ASessionWithoutAValidHeaderIsRejectedBeforeAnyFrameIsProcessed()
    {
        var secret = "cluster-secret"u8.ToArray();
        int port = GetFreeTcpPort();

        WebApplicationBuilder builder = WebApplication.CreateBuilder([]);
        builder.Logging.ClearProviders();
        builder.WebHost.UseKestrel(options =>
            options.ConfigureRaftEndpoint(IPAddress.Loopback, port, RaftGrpcProtocol.Http2));
        builder.Services.AddRaftProtocol(
            _ => new RejectAllListener(),
            _ => new SharedSecretRaftPeerAuthenticator(secret));

        await using WebApplication app = builder.Build();
        app.MapRaftProtocol();
        await app.StartAsync();

        try
        {
            // No Authenticator configured on the client -- the outbound session carries no x-raft-auth header
            // at all, which the server-side authenticator must treat the same as an invalid one.
            await using var transport = new GrpcRaftTransport(
                new GrpcRaftTransportOptions
                {
                    Peers = new Dictionary<NodeId, Uri> { [new NodeId(1)] = new("http://localhost:" + port) },
                    LocalNode = new NodeId(0),
                    Protocol = RaftGrpcProtocol.Http2,
                },
                new RejectAllListener());

            await Assert.ThrowsAsync<IOException>(() => transport.RequestVoteAsync(
                new NodeId(1),
                new VoteRequest(new RaftGroupId(1), 1, new NodeId(0), 0, 0, false),
                CancellationToken.None).AsTask());
        }
        finally
        {
            await app.StopAsync();
        }
    }

    [Fact]
    public async Task AMismatchedClaimedSenderIsDroppedRatherThanDispatched()
    {
        var secret = "cluster-secret"u8.ToArray();
        int port = GetFreeTcpPort();
        var listener = new RecordingListener();

        WebApplicationBuilder builder = WebApplication.CreateBuilder([]);
        builder.Logging.ClearProviders();
        builder.WebHost.UseKestrel(options =>
            options.ConfigureRaftEndpoint(IPAddress.Loopback, port, RaftGrpcProtocol.Http2));
        builder.Services.AddRaftProtocol(
            _ => listener,
            _ => new SharedSecretRaftPeerAuthenticator(secret));

        await using WebApplication app = builder.Build();
        app.MapRaftProtocol();
        await app.StartAsync();

        try
        {
            // Authenticated as node 5, but every frame below claims to be leader/candidate 99 -- the mismatch
            // this check exists to catch: an authenticated-but-malicious peer speaking for someone else.
            await using var transport = new GrpcRaftTransport(
                new GrpcRaftTransportOptions
                {
                    Peers = new Dictionary<NodeId, Uri> { [new NodeId(1)] = new("http://localhost:" + port) },
                    LocalNode = new NodeId(5),
                    Protocol = RaftGrpcProtocol.Http2,
                    Authenticator = new SharedSecretRaftPeerAuthenticator(secret),
                },
                new RejectAllListener());

            // The frame is dropped, not dispatched: no reply ever arrives for it, so waiting without a bound
            // would hang forever. A short deadline is the observable proof that nothing answered.
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => transport.RequestVoteAsync(
                new NodeId(1),
                new VoteRequest(new RaftGroupId(1), 1, new NodeId(99), 0, 0, false),
                deadline.Token).AsTask());

            Assert.False(listener.VoteWasCalled);
        }
        finally
        {
            await app.StopAsync();
        }
    }

    private static int GetFreeTcpPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class RejectAllListener : IRaftProtocolListener
    {
        public ValueTask<AppendEntriesResponse> OnAppendEntriesAsync(
            AppendEntriesRequest request,
            IAsyncEnumerable<RaftLogEntry> entries,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(new AppendEntriesResponse(0, Success: false, 0, 0));

        public ValueTask<InstallSnapshotResponse> OnInstallSnapshotAsync(
            InstallSnapshotRequest request,
            IAsyncEnumerable<ReadOnlyMemory<byte>> body,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(new InstallSnapshotResponse(0, Success: false));

        public ValueTask<ReadIndexResponse> OnReadIndexAsync(
            ReadIndexRequest request,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(new ReadIndexResponse(0, Success: false, 0, 0));

        public ValueTask<VoteResponse> OnRequestVoteAsync(
            VoteRequest request,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(new VoteResponse(0, Granted: false));

        public ValueTask<TimeoutNowResponse> OnTimeoutNowAsync(
            TimeoutNowRequest request,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(new TimeoutNowResponse(0, Accepted: false));
    }

    private sealed class RecordingListener : IRaftProtocolListener
    {
        public bool VoteWasCalled { get; private set; }

        public ValueTask<AppendEntriesResponse> OnAppendEntriesAsync(
            AppendEntriesRequest request,
            IAsyncEnumerable<RaftLogEntry> entries,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(new AppendEntriesResponse(0, Success: false, 0, 0));

        public ValueTask<InstallSnapshotResponse> OnInstallSnapshotAsync(
            InstallSnapshotRequest request,
            IAsyncEnumerable<ReadOnlyMemory<byte>> body,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(new InstallSnapshotResponse(0, Success: false));

        public ValueTask<ReadIndexResponse> OnReadIndexAsync(
            ReadIndexRequest request,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(new ReadIndexResponse(0, Success: false, 0, 0));

        public ValueTask<VoteResponse> OnRequestVoteAsync(
            VoteRequest request,
            CancellationToken cancellationToken = default)
        {
            VoteWasCalled = true;
            return ValueTask.FromResult(new VoteResponse(1, Granted: true));
        }

        public ValueTask<TimeoutNowResponse> OnTimeoutNowAsync(
            TimeoutNowRequest request,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(new TimeoutNowResponse(0, Accepted: false));
    }
}
