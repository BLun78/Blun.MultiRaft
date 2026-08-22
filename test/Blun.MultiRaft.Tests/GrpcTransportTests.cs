// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using Blun.MultiRaft.Grpc;
using Blun.MultiRaft.Grpc.Protocol;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Blun.MultiRaft.Tests;

/// <summary>
/// The claim behind raising Kestrel's per-connection stream ceiling and enabling
/// <c>EnableMultipleHttp2Connections</c> on the client: more than the default 100 concurrent things in
/// flight through one Raft session must not stall. Verified against a real Kestrel server over real HTTP/2,
/// not just reasoned about.
/// </summary>
public sealed class GrpcTransportTests : IAsyncLifetime
{
    private WebApplication _app = null!;
    private int _port;

    public async ValueTask InitializeAsync()
    {
        _port = GetFreeTcpPort();

        WebApplicationBuilder builder = WebApplication.CreateBuilder([]);
        builder.Logging.ClearProviders();
        builder.WebHost.UseKestrel(options =>
            options.ConfigureRaftEndpoint(
                System.Net.IPAddress.Loopback,
                _port,
                RaftGrpcProtocol.Http2,
                maxStreamsPerConnection: 4096));
        builder.Services.AddGrpc();
        builder.Services.AddSingleton<IRaftProtocolListener, InstantListener>();

        _app = builder.Build();
        _app.MapGrpcService<RaftProtocolService>();
        await _app.StartAsync();
    }

    private InstantListener Listener => (InstantListener)_app.Services.GetRequiredService<IRaftProtocolListener>();

    public async ValueTask DisposeAsync() => await _app.StopAsync();

    [Fact]
    public async Task TwoHundredConcurrentRequestsOnOneMultiplexedSessionAllSucceed()
    {
        var peers = new Dictionary<NodeId, Uri> { [new NodeId(1)] = new("http://localhost:" + _port) };
        await using var transport = new GrpcRaftTransport(
            new GrpcRaftTransportOptions { Peers = peers, LocalNode = new NodeId(0), Protocol = RaftGrpcProtocol.Http2 },
            new InstantListener());

        // Well past Kestrel's own default of 100 concurrent streams per connection -- this transport holds
        // exactly one stream per peer regardless of group count, so every one of these requests multiplexes
        // as frames on that single stream. If the raised Http2Limits.MaxStreamsPerConnection or the client's
        // EnableMultipleHttp2Connections mattered here, the 101st-and-beyond would stall; if this library's
        // own frame-correlation multiplexing had a hidden ceiling, some subset would time out or throw.
        const int concurrentRequests = 200;
        var inFlight = new Task<AppendEntriesResponse>[concurrentRequests];

        for (int i = 0; i < concurrentRequests; i++)
        {
            var request = new AppendEntriesRequest(new RaftGroupId((ulong)i), 1, new NodeId(0), 0, 0, 0);
            inFlight[i] = transport
                .AppendEntriesAsync(new NodeId(1), request, EmptyEntries(), CancellationToken.None)
                .AsTask();
        }

        AppendEntriesResponse[] responses = await Task.WhenAll(inFlight).WaitAsync(TimeSpan.FromSeconds(15));

        Assert.Equal(concurrentRequests, responses.Length);
        Assert.All(responses, r => Assert.True(r.Success));
    }

    [Fact]
    public async Task OneHundredAndFiftyRealHttp2StreamsOnOneConnectionAllComplete()
    {
        // The test above proves this library's own frame multiplexing has no ceiling, but that layer only
        // ever opens one HTTP/2 stream per peer -- it never approaches Kestrel's own per-connection limit.
        // This test is the literal claim: open more real HTTP/2 streams on one connection than Kestrel's
        // default of 100 allows, and confirm every one of them still completes rather than the 101st stalling
        // behind the ones ahead of it. Each Session() call here is its own stream, sharing one GrpcChannel
        // (and so, absent EnableMultipleHttp2Connections forcing a second connection, one TCP connection).
        var handler = new System.Net.Http.SocketsHttpHandler { EnableMultipleHttp2Connections = true };
        using var httpClient = new System.Net.Http.HttpClient(handler)
        {
            DefaultRequestVersion = new Version(2, 0),
            DefaultVersionPolicy = System.Net.Http.HttpVersionPolicy.RequestVersionExact,
        };
        using GrpcChannel channel = GrpcChannel.ForAddress(
            "http://localhost:" + _port,
            new GrpcChannelOptions { HttpClient = httpClient });
        var client = new RaftProtocol.RaftProtocolClient(channel);

        const int streamCount = 150;
        var calls = new AsyncDuplexStreamingCall<RaftFrame, RaftFrame>[streamCount];
        var completions = new Task[streamCount];

        for (int i = 0; i < streamCount; i++)
        {
            calls[i] = client.Session();
            int index = i;
            completions[i] = RoundTripAsync(calls[index], (ulong)index);
        }

        await Task.WhenAll(completions).WaitAsync(TimeSpan.FromSeconds(15));

        foreach (AsyncDuplexStreamingCall<RaftFrame, RaftFrame> call in calls)
        {
            await call.RequestStream.CompleteAsync();
            call.Dispose();
        }

        static async Task RoundTripAsync(AsyncDuplexStreamingCall<RaftFrame, RaftFrame> call, ulong groupId)
        {
            await call.RequestStream.WriteAsync(new RaftFrame
            {
                CorrelationId = 1,
                GroupId = groupId,
                AppendEntries = new AppendEntries { Term = 1, Leader = 0, PrevLogIndex = 0, PrevLogTerm = 0, LeaderCommit = 0 },
            });

            bool moved = await call.ResponseStream.MoveNext(CancellationToken.None);
            Assert.True(moved, "expected a reply on stream for group " + groupId);
        }
    }

    [Fact]
    public async Task ApplicationTagSurvivesTheWireRoundTrip()
    {
        // RaftFrameCodec is internal, so this exercises the same claim from the outside: a header field this
        // library never reads must still arrive at the follower's IRaftProtocolListener unchanged, or a
        // Blun.MQ priority-index rebuild on a replica would silently disagree with the leader's.
        var peers = new Dictionary<NodeId, Uri> { [new NodeId(1)] = new("http://localhost:" + _port) };
        await using var transport = new GrpcRaftTransport(
            new GrpcRaftTransportOptions { Peers = peers, LocalNode = new NodeId(0), Protocol = RaftGrpcProtocol.Http2 },
            new InstantListener());

        var request = new AppendEntriesRequest(new RaftGroupId(1), 1, new NodeId(0), 0, 0, 0);
        var entry = new RaftLogEntry(1, 1, RaftEntryKind.Command, "hello"u8.ToArray(), applicationTag: 7);

        AppendEntriesResponse response = await transport.AppendEntriesAsync(
            new NodeId(1),
            request,
            SingleEntry(entry),
            CancellationToken.None);

        Assert.True(response.Success);
        Assert.Contains(Listener.ReceivedHeaders, header => header.Index == 1 && header.ApplicationTag == 7);

        static async IAsyncEnumerable<RaftLogEntry> SingleEntry(RaftLogEntry entry)
        {
            await Task.CompletedTask;
            yield return entry;
        }
    }

    [Fact]
    public async Task CompressionFlagSurvivesTheWireRoundTrip()
    {
        // Same shape of claim as the application tag above, but this one corrupts data rather than an index:
        // a follower stores what it is sent verbatim and only expands at apply time, so an entry that arrives
        // marked uncompressed hands a compressed block to the state machine as though it were the command.
        // Term, index and checksum would all still agree -- silent, exactly like the recycled-buffer bug in
        // RaftFrameCodec's remarks, and equally invisible to InMemoryRaftTransport, which copies headers by
        // value and carries the flag no matter what the proto says.
        var peers = new Dictionary<NodeId, Uri> { [new NodeId(1)] = new("http://localhost:" + _port) };
        await using var transport = new GrpcRaftTransport(
            new GrpcRaftTransportOptions { Peers = peers, LocalNode = new NodeId(0), Protocol = RaftGrpcProtocol.Http2 },
            new InstantListener());

        var request = new AppendEntriesRequest(new RaftGroupId(1), 1, new NodeId(0), 0, 0, 0);
        byte[] payload = "compressed-block-stand-in"u8.ToArray();
        var header = new RaftEntryHeader(
            term: 1,
            index: 1,
            RaftEntryKind.Command,
            payload.Length,
            timestampTicks: 1234,
            applicationTag: 0,
            RaftPayloadCompression.Lz4Fast);

        AppendEntriesResponse response = await transport.AppendEntriesAsync(
            new NodeId(1),
            request,
            SingleEntry(new RaftLogEntry(in header, payload)),
            CancellationToken.None);

        Assert.True(response.Success);
        Assert.Contains(
            Listener.ReceivedHeaders,
            h => h.Index == 1 && h.Compression == RaftPayloadCompression.Lz4Fast);

        static async IAsyncEnumerable<RaftLogEntry> SingleEntry(RaftLogEntry entry)
        {
            await Task.CompletedTask;
            yield return entry;
        }
    }

    [Fact]
    public async Task EntriesReadFromTheLogArriveWithTheirOwnPayloads()
    {
        // The log hands out payloads that alias a buffer it recycles on every MoveNextAsync -- that is its
        // documented contract, and the replication loop passes the enumerable straight to the transport. The
        // gRPC session drains a whole round into one frame and serialises it later, on the writer pump, so a
        // transport that wraps that memory instead of copying it ends up sending several entries that all
        // point at the same slot. Headers are copied by value and stay right, so the follower's consistency
        // check passes and the entries land -- with the wrong contents. Distinct payloads of distinct lengths
        // are what makes that visible: a batch that all arrives as the last entry's bytes cannot pass.
        string directory = Path.Combine(Path.GetTempPath(), "blun-grpc-alias-" + Guid.NewGuid().ToString("N"));
        var group = new RaftGroupId(11);

        try
        {
            await using (SegmentedRaftWal wal = await SegmentedRaftWal.OpenAsync(directory))
            {
                RaftLogEntry[] written =
                [
                    .. Enumerable.Range(1, 8).Select(i => new RaftLogEntry(
                        term: 1,
                        index: i,
                        RaftEntryKind.Command,
                        System.Text.Encoding.UTF8.GetBytes(new string((char)('a' + i - 1), i * 3)))),
                ];

                await wal.AppendAsync(written);
                await wal.FlushAsync();

                var peers = new Dictionary<NodeId, Uri> { [new NodeId(1)] = new("http://localhost:" + _port) };
                await using var transport = new GrpcRaftTransport(
                    new GrpcRaftTransportOptions { Peers = peers, LocalNode = new NodeId(0), Protocol = RaftGrpcProtocol.Http2 },
                    new InstantListener());

                AppendEntriesResponse response = await transport.AppendEntriesAsync(
                    new NodeId(1),
                    new AppendEntriesRequest(group, 1, new NodeId(0), 0, 0, 0),
                    wal.ReadFromAsync(1, 8),
                    CancellationToken.None);

                Assert.True(response.Success);

                RaftLogEntry[] received = [.. Listener.ReceivedEntries.OrderBy(e => e.Index)];
                Assert.Equal(8, received.Length);
                for (int i = 0; i < written.Length; i++)
                {
                    Assert.Equal(written[i].Index, received[i].Index);
                    Assert.Equal(written[i].Payload.ToArray(), received[i].Payload.ToArray());
                }
            }
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task ASnapshotBodyLongerThanOneChunkArrivesIntact()
    {
        // Same hazard on the snapshot path, and worse if it slips through: the file-backed store refills one
        // chunk array per iteration, and the session only *queues* each chunk frame, so wrapping that array
        // means every chunk but the last is serialised from a buffer the producer has already overwritten. A
        // snapshot is how a node that fell too far behind is rebuilt from scratch, so corruption here is
        // adopted as state rather than rejected. The body deliberately spans several 64 KB chunks and is
        // position-dependent, so a repeat of one chunk cannot go unnoticed.
        string directory = Path.Combine(Path.GetTempPath(), "blun-grpc-snap-" + Guid.NewGuid().ToString("N"));
        var group = new RaftGroupId(12);

        try
        {
            var store = new FileRaftSnapshotStore(directory);
            byte[] body = new byte[(64 * 1024 * 3) + 977];
            for (int i = 0; i < body.Length; i++)
            {
                body[i] = (byte)(i * 31 % 251);
            }

            await store.WriteAsync(
                group,
                new RaftSnapshotMetadata(9, 2, new byte[] { 1, 2, 3 }),
                OneBlock(body));

            var peers = new Dictionary<NodeId, Uri> { [new NodeId(1)] = new("http://localhost:" + _port) };
            await using var transport = new GrpcRaftTransport(
                new GrpcRaftTransportOptions { Peers = peers, LocalNode = new NodeId(0), Protocol = RaftGrpcProtocol.Http2 },
                new InstantListener());

            InstallSnapshotResponse response = await transport.InstallSnapshotAsync(
                new NodeId(1),
                new InstallSnapshotRequest(group, 2, new NodeId(0), 9, 2, new byte[] { 1, 2, 3 }),
                store.ReadAsync(group),
                CancellationToken.None);

            Assert.True(response.Success);
            Assert.Equal(body, Listener.ReceivedSnapshotBody);

            static async IAsyncEnumerable<ReadOnlyMemory<byte>> OneBlock(byte[] block)
            {
                await Task.CompletedTask;
                yield return block;
            }
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    private static int GetFreeTcpPort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        int port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static async IAsyncEnumerable<RaftLogEntry> EmptyEntries()
    {
        await Task.CompletedTask;
        yield break;
    }

    private sealed class InstantListener : IRaftProtocolListener
    {
        public System.Collections.Concurrent.ConcurrentBag<RaftEntryHeader> ReceivedHeaders { get; } = [];

        /// <summary>Detached copies — the receiving side recycles its buffers just as the sending side does.</summary>
        public System.Collections.Concurrent.ConcurrentBag<RaftLogEntry> ReceivedEntries { get; } = [];

        public byte[] ReceivedSnapshotBody { get; private set; } = [];

        public async ValueTask<AppendEntriesResponse> OnAppendEntriesAsync(
            AppendEntriesRequest request,
            IAsyncEnumerable<RaftLogEntry> entries,
            CancellationToken cancellationToken = default)
        {
            await foreach (RaftLogEntry entry in entries.WithCancellation(cancellationToken))
            {
                ReceivedHeaders.Add(entry.Header);
                ReceivedEntries.Add(entry.ToOwned());
            }

            return new AppendEntriesResponse(1, Success: true, MatchIndex: 0, ConflictIndex: 0);
        }

        public async ValueTask<InstallSnapshotResponse> OnInstallSnapshotAsync(
            InstallSnapshotRequest request,
            IAsyncEnumerable<ReadOnlyMemory<byte>> body,
            CancellationToken cancellationToken = default)
        {
            var assembled = new List<byte>();
            await foreach (ReadOnlyMemory<byte> chunk in body.WithCancellation(cancellationToken))
            {
                assembled.AddRange(chunk.ToArray());
            }

            ReceivedSnapshotBody = [.. assembled];
            return new InstallSnapshotResponse(1, Success: true);
        }

        public ValueTask<ReadIndexResponse> OnReadIndexAsync(
            ReadIndexRequest request,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(new ReadIndexResponse(1, Success: true, ReadIndex: 0, Leader: 0));

        public ValueTask<VoteResponse> OnRequestVoteAsync(
            VoteRequest request,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(new VoteResponse(1, Granted: true));

        public ValueTask<TimeoutNowResponse> OnTimeoutNowAsync(
            TimeoutNowRequest request,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(new TimeoutNowResponse(1, Accepted: true));
    }
}
