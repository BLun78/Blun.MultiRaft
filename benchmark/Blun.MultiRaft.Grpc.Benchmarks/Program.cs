// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

// Does multiplexing actually pay?
//
// The gRPC transport puts ONE bidirectional stream between each pair of nodes and carries every group over
// it, with the group id in the frame. The argument for that over a stream per group was that thousands of
// HTTP/2 streams each carry their own flow-control window and HPACK state. This measures that argument
// against the alternative: the same gRPC stack, one dedicated stream per group.
//
// What is measured is transport overhead and nothing else. The listener answers instantly from memory --
// no log, no fsync, no consensus -- and the payload is an empty heartbeat-shaped AppendEntries. Connection
// setup is timed separately and excluded from the steady-state figure, because for the per-group arm it is
// a real and separate cost that would otherwise be blamed on the wrong thing.
//
// Result, on localhost: it does NOT confirm the latency claim. See README.md.

using System.Diagnostics;
using System.Globalization;
using System.Net;
using Blun.MultiRaft;
using Blun.MultiRaft.Grpc;
using Blun.MultiRaft.Grpc.Protocol;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Server.Kestrel.Core;

int[] groupCounts = args.Length > 0
    ? [.. args.Select(a => int.Parse(a, CultureInfo.InvariantCulture))]
    : [10, 100, 500, 2000];

const int Port = 7399;
var address = new Uri("http://127.0.0.1:" + Port.ToString(CultureInfo.InvariantCulture));
var self = new NodeId(1);
var peer = new NodeId(2);

WebApplicationBuilder builder = WebApplication.CreateBuilder();
builder.Logging.ClearProviders();
builder.WebHost.ConfigureKestrel(options =>
{
    // The ceiling matters for the per-group arm specifically: it opens one stream per group, and the default
    // limit of 100 would turn this into a measurement of stream queueing rather than of framing overhead.
    options.ConfigureRaftEndpoint(IPAddress.Loopback, Port, maxStreamsPerConnection: 100_000);
});

builder.Services.AddGrpc();
builder.Services.AddSingleton<IRaftProtocolListener, InstantListener>();

WebApplication app = builder.Build();
app.MapGrpcService<RaftProtocolService>();
await app.StartAsync().ConfigureAwait(false);

Console.WriteLine("Groups | Multiplexed (steady) | Per-group setup | Per-group (steady) | Ratio");
Console.WriteLine("-------+----------------------+-----------------+--------------------+------");

foreach (int groups in groupCounts)
{
    // One warm-up round per arm, discarded. The first call on a fresh channel pays for TLS-less handshake,
    // HTTP/2 preface and JIT, none of which is what this is about.
    await MeasureMultiplexedAsync(groups, warmup: true).ConfigureAwait(false);
    double multiplexed = await MeasureMultiplexedAsync(groups, warmup: false).ConfigureAwait(false);

    (double setup, double steady) = await MeasurePerGroupAsync(groups).ConfigureAwait(false);

    Console.WriteLine(
        "{0,6} | {1,20} | {2,15} | {3,18} | {4,5}",
        groups,
        multiplexed.ToString("F1", CultureInfo.InvariantCulture) + " ms",
        setup.ToString("F1", CultureInfo.InvariantCulture) + " ms",
        steady.ToString("F1", CultureInfo.InvariantCulture) + " ms",
        (steady / multiplexed).ToString("F2", CultureInfo.InvariantCulture) + "x");
}

Console.WriteLine();
Console.WriteLine("Ratio is per-group-steady over multiplexed-steady; above 1 favours multiplexing.");

await app.StopAsync().ConfigureAwait(false);
return;

// One shared session for every group -- what the library actually does.
async Task<double> MeasureMultiplexedAsync(int groups, bool warmup)
{
    await using var transport = new GrpcRaftTransport(
        new GrpcRaftTransportOptions { Peers = new Dictionary<NodeId, Uri> { [peer] = address }, LocalNode = new NodeId(1) },
        new InstantListener());

    // Excluded from the figure deliberately: one connection is opened per node pair regardless of group
    // count, so charging it to a per-group measurement would flatter multiplexing for the wrong reason.
    await RoundTripAsync(transport, new RaftGroupId(0)).ConfigureAwait(false);

    if (warmup)
    {
        return 0;
    }

    long start = Stopwatch.GetTimestamp();
    Task[] calls = new Task[groups];
    for (int i = 0; i < groups; i++)
    {
        calls[i] = RoundTripAsync(transport, new RaftGroupId((ulong)i + 1));
    }

    await Task.WhenAll(calls).ConfigureAwait(false);
    return Stopwatch.GetElapsedTime(start).TotalMilliseconds;
}

// One dedicated channel and stream per group -- the alternative design.
async Task<(double Setup, double Steady)> MeasurePerGroupAsync(int groups)
{
    var channels = new GrpcChannel[groups];
    var calls = new AsyncDuplexStreamingCall<RaftFrame, RaftFrame>[groups];

    long setupStart = Stopwatch.GetTimestamp();
    for (int i = 0; i < groups; i++)
    {
        channels[i] = GrpcChannel.ForAddress(address);
        calls[i] = new RaftProtocol.RaftProtocolClient(channels[i]).Session();

        // Connecting is not enough to call it established: the first frame is what actually opens the
        // stream, so the setup figure has to include one.
        await calls[i].RequestStream.WriteAsync(Heartbeat((ulong)i + 1, correlation: 0)).ConfigureAwait(false);
        await calls[i].ResponseStream.MoveNext(CancellationToken.None).ConfigureAwait(false);
    }

    double setup = Stopwatch.GetElapsedTime(setupStart).TotalMilliseconds;

    long steadyStart = Stopwatch.GetTimestamp();
    Task[] round = new Task[groups];
    for (int i = 0; i < groups; i++)
    {
        round[i] = RoundTripOnStreamAsync(calls[i], (ulong)i + 1);
    }

    await Task.WhenAll(round).ConfigureAwait(false);
    double steady = Stopwatch.GetElapsedTime(steadyStart).TotalMilliseconds;

    for (int i = 0; i < groups; i++)
    {
        calls[i].Dispose();
        channels[i].Dispose();
    }

    return (setup, steady);
}

static async Task RoundTripAsync(IRaftProtocolTransport transport, RaftGroupId group)
{
    var request = new AppendEntriesRequest(group, 1, new NodeId(1), 0, 0, 0);
    await transport.AppendEntriesAsync(new NodeId(2), request, Empty()).ConfigureAwait(false);
}

static async Task RoundTripOnStreamAsync(AsyncDuplexStreamingCall<RaftFrame, RaftFrame> call, ulong group)
{
    await call.RequestStream.WriteAsync(Heartbeat(group, correlation: 1)).ConfigureAwait(false);
    await call.ResponseStream.MoveNext(CancellationToken.None).ConfigureAwait(false);
}

static RaftFrame Heartbeat(ulong group, ulong correlation)
    => new()
    {
        CorrelationId = correlation,
        GroupId = group,
        AppendEntries = new AppendEntries { Term = 1, Leader = 1, PrevLogIndex = 0, PrevLogTerm = 0, LeaderCommit = 0 },
    };

static async IAsyncEnumerable<RaftLogEntry> Empty()
{
    await ValueTask.CompletedTask.ConfigureAwait(false);
    yield break;
}

/// <summary>
/// Answers every request at once, from nothing. Any real work here would be measured as transport cost.
/// </summary>
internal sealed class InstantListener : IRaftProtocolListener
{
    public ValueTask<AppendEntriesResponse> OnAppendEntriesAsync(
        AppendEntriesRequest request,
        IAsyncEnumerable<RaftLogEntry> entries,
        CancellationToken cancellationToken = default)
        => ValueTask.FromResult(new AppendEntriesResponse(1, Success: true, 0, 0));

    public ValueTask<InstallSnapshotResponse> OnInstallSnapshotAsync(
        InstallSnapshotRequest request,
        IAsyncEnumerable<ReadOnlyMemory<byte>> body,
        CancellationToken cancellationToken = default)
        => ValueTask.FromResult(new InstallSnapshotResponse(1, Success: true));

    public ValueTask<ReadIndexResponse> OnReadIndexAsync(
        ReadIndexRequest request,
        CancellationToken cancellationToken = default)
        => ValueTask.FromResult(new ReadIndexResponse(1, Success: true, 0, 1));

    public ValueTask<TimeoutNowResponse> OnTimeoutNowAsync(
        TimeoutNowRequest request,
        CancellationToken cancellationToken = default)
        => ValueTask.FromResult(new TimeoutNowResponse(1, Accepted: true));

    public ValueTask<VoteResponse> OnRequestVoteAsync(
        VoteRequest request,
        CancellationToken cancellationToken = default)
        => ValueTask.FromResult(new VoteResponse(1, Granted: true));
}
