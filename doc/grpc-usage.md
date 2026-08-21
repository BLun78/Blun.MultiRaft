# Using `Blun.MultiRaft.Grpc`

The gRPC transport: one multiplexed bidirectional stream per node pair, carrying every Raft group over it.
It implements `IRaftProtocolTransport`/`IRaftProtocolListener` from the core library — see
[raft-usage.md](raft-usage.md) for what those interfaces are for. This package only concerns itself with
getting frames between two nodes; it has no idea what a term or a log entry means.

```
dotnet add package Blun.MultiRaft.Grpc
```

Everything here runs on ASP.NET Core (server) and `Grpc.Net.Client` (client, which is really the same
`GrpcRaftTransport` object doing both — see below).

## The pattern in one picture

Each node in the cluster is *both* a gRPC server (so peers can reach it) and a gRPC client (so it can reach
peers). There's exactly one `GrpcRaftTransport` per node, and it plays both roles: the server side is wired
into ASP.NET Core's request pipeline, and the very same transport instance is what `MultiRaftHost` calls
outbound methods on.

```csharp
var builder = WebApplication.CreateBuilder(args);

// Two ports, not one -- see "The two-port gotcha" below before you reach for a single endpoint.
builder.WebHost.ConfigureKestrel(options =>
    options.ListenLocalhost(7101, listen => listen.Protocols = HttpProtocols.Http2));

builder.Services.AddGrpc();

// The listener is whatever answers "OnAppendEntriesAsync" etc. -- in practice, MultiRaftHost.
// Registered before the transport, because the transport needs it at construction time.
builder.Services.AddSingleton<IRaftProtocolListener>(/* your MultiRaftHost instance, see raft-usage.md */);

builder.Services.AddSingleton(provider => new GrpcRaftTransport(
    new GrpcRaftTransportOptions
    {
        Peers = new Dictionary<NodeId, Uri>
        {
            [new NodeId(2)] = new Uri("http://node2:7101"),
            [new NodeId(3)] = new Uri("http://node3:7101"),
        },
    },
    provider.GetRequiredService<IRaftProtocolListener>()));

WebApplication app = builder.Build();
app.MapGrpcService<RaftProtocolService>();
app.Run();
```

`RaftProtocolService` is the server side — it accepts an inbound session and hands its frames to whatever
`IRaftProtocolListener` you registered. `GrpcRaftTransport` is the client side — it dials `Peers` lazily
(the first call to a given peer opens the connection; nothing is dialled up front) and reuses that one
session for every group and every subsequent call to that peer.

There's also an `AddRaftProtocol`/`MapRaftProtocol` extension pair (`RaftProtocolServiceExtensions`) meant to
collapse the server-side registration into one call:

```csharp
builder.Services.AddRaftProtocol(provider => /* your listener */);
// ...
app.MapRaftProtocol();
```

The worked example above wires `AddGrpc()`/`MapGrpcService` directly instead, because that's the exact path
exercised end-to-end by this repository's own demo and its Aspire three-node scenario — if you hit something
the extension methods don't cover, falling back to the two calls they wrap is always available.

## The two-port gotcha

This will cost you an afternoon if you don't know about it going in: **a single Kestrel endpoint configured
for `HttpProtocols.Http1AndHttp2` does not multiplex based on the request.** Without TLS there is no ALPN, so
Kestrel silently serves that endpoint as HTTP/1.1 *only* — every gRPC call to it fails, and from the outside
that looks exactly like a cluster that will never elect a leader, not like a configuration mistake.

If you want an HTTP/1.1 endpoint on the same node (a health check, an admin API, whatever), give it a
*separate port*, configured for `Http1` alone:

```csharp
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenLocalhost(raftPort, listen => listen.Protocols = HttpProtocols.Http2);
    options.ListenLocalhost(statusPort, listen => listen.Protocols = HttpProtocols.Http1);
});
```

This is exactly what `demo/Blun.MultiRaft.Node/Program.cs` does, and the comment there is the same warning
in more detail if you want the full story.

No TLS in the example above — that's deliberate for a local/dev cluster on a trusted network. A real
deployment terminates mutual TLS on the Raft port; this library doesn't do anything auth-related itself, it
just carries frames.

## HTTP/2 vs HTTP/3, and the 101st stream

`RaftGrpcProtocol` picks which HTTP version the session runs over, on both ends of a node pair — there's no
negotiation, because a Raft session is long-lived and internal to the cluster, not a public endpoint that has
to accommodate whatever a caller happens to speak:

```csharp
public enum RaftGrpcProtocol
{
    Http2 = 0, // cleartext (h2c) unless the peer address is https:// -- the default
    Http3 = 1, // QUIC. TLS is mandatory -- there is no cleartext HTTP/3.
}
```

Set it on the client side via `GrpcRaftTransportOptions.Protocol`, and on the server side via
`RaftKestrelExtensions.ConfigureRaftEndpoint` in place of a raw `options.Listen(...)`/`ListenLocalhost(...)`
call:

```csharp
builder.WebHost.ConfigureKestrel(options =>
    options.ConfigureRaftEndpoint(
        IPAddress.Loopback,
        raftPort,
        RaftGrpcProtocol.Http2,
        maxStreamsPerConnection: RaftKestrelExtensions.DefaultMaxStreamsPerConnection)); // 4096

builder.Services.AddSingleton(provider => new GrpcRaftTransport(
    new GrpcRaftTransportOptions
    {
        Peers = peers,
        Protocol = RaftGrpcProtocol.Http2, // must match what the peer's ConfigureRaftEndpoint listens with
    },
    provider.GetRequiredService<IRaftProtocolListener>()));
```

`ConfigureRaftEndpoint` does two things beyond picking the protocol:

- Raises Kestrel's `Http2Limits.MaxStreamsPerConnection` from its default of 100 to 4096 (override via the
  `maxStreamsPerConnection` parameter). In this transport's normal usage that default is rarely the actual
  constraint — every group multiplexes onto the one stream this transport holds per peer, as frames, not as
  separate HTTP/2 streams — but "the 101st concurrent thing still works" should hold regardless of how many
  streams a given deployment ends up opening, not just under this library's own current usage pattern.
- On the client, `GrpcRaftTransport` sets `SocketsHttpHandler.EnableMultipleHttp2Connections` and
  `EnableMultipleHttp3Connections` (both default to `true`, both configurable on
  `GrpcRaftTransportOptions`): if a connection to a peer ever does hit that peer's advertised stream limit,
  the handler opens a second physical connection instead of queuing new streams behind the ones already in
  flight. This is the client-side half of the same guarantee.

`maxStreamsPerConnection` reaches **HTTP/2 only.** HTTP/3's equivalent ceiling is not a Kestrel limit at all
— it lives on the QUIC transport as `QuicTransportOptions.MaxBidirectionalStreamCount`, bound through DI,
which nothing on `KestrelServerOptions` can reach. An endpoint serving `RaftGrpcProtocol.Http3` therefore
keeps QUIC's own default of 100 concurrent request streams unless the host also calls:

```csharp
builder.Services.ConfigureRaftQuicTransport();   // same 4096 default as the HTTP/2 side
```

`QuicTransportOptions` is still a .NET preview API, so `ConfigureRaftQuicTransport` carries
`[RequiresPreviewFeatures]` rather than opting every consumer of this library in — a host that wants HTTP/3
sets `<EnablePreviewFeatures>true</EnablePreviewFeatures>` and accepts that the shape of those options may
change; a host on HTTP/2 never sees the requirement.

`ConfigureRaftEndpoint` deliberately does **not** call `UseHttps(...)` itself — that means a using directive
on `Microsoft.AspNetCore.Server.Kestrel.Https` and a certificate that's the host's responsibility, not this
library's. Layer it on through the `configureListen` callback:

```csharp
options.ConfigureRaftEndpoint(
    IPAddress.Loopback,
    raftPort,
    RaftGrpcProtocol.Http3,
    configureListen: listen => listen.UseHttps(myCertificate));
```

Skipping that step for `RaftGrpcProtocol.Http3` isn't a configuration error you'll see at startup — QUIC has
no cleartext mode at all, so the endpoint simply accepts no connections, and the cluster looks like it can
never reach a peer rather than like a missing certificate.

Verified against a real Kestrel server over real HTTP/2 in `test/Blun.MultiRaft.Tests/GrpcTransportTests.cs`:
one test drives 200 concurrent Raft RPCs through this transport's own one-stream-per-peer session, another
opens 150 real, separate HTTP/2 streams on one connection — both past Kestrel's default ceiling of 100 — and
every one of them completes.

## Calling outbound methods directly

Normally `RaftGroupInstance`/`MultiRaftHost` are the only callers of `IRaftProtocolTransport` — but the shape
is plain enough to use standalone if you're testing connectivity or writing a diagnostic:

```csharp
var request = new AppendEntriesRequest(
    Group: new RaftGroupId(1),
    Term: 1,
    Leader: selfNodeId,
    PrevLogIndex: 0,
    PrevLogTerm: 0,
    LeaderCommit: 0);

AppendEntriesResponse response = await transport.AppendEntriesAsync(
    target: new NodeId(2),
    request,
    entries: EmptyEntries(),   // an IAsyncEnumerable<RaftLogEntry>; empty is a plain heartbeat
    cancellationToken);

static async IAsyncEnumerable<RaftLogEntry> EmptyEntries()
{
    await Task.CompletedTask;
    yield break;
}
```

An unreachable or misbehaving peer surfaces as `IOException` — not a transport-specific exception type. That's
deliberate: `RaftGroupInstance` treats an unreachable peer as an ordinary, expected condition (the quorum rule
is what decides whether it matters), and every transport implementation in this library — this one and the
in-process one used by tests — raises the same exception type for it.

## Options

```csharp
new GrpcRaftTransportOptions
{
    Peers = peers,                                   // required: NodeId -> base address
    RequestTimeout = TimeSpan.FromSeconds(5),         // per-call deadline for AppendEntries/RequestVote/ReadIndex/TimeoutNow
    Protocol = RaftGrpcProtocol.Http2,                // Http2 (default, cleartext) or Http3 (TLS mandatory)
};
```

`InstallSnapshotAsync` deliberately ignores `RequestTimeout` — a snapshot is as large as the state machine's
state is, and cutting it off at a fixed deadline would guarantee the receiving replica never catches up. Let
it run.

## Lifetime

`GrpcRaftTransport` implements `IAsyncDisposable`. Disposing it tears down every peer connection it opened.
If a peer connection fails mid-use, the transport drops it and the *next* call to that peer dials fresh —
you don't need to detect or recover from a dead connection yourself.

## What's actually on the wire

One protobuf message type, `RaftFrame`, carrying a `oneof` of every RPC this library has (`AppendEntries`,
`Vote`, `InstallSnapshot`/`SnapshotChunk`, `ReadIndex`, `TimeoutNow`, and their replies), tagged with a
`group_id` and a `correlation_id` that pairs a reply to its request. If you're debugging at the wire level or
writing an interoperating client in another language, `src/Blun.MultiRaft.Grpc/Protos/raft.proto` is the
actual, current contract — read it directly rather than this document, which describes the C# surface over
it.
