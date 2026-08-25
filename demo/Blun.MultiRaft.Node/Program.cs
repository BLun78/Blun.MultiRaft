// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Globalization;
using System.Net;
using Blun.MultiRaft;
using Blun.MultiRaft.Core;
using Blun.MultiRaft.Grpc;
using Blun.MultiRaft.Hosting;
using Blun.MultiRaft.Node;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

// SEC-005: loopback binding is a boundary against the network, not against a browser tab -- any page the
// developer has open can already reach 127.0.0.1. Host filtering closes the DNS-rebinding half of that
// (a domain resolving to 127.0.0.1 bypassing same-origin); the Origin check registered below closes the
// CSRF half for the one state-changing endpoint this app exposes.
builder.Services.AddHostFiltering(options =>
{
    options.AllowedHosts = ["localhost", "127.0.0.1", "[::1]"];
});

// D-009: a bare Parse on a typo'd environment variable threw a FormatException with no indication of which
// setting was at fault. Aborting is still the right call here -- a node with the wrong id must not start --
// but the message should say what to fix instead of leaving a bare stack trace to read.
var self = new NodeId(ParseConfigValue<ulong>(builder.Configuration, "RAFT_NODE_ID", "1", ulong.TryParse));

// Peers come in as RAFT_PEER_<id>=http://host:port. Literal addresses rather than service discovery on
// purpose: every node needs every other node's address, and three mutually referencing resources is a
// dependency cycle Aspire would have to resolve at startup.
Dictionary<NodeId, Uri> peers = [];
foreach (KeyValuePair<string, string?> setting in builder.Configuration.AsEnumerable())
{
    if (setting.Key.StartsWith("RAFT_PEER_", StringComparison.Ordinal)
        && !string.IsNullOrWhiteSpace(setting.Value)
        && ulong.TryParse(setting.Key["RAFT_PEER_".Length..], CultureInfo.InvariantCulture, out ulong id))
    {
        peers[new NodeId(id)] = new Uri(setting.Value);
    }
}

// Two ports, because one will not do. Configuring a single cleartext endpoint as Http1AndHttp2 does NOT get
// you protocol sniffing: without TLS there is no ALPN, and Kestrel silently serves HTTP/1.1 only -- which
// makes every gRPC call fail and, from the outside, looks exactly like a cluster that will not elect.
//
// So: a dedicated port for the Raft protocol, and an HTTP/1.1 port for /status so the scenario stays
// observable with an ordinary HTTP client without ever needing a certificate.
int raftPort = ParseConfigValue<int>(builder.Configuration, "RAFT_PORT", "7101", int.TryParse);
int statusPort = ParseConfigValue<int>(builder.Configuration, "RAFT_STATUS_PORT", "8101", int.TryParse);

// RAFT_PROTOCOL is opt-in and defaults to Http2 (cleartext h2c, no certificate needed -- the local-playground
// default). Http3 has no cleartext mode at all -- QUIC mandates TLS -- so that branch also turns on the
// ASP.NET Core HTTPS development certificate (`dotnet dev-certs https --trust`, once, on this machine) rather
// than plain h2c. AppHost.cs sets this same variable and switches the peer addresses to https:// alongside it;
// the two must agree; a real deployment would use a real certificate here instead of the dev one.
var protocol = Enum.TryParse(builder.Configuration["RAFT_PROTOCOL"], ignoreCase: true, out RaftGrpcProtocol parsedProtocol)
    ? parsedProtocol
    : RaftGrpcProtocol.Http2;
Action<ListenOptions>? configureRaftListen = protocol == RaftGrpcProtocol.Http3
    ? listen => listen.UseHttps()
    : null;

builder.WebHost.ConfigureKestrel(options =>
{
    // ConfigureRaftEndpoint takes one address at a time, so both loopback families get their own call --
    // ListenLocalhost's IPv4-and-IPv6 convenience isn't available on this path.
    options.ConfigureRaftEndpoint(IPAddress.Loopback, raftPort, protocol, configureListen: configureRaftListen);
    options.ConfigureRaftEndpoint(IPAddress.IPv6Loopback, raftPort, protocol, configureListen: configureRaftListen);

    // HTTP/1.1 only, and deliberately so. Offering Http1AndHttp2 on a cleartext port cannot work -- there is
    // no ALPN without TLS to negotiate with -- and Kestrel says so, once per endpoint per start, which is a
    // warning about nothing in a log where warnings should mean something.
    options.ListenLocalhost(statusPort, listen => listen.Protocols = HttpProtocols.Http1);
});

builder.Services.AddGrpc();
builder.Services.AddSingleton<RaftNodeHost>(provider => new RaftNodeHost(
    self,
    peers,
    builder.Configuration["RAFT_DATA_DIR"],
    protocol,
    provider.GetRequiredService<ILoggerFactory>()));

builder.Services.AddSingleton<IRaftProtocolListener>(
    provider => provider.GetRequiredService<RaftNodeHost>());

// Registered separately so the gRPC session can discover it with an `is` check: load reports and
// leadership-target questions travel the same multiplexed stream, but a listener that does not handle them
// still works, it simply has no cluster plane.
builder.Services.AddSingleton<IRaftClusterListener>(
    provider => provider.GetRequiredService<RaftNodeHost>());
builder.Services.AddHostedService(provider => provider.GetRequiredService<RaftNodeHost>());

WebApplication app = builder.Build();

app.UseHostFiltering();

// SEC-005: a same-site or missing Origin is allowed through (curl and other non-browser tooling send none,
// and this is a demo control surface scripts are expected to drive), but a foreign Origin on a
// state-changing request is rejected. Every modern browser attaches Origin to a cross-origin POST, so this
// stops the fetch()/form-based CSRF the audit's proof of concept relies on without breaking scripted use.
app.Use(async (context, next) =>
{
    if (!HttpMethods.IsGet(context.Request.Method) && !HttpMethods.IsHead(context.Request.Method))
    {
        string? origin = context.Request.Headers.Origin;
        if (origin is not null
            && !string.Equals(origin, $"{context.Request.Scheme}://{context.Request.Host}", StringComparison.OrdinalIgnoreCase))
        {
            context.Response.StatusCode = StatusCodes.Status403Forbidden;
            return;
        }
    }

    await next(context).ConfigureAwait(false);
});

app.MapGrpcService<RaftProtocolService>();

// The whole point of the scenario: watch three nodes agree on one leader, over a real network.
app.MapGet("/status", async (RaftNodeHost host) => Results.Json(await host.DescribeAsync()));

// What the cluster leader believes each node is carrying. Empty on the other two, which is the honest
// answer -- reports are pushed to the leader and nobody else has a picture to offer.
app.MapGet("/cluster/load", (RaftNodeHost host) => Results.Json(host.DescribeLoad()));

// Placement, driven from outside. The library never moves a leader on its own; this is the door it moves
// through. `node` may be omitted to let the group's leader pick the best-placed candidate itself.
// No :long route constraint. The cluster group's id is ulong.MaxValue, which does not fit in a long, so the
// constraint quietly refused to match the one group whose leadership is most interesting to move -- a 404
// with an empty body, indistinguishable from a request that did nothing.
app.MapPost(
    "/cluster/groups/{group}/leader",
    async (RaftNodeHost host, ulong group, ulong? node) => Results.Json(await host.RequestLeaderAsync(group, node)));

// Dummy traffic, so the write-ahead log has something in it to look at. Leader only -- an append goes to the
// leader -- and the run happens on the node, one message every `intervalMs`, rather than as one request per
// message from outside.
//
// size/count are rejected outright rather than silently clamped (SEC-006): a size above the WAL's own
// message cap allocated *before* the run's try block used to OOM the process and leave the job latched
// "running" forever, since the abort happened before job.Finish() could run. A 400 makes the mistake visible
// instead of bricking the generator for that group until the process restarts.
app.MapPost(
    "/groups/{group}/messages",
    (RaftNodeHost host, ulong group, int? count, int? intervalMs, int? size) =>
    {
        int resolvedCount = count ?? 100;
        int resolvedSize = size ?? 256;

        if (resolvedCount is < 1 or > TrafficLimits.MaxMessageCount)
        {
            return Results.BadRequest($"count must be between 1 and {TrafficLimits.MaxMessageCount}.");
        }

        if (resolvedSize is < 16 or > SegmentedRaftWalOptions.MaxMessageBytes)
        {
            return Results.BadRequest($"size must be between 16 and {SegmentedRaftWalOptions.MaxMessageBytes} bytes.");
        }

        return Results.Json(host.StartSending(group, resolvedCount, intervalMs ?? 10, resolvedSize));
    });

app.MapDelete(
    "/groups/{group}/messages",
    (RaftNodeHost host, ulong group) => Results.Json(host.StopSending(group)));

// Captures this node's own applied state and compacts the log up to it. Local to this node -- unlike an
// append, a snapshot needs no quorum -- so any answering node can serve this, not just the group's leader.
app.MapPost(
    "/groups/{group}/snapshot",
    async (RaftNodeHost host, ulong group) => Results.Json(await host.TakeSnapshotAsync(group)));

// Membership, driven from outside: a node out of the cluster group's configuration and back in. Only the
// cluster leader can answer these, because only a leader appends.
app.MapDelete(
    "/cluster/nodes/{node}",
    async (RaftNodeHost host, ulong node) => Results.Json(await host.ChangeMembershipAsync(node, remove: true)));

app.MapPost(
    "/cluster/nodes/{node}",
    async (RaftNodeHost host, ulong node) => Results.Json(await host.ChangeMembershipAsync(node, remove: false)));

app.Run();

// D-009: turns a mistyped environment variable into a message naming the setting instead of a bare
// FormatException stack trace. Delegate matches the shape int.TryParse/ulong.TryParse already have.
static T ParseConfigValue<T>(
    IConfiguration configuration, string key, string defaultValue, TryParseHandler<T> tryParse)
    where T : struct
{
    string raw = configuration[key] ?? defaultValue;
    if (!tryParse(raw, CultureInfo.InvariantCulture, out T value))
    {
        throw new InvalidOperationException($"{key} is set to '{raw}', which is not a valid {typeof(T).Name}.");
    }

    return value;
}

delegate bool TryParseHandler<T>(string? s, IFormatProvider? provider, out T result);

// SEC-006: an upper bound on how many messages one traffic-generator run will send. Not tied to any WAL
// limit -- it exists only so a request typo (or a hostile POST, see SEC-005) can't schedule a run that
// pins a group's generator for days instead of the demo's intended "a few thousand at most".
internal static class TrafficLimits
{
    public const int MaxMessageCount = 1_000_000;
}
