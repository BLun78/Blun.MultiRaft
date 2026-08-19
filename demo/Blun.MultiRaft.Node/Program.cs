// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Globalization;
using System.Net;
using Blun.MultiRaft;
using Blun.MultiRaft.Grpc;
using Blun.MultiRaft.Hosting;
using Blun.MultiRaft.Node;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

var self = new NodeId(ulong.Parse(
    builder.Configuration["RAFT_NODE_ID"] ?? "1",
    CultureInfo.InvariantCulture));

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
int raftPort = int.Parse(builder.Configuration["RAFT_PORT"] ?? "7101", CultureInfo.InvariantCulture);
int statusPort = int.Parse(builder.Configuration["RAFT_STATUS_PORT"] ?? "8101", CultureInfo.InvariantCulture);

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

    options.ListenLocalhost(statusPort, listen => listen.Protocols = HttpProtocols.Http1 | HttpProtocols.Http2);
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
builder.Services.AddHostedService(provider => provider.GetRequiredService<RaftNodeHost>());

WebApplication app = builder.Build();

app.MapGrpcService<RaftProtocolService>();

// The whole point of the scenario: watch three nodes agree on one leader, over a real network.
app.MapGet("/status", (RaftNodeHost host) => Results.Json(host.Describe()));

app.Run();
