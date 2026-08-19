// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

// Three nodes, over a real network, electing real leaders.
//
// Addresses are literal rather than resolved through service discovery, and that is not laziness: every node
// needs every other node's address at startup, so three resources referencing each other is a dependency
// cycle Aspire would have to break before any of them could start. Fixed ports sidestep it entirely.

using System.Globalization;

IDistributedApplicationBuilder builder = DistributedApplication.CreateBuilder(args);

// Http2 is cleartext h2c and needs no certificate, which is what makes this runnable with nothing installed.
// Http3 mandates TLS -- QUIC has no cleartext mode -- so switching this also switches the peer addresses to
// https:// below, and needs `dotnet dev-certs https --trust` once on the machine. The node reads the same
// variable and must agree with what is set here.
const string Protocol = "Http2";
string scheme = string.Equals(Protocol, "Http3", StringComparison.OrdinalIgnoreCase) ? "https" : "http";

(ulong Id, int RaftPort, int StatusPort)[] nodes =
[
    (1, 7101, 8101),
    (2, 7102, 8102),
    (3, 7103, 8103),
];

// Data lives under the AppHost's own directory so a run is self-contained and a wiped folder is a clean
// cluster. Each node gets its own, because a shared one would have three nodes writing the same segments.
string dataRoot = Path.Combine(builder.AppHostDirectory, "data");

foreach ((ulong id, int raftPort, int statusPort) in nodes)
{
    IResourceBuilder<ProjectResource> node = builder
        .AddProject<Projects.Blun_MultiRaft_Node>("raft-node-" + id.ToString(CultureInfo.InvariantCulture))
        .WithEnvironment("RAFT_NODE_ID", id.ToString(CultureInfo.InvariantCulture))
        .WithEnvironment("RAFT_PORT", raftPort.ToString(CultureInfo.InvariantCulture))
        .WithEnvironment("RAFT_STATUS_PORT", statusPort.ToString(CultureInfo.InvariantCulture))
        .WithEnvironment("RAFT_PROTOCOL", Protocol)
        .WithEnvironment(
            "RAFT_DATA_DIR",
            Path.Combine(dataRoot, "node-" + id.ToString(CultureInfo.InvariantCulture)))
        .WithHttpEndpoint(port: statusPort, targetPort: statusPort, name: "status", isProxied: false);

    // Every node learns every node, itself included -- the node process simply never dials its own entry,
    // and including it keeps the membership the same string on all three.
    foreach ((ulong peerId, int peerPort, _) in nodes)
    {
        node = node.WithEnvironment(
            "RAFT_PEER_" + peerId.ToString(CultureInfo.InvariantCulture),
            scheme + "://127.0.0.1:" + peerPort.ToString(CultureInfo.InvariantCulture));
    }
}

builder.Build().Run();
