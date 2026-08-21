// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

// Five nodes, over a real network, electing real leaders -- plus an observer that watches all five and can
// stop and start them again.
//
// Five rather than three because the quorum is then three, which means the interesting states are reachable
// by hand: kill one node and the cluster shrugs, kill two and it still works, kill the third and writes stop.
// Three nodes only ever demonstrate the first and the last.
//
// Addresses are literal rather than resolved through service discovery, and that is not laziness: every node
// needs every other node's address at startup, so five resources referencing each other is a dependency
// cycle Aspire would have to break before any of them could start. Fixed ports sidestep it entirely.

using System.Globalization;
using Blun.MultiRaft.AppHost;
using Microsoft.Extensions.DependencyInjection;

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
    (4, 7104, 8104),
    (5, 7105, 8105),
];

// The control plane runs inside this process because that is the only place it can run: starting and
// stopping a resource, and reading its console output, are things only the app host knows how to do.
const int ControlPlanePort = 8200;
const int ObserverPort = 8300;
const int ObserverUiPort = 4200;

// Data lives under the AppHost's own directory so a run is self-contained and a wiped folder is a clean
// cluster. Each node gets its own, because a shared one would have five nodes writing the same segments.
string dataRoot = Path.Combine(builder.AppHostDirectory, "data");

List<RaftNodeDescriptor> descriptors = [];

foreach ((ulong id, int raftPort, int statusPort) in nodes)
{
    string resourceName = "raft-node-" + id.ToString(CultureInfo.InvariantCulture);

    // launchProfileName: null suppresses the implicit http/https endpoints Aspire would otherwise derive
    // from launchSettings.json's applicationUrl. Those endpoints would need a port from ASPNETCORE_URLS,
    // which is deliberately blanked below -- without this, DCP fails every node with "information about
    // the port to expose the service is missing; service-producer annotation is invalid".
    IResourceBuilder<ProjectResource> node = builder
        .AddProject<Projects.Blun_MultiRaft_Node>(resourceName, launchProfileName: null)
        .WithEnvironment("RAFT_NODE_ID", id.ToString(CultureInfo.InvariantCulture))
        .WithEnvironment("RAFT_PORT", raftPort.ToString(CultureInfo.InvariantCulture))
        .WithEnvironment("RAFT_STATUS_PORT", statusPort.ToString(CultureInfo.InvariantCulture))
        .WithEnvironment("RAFT_PROTOCOL", Protocol)

        // Aspire injects ASPNETCORE_URLS for a project resource, and this node cannot use it: it binds two
        // endpoints itself, one of them cleartext h2c for the Raft protocol, which no URL string expresses.
        // Blanked rather than ignored, because ignoring it makes Kestrel warn that it is overriding the
        // addresses on every start -- a warning about a decision that was made on purpose.
        .WithEnvironment("ASPNETCORE_URLS", string.Empty)
        .WithEnvironment(
            "RAFT_DATA_DIR",
            Path.Combine(dataRoot, "node-" + id.ToString(CultureInfo.InvariantCulture)))
        .WithHttpEndpoint(port: statusPort, targetPort: statusPort, name: "status", isProxied: false);

    // Every node learns every node, itself included -- the node process simply never dials its own entry,
    // and including it keeps the membership the same string on all five.
    foreach ((ulong peerId, int peerPort, _) in nodes)
    {
        node = node.WithEnvironment(
            "RAFT_PEER_" + peerId.ToString(CultureInfo.InvariantCulture),
            scheme + "://127.0.0.1:" + peerPort.ToString(CultureInfo.InvariantCulture));
    }

    descriptors.Add(new RaftNodeDescriptor(resourceName, id, statusPort));
}

builder.Services.AddSingleton(new ControlPlaneOptions(ControlPlanePort, descriptors));
builder.Services.AddHostedService<ControlPlaneService>();

// The observer is an ordinary client of the nodes' /status endpoints -- it is given no privileged access to
// the cluster, which is the point: everything it shows, anything else could have asked for too. What it
// cannot do on its own is start and stop processes, so for that it goes back through the control plane.
// launchProfileName: null for the same reason as the node resources above -- otherwise the "http" endpoint
// declared below collides with the implicit one Aspire derives from launchSettings.json's applicationUrl.
builder
    .AddProject<Projects.Blun_MultiRaft_Observer>("observer", launchProfileName: null)
    .WithEnvironment("OBSERVER_PORT", ObserverPort.ToString(CultureInfo.InvariantCulture))
    .WithEnvironment(
        "OBSERVER_CONTROL_PLANE",
        "http://127.0.0.1:" + ControlPlanePort.ToString(CultureInfo.InvariantCulture))
    .WithEnvironment("OBSERVER_NODES", RaftNodeDescriptor.Format(descriptors))
    .WithHttpEndpoint(port: ObserverPort, targetPort: ObserverPort, name: "http", isProxied: false);

// The UI is optional on purpose. A machine without Node.js should still get the five nodes and the observer
// API; refusing to start the whole demo over a missing toolchain would be the wrong trade.
string uiDirectory = Path.Combine(builder.AppHostDirectory, "..", "Blun.MultiRaft.Observer.Ui");

if (NodeJsIsAvailable())
{
    builder
        .AddJavaScriptApp("observer-ui", uiDirectory, "start")
        .WithNpm()
        .WithHttpEndpoint(port: ObserverUiPort, targetPort: ObserverUiPort, name: "http", isProxied: false);
}
else
{
    Console.WriteLine(
        "Node.js was not found on PATH -- skipping the observer UI. The observer API is still on "
        + "http://127.0.0.1:" + ObserverPort.ToString(CultureInfo.InvariantCulture) + ".");
}

builder.Build().Run();

// Looks for the executable rather than shelling out to it: spawning a process during app-host construction
// would block startup on Windows for as long as the shim takes to answer, and PATH is the same question.
static bool NodeJsIsAvailable()
{
    string[] names = OperatingSystem.IsWindows() ? ["node.exe", "node.cmd"] : ["node"];
    string path = Environment.GetEnvironmentVariable("PATH") ?? string.Empty;

    foreach (string directory in path.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
    {
        foreach (string name in names)
        {
            try
            {
                if (File.Exists(Path.Combine(directory.Trim('"'), name)))
                {
                    return true;
                }
            }
            catch (ArgumentException)
            {
                // A malformed PATH entry is not a reason to fail; skip it and keep looking.
            }
        }
    }

    return false;
}
