// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Globalization;
using Blun.MultiRaft.Observer;
using Microsoft.AspNetCore.Server.Kestrel.Core;

WebApplicationBuilder builder = WebApplication.CreateSlimBuilder(args);

int port = int.Parse(builder.Configuration["OBSERVER_PORT"] ?? "8300", CultureInfo.InvariantCulture);

var options = new ObserverOptions(
    port,
    new Uri(builder.Configuration["OBSERVER_CONTROL_PLANE"] ?? "http://127.0.0.1:8200"),
    ObservedNode.Parse(builder.Configuration["OBSERVER_NODES"]),
    // Half a second is fast enough to watch an election happen and slow enough that five nodes answering it
    // costs nothing. The timeout is deliberately shorter than the interval: a poll that has not answered by
    // the time the next one is due has told us what we needed to know.
    PollInterval: TimeSpan.FromMilliseconds(500),
    PollTimeout: TimeSpan.FromMilliseconds(300));

builder.Services.AddSingleton(options);
builder.Services.AddHttpClient("node");
builder.Services.AddHttpClient("stream", client => client.Timeout = Timeout.InfiniteTimeSpan);
builder.Services.AddSingleton<ClusterWatcher>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<ClusterWatcher>());
builder.Services.ConfigureHttpJsonOptions(json =>
    json.SerializerOptions.TypeInfoResolverChain.Insert(0, ObserverJson.Default));

builder.WebHost.ConfigureKestrel(kestrel =>
    kestrel.ListenLocalhost(options.Port, listen => listen.Protocols = HttpProtocols.Http1));

WebApplication app = builder.Build();

// What every node believes, right now.
app.MapGet("/api/cluster", (ClusterWatcher watcher)
    => Results.Json(watcher.Current, ObserverJson.Default.ClusterSnapshot));

// The same thing, as it changes. One event per poll, so a browser that connects mid-election sees the
// disagreement resolve rather than only the tidy state afterwards.
app.MapGet("/api/cluster/stream", async (HttpContext context, ClusterWatcher watcher, CancellationToken token) =>
{
    Sse.Begin(context);

    try
    {
        await foreach (ClusterSnapshot snapshot in watcher.WatchAsync(token))
        {
            await Sse.WriteAsync(context, snapshot, ObserverJson.Default.ClusterSnapshot, token)
                .ConfigureAwait(false);
        }
    }
    catch (OperationCanceledException)
    {
        // The browser went away.
    }
});

// Everything below is the app host's control plane, reached through the observer so the UI has one origin
// to talk to. The observer adds nothing here; it forwards.
// D-011: the app host is the one process the observer cannot demand anything of -- it may still be starting
// when the UI makes its first call, and ClusterWatcher.PollResourcesAsync already treats that as ordinary.
// These two proxying endpoints didn't, so an early request surfaced as a bare 500 and an empty page. 503
// plus a reason lets the UI say "the app host isn't answering yet" instead of showing nothing.
app.MapGet("/api/resources", async (
    IHttpClientFactory clients,
    ObserverOptions settings,
    ILogger<Program> logger,
    CancellationToken token) =>
{
    try
    {
        ResourceState[] states = await clients
            .CreateClient("node")
            .GetFromJsonAsync(new Uri(settings.ControlPlane, "/api/resources"), ObserverJson.Default.ResourceStateArray, token)
            .ConfigureAwait(false) ?? [];

        return Results.Json(states, ObserverJson.Default.ResourceStateArray);
    }
    catch (HttpRequestException ex)
    {
        ObserverLog.ControlPlaneUnreachable(logger, ex.Message);
        return Results.Problem("The app host is not answering yet.", statusCode: StatusCodes.Status503ServiceUnavailable);
    }
});

app.MapPost("/api/resources/{name}/{command}", async (
    string name,
    string command,
    IHttpClientFactory clients,
    ObserverOptions settings,
    ILogger<Program> logger,
    CancellationToken token) =>
{
    // SEC-009: name and command are route values, already URL-decoded by the time they get here, and were
    // being interpolated straight into the proxied URI unencoded and unchecked -- an encoded '/' or a '?'/'#'
    // in either could redirect the request to a different control-plane path or inject query/fragment data.
    // The control plane's own Resolve(name) and command switch already reject anything unexpected, but that
    // is a coupling nothing here made visible; checking both at the door the traffic actually enters through
    // makes the assumption obvious and removes any dependence on the far side getting it right.
    if (settings.Nodes.All(n => n.ResourceName != name))
    {
        return Results.NotFound();
    }

    if (command is not ("start" or "stop" or "restart"))
    {
        return Results.BadRequest($"Unknown command '{command}'.");
    }

    try
    {
        using HttpResponseMessage response = await clients
            .CreateClient("node")
            .PostAsync(
                new Uri(settings.ControlPlane, $"/api/resources/{Uri.EscapeDataString(name)}/{Uri.EscapeDataString(command)}"),
                null,
                token)
            .ConfigureAwait(false);

        if (!response.IsSuccessStatusCode)
        {
            return Results.StatusCode((int)response.StatusCode);
        }

        CommandResult? result = await response.Content
            .ReadFromJsonAsync(ObserverJson.Default.CommandResult, token)
            .ConfigureAwait(false);

        return Results.Json(result ?? new CommandResult(false, false, "no answer"), ObserverJson.Default.CommandResult);
    }
    catch (HttpRequestException ex)
    {
        ObserverLog.ControlPlaneUnreachable(logger, ex.Message);
        return Results.Problem("The app host is not answering yet.", statusCode: StatusCodes.Status503ServiceUnavailable);
    }
});

// Every node's console output on one connection. One rather than five is not a tidiness preference: a
// browser allows six concurrent connections per origin over HTTP/1.1, and five log streams plus the cluster
// stream is exactly six -- which leaves nothing for the buttons, and they hang forever. See LogStream.
app.MapGet("/api/logs/stream", async (
    HttpContext context,
    ObserverOptions settings,
    IHttpClientFactory clients,
    ILoggerFactory loggers,
    CancellationToken token) =>
    await LogStream
        .MergeAsync(context, settings, clients, loggers.CreateLogger("Blun.MultiRaft.Observer.LogStream"), token)
        .ConfigureAwait(false));

// Membership, driven from the UI: a node out of the cluster group's configuration and back in. Unlike
// placement, this one has to reach the cluster leader itself -- a membership change is an append.
app.MapPost("/api/cluster/nodes/{node}", (ulong node, ClusterWatcher watcher, IHttpClientFactory clients, CancellationToken token)
    => Membership.ChangeAsync(node, HttpMethod.Post, watcher, clients, token));

app.MapDelete("/api/cluster/nodes/{node}", (ulong node, ClusterWatcher watcher, IHttpClientFactory clients, CancellationToken token)
    => Membership.ChangeAsync(node, HttpMethod.Delete, watcher, clients, token));

// Dummy traffic into a group's log, driven from the UI. See Traffic for why it goes to the group's leader.
app.MapPost("/api/groups/{group}/messages", (
    string group,
    int? count,
    int? intervalMs,
    int? size,
    ClusterWatcher watcher,
    IHttpClientFactory clients,
    CancellationToken token)
    => Traffic.StartAsync(group, count ?? 100, intervalMs ?? 10, size ?? 256, watcher, clients, token));

app.MapDelete("/api/groups/{group}/messages", (
    string group,
    ClusterWatcher watcher,
    IHttpClientFactory clients,
    CancellationToken token)
    => Traffic.StopAsync(group, watcher, clients, token));

// Placement, driven from the UI. Any answering node may be asked -- the request is routed to the group's
// leader wherever it is -- so the observer simply picks one that is talking to it.
app.MapPost("/api/groups/{group}/leader", async (
    ulong group,
    ulong? node,
    ClusterWatcher watcher,
    IHttpClientFactory clients,
    CancellationToken token) =>
{
    NodeView? target = watcher.Current.Nodes.FirstOrDefault(n => n.Online);

    if (target is null)
    {
        return Results.Json(
            new CommandResult(false, false, "no node is answering"),
            ObserverJson.Default.CommandResult,
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }

    string query = node is { } preferred
        ? string.Create(CultureInfo.InvariantCulture, $"?node={preferred}")
        : string.Empty;

    using HttpResponseMessage response = await clients
        .CreateClient("node")
        .PostAsync(
            new Uri(string.Create(
                CultureInfo.InvariantCulture,
                $"http://127.0.0.1:{target.StatusPort}/cluster/groups/{group}/leader{query}")),
            null,
            token)
        .ConfigureAwait(false);

    // Handed back exactly as the node worded it. Lagging and NotResponding are the interesting answers, and
    // rewriting them into something friendlier would hide the distinction the cluster plane draws.
    string body = await response.Content.ReadAsStringAsync(token).ConfigureAwait(false);

    return Results.Content(body, "application/json", statusCode: (int)response.StatusCode);
});

app.Run();
