// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Aspire.Hosting.ApplicationModel;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Blun.MultiRaft.AppHost;

/// <summary>One Raft node as the control plane knows it: an Aspire resource, a node id, a status port.</summary>
internal sealed record RaftNodeDescriptor(string ResourceName, ulong NodeId, int StatusPort)
{
    /// <summary>
    /// Packs the descriptors into one environment variable for the observer, as
    /// <c>id|resource|statusPort;id|resource|statusPort</c>. Deliberately not JSON: it travels through an
    /// environment variable, where quoting rules differ per shell and per platform.
    /// </summary>
    public static string Format(IEnumerable<RaftNodeDescriptor> nodes)
        => string.Join(
            ';',
            nodes.Select(n => string.Create(
                CultureInfo.InvariantCulture,
                $"{n.NodeId}|{n.ResourceName}|{n.StatusPort}")));
}

internal sealed record ControlPlaneOptions(int Port, IReadOnlyList<RaftNodeDescriptor> Nodes);

/// <summary>
/// A small HTTP surface over the app host's own view of its resources: which are running, how to stop and
/// start them, and what they are writing to their console.
/// </summary>
/// <remarks>
/// <para>
/// It lives inside the app host process because it has to. Starting a resource, stopping it and reading its
/// log stream are things <see cref="ResourceCommandService"/>, <see cref="ResourceNotificationService"/> and
/// <see cref="ResourceLoggerService"/> can do, and those exist only here -- an outside process would have to
/// go hunting for operating-system process handles instead, which is exactly the platform-specific mess this
/// repository avoids elsewhere.
/// </para>
/// <para>
/// It binds to loopback only, and it will only command resources it was told about at construction. It can
/// stop processes; it has no business being reachable from the network, and an open resource-name parameter
/// would let a caller reach past the demo into anything else the app host happens to be running.
/// </para>
/// </remarks>
internal sealed class ControlPlaneService(
    ControlPlaneOptions options,
    ResourceCommandService commands,
    ResourceLoggerService logs,
    ResourceNotificationService notifications,
    ILogger<ControlPlaneService> logger) : IHostedService, IAsyncDisposable
{
    private static readonly byte[] SseDataPrefix = Encoding.UTF8.GetBytes("data: ");
    private static readonly byte[] SseTerminator = Encoding.UTF8.GetBytes("\n\n");

    private WebApplication? _app;

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        WebApplicationBuilder builder = WebApplication.CreateSlimBuilder();

        // The app host's own console is already busy; the control plane's request log would bury it.
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.ListenLocalhost(options.Port, listen => listen.Protocols = HttpProtocols.Http1));

        builder.Services.ConfigureHttpJsonOptions(json =>
            json.SerializerOptions.TypeInfoResolverChain.Insert(0, ControlPlaneJson.Default));

        WebApplication app = builder.Build();

        app.MapGet("/api/resources", () => Results.Json(Describe(), ControlPlaneJson.Default.ResourceStateDtoArray));

        app.MapPost("/api/resources/{name}/{command}", async (string name, string command) =>
        {
            if (Resolve(name) is not { } descriptor)
            {
                return Results.NotFound();
            }

            string? resourceCommand = command.ToLowerInvariant() switch
            {
                "start" => KnownResourceCommands.StartCommand,
                "stop" => KnownResourceCommands.StopCommand,
                "restart" => KnownResourceCommands.RestartCommand,
                _ => null,
            };

            if (resourceCommand is null)
            {
                return Results.BadRequest();
            }

            ControlPlaneLog.CommandRequested(logger, command, descriptor.ResourceName);

            // Deliberately not the request's cancellation token. Stopping a resource is not a query that can
            // be abandoned halfway: a client that closes the connection mid-flight would otherwise leave the
            // command cancelled somewhere in the middle, with the resource in whichever state it got to.
            ExecuteCommandResult result = await commands
                .ExecuteCommandAsync(descriptor.ResourceName, resourceCommand, CancellationToken.None)
                .ConfigureAwait(false);

            return Results.Json(
                new CommandResultDto(result.Success, result.Canceled, result.Message),
                ControlPlaneJson.Default.CommandResultDto);
        });

        app.MapGet("/api/resources/{name}/logs", async (HttpContext context, string name, CancellationToken token) =>
        {
            if (Resolve(name) is not { } descriptor)
            {
                context.Response.StatusCode = StatusCodes.Status404NotFound;
                return;
            }

            // WatchAsync replays the backlog before it starts following, so a client that connects late
            // still gets the history that explains what it is looking at.
            await StreamLogsAsync(context, InstanceIdOf(descriptor), token).ConfigureAwait(false);
        });

        await app.StartAsync(cancellationToken).ConfigureAwait(false);
        _app = app;

        ControlPlaneLog.Started(logger, options.Port, options.Nodes.Count);
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_app is not null)
        {
            await _app.StopAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_app is not null)
        {
            await _app.DisposeAsync().ConfigureAwait(false);
            _app = null;
        }
    }

    /// <summary>Only the resources this control plane was told about can be commanded; see the type remarks.</summary>
    private RaftNodeDescriptor? Resolve(string name)
        => options.Nodes.FirstOrDefault(
            n => string.Equals(n.ResourceName, name, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// The id DCP knows the resource by — the display name plus a unique suffix. Commands accept the plain
    /// name, but the log stream is keyed by the instance that is actually running, and a resource that has
    /// been stopped and started again is a different instance under the same name.
    /// </summary>
    private string InstanceIdOf(RaftNodeDescriptor descriptor)
        => notifications.TryGetCurrentState(descriptor.ResourceName, out ResourceEvent? current)
            ? current?.ResourceId ?? descriptor.ResourceName
            : descriptor.ResourceName;

    private ResourceStateDto[] Describe()
    {
        ResourceStateDto[] states = new ResourceStateDto[options.Nodes.Count];

        for (int i = 0; i < options.Nodes.Count; i++)
        {
            RaftNodeDescriptor node = options.Nodes[i];

            string state = notifications.TryGetCurrentState(node.ResourceName, out ResourceEvent? current)
                ? current?.Snapshot.State?.Text ?? "Unknown"
                : "Unknown";

            states[i] = new ResourceStateDto(
                node.ResourceName,
                node.NodeId,
                node.StatusPort,
                state,
                string.Equals(state, KnownResourceStates.Running, StringComparison.Ordinal));
        }

        return states;
    }

    private async Task StreamLogsAsync(HttpContext context, string resourceName, CancellationToken token)
    {
        BeginEventStream(context);

        // Flushed before the first line exists, so the response actually begins: ASP.NET Core holds the
        // headers back until something is written, and a quiet resource would otherwise look like a hung
        // request rather than an open stream with nothing to say yet.
        await context.Response.Body.FlushAsync(token).ConfigureAwait(false);

        try
        {
            await foreach (IReadOnlyList<LogLine> batch in logs.WatchAsync(resourceName).WithCancellation(token))
            {
                foreach (LogLine line in batch)
                {
                    await WriteEventAsync(
                        context,
                        new LogLineDto(line.LineNumber, line.Content, line.IsErrorMessage),
                        ControlPlaneJson.Default.LogLineDto,
                        token).ConfigureAwait(false);
                }

                await context.Response.Body.FlushAsync(token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // The browser closed the stream. That is how these end.
        }
    }

    private static void BeginEventStream(HttpContext context)
    {
        context.Response.Headers.ContentType = "text/event-stream";
        context.Response.Headers.CacheControl = "no-cache";
        context.Response.Headers["X-Accel-Buffering"] = "no";
    }

    private static async Task WriteEventAsync<T>(
        HttpContext context,
        T payload,
        JsonTypeInfo<T> typeInfo,
        CancellationToken token)
    {
        // Serialised to bytes first and written through Response.Body only: interleaving Response.WriteAsync
        // with a serializer writing to the body stream mixes two writers over one pipe.
        byte[] json = JsonSerializer.SerializeToUtf8Bytes(payload, typeInfo);

        await context.Response.Body.WriteAsync(SseDataPrefix, token).ConfigureAwait(false);
        await context.Response.Body.WriteAsync(json, token).ConfigureAwait(false);
        await context.Response.Body.WriteAsync(SseTerminator, token).ConfigureAwait(false);
    }
}

internal sealed record ResourceStateDto(string Name, ulong Node, int StatusPort, string State, bool Running);

internal sealed record CommandResultDto(bool Success, bool Canceled, string? Message);

internal sealed record LogLineDto(int LineNumber, string Content, bool IsError);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(ResourceStateDto[]))]
[JsonSerializable(typeof(CommandResultDto))]
[JsonSerializable(typeof(LogLineDto))]
internal sealed partial class ControlPlaneJson : JsonSerializerContext;

internal static partial class ControlPlaneLog
{
    [LoggerMessage(
        EventId = 2200,
        Level = LogLevel.Information,
        Message = "Control plane listening on http://127.0.0.1:{Port} for {NodeCount} nodes.")]
    public static partial void Started(ILogger logger, int port, int nodeCount);

    [LoggerMessage(
        EventId = 2201,
        Level = LogLevel.Information,
        Message = "Control plane executing {Command} on {Resource}.")]
    public static partial void CommandRequested(ILogger logger, string command, string resource);
}
