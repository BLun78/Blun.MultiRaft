// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Text.Json;
using System.Threading.Channels;

namespace Blun.MultiRaft.Observer;

/// <summary>
/// Every node's console output on one connection.
/// </summary>
/// <remarks>
/// <para>
/// One stream rather than one per node, and not for tidiness: a browser allows six concurrent connections
/// per origin over HTTP/1.1, and five log streams plus the cluster stream is exactly six. The seventh request
/// — every button in the UI — then queues behind connections that never end, and the page looks broken while
/// the server sits idle. It cost an afternoon to see, because each piece works perfectly on its own.
/// </para>
/// <para>
/// Which is the same trade the gRPC transport makes for the same reason, one layer down: multiplex by hand
/// and carry the sender's id in the frame.
/// </para>
/// </remarks>
internal static class LogStream
{
    /// <summary>Dropped oldest when a slow reader falls behind: recent output matters, a backlog does not.</summary>
    private const int Capacity = 4096;

    public static async Task MergeAsync(
        HttpContext context,
        ObserverOptions options,
        IHttpClientFactory clients,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        Sse.Begin(context);
        await context.Response.Body.FlushAsync(cancellationToken).ConfigureAwait(false);

        Channel<LogEvent> channel = Channel.CreateBounded<LogEvent>(
            new BoundedChannelOptions(Capacity) { FullMode = BoundedChannelFullMode.DropOldest, SingleReader = true });

        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        Task[] pumps =
        [
            .. options.Nodes.Select(node => Task.Run(
                () => PumpAsync(node, channel.Writer, options, clients, logger, stopping.Token),
                CancellationToken.None)),
        ];

        try
        {
            await foreach (LogEvent line in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                await Sse.WriteAsync(context, line, ObserverJson.Default.LogEvent, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // The browser closed the page.
        }
        finally
        {
            await stopping.CancelAsync().ConfigureAwait(false);

            try
            {
                await Task.WhenAll(pumps).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected while shutting the upstream connections down.
            }
        }
    }

    /// <summary>
    /// Follows one node's log on the control plane and forwards what it says. Reconnects on its own: the
    /// stream ends when the resource stops, and the resource stopping is a thing this demo does on purpose.
    /// </summary>
    private static async Task PumpAsync(
        ObservedNode node,
        ChannelWriter<LogEvent> writer,
        ObserverOptions options,
        IHttpClientFactory clients,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var address = new Uri(options.ControlPlane, $"/api/resources/{node.ResourceName}/logs");

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                HttpClient client = clients.CreateClient("stream");

                using HttpResponseMessage response = await client
                    .GetAsync(address, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
                    .ConfigureAwait(false);

                response.EnsureSuccessStatusCode();

                // The upstream replays its backlog on every connect, so the receiver has to be told to throw
                // away what it already has for this node -- otherwise a reconnect duplicates its history.
                writer.TryWrite(new LogEvent(node.Id, 0, string.Empty, false, true));

                await using Stream body = await response.Content
                    .ReadAsStreamAsync(cancellationToken)
                    .ConfigureAwait(false);

                using var reader = new StreamReader(body);

                while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
                {
                    if (!line.StartsWith("data: ", StringComparison.Ordinal))
                    {
                        continue;
                    }

                    LogLine? parsed = JsonSerializer.Deserialize(line[6..], ObserverJson.Default.LogLine);

                    if (parsed is not null)
                    {
                        writer.TryWrite(new LogEvent(node.Id, parsed.LineNumber, parsed.Content, parsed.IsError, false));
                    }
                }
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (HttpRequestException ex)
            {
                ObserverLog.LogStreamDropped(logger, node.Id, ex.Message);
            }
            catch (IOException ex)
            {
                ObserverLog.LogStreamDropped(logger, node.Id, ex.Message);
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }
}
