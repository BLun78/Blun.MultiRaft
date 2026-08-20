// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Globalization;

namespace Blun.MultiRaft.Observer;

/// <summary>
/// Generated traffic into a group's log, started and stopped from the UI.
/// </summary>
/// <remarks>
/// Routed to that group's own leader, which is not the cluster leader and not just any answering node: an
/// append goes to the leader of the group being appended to. The run itself happens on that node — one
/// request here, not one per message — so its pacing is not at the mercy of this hop.
/// </remarks>
internal static class Traffic
{
    public static Task<IResult> StartAsync(
        string group,
        int count,
        int intervalMs,
        int size,
        ClusterWatcher watcher,
        IHttpClientFactory clients,
        CancellationToken cancellationToken)
        => SendAsync(
            group,
            HttpMethod.Post,
            string.Create(CultureInfo.InvariantCulture, $"?count={count}&intervalMs={intervalMs}&size={size}"),
            watcher,
            clients,
            cancellationToken);

    public static Task<IResult> StopAsync(
        string group,
        ClusterWatcher watcher,
        IHttpClientFactory clients,
        CancellationToken cancellationToken)
        => SendAsync(group, HttpMethod.Delete, string.Empty, watcher, clients, cancellationToken);

    private static async Task<IResult> SendAsync(
        string group,
        HttpMethod method,
        string query,
        ClusterWatcher watcher,
        IHttpClientFactory clients,
        CancellationToken cancellationToken)
    {
        ClusterSnapshot current = watcher.Current;

        GroupView? view = current.Groups
            .FirstOrDefault(g => string.Equals(g.Group, group, StringComparison.Ordinal));

        NodeView? leader = view?.Leader is { } id
            ? current.Nodes.FirstOrDefault(
                n => n.Online && string.Equals(n.Id.ToString(CultureInfo.InvariantCulture), id, StringComparison.Ordinal))
            : null;

        if (leader is null)
        {
            return Results.Json(
                new CommandResult(false, false, "that group has no answering leader to send to"),
                ObserverJson.Default.CommandResult,
                statusCode: StatusCodes.Status503ServiceUnavailable);
        }

        using var request = new HttpRequestMessage(
            method,
            new Uri(string.Create(
                CultureInfo.InvariantCulture,
                $"http://127.0.0.1:{leader.StatusPort}/groups/{group}/messages{query}")));

        using HttpResponseMessage response = await clients
            .CreateClient("node")
            .SendAsync(request, cancellationToken)
            .ConfigureAwait(false);

        return Results.Content(
            await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false),
            "application/json",
            statusCode: (int)response.StatusCode);
    }
}
