// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Globalization;

namespace Blun.MultiRaft.Observer;

/// <summary>Taking a node out of the cluster group's configuration, and putting it back.</summary>
internal static class Membership
{
    /// <summary>
    /// Forwards the change to the cluster leader. Placement can be asked of anyone and routes itself; a
    /// membership change cannot, because it is an append and only a leader appends. When no leader is known
    /// the request still goes to an answering node, which replies saying so and naming who it thinks leads —
    /// a more useful answer than one this proxy could invent.
    /// </summary>
    public static async Task<IResult> ChangeAsync(
        ulong node,
        HttpMethod method,
        ClusterWatcher watcher,
        IHttpClientFactory clients,
        CancellationToken cancellationToken)
    {
        ClusterSnapshot current = watcher.Current;

        NodeView? target =
            current.Nodes.FirstOrDefault(n => n.Online && n.IsClusterLeader)
            ?? current.Nodes.FirstOrDefault(n => n.Online);

        if (target is null)
        {
            return Results.Json(
                new CommandResult(false, false, "no node is answering"),
                ObserverJson.Default.CommandResult,
                statusCode: StatusCodes.Status503ServiceUnavailable);
        }

        using var request = new HttpRequestMessage(
            method,
            new Uri(string.Create(
                CultureInfo.InvariantCulture,
                $"http://127.0.0.1:{target.StatusPort}/cluster/nodes/{node}")));

        using HttpResponseMessage response = await clients
            .CreateClient("node")
            .SendAsync(request, cancellationToken)
            .ConfigureAwait(false);

        string body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);

        return Results.Content(body, "application/json", statusCode: (int)response.StatusCode);
    }
}
