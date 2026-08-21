// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Globalization;

namespace Blun.MultiRaft.Observer;

/// <summary>
/// "Take a snapshot now", driven from the UI.
/// </summary>
/// <remarks>
/// Unlike <see cref="Traffic"/>, this is not routed to a group's leader: a snapshot captures a node's own
/// applied state and needs no quorum, so any node running that group can serve it locally. The button in
/// the WAL panel points at the group's leader anyway, by convention, since that is the copy already shown
/// there — but the endpoint itself works against whichever node answers.
/// </remarks>
internal static class Snapshot
{
    public static async Task<IResult> TakeAsync(
        string group,
        ulong node,
        ClusterWatcher watcher,
        IHttpClientFactory clients,
        CancellationToken cancellationToken)
    {
        NodeView? target = watcher.Current.Nodes.FirstOrDefault(n => n.Online && n.Id == node);

        if (target is null)
        {
            return Results.Json(
                new CommandResult(false, false, "that node is not answering"),
                ObserverJson.Default.CommandResult,
                statusCode: StatusCodes.Status503ServiceUnavailable);
        }

        using HttpResponseMessage response = await clients
            .CreateClient("node")
            .PostAsync(
                new Uri(string.Create(
                    CultureInfo.InvariantCulture,
                    $"http://127.0.0.1:{target.StatusPort}/groups/{group}/snapshot")),
                null,
                cancellationToken)
            .ConfigureAwait(false);

        return Results.Content(
            await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false),
            "application/json",
            statusCode: (int)response.StatusCode);
    }
}
