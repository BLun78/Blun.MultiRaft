// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Diagnostics;
using System.Globalization;
using System.Net.Http.Json;
using System.Threading.Channels;

namespace Blun.MultiRaft.Observer;

internal sealed record ObservedNode(ulong Id, string ResourceName, int StatusPort)
{
    /// <summary>
    /// Parses <c>id|resource|statusPort;id|resource|statusPort</c> as written by the app host. Anything
    /// malformed is dropped rather than thrown on: the observer's job is to report, and a demo that refuses
    /// to start because one entry was mistyped reports nothing at all.
    /// </summary>
    public static IReadOnlyList<ObservedNode> Parse(string? value)
    {
        List<ObservedNode> nodes = [];

        foreach (string entry in (value ?? string.Empty).Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            string[] parts = entry.Split('|');

            if (parts.Length == 3
                && ulong.TryParse(parts[0], CultureInfo.InvariantCulture, out ulong id)
                && int.TryParse(parts[2], CultureInfo.InvariantCulture, out int port))
            {
                nodes.Add(new ObservedNode(id, parts[1], port));
            }
        }

        return nodes;
    }
}

internal sealed record ObserverOptions(
    int Port,
    Uri ControlPlane,
    IReadOnlyList<ObservedNode> Nodes,
    TimeSpan PollInterval,
    TimeSpan PollTimeout);

/// <summary>
/// Polls every node's <c>/status</c>, folds the answers together with what the app host says about the
/// processes, and publishes the result to whoever is watching.
/// </summary>
/// <remarks>
/// Polling rather than subscribing, because <c>/status</c> is what a node offers and the observer is
/// deliberately an ordinary client of it — it holds no privileged position in the cluster, and everything it
/// displays could have been asked for with curl.
/// </remarks>
internal sealed class ClusterWatcher(
    ObserverOptions options,
    IHttpClientFactory clients,
    ILogger<ClusterWatcher> logger) : BackgroundService
{
    /// <summary>
    /// The reserved administrative group, <c>RaftGroupId.Cluster</c>. Repeated as a literal rather than
    /// referenced, because the observer takes no dependency on the library it watches.
    /// </summary>
    private const ulong ClusterGroupId = ulong.MaxValue;

    private readonly Lock _subscribers = new();
    private readonly List<Channel<ClusterSnapshot>> _channels = [];

    private volatile ClusterSnapshot _current = Empty(options.Nodes);

    public ClusterSnapshot Current => _current;

    /// <summary>
    /// Follows the snapshot stream. Each subscriber gets a one-deep mailbox that drops what it did not keep
    /// up with: a client that stalls should fall behind into the present, not into a growing backlog of
    /// states that stopped being true seconds ago.
    /// </summary>
    public async IAsyncEnumerable<ClusterSnapshot> WatchAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Channel<ClusterSnapshot> channel = Channel.CreateBounded<ClusterSnapshot>(
            new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropOldest, SingleReader = true });

        lock (_subscribers)
        {
            _channels.Add(channel);
        }

        try
        {
            yield return _current;

            await foreach (ClusterSnapshot snapshot in channel.Reader.ReadAllAsync(cancellationToken))
            {
                yield return snapshot;
            }
        }
        finally
        {
            lock (_subscribers)
            {
                _channels.Remove(channel);
            }
        }
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        ObserverLog.WatcherStarted(logger, options.Nodes.Count, (long)options.PollInterval.TotalMilliseconds);

        using var timer = new PeriodicTimer(options.PollInterval);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                ClusterSnapshot snapshot = await PollAsync(stoppingToken).ConfigureAwait(false);
                _current = snapshot;

                lock (_subscribers)
                {
                    foreach (Channel<ClusterSnapshot> channel in _channels)
                    {
                        channel.Writer.TryWrite(snapshot);
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
#pragma warning disable CA1031 // The watcher must outlive any single bad round; it is the thing reporting failure.
            catch (Exception ex)
            {
                ObserverLog.PollFailed(logger, ex);
            }
#pragma warning restore CA1031

            try
            {
                await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task<ClusterSnapshot> PollAsync(CancellationToken cancellationToken)
    {
        Task<(ObservedNode Node, NodeStatus? Status, long Latency, string? Error)>[] polls =
            [.. options.Nodes.Select(node => PollNodeAsync(node, cancellationToken))];

        Task<ResourceState[]> resources = PollResourcesAsync(cancellationToken);

        await Task.WhenAll([.. polls.Cast<Task>(), resources]).ConfigureAwait(false);

        Dictionary<ulong, ResourceState> states = resources.Result.ToDictionary(r => r.Node);
        (ObservedNode Node, NodeStatus? Status, long Latency, string? Error)[] answers =
            [.. polls.Select(p => p.Result)];

        NodeView[] views =
        [
            .. answers.Select(answer =>
            {
                NodeClusterStatus? cluster = answer.Status?.Cluster;
                states.TryGetValue(answer.Node.Id, out ResourceState? state);

                // What this node's logs cost it, all groups together -- the administrative one included,
                // because it is a log on the same disk and its size is part of the answer.
                WalStatus?[] logs =
                [
                    .. (answer.Status?.Groups ?? []).Select(g => g.Wal),
                    cluster?.Group?.Wal,
                ];

                return new NodeView(
                    answer.Node.Id,
                    answer.Node.ResourceName,
                    answer.Node.StatusPort,
                    answer.Status is not null,
                    state?.State,
                    cluster?.State,
                    cluster?.Mode,
                    cluster?.IsClusterLeader ?? false,
                    cluster?.ClusterLeader,
                    cluster?.Voters ?? [],
                    cluster?.Learners ?? [],
                    cluster?.RecentEvents ?? [],
                    answer.Status is null ? null : answer.Latency,
                    answer.Error,
                    logs.Sum(w => w?.SizeBytes ?? 0),
                    logs.Sum(w => w?.Entries ?? 0));
            }),
        ];

        int online = views.Count(v => v.Online);
        int required = (options.Nodes.Count / 2) + 1;

        string?[] leaderOpinions = [.. views.Where(v => v.Online).Select(v => v.ClusterLeader)];

        return new ClusterSnapshot(
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            online,
            options.Nodes.Count,
            required,
            online >= required,
            Majority(leaderOpinions),
            Disagrees(leaderOpinions),
            views,
            BuildGroups(answers));
    }

    private async Task<(ObservedNode Node, NodeStatus? Status, long Latency, string? Error)> PollNodeAsync(
        ObservedNode node,
        CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(options.PollTimeout);

        long started = Stopwatch.GetTimestamp();

        try
        {
            HttpClient client = clients.CreateClient("node");

            NodeStatus? status = await client
                .GetFromJsonAsync(
                    string.Create(CultureInfo.InvariantCulture, $"http://127.0.0.1:{node.StatusPort}/status"),
                    ObserverJson.Default.NodeStatus,
                    timeout.Token)
                .ConfigureAwait(false);

            return (node, status, (long)Stopwatch.GetElapsedTime(started).TotalMilliseconds, null);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return (node, null, 0, "timed out");
        }
        catch (HttpRequestException ex)
        {
            return (node, null, 0, ex.Message);
        }
        catch (System.Text.Json.JsonException ex)
        {
            return (node, null, 0, ex.Message);
        }
    }

    private async Task<ResourceState[]> PollResourcesAsync(CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(options.PollTimeout);

        try
        {
            HttpClient client = clients.CreateClient("node");

            return await client
                .GetFromJsonAsync(
                    new Uri(options.ControlPlane, "/api/resources"),
                    ObserverJson.Default.ResourceStateArray,
                    timeout.Token)
                .ConfigureAwait(false) ?? [];
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return [];
        }
        catch (HttpRequestException)
        {
            // The app host is the one process the observer cannot demand anything of: it may still be
            // starting, and the cluster view does not depend on it.
            return [];
        }
    }

    private GroupView[] BuildGroups(
        IReadOnlyList<(ObservedNode Node, NodeStatus? Status, long Latency, string? Error)> answers)
    {
        SortedSet<ulong> ids = [];

        foreach ((_, NodeStatus? status, _, _) in answers)
        {
            foreach (NodeGroupStatus group in status?.Groups ?? [])
            {
                ids.Add(group.Group);
            }
        }

        List<GroupView> groups = [];

        foreach (ulong id in ids)
        {
            groups.Add(BuildGroup(
                id,
                string.Create(CultureInfo.InvariantCulture, $"Group {id}"),
                answers,
                status => status.Groups?.FirstOrDefault(g => g.Group == id)));
        }

        // The administrative group last: it is the one every other row depends on, and putting it at the
        // bottom keeps the queue groups -- the reason multi-Raft exists -- at the top where they are read.
        groups.Add(BuildGroup(ClusterGroupId, "Cluster group", answers, status => status.Cluster?.Group));

        return [.. groups];
    }

    private GroupView BuildGroup(
        ulong id,
        string label,
        IReadOnlyList<(ObservedNode Node, NodeStatus? Status, long Latency, string? Error)> answers,
        Func<NodeStatus, NodeGroupStatus?> select)
    {
        List<GroupCell> cells = [];
        List<string?> opinions = [];
        SendStatus? sending = null;

        foreach ((ObservedNode node, NodeStatus? status, _, _) in answers)
        {
            NodeGroupStatus? group = status is null ? null : select(status);

            if (group is null)
            {
                cells.Add(new GroupCell(node.Id, false, "Unknown", 0, null, 0, null));
                continue;
            }

            cells.Add(new GroupCell(node.Id, true, group.Role, group.Term, group.Leader, group.CommitIndex, group.Wal));
            opinions.Add(group.Leader);

            // A run is carried by whichever node was leading when it started, and it is the only node that
            // knows how far it has got. A finished run stays visible until another one replaces it.
            if (group.Send is not null && (sending is null || group.Send.Running))
            {
                sending = group.Send;
            }
        }

        return new GroupView(
            id.ToString(CultureInfo.InvariantCulture),
            label,
            Majority(opinions),
            Disagrees(opinions),
            [.. cells],
            sending);
    }

    /// <summary>The value most of the answering nodes named, or <see langword="null"/> if none named one.</summary>
    private static string? Majority(IReadOnlyCollection<string?> opinions)
        => opinions
            .Where(o => o is not null)
            .GroupBy(o => o, StringComparer.Ordinal)
            .OrderByDescending(g => g.Count())
            .FirstOrDefault()
            ?.Key;

    private static bool Disagrees(IReadOnlyCollection<string?> opinions)
        => opinions.Where(o => o is not null).Distinct(StringComparer.Ordinal).Count() > 1;

    private static ClusterSnapshot Empty(IReadOnlyList<ObservedNode> nodes)
        => new(
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            0,
            nodes.Count,
            (nodes.Count / 2) + 1,
            false,
            null,
            false,
            [
                .. nodes.Select(n => new NodeView(
                    n.Id, n.ResourceName, n.StatusPort, false, null, null, null, false, null, [], [], [], null, null, 0, 0)),
            ],
            []);
}
