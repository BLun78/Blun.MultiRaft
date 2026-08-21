// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Threading.Channels;
using Blun.MultiRaft.Hosting;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Blun.MultiRaft.Cluster;

/// <summary>
/// The cluster-management group and everything around it: which nodes exist, who leads the cluster, how much
/// each node is carrying, and whether a given node could take over a group's leadership.
/// </summary>
/// <remarks>
/// It decides nothing on its own. Nothing here moves a group's leader on a timer, on a threshold, or on a
/// hunch — leadership changes when Raft decides one is needed, or when a caller asks for one. What this class
/// offers is the two things a caller cannot work out for itself: whether a proposed node is actually a legal
/// target, and which node is carrying the least. What to do with that is the host's call.
/// <para>
/// It runs on every node and acts only where it must. Load reports are pushed by everyone to whoever leads
/// the cluster group; configuration reconciliation and the actionable event happen on that leader alone.
/// </para>
/// </remarks>
public sealed class ClusterCoordinator : IRaftClusterListener, IAsyncDisposable
{
    private readonly MultiRaftHost _host;
    private readonly ClusterOptions _options;
    private readonly ClusterModeStore _modeStore;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _shutdown = new();

    private readonly ConcurrentDictionary<NodeId, LoadEntry> _load = new();
    private readonly Channel<ClusterEvent> _events;

    private RaftGroupInstance? _group;
    private Task? _loop;
    private long _reportSequence;
    private bool _disposed;

    // Set on a seed until its own voter status has been written to the log. Retried rather than attempted
    // once: the coordinator's opening campaign races the host's tick loop, which may already have queued one
    // of its own for this group, and whichever loses leaves IsLeader false at exactly the moment a one-shot
    // attempt would look.
    private bool _selfMembershipPending;

    // What the cluster leader last saw as available, so the actionable event fires on the change rather than
    // on every pass. Seeded silently when this node takes over, or a fresh leader would announce the whole
    // cluster as newly arrived.
    private ImmutableHashSet<NodeId> _available = [];
    private bool _availableSeeded;

    // Last published observational state, for the same reason.
    private (NodeId? Leader, ClusterServiceState State, int Members) _lastPublished = (null, ClusterServiceState.Starting, -1);

    /// <summary>Creates a coordinator over an already-constructed host. Nothing happens until <see cref="StartAsync"/>.</summary>
    public ClusterCoordinator(
        MultiRaftHost host,
        ClusterOptions options,
        TimeProvider? timeProvider = null,
        ILogger<ClusterCoordinator>? logger = null)
    {
        _host = host ?? throw new ArgumentNullException(nameof(host));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _time = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger<ClusterCoordinator>.Instance;
        _modeStore = new ClusterModeStore(options.DataDirectory);

        if (host.Self != options.Self)
        {
            throw new ArgumentException(
                "The host is node " + host.Self + " but the cluster options say " + options.Self + ".",
                nameof(options));
        }

        // Bounded and dropping the oldest. A consumer that stops reading must not be able to grow this
        // without limit on a node whose job is to keep thousands of queues alive; at this event rate, a
        // backlog of 256 means the consumer is not reading at all, and the newest state is the useful one.
        _events = Channel.CreateBounded<ClusterEvent>(
            new BoundedChannelOptions(256) { FullMode = BoundedChannelFullMode.DropOldest, SingleReader = false });
    }

    /// <summary>Single-node or replicated. Fixed for the life of this process.</summary>
    public ClusterMode Mode => _options.Mode;

    /// <summary>Identity of this node.</summary>
    public NodeId Self => _options.Self;

    /// <summary>The cluster-management group, once started.</summary>
    public RaftGroupInstance? Group => _group;

    /// <summary>Whether this node leads the cluster group, and is therefore the cluster leader.</summary>
    public bool IsClusterLeader => _group?.IsLeader ?? false;

    /// <summary>Who leads the cluster, as far as this node knows.</summary>
    public NodeId? ClusterLeader => _group?.LeaderId;

    /// <summary>
    /// Whether this node's administrative plane is usable. Queue groups are unaffected by this either way —
    /// each has its own quorum, and losing the cluster group's does not touch them.
    /// </summary>
    public ClusterServiceState State
    {
        get
        {
            if (_disposed)
            {
                return ClusterServiceState.Stopped;
            }

            if (_group is not { } group)
            {
                return ClusterServiceState.Starting;
            }

            if (group.IsLeader)
            {
                // A leader that had lost the majority would already have stepped aside: the cluster group
                // runs with CheckQuorum on, which is exactly what makes this answer trustworthy rather than
                // an assumption.
                return ClusterServiceState.Available;
            }

            return group.LeaderId is not null && group.HasLeaderContact
                ? ClusterServiceState.Available
                : ClusterServiceState.AdminSuspended;
        }
    }

    /// <summary>
    /// Cluster events, in order. Two kinds, and they are for different things — see
    /// <see cref="ClusterEventKind"/>.
    /// </summary>
    /// <remarks>
    /// A stream rather than a C# <c>event</c>, and not for taste. A handler invoked inline would run
    /// consumer code of unknown duration on the caller's stack, and this library has already been bitten once
    /// by exactly that shape: work started on the tick loop's own stack kept the loop from ever observing
    /// shutdown. Reading at your own pace cannot do that, and a handler that throws cannot take the
    /// coordinator with it.
    /// <para>
    /// Meant for one consumer. Events are handed out, not broadcast, so two enumerators would each see part
    /// of the sequence rather than both seeing all of it.
    /// </para>
    /// <para>
    /// No cancellation token is woven in here. <see cref="DisposeAsync"/> completes the channel, which ends
    /// the enumeration on its own — whereas a token read from the coordinator's own source would be resolved
    /// when the consumer starts enumerating rather than when this property is read, and a consumer that
    /// started a moment after disposal would get <see cref="ObjectDisposedException"/> instead of a clean end.
    /// Pass your own token to <c>WithCancellation</c> if you want to stop reading early.
    /// </para>
    /// </remarks>
    public IAsyncEnumerable<ClusterEvent> Events => _events.Reader.ReadAllAsync();

    /// <summary>
    /// Opens the cluster group and starts reporting. Validates the recorded mode first, and refuses to start
    /// if this node last ran in a way that makes the requested one unsafe.
    /// </summary>
    public async ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        if (_group is not null)
        {
            return;
        }

        ClusterModeMarker? previous = await _modeStore.ReadAsync(cancellationToken).ConfigureAwait(false);
        bool promoted = ClusterModeStore.ValidateTransition(previous, _options.Mode, _options.Self);
        await _modeStore
            .WriteAsync(new ClusterModeMarker(_options.Mode, _options.Self), cancellationToken)
            .ConfigureAwait(false);

        if (promoted)
        {
            ClusterLog.PromotedToReplicated(_logger, _options.Self.Value, _options.EffectiveNodes.Length);
        }

        if (!_modeStore.IsDurable)
        {
            ClusterLog.ModeNotDurable(_logger, _options.Self.Value);
        }

        // "Has this node ever run before" is the only question that decides how it starts, and the marker is
        // the one answer to it that cannot race. Asking the log instead -- has it any entries yet -- looks
        // equivalent and is not: the host's tick loop is already running by the time this method is reached,
        // so it can campaign the group and append a term's no-op between the group being added and the
        // length being read.
        bool fresh = previous is null;
        bool seed = fresh && ShouldSeed();

        // A node with history passes an empty configuration and lets its own log say who votes. Replay
        // applies membership entries over whatever is handed in, so anything passed here would be a second
        // opinion competing with the record -- and the record is the one every other node also rebuilds from.
        RaftMembership membership = seed ? RaftMembership.OfVoters(_options.Self) : RaftMembership.Empty;

        _group = await _host.AddGroupCoreAsync(
            RaftGroupId.Cluster,
            membership,
            _options.AdminStateMachine,
            _options.EffectiveGroupOptions,
            cancellationToken).ConfigureAwait(false);

        _host.ClusterSink = this;

        ClusterLog.Started(
            _logger,
            _options.Self.Value,
            _options.Mode.ToString(),
            seed,
            _options.EffectiveNodes.Length);

        if (seed)
        {
            // The seed's own voter status has to reach the log, and until it does the configuration is
            // asymmetric in a way that stays hidden: this node is a voter because StartAsync was handed a
            // configuration saying so, and nothing in the log says it. Every node joining later rebuilds the
            // configuration by replaying membership entries -- that is the whole mechanism -- so it would see
            // the nodes this one added and not this one. Two views of who votes is two different quorums, and
            // the disagreement is silent until it decides an election.
            _selfMembershipPending = true;

            // Campaigning outright rather than waiting out an election timeout. A seed is the only voter it
            // knows of, so the round is decided without a single message going anywhere. Only a genuinely
            // new node does this: a node with history starts as an ordinary follower and campaigns on its
            // timer like everyone else, rather than forcing an election on the cluster at every restart.
            await _group.CampaignAsync(cancellationToken).ConfigureAwait(false);
            await WriteSelfMembershipAsync(_group, cancellationToken).ConfigureAwait(false);
        }

        _loop = Task.Run(CoordinationLoopAsync, CancellationToken.None);
    }

    /// <summary>
    /// Appends an administrative command to the cluster group. Cluster leader only.
    /// </summary>
    /// <remarks>
    /// The payload is the host's own; this library never reads it. Node membership travels the same log as
    /// <see cref="RaftEntryKind.Membership"/> entries, which are applied by the group itself and never
    /// reach a state machine — so the two cannot be mistaken for one another however the host encodes its own
    /// commands.
    /// </remarks>
    public ValueTask<long> AppendAdminAsync(
        ReadOnlyMemory<byte> payload,
        byte applicationTag = 0,
        CancellationToken cancellationToken = default)
    {
        RaftGroupInstance group = _group
            ?? throw new InvalidOperationException("The cluster coordinator has not been started.");

        return group.AppendAsync(payload, applicationTag, cancellationToken);
    }

    /// <summary>
    /// Creates a group on this node, optionally asking a particular node to lead it from the start.
    /// </summary>
    /// <remarks>
    /// Local. Every node that should hold a replica calls this for itself — the library does not push group
    /// creation across the cluster, because which queues exist is the host's catalogue, and the host already
    /// has a replicated place to keep it in the cluster group's log.
    /// <para>
    /// The initial-leader hint is honoured by campaigning immediately on the named node, which costs nothing:
    /// at creation every replica's log is empty, so the election restriction cannot separate them and the
    /// first candidate wins. Doing it this way rather than by electing at random and transferring afterwards
    /// saves a handover the group would otherwise pay for within seconds of existing.
    /// </para>
    /// </remarks>
    public async ValueTask<RaftGroupInstance> CreateGroupAsync(
        RaftGroupId group,
        RaftMembership membership,
        NodeId? initialLeader = null,
        IRaftStateMachine? stateMachine = null,
        RaftGroupOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        RaftGroupInstance instance = await _host
            .AddGroupAsync(group, membership, stateMachine, options, cancellationToken)
            .ConfigureAwait(false);

        if (initialLeader == _options.Self && membership.IsVoter(_options.Self) && !instance.IsLeader)
        {
            await instance.CampaignAsync(cancellationToken).ConfigureAwait(false);
        }

        return instance;
    }

    /// <summary>
    /// Asks whether a specific node could take over a group, changing nothing. The question is answered by
    /// that group's current leader, wherever it is.
    /// </summary>
    public ValueTask<LeaderTargetResponse> CheckLeaderTargetAsync(
        RaftGroupId group,
        NodeId candidate,
        CancellationToken cancellationToken = default)
        => AskLeaderAsync(new LeaderTargetRequest(group, candidate, Execute: false), cancellationToken);

    /// <summary>
    /// Returns a node that could lead <paramref name="group"/>: <paramref name="preferred"/> if it is a legal
    /// target, otherwise the best of the alternatives. Changes nothing.
    /// </summary>
    public ValueTask<LeaderTargetResponse> ResolveLeaderTargetAsync(
        RaftGroupId group,
        NodeId? preferred = null,
        CancellationToken cancellationToken = default)
        => ResolveAsync(group, preferred, execute: false, cancellationToken);

    /// <summary>
    /// Hands leadership of <paramref name="group"/> to <paramref name="preferred"/>, or to the best
    /// alternative if that node cannot take it.
    /// </summary>
    /// <remarks>
    /// The group is never left without a leader. A target that turns out to be unsuitable, declines, or
    /// cannot be reached simply does not receive leadership, and the node that has it keeps it — there is no
    /// step in this that gives leadership up before a successor has accepted.
    /// </remarks>
    public ValueTask<LeaderTargetResponse> RequestLeaderTransferAsync(
        RaftGroupId group,
        NodeId? preferred = null,
        CancellationToken cancellationToken = default)
        => ResolveAsync(group, preferred, execute: true, cancellationToken);

    /// <summary>
    /// What the cluster leader currently believes about each node's load, ordered the way placement orders
    /// it: least-loaded first, the cluster leader itself last.
    /// </summary>
    /// <remarks>
    /// Empty on a node that is not the cluster leader — reports are pushed to the leader, and nobody else has
    /// a picture to offer. Advisory in every case: it is as fresh as the last round of reports.
    /// </remarks>
    public ClusterLoadSnapshot GetLoad()
    {
        if (!IsClusterLeader)
        {
            return new ClusterLoadSnapshot([], ClusterLeader);
        }

        long now = _time.GetTimestamp();
        List<NodeLoad> nodes = [];
        foreach (LoadEntry entry in _load.Values)
        {
            TimeSpan age = ElapsedSince(entry.Timestamp, now);
            if (age <= _options.LoadReportTtl)
            {
                nodes.Add(new NodeLoad(entry.Node, entry.GroupCount, entry.LeaderCount, age));
            }
        }

        // Self is the cluster leader here — this method returns early otherwise — so the rule's "cluster
        // leader last" clause applies to this node.
        nodes.Sort((left, right) => ComparePlacement(left.Node, left.LeaderCount, right.Node, right.LeaderCount, Self));
        return new ClusterLoadSnapshot(nodes, Self);
    }

    /// <inheritdoc />
    public ValueTask OnLoadReportAsync(NodeLoadReport report, CancellationToken cancellationToken = default)
    {
        long now = _time.GetTimestamp();
        _load.AddOrUpdate(
            report.Node,
            _ => new LoadEntry(report.Node, report.Sequence, report.GroupCount, report.LeaderCount, now),
            (_, existing) => report.Sequence < existing.Sequence
                // A report that overtook a newer one on the wire. Keeping the newer numbers but refreshing
                // the timestamp would be wrong too -- this node is demonstrably alive, and that is what the
                // timestamp is for.
                ? existing with { Timestamp = now }
                : new LoadEntry(report.Node, report.Sequence, report.GroupCount, report.LeaderCount, now));

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<LeaderTargetResponse> OnLeaderTargetAsync(
        LeaderTargetRequest request,
        CancellationToken cancellationToken = default)
        => _host.OnLeaderTargetAsync(request, cancellationToken);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _host.ClusterSink = null;

        // Completed before the loop is cancelled, so a consumer mid-enumeration sees the sequence end rather
        // than racing the source's disposal a few lines further down.
        _events.Writer.TryComplete();
        await _shutdown.CancelAsync().ConfigureAwait(false);

        if (_loop is not null)
        {
            try
            {
                await _loop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }
        }

        _shutdown.Dispose();
    }

    /// <summary>
    /// Whether this node starts the cluster group as its own voter, or waits to be adopted as a learner.
    /// </summary>
    /// <remarks>
    /// This is the whole of the bootstrap rule, and getting it wrong is how a node with data loses it. A
    /// fresh node must <em>not</em> come up believing it is a voter: two empty voters are equally complete
    /// under the election restriction, so they will happily elect one of themselves and then truncate the
    /// node that actually had the log. Starting empty-handed and waiting to be adopted removes that
    /// possibility rather than making it unlikely.
    /// <list type="bullet">
    /// <item>Single-node — the only node there is.</item>
    /// <item>Replicated, lowest id — somebody has to start a cold cluster, and the lowest id is the one
    /// choice every node reaches without being told.</item>
    /// <item>Replicated, any other id — waits. If the seed is down the cluster does not form, which is
    /// correct: there is no data yet to be unavailable, and it forms as soon as the seed appears.</item>
    /// </list>
    /// <para>
    /// Only ever asked of a node with no history. One that has run before takes its configuration from its
    /// own log and starts as an ordinary follower, whatever its id.
    /// </para>
    /// </remarks>
    private bool ShouldSeed()
        => _options.Mode == ClusterMode.SingleNode
           || _options.Self == _options.EffectiveNodes.Min();

    private async ValueTask<LeaderTargetResponse> ResolveAsync(
        RaftGroupId group,
        NodeId? preferred,
        bool execute,
        CancellationToken cancellationToken)
    {
        if (preferred is { } named)
        {
            LeaderTargetResponse first = await AskLeaderAsync(
                new LeaderTargetRequest(group, named, execute),
                cancellationToken).ConfigureAwait(false);

            if (first.Status is LeaderTargetStatus.Valid or LeaderTargetStatus.IsCurrentLeader)
            {
                return first;
            }

            if (first.Status is LeaderTargetStatus.NoLeader or LeaderTargetStatus.Unreachable)
            {
                // Nothing is wrong with the candidate; the group cannot be asked at all. Walking the
                // alternatives would just ask the same unreachable leader again.
                return first;
            }

            ClusterLog.TargetRejected(_logger, group.Value, named.Value, first.Status.ToString());
        }

        LeaderTargetResponse last = new(Self, LeaderTargetStatus.NoLeader, 0, Transferred: false);
        foreach (NodeId candidate in OrderCandidates(group, preferred))
        {
            last = await AskLeaderAsync(
                new LeaderTargetRequest(group, candidate, execute),
                cancellationToken).ConfigureAwait(false);

            if (last.Status is LeaderTargetStatus.Valid or LeaderTargetStatus.IsCurrentLeader)
            {
                return last;
            }

            // Only these two end the walk, and only because they say nothing about the candidate: the group's
            // leader could not be asked at all, so the next candidate would put the same question to the same
            // silent leader. Every other refusal -- Lagging, NotResponding, NotAVoter -- is an answer about
            // this one node, and the point of the walk is to try the next one.
            if (last.Status is LeaderTargetStatus.NoLeader or LeaderTargetStatus.Unreachable)
            {
                return last;
            }
        }

        return last;
    }

    /// <summary>
    /// Orders the group's voters the way placement wants them: fewest leaderships first, the cluster leader
    /// last, node id breaking ties.
    /// </summary>
    /// <remarks>
    /// The cluster leader is sorted last, not filtered out. As a filter it would leave a single-node cluster —
    /// or any group whose only other voter is the cluster leader — with no legal target at all, turning a
    /// preference into an outage. As a sort key it is simply never chosen while anyone else will do.
    /// <para>
    /// A node that has not reported is ranked below every node that has. There is no evidence it is idle, and
    /// preferring an unmeasured node over a measured empty one would be a guess dressed as a decision.
    /// </para>
    /// </remarks>
    private List<NodeId> OrderCandidates(RaftGroupId group, NodeId? exclude)
    {
        List<NodeId> candidates = [];
        if (!_host.TryGetGroup(group, out RaftGroupInstance? instance) || instance is null)
        {
            return candidates;
        }

        foreach (NodeId voter in instance.Membership.Voters)
        {
            if (voter != exclude)
            {
                candidates.Add(voter);
            }
        }

        long now = _time.GetTimestamp();
        NodeId? clusterLeader = ClusterLeader;

        candidates.Sort((left, right) => ComparePlacement(
            left,
            LeaderCountOf(left),
            right,
            LeaderCountOf(right),
            clusterLeader));

        return candidates;

        int LeaderCountOf(NodeId node)
            => _load.TryGetValue(node, out LoadEntry entry)
               && ElapsedSince(entry.Timestamp, now) <= _options.LoadReportTtl
                ? entry.LeaderCount
                : int.MaxValue;
    }

    /// <summary>Puts a leadership question to the group's leader, wherever it is.</summary>
    private async ValueTask<LeaderTargetResponse> AskLeaderAsync(
        LeaderTargetRequest request,
        CancellationToken cancellationToken)
    {
        if (!_host.TryGetGroup(request.Group, out RaftGroupInstance? instance) || instance is null)
        {
            return new LeaderTargetResponse(Self, LeaderTargetStatus.Unreachable, 0, Transferred: false);
        }

        if (instance.IsLeader)
        {
            return await instance.EvaluateLeaderTargetAsync(request, cancellationToken).ConfigureAwait(false);
        }

        if (instance.LeaderId is not { } leader)
        {
            return new LeaderTargetResponse(Self, LeaderTargetStatus.NoLeader, 0, Transferred: false);
        }

        if (_host.Transport is not IRaftClusterTransport transport)
        {
            return new LeaderTargetResponse(leader, LeaderTargetStatus.Unreachable, 0, Transferred: false);
        }

        try
        {
            return await transport.LeaderTargetAsync(leader, request, cancellationToken).ConfigureAwait(false);
        }
        catch (IOException)
        {
            // An unreachable leader is an ordinary condition everywhere else in this library, and it is one
            // here too: the caller learns nothing about the target, not that the target is bad.
            return new LeaderTargetResponse(leader, LeaderTargetStatus.Unreachable, 0, Transferred: false);
        }
    }

    private async Task CoordinationLoopAsync()
    {
        var ticker = new PeriodicTimer(_options.LoadReportInterval, _time);
        long nextReconcile = 0;

        try
        {
            while (await ticker.WaitForNextTickAsync(_shutdown.Token).ConfigureAwait(false))
            {
                try
                {
                    await ReportLoadAsync().ConfigureAwait(false);
                    PublishStateChange();

                    long now = _time.GetTimestamp();
                    if (now >= nextReconcile)
                    {
                        nextReconcile = now + ToTicks(_options.ReconcileInterval);
                        await ReconcileAsync().ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    // The cluster group being briefly unusable is normal -- an election, a partition, a peer
                    // restarting. The loop has to survive all of them, not just IOException/
                    // InvalidOperationException; the next pass is seconds away.
                    ClusterLog.PassFailed(_logger, ex, _options.Self.Value);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
        finally
        {
            ticker.Dispose();
        }
    }

    /// <summary>Pushes this node's load to the cluster leader. Skipped when this node is the leader.</summary>
    private async ValueTask ReportLoadAsync()
    {
        if (_group is not { } group || group.IsLeader || group.LeaderId is not { } leader)
        {
            if (_group?.IsLeader == true)
            {
                // The leader's own numbers never go on the wire; they are simply read locally, which also
                // means they are never stale in the model.
                await OnLoadReportAsync(
                    new NodeLoadReport(
                        Self,
                        Interlocked.Increment(ref _reportSequence),
                        _host.Groups.Count,
                        _host.LeaderCount)).ConfigureAwait(false);
            }

            return;
        }

        if (_host.Transport is not IRaftClusterTransport transport)
        {
            return;
        }

        var report = new NodeLoadReport(
            Self,
            Interlocked.Increment(ref _reportSequence),
            _host.Groups.Count,
            _host.LeaderCount);

        try
        {
            await transport.ReportLoadAsync(leader, report, _shutdown.Token).ConfigureAwait(false);
        }
        catch (IOException)
        {
            // A dropped report costs nothing: the leader ages this node out and picks it up again on the next
            // one. Reports are advisory by construction, which is what makes losing them cheap.
        }
    }

    /// <summary>
    /// Brings the cluster group's configuration towards the configured node set, and raises the actionable
    /// event when a node becomes usable. Cluster leader only.
    /// </summary>
    /// <remarks>
    /// This is membership convergence, not load balancing. It adds nodes and promotes caught-up learners; it
    /// never removes anyone and never moves a group's leader. Removing a node destroys its vote and is the
    /// kind of thing an operator should ask for explicitly rather than have inferred from a configuration
    /// file that happened to be shorter today.
    /// </remarks>
    private async ValueTask ReconcileAsync()
    {
        if (_group is not { IsLeader: true } group || _options.Mode == ClusterMode.SingleNode)
        {
            // Not the leader: whatever this node believed about availability belongs to a term it no longer
            // owns. Dropping it means the next leadership will seed itself afresh rather than announcing
            // stale arrivals.
            _availableSeeded = false;
            _available = [];
            return;
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(_shutdown.Token);
        timeout.CancelAfter(_options.ReconcileInterval);
        CancellationToken token = timeout.Token;

        // Before anything else. Until this lands, every node adopted below rebuilds a configuration that is
        // missing the node adopting it.
        await WriteSelfMembershipAsync(group, token).ConfigureAwait(false);

        RaftMembership membership = group.Membership;

        foreach (NodeId node in _options.EffectiveNodes)
        {
            if (!membership.Contains(node))
            {
                // Adding a learner never moves the quorum, so several in one pass are all equally safe.
                await group.AddLearnerAsync(node, token).ConfigureAwait(false);
                ClusterLog.NodeAdopted(_logger, node.Value);
            }
        }

        membership = group.Membership;
        foreach (NodeId learner in membership.Learners)
        {
            if (!_options.EffectiveNodes.Contains(learner))
            {
                continue;
            }

            // One promotion per pass. This is the single operation that moves the quorum, and Raft's
            // single-server rule is what keeps the old and new majorities overlapping.
            if (await group.PromoteToVoterAsync(learner, token).ConfigureAwait(false))
            {
                ClusterLog.NodePromoted(_logger, learner.Value);
            }

            break;
        }

        RaiseAvailability(group);
    }

    /// <summary>
    /// Records the seed's own voter status as an ordinary membership entry, once it is actually leading.
    /// </summary>
    /// <remarks>
    /// Retried from the reconciliation pass rather than attempted once at startup. The opening campaign
    /// races the tick loop's own queued campaign for the same group, so whether this node is leading at any
    /// particular instant after <c>CampaignAsync</c> returns is not something to build on — but leading
    /// eventually is, and one pass later costs nothing because a membership entry applies wherever it lands
    /// in the order.
    /// <para>
    /// Idempotent by construction: promoting a node that already votes changes nothing, so a duplicate would
    /// be a dead entry rather than a wrong one.
    /// </para>
    /// </remarks>
    private async ValueTask WriteSelfMembershipAsync(RaftGroupInstance group, CancellationToken cancellationToken)
    {
        if (!_selfMembershipPending || !group.IsLeader)
        {
            return;
        }

        await group.PromoteToVoterAsync(Self, cancellationToken).ConfigureAwait(false);
        _selfMembershipPending = false;
        ClusterLog.SeedRecorded(_logger, Self.Value);
    }

    private void RaiseAvailability(RaftGroupInstance group)
    {
        TimeSpan window = _options.EffectiveGroupOptions.ElectionTimeout * 2;
        ImmutableHashSet<NodeId>.Builder builder = ImmutableHashSet.CreateBuilder<NodeId>();

        foreach (NodeId node in group.Membership.AllMembers)
        {
            if (node == Self || group.HasRecentContact(node, window))
            {
                builder.Add(node);
            }
        }

        ImmutableHashSet<NodeId> current = builder.ToImmutable();

        if (!_availableSeeded)
        {
            // First pass as leader. Everything currently up is the baseline, not news.
            _available = current;
            _availableSeeded = true;
            return;
        }

        foreach (NodeId node in current)
        {
            if (!_available.Contains(node))
            {
                ClusterLog.NodeAvailable(_logger, node.Value);
                _events.Writer.TryWrite(
                    new ClusterEvent(ClusterEventKind.NodeBecameAvailable, node, Self, State));
            }
        }

        _available = current;
    }

    /// <summary>Emits the observational event when this node's view actually changed. Every node.</summary>
    private void PublishStateChange()
    {
        NodeId? leader = ClusterLeader;
        ClusterServiceState state = State;
        int members = _group?.Membership.Voters.Length ?? 0;

        if (_lastPublished == (leader, state, members))
        {
            return;
        }

        _lastPublished = (leader, state, members);
        _events.Writer.TryWrite(new ClusterEvent(ClusterEventKind.ClusterStateChanged, Self, leader, state));
    }

    private TimeSpan ElapsedSince(long timestamp, long now)
        => TimeSpan.FromSeconds((now - timestamp) / (double)_time.TimestampFrequency);

    private long ToTicks(TimeSpan span) => (long)(span.TotalSeconds * _time.TimestampFrequency);

    /// <summary>
    /// The placement rule, and the only place it is written down: fewest leaderships first, the cluster
    /// leader last, node id breaking ties.
    /// </summary>
    /// <remarks>
    /// Shared by <see cref="OrderCandidates"/>, which picks a transfer target, and by <see cref="GetLoad"/>,
    /// which shows an operator the same ranking. They had a rule each, and only the placement one sorted the
    /// cluster leader last — so the snapshot could name a node as least-loaded that placement would never have
    /// chosen, which is exactly the kind of quiet disagreement a single rule everywhere was meant to prevent.
    /// </remarks>
    private static int ComparePlacement(
        NodeId left,
        int leftLeaderCount,
        NodeId right,
        int rightLeaderCount,
        NodeId? clusterLeader)
    {
        int byLeaderRole = Rank(left).CompareTo(Rank(right));
        if (byLeaderRole != 0)
        {
            return byLeaderRole;
        }

        int byLoad = leftLeaderCount.CompareTo(rightLeaderCount);
        return byLoad != 0 ? byLoad : left.CompareTo(right);

        int Rank(NodeId node) => node == clusterLeader ? 1 : 0;
    }

    private readonly record struct LoadEntry(
        NodeId Node,
        long Sequence,
        int GroupCount,
        int LeaderCount,
        long Timestamp);
}

internal static partial class ClusterLog
{
    [LoggerMessage(
        EventId = 1200,
        Level = LogLevel.Information,
        Message = "Cluster coordinator started on node {Node} in {Mode} mode (seed: {Seed}, {NodeCount} nodes configured).")]
    public static partial void Started(ILogger logger, ulong node, string mode, bool seed, int nodeCount);

    [LoggerMessage(
        EventId = 1201,
        Level = LogLevel.Information,
        Message = "Node {Node} was promoted from a single-node cluster to a replicated one of {NodeCount} nodes; its log is the seed.")]
    public static partial void PromotedToReplicated(ILogger logger, ulong node, int nodeCount);

    [LoggerMessage(
        EventId = 1202,
        Level = LogLevel.Warning,
        Message = "Node {Node} has no data directory, so its cluster mode is not enforced across restarts.")]
    public static partial void ModeNotDurable(ILogger logger, ulong node);

    [LoggerMessage(
        EventId = 1203,
        Level = LogLevel.Information,
        Message = "Node {Node} was adopted into the cluster group as a learner.")]
    public static partial void NodeAdopted(ILogger logger, ulong node);

    [LoggerMessage(
        EventId = 1204,
        Level = LogLevel.Information,
        Message = "Node {Node} caught up and was promoted to a cluster voter.")]
    public static partial void NodePromoted(ILogger logger, ulong node);

    [LoggerMessage(
        EventId = 1205,
        Level = LogLevel.Information,
        Message = "Node {Node} became available in the cluster group.")]
    public static partial void NodeAvailable(ILogger logger, ulong node);

    [LoggerMessage(
        EventId = 1206,
        Level = LogLevel.Debug,
        Message = "Group {Group}: node {Node} was refused as a leadership target ({Status}); trying alternatives.")]
    public static partial void TargetRejected(ILogger logger, ulong group, ulong node, string status);

    [LoggerMessage(
        EventId = 1207,
        Level = LogLevel.Warning,
        Message = "A cluster coordination pass on node {Node} failed.")]
    public static partial void PassFailed(ILogger logger, Exception exception, ulong node);

    [LoggerMessage(
        EventId = 1208,
        Level = LogLevel.Information,
        Message = "Seed node {Node} recorded its own voter status in the cluster group's log.")]
    public static partial void SeedRecorded(ILogger logger, ulong node);
}
