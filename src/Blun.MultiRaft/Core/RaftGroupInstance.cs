// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Blun.MultiRaft;

/// <summary>
/// One Raft consensus group — in Blun.MQ, one queue. Everything that makes a group a group lives here: its
/// term, its role, its log, its followers. Nothing is shared with sibling groups except the transport, the
/// timer that ticks them, and the thread pool.
/// </summary>
/// <remarks>
/// Replication is leader-driven and asymmetric: followers never talk to each other, and the leader does not
/// wait on them in line. An append lands in the local log and returns as soon as the group's
/// <see cref="DurabilityLevel"/> is satisfied, while the push to followers proceeds on its own. Commit is
/// still the real thing — a majority of voters must report a matching index — but the leader discovers that
/// through responses arriving asynchronously rather than by blocking a request thread on the slowest peer.
/// </remarks>
public sealed partial class RaftGroupInstance : IAsyncDisposable
{
    private readonly IRaftWal _wal;
    private readonly IRaftMetaStore _metaStore;
    private readonly IRaftProtocolTransport _transport;
    private readonly IRaftStateMachine _stateMachine;
    private readonly IRaftSnapshotStore? _snapshots;
    private readonly RaftGroupOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;

    // Serializes everything that mutates term, role or log shape. Async, never a Monitor: a group holding an
    // OS lock while its fsync completes would block a pool thread that thousands of sibling groups need.
    private readonly SemaphoreSlim _stateGate = new(1, 1);
    private readonly SemaphoreSlim _applyGate = new(1, 1);

    private readonly ConcurrentDictionary<NodeId, PeerReplicationState> _peers = new();
    private readonly ConcurrentDictionary<long, CommitWaiter> _commitWaiters = new();
    private readonly CancellationTokenSource _shutdown = new();

    private long _currentTerm;
    private NodeId? _votedFor;
    private int _role = (int)RaftRole.Follower;
    private NodeId? _leaderId;
    private long _commitIndex;
    private long _lastApplied;
    private RaftMembership _membership = RaftMembership.Empty;

    private long _electionDeadline;

    // Distinct from the election deadline on purpose. The election timer is reset by this node campaigning
    // and by granting a vote; this one moves only when a leader is actually heard from. Conflating the two
    // makes two rival candidates each look "still led" to the other, and their pre-votes deadlock forever.
    private long _leaderContactDeadline;

    // Set while a leadership transfer (Raft §3.10) is in flight. New client writes are refused for as long
    // as _transferInProgress is nonzero -- the paper requires the leader to stop taking requests during a
    // transfer, since anything appended after the successor was told to take over could be lost if it wins
    // the election before this leader replicates it. Two fields rather than a nullable NodeId because
    // Volatile/Interlocked need a reference type or a primitive, not a nullable struct; _transferTarget is
    // only meaningful while the flag is set, and is written before the flag so a reader that sees the flag
    // always sees a valid target.
    private int _transferInProgress;
    private NodeId _transferTarget;
    private long _nextHeartbeat;
    private long _waiterSequence;
    private bool _started;
    private bool _disposed;
    private int _compactionInFlight;
    private int _campaignInFlight;

    /// <summary>Creates a group instance. It does nothing until <see cref="StartAsync"/> is called.</summary>
    /// <remarks>
    /// Takes ownership of <paramref name="wal"/> and disposes it with itself. Nothing else may write to that
    /// log while this group is alive, so sharing one would corrupt it either way.
    /// </remarks>
    public RaftGroupInstance(
        RaftGroupId group,
        NodeId self,
        IRaftWal wal,
        IRaftMetaStore metaStore,
        IRaftProtocolTransport transport,
        IRaftStateMachine? stateMachine = null,
        RaftGroupOptions? options = null,
        TimeProvider? timeProvider = null,
        ILogger? logger = null,
        IRaftSnapshotStore? snapshotStore = null)
    {
        Group = group;
        Self = self;
        _wal = wal ?? throw new ArgumentNullException(nameof(wal));
        _metaStore = metaStore ?? throw new ArgumentNullException(nameof(metaStore));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _stateMachine = stateMachine ?? NullRaftStateMachine.Instance;
        _snapshots = snapshotStore;
        _options = options ?? new RaftGroupOptions();
        _time = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger.Instance;
    }

    /// <summary>Identity of this group.</summary>
    public RaftGroupId Group { get; }

    /// <summary>Identity of the node this instance runs on.</summary>
    public NodeId Self { get; }

    /// <summary>Current role. Read without a lock; it is a single <see cref="int"/>.</summary>
    public RaftRole Role => (RaftRole)Volatile.Read(ref _role);

    /// <summary>Current term.</summary>
    public long CurrentTerm => Volatile.Read(ref _currentTerm);

    /// <summary>Highest index known to be committed — replicated to a majority of voters.</summary>
    public long CommitIndex => Volatile.Read(ref _commitIndex);

    /// <summary>Highest index applied to the state machine.</summary>
    public long LastApplied => Volatile.Read(ref _lastApplied);

    /// <summary>Highest index in the local log, committed or not.</summary>
    public long LastIndex => _wal.LastIndex;

    /// <summary>Lowest index still retained. Above 1 once a snapshot has compacted the log's prefix away.</summary>
    public long FirstIndex => _wal.FirstIndex;

    /// <summary>Who this node currently believes is leader, if anyone.</summary>
    public NodeId? LeaderId => _leaderId;

    /// <summary>Current configuration. Immutable; replaced wholesale when a membership entry is appended.</summary>
    public RaftMembership Membership => Volatile.Read(ref _membership);

    /// <summary>True when this node is the leader of this group.</summary>
    public bool IsLeader => Role == RaftRole.Leader;

    /// <summary>
    /// Whether a leader has been heard from recently enough to still be believed in. False on a replica that
    /// has been cut off, and the honest answer to "can this node still reach the group".
    /// </summary>
    /// <remarks>
    /// Driven by the same deadline the pre-vote check uses, and deliberately not by the election timer: the
    /// election timer is also reset by campaigning and by granting a vote, neither of which is evidence that
    /// a leader exists.
    /// </remarks>
    public bool HasLeaderContact => _time.GetTimestamp() < Volatile.Read(ref _leaderContactDeadline);

    /// <summary>
    /// Whether a peer has replied within <paramref name="window"/>. Meaningful on a leader only — nobody else
    /// tracks per-peer contact — and false for anyone who is not a peer of this node.
    /// </summary>
    public bool HasRecentContact(NodeId node, TimeSpan window)
        => _peers.TryGetValue(node, out PeerReplicationState? peer)
           && _time.GetTimestamp() - Volatile.Read(ref peer.LastContact) <= ToTicks(window);

    /// <summary>
    /// Recovers persisted term and vote, adopts <paramref name="membership"/> and arms the election timer.
    /// </summary>
    public async ValueTask StartAsync(RaftMembership membership, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(membership);
        await _stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_started)
            {
                return;
            }

            RaftMeta meta = await _metaStore.ReadAsync(Group, cancellationToken).ConfigureAwait(false);
            _currentTerm = meta.Term;
            _votedFor = meta.VotedFor;
            Volatile.Write(ref _membership, membership);

            // The log is replayed for membership entries before anything else: a configuration is in force
            // from the moment it was appended, so recovering it from committed state alone would resurrect a
            // node that had already been removed.
            await ReplayMembershipAsync(cancellationToken).ConfigureAwait(false);

            Volatile.Write(ref _role, (int)(membership.IsVoter(Self) ? RaftRole.Follower : RaftRole.Learner));
            ArmElectionTimer();
            _started = true;
            Log.GroupStarted(_logger, Group.Value, Self.Value, _currentTerm, _wal.LastIndex);
        }
        finally
        {
            _stateGate.Release();
        }
    }

    /// <summary>
    /// Appends a client command and returns once the group's <see cref="DurabilityLevel"/> is satisfied.
    /// Leader only; a follower gets <see cref="NotLeaderException"/> naming the leader it knows of, so the
    /// caller can redirect instead of retrying blindly.
    /// </summary>
    /// <param name="payload">The command itself. Opaque to this library.</param>
    /// <param name="applicationTag">
    /// Opaque to this library, carried unread into <see cref="RaftEntryHeader.ApplicationTag"/> — for
    /// Blun.MQ, a priority bucket or SMQ due-time bucket, so a priority-index rebuild can use
    /// <see cref="IRaftWal.ReadHeadersFromAsync"/> instead of deserializing every payload.
    /// </param>
    /// <returns>The index the command was assigned.</returns>
    public ValueTask<long> AppendAsync(
        ReadOnlyMemory<byte> payload,
        byte applicationTag = 0,
        CancellationToken cancellationToken = default)
        => AppendCoreAsync(RaftEntryKind.Command, payload, applicationTag, cancellationToken);

    /// <summary>Adds a node as a learner. Always safe: learners do not vote, so the quorum does not move.</summary>
    public async ValueTask AddLearnerAsync(NodeId node, CancellationToken cancellationToken = default)
        => await AppendMembershipAsync(new MembershipChange(MembershipChangeKind.AddLearner, node), cancellationToken)
            .ConfigureAwait(false);

    /// <summary>
    /// Promotes a learner to voter, but only once its log is within
    /// <see cref="RaftGroupOptions.PromotionCatchUpThreshold"/> of the leader's — promoting a replica that
    /// cannot yet serve reads hands the quorum a member that will only slow it down.
    /// </summary>
    /// <returns><see langword="false"/> when the node is not caught up enough yet; retry later.</returns>
    public async ValueTask<bool> PromoteToVoterAsync(NodeId node, CancellationToken cancellationToken = default)
    {
        if (!IsLeader)
        {
            throw new NotLeaderException(Group, _leaderId);
        }

        if (_peers.TryGetValue(node, out PeerReplicationState? peer)
            && _wal.LastIndex - peer.MatchIndex > _options.PromotionCatchUpThreshold)
        {
            Log.PromotionDeferred(_logger, Group.Value, node.Value, peer.MatchIndex, _wal.LastIndex);
            return false;
        }

        await AppendMembershipAsync(new MembershipChange(MembershipChangeKind.PromoteToVoter, node), cancellationToken)
            .ConfigureAwait(false);
        return true;
    }

    /// <summary>Removes a node from the group.</summary>
    public async ValueTask RemoveNodeAsync(NodeId node, CancellationToken cancellationToken = default)
        => await AppendMembershipAsync(new MembershipChange(MembershipChangeKind.Remove, node), cancellationToken)
            .ConfigureAwait(false);

    /// <summary>
    /// Transfers leadership to <paramref name="target"/> (Raft §3.10), or to whichever voter's log is
    /// closest to this leader's if none is named. Returns once the target has been told to take over;
    /// the transfer itself completes as an ordinary election a moment later.
    /// </summary>
    /// <remarks>
    /// The steps, in order: stop admitting new client writes, wait for the target's log to actually match
    /// this leader's, hand it a <c>TimeoutNow</c> so it campaigns without waiting out its timer, then step
    /// aside. Skipping the "wait to catch up" step is the classic way to get this wrong — a target sent
    /// into an election before its log matches could lose committed entries were it to win, because Raft's
    /// election restriction (§5.4.1) would then be voting for a log that is not actually the most complete
    /// one.
    /// </remarks>
    /// <returns>
    /// <see langword="false"/> when the target could not be caught up or declined the handoff; the caller
    /// remains leader and may retry. <see langword="true"/> once this node has stepped aside.
    /// </returns>
    public async ValueTask<bool> TransferLeadershipAsync(
        NodeId? target = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsLeader)
        {
            throw new NotLeaderException(Group, _leaderId);
        }

        RaftMembership membership = Membership;
        NodeId chosen = target ?? PickTransferee(membership);

        if (chosen == Self)
        {
            // Already leader and already the best choice: nothing to do, and nothing to fail either.
            return true;
        }

        if (!membership.IsVoter(chosen))
        {
            throw new ArgumentException("Leadership can only transfer to a voting member.", nameof(target));
        }

        long term = CurrentTerm;

        // The flag is claimed first, by CAS, and only the caller that wins it may then write the target —
        // so a second concurrent call can never overwrite the winner's target with its own.
        if (Interlocked.CompareExchange(ref _transferInProgress, 1, 0) != 0)
        {
            // A transfer to someone else is already in flight; refuse to start a second one rather than
            // leaving it ambiguous which target the eventual TimeoutNow was meant for.
            throw new InvalidOperationException(
                "A leadership transfer to " + _transferTarget + " is already in progress.");
        }

        _transferTarget = chosen;

        try
        {
            if (!await WaitForTransfereeToCatchUpAsync(chosen, cancellationToken).ConfigureAwait(false))
            {
                Log.TransferAbandoned(_logger, Group.Value, chosen.Value, "target did not catch up in time");
                return false;
            }

            if (!IsLeader || CurrentTerm != term)
            {
                // Overtaken by an ordinary election while waiting — nothing left to transfer.
                return false;
            }

            TimeoutNowResponse response = await _transport
                .TimeoutNowAsync(chosen, new TimeoutNowRequest(Group, term), cancellationToken)
                .ConfigureAwait(false);

            if (!response.Accepted)
            {
                Log.TransferAbandoned(_logger, Group.Value, chosen.Value, "target declined");
                return false;
            }

            await StepAsideAsync(cancellationToken).ConfigureAwait(false);
            Log.LeadershipTransferred(_logger, Group.Value, Self.Value, chosen.Value, term);
            return true;
        }
        catch (IOException ex)
        {
            // An unreachable target is a normal failure of this operation, not of the group: the caller
            // stays leader and can pick a different target or try again later.
            Log.TransferAbandoned(_logger, Group.Value, chosen.Value, ex.Message);
            return false;
        }
        finally
        {
            Volatile.Write(ref _transferInProgress, 0);
        }
    }

    /// <summary>Handles an inbound <c>TimeoutNow</c> by starting an election immediately.</summary>
    public async ValueTask<TimeoutNowResponse> OnTimeoutNowAsync(
        TimeoutNowRequest request,
        CancellationToken cancellationToken = default)
    {
        if (request.Term < CurrentTerm || !Membership.IsVoter(Self))
        {
            return new TimeoutNowResponse(CurrentTerm, Accepted: false);
        }

        long term = CurrentTerm;

        // Accepted first, campaign after: the sender is stepping aside on the strength of this reply, so it
        // must not be left waiting on an election that runs long before it learns the handoff was received.
        // Pre-vote is skipped: every other voter's pre-vote check would otherwise refuse this candidate for
        // up to a full election timeout, still believing the outgoing leader is alive from its last
        // heartbeat, even though the transfer has already made a new election legitimate.
        _ = CampaignAsync(skipPreVote: true, cancellationToken).AsTask();
        return new TimeoutNowResponse(term, Accepted: true);
    }

    /// <summary>
    /// Picks the voter whose log is closest to this leader's — the one an election restriction check
    /// (Raft §5.4.1) is likeliest to let win outright, so the transfer costs one round rather than several.
    /// </summary>
    private NodeId PickTransferee(RaftMembership membership)
    {
        NodeId? best = null;
        long bestMatch = -1;
        foreach (NodeId voter in membership.Voters)
        {
            if (voter == Self)
            {
                continue;
            }

            long match = _peers.TryGetValue(voter, out PeerReplicationState? peer) ? peer.MatchIndex : 0;
            if (match > bestMatch)
            {
                bestMatch = match;
                best = voter;
            }
        }

        return best ?? throw new InvalidOperationException("No other voter to transfer leadership to.");
    }

    /// <summary>
    /// Whether a majority of voters has been heard from within one election timeout. Leader-side only.
    /// </summary>
    private bool HasQuorumContact(long now)
    {
        RaftMembership membership = Membership;
        ImmutableArray<NodeId> voters = membership.Voters;
        if (voters.Length <= 1)
        {
            // A single-voter group is its own majority, and there is nobody whose silence could mean
            // anything. Notably this is also the whole of single-node mode, which must never step aside.
            return true;
        }

        long window = ToTicks(_options.ElectionTimeout);
        int fresh = membership.IsVoter(Self) ? 1 : 0;

        foreach (NodeId voter in voters)
        {
            if (voter != Self
                && _peers.TryGetValue(voter, out PeerReplicationState? peer)
                && now - Volatile.Read(ref peer.LastContact) <= window)
            {
                fresh++;
            }
        }

        return fresh >= membership.QuorumSize;
    }

    /// <summary>
    /// Answers whether a node could take leadership of this group, and hands it over when asked to.
    /// Leader only — the judgement rests on per-peer match indices, which no other replica has.
    /// </summary>
    /// <remarks>
    /// The check and the act are one operation on purpose. Split apart, the answer could go stale between
    /// them, and every caller would have to reason about a race the leader is already holding the state to
    /// resolve. A caller that only wants to know passes <see cref="LeaderTargetRequest.Execute"/> clear.
    /// <para>
    /// A rejected or failed handover leaves this node leader. There is no path here that ends with the group
    /// unled: <see cref="TransferLeadershipAsync"/> returns <see langword="false"/> rather than stepping
    /// aside when the target does not work out.
    /// </para>
    /// </remarks>
    public async ValueTask<LeaderTargetResponse> EvaluateLeaderTargetAsync(
        LeaderTargetRequest request,
        CancellationToken cancellationToken = default)
    {
        if (!IsLeader)
        {
            return new LeaderTargetResponse(
                _leaderId ?? Self,
                LeaderTargetStatus.NoLeader,
                0,
                Transferred: false);
        }

        RaftMembership membership = Membership;
        NodeId candidate;

        if (request.Candidate is { } named)
        {
            candidate = named;
        }
        else if (TryPickTransferee(membership, out NodeId picked))
        {
            candidate = picked;
        }
        else
        {
            // Nobody else votes in this group, so there is no target and never will be while the
            // configuration stays as it is. Reported as "not a member" against this node itself rather than
            // invented, so the caller sees a fact instead of a guess.
            return new LeaderTargetResponse(Self, LeaderTargetStatus.NotAMember, 0, Transferred: false);
        }

        if (candidate == Self)
        {
            return new LeaderTargetResponse(Self, LeaderTargetStatus.IsCurrentLeader, 0, Transferred: false);
        }

        if (!membership.Contains(candidate))
        {
            return new LeaderTargetResponse(candidate, LeaderTargetStatus.NotAMember, 0, Transferred: false);
        }

        if (!membership.IsVoter(candidate))
        {
            return new LeaderTargetResponse(candidate, LeaderTargetStatus.NotAVoter, 0, Transferred: false);
        }

        long match = _peers.TryGetValue(candidate, out PeerReplicationState? peer) ? peer.MatchIndex : 0;
        long lag = Math.Max(0, _wal.LastIndex - match);

        if (lag > LeaderTargetLagThreshold)
        {
            return new LeaderTargetResponse(candidate, LeaderTargetStatus.Lagging, lag, Transferred: false);
        }

        // "Fully in sync and healthy" is two claims, and the match index only supports the first. A node that
        // died a moment ago still has a match index close to this one's -- on a quiet queue, indistinguishable
        // from a live replica -- so lag alone would report it as a legal target. The handover would then stop
        // accepting writes and wait out LeadershipTransferCatchUpTimeout against a node that will never
        // answer. The group keeps its leader regardless; the stall is what this avoids.
        if (!HasRecentContact(candidate, LeaderTargetContactWindow))
        {
            return new LeaderTargetResponse(candidate, LeaderTargetStatus.NotResponding, lag, Transferred: false);
        }

        if (Volatile.Read(ref _transferInProgress) != 0)
        {
            return new LeaderTargetResponse(candidate, LeaderTargetStatus.TransferInFlight, lag, Transferred: false);
        }

        if (!request.Execute)
        {
            return new LeaderTargetResponse(candidate, LeaderTargetStatus.Valid, lag, Transferred: false);
        }

        bool transferred = await TransferLeadershipAsync(candidate, cancellationToken).ConfigureAwait(false);
        return new LeaderTargetResponse(candidate, LeaderTargetStatus.Valid, lag, transferred);
    }

    /// <summary>Effective lag tolerance for a leadership target: one replication round unless configured.</summary>
    private long LeaderTargetLagThreshold => _options.LeaderTargetLagThreshold ?? _options.MaxEntriesPerAppend;

    /// <summary>Effective liveness window for a leadership target: twice the election timeout unless configured.</summary>
    private TimeSpan LeaderTargetContactWindow
        => _options.LeaderTargetContactWindow ?? TimeSpan.FromTicks(_options.ElectionTimeout.Ticks * 2);

    /// <summary>The non-throwing half of <see cref="PickTransferee"/>, for the query path.</summary>
    /// <remarks>
    /// A responding peer wins over a silent one even when the silent one's match index is higher. That index
    /// is only a record of the last thing the node admitted to; once it has stopped answering it stops being
    /// evidence, and picking on it would nominate the deadest node in the group whenever it happened to die
    /// furthest ahead. When nothing is responding the best match index is still returned rather than nothing,
    /// so the caller is told <see cref="LeaderTargetStatus.NotResponding"/> about a real node instead of
    /// "no member" about the group.
    /// </remarks>
    private bool TryPickTransferee(RaftMembership membership, out NodeId chosen)
    {
        TimeSpan window = LeaderTargetContactWindow;
        NodeId? best = null;
        NodeId? bestLive = null;
        long bestMatch = -1;
        long bestLiveMatch = -1;

        foreach (NodeId voter in membership.Voters)
        {
            if (voter == Self)
            {
                continue;
            }

            long match = _peers.TryGetValue(voter, out PeerReplicationState? peer) ? peer.MatchIndex : 0;
            if (match > bestMatch)
            {
                bestMatch = match;
                best = voter;
            }

            if (match > bestLiveMatch && HasRecentContact(voter, window))
            {
                bestLiveMatch = match;
                bestLive = voter;
            }
        }

        // Node id 0 is legal, so presence decides this, never the value.
        NodeId? picked = bestLive ?? best;
        chosen = picked ?? default;
        return picked is not null;
    }

    /// <summary>
    /// Blocks until the target's replicated log matches this leader's. New writes are already refused by
    /// this point, so the target only has to catch up on what already exists — but "what already exists" can
    /// be a large backlog for a node that just joined or is recovering from a partition, which is why the
    /// deadline is a multiple of the election timeout rather than the timeout itself.
    /// </summary>
    private async ValueTask<bool> WaitForTransfereeToCatchUpAsync(NodeId target, CancellationToken cancellationToken)
    {
        long targetIndex = _wal.LastIndex;
        TimeSpan timeout = _options.LeadershipTransferCatchUpTimeout
            ?? TimeSpan.FromTicks(_options.ElectionTimeout.Ticks * 10);
        long deadline = _time.GetTimestamp() + ToTicks(timeout);

        while (_time.GetTimestamp() < deadline)
        {
            if (_peers.TryGetValue(target, out PeerReplicationState? peer) && peer.MatchIndex >= targetIndex)
            {
                return true;
            }

            // Drive replication rather than only watching it. Waiting on the heartbeat quantizes catch-up to
            // HeartbeatInterval per round, which is fine for ordinary replication and wrong here: writes are
            // already blocked, so every round this waits for is dead time inside a window that is bounded.
            // It matters most in exactly the case that needs the most rounds — a target so far behind it
            // needs a snapshot first and the trailing entries after. The call is debounced per peer, so
            // asking on every poll costs nothing when a round is already in flight.
            PushToAllPeers();
            await Task.Delay(5, cancellationToken).ConfigureAwait(false);
        }

        return _peers.TryGetValue(target, out PeerReplicationState? final) && final.MatchIndex >= targetIndex;
    }

    /// <summary>
    /// Steps down without changing term or vote — unlike <see cref="StepDownAsync"/>, which is a response to
    /// discovering a higher term. A transfer hands leadership to a successor still in this same term, so
    /// there is nothing stale about the term or the vote this node cast in it.
    /// </summary>
    private async ValueTask StepAsideAsync(CancellationToken cancellationToken)
    {
        await _stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!IsLeader)
            {
                return;
            }

            Volatile.Write(ref _role, (int)(Membership.IsVoter(Self) ? RaftRole.Follower : RaftRole.Learner));
            _leaderId = null;
            _peers.Clear();
            ArmElectionTimer();
        }
        finally
        {
            _stateGate.Release();
        }
    }

    /// <summary>
    /// Prepares a linearizable read and returns the index the caller must wait for. On a leader this confirms
    /// leadership with a quorum first; on a follower it asks the leader (Raft §6.4).
    /// </summary>
    /// <remarks>
    /// Nothing is appended and nothing is fsynced. That is the entire point: without it, either every read
    /// goes to the leader — the bottleneck multi-Raft exists to spread — or reads are served from possibly
    /// stale followers, which for a queue is not "slightly old data" but duplicate deliveries and lost
    /// settlements.
    /// </remarks>
    public async ValueTask<long> ReadIndexAsync(CancellationToken cancellationToken = default)
    {
        if (IsLeader)
        {
            return await LeaderReadIndexAsync(cancellationToken).ConfigureAwait(false);
        }

        NodeId leader = _leaderId ?? throw new NotLeaderException(Group, null);
        ReadIndexResponse response = await _transport
            .ReadIndexAsync(leader, new ReadIndexRequest(Group, CurrentTerm), cancellationToken)
            .ConfigureAwait(false);

        if (!response.Success)
        {
            throw new NotLeaderException(Group, response.Leader == 0 ? null : new NodeId(response.Leader));
        }

        return response.ReadIndex;
    }

    /// <summary>Blocks until this replica has applied <paramref name="index"/>, so a read at it is safe.</summary>
    public async ValueTask WaitForAppliedAsync(long index, CancellationToken cancellationToken = default)
    {
        while (Volatile.Read(ref _lastApplied) < index)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await ApplyCommittedAsync().ConfigureAwait(false);
            if (Volatile.Read(ref _lastApplied) >= index)
            {
                return;
            }

            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The two halves together: obtain a read index and wait until this replica has caught up to it. After
    /// this returns, reading local state is linearizable.
    /// </summary>
    public async ValueTask<long> PrepareLinearizableReadAsync(CancellationToken cancellationToken = default)
    {
        long index = await ReadIndexAsync(cancellationToken).ConfigureAwait(false);
        await WaitForAppliedAsync(index, cancellationToken).ConfigureAwait(false);
        return index;
    }

    /// <summary>Handles an inbound <c>ReadIndex</c> from a follower.</summary>
    public async ValueTask<ReadIndexResponse> OnReadIndexAsync(
        ReadIndexRequest request,
        CancellationToken cancellationToken = default)
    {
        if (!IsLeader)
        {
            return new ReadIndexResponse(CurrentTerm, Success: false, 0, _leaderId?.Value ?? 0);
        }

        try
        {
            long index = await LeaderReadIndexAsync(cancellationToken).ConfigureAwait(false);
            return new ReadIndexResponse(CurrentTerm, Success: true, index, Self.Value);
        }
        catch (NotLeaderException)
        {
            return new ReadIndexResponse(CurrentTerm, Success: false, 0, _leaderId?.Value ?? 0);
        }
    }

    /// <summary>
    /// Drives election and heartbeat timing. Called by the host's shared timer rather than by a timer per
    /// group: with thousands of groups, per-group timers are the dominant cost long before consensus is.
    /// </summary>
    public async ValueTask TickAsync(CancellationToken cancellationToken = default)
    {
        if (!_started || _disposed)
        {
            return;
        }

        long now = _time.GetTimestamp();
        if (Role == RaftRole.Leader)
        {
            // Checked before the heartbeat, not after: if this leader has already lost touch with the
            // majority, the last thing worth doing is sending another round of heartbeats as though it had
            // not. Stepping aside costs nothing when the partition heals — an election follows, and this node
            // is as eligible as anyone.
            if (_options.CheckQuorum && !HasQuorumContact(now))
            {
                Log.QuorumContactLost(_logger, Group.Value, Self.Value, CurrentTerm);
                await StepAsideAsync(cancellationToken).ConfigureAwait(false);
                return;
            }

            if (now >= Volatile.Read(ref _nextHeartbeat))
            {
                Volatile.Write(ref _nextHeartbeat, now + ToTicks(_options.HeartbeatInterval));
                PushToAllPeers();
            }

            return;
        }

        // Learners never campaign: they have no vote to cast and winning would be meaningless.
        if (Role != RaftRole.Learner && now >= Volatile.Read(ref _electionDeadline))
        {
            StartCampaign();
        }

        await ValueTask.CompletedTask.ConfigureAwait(false);
    }

    /// <summary>
    /// Queues an election rather than running one on the caller's stack.
    /// </summary>
    /// <remarks>
    /// The caller is the host's shared tick loop, and it awaits each group in turn. Campaigning inline puts
    /// the whole of <see cref="CampaignAsync(CancellationToken)"/> on that stack — including
    /// <see cref="BecomeLeaderAsync"/>, which appends the term's no-op and, at
    /// <see cref="DurabilityLevel.Quorum"/>, waits for it to commit. A winner that cannot reach a majority
    /// therefore stops the clock for <em>every</em> group on the node: no heartbeats anywhere, and the loop
    /// never gets back to observing shutdown. Observed as a test run that sat idle indefinitely, with the
    /// tick loop parked in <c>WaitForCommitAsync</c>.
    /// <para>
    /// This is the same fix, for the same reason, that peer replication and automatic compaction already
    /// carry: work whose duration is set by the rest of the cluster does not belong on the clock's stack.
    /// The election deadline is re-armed by the caller before queuing, so a tick that fires while a campaign
    /// is still running does not see the same expired deadline again, and the in-flight flag makes sure only
    /// one runs at a time.
    /// </para>
    /// </remarks>
    private void StartCampaign()
    {
        // Re-armed before queuing, not inside the campaign: otherwise the next tick arrives before
        // CampaignAsync has run far enough to move the deadline, and fires a second one behind this.
        ArmElectionTimer();

        if (Interlocked.CompareExchange(ref _campaignInFlight, 1, 0) != 0)
        {
            return;
        }

        _ = Task.Run(
            async () =>
            {
                try
                {
                    await CampaignAsync(skipPreVote: false, _shutdown.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Shutting down.
                }
                catch (Exception ex) when (ex is IOException or InvalidOperationException)
                {
                    Log.ElectionFailed(_logger, ex, Group.Value, Self.Value);
                }
                finally
                {
                    Volatile.Write(ref _campaignInFlight, 0);
                }
            },
            _shutdown.Token);
    }

    /// <summary>Starts an election immediately, skipping the timer. Used for bootstrap and for leader transfer.</summary>
    public ValueTask<bool> CampaignAsync(CancellationToken cancellationToken = default)
        => CampaignAsync(skipPreVote: false, cancellationToken);

    /// <summary>
    /// The <paramref name="skipPreVote"/> overload exists for exactly one caller: a candidacy triggered by
    /// <c>TimeoutNow</c> during a leadership transfer. Every other voter's pre-vote check refuses a grant
    /// while it still believes a leader is alive, and that belief is driven by when it last heard a
    /// heartbeat — which lags behind the outgoing leader actually stepping aside by up to a full election
    /// timeout. Without skipping pre-vote here, a deliberate handoff would stall for that whole window
    /// despite every voter already knowing, from the transfer itself, that a new election is legitimate.
    /// </summary>
    private async ValueTask<bool> CampaignAsync(bool skipPreVote, CancellationToken cancellationToken)
    {
        RaftMembership membership = Membership;
        if (!membership.IsVoter(Self))
        {
            return false;
        }

        ArmElectionTimer();

        // Pre-vote first: ask whether an election could be won without raising the term. A node that was
        // partitioned away comes back with a term far ahead of the group's, and without this it forces every
        // healthy member to step down for no reason.
        if (!skipPreVote && _options.PreVote && membership.Voters.Length > 1)
        {
            bool viable = await RunVoteRoundAsync(membership, CurrentTerm + 1, preVote: true, cancellationToken)
                .ConfigureAwait(false);
            if (!viable)
            {
                return false;
            }
        }

        long term;
        await _stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            term = ++_currentTerm;
            _votedFor = Self;
            _leaderId = null;
            Volatile.Write(ref _role, (int)RaftRole.Candidate);

            // Persisted before a single vote request goes out: if this write is lost, the node can vote twice
            // in one term after a restart, and two leaders in one term is the one failure Raft must not have.
            await _metaStore.WriteAsync(Group, new RaftMeta(term, Self), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _stateGate.Release();
        }

        Log.ElectionStarted(_logger, Group.Value, Self.Value, term);
        bool won = await RunVoteRoundAsync(membership, term, preVote: false, cancellationToken).ConfigureAwait(false);
        if (won && CurrentTerm == term && Role == RaftRole.Candidate)
        {
            await BecomeLeaderAsync(term, cancellationToken).ConfigureAwait(false);
            return true;
        }

        return false;
    }

    /// <summary>Handles an inbound <c>AppendEntries</c>, including heartbeats.</summary>
    public async ValueTask<AppendEntriesResponse> OnAppendEntriesAsync(
        AppendEntriesRequest request,
        IAsyncEnumerable<RaftLogEntry> entries,
        CancellationToken cancellationToken = default)
    {
        await _stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (request.Term < _currentTerm)
            {
                return new AppendEntriesResponse(_currentTerm, Success: false, MatchIndex: 0, ConflictIndex: 0);
            }

            if (request.Term > _currentTerm)
            {
                await StepDownAsync(request.Term, cancellationToken).ConfigureAwait(false);
            }

            Volatile.Write(ref _role, (int)(Membership.IsVoter(Self) ? RaftRole.Follower : RaftRole.Learner));
            _leaderId = request.Leader;
            ArmElectionTimer();
            Volatile.Write(ref _leaderContactDeadline, _time.GetTimestamp() + ToTicks(_options.ElectionTimeout));

            long localPrevTerm = await _wal.TermAtAsync(request.PrevLogIndex, cancellationToken).ConfigureAwait(false);
            if (localPrevTerm != request.PrevLogTerm)
            {
                // Hand back where to resume rather than letting the leader walk back one index per round trip.
                long hint = localPrevTerm < 0
                    ? Math.Max(_wal.LastIndex + 1, _wal.FirstIndex)
                    : await FirstIndexOfTermAsync(localPrevTerm, request.PrevLogIndex, cancellationToken)
                        .ConfigureAwait(false);
                return new AppendEntriesResponse(_currentTerm, Success: false, MatchIndex: 0, ConflictIndex: hint);
            }

            long match = await AbsorbEntriesAsync(request, entries, cancellationToken).ConfigureAwait(false);

            if (request.LeaderCommit > Volatile.Read(ref _commitIndex))
            {
                Volatile.Write(ref _commitIndex, Math.Min(request.LeaderCommit, _wal.LastIndex));
            }

            return new AppendEntriesResponse(_currentTerm, Success: true, match, ConflictIndex: 0);
        }
        finally
        {
            _stateGate.Release();
            _ = ApplyCommittedAsync();
        }
    }

    /// <summary>Handles an inbound <c>RequestVote</c> or pre-vote probe.</summary>
    public async ValueTask<VoteResponse> OnRequestVoteAsync(
        VoteRequest request,
        CancellationToken cancellationToken = default)
    {
        await _stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // A node that does not believe itself a voter grants nothing. Defence in depth: a correct
            // candidate only ever polls the voters it knows of, so this should be unreachable — but the
            // safety of an election rests on the candidate's view of the configuration, and this is the one
            // place the responder's own view can also be made to count. It can only ever withhold a vote,
            // never add one, so a replica that has not yet caught up on its own promotion merely fails to
            // help rather than doing damage. The same guard already sits on TimeoutNow.
            if (!Membership.IsVoter(Self))
            {
                return new VoteResponse(_currentTerm, Granted: false);
            }

            bool logOk = await IsCandidateLogCurrentAsync(request, cancellationToken).ConfigureAwait(false);

            if (request.PreVote)
            {
                // A pre-vote changes nothing on this node — no term bump, no persisted vote. It is refused
                // while a leader is still being heard from, which is precisely what stops a rejoining node
                // from disrupting a working group.
                //
                // A leader counts itself as that leader. Nothing sets _leaderContactDeadline on the node that
                // IS the leader — it is written when a leader is heard *from* — so without the first clause a
                // healthy leader reads its own deadline as long expired and cheerfully grants a pre-vote
                // against itself. The candidate then wins its pre-vote round on the leader's own grant, bumps
                // the term for real, and the leader steps down for a challenger it had no reason to yield to.
                // A deliberate handover is unaffected: it goes through TimeoutNow with skipPreVote, which is
                // exactly why that flag exists.
                bool leaderAlive = IsLeader
                    || _time.GetTimestamp() < Volatile.Read(ref _leaderContactDeadline);
                bool grant = logOk && request.Term > _currentTerm && !leaderAlive;
                return new VoteResponse(_currentTerm, grant);
            }

            if (request.Term < _currentTerm)
            {
                return new VoteResponse(_currentTerm, Granted: false);
            }

            if (request.Term > _currentTerm)
            {
                await StepDownAsync(request.Term, cancellationToken).ConfigureAwait(false);
            }

            bool free = _votedFor is null || _votedFor.Value == request.Candidate;
            if (!free || !logOk)
            {
                return new VoteResponse(_currentTerm, Granted: false);
            }

            _votedFor = request.Candidate;

            // The vote hits disk before it is granted. Anything less and a crash-restart can grant a second
            // vote in the same term.
            await _metaStore.WriteAsync(Group, new RaftMeta(_currentTerm, _votedFor), cancellationToken)
                .ConfigureAwait(false);
            ArmElectionTimer();
            return new VoteResponse(_currentTerm, Granted: true);
        }
        finally
        {
            _stateGate.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        await _shutdown.CancelAsync().ConfigureAwait(false);

        foreach (CommitWaiter waiter in _commitWaiters.Values)
        {
            waiter.Completion.TrySetCanceled();
        }

        _commitWaiters.Clear();

        // The log is disposed here because this instance owns it: it is handed one at construction, nothing
        // else may write to it while the group lives, and the two lifetimes are the same. Leaving it open
        // leaked a file handle per group on every host shutdown — on Windows that keeps the segment files
        // locked, so a group's directory could not be removed afterwards, and on Unix it is a quieter
        // descriptor leak that only shows up at scale.
        await _wal.DisposeAsync().ConfigureAwait(false);

        _shutdown.Dispose();
        _stateGate.Dispose();
        _applyGate.Dispose();
    }

    private async ValueTask<long> AppendCoreAsync(
        RaftEntryKind kind,
        ReadOnlyMemory<byte> payload,
        byte applicationTag,
        CancellationToken cancellationToken)
    {
        if (!IsLeader)
        {
            throw new NotLeaderException(Group, _leaderId);
        }

        if (Volatile.Read(ref _transferInProgress) != 0)
        {
            throw new LeadershipTransferInProgressException(Group, _transferTarget);
        }

        long index;
        long term;
        await _stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!IsLeader)
            {
                throw new NotLeaderException(Group, _leaderId);
            }

            if (Volatile.Read(ref _transferInProgress) != 0)
            {
                throw new LeadershipTransferInProgressException(Group, _transferTarget);
            }

            term = _currentTerm;
            index = _wal.LastIndex + 1;
            var header = new RaftEntryHeader(term, index, kind, payload.Length, _time.GetUtcNow().UtcTicks, applicationTag);
            await _wal.AppendAsync(header, payload, cancellationToken).ConfigureAwait(false);

            if (kind == RaftEntryKind.Membership)
            {
                // A configuration takes effect when appended, not when committed (Raft §4.1). Deferring it to
                // commit is the classic way to end up counting a removed node in the quorum that removes it.
                ApplyMembership(MembershipChange.Read(payload.Span));
            }
        }
        finally
        {
            _stateGate.Release();
        }

        if (_options.Durability != DurabilityLevel.LeaderBuffered)
        {
            await _wal.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        // The push is started, not awaited: the leader does not sit behind the slowest follower. Quorum is
        // still required for the commit below, but it is discovered from replies as they land.
        PushToAllPeers();
        await AdvanceCommitIndexAsync().ConfigureAwait(false);

        if (_options.Durability == DurabilityLevel.Quorum)
        {
            await WaitForCommitAsync(index, cancellationToken).ConfigureAwait(false);
        }

        return index;
    }

    private async ValueTask AppendMembershipAsync(MembershipChange change, CancellationToken cancellationToken)
    {
        byte[] payload = change.ToPayload();
        await AppendCoreAsync(RaftEntryKind.Membership, payload, applicationTag: 0, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Consumes the streamed entries, reconciling any divergence, and returns the new match index.
    /// Entries are buffered into one pooled block and written as a single batch: a replication round should
    /// cost one write, and the payloads handed over by the transport are only valid for one iteration anyway.
    /// </summary>
    private async ValueTask<long> AbsorbEntriesAsync(
        AppendEntriesRequest request,
        IAsyncEnumerable<RaftLogEntry> entries,
        CancellationToken cancellationToken)
    {
        long match = request.PrevLogIndex;
        byte[] buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        var batch = new List<RaftLogEntry>();
        int used = 0;

        try
        {
            await foreach (RaftLogEntry entry in entries.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (entry.Index <= _wal.LastIndex)
                {
                    long existing = await _wal.TermAtAsync(entry.Index, cancellationToken).ConfigureAwait(false);
                    if (existing == entry.Term)
                    {
                        match = entry.Index;
                        continue;
                    }

                    // Divergence: the leader's log wins, so everything from here down goes. Whatever is
                    // already buffered belongs after the cut, so it has to be flushed first.
                    await FlushBatchAsync(batch, cancellationToken).ConfigureAwait(false);
                    used = 0;
                    await _wal.TruncateTailAsync(entry.Index, cancellationToken).ConfigureAwait(false);
                }

                if (used + entry.Payload.Length > buffer.Length)
                {
                    await FlushBatchAsync(batch, cancellationToken).ConfigureAwait(false);
                    used = 0;
                    if (entry.Payload.Length > buffer.Length)
                    {
                        ArrayPool<byte>.Shared.Return(buffer);
                        buffer = ArrayPool<byte>.Shared.Rent(entry.Payload.Length);
                    }
                }

                entry.Payload.CopyTo(buffer.AsMemory(used));
                batch.Add(new RaftLogEntry(in entry.Header, buffer.AsMemory(used, entry.Payload.Length)));
                used += entry.Payload.Length;
                match = entry.Index;
            }

            await FlushBatchAsync(batch, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        // A follower's acknowledgement is a durability claim, so it fsyncs before it makes one.
        if (_options.Durability != DurabilityLevel.LeaderBuffered)
        {
            await _wal.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        return match;
    }

    private async ValueTask FlushBatchAsync(List<RaftLogEntry> batch, CancellationToken cancellationToken)
    {
        if (batch.Count == 0)
        {
            return;
        }

        RaftLogEntry[] items = [.. batch];
        await _wal.AppendAsync(items, cancellationToken).ConfigureAwait(false);

        foreach (RaftLogEntry entry in items)
        {
            if (entry.Kind == RaftEntryKind.Membership)
            {
                ApplyMembership(MembershipChange.Read(entry.Payload.Span));
            }
        }

        batch.Clear();
    }

    private async ValueTask<bool> RunVoteRoundAsync(
        RaftMembership membership,
        long term,
        bool preVote,
        CancellationToken cancellationToken)
    {
        int needed = membership.QuorumSize;
        int granted = 1; // itself
        if (granted >= needed)
        {
            return true;
        }

        var request = new VoteRequest(Group, term, Self, _wal.LastIndex, _wal.LastTerm, preVote);
        var ballots = new List<Task<VoteResponse>>(membership.Voters.Length);

        foreach (NodeId voter in membership.Voters)
        {
            if (voter == Self)
            {
                continue;
            }

            ballots.Add(SendVoteAsync(voter, request, cancellationToken));
        }

        while (ballots.Count > 0)
        {
            Task<VoteResponse> finished = await Task.WhenAny(ballots).ConfigureAwait(false);
            ballots.Remove(finished);

            VoteResponse response = finished.IsCompletedSuccessfully
                ? finished.Result
                : new VoteResponse(0, Granted: false);

            if (!preVote && response.Term > CurrentTerm)
            {
                await _stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    await StepDownAsync(response.Term, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    _stateGate.Release();
                }

                return false;
            }

            if (response.Granted && ++granted >= needed)
            {
                return true;
            }
        }

        return false;
    }

    private async Task<VoteResponse> SendVoteAsync(
        NodeId target,
        VoteRequest request,
        CancellationToken cancellationToken)
    {
        try
        {
            return await _transport.RequestVoteAsync(target, request, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return new VoteResponse(0, Granted: false);
        }
        catch (IOException)
        {
            // An unreachable voter is a normal condition, not an error: it simply does not count.
            return new VoteResponse(0, Granted: false);
        }
    }

    private async ValueTask BecomeLeaderAsync(long term, CancellationToken cancellationToken)
    {
        await _stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_currentTerm != term || Role != RaftRole.Candidate)
            {
                return;
            }

            Volatile.Write(ref _role, (int)RaftRole.Leader);
            _leaderId = Self;

            long next = _wal.LastIndex + 1;
            long now = _time.GetTimestamp();
            _peers.Clear();
            foreach (NodeId member in Membership.AllMembers)
            {
                if (member != Self)
                {
                    _peers[member] = new PeerReplicationState(member, next, now);
                }
            }

            Log.BecameLeader(_logger, Group.Value, Self.Value, term, _wal.LastIndex);
        }
        finally
        {
            _stateGate.Release();
        }

        // The no-op entry is not ceremony: a leader may not commit an entry from an earlier term by counting
        // replicas (Raft §5.4.2), so it commits one of its own first and carries the backlog with it.
        await AppendCoreAsync(RaftEntryKind.NoOp, ReadOnlyMemory<byte>.Empty, applicationTag: 0, cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask StepDownAsync(long newTerm, CancellationToken cancellationToken)
    {
        _currentTerm = newTerm;
        _votedFor = null;
        _leaderId = null;
        Volatile.Write(ref _role, (int)(Membership.IsVoter(Self) ? RaftRole.Follower : RaftRole.Learner));
        _peers.Clear();
        await _metaStore.WriteAsync(Group, new RaftMeta(newTerm, null), cancellationToken).ConfigureAwait(false);
        Log.SteppedDown(_logger, Group.Value, Self.Value, newTerm);
    }

    /// <summary>Kicks a replication round at every peer. Fire-and-forget by design — this is the asymmetric half.</summary>
    private void PushToAllPeers()
    {
        if (!IsLeader)
        {
            return;
        }

        foreach (PeerReplicationState peer in _peers.Values)
        {
            // One outstanding round per peer. Without this, a slow follower accumulates a queue of redundant
            // RPCs, each carrying entries the next one supersedes.
            if (Interlocked.CompareExchange(ref peer.InFlight, 1, 0) != 0)
            {
                peer.Pending = 1;
                continue;
            }

            // Queued rather than started inline. Called synchronously from the tick loop, an in-process
            // transport has no real latency to force a yield, so ReplicateToPeerAsync's whole retry loop —
            // possibly several rounds, if the peer is behind — can otherwise run to completion on the SAME
            // call stack as the tick itself. That holds the tick loop's frame open the entire time, which
            // means it cannot get back to observing shutdown cancellation until replication is done. Handing
            // the work to the pool lets the tick return immediately, so shutdown is never held hostage by
            // however long replication to a peer happens to take.
            _ = Task.Run(() => ReplicateToPeerAsync(peer), _shutdown.Token);
        }
    }

    private async Task ReplicateToPeerAsync(PeerReplicationState peer)
    {
        try
        {
            do
            {
                peer.Pending = 0;
                if (!IsLeader || _shutdown.IsCancellationRequested)
                {
                    return;
                }

                long term = CurrentTerm;
                long prevIndex = peer.NextIndex - 1;
                long prevTerm = await _wal.TermAtAsync(prevIndex, _shutdown.Token).ConfigureAwait(false);

                // Two ways the log can no longer serve this peer, and the term check alone catches only one.
                // A compacted prevIndex answers -1, which is the case that check was written for. But index 0
                // answers 0 — "before the log began" — and that is exactly what a peer which has never been
                // replicated to asks about. So a follower still at NextIndex 1, against a log whose prefix has
                // been compacted away, passes the term check, is sent an empty range, is acknowledged at match
                // 0, and repeats that forever without ever moving. Comparing against FirstIndex is what
                // catches it, and it is not a corner case: it is what every node joining a group that has
                // been running long enough to compact looks like.
                if (prevTerm < 0 || peer.NextIndex < _wal.FirstIndex)
                {
                    // The follower needs entries this leader has already compacted away, so there is nothing
                    // to send: replication has to restart from a snapshot instead of from the log.
                    Log.SnapshotRequired(_logger, Group.Value, peer.Node.Value, peer.NextIndex, _wal.FirstIndex);
                    if (!await SendSnapshotAsync(peer).ConfigureAwait(false))
                    {
                        return;
                    }

                    // A snapshot only carries the peer up to the compaction boundary; everything appended
                    // since is still owed to it. Marking the round pending is what makes `continue` actually
                    // iterate — in a do-while it jumps to the condition, and the flag was cleared at the top
                    // of this body, so without this the loop exits here and the trailing entries wait for the
                    // next heartbeat. That delay is invisible in ordinary replication and decisive during a
                    // leadership transfer, whose catch-up window is bounded.
                    peer.Pending = 1;
                    continue;
                }

                long upto = Math.Min(_wal.LastIndex, prevIndex + _options.MaxEntriesPerAppend);
                var request = new AppendEntriesRequest(
                    Group,
                    term,
                    Self,
                    prevIndex,
                    prevTerm,
                    Volatile.Read(ref _commitIndex));

                AppendEntriesResponse response = await _transport
                    .AppendEntriesAsync(
                        peer.Node,
                        request,
                        _wal.ReadFromAsync(peer.NextIndex, upto, _shutdown.Token),
                        _shutdown.Token)
                    .ConfigureAwait(false);

                // A reply is a liveness proof regardless of what it says: check-quorum counts nodes that are
                // talking, not nodes that are agreeing.
                Volatile.Write(ref peer.LastContact, _time.GetTimestamp());

                if (response.Term > term)
                {
                    await _stateGate.WaitAsync(_shutdown.Token).ConfigureAwait(false);
                    try
                    {
                        if (response.Term > _currentTerm)
                        {
                            await StepDownAsync(response.Term, _shutdown.Token).ConfigureAwait(false);
                        }
                    }
                    finally
                    {
                        _stateGate.Release();
                    }

                    return;
                }

                if (response.Success)
                {
                    peer.MatchIndex = response.MatchIndex;
                    peer.NextIndex = response.MatchIndex + 1;
                    await AdvanceCommitIndexAsync().ConfigureAwait(false);

                    if (peer.NextIndex <= _wal.LastIndex)
                    {
                        peer.Pending = 1;
                    }
                }
                else
                {
                    peer.NextIndex = Math.Max(1, Math.Min(peer.NextIndex - 1, response.ConflictIndex));
                    peer.Pending = 1;
                }
            }
            while (Volatile.Read(ref peer.Pending) == 1);
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
        catch (IOException ex)
        {
            Log.ReplicationFailed(_logger, ex, Group.Value, peer.Node.Value);
        }
        finally
        {
            Volatile.Write(ref peer.InFlight, 0);
        }
    }

    /// <summary>
    /// Captures the applied state, stores it as this group's snapshot, and compacts the log up to it.
    /// </summary>
    /// <remarks>
    /// The order is deliberate and not interchangeable: the snapshot is durable <em>before</em> a single log
    /// entry is discarded. Compacting first would open a window in which the state exists in neither place.
    /// </remarks>
    /// <returns><see langword="false"/> when there is no snapshotable state machine, or nothing to compact.</returns>
    public async ValueTask<bool> TakeSnapshotAsync(CancellationToken cancellationToken = default)
    {
        if (_snapshots is null || _stateMachine is not IRaftSnapshotableStateMachine snapshotable)
        {
            return false;
        }

        long upto = Math.Min(Volatile.Read(ref _lastApplied), Volatile.Read(ref _commitIndex));
        if (upto < _wal.FirstIndex)
        {
            return false;
        }

        long term = await _wal.TermAtAsync(upto, cancellationToken).ConfigureAwait(false);
        if (term < 0)
        {
            return false;
        }

        var metadata = new RaftSnapshotMetadata(upto, term, Membership.Serialize());
        await _snapshots.WriteAsync(
            Group,
            metadata,
            snapshotable.CaptureAsync(Group, upto, cancellationToken),
            cancellationToken).ConfigureAwait(false);

        await _wal.TruncateHeadAsync(upto, cancellationToken).ConfigureAwait(false);
        Log.SnapshotTaken(_logger, Group.Value, upto, term);
        return true;
    }

    /// <summary>Handles an inbound <c>InstallSnapshot</c>.</summary>
    public async ValueTask<InstallSnapshotResponse> OnInstallSnapshotAsync(
        InstallSnapshotRequest request,
        IAsyncEnumerable<ReadOnlyMemory<byte>> body,
        CancellationToken cancellationToken = default)
    {
        await _stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (request.Term < _currentTerm)
            {
                return new InstallSnapshotResponse(_currentTerm, Success: false);
            }

            if (request.Term > _currentTerm)
            {
                await StepDownAsync(request.Term, cancellationToken).ConfigureAwait(false);
            }

            _leaderId = request.Leader;
            ArmElectionTimer();
            Volatile.Write(ref _leaderContactDeadline, _time.GetTimestamp() + ToTicks(_options.ElectionTimeout));

            // Already covered by what this replica has; installing would move it backwards.
            if (request.LastIncludedIndex <= Volatile.Read(ref _commitIndex))
            {
                await DrainAsync(body, cancellationToken).ConfigureAwait(false);
                return new InstallSnapshotResponse(_currentTerm, Success: true);
            }

            if (_snapshots is null || _stateMachine is not IRaftSnapshotableStateMachine snapshotable)
            {
                await DrainAsync(body, cancellationToken).ConfigureAwait(false);
                return new InstallSnapshotResponse(_currentTerm, Success: false);
            }

            var metadata = new RaftSnapshotMetadata(
                request.LastIncludedIndex,
                request.LastIncludedTerm,
                request.Configuration);

            // Stored first, then applied. If the process dies between the two, the snapshot is on disk and
            // recovery can finish the job; the other order could leave the state machine holding state that
            // nothing on disk accounts for.
            await _snapshots.WriteAsync(Group, metadata, body, cancellationToken).ConfigureAwait(false);
            await snapshotable
                .RestoreAsync(Group, _snapshots.ReadAsync(Group, cancellationToken), cancellationToken)
                .ConfigureAwait(false);

            await _wal.ResetToSnapshotAsync(request.LastIncludedIndex, request.LastIncludedTerm, cancellationToken)
                .ConfigureAwait(false);

            Volatile.Write(ref _commitIndex, request.LastIncludedIndex);
            Volatile.Write(ref _lastApplied, request.LastIncludedIndex);

            if (!request.Configuration.IsEmpty)
            {
                // The configuration rides along because the log that carried membership changes has just been
                // thrown away. Without it this replica could not learn who is in the group — including that
                // it may no longer be in it itself.
                Volatile.Write(ref _membership, RaftMembership.Deserialize(request.Configuration.Span));
                Volatile.Write(ref _role, (int)(Membership.IsVoter(Self) ? RaftRole.Follower : RaftRole.Learner));
            }

            Log.SnapshotInstalled(_logger, Group.Value, request.Leader.Value, request.LastIncludedIndex);
            return new InstallSnapshotResponse(_currentTerm, Success: true);
        }
        finally
        {
            _stateGate.Release();
        }
    }

    /// <summary>
    /// Ships this group's snapshot to a peer that cannot be caught up from the log.
    /// </summary>
    /// <returns><see langword="false"/> when no snapshot exists, or the peer refused it.</returns>
    private async ValueTask<bool> SendSnapshotAsync(PeerReplicationState peer)
    {
        if (_snapshots is null)
        {
            return false;
        }

        RaftSnapshotMetadata? metadata = await _snapshots
            .ReadMetadataAsync(Group, _shutdown.Token)
            .ConfigureAwait(false);
        if (metadata is null)
        {
            // The log was compacted with no snapshot standing in for it. That is a configuration mistake, not
            // a transient condition, so it is worth saying so rather than retrying forever.
            Log.SnapshotMissing(_logger, Group.Value, peer.Node.Value);
            return false;
        }

        var request = new InstallSnapshotRequest(
            Group,
            CurrentTerm,
            Self,
            metadata.Value.LastIncludedIndex,
            metadata.Value.LastIncludedTerm,
            metadata.Value.Configuration);

        InstallSnapshotResponse response = await _transport
            .InstallSnapshotAsync(peer.Node, request, _snapshots.ReadAsync(Group, _shutdown.Token), _shutdown.Token)
            .ConfigureAwait(false);

        if (response.Term > CurrentTerm)
        {
            await _stateGate.WaitAsync(_shutdown.Token).ConfigureAwait(false);
            try
            {
                if (response.Term > _currentTerm)
                {
                    await StepDownAsync(response.Term, _shutdown.Token).ConfigureAwait(false);
                }
            }
            finally
            {
                _stateGate.Release();
            }

            return false;
        }

        if (!response.Success)
        {
            return false;
        }

        peer.MatchIndex = metadata.Value.LastIncludedIndex;
        peer.NextIndex = metadata.Value.LastIncludedIndex + 1;
        Log.SnapshotSent(_logger, Group.Value, peer.Node.Value, metadata.Value.LastIncludedIndex);
        return true;
    }

    /// <summary>
    /// The leader half of ReadIndex: confirm this node still leads, then hand back its commit index.
    /// </summary>
    /// <remarks>
    /// The confirmation is not optional and not a formality. A leader that has been partitioned away still
    /// believes it leads, and answering from its own commit index would serve a read from a replica that a
    /// newer leader has already moved past — a stale read presented as a linearizable one, which is worse
    /// than an honestly stale one. Exchanging heartbeats with a quorum first is what rules that out.
    /// <para>
    /// The commit index is only meaningful once this leader has committed an entry of its own term, which the
    /// no-op appended on election guarantees (Raft §5.4.2).
    /// </para>
    /// </remarks>
    private async ValueTask<long> LeaderReadIndexAsync(CancellationToken cancellationToken)
    {
        long term = CurrentTerm;
        long index = Volatile.Read(ref _commitIndex);

        RaftMembership membership = Membership;
        if (membership.Voters.Length <= 1)
        {
            // A single-voter group is its own quorum; there is nobody who could have superseded it.
            return IsLeader ? index : throw new NotLeaderException(Group, _leaderId);
        }

        int confirmations = 1; // itself
        int needed = membership.QuorumSize;
        var probes = new List<Task<bool>>(membership.Voters.Length);

        foreach (NodeId voter in membership.Voters)
        {
            if (voter != Self)
            {
                probes.Add(ConfirmLeadershipAsync(voter, term, index, cancellationToken));
            }
        }

        while (probes.Count > 0 && confirmations < needed)
        {
            Task<bool> finished = await Task.WhenAny(probes).ConfigureAwait(false);
            probes.Remove(finished);
            if (finished.IsCompletedSuccessfully && finished.Result)
            {
                confirmations++;
            }
        }

        if (confirmations < needed || CurrentTerm != term || !IsLeader)
        {
            throw new NotLeaderException(Group, _leaderId);
        }

        return index;
    }

    /// <summary>An empty AppendEntries used purely as a leadership probe — no entries, no log change.</summary>
    private async Task<bool> ConfirmLeadershipAsync(
        NodeId voter,
        long term,
        long commitIndex,
        CancellationToken cancellationToken)
    {
        try
        {
            long prevIndex = _wal.LastIndex;
            long prevTerm = await _wal.TermAtAsync(prevIndex, cancellationToken).ConfigureAwait(false);
            if (prevTerm < 0)
            {
                return false;
            }

            var request = new AppendEntriesRequest(Group, term, Self, prevIndex, prevTerm, commitIndex);
            AppendEntriesResponse response = await _transport
                .AppendEntriesAsync(voter, request, EmptyEntries(), cancellationToken)
                .ConfigureAwait(false);

            return response.Success && response.Term == term;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch (IOException)
        {
            // An unreachable voter simply does not confirm. That is a normal condition, and the quorum rule
            // is what decides whether it matters.
            return false;
        }
    }

    private static async IAsyncEnumerable<RaftLogEntry> EmptyEntries()
    {
        await ValueTask.CompletedTask.ConfigureAwait(false);
        yield break;
    }

    /// <summary>
    /// Consumes a body that is not going to be stored. A streaming transport does not know the receiver lost
    /// interest, and leaving the stream half-read would strand it mid-message.
    /// </summary>
    private static async ValueTask DrainAsync(
        IAsyncEnumerable<ReadOnlyMemory<byte>> body,
        CancellationToken cancellationToken)
    {
        await foreach (ReadOnlyMemory<byte> _ in body.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            // Discarded on purpose.
        }
    }

    /// <summary>
    /// Recomputes the commit index from the voters' match indices. Sorting descending and taking the
    /// quorum-th element is the median rule of Raft §5.3: that index is present on a majority by definition.
    /// </summary>
    private async ValueTask AdvanceCommitIndexAsync()
    {
        if (!IsLeader)
        {
            return;
        }

        RaftMembership membership = Membership;
        ImmutableArray<NodeId> voters = membership.Voters;
        if (voters.IsDefaultOrEmpty)
        {
            return;
        }

        Span<long> matches = voters.Length <= 16 ? stackalloc long[voters.Length] : new long[voters.Length];
        int count = 0;
        foreach (NodeId voter in voters)
        {
            matches[count++] = voter == Self
                ? _wal.LastIndex
                : _peers.TryGetValue(voter, out PeerReplicationState? peer) ? peer.MatchIndex : 0;
        }

        matches[..count].Sort();
        matches[..count].Reverse();
        long candidate = matches[membership.QuorumSize - 1];

        long current = Volatile.Read(ref _commitIndex);
        if (candidate <= current)
        {
            return;
        }

        // Only entries from the leader's own term may be committed by counting replicas. Earlier-term entries
        // ride along once one of them is (Raft §5.4.2) — the no-op appended on election is what triggers that.
        long term = await _wal.TermAtAsync(candidate, _shutdown.Token).ConfigureAwait(false);
        if (term != CurrentTerm)
        {
            return;
        }

        Volatile.Write(ref _commitIndex, candidate);
        ReleaseCommitWaiters(candidate);
        _ = ApplyCommittedAsync();
    }

    private void ReleaseCommitWaiters(long committed)
    {
        foreach (KeyValuePair<long, CommitWaiter> pair in _commitWaiters)
        {
            if (pair.Value.Index <= committed && _commitWaiters.TryRemove(pair.Key, out CommitWaiter? waiter))
            {
                waiter.Completion.TrySetResult();
            }
        }
    }

    private async ValueTask WaitForCommitAsync(long index, CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _commitIndex) >= index)
        {
            return;
        }

        var waiter = new CommitWaiter(index);
        long key = Interlocked.Increment(ref _waiterSequence);
        _commitWaiters[key] = waiter;

        // Re-check after registering: the commit may have landed in the gap, and a waiter that missed it
        // would hang until the next unrelated append.
        if (Volatile.Read(ref _commitIndex) >= index)
        {
            _commitWaiters.TryRemove(key, out _);
            return;
        }

        using CancellationTokenRegistration registration = cancellationToken.Register(
            static state => ((CommitWaiter)state!).Completion.TrySetCanceled(),
            waiter);

        try
        {
            await waiter.Completion.Task.ConfigureAwait(false);
        }
        finally
        {
            _commitWaiters.TryRemove(key, out _);
        }
    }

    private async ValueTask ApplyCommittedAsync()
    {
        if (!await _applyGate.WaitAsync(0).ConfigureAwait(false))
        {
            // Another apply pass is already running; it will pick up whatever this one would have.
            return;
        }

        try
        {
            long committed = Volatile.Read(ref _commitIndex);
            long applied = Volatile.Read(ref _lastApplied);
            if (applied >= committed)
            {
                return;
            }

            await foreach (RaftLogEntry entry in _wal
                .ReadFromAsync(applied + 1, committed, _shutdown.Token)
                .ConfigureAwait(false))
            {
                if (entry.Kind == RaftEntryKind.Command)
                {
                    await _stateMachine.ApplyAsync(Group, entry, _shutdown.Token).ConfigureAwait(false);
                }

                Volatile.Write(ref _lastApplied, entry.Index);
            }
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
        finally
        {
            _applyGate.Release();
        }

        MaybeStartAutoCompaction();
    }

    /// <summary>
    /// Fires <see cref="TakeSnapshotAsync"/> in the background once applied entries since the log's retained
    /// start cross <see cref="RaftGroupOptions.AutoCompactionThreshold"/>. Checked after every apply pass
    /// rather than on a timer: compaction only ever becomes newly due when <see cref="_lastApplied"/> moves,
    /// so a timer would either check uselessly often on an idle group or lag behind a busy one.
    /// </summary>
    private void MaybeStartAutoCompaction()
    {
        long? threshold = _options.AutoCompactionThreshold;
        if (threshold is null || _snapshots is null || _stateMachine is not IRaftSnapshotableStateMachine)
        {
            return;
        }

        long sinceRetainedStart = Volatile.Read(ref _lastApplied) - (_wal.FirstIndex - 1);
        if (sinceRetainedStart < threshold.Value)
        {
            return;
        }

        // One compaction in flight at a time, same debounce pattern as peer replication: a slow CaptureAsync
        // must not stack up redundant attempts behind it, and TakeSnapshotAsync's own FirstIndex check makes
        // a run that finds nothing left to do a cheap no-op rather than a stale-data hazard.
        if (Interlocked.CompareExchange(ref _compactionInFlight, 1, 0) != 0)
        {
            return;
        }

        // Queued rather than awaited here for the same reason peer replication is: this runs from inside
        // ApplyCommittedAsync, itself reachable from the tick loop, and CaptureAsync's cost is set by the
        // host's state machine, not by this library -- holding the tick hostage to however long that takes
        // would reopen exactly the shutdown-latency problem PushToAllPeers had.
        _ = Task.Run(
            async () =>
            {
                try
                {
                    await TakeSnapshotAsync(_shutdown.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Shutting down.
                }
                catch (Exception ex) when (ex is IOException or InvalidOperationException)
                {
                    Log.AutoCompactionFailed(_logger, ex, Group.Value);
                }
                finally
                {
                    Volatile.Write(ref _compactionInFlight, 0);
                }
            },
            _shutdown.Token);
    }

    private async ValueTask<bool> IsCandidateLogCurrentAsync(VoteRequest request, CancellationToken cancellationToken)
    {
        await ValueTask.CompletedTask.ConfigureAwait(false);
        long lastTerm = _wal.LastTerm;
        long lastIndex = _wal.LastIndex;

        // Raft §5.4.1: the later term wins; on equal terms the longer log wins. This is the whole of the
        // election restriction, and it is what guarantees a leader holds every committed entry.
        return request.LastLogTerm != lastTerm
            ? request.LastLogTerm > lastTerm
            : request.LastLogIndex >= lastIndex;
    }

    private async ValueTask<long> FirstIndexOfTermAsync(long term, long from, CancellationToken cancellationToken)
    {
        long index = from;
        while (index > _wal.FirstIndex)
        {
            long candidate = await _wal.TermAtAsync(index - 1, cancellationToken).ConfigureAwait(false);
            if (candidate != term)
            {
                break;
            }

            index--;
        }

        return index;
    }

    private async ValueTask ReplayMembershipAsync(CancellationToken cancellationToken)
    {
        await foreach (RaftLogEntry entry in _wal
            .ReadFromAsync(_wal.FirstIndex, _wal.LastIndex, cancellationToken)
            .ConfigureAwait(false))
        {
            if (entry.Kind == RaftEntryKind.Membership)
            {
                ApplyMembership(MembershipChange.Read(entry.Payload.Span));
            }
        }
    }

    private void ApplyMembership(MembershipChange change)
    {
        RaftMembership updated = Membership.Apply(change);
        Volatile.Write(ref _membership, updated);

        // Logged where the change takes effect rather than where it is appended. Only the leader appends, so
        // logging there says what the leader intended and nothing about what any follower actually adopted --
        // and a configuration that has diverged between nodes is exactly the failure this line has to be able
        // to show.
        Log.MembershipChanged(
            _logger,
            Group.Value,
            change.Kind.ToString(),
            change.Node.Value,
            updated.Voters.Length);

        if (Role == RaftRole.Leader)
        {
            if (change.Kind == MembershipChangeKind.Remove)
            {
                _peers.TryRemove(change.Node, out _);
            }
            else if (change.Node != Self)
            {
                _peers.TryAdd(
                    change.Node,
                    new PeerReplicationState(change.Node, _wal.LastIndex + 1, _time.GetTimestamp()));
            }
        }
    }

    private void ArmElectionTimer()
    {
        double jitter = Random.Shared.NextDouble() * _options.ElectionJitter;
        TimeSpan timeout = _options.ElectionTimeout * (1.0 + jitter);
        Volatile.Write(ref _electionDeadline, _time.GetTimestamp() + ToTicks(timeout));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private long ToTicks(TimeSpan span) => (long)(span.TotalSeconds * _time.TimestampFrequency);

    /// <summary>Per-follower replication bookkeeping. A class, not a struct: the fields are updated by CAS.</summary>
    private sealed class PeerReplicationState(NodeId node, long nextIndex, long lastContact)
    {
        public int InFlight;
        public int Pending;

        /// <summary>
        /// Timestamp of the last reply from this peer, successful or not. A reply is a liveness proof either
        /// way — a rejected <c>AppendEntries</c> still came from a node that is up and talking. Seeded to the
        /// moment the peer state was created so a freshly elected leader is not judged on silence it has not
        /// had time to break.
        /// </summary>
        public long LastContact = lastContact;

        public NodeId Node { get; } = node;

        public long NextIndex { get; set; } = nextIndex;

        public long MatchIndex { get; set; }
    }

    private sealed class CommitWaiter(long index)
    {
        public long Index { get; } = index;

        public TaskCompletionSource Completion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}

/// <summary>Thrown when a leader-only operation reaches a node that is not the leader.</summary>
public sealed class NotLeaderException : InvalidOperationException
{
    /// <summary>Creates the exception, naming the leader if this node knows of one.</summary>
    public NotLeaderException(RaftGroupId group, NodeId? leader)
        : base("Node is not the leader of group " + group + (leader is null ? "." : "; leader is " + leader.Value + "."))
    {
        Group = group;
        Leader = leader;
    }

    /// <summary>The group the operation targeted.</summary>
    public RaftGroupId Group { get; }

    /// <summary>The leader this node believes is current, so the caller can redirect rather than retry.</summary>
    public NodeId? Leader { get; }
}

/// <summary>
/// Thrown when a client write reaches a leader that is mid-handoff to a successor (Raft §3.10).
/// The transfer is normally brief; the caller should retry rather than treat this as a lasting failure.
/// </summary>
public sealed class LeadershipTransferInProgressException : InvalidOperationException
{
    /// <summary>Creates the exception, naming the node leadership is being handed to.</summary>
    public LeadershipTransferInProgressException(RaftGroupId group, NodeId successor)
        : base("Group " + group + " is transferring leadership to " + successor + "; retry shortly.")
    {
        Group = group;
        Successor = successor;
    }

    /// <summary>The group the operation targeted.</summary>
    public RaftGroupId Group { get; }

    /// <summary>The node leadership is being handed to.</summary>
    public NodeId Successor { get; }
}
