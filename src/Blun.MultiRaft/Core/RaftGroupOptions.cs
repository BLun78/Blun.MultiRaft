// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Core;

/// <summary>
/// How durable a leader's acknowledgement is, chosen per group.
/// </summary>
/// <remarks>
/// Only <see cref="Quorum"/> is the Raft guarantee: once <see cref="Core.RaftGroupInstance.AppendAsync"/> returns,
/// the entry survives any single node failing, including the leader. The other two levels return earlier and
/// are correspondingly weaker — not "slightly less safe", but a caller-visible acknowledgement that can be
/// contradicted after the fact. A client that received a successful <c>AppendAsync</c> and then observes the
/// entry gone (because the node it talked to lost leadership before replicating, or crashed before fsyncing)
/// is not a bug in this library; it is what the chosen level promised. Pick <see cref="LeaderBuffered"/> or
/// <see cref="LeaderDurable"/> only where that is an acceptable, understood trade — a queue where an
/// occasional silently-dropped message is cheaper than the latency of waiting for quorum.
/// </remarks>
public enum DurabilityLevel : byte
{
    /// <summary>
    /// Acknowledge as soon as the entry is in the leader's log buffer, before fsync. Fastest, and the
    /// weakest: an acknowledged entry can vanish on a process crash or an OS/power failure, with no other
    /// replica ever having seen it.
    /// </summary>
    LeaderBuffered = 0,

    /// <summary>
    /// Acknowledge once the leader has fsynced its own log, before any replica has the entry. Survives the
    /// leader's process crashing and restarting on the same disk; does not survive that disk failing, and
    /// does not survive the leader losing an election before replication catches up — an acknowledged entry
    /// that only the outgoing leader ever wrote is not guaranteed to reach whoever wins next.
    /// </summary>
    LeaderDurable = 1,

    /// <summary>
    /// Acknowledge once a majority of voters have the entry. The real Raft guarantee: from this point on, no
    /// single node failure — including losing the leader — can make the entry disappear.
    /// </summary>
    Quorum = 2,
}

/// <summary>Per-group tuning. Every duration is per group, because a busy queue and an idle one want different ones.</summary>
public sealed class RaftGroupOptions
{
    /// <summary>Base election timeout. The actual timeout is this plus a random share of itself.</summary>
    public TimeSpan ElectionTimeout { get; init; } = TimeSpan.FromMilliseconds(300);

    /// <summary>
    /// How much randomized jitter is added, as a fraction of <see cref="ElectionTimeout"/>. Without it,
    /// followers that lost the same leader all time out together and split the vote — and with thousands of
    /// groups sharing a node, they would do so in lockstep.
    /// </summary>
    public double ElectionJitter { get; init; } = 1.0;

    /// <summary>Interval between heartbeats from a leader. Must be well below <see cref="ElectionTimeout"/>.</summary>
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromMilliseconds(75);

    /// <summary>
    /// Heartbeat interval a leader falls back to once <see cref="ElectionTimeout"/> has passed with no new
    /// entry appended. <see langword="null"/> (the default) disables this — the leader always heartbeats at
    /// <see cref="HeartbeatInterval"/>, busy or not.
    /// </summary>
    /// <remarks>
    /// Real replication is never delayed by this: <see cref="Core.RaftGroupInstance.AppendAsync"/> pushes to every
    /// peer immediately regardless of the tick loop, so this only stretches the cadence of empty keep-alive
    /// heartbeats sent while nothing is happening. For a group that is idle for long stretches — most queues,
    /// most of the time — those empty heartbeats are pure 24/7 network cost with no data to show for it.
    /// <para>
    /// Must stay below <see cref="ElectionTimeout"/>, same as <see cref="HeartbeatInterval"/> — a follower's
    /// own timeout does not know this group went idle, so nothing about the safety margin between heartbeats
    /// and elections changes, only how large that margin is allowed to be used. <see cref="Core.RaftGroupInstance"/>'s
    /// constructor throws <see cref="ArgumentOutOfRangeException"/> if it is set at or above
    /// <see cref="ElectionTimeout"/>, or below <see cref="HeartbeatInterval"/> (which would make it not idle
    /// at all). A group running <see cref="CheckQuorum"/> should leave extra margin: that check's own window
    /// is exactly one <see cref="ElectionTimeout"/>, so an idle interval close to it risks a leader stepping
    /// aside from its own slow heartbeat rather than from real silence.
    /// </para>
    /// </remarks>
    public TimeSpan? IdleHeartbeatInterval { get; init; }

    /// <summary>Maximum entries carried by one replication round.</summary>
    public int MaxEntriesPerAppend { get; init; } = 256;

    /// <summary>Where a client acknowledgement is taken to be safe.</summary>
    public DurabilityLevel Durability { get; init; } = DurabilityLevel.Quorum;

    /// <summary>
    /// Whether appends try to LZ4-compress the payload before it reaches the log. Off by default.
    /// </summary>
    /// <remarks>
    /// An on/off switch rather than a choice of level, and the entry-by-entry decision is made by trying
    /// rather than by predicting. Both fall out of measurement (<c>doc/compression-level-benchmark.md</c>):
    /// every level above the fastest cost more on the append-and-flush path than it saved, at every payload
    /// size measured — up to eight times the uncompressed time at a megabyte — so there is nothing to choose
    /// between. And whether compression helps at all turns on how compressible the payload is, not how large
    /// it is: the same level and size differed by a factor of six between repetitive and random content, so a
    /// size threshold would guess wrong precisely on the payloads that are already compressed or encrypted.
    /// Trying costs little enough (~13% on the worst measured case, an incompressible megabyte) to be worth
    /// paying for a correct answer per entry.
    /// <para>
    /// The result of that per-entry decision travels in <see cref="RaftEntryHeader.Compression"/>, so a group
    /// switched on still writes <see cref="RaftPayloadCompression.None"/> for anything that did not shrink,
    /// and a reader never has to know what the group was configured with.
    /// </para>
    /// </remarks>
    public RaftPayloadCompression PayloadCompression { get; init; } = RaftPayloadCompression.None;

    /// <summary>
    /// Whether a candidate runs a pre-vote round before incrementing its term (Raft §9.6). On by default:
    /// the cost is one extra round trip on a real election, and it prevents a rejoining node from disrupting
    /// a healthy group.
    /// </summary>
    public bool PreVote { get; init; } = true;

    /// <summary>
    /// How close a learner's log must be to the leader's before it may be promoted, in entries. A promotion
    /// while the node is still far behind hands a vote to a replica that cannot serve it.
    /// </summary>
    public long PromotionCatchUpThreshold { get; init; } = 64;

    /// <summary>
    /// How many entries behind the leader a voter may be and still be reported as a legal leadership target by
    /// <see cref="Core.RaftGroupInstance.EvaluateLeaderTargetAsync"/>. <see langword="null"/> (the default) uses
    /// <see cref="MaxEntriesPerAppend"/> — at most one replication round behind.
    /// </summary>
    /// <remarks>
    /// Separate from <see cref="PromotionCatchUpThreshold"/> because the two questions have different costs
    /// when answered wrongly. Promotion asks whether a replica is close enough to be useful as a voter, and
    /// getting it wrong yields a slightly weaker quorum for a while. This asks whether a node can take over
    /// without the group standing still, and getting it wrong stops writes for up to
    /// <see cref="LeadershipTransferCatchUpTimeout"/> before the handover is abandoned. One number steering
    /// both would mean tuning promotions silently retunes handovers.
    /// <para>
    /// The default is derived rather than picked: one replication round is the unit the protocol already works
    /// in, so "at most one round behind" scales with <see cref="MaxEntriesPerAppend"/> instead of being a
    /// constant that goes stale the moment someone changes the batch size. Exact equality is not usable — this
    /// is a snapshot with writes still flowing, so <c>MatchIndex == LastIndex</c> is momentary and on a busy
    /// queue almost never true, which would report every target as lagging. The transfer itself does demand
    /// exact equality, but it can afford to: it stops accepting writes first.
    /// </para>
    /// </remarks>
    public long? LeaderTargetLagThreshold { get; init; }

    /// <summary>
    /// How recently a voter must have replied to count as healthy enough to be handed leadership.
    /// <see langword="null"/> (the default) uses twice <see cref="ElectionTimeout"/>.
    /// </summary>
    /// <remarks>
    /// Lag alone does not answer this. A node that died moments ago still has a <c>MatchIndex</c> close to the
    /// leader's — on a quiet queue, indistinguishable from a healthy one — so without a liveness check it would
    /// be reported as a legal target, and the handover would then stop writes and wait out
    /// <see cref="LeadershipTransferCatchUpTimeout"/> against a node that is never going to answer. The group
    /// keeps its leader either way; what this saves is the stall.
    /// <para>
    /// Twice the election timeout by default, which is the same window the cluster coordinator uses to decide
    /// a node is available — the same question deserves the same answer in both places.
    /// </para>
    /// </remarks>
    public TimeSpan? LeaderTargetContactWindow { get; init; }

    /// <summary>
    /// How long <see cref="Core.RaftGroupInstance.TransferLeadershipAsync"/> waits for the target's log to reach
    /// the leader's before giving up and sending nothing. <see langword="null"/> (the default) uses ten times
    /// <see cref="ElectionTimeout"/>.
    /// </summary>
    /// <remarks>
    /// A single election timeout is enough to catch a target up on the handful of entries appended since the
    /// last heartbeat, but not on the backlog a node that just joined — or one recovering from a partition —
    /// can be carrying. Tying the default to <see cref="ElectionTimeout"/> rather than a fixed constant keeps
    /// groups with a long election timeout (a deliberately quiet, low-churn queue) generous by the same
    /// proportion, instead of a fixed number that is too short for some groups and needlessly long for others.
    /// </remarks>
    public TimeSpan? LeadershipTransferCatchUpTimeout { get; init; }

    /// <summary>
    /// Entries applied since the log's retained start before this group triggers a snapshot-and-compact of
    /// its own accord. <see langword="null"/> (the default) never compacts automatically — the operation
    /// still works when called directly, just not on a schedule.
    /// </summary>
    /// <remarks>
    /// Deliberately opt-in rather than a library-chosen default. When to compact trades disk (an
    /// uncompacted log keeps growing) against work (every compaction captures and writes out the full state);
    /// where that trade lands depends on how large the state machine's state actually is, which only the host
    /// knows. A message queue's state can be tiny (a handful of in-flight message ids) or, for a
    /// slow-draining queue with a large backlog, large enough that compacting on every tick's worth of growth
    /// would be its own throughput problem.
    /// </remarks>
    public long? AutoCompactionThreshold { get; init; }

    /// <summary>
    /// Whether a leader steps aside once it stops hearing from a majority of voters. Off by default; the
    /// cluster-management group turns it on unconditionally.
    /// </summary>
    /// <remarks>
    /// Without it a leader that has been partitioned away keeps believing it leads, indefinitely, until
    /// something hands it a higher term. That is harmless for plain replication — it cannot commit anything,
    /// because commit needs a majority either way — but it is not harmless for anyone asking the node whether
    /// its administrative plane is usable: the honest answer is no, and nothing on the node knows it.
    /// <para>
    /// The window is one <see cref="ElectionTimeout"/>, measured from the last successful reply per voter.
    /// The check only ever fires on a leader, and stepping aside leaves term and vote untouched — this is not
    /// a response to a newer term, only to silence.
    /// </para>
    /// </remarks>
    public bool CheckQuorum { get; init; }
}

/// <summary>
/// Where committed entries go. The library replicates bytes; deciding what they mean is the host's job —
/// for Blun.MQ that means turning a committed command into an enqueue or a settlement.
/// </summary>
public interface IRaftStateMachine
{
    /// <summary>
    /// Applies one committed entry, in index order, never concurrently for the same group.
    /// The payload aliases a buffer that is recycled on return; anything retained must be copied.
    /// </summary>
    ValueTask ApplyAsync(RaftGroupId group, RaftLogEntry entry, CancellationToken cancellationToken = default);
}

/// <summary>
/// A state machine that can be captured and restored, which is what makes log compaction possible: a log can
/// only be truncated at the head once the state it produced can be reconstructed some other way.
/// </summary>
/// <remarks>
/// Optional. A group whose state machine does not implement this can still run — it simply never compacts,
/// and a replica that falls too far behind cannot be caught up by snapshot.
/// </remarks>
public interface IRaftSnapshotableStateMachine : IRaftStateMachine
{
    /// <summary>
    /// Writes the state as of <paramref name="lastAppliedIndex"/> as a chunked stream. Streamed rather than
    /// returned whole because a snapshot is the one payload in this system with no size bound.
    /// </summary>
    IAsyncEnumerable<ReadOnlyMemory<byte>> CaptureAsync(
        RaftGroupId group,
        long lastAppliedIndex,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces the state wholesale from a snapshot. Everything previously applied is discarded — a partial
    /// restore would leave the replica agreeing with nobody.
    /// </summary>
    ValueTask RestoreAsync(
        RaftGroupId group,
        IAsyncEnumerable<ReadOnlyMemory<byte>> body,
        CancellationToken cancellationToken = default);
}

/// <summary>A state machine that discards everything. For groups whose value is the log itself, and for tests.</summary>
public sealed class NullRaftStateMachine : IRaftStateMachine
{
    /// <summary>The shared instance; it holds no state.</summary>
    public static readonly NullRaftStateMachine Instance = new();

    /// <inheritdoc />
    public ValueTask ApplyAsync(RaftGroupId group, RaftLogEntry entry, CancellationToken cancellationToken = default)
        => ValueTask.CompletedTask;
}
