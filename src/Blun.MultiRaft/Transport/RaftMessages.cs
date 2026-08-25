// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using Blun.MultiRaft.Core;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Transport;

/// <summary>
/// Everything about an <c>AppendEntries</c> RPC except the entries themselves, which stream separately so
/// they never have to be materialized as a collection. All fields are scalars, so the whole struct fits in
/// a handful of registers and the message costs no allocation on either side.
/// </summary>
public readonly record struct AppendEntriesRequest(
    RaftGroupId Group,
    long Term,
    NodeId Leader,
    long PrevLogIndex,
    long PrevLogTerm,
    long LeaderCommit);

/// <summary>Reply to <see cref="AppendEntriesRequest"/>.</summary>
/// <param name="Term">The responder's term, so a stale leader learns to step down.</param>
/// <param name="Success">Whether the consistency check at <c>PrevLogIndex</c> passed.</param>
/// <param name="MatchIndex">Highest index the responder now has that matches the leader.</param>
/// <param name="ConflictIndex">
/// On failure, the index the leader should retry from. Carrying it turns log reconciliation into roughly one
/// round trip instead of the one-index-per-round-trip decrement of the plain algorithm.
/// </param>
public readonly record struct AppendEntriesResponse(
    long Term,
    bool Success,
    long MatchIndex,
    long ConflictIndex);

/// <summary>A <c>RequestVote</c> RPC.</summary>
/// <param name="PreVote">
/// When true this is a pre-vote probe: the candidate asks whether it <em>would</em> win without bumping any
/// term. A node partitioned away and re-joining would otherwise force a term bump and a needless election on
/// an otherwise healthy group — with thousands of groups per node, that thrash is the difference between a
/// blip and an outage.
/// </param>
public readonly record struct VoteRequest(
    RaftGroupId Group,
    long Term,
    NodeId Candidate,
    long LastLogIndex,
    long LastLogTerm,
    bool PreVote);

/// <summary>Reply to <see cref="VoteRequest"/>.</summary>
public readonly record struct VoteResponse(long Term, bool Granted);

/// <summary>
/// An <c>InstallSnapshot</c> RPC. Sent when a follower needs entries the leader has already compacted away —
/// the one case where replication cannot proceed by sending log entries at all.
/// </summary>
/// <param name="Configuration">
/// The group configuration as of the snapshot, serialized. It has to travel with the snapshot because the
/// receiver is about to discard the log that membership changes lived in.
/// </param>
public readonly record struct InstallSnapshotRequest(
    RaftGroupId Group,
    long Term,
    NodeId Leader,
    long LastIncludedIndex,
    long LastIncludedTerm,
    ReadOnlyMemory<byte> Configuration);

/// <summary>Reply to <see cref="InstallSnapshotRequest"/>.</summary>
public readonly record struct InstallSnapshotResponse(long Term, bool Success);

/// <summary>
/// A <c>ReadIndex</c> RPC (Raft §6.4). A follower asks the leader for the index it must have applied before
/// it may answer a read linearizably.
/// </summary>
/// <remarks>
/// The point is what it does <em>not</em> do: no log entry, no fsync, no replication. The leader confirms it
/// still leads by exchanging heartbeats with a quorum and hands back its commit index. That turns a
/// linearizable read from an append into one round trip, and lets followers serve reads at all — without it
/// every read has to go to the leader, which is the bottleneck the whole multi-Raft design tries to spread.
/// </remarks>
public readonly record struct ReadIndexRequest(RaftGroupId Group, long Term);

/// <summary>Reply to <see cref="ReadIndexRequest"/>.</summary>
/// <param name="ReadIndex">The index the asker must have applied before serving the read.</param>
/// <param name="Leader">Who leads, so a misdirected request becomes a redirect rather than a failure.</param>
public readonly record struct ReadIndexResponse(long Term, bool Success, long ReadIndex, ulong Leader);

/// <summary>
/// A <c>TimeoutNow</c> RPC (Raft §3.10). Sent by a leader to the node it is transferring leadership to,
/// telling it to start an election immediately instead of waiting out its timer.
/// </summary>
/// <remarks>
/// This is the entire transfer mechanism — there is no separate handshake. The sender has already made sure
/// the target's log is caught up before sending it, which is what makes the resulting election safe: with a
/// log at least as long as the outgoing leader's, the target cannot lose to a less current candidate, and it
/// wins in one round because everyone's timer is otherwise still running normally.
/// </remarks>
public readonly record struct TimeoutNowRequest(RaftGroupId Group, long Term);

/// <summary>Reply to <see cref="TimeoutNowRequest"/>. Acknowledgement only — the outcome is the election that follows.</summary>
public readonly record struct TimeoutNowResponse(long Term, bool Accepted);
