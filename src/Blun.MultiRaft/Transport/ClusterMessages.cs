// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Transport;

/// <summary>
/// What one node tells the cluster leader about its own load. Pushed on a timer, never replicated.
/// </summary>
/// <remarks>
/// Deliberately not a log entry. Leader counts change on every election of every group, and putting them
/// through the cluster group's log would turn an advisory number into a write firehose on the one group that
/// has to stay responsive for administrative work. It is soft state: the cluster leader holds it in memory,
/// ages it out when a node stops reporting, and rebuilds it within one report interval after an election.
/// <para>
/// Push rather than pull. A node that dies simply stops reporting and is aged out; pulling would cost the
/// cluster leader one round trip per node with timeouts that serialize behind each other.
/// </para>
/// </remarks>
/// <param name="Node">Who is reporting.</param>
/// <param name="Sequence">
/// Monotonic per reporting node, so a report that overtakes an older one on the wire cannot move the model
/// backwards.
/// </param>
/// <param name="GroupCount">Groups hosted on the reporting node, whatever their role.</param>
/// <param name="LeaderCount">Groups the reporting node currently leads. The number placement is chosen by.</param>
public readonly record struct NodeLoadReport(NodeId Node, long Sequence, int GroupCount, int LeaderCount);

/// <summary>Why a node was refused as a leadership target, or <see cref="Valid"/> when it was not.</summary>
/// <remarks>
/// An enum rather than a string: this travels on the wire, and the rest of the protocol allocates nothing
/// per message.
/// </remarks>
public enum LeaderTargetStatus : byte
{
    /// <summary>Caught up, voting, reachable — a legal target for a transfer.</summary>
    Valid = 0,

    /// <summary>Not in the group's configuration at all, as voter or learner.</summary>
    NotAMember = 1,

    /// <summary>A learner. Leadership can only move to a voter (Raft §4.2.1).</summary>
    NotAVoter = 2,

    /// <summary>In the group, voting, but its log is too far behind to hand leadership to.</summary>
    Lagging = 3,

    /// <summary>The group's leader could not be reached to ask, so nothing can be said about the target.</summary>
    Unreachable = 4,

    /// <summary>Already the leader of this group. Not an error — there is simply nothing to do.</summary>
    IsCurrentLeader = 5,

    /// <summary>Nobody leads this group right now, so there is no leadership to transfer.</summary>
    NoLeader = 6,

    /// <summary>A transfer for this group is already in flight.</summary>
    TransferInFlight = 7,
}

/// <summary>
/// Asks the leader of a group whether <paramref name="Candidate"/> could take over, and optionally tells it
/// to go ahead.
/// </summary>
/// <remarks>
/// Both halves are one message because they answer the same question against the same state. Splitting them
/// would let the answer go stale between the check and the act, which is exactly the race a caller would then
/// have to reason about.
/// <para>
/// Only the group's current leader can answer: the judgement rests on <c>MatchIndex</c> per peer, and no
/// other node has it. The cluster leader knows how many groups a node leads; it does not know how far behind
/// that node is on any particular one.
/// </para>
/// </remarks>
/// <param name="Group">The group whose leadership is in question.</param>
/// <param name="Candidate">
/// The node being proposed, or <see langword="null"/> to ask the leader to choose one itself.
/// </param>
/// <param name="Execute">
/// When set, a valid candidate is not merely reported but handed leadership. When clear this is a pure query
/// and changes nothing.
/// </param>
public readonly record struct LeaderTargetRequest(RaftGroupId Group, NodeId? Candidate, bool Execute);

/// <summary>Reply to <see cref="LeaderTargetRequest"/>.</summary>
/// <param name="Node">
/// The node the answer is about: the candidate that was asked for, or the one the leader picked.
/// </param>
/// <param name="Status">Whether that node is a legal target, and if not, why not.</param>
/// <param name="Lag">
/// How many entries behind the leader the node is. Zero for a caught-up node; meaningful mainly alongside
/// <see cref="LeaderTargetStatus.Lagging"/>, where it says how far off the target was.
/// </param>
/// <param name="Transferred">
/// Whether leadership actually moved. Always <see langword="false"/> for a query, and possibly
/// <see langword="false"/> for an execute — a target can be valid at the moment of the check and still
/// decline, or fail to finish catching up once writes stop.
/// </param>
public readonly record struct LeaderTargetResponse(
    NodeId Node,
    LeaderTargetStatus Status,
    long Lag,
    bool Transferred);
