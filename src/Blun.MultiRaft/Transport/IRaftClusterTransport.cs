// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

namespace Blun.MultiRaft.Transport;

/// <summary>
/// The cluster-management half of a transport: load reports, and leadership-target questions routed to a
/// group's leader.
/// </summary>
/// <remarks>
/// A separate interface rather than two more methods on <see cref="IRaftProtocolTransport"/>, discovered with
/// an <c>is</c> check at the call site. That is the same shape the library already uses for
/// <see cref="IRaftSnapshotableStateMachine"/>: a transport that does not implement it keeps compiling, and
/// the cluster coordinator degrades honestly — no load model, so leadership placement falls back to whatever
/// the caller names explicitly, and nothing silently pretends to work.
/// </remarks>
public interface IRaftClusterTransport : IRaftProtocolTransport
{
    /// <summary>
    /// Pushes this node's load to the cluster leader. One-way in spirit: the reply carries nothing, and a
    /// failed report is a normal condition — the next one is a couple of seconds away.
    /// </summary>
    ValueTask ReportLoadAsync(
        NodeId target,
        NodeLoadReport report,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Asks a group's leader whether a node could take over, and optionally tells it to hand over.
    /// </summary>
    ValueTask<LeaderTargetResponse> LeaderTargetAsync(
        NodeId target,
        LeaderTargetRequest request,
        CancellationToken cancellationToken = default);
}

/// <summary>The receiving half of <see cref="IRaftClusterTransport"/>. Implemented by the host.</summary>
public interface IRaftClusterListener
{
    /// <summary>Handles a load report that arrived for this node.</summary>
    ValueTask OnLoadReportAsync(NodeLoadReport report, CancellationToken cancellationToken = default);

    /// <summary>Handles a leadership-target question about a group hosted here.</summary>
    ValueTask<LeaderTargetResponse> OnLeaderTargetAsync(
        LeaderTargetRequest request,
        CancellationToken cancellationToken = default);
}
