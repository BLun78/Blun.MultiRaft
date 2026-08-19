// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

namespace Blun.MultiRaft.Cluster;

/// <summary>What happened. Two kinds, and the split is between acting and watching.</summary>
public enum ClusterEventKind : byte
{
    /// <summary>
    /// A node became a usable member of the cluster group — it joined for the first time, or it came back.
    /// <b>Raised on the cluster leader only.</b>
    /// </summary>
    /// <remarks>
    /// This is the one event meant to be acted on, and raising it in exactly one place is the point. A node
    /// coming back is the moment a host may want to move some group leaderships onto it, and if every node
    /// raised the event, every node's consumer would decide that at the same instant and fire the same
    /// handovers at the same groups. One reactor, one decision.
    /// </remarks>
    NodeBecameAvailable = 0,

    /// <summary>
    /// Something about this node's view of the cluster changed: who leads it, whether the administrative
    /// plane is usable, or which nodes are configured. <b>Raised on every node.</b>
    /// </summary>
    /// <remarks>
    /// Observational. It exists so a host can keep its own bookkeeping straight and tell its callers the
    /// truth about what is available — not so several nodes can independently decide to reshape the cluster.
    /// </remarks>
    ClusterStateChanged = 1,
}

/// <summary>One thing that happened to the cluster.</summary>
/// <param name="Kind">Which of the two kinds this is.</param>
/// <param name="Node">
/// The node the event concerns. For <see cref="ClusterEventKind.NodeBecameAvailable"/> the node that came up;
/// for <see cref="ClusterEventKind.ClusterStateChanged"/> the node reporting.
/// </param>
/// <param name="Leader">Who leads the cluster group, as far as the reporting node knows.</param>
/// <param name="State">The reporting node's administrative-plane state.</param>
public readonly record struct ClusterEvent(
    ClusterEventKind Kind,
    NodeId Node,
    NodeId? Leader,
    ClusterServiceState State);

/// <summary>What one node last said about its own load, and when.</summary>
/// <param name="Node">The node.</param>
/// <param name="GroupCount">Groups hosted there.</param>
/// <param name="LeaderCount">Groups it leads.</param>
/// <param name="Age">How long ago it last reported. Fresher is more trustworthy; nothing else uses it.</param>
public readonly record struct NodeLoad(NodeId Node, int GroupCount, int LeaderCount, TimeSpan Age);

/// <summary>
/// The cluster leader's picture of who is carrying how much. Only ever a picture: it is as fresh as the last
/// round of reports, and it is advisory — nothing in this library acts on it by itself.
/// </summary>
/// <param name="Nodes">One entry per node that has reported recently, ordered as the placement rule orders them.</param>
/// <param name="ClusterLeader">Who assembled this, or <see langword="null"/> if this node is not the leader.</param>
public readonly record struct ClusterLoadSnapshot(IReadOnlyList<NodeLoad> Nodes, NodeId? ClusterLeader);
