// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Collections.Immutable;
using Blun.MultiRaft.Core;

namespace Blun.MultiRaft.Cluster;

/// <summary>How many nodes this process expects to share a cluster with. Fixed for the life of the process.</summary>
public enum ClusterMode : byte
{
    /// <summary>
    /// One node, which is therefore always the cluster leader, with no election and no transport. Everything
    /// works as it does in a replicated cluster; there is simply nobody to agree with.
    /// </summary>
    SingleNode = 0,

    /// <summary>Several nodes, with a real election over a real transport.</summary>
    Replicated = 1,
}

/// <summary>Whether this node's administrative plane can be used right now.</summary>
public enum ClusterServiceState : byte
{
    /// <summary>The cluster group is up but has not settled on a leader yet.</summary>
    Starting = 0,

    /// <summary>A leader is known and reachable; administrative operations work.</summary>
    Available = 1,

    /// <summary>
    /// No leader is reachable for the cluster group — this node is in a minority. Administrative operations
    /// are refused; the queue groups are untouched and keep serving on their own quorums.
    /// </summary>
    AdminSuspended = 2,

    /// <summary>The coordinator has been disposed.</summary>
    Stopped = 3,
}

/// <summary>Configuration of this node's participation in the cluster.</summary>
public sealed class ClusterOptions
{
    /// <summary>
    /// Single-node or replicated. Fixed at construction and checked against what this node recorded on its
    /// last run — see <see cref="ClusterModeStore"/> for why a silent change is not survivable.
    /// </summary>
    public required ClusterMode Mode { get; init; }

    /// <summary>Identity of this node.</summary>
    public required NodeId Self { get; init; }

    /// <summary>
    /// Every node the cluster should eventually consist of, this one included. The cluster leader reconciles
    /// the cluster group's configuration towards this set, one single-server change at a time.
    /// </summary>
    /// <remarks>
    /// Ignored in <see cref="ClusterMode.SingleNode"/>, where the set is this node and nothing else.
    /// </remarks>
    public ImmutableArray<NodeId> Nodes { get; init; } = [];

    /// <summary>How often this node pushes its load to the cluster leader.</summary>
    public TimeSpan LoadReportInterval { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// How long a load report stays usable. A node that stops reporting is dropped from the model rather than
    /// left in it at a stale number — which would otherwise make a dead node look like the emptiest one and
    /// attract every placement decision.
    /// </summary>
    public TimeSpan LoadReportTtl { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// How often the cluster leader reconciles the cluster group's configuration towards <see cref="Nodes"/>
    /// and re-checks which nodes are online.
    /// </summary>
    /// <remarks>
    /// This pass also drives <see cref="RebalanceOnFailover"/> when that is enabled: it is the same
    /// cadence at which the cluster leader notices a group's leader has changed.
    /// </remarks>
    public TimeSpan ReconcileInterval { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Whether the cluster leader applies the load-based placement rule after an unplanned leadership
    /// change, moving a group's new leader to the least-loaded legal target if the node Raft's own election
    /// picked is not already that target. Default <see langword="true"/>.
    /// </summary>
    /// <remarks>
    /// Raft's election is unaware of load by design — it cannot be, an election has to complete under
    /// partition without waiting on load reports that may never arrive. This option is what closes that
    /// gap afterwards: once a term settles, the cluster leader compares the node Raft picked against the
    /// placement rule's own answer, and if they disagree, asks for a transfer the same way an operator would
    /// through <see cref="ClusterCoordinator.RequestLeaderTransferAsync"/>.
    /// A transfer here is exactly one immediate handover, not a standing preference — nothing loops back to
    /// re-check a decision once it is made, so this cannot fight a leader that later loses more elections
    /// for reasons of its own. Only ever evaluated for groups hosted on the cluster leader itself, and only
    /// on the change, not continuously, so a healthy cluster settled on a good placement pays nothing for
    /// this being on.
    /// </remarks>
    public bool RebalanceOnFailover { get; init; } = true;

    /// <summary>Tuning for the cluster group itself. Election timings are deliberately more generous than a
    /// queue group's: administrative work is rare, and a needless election here is more costly than a slow
    /// one.</summary>
    public RaftGroupOptions? GroupOptions { get; init; }

    /// <summary>
    /// Where the cluster-mode marker is kept. <see langword="null"/> keeps it in memory, which means the
    /// mode is not enforced across restarts — acceptable for tests, never for a real node.
    /// </summary>
    public string? DataDirectory { get; init; }

    /// <summary>The state machine for administrative commands, if the host has one.</summary>
    /// <remarks>
    /// The library owns node membership on this group and nothing else; it travels as
    /// <see cref="Wal.RaftEntryKind.Membership"/> entries, which are never handed to a state machine. Users,
    /// access changes and anything else administrative are the host's, travel as
    /// <see cref="Wal.RaftEntryKind.Command"/>, and land here. The two share one log and cannot be confused
    /// for each other.
    /// </remarks>
    public IRaftStateMachine? AdminStateMachine { get; init; }

    internal ImmutableArray<NodeId> EffectiveNodes
        => Mode == ClusterMode.SingleNode || Nodes.IsDefaultOrEmpty ? [Self] : Nodes;

    internal RaftGroupOptions EffectiveGroupOptions
    {
        get
        {
            RaftGroupOptions source = GroupOptions ?? new RaftGroupOptions
            {
                // More generous than a queue group's, deliberately. Administrative work is rare, so a
                // needless election here buys nothing and costs the plane a caller may be waiting on.
                ElectionTimeout = TimeSpan.FromMilliseconds(1000),
                HeartbeatInterval = TimeSpan.FromMilliseconds(250),
            };

            return new RaftGroupOptions
            {
                ElectionTimeout = source.ElectionTimeout,
                ElectionJitter = source.ElectionJitter,
                HeartbeatInterval = source.HeartbeatInterval,
                MaxEntriesPerAppend = source.MaxEntriesPerAppend,
                PreVote = source.PreVote,
                PromotionCatchUpThreshold = source.PromotionCatchUpThreshold,
                LeaderTargetLagThreshold = source.LeaderTargetLagThreshold,
                LeaderTargetContactWindow = source.LeaderTargetContactWindow,
                LeadershipTransferCatchUpTimeout = source.LeadershipTransferCatchUpTimeout,
                AutoCompactionThreshold = source.AutoCompactionThreshold,

                // The last two are not taken from the caller, and both for the same reason. An
                // administrative write that is acknowledged and then vanishes is a revoked user who is still
                // let in, so the acknowledgement has to mean quorum. And a partitioned cluster leader that
                // kept believing it leads would report the administrative plane as fine while holding an
                // arbitrarily stale view of exactly that state.
                Durability = DurabilityLevel.Quorum,
                CheckQuorum = true,
            };
        }
    }
}
