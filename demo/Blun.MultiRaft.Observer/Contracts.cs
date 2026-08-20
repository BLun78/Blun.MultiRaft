// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Text.Json.Serialization;

namespace Blun.MultiRaft.Observer;

// ---------------------------------------------------------------------------------------------------------
// What a node says about itself. Mirrors RaftNodeHost.Describe() on the other side of /status.
// ---------------------------------------------------------------------------------------------------------

internal sealed record NodeStatus(string Node, NodeClusterStatus? Cluster, NodeGroupStatus[]? Groups);

internal sealed record NodeClusterStatus(
    string? Mode,
    string? State,
    bool IsClusterLeader,
    string? ClusterLeader,
    ulong[]? Voters,
    ulong[]? Learners,
    string[]? RecentEvents,
    NodeGroupStatus? Group);

internal sealed record NodeGroupStatus(
    ulong Group,
    string Role,
    long Term,
    string? Leader,
    long CommitIndex,
    WalStatus? Wal,
    SendStatus? Send);

/// <summary>What a node's copy of one group's log costs on disk, and what is in it.</summary>
internal sealed record WalStatus(
    long SizeBytes,
    int Segments,
    long FirstIndex,
    long LastIndex,
    long Entries,
    long BytesPerEntry);

/// <summary>A run of generated traffic, as the node carrying it reports it.</summary>
internal sealed record SendStatus(
    bool Running,
    long Sent,
    long Failed,
    int Total,
    int Size,
    double RatePerSecond,
    string? Error);

// ---------------------------------------------------------------------------------------------------------
// What the app host's control plane says about a resource.
// ---------------------------------------------------------------------------------------------------------

internal sealed record ResourceState(string Name, ulong Node, int StatusPort, string State, bool Running);

// ---------------------------------------------------------------------------------------------------------
// What the observer publishes to the UI.
// ---------------------------------------------------------------------------------------------------------

/// <param name="Online">Nodes whose <c>/status</c> answered this round.</param>
/// <param name="Required">How many nodes a majority needs — the quorum of the configured node set.</param>
/// <param name="ClusterLeader">
/// The node most of the answering nodes name as cluster leader. It is a tally, not a fact: every node
/// reports its own belief, and during an election they legitimately differ.
/// </param>
/// <param name="LeaderDisagreement">Set while the answering nodes do not all name the same cluster leader.</param>
internal sealed record ClusterSnapshot(
    long TimestampMs,
    int Online,
    int Total,
    int Required,
    bool HasQuorum,
    string? ClusterLeader,
    bool LeaderDisagreement,
    NodeView[] Nodes,
    GroupView[] Groups);

/// <param name="AspireState">
/// The resource state Aspire reports — whether the process exists. Deliberately kept apart from
/// <paramref name="Online"/>, which is whether it answers. A node can be Running and silent, and the whole
/// point of watching a consensus cluster is being able to see that.
/// </param>
internal sealed record NodeView(
    ulong Id,
    string ResourceName,
    int StatusPort,
    bool Online,
    string? AspireState,
    string? ClusterState,
    string? ClusterMode,
    bool IsClusterLeader,
    string? ClusterLeader,
    ulong[] Voters,
    ulong[] Learners,
    string[] RecentEvents,
    long? LatencyMs,
    string? Error,
    long WalBytes,
    long WalEntries);

/// <param name="Group">
/// The group id as a string. The cluster group is <c>ulong.MaxValue</c>, which is larger than a JavaScript
/// number can hold exactly — sent as a number it would arrive in the browser rounded, and the UI would ask
/// for leadership of a group that does not exist.
/// </param>
internal sealed record GroupView(
    string Group,
    string Label,
    string? Leader,
    bool Disagreement,
    GroupCell[] Cells,
    SendStatus? Send);

internal sealed record GroupCell(
    ulong Node,
    bool Online,
    string Role,
    long Term,
    string? Leader,
    long CommitIndex,
    WalStatus? Wal);

internal sealed record CommandResult(bool Success, bool Canceled, string? Message);

/// <summary>One line as the control plane hands it over.</summary>
internal sealed record LogLine(int LineNumber, string Content, bool IsError);

/// <param name="Reset">
/// Not a line: the upstream stream for this node reconnected and is about to replay its backlog, so whatever
/// the receiver already holds for it should be dropped rather than duplicated.
/// </param>
internal sealed record LogEvent(ulong Node, int LineNumber, string Content, bool IsError, bool Reset);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(NodeStatus))]
[JsonSerializable(typeof(ResourceState[]))]
[JsonSerializable(typeof(ClusterSnapshot))]
[JsonSerializable(typeof(CommandResult))]
[JsonSerializable(typeof(LogLine))]
[JsonSerializable(typeof(LogEvent))]
internal sealed partial class ObserverJson : JsonSerializerContext;
