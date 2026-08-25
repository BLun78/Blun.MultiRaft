// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using Blun.MultiRaft.Core;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Transport;

/// <summary>
/// The transport for a cluster of one. Every method throws — and that is the whole implementation, because
/// in single-node mode nothing ever calls one.
/// </summary>
/// <remarks>
/// A single-voter group never touches a transport: the vote round returns before sending anything, pre-vote
/// is skipped, the read-index barrier answers from itself, the commit index is its own match index, and the
/// peer set is empty so there is nobody to replicate to. Consensus is complete without a wire. What this
/// class exists for is the constructor signature — <see cref="Core.RaftGroupInstance"/> requires a transport — and
/// to avoid standing up a server for a node that has nobody to talk to.
/// <para>
/// It throws <see cref="InvalidOperationException"/>, deliberately not <see cref="IOException"/>. An
/// <see cref="IOException"/> reads as "the peer is unreachable", which the replication loop treats as an
/// ordinary condition and retries forever; the group would spin quietly instead of reporting anything. A call
/// arriving here means the configuration grew past this node, which single-node mode forbids — an integrity
/// violation, and it should say so at once.
/// </para>
/// </remarks>
public sealed class SingleNodeRaftTransport : IRaftProtocolTransport
{
    /// <summary>The shared instance; it holds no state.</summary>
    public static readonly SingleNodeRaftTransport Instance = new();

    /// <inheritdoc />
    public ValueTask<AppendEntriesResponse> AppendEntriesAsync(
        NodeId target,
        AppendEntriesRequest request,
        IAsyncEnumerable<RaftLogEntry> entries,
        CancellationToken cancellationToken = default)
        => throw Unreachable(target, nameof(AppendEntriesAsync));

    /// <inheritdoc />
    public ValueTask<InstallSnapshotResponse> InstallSnapshotAsync(
        NodeId target,
        InstallSnapshotRequest request,
        IAsyncEnumerable<ReadOnlyMemory<byte>> body,
        CancellationToken cancellationToken = default)
        => throw Unreachable(target, nameof(InstallSnapshotAsync));

    /// <inheritdoc />
    public ValueTask<ReadIndexResponse> ReadIndexAsync(
        NodeId target,
        ReadIndexRequest request,
        CancellationToken cancellationToken = default)
        => throw Unreachable(target, nameof(ReadIndexAsync));

    /// <inheritdoc />
    public ValueTask<TimeoutNowResponse> TimeoutNowAsync(
        NodeId target,
        TimeoutNowRequest request,
        CancellationToken cancellationToken = default)
        => throw Unreachable(target, nameof(TimeoutNowAsync));

    /// <inheritdoc />
    public ValueTask<VoteResponse> RequestVoteAsync(
        NodeId target,
        VoteRequest request,
        CancellationToken cancellationToken = default)
        => throw Unreachable(target, nameof(RequestVoteAsync));

    private static InvalidOperationException Unreachable(NodeId target, string operation)
        => new(
            operation + " was called for node " + target + " on a single-node cluster. Nothing should ever "
            + "address a peer in this mode; reaching here means a group's configuration grew beyond this "
            + "node, which single-node mode does not allow.");
}
