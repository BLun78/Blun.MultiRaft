// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Transport;

/// <summary>
/// Node-to-node communication, as seen by a Raft group. The group knows nothing about sockets, connections
/// or serialization — it names a target node and hands over a stream of entries.
/// </summary>
/// <remarks>
/// Entries arrive and depart as <see cref="IAsyncEnumerable{T}"/> rather than a materialized batch. That is
/// what lets the leader read from its log and write to the wire in lockstep: a follower that is a hundred
/// thousand entries behind is streamed to, not buffered for.
/// <para>
/// One transport instance serves every group on the node. Group identity travels in the message, because the
/// alternative — one connection or stream per group — is exactly what makes naive multi-Raft collapse once a
/// node hosts thousands of them.
/// </para>
/// </remarks>
public interface IRaftProtocolTransport
{
    /// <summary>
    /// Sends <c>AppendEntries</c> and streams <paramref name="entries"/> to the target.
    /// The entries' payloads may alias buffers the caller recycles as the enumeration advances, so an
    /// implementation must write each one out before requesting the next.
    /// </summary>
    ValueTask<AppendEntriesResponse> AppendEntriesAsync(
        NodeId target,
        AppendEntriesRequest request,
        IAsyncEnumerable<RaftLogEntry> entries,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Ships a snapshot to a follower that has fallen behind the leader's retained log. The body streams for
    /// the same reason entries do: it is unbounded in size, and buffering it would put the largest object in
    /// the system on the heap of the node least able to spare it.
    /// </summary>
    ValueTask<InstallSnapshotResponse> InstallSnapshotAsync(
        NodeId target,
        InstallSnapshotRequest request,
        IAsyncEnumerable<ReadOnlyMemory<byte>> body,
        CancellationToken cancellationToken = default);

    /// <summary>Asks a leader for the index a linearizable read must wait for.</summary>
    ValueTask<ReadIndexResponse> ReadIndexAsync(
        NodeId target,
        ReadIndexRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>Tells a target to start an election immediately, skipping its timer (Raft §3.10).</summary>
    ValueTask<TimeoutNowResponse> TimeoutNowAsync(
        NodeId target,
        TimeoutNowRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>Sends <c>RequestVote</c>, or its pre-vote probe.</summary>
    ValueTask<VoteResponse> RequestVoteAsync(
        NodeId target,
        VoteRequest request,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// The receiving half: what a transport calls when a message arrives for a group on this node.
/// Implemented by the host, which routes by <see cref="RaftGroupId"/>.
/// </summary>
public interface IRaftProtocolListener
{
    /// <summary>
    /// Handles an inbound <c>AppendEntries</c>. The <paramref name="entries"/> stream is read once, in order,
    /// and its payloads are only valid until the next <c>MoveNextAsync</c>.
    /// </summary>
    ValueTask<AppendEntriesResponse> OnAppendEntriesAsync(
        AppendEntriesRequest request,
        IAsyncEnumerable<RaftLogEntry> entries,
        CancellationToken cancellationToken = default);

    /// <summary>Handles an inbound <c>InstallSnapshot</c>.</summary>
    ValueTask<InstallSnapshotResponse> OnInstallSnapshotAsync(
        InstallSnapshotRequest request,
        IAsyncEnumerable<ReadOnlyMemory<byte>> body,
        CancellationToken cancellationToken = default);

    /// <summary>Handles an inbound <c>ReadIndex</c>.</summary>
    ValueTask<ReadIndexResponse> OnReadIndexAsync(
        ReadIndexRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>Handles an inbound <c>TimeoutNow</c>.</summary>
    ValueTask<TimeoutNowResponse> OnTimeoutNowAsync(
        TimeoutNowRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>Handles an inbound <c>RequestVote</c> or pre-vote probe.</summary>
    ValueTask<VoteResponse> OnRequestVoteAsync(
        VoteRequest request,
        CancellationToken cancellationToken = default);
}
