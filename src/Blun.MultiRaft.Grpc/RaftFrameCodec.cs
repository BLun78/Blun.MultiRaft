// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using Blun.MultiRaft.Grpc.Protocol;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;
using Google.Protobuf;

namespace Blun.MultiRaft.Grpc;

/// <summary>Translation between the protocol buffers on the wire and the library's own types.</summary>
/// <remarks>
/// Deliberately the only place that knows both. The consensus core never sees a generated type, which is
/// what keeps a second transport — or a replacement for this one — from being a rewrite.
/// <para>
/// Every byte payload crossing into a generated message is <em>copied</em>, never wrapped. A frame built here
/// is queued to <see cref="RaftStreamSession"/>'s outbound channel and serialised later, on a different task,
/// so the frame outlives the call that produced it — while its sources are explicitly recycled buffers:
/// <see cref="IRaftWal.ReadFromAsync"/> hands out slices of a pooled buffer it reuses on every
/// <c>MoveNextAsync</c>, and <see cref="IRaftSnapshotStore.ReadAsync"/> refills one chunk array. This codec
/// cannot see how long a frame will live, so it cannot judge when wrapping would be safe, and the one case
/// where it guessed wrong is the defect this rule exists to prevent — see the note on <see cref="ToProto(in RaftLogEntry)"/>.
/// </para>
/// </remarks>
internal static class RaftFrameCodec
{
    public static AppendEntries ToProto(in AppendEntriesRequest request)
        => new()
        {
            Term = request.Term,
            Leader = request.Leader.Value,
            PrevLogIndex = request.PrevLogIndex,
            PrevLogTerm = request.PrevLogTerm,
            LeaderCommit = request.LeaderCommit,
        };

    public static AppendEntriesRequest ToDomain(RaftGroupId group, AppendEntries message)
        => new(
            group,
            message.Term,
            new NodeId(message.Leader),
            message.PrevLogIndex,
            message.PrevLogTerm,
            message.LeaderCommit);

    /// <remarks>
    /// The payload copy is the one unavoidable cost on this path, and it is load-bearing rather than
    /// defensive. <see cref="IRaftWal.ReadFromAsync"/> documents that the payload it yields is a slice of a
    /// buffer the log owns and recycles on the next iteration, and the replication loop hands that enumerable
    /// straight to the transport. <see cref="RaftStreamSession.AppendEntriesAsync"/> drains the whole round
    /// into one frame before sending, so wrapping the memory instead of copying it left every entry in the
    /// batch pointing at the same recycled slot: by serialisation time they all carried the <em>last</em>
    /// entry's bytes. Headers were copied by value and so stayed correct, which is what made it silent —
    /// indices and terms lined up, the follower's consistency check passed, entries were appended, and only
    /// the contents were wrong. In the cluster group that surfaced as a follower applying one membership
    /// change several times over and never seeing the others, so two nodes disagreed about who votes.
    /// <para>
    /// It was invisible in process because <c>InMemoryRaftTransport</c> detaches entries with
    /// <see cref="RaftLogEntry.ToOwned"/> — the in-memory transport honoured the contract that this one broke,
    /// so no test on either WAL implementation could see it.
    /// </para>
    /// </remarks>
    public static LogEntry ToProto(in RaftLogEntry entry)
        => new()
        {
            Term = entry.Header.Term,
            Index = entry.Header.Index,
            TimestampTicks = entry.Header.TimestampTicks,
            Kind = (uint)entry.Header.Kind,
            ApplicationTag = entry.Header.ApplicationTag,
            Payload = ByteString.CopyFrom(entry.Payload.Span),
        };

    public static RaftLogEntry ToDomain(LogEntry entry)
    {
        byte[] payload = entry.Payload.ToByteArray();
        return new RaftLogEntry(
            new RaftEntryHeader(
                entry.Term,
                entry.Index,
                (RaftEntryKind)entry.Kind,
                payload.Length,
                entry.TimestampTicks,
                (byte)entry.ApplicationTag),
            payload);
    }

    public static AppendEntriesReply ToProto(in AppendEntriesResponse response)
        => new()
        {
            Term = response.Term,
            Success = response.Success,
            MatchIndex = response.MatchIndex,
            ConflictIndex = response.ConflictIndex,
        };

    public static AppendEntriesResponse ToDomain(AppendEntriesReply reply)
        => new(reply.Term, reply.Success, reply.MatchIndex, reply.ConflictIndex);

    public static Vote ToProto(in VoteRequest request)
        => new()
        {
            Term = request.Term,
            Candidate = request.Candidate.Value,
            LastLogIndex = request.LastLogIndex,
            LastLogTerm = request.LastLogTerm,
            PreVote = request.PreVote,
        };

    public static VoteRequest ToDomain(RaftGroupId group, Vote message)
        => new(
            group,
            message.Term,
            new NodeId(message.Candidate),
            message.LastLogIndex,
            message.LastLogTerm,
            message.PreVote);

    public static VoteReply ToProto(in VoteResponse response)
        => new() { Term = response.Term, Granted = response.Granted };

    public static VoteResponse ToDomain(VoteReply reply) => new(reply.Term, reply.Granted);

    public static InstallSnapshot ToProto(in InstallSnapshotRequest request)
        => new()
        {
            Term = request.Term,
            Leader = request.Leader.Value,
            LastIncludedIndex = request.LastIncludedIndex,
            LastIncludedTerm = request.LastIncludedTerm,
            Configuration = ByteString.CopyFrom(request.Configuration.Span),
        };

    public static InstallSnapshotRequest ToDomain(RaftGroupId group, InstallSnapshot message)
        => new(
            group,
            message.Term,
            new NodeId(message.Leader),
            message.LastIncludedIndex,
            message.LastIncludedTerm,
            message.Configuration.ToByteArray());

    public static ReadIndex ToProto(in ReadIndexRequest request)
        => new() { Term = request.Term };

    public static ReadIndexRequest ToDomain(RaftGroupId group, ReadIndex message)
        => new(group, message.Term);

    public static ReadIndexReply ToProto(in ReadIndexResponse response)
        => new()
        {
            Term = response.Term,
            Success = response.Success,
            ReadIndex = response.ReadIndex,
            Leader = response.Leader,
        };

    public static ReadIndexResponse ToDomain(ReadIndexReply reply)
        => new(reply.Term, reply.Success, reply.ReadIndex, reply.Leader);

    public static TimeoutNow ToProto(in TimeoutNowRequest request)
        => new() { Term = request.Term };

    public static TimeoutNowRequest ToDomain(RaftGroupId group, TimeoutNow message)
        => new(group, message.Term);

    public static TimeoutNowReply ToProto(in TimeoutNowResponse response)
        => new() { Term = response.Term, Accepted = response.Accepted };

    public static TimeoutNowResponse ToDomain(TimeoutNowReply reply)
        => new(reply.Term, reply.Accepted);

    public static InstallSnapshotReply ToProto(in InstallSnapshotResponse response)
        => new() { Term = response.Term, Success = response.Success };

    public static InstallSnapshotResponse ToDomain(InstallSnapshotReply reply)
        => new(reply.Term, reply.Success);

    public static NodeLoad ToProto(in NodeLoadReport report)
        => new()
        {
            Node = report.Node.Value,
            Sequence = report.Sequence,
            GroupCount = report.GroupCount,
            LeaderCount = report.LeaderCount,
        };

    public static NodeLoadReport ToDomain(NodeLoad message)
        => new(new NodeId(message.Node), message.Sequence, message.GroupCount, message.LeaderCount);

    public static LeaderTarget ToProto(in LeaderTargetRequest request)
        => new()
        {
            Candidate = request.Candidate?.Value ?? 0,
            HasCandidate = request.Candidate is not null,
            Execute = request.Execute,
        };

    public static LeaderTargetRequest ToDomain(RaftGroupId group, LeaderTarget message)
        => new(group, message.HasCandidate ? new NodeId(message.Candidate) : null, message.Execute);

    public static LeaderTargetReply ToProto(in LeaderTargetResponse response)
        => new()
        {
            Node = response.Node.Value,
            Status = (uint)response.Status,
            Lag = response.Lag,
            Transferred = response.Transferred,
        };

    public static LeaderTargetResponse ToDomain(LeaderTargetReply reply)
        => new(new NodeId(reply.Node), (LeaderTargetStatus)reply.Status, reply.Lag, reply.Transferred);
}
