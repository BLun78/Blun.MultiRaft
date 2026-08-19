// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using Microsoft.Extensions.Logging;

namespace Blun.MultiRaft;

public sealed partial class RaftGroupInstance
{
    /// <summary>
    /// Every log statement in this library is source-generated. The generic <c>ILogger.Log*</c> extensions
    /// take <c>params object?[]</c>, so they allocate an array and box every value type <em>before</em> the
    /// level is even checked — and a node running thousands of groups pays that on every heartbeat of every
    /// one of them. The generated methods emit the <c>IsEnabled</c> check first and are trim-safe.
    /// </summary>
    private static partial class Log
    {
        [LoggerMessage(
            EventId = 1000,
            Level = LogLevel.Information,
            Message = "Raft group {Group} started on node {Node} at term {Term}, last index {LastIndex}.")]
        public static partial void GroupStarted(ILogger logger, ulong group, ulong node, long term, long lastIndex);

        [LoggerMessage(
            EventId = 1001,
            Level = LogLevel.Debug,
            Message = "Raft group {Group}: node {Node} starting election for term {Term}.")]
        public static partial void ElectionStarted(ILogger logger, ulong group, ulong node, long term);

        [LoggerMessage(
            EventId = 1002,
            Level = LogLevel.Information,
            Message = "Raft group {Group}: node {Node} became leader for term {Term} at index {LastIndex}.")]
        public static partial void BecameLeader(ILogger logger, ulong group, ulong node, long term, long lastIndex);

        [LoggerMessage(
            EventId = 1003,
            Level = LogLevel.Information,
            Message = "Raft group {Group}: node {Node} stepped down to term {Term}.")]
        public static partial void SteppedDown(ILogger logger, ulong group, ulong node, long term);

        [LoggerMessage(
            EventId = 1004,
            Level = LogLevel.Information,
            Message = "Raft group {Group}: applied membership change {Change} for node {Node}; {VoterCount} voters now.")]
        public static partial void MembershipChanged(
            ILogger logger,
            ulong group,
            string change,
            ulong node,
            int voterCount);

        [LoggerMessage(
            EventId = 1005,
            Level = LogLevel.Debug,
            Message = "Raft group {Group}: promotion of node {Node} deferred, match {MatchIndex} of {LastIndex}.")]
        public static partial void PromotionDeferred(
            ILogger logger,
            ulong group,
            ulong node,
            long matchIndex,
            long lastIndex);

        [LoggerMessage(
            EventId = 1006,
            Level = LogLevel.Warning,
            Message = "Raft group {Group}: node {Node} needs index {NextIndex} but the log starts at {FirstIndex}; a snapshot is required.")]
        public static partial void SnapshotRequired(
            ILogger logger,
            ulong group,
            ulong node,
            long nextIndex,
            long firstIndex);

        [LoggerMessage(
            EventId = 1008,
            Level = LogLevel.Information,
            Message = "Raft group {Group}: snapshot taken through index {Index} of term {Term}.")]
        public static partial void SnapshotTaken(ILogger logger, ulong group, long index, long term);

        [LoggerMessage(
            EventId = 1009,
            Level = LogLevel.Information,
            Message = "Raft group {Group}: snapshot through index {Index} sent to node {Node}.")]
        public static partial void SnapshotSent(ILogger logger, ulong group, ulong node, long index);

        [LoggerMessage(
            EventId = 1010,
            Level = LogLevel.Information,
            Message = "Raft group {Group}: installed snapshot from node {Node} through index {Index}.")]
        public static partial void SnapshotInstalled(ILogger logger, ulong group, ulong node, long index);

        [LoggerMessage(
            EventId = 1011,
            Level = LogLevel.Error,
            Message = "Raft group {Group}: node {Node} needs a snapshot but none is stored; the log was compacted without one.")]
        public static partial void SnapshotMissing(ILogger logger, ulong group, ulong node);

        [LoggerMessage(
            EventId = 1007,
            Level = LogLevel.Warning,
            Message = "Raft group {Group}: replication to node {Node} failed.")]
        public static partial void ReplicationFailed(ILogger logger, Exception exception, ulong group, ulong node);

        [LoggerMessage(
            EventId = 1012,
            Level = LogLevel.Information,
            Message = "Raft group {Group}: leadership transferred from node {From} to node {To} in term {Term}.")]
        public static partial void LeadershipTransferred(ILogger logger, ulong group, ulong from, ulong to, long term);

        [LoggerMessage(
            EventId = 1013,
            Level = LogLevel.Warning,
            Message = "Raft group {Group}: leadership transfer to node {Node} abandoned: {Reason}.")]
        public static partial void TransferAbandoned(ILogger logger, ulong group, ulong node, string reason);

        [LoggerMessage(
            EventId = 1014,
            Level = LogLevel.Warning,
            Message = "Raft group {Group}: automatic compaction failed.")]
        public static partial void AutoCompactionFailed(ILogger logger, Exception exception, ulong group);

        [LoggerMessage(
            EventId = 1015,
            Level = LogLevel.Warning,
            Message = "Raft group {Group}: node {Node} lost contact with a majority in term {Term} and stepped aside.")]
        public static partial void QuorumContactLost(ILogger logger, ulong group, ulong node, long term);

        [LoggerMessage(
            EventId = 1016,
            Level = LogLevel.Warning,
            Message = "Raft group {Group}: an election started by node {Node} failed.")]
        public static partial void ElectionFailed(ILogger logger, Exception exception, ulong group, ulong node);
    }
}
