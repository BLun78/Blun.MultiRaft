// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

namespace Blun.MultiRaft.Core;

/// <summary>Role of a member within a single Raft group. Backed by <see cref="byte"/> so role transitions
/// are plain <see cref="System.Threading.Interlocked"/> operations on an <see cref="int"/> field.</summary>
public enum RaftRole : byte
{
    /// <summary>Replicates but does not vote and is not counted for quorum. The catch-up state a joining
    /// node occupies until its log is close enough to the leader's to be promoted (Raft §4.2.1).</summary>
    Learner = 0,

    Follower = 1,
    Candidate = 2,
    Leader = 3,
}
