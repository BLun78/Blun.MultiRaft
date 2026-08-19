// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Buffers.Binary;
using System.Collections.Immutable;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft;

/// <summary>The single-server change kinds of Raft §4.1.</summary>
public enum MembershipChangeKind : byte
{
    /// <summary>Adds a node as a non-voting learner. Never changes the quorum, so it is always safe.</summary>
    AddLearner = 0,

    /// <summary>Turns a caught-up learner into a voter. The one operation that moves the quorum.</summary>
    PromoteToVoter = 1,

    /// <summary>Removes a node from the group entirely, voter or learner.</summary>
    Remove = 2,
}

/// <summary>
/// A membership change, one node at a time. Joint consensus is deliberately not implemented: with
/// single-server changes the old and new quorums always overlap, so no two-phase configuration is needed
/// and there is far less that can go wrong.
/// </summary>
public readonly record struct MembershipChange(MembershipChangeKind Kind, NodeId Node)
{
    /// <summary>Serialized size of the payload written into a <see cref="RaftEntryKind.Membership"/> entry.</summary>
    public const int PayloadSize = 9;

    /// <summary>Writes the change as the payload of a log entry. Fixed nine bytes, no serializer involved.</summary>
    public void Write(Span<byte> destination)
    {
        destination[0] = (byte)Kind;
        BinaryPrimitives.WriteUInt64LittleEndian(destination[1..], Node.Value);
    }

    /// <summary>Reads a change back out of a log entry payload.</summary>
    public static MembershipChange Read(ReadOnlySpan<byte> source)
        => new((MembershipChangeKind)source[0], new NodeId(BinaryPrimitives.ReadUInt64LittleEndian(source[1..])));

    /// <summary>Allocates the nine-byte payload for this change.</summary>
    public byte[] ToPayload()
    {
        var payload = new byte[PayloadSize];
        Write(payload);
        return payload;
    }
}

/// <summary>
/// The configuration of one Raft group: who votes, and who is only catching up.
/// Immutable and swapped atomically, so a reader never sees a half-applied change and no lock is needed on
/// the read path — which matters because quorum is recomputed on every single match-index update.
/// </summary>
public sealed record RaftMembership
{
    /// <summary>An empty configuration — the state of a group that has not been bootstrapped yet.</summary>
    public static readonly RaftMembership Empty = new([], []);

    /// <summary>Creates a configuration from explicit voter and learner sets.</summary>
    public RaftMembership(ImmutableArray<NodeId> voters, ImmutableArray<NodeId> learners)
    {
        Voters = voters;
        Learners = learners;
    }

    /// <summary>Nodes that vote and count towards quorum.</summary>
    public ImmutableArray<NodeId> Voters { get; }

    /// <summary>Nodes that replicate but neither vote nor count towards quorum (Raft §4.2.1).</summary>
    public ImmutableArray<NodeId> Learners { get; }

    /// <summary>Number of matching replicas needed to commit. A single-node group commits on itself alone.</summary>
    public int QuorumSize => (Voters.Length / 2) + 1;

    /// <summary>Everyone the leader replicates to, voters and learners alike.</summary>
    public IEnumerable<NodeId> AllMembers => Voters.Concat(Learners);

    /// <summary>Creates the initial configuration of a group from a voter set.</summary>
    public static RaftMembership OfVoters(params ReadOnlySpan<NodeId> voters)
        => new([.. voters], []);

    public bool IsVoter(NodeId node) => Voters.Contains(node);

    public bool IsLearner(NodeId node) => Learners.Contains(node);

    public bool Contains(NodeId node) => IsVoter(node) || IsLearner(node);

    /// <summary>
    /// Serializes the configuration so it can travel inside a snapshot. A follower that installed a snapshot
    /// has discarded its log, and the log is where membership changes lived — without carrying the
    /// configuration along, it would have no way to learn who is in the group, or that it no longer is.
    /// </summary>
    public byte[] Serialize()
    {
        var buffer = new byte[8 + ((Voters.Length + Learners.Length) * 8)];
        Span<byte> span = buffer;
        BinaryPrimitives.WriteInt32LittleEndian(span, Voters.Length);
        BinaryPrimitives.WriteInt32LittleEndian(span[4..], Learners.Length);

        int offset = 8;
        foreach (NodeId voter in Voters)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(span[offset..], voter.Value);
            offset += 8;
        }

        foreach (NodeId learner in Learners)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(span[offset..], learner.Value);
            offset += 8;
        }

        return buffer;
    }

    /// <summary>Reads back what <see cref="Serialize"/> wrote. Returns <see cref="Empty"/> for empty input.</summary>
    public static RaftMembership Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < 8)
        {
            return Empty;
        }

        int voterCount = BinaryPrimitives.ReadInt32LittleEndian(source);
        int learnerCount = BinaryPrimitives.ReadInt32LittleEndian(source[4..]);
        if (voterCount < 0 || learnerCount < 0 || source.Length < 8 + ((voterCount + learnerCount) * 8))
        {
            return Empty;
        }

        var voters = ImmutableArray.CreateBuilder<NodeId>(voterCount);
        var learners = ImmutableArray.CreateBuilder<NodeId>(learnerCount);

        int offset = 8;
        for (int i = 0; i < voterCount; i++, offset += 8)
        {
            voters.Add(new NodeId(BinaryPrimitives.ReadUInt64LittleEndian(source[offset..])));
        }

        for (int i = 0; i < learnerCount; i++, offset += 8)
        {
            learners.Add(new NodeId(BinaryPrimitives.ReadUInt64LittleEndian(source[offset..])));
        }

        return new RaftMembership(voters.ToImmutable(), learners.ToImmutable());
    }

    /// <summary>Applies a change, returning the resulting configuration. Idempotent: re-applying is a no-op.</summary>
    public RaftMembership Apply(MembershipChange change) => change.Kind switch
    {
        MembershipChangeKind.AddLearner when !Contains(change.Node)
            => new RaftMembership(Voters, Learners.Add(change.Node)),
        MembershipChangeKind.PromoteToVoter when IsLearner(change.Node)
            => new RaftMembership(Voters.Add(change.Node), Learners.Remove(change.Node)),
        MembershipChangeKind.PromoteToVoter when !Contains(change.Node)
            => new RaftMembership(Voters.Add(change.Node), Learners),
        MembershipChangeKind.Remove
            => new RaftMembership(Voters.Remove(change.Node), Learners.Remove(change.Node)),
        _ => this,
    };
}
