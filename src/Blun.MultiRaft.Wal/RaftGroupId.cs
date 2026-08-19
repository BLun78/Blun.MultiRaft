// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Globalization;

namespace Blun.MultiRaft.Wal;

/// <summary>
/// Identity of one Raft group. In Blun.MQ this is one queue: the whole point of multi-Raft is that
/// thousands of these coexist in a process, so the id is a bare 64-bit value that fits in a register
/// and can be stamped into a frame header without allocating.
/// </summary>
/// <remarks>
/// It lives with the log rather than with the consensus core because it is what a log is keyed by:
/// <see cref="Wal.IRaftWalFactory"/> opens and deletes by group, and that has to be expressible without
/// referencing anything about elections or transports.
/// </remarks>
public readonly record struct RaftGroupId(ulong Value) : IComparable<RaftGroupId>
{
    /// <summary>
    /// Lowest id of the band reserved for the library's own groups. Everything at or above it is refused
    /// to callers creating ordinary groups.
    /// </summary>
    public const ulong ReservedFloor = ulong.MaxValue - 255;

    /// <summary>
    /// The one cluster-management group: node membership, and whatever administrative state the host layers
    /// on top of it. Exactly one exists per cluster.
    /// </summary>
    /// <remarks>
    /// <see cref="ulong.MaxValue"/> rather than 0, and the choice is not cosmetic: 0 is
    /// <c>default(RaftGroupId)</c>, so a struct that was never initialized would silently address the
    /// administrative plane. No caller reaches <see cref="ulong.MaxValue"/> by accident. It also formats as
    /// exactly twenty digits, so it still fits the zero-padded filenames the segmented log and the meta store
    /// generate without widening them.
    /// </remarks>
    public static RaftGroupId Cluster => new(ulong.MaxValue);

    /// <summary>Whether this id falls in the band the library keeps for itself.</summary>
    public bool IsReserved => Value >= ReservedFloor;

    public int CompareTo(RaftGroupId other) => Value.CompareTo(other.Value);

    public override string ToString() => Value.ToString(CultureInfo.InvariantCulture);
}
