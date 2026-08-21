// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Globalization;
using Blun.MultiRaft.Wal.Checksum;

namespace Blun.MultiRaft.Wal;

/// <summary>Tuning for <see cref="SegmentedRaftWal"/>.</summary>
public sealed class SegmentedRaftWalOptions
{
    /// <summary>
    /// The message size ceiling this log is dimensioned for: 1024 KB. Larger payloads are not transported
    /// inline — they are referenced — so nothing above this ever reaches the log as a single record.
    /// </summary>
    public const int MaxMessageBytes = 1024 * 1024;

    /// <summary>
    /// Room above <see cref="MaxMessageBytes"/> for whatever wraps a message on its way into a log record:
    /// headers, routing metadata, the command envelope. Without it a maximum-size message would be rejected
    /// by the very limit that was meant to admit it.
    /// </summary>
    public const int EnvelopeHeadroomBytes = 16 * 1024;

    /// <summary>
    /// Byte size at which a segment is rolled. Segments are the unit of head truncation, so this trades
    /// truncation granularity against file count. The default holds a few dozen maximum-size messages:
    /// small enough that a drained queue gets its space back promptly, large enough that a log of large
    /// messages does not roll on nearly every append.
    /// </summary>
    public long SegmentSizeBytes { get; init; } = 32L * 1024 * 1024;

    /// <summary>
    /// Whether <see cref="IRaftWal.FlushAsync"/> issues a real fsync. Off is for tests and benchmarks only:
    /// with it off a committed entry is not actually durable and the Raft guarantee is void.
    /// </summary>
    public bool FlushToDisk { get; init; } = true;

    /// <summary>
    /// How the active segment is written: one syscall per append, or a preallocated memory-mapped view.
    /// Defaults to <see cref="WalSegmentAccess.RandomAccess"/>, because with one log per queue the address
    /// space a mapping reserves is charged per group, and most groups are idle.
    /// </summary>
    public WalSegmentAccess SegmentAccess { get; init; } = WalSegmentAccess.RandomAccess;

    /// <summary>
    /// Checksum algorithm used for on-disk record integrity. Immutable once the log is created — the
    /// algorithm is persisted in the base file and must match on every reopen. Defaults to XxHash3.
    /// </summary>
    public WalChecksumAlgorithm ChecksumAlgorithm { get; init; } = WalChecksumAlgorithm.XxHash3;

    /// <summary>
    /// Largest net message this log accepts from its consumer, defaulting to <see cref="MaxMessageBytes"/>.
    /// This is what the caller may hand to <see cref="IRaftWal.AppendAsync(RaftEntryHeader, ReadOnlyMemory{byte}, CancellationToken)"/> —
    /// the record actually written
    /// to disk is larger, because <see cref="EffectiveMaxPayloadBytes"/> adds <see cref="EnvelopeHeadroomBytes"/>
    /// for whatever wraps the message on its way into a log record.
    /// </summary>
    public int MaxPayloadBytes { get; init; } = MaxMessageBytes;

    /// <summary>
    /// The actual on-disk record payload cap: <see cref="MaxPayloadBytes"/> plus
    /// <see cref="EnvelopeHeadroomBytes"/>. It also bounds the recovery scan's trust in a corrupt length
    /// field, which would otherwise ask for a multi-gigabyte buffer off a single flipped bit.
    /// </summary>
    internal int EffectiveMaxPayloadBytes => MaxPayloadBytes + EnvelopeHeadroomBytes;

    /// <summary>
    /// Size of the per-log buffer a single append is framed into. Appends larger than this borrow from the
    /// array pool instead.
    /// </summary>
    /// <remarks>
    /// Deliberately unrelated to <see cref="MaxPayloadBytes"/>. Sizing it to the maximum would charge every
    /// group a megabyte for a message size almost none of them will ever see — with one log per queue that
    /// is gigabytes of idle buffer, which is the exact footprint this design exists to avoid. The default
    /// covers ordinary queue commands, which are the ones that need to stay allocation-free.
    /// </remarks>
    public int ScratchBufferBytes { get; init; } = 8 * 1024;

    /// <summary>Rejects combinations that cannot work, at open time rather than at the first large append.</summary>
    internal void Validate()
    {
        if (MaxPayloadBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxPayloadBytes), MaxPayloadBytes, "Must be positive.");
        }

        int checksumSize = WalChecksumStrategy.ChecksumSizeFor(ChecksumAlgorithm);
        long largestRecord = RaftWalRecord.SizeOf(EffectiveMaxPayloadBytes, checksumSize);
        if (SegmentSizeBytes < largestRecord)
        {
            // A record that cannot fit a segment would roll forever without ever being written.
            throw new ArgumentOutOfRangeException(
                nameof(SegmentSizeBytes),
                SegmentSizeBytes,
                "A segment must hold at least one maximum-size record (" +
                largestRecord.ToString(CultureInfo.InvariantCulture) + " bytes).");
        }

        if (ScratchBufferBytes < RaftEntryHeader.Size + checksumSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ScratchBufferBytes),
                ScratchBufferBytes,
                "Must be large enough for an empty record's framing.");
        }
    }
}
