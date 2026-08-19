// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Blun.MultiRaft.Wal.Checksum;

namespace Blun.MultiRaft.Wal;

/// <summary>
/// On-disk framing shared by the segmented log and its recovery scan.
/// Layout: <c>[header 32][payload N][checksum C][padding to 8]</c>. The checksum covers header and payload
/// and exists only to find the tear at the end of a log that was cut off by a crash — a partially written
/// record is indistinguishable from a complete one without it.
/// </summary>
internal static class RaftWalRecord
{
    public const int Alignment = 8;

    /// <summary>Total on-disk size of a record with the given payload length, including checksum and alignment padding.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int SizeOf(int payloadLength, int checksumSize)
    {
        int raw = RaftEntryHeader.Size + payloadLength + checksumSize;
        return (raw + (Alignment - 1)) & ~(Alignment - 1);
    }

    /// <summary>
    /// Writes one complete record into <paramref name="destination"/>, which must be at least
    /// <see cref="SizeOf(int,int)"/> bytes. Single pass, no intermediate buffer.
    /// </summary>
    public static int Write(
        Span<byte> destination,
        RaftEntryHeader header,
        ReadOnlySpan<byte> payload,
        IWalChecksumStrategy checksumStrategy)
    {
        int checksumSize = checksumStrategy.ChecksumSize;
        int size = SizeOf(payload.Length, checksumSize);
        Span<byte> record = destination[..size];
        record[(RaftEntryHeader.Size + payload.Length + checksumSize)..].Clear();

        header.Write(record);
        payload.CopyTo(record[RaftEntryHeader.Size..]);

        checksumStrategy.WriteChecksum(record[..(RaftEntryHeader.Size + payload.Length)], destination, RaftEntryHeader.Size + payload.Length);
        return size;
    }

    /// <summary>Verifies the checksum of a fully materialized record. Payload length comes from the header.</summary>
    public static bool Verify(ReadOnlySpan<byte> record, in RaftEntryHeader header, IWalChecksumStrategy strategy)
    {
        int checksumSize = strategy.ChecksumSize;
        int covered = RaftEntryHeader.Size + header.PayloadLength;
        if (record.Length < covered + checksumSize)
        {
            return false;
        }

        return strategy.VerifyChecksum(record, covered);
    }
}
