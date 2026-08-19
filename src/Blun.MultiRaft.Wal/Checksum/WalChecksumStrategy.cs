// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Runtime.CompilerServices;

namespace Blun.MultiRaft.Wal.Checksum;

/// <summary>Factory that maps <see cref="WalChecksumAlgorithm"/> to the matching <see cref="IWalChecksumStrategy"/>.</summary>
internal static class WalChecksumStrategy
{
    public static IWalChecksumStrategy For(WalChecksumAlgorithm algorithm) => algorithm switch
    {
        WalChecksumAlgorithm.Crc32 => Crc32ChecksumStrategy.Instance,
        WalChecksumAlgorithm.Crc64 => Crc64ChecksumStrategy.Instance,
        WalChecksumAlgorithm.XxHash32 => XxHash32ChecksumStrategy.Instance,
        WalChecksumAlgorithm.XxHash3 => XxHash3ChecksumStrategy.Instance,
        WalChecksumAlgorithm.XxHash64 => XxHash64ChecksumStrategy.Instance,
        WalChecksumAlgorithm.XxHash128 => XxHash128ChecksumStrategy.Instance,
        _ => throw new ArgumentOutOfRangeException(nameof(algorithm), algorithm, "Unknown checksum algorithm."),
    };

    /// <summary>Returns the byte width for the given <paramref name="algorithm"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ChecksumSizeFor(WalChecksumAlgorithm algorithm) => algorithm switch
    {
        WalChecksumAlgorithm.Crc32 => 4,
        WalChecksumAlgorithm.Crc64 => 8,
        WalChecksumAlgorithm.XxHash32 => 4,
        WalChecksumAlgorithm.XxHash3 => 8,
        WalChecksumAlgorithm.XxHash64 => 8,
        WalChecksumAlgorithm.XxHash128 => 16,
        _ => throw new ArgumentOutOfRangeException(nameof(algorithm), algorithm, "Unknown checksum algorithm."),
    };
}