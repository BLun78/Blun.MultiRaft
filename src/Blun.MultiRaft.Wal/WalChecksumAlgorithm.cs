// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

namespace Blun.MultiRaft.Wal;

/// <summary>
/// Checksum algorithm persisted with each WAL and immutable once the log is created.
/// The choice trades detection strength against framing overhead per record.
/// </summary>
public enum WalChecksumAlgorithm : byte
{
    /// <summary>IEEE 802.3 CRC-32. Fast, 4-byte overhead, well-proven for burst detection.</summary>
    Crc32 = 0,

    /// <summary>IEEE 802.3 CRC-64. Slower than CRC-32, 8-byte overhead, better for long-term data integrity.</summary>
    Crc64 = 1,

    /// <summary>xxHash 32-bit. Faster than CRC-32 on modern CPUs, same 4-byte overhead.</summary>
    XxHash32 = 2,

    /// <summary>xxHash 64-bit canonical. Stronger than 32-bit variants, 8-byte overhead.</summary>
    XxHash3 = 3,

    /// <summary>xxHash 64-bit. 8-byte overhead.</summary>
    XxHash64 = 4,

    /// <summary>xxHash 128-bit. Strongest available, 16-byte overhead.</summary>
    XxHash128 = 5
}
