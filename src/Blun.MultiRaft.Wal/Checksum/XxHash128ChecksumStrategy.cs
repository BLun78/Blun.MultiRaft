using System.Buffers.Binary;
using System.IO.Hashing;
using System.Runtime.CompilerServices;

namespace Blun.MultiRaft.Wal.Checksum;

internal sealed class XxHash128ChecksumStrategy : IWalChecksumStrategy
{
    public static readonly XxHash128ChecksumStrategy Instance = new();

    public int ChecksumSize => 16;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteChecksum(ReadOnlySpan<byte> data, Span<byte> destination, int checksumOffset)
    {
        UInt128 hash = XxHash128.HashToUInt128(data);
        BinaryPrimitives.WriteUInt128LittleEndian(destination.Slice(checksumOffset, 16), hash);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool VerifyChecksum(ReadOnlySpan<byte> record, int coveredLength)
    {
        UInt128 expected = BinaryPrimitives.ReadUInt128LittleEndian(record.Slice(coveredLength, 16));
        return XxHash128.HashToUInt128(record[..coveredLength]) == expected;
    }
}