using System.Buffers.Binary;
using System.IO.Hashing;
using System.Runtime.CompilerServices;

namespace Blun.MultiRaft.Wal.Checksum;

internal sealed class Crc64ChecksumStrategy : IWalChecksumStrategy
{
    public static readonly Crc64ChecksumStrategy Instance = new();

    public int ChecksumSize => 8;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteChecksum(ReadOnlySpan<byte> data, Span<byte> destination, int checksumOffset)
    {
        ulong crc = Crc64.HashToUInt64(data);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(checksumOffset, 8), crc);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool VerifyChecksum(ReadOnlySpan<byte> record, int coveredLength)
    {
        ulong expected = BinaryPrimitives.ReadUInt64LittleEndian(record.Slice(coveredLength, 8));
        return Crc64.HashToUInt64(record[..coveredLength]) == expected;
    }
}