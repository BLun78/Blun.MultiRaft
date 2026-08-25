using System.Buffers.Binary;
using System.IO.Hashing;

namespace Blun.MultiRaft.Wal.Checksum;

internal sealed class Crc64ChecksumStrategy : IWalChecksumStrategy
{
    public static readonly Crc64ChecksumStrategy Instance = new();

    public int ChecksumSize => 8;

    public void WriteChecksum(ReadOnlySpan<byte> data, Span<byte> destination, int checksumOffset)
    {
        ulong crc = Crc64.HashToUInt64(data);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(checksumOffset, 8), crc);
    }

    public bool VerifyChecksum(ReadOnlySpan<byte> record, int coveredLength)
    {
        ulong expected = BinaryPrimitives.ReadUInt64LittleEndian(record.Slice(coveredLength, 8));
        return Crc64.HashToUInt64(record[..coveredLength]) == expected;
    }
}