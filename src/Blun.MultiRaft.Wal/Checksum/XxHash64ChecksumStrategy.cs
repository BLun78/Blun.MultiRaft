using System.Buffers.Binary;
using System.IO.Hashing;

namespace Blun.MultiRaft.Wal.Checksum;

internal sealed class XxHash64ChecksumStrategy : IWalChecksumStrategy
{
    public static readonly XxHash64ChecksumStrategy Instance = new();

    public int ChecksumSize => 8;

    public void WriteChecksum(ReadOnlySpan<byte> data, Span<byte> destination, int checksumOffset)
    {
        ulong hash = XxHash64.HashToUInt64(data);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(checksumOffset, 8), hash);
    }

    public bool VerifyChecksum(ReadOnlySpan<byte> record, int coveredLength)
    {
        ulong expected = BinaryPrimitives.ReadUInt64LittleEndian(record.Slice(coveredLength, 8));
        return XxHash64.HashToUInt64(record[..coveredLength]) == expected;
    }
}