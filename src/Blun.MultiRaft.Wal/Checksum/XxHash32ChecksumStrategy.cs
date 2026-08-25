using System.Buffers.Binary;
using System.IO.Hashing;

namespace Blun.MultiRaft.Wal.Checksum;

internal sealed class XxHash32ChecksumStrategy : IWalChecksumStrategy
{
    public static readonly XxHash32ChecksumStrategy Instance = new();

    public int ChecksumSize => 4;

    public void WriteChecksum(ReadOnlySpan<byte> data, Span<byte> destination, int checksumOffset)
    {
        uint hash = XxHash32.HashToUInt32(data);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(checksumOffset, 4), hash);
    }

    public bool VerifyChecksum(ReadOnlySpan<byte> record, int coveredLength)
    {
        uint expected = BinaryPrimitives.ReadUInt32LittleEndian(record.Slice(coveredLength, 4));
        return XxHash32.HashToUInt32(record[..coveredLength]) == expected;
    }
}