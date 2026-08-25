using System.Buffers.Binary;
using System.IO.Hashing;

namespace Blun.MultiRaft.Wal.Checksum;

internal sealed class Crc32ChecksumStrategy : IWalChecksumStrategy
{
    public static readonly Crc32ChecksumStrategy Instance = new();

    public int ChecksumSize => 4;

    public void WriteChecksum(ReadOnlySpan<byte> data, Span<byte> destination, int checksumOffset)
    {
        uint crc = Crc32.HashToUInt32(data);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(checksumOffset, 4), crc);
    }

    public bool VerifyChecksum(ReadOnlySpan<byte> record, int coveredLength)
    {
        uint expected = BinaryPrimitives.ReadUInt32LittleEndian(record.Slice(coveredLength, 4));
        return Crc32.HashToUInt32(record[..coveredLength]) == expected;
    }
}