namespace Blun.MultiRaft.Wal.Checksum;

/// <summary>
/// Computes and verifies the per-record checksum that guards the on-disk framing.
/// Each implementation maps to a <see cref="WalChecksumAlgorithm"/> and its native hash width.
/// </summary>
internal interface IWalChecksumStrategy
{
    /// <summary>Byte width of the checksum appended to each record.</summary>
    int ChecksumSize { get; }

    /// <summary>Computes the checksum over <paramref name="data"/> and writes it into <paramref name="destination"/> at <paramref name="checksumOffset"/>.</summary>
    void WriteChecksum(ReadOnlySpan<byte> data, Span<byte> destination, int checksumOffset);

    /// <summary>Recomputes the checksum over the first <paramref name="coveredLength"/> bytes of <paramref name="record"/>
    /// and compares it against the stored value immediately after.</summary>
    bool VerifyChecksum(ReadOnlySpan<byte> record, int coveredLength);
}