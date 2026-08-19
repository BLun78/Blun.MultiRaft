// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Buffers;
using System.Globalization;
using Blun.MultiRaft.Wal.Checksum;
using Microsoft.Win32.SafeHandles;

namespace Blun.MultiRaft.Wal;

/// <summary>
/// One file of the segmented log. Owns a write device while it is the active segment and nothing but a path
/// once it has been rolled — an idle segment costs no file descriptor and no mapping, which is what lets a
/// node hold thousands of groups open at once.
/// </summary>
/// <remarks>
/// The index map is two parallel <see cref="List{T}"/>s rather than a list of tuples so the term lookup —
/// the hot one, hit on every AppendEntries consistency check — touches one cache line of <see cref="long"/>s
/// instead of striding over a struct.
/// </remarks>
internal sealed class WalSegment : IDisposable
{
    private readonly List<long> _offsets;
    private readonly List<long> _terms;
    private readonly List<int> _sizes;
    private readonly SegmentedRaftWalOptions _options;

    private IWalSegmentDevice? _device;
    private long _length;

    private WalSegment(
        string path,
        long firstIndex,
        long length,
        SegmentedRaftWalOptions options,
        List<long> offsets,
        List<long> terms,
        List<int> sizes)
    {
        Path = path;
        FirstIndex = firstIndex;
        _length = length;
        _options = options;
        _offsets = offsets;
        _terms = terms;
        _sizes = sizes;
    }

    public string Path { get; }

    public long FirstIndex { get; }

    public int Count => _offsets.Count;

    /// <summary>Highest index in this segment, or <see cref="FirstIndex"/> - 1 when it is empty.</summary>
    public long LastIndex => FirstIndex + _offsets.Count - 1;

    /// <summary>Bytes of valid data written so far. Not the file size: a mapped segment is preallocated.</summary>
    public long Length => _length;

    /// <summary>True when recovery stopped early inside this file — nothing may follow it.</summary>
    public bool Torn { get; private init; }

    public static WalSegment Create(string directory, long firstIndex, SegmentedRaftWalOptions options)
    {
        string path = System.IO.Path.Combine(
            directory,
            firstIndex.ToString("D20", CultureInfo.InvariantCulture) + SegmentedRaftWal.SegmentExtension);
        var segment = new WalSegment(path, firstIndex, 0, options, [], [], []);
        segment.SetActive(true);
        return segment;
    }

    public static async ValueTask<WalSegment> RecoverAsync(
        string path,
        SegmentedRaftWalOptions options,
        IWalChecksumStrategy checksum,
        CancellationToken cancellationToken)
    {
        string name = System.IO.Path.GetFileNameWithoutExtension(path);
        if (!long.TryParse(name, NumberStyles.None, CultureInfo.InvariantCulture, out long firstIndex))
        {
            return new WalSegment(path, 0, 0, options, [], [], []) { Torn = true };
        }

        var offsets = new List<long>();
        var terms = new List<long>();
        var sizes = new List<int>();
        long good = 0;
        bool torn = false;

        using SafeFileHandle handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        long fileLength = RandomAccess.GetLength(handle);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            long offset = 0;
            long expected = firstIndex;
            while (offset + RaftEntryHeader.Size <= fileLength)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int probe = await RandomAccess
                    .ReadAsync(handle, buffer.AsMemory(0, RaftEntryHeader.Size), offset, cancellationToken)
                    .ConfigureAwait(false);
                if (probe < RaftEntryHeader.Size)
                {
                    torn = true;
                    break;
                }

                RaftEntryHeader header = RaftEntryHeader.Read(buffer);

                // An all-zero header is the unwritten remainder of a preallocated (memory-mapped) segment,
                // not damage. Reading it as a tear would make recovery delete every segment after this one.
                if (header.Term == 0 && header.Index == 0 && header.PayloadLength == 0)
                {
                    break;
                }

                // A corrupt length field must never be trusted into an allocation, so it is bounded by the
                // same limit the write path enforces before it is used for anything.
                if (header.PayloadLength < 0
                    || header.PayloadLength > options.MaxPayloadBytes
                    || header.Index != expected
                    || header.Term <= 0)
                {
                    torn = true;
                    break;
                }

                int size = RaftWalRecord.SizeOf(header.PayloadLength, checksum.ChecksumSize);
                if (offset + size > fileLength)
                {
                    torn = true;
                    break;
                }

                if (size > buffer.Length)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                    buffer = ArrayPool<byte>.Shared.Rent(size);
                }

                int read = await RandomAccess
                    .ReadAsync(handle, buffer.AsMemory(0, size), offset, cancellationToken)
                    .ConfigureAwait(false);
                if (read < size || !RaftWalRecord.Verify(buffer.AsSpan(0, size), in header, checksum))
                {
                    torn = true;
                    break;
                }

                offsets.Add(offset);
                terms.Add(header.Term);
                sizes.Add(size);
                offset += size;
                good = offset;
                expected++;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        if (torn && good < fileLength)
        {
            // Cut the tear off now rather than leaving it to be re-scanned — and so the next append starts
            // from a byte offset that is actually the end of valid data. A preallocated tail is left alone:
            // shrinking it would only force the file to be grown again on the next append.
            using SafeFileHandle trim = File.OpenHandle(path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);
            RandomAccess.SetLength(trim, good);
        }

        return new WalSegment(path, firstIndex, good, options, offsets, terms, sizes) { Torn = torn };
    }

    /// <summary>
    /// Opens or closes the write device. Only the newest segment keeps one; sealing a rolled segment also
    /// trims a preallocated file back to the bytes it actually holds, so the disk cost of a mapped log is
    /// the same as an unmapped one everywhere except the single segment being written.
    /// </summary>
    public void SetActive(bool active)
    {
        if (active)
        {
            _device ??= _options.SegmentAccess == WalSegmentAccess.MemoryMapped
                ? new MemoryMappedSegmentDevice(Path, _options.SegmentSizeBytes)
                : new RandomAccessSegmentDevice(Path);
            return;
        }

        if (_device is null)
        {
            return;
        }

        _device.Dispose();
        _device = null;
        TrimToLength();
    }

    public SafeFileHandle OpenRead()
        => File.OpenHandle(Path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, FileOptions.Asynchronous);

    public long TermAt(long index)
    {
        int slot = (int)(index - FirstIndex);
        return (uint)slot < (uint)_terms.Count ? _terms[slot] : -1L;
    }

    public bool TryLocate(long index, out long offset, out int size)
    {
        int slot = (int)(index - FirstIndex);
        if ((uint)slot >= (uint)_offsets.Count)
        {
            offset = 0;
            size = 0;
            return false;
        }

        offset = _offsets[slot];
        size = _sizes[slot];
        return true;
    }

    public async ValueTask AppendAsync(
        ReadOnlyMemory<byte> record,
        long term,
        int size,
        CancellationToken cancellationToken)
    {
        IWalSegmentDevice device = _device ?? throw new InvalidOperationException("Segment is not active.");
        await device.WriteAsync(record, _length, cancellationToken).ConfigureAwait(false);
        _offsets.Add(_length);
        _terms.Add(term);
        _sizes.Add(size);
        _length += size;
    }

    public async ValueTask AppendBatchAsync(
        ReadOnlyMemory<byte> packed,
        ReadOnlyMemory<RaftLogEntry> entries,
        CancellationToken cancellationToken)
    {
        IWalSegmentDevice device = _device ?? throw new InvalidOperationException("Segment is not active.");
        await device.WriteAsync(packed, _length, cancellationToken).ConfigureAwait(false);

        long offset = _length;
        for (int i = 0; i < entries.Length; i++)
        {
            ref readonly RaftLogEntry entry = ref entries.Span[i];
            int size = RaftWalRecord.SizeOf(entry.Payload.Length, WalChecksumStrategy.ChecksumSizeFor(_options.ChecksumAlgorithm));
            _offsets.Add(offset);
            _terms.Add(entry.Term);
            _sizes.Add(size);
            offset += size;
        }

        _length = offset;
    }

    /// <summary>fsync. The single most expensive call in the system, hence never once per entry.</summary>
    public void FlushToDisk() => _device?.FlushToDisk();

    /// <summary>Drops <paramref name="fromIndex"/> and everything after it, shrinking the file to match.</summary>
    public void TruncateFrom(long fromIndex)
    {
        int slot = (int)(fromIndex - FirstIndex);
        if (slot < 0 || slot >= _offsets.Count)
        {
            return;
        }

        _length = _offsets[slot];
        _offsets.RemoveRange(slot, _offsets.Count - slot);
        _terms.RemoveRange(slot, _terms.Count - slot);
        _sizes.RemoveRange(slot, _sizes.Count - slot);

        // The device has to go before the file can be resized — a mapping pins the file's length.
        bool wasActive = _device is not null;
        SetActive(false);
        if (wasActive)
        {
            SetActive(true);
        }
    }

    public void Delete()
    {
        _device?.Dispose();
        _device = null;
        try
        {
            File.Delete(Path);
        }
        catch (FileNotFoundException)
        {
            // Already gone: the outcome we wanted.
        }
        catch (DirectoryNotFoundException)
        {
            // The whole group directory was removed underneath us; nothing left to delete.
        }
    }

    public void Dispose() => SetActive(false);

    private void TrimToLength()
    {
        try
        {
            using SafeFileHandle trim = File.OpenHandle(Path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);
            if (RandomAccess.GetLength(trim) != _length)
            {
                RandomAccess.SetLength(trim, _length);
            }
        }
        catch (FileNotFoundException)
        {
            // Deleted underneath us; nothing to trim.
        }
    }
}
