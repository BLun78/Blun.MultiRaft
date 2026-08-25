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
    /// <summary>
    /// How much of a segment the recovery scan holds at once. Deliberately unrelated to the segment size:
    /// it bounds the pooled buffer a restart borrows per group, not how much of the log can be recovered.
    /// </summary>
    private const int ScanWindowBytes = 64 * 1024;

    private readonly List<long> _offsets;
    private readonly List<long> _terms;
    private readonly List<int> _sizes;
    private readonly SegmentedRaftWalOptions _options;

    /// <summary>Checksum width for this segment's algorithm. Constant per instance, so it is resolved once
    /// rather than re-switched for every entry of every batch.</summary>
    private readonly int _checksumSize;

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
        _checksumSize = WalChecksumStrategy.ChecksumSizeFor(options.ChecksumAlgorithm);
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

        // SequentialScan for the readahead: this is the one place in the log that reads a whole segment
        // front to back.
        //
        // Asynchronous does NOT pay for itself here and is not kept because it is faster -- measured warm,
        // it is a wash at one group (30.6 ms vs 30.1 ms for 200k entries) and slightly behind at two
        // hundred opened at once (205 ms vs 188 ms). It is kept because without it RandomAccess.ReadAsync
        // is not overlapped I/O at all: it is a blocking read parked on a thread-pool thread for as long as
        // the read takes, which is nothing on a warm page cache and a real wait on a cold one. Recovery of
        // every group happens at once, at startup, which is exactly when the cache is cold -- and that case
        // is not measured here, so do not cite either direction as settled.
        using SafeFileHandle handle = File.OpenHandle(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        long fileLength = RandomAccess.GetLength(handle);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(ScanWindowBytes);
        try
        {
            long offset = 0;
            long expected = firstIndex;

            // A window of the file at a time, with records parsed out of it, rather than a read per record:
            // a segment of small entries is tens of thousands of records, and a syscall each to read a
            // 32-byte header and then the ~100 bytes behind it is the whole cost of opening a group.
            // The window always starts at a record boundary, so the file position of the record at `cursor`
            // is exactly (window origin + cursor) -- which is what `offset` already tracks, so the origin
            // itself never has to be stored.
            int windowLength = 0;
            int cursor = 0;

            while (offset + RaftEntryHeader.Size <= fileLength)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (windowLength - cursor < RaftEntryHeader.Size)
                {
                    windowLength = await FillAsync(handle, buffer, offset, fileLength, cancellationToken)
                        .ConfigureAwait(false);
                    cursor = 0;
                    if (windowLength < RaftEntryHeader.Size)
                    {
                        // The loop condition established that the file holds these bytes, so failing to
                        // read them is damage rather than the end of the log.
                        torn = true;
                        break;
                    }
                }

                RaftEntryHeader header = RaftEntryHeader.Read(buffer.AsSpan(cursor, RaftEntryHeader.Size));

                // An all-zero header is the unwritten remainder of a preallocated (memory-mapped) segment,
                // not damage. Reading it as a tear would make recovery delete every segment after this one.
                if (header.Term == 0 && header.Index == 0 && header.PayloadLength == 0)
                {
                    break;
                }

                // A corrupt length field must never be trusted into an allocation, so it is bounded by the
                // same limit the write path enforces before it is used for anything.
                if (header.PayloadLength < 0
                    || header.PayloadLength > options.EffectiveMaxPayloadBytes
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

                if (windowLength - cursor < size)
                {
                    if (size > buffer.Length)
                    {
                        // Rent before returning: if Rent throws (an oversized request from the wire), the
                        // old buffer must not already be back in the pool, or two callers end up sharing
                        // it. The larger buffer is kept for the rest of the scan rather than shrunk back --
                        // one record this big is a poor reason to re-read the window on the next one.
                        byte[] larger = ArrayPool<byte>.Shared.Rent(size);
                        ArrayPool<byte>.Shared.Return(buffer);
                        buffer = larger;
                    }

                    // The record straddles the end of the window (or outgrew it). Refill from its own start
                    // so the whole of it is contiguous for the checksum.
                    windowLength = await FillAsync(handle, buffer, offset, fileLength, cancellationToken)
                        .ConfigureAwait(false);
                    cursor = 0;
                    if (windowLength < size)
                    {
                        torn = true;
                        break;
                    }
                }

                if (!RaftWalRecord.Verify(buffer.AsSpan(cursor, size), in header, checksum))
                {
                    torn = true;
                    break;
                }

                offsets.Add(offset);
                terms.Add(header.Term);
                sizes.Add(size);
                offset += size;
                cursor += size;
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
    /// Reads as much of the file from <paramref name="offset"/> as <paramref name="buffer"/> holds, and
    /// returns how many bytes arrived. It loops because RandomAccess.ReadAsync is allowed to return a short
    /// read in the middle of a file; taking the first one as the end of the data would report a tear in a
    /// log that is perfectly intact.
    /// </summary>
    private static async ValueTask<int> FillAsync(
        SafeFileHandle handle,
        byte[] buffer,
        long offset,
        long fileLength,
        CancellationToken cancellationToken)
    {
        int want = (int)Math.Min(buffer.Length, fileLength - offset);
        int filled = 0;
        while (filled < want)
        {
            int read = await RandomAccess
                .ReadAsync(handle, buffer.AsMemory(filled, want - filled), offset + filled, cancellationToken)
                .ConfigureAwait(false);
            if (read <= 0)
            {
                break;
            }

            filled += read;
        }

        return filled;
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

    // FileShare.Delete, not just ReadWrite: a follower read can hold this handle open for the length of a
    // replication round while a concurrent TruncateHeadAsync deletes the segment underneath it. On Windows,
    // where FileShare is enforced rather than advisory, a handle opened without FILE_SHARE_DELETE turns that
    // delete into an IOException instead of the POSIX-style detach-and-continue Unix gives for free.
    public SafeFileHandle OpenRead()
        => File.OpenHandle(
            Path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete,
            FileOptions.Asynchronous);

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

        RecordBatch(entries.Span);
    }

    /// <summary>
    /// Adds one index-map slot per entry of a batch that has just been written. Split out of
    /// <see cref="AppendBatchAsync"/> so the span is taken once instead of per iteration — the
    /// <see cref="ReadOnlyMemory{T}.Span"/> getter is a type check and a span construction, not a field read,
    /// and an async method cannot hold the span in a local across its await.
    /// </summary>
    private void RecordBatch(ReadOnlySpan<RaftLogEntry> entries)
    {
        long offset = _length;
        _offsets.EnsureCapacity(_offsets.Count + entries.Length);
        _terms.EnsureCapacity(_terms.Count + entries.Length);
        _sizes.EnsureCapacity(_sizes.Count + entries.Length);

        foreach (ref readonly RaftLogEntry entry in entries)
        {
            int size = RaftWalRecord.SizeOf(entry.Payload.Length, _checksumSize);
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
