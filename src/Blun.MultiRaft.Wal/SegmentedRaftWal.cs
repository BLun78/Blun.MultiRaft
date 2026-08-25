// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.Runtime.CompilerServices;
using Blun.MultiRaft.Wal.Checksum;
using Microsoft.Win32.SafeHandles;

namespace Blun.MultiRaft.Wal;

/// <summary>
/// Append-only segmented write-ahead log, one instance per Raft group.
/// <para>
/// Per-instance footprint is one open file handle for the active segment plus an index map of 16 bytes per
/// retained entry — no page cache, no background thread. That is the property that decides whether thousands
/// of these can coexist, and it is why this exists instead of a general-purpose embedded log.
/// </para>
/// <para>
/// Writes are serialized through an async gate (single writer). Reads do not take the gate at all: an
/// iteration opens its own read handle and works from a snapshot of the segment list, so a follower catching
/// up from disk cannot stall the leader's append path.
/// </para>
/// </summary>
public sealed class SegmentedRaftWal : IRaftWal
{
    internal const string SegmentExtension = ".seg";

    private const string BaseFileName = "log.base";
    private const int BaseRecordSize = 32;
    private const ulong BaseMagic = 0x424C554E42415345; // "BLUNBASE"
    private const string ConfigFileName = "wal.cfg";

    private readonly string _directory;
    private readonly SegmentedRaftWalOptions _options;
    private readonly IWalChecksumStrategy _checksum;
    private readonly SemaphoreSlim _writeGate = new(1, 1);

    // Reused across appends. Legal only because every append holds the write gate: single writer, so a
    // per-instance scratch buffer replaces a per-append allocation.
    //
    // Sized for ordinary commands, NOT for MaxPayloadBytes. Sizing it to the maximum would charge every
    // group a megabyte for a message size almost none of them will ever see — with one log per queue that
    // is gigabytes of idle buffer, which is the exact footprint this design exists to avoid. Records that
    // do not fit borrow from the array pool for the duration of the write.
    private readonly byte[] _scratch;

    private WalSegment[] _segments = [];
    private WalSegment? _active;

    // Segments whose file delete failed (Windows only: a reader still had the file open without
    // FileShare.Delete). Already dropped from _segments by the time they land here, so leaving them
    // undeleted costs disk space, never correctness; each truncation retries whatever is still pending.
    private readonly List<WalSegment> _pendingDeletes = [];

    private long _firstIndex = 1;
    private long _lastIndex;
    private long _lastTerm;
    private long _durableIndex;

    // The index/term the log now starts *after*, once a prefix has been compacted away or replaced by a
    // snapshot. Persisted alongside the segments, because a restart that forgot it could not answer the
    // consistency check at the boundary and the replica would never accept another entry.
    private long _baseIndex;
    private long _baseTerm;
    private bool _baseFileWritten;
    private bool _disposed;

    private SegmentedRaftWal(string directory, SegmentedRaftWalOptions options)
    {
        _directory = directory;
        _options = options;
        _checksum = WalChecksumStrategy.For(options.ChecksumAlgorithm);
        _scratch = new byte[options.ScratchBufferBytes];
    }

    private string BasePath => Path.Combine(_directory, BaseFileName);

    private string ConfigPath => Path.Combine(_directory, ConfigFileName);

    /// <inheritdoc />
    public long FirstIndex => Volatile.Read(ref _firstIndex);

    /// <inheritdoc />
    public long LastIndex => Volatile.Read(ref _lastIndex);

    /// <inheritdoc />
    public long LastTerm => Volatile.Read(ref _lastTerm);

    /// <inheritdoc />
    public long DurableIndex => Volatile.Read(ref _durableIndex);

    /// <summary>Opens (and recovers) the log rooted at <paramref name="directory"/>, creating it if absent.</summary>
    public static async ValueTask<SegmentedRaftWal> OpenAsync(
        string directory,
        SegmentedRaftWalOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        SegmentedRaftWalOptions effective = options ?? new SegmentedRaftWalOptions();
        effective.Validate();
        var wal = new SegmentedRaftWal(directory, effective);
        Directory.CreateDirectory(directory);
        await wal.LoadConfigAsync(cancellationToken).ConfigureAwait(false);
        await wal.LoadBaseAsync(cancellationToken).ConfigureAwait(false);
        await wal.RecoverAsync(cancellationToken).ConfigureAwait(false);
        return wal;
    }

    /// <inheritdoc />
    public async ValueTask<long> AppendAsync(
        RaftEntryHeader header,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (payload.Length != header.PayloadLength)
        {
            throw new ArgumentException("Header payload length does not match the payload.", nameof(payload));
        }

        if (payload.Length > _options.EffectiveMaxPayloadBytes)
        {
            throw new ArgumentOutOfRangeException(nameof(payload), payload.Length, "Payload exceeds MaxPayloadBytes.");
        }

        int required = RaftWalRecord.SizeOf(payload.Length, _checksum.ChecksumSize);
        byte[]? rented = required > _scratch.Length ? ArrayPool<byte>.Shared.Rent(required) : null;

        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureDense(header.Index);
            SaveConfig();
            byte[] frame = rented ?? _scratch;
            int size = RaftWalRecord.Write(frame, header, payload.Span, _checksum);
            WalSegment segment = RollIfNeeded(header.Index, size);
            await segment.AppendAsync(frame.AsMemory(0, size), header.Term, size, cancellationToken)
                .ConfigureAwait(false);
            Advance(header.Index, header.Term);
            return header.Index;
        }
        finally
        {
            _writeGate.Release();
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask<long> AppendAsync(
        ReadOnlyMemory<RaftLogEntry> entries,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (entries.IsEmpty)
        {
            return LastIndex;
        }

        // Checked before the gate, for the whole batch at once (SEC-008): the single-entry overload above
        // enforces MaxPayloadBytes, but this is the path replicated entries from a peer actually travel, and
        // it enforced nothing. An oversized entry written here is accepted at the time, then rejected by
        // WalSegment.RecoverAsync as torn on the next restart -- which truncates the log from that point on,
        // discarding every committed entry after it. Checking the whole batch up front, not entry-by-entry
        // as each is written, means a rejected batch never leaves a partial write behind.
        //
        // InvalidOperationException, not ArgumentException/ArgumentOutOfRangeException as the single-entry
        // overload uses for its (locally-originated, not peer-reachable) callers: this batch is exactly what
        // AppendEntries writes on the receive path, and RaftStreamSession.HandleRequestAsync's catch list
        // (RpcException, InvalidOperationException, IOException) is what turns a rejection here into a
        // logged, session-preserving failure instead of an unobserved exception in a fire-and-forget task.
        foreach (ref readonly RaftLogEntry entry in entries.Span)
        {
            if (entry.Payload.Length != entry.Header.PayloadLength)
            {
                throw new InvalidOperationException("Header payload length does not match the payload.");
            }

            if (entry.Payload.Length > _options.MaxPayloadBytes)
            {
                throw new InvalidOperationException(
                    $"Payload of {entry.Payload.Length} bytes exceeds MaxPayloadBytes ({_options.MaxPayloadBytes}).");
            }
        }

        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            SaveConfig();
            int checksumSize = _checksum.ChecksumSize;
            long last = _lastIndex;
            int i = 0;
            while (i < entries.Length)
            {
                ReadOnlyMemory<RaftLogEntry> remaining = entries[i..];

                // Headers are copied by value rather than held by reference: this method awaits, so nothing
                // that aliases the span may stay alive across the write below.
                RaftEntryHeader first = remaining.Span[0].Header;
                EnsureDense(first.Index);

                int firstSize = RaftWalRecord.SizeOf(first.PayloadLength, checksumSize);
                WalSegment segment = RollIfNeeded(first.Index, firstSize);
                long room = _options.SegmentSizeBytes - segment.Length;

                int count = CountThatFit(remaining.Span, room, checksumSize, out int bytes);

                byte[] buffer = ArrayPool<byte>.Shared.Rent(bytes);
                try
                {
                    int offset = Pack(remaining.Span[..count], buffer);
                    await segment.AppendBatchAsync(buffer.AsMemory(0, offset), remaining[..count], cancellationToken)
                        .ConfigureAwait(false);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }

                RaftEntryHeader tail = remaining.Span[count - 1].Header;
                last = tail.Index;
                Advance(tail.Index, tail.Term);
                i += count;
            }

            return last;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_options.FlushToDisk)
            {
                _active?.FlushToDisk();
            }

            Volatile.Write(ref _durableIndex, _lastIndex);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    /// <summary>
    /// Counts how many leading entries of <paramref name="entries"/> fit in <paramref name="room"/> bytes
    /// and reports their packed size. The first entry always counts, whether it fits or not — it is what
    /// <see cref="RollIfNeeded"/> has already made room for, and refusing it here would loop forever.
    /// </summary>
    /// <remarks>
    /// Synchronous and span-taking on purpose. Its caller awaits, so it cannot hold a span in a local;
    /// indexing through <see cref="ReadOnlyMemory{T}.Span"/> instead would re-run the getter — a type check
    /// and a span construction, not a field read — once per entry.
    /// </remarks>
    private static int CountThatFit(ReadOnlySpan<RaftLogEntry> entries, long room, int checksumSize, out int bytes)
    {
        // Coalesce as much of the batch as fits the current segment into one buffer: a replication
        // message should cost one write, not one per entry. The split point is the segment boundary.
        int total = 0;
        int count = 0;
        while (count < entries.Length)
        {
            int size = RaftWalRecord.SizeOf(entries[count].Payload.Length, checksumSize);
            if (count > 0 && total + size > room)
            {
                break;
            }

            total += size;
            count++;
        }

        bytes = total;
        return count;
    }

    /// <summary>
    /// Writes every entry of <paramref name="entries"/> back to back into <paramref name="buffer"/> and
    /// returns the bytes used. Split out of the append loop for the same reason as
    /// <see cref="CountThatFit"/>.
    /// </summary>
    private int Pack(ReadOnlySpan<RaftLogEntry> entries, Span<byte> buffer)
    {
        int offset = 0;
        foreach (ref readonly RaftLogEntry entry in entries)
        {
            offset += RaftWalRecord.Write(buffer[offset..], entry.Header, entry.Payload.Span, _checksum);
        }

        return offset;
    }

    /// <inheritdoc />
    public ValueTask<long> TermAtAsync(long index, CancellationToken cancellationToken = default)
    {
        // Terms live in the in-memory index map, so the consistency check run on every AppendEntries is a
        // lookup rather than a disk read. This is the hottest read in the whole protocol.
        if (index == 0)
        {
            return ValueTask.FromResult(0L);
        }

        // The boundary index survives its own entry being discarded. Raft checks consistency against the
        // index just before the first one it sends, so a compacted log that could not answer for its own
        // starting point would reject everything after it.
        if (index == Volatile.Read(ref _baseIndex))
        {
            return ValueTask.FromResult(Volatile.Read(ref _baseTerm));
        }

        WalSegment[] segments = Volatile.Read(ref _segments);
        if (index < Volatile.Read(ref _firstIndex) || index > Volatile.Read(ref _lastIndex))
        {
            return ValueTask.FromResult(-1L);
        }

        WalSegment? segment = Locate(segments, index);
        return ValueTask.FromResult(segment?.TermAt(index) ?? -1L);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RaftLogEntry> ReadFromAsync(
        long fromIndex,
        long toIndexInclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        WalSegment[] segments = Volatile.Read(ref _segments);
        long index = Math.Max(fromIndex, Volatile.Read(ref _firstIndex));
        long end = Math.Min(toIndexInclusive, Volatile.Read(ref _lastIndex));

        byte[] buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            while (index <= end)
            {
                cancellationToken.ThrowIfCancellationRequested();
                WalSegment? segment = Locate(segments, index);
                if (segment is null)
                {
                    yield break;
                }

                using SafeFileHandle handle = segment.OpenRead();
                long stop = Math.Min(end, segment.LastIndex);
                while (index <= stop)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (!segment.TryLocate(index, out long offset, out int size))
                    {
                        yield break;
                    }

                    if (size > buffer.Length)
                    {
                        // Rent before returning: if Rent throws, the old buffer must not already be back in
                        // the pool, or two callers end up sharing it.
                        byte[] larger = ArrayPool<byte>.Shared.Rent(size);
                        ArrayPool<byte>.Shared.Return(buffer);
                        buffer = larger;
                    }

                    Memory<byte> slot = buffer.AsMemory(0, size);
                    int read = await RandomAccess.ReadAsync(handle, slot, offset, cancellationToken).ConfigureAwait(false);
                    if (read < size)
                    {
                        yield break;
                    }

                    RaftEntryHeader header = RaftEntryHeader.Read(slot.Span);
                    if (!RaftWalRecord.Verify(slot.Span, in header, _checksum))
                    {
                        yield break;
                    }

                    // The payload aliases the pooled buffer: no copy on the way to the transport. The
                    // contract on IRaftWal.ReadFromAsync makes the lifetime the consumer's problem.
                    yield return new RaftLogEntry(
                        in header,
                        slot[RaftEntryHeader.Size..(RaftEntryHeader.Size + header.PayloadLength)]);
                    index++;
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RaftEntryHeader> ReadHeadersFromAsync(
        long fromIndex,
        long toIndexInclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        WalSegment[] segments = Volatile.Read(ref _segments);
        long index = Math.Max(fromIndex, Volatile.Read(ref _firstIndex));
        long end = Math.Min(toIndexInclusive, Volatile.Read(ref _lastIndex));

        byte[] buffer = ArrayPool<byte>.Shared.Rent(RaftEntryHeader.Size);
        try
        {
            while (index <= end)
            {
                cancellationToken.ThrowIfCancellationRequested();
                WalSegment? segment = Locate(segments, index);
                if (segment is null)
                {
                    yield break;
                }

                using SafeFileHandle handle = segment.OpenRead();
                long stop = Math.Min(end, segment.LastIndex);
                while (index <= stop)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (!segment.TryLocate(index, out long offset, out _))
                    {
                        yield break;
                    }

                    // Only the header's own bytes -- the payload and checksum that follow it on disk are
                    // never read here, which is the entire point of this method over ReadFromAsync.
                    Memory<byte> slot = buffer.AsMemory(0, RaftEntryHeader.Size);
                    int read = await RandomAccess.ReadAsync(handle, slot, offset, cancellationToken).ConfigureAwait(false);
                    if (read < RaftEntryHeader.Size)
                    {
                        yield break;
                    }

                    yield return RaftEntryHeader.Read(slot.Span);
                    index++;
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <inheritdoc />
    public async ValueTask TruncateTailAsync(long fromIndex, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (fromIndex > _lastIndex)
            {
                return;
            }

            var kept = new List<WalSegment>(_segments.Length);
            foreach (WalSegment segment in _segments)
            {
                if (segment.FirstIndex >= fromIndex)
                {
                    segment.Delete();
                }
                else
                {
                    kept.Add(segment);
                }
            }

            WalSegment? tail = kept.Count > 0 ? kept[^1] : null;
            tail?.TruncateFrom(fromIndex);

            Volatile.Write(ref _segments, [.. kept]);
            _active?.SetActive(false);
            _active = tail;
            _active?.SetActive(true);

            _lastIndex = fromIndex - 1;
            _lastTerm = tail is not null && _lastIndex >= tail.FirstIndex ? tail.TermAt(_lastIndex) : 0;
            if (_durableIndex > _lastIndex)
            {
                Volatile.Write(ref _durableIndex, _lastIndex);
            }

            if (kept.Count == 0)
            {
                Volatile.Write(ref _firstIndex, _lastIndex + 1);
            }
        }
        finally
        {
            _writeGate.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask TruncateHeadAsync(long uptoIndexInclusive, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Whole segments only: a partial head truncation would have to rewrite a file, which is exactly
            // the write amplification a segmented log exists to avoid.
            var kept = new List<WalSegment>(_segments.Length);
            var doomed = new List<WalSegment>();
            foreach (WalSegment segment in _segments)
            {
                if (!ReferenceEquals(segment, _active) && segment.LastIndex <= uptoIndexInclusive)
                {
                    doomed.Add(segment);
                }
                else
                {
                    kept.Add(segment);
                }
            }

            if (doomed.Count == 0)
            {
                return;
            }

            // Read the boundary term out of the last doomed segment *before* deleting it. Without it the
            // compacted log could not answer the consistency check at its own starting point, and every
            // entry after that point would be refused forever.
            WalSegment boundary = doomed[^1];
            long baseIndex = boundary.LastIndex;
            long baseTerm = boundary.TermAt(baseIndex);

            // _segments/_firstIndex/base are updated BEFORE the files are deleted, not after. A segment that
            // fails to delete (Windows: a follower's ReadFromAsync still has it open -- see WalSegment.OpenRead)
            // now just costs disk space, retried on the next truncation. Deleting first and updating state
            // second, the old order, left a segment removed from disk but still listed in _segments whenever
            // the delete succeeded but the process crashed before the state write -- every read of it then
            // failed with FileNotFoundException, a correctness bug rather than a space leak.
            Volatile.Write(ref _segments, [.. kept]);
            Volatile.Write(ref _firstIndex, kept.Count > 0 ? kept[0].FirstIndex : _lastIndex + 1);
            await SetBaseAsync(baseIndex, baseTerm, cancellationToken).ConfigureAwait(false);

            DeleteSegments(doomed);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    /// <summary>
    /// Deletes each segment's file, retrying anything left over from a previous call first. A segment whose
    /// delete fails (Windows: a concurrent reader still has it open without FileShare.Delete) is kept for the
    /// next attempt rather than lost track of -- it has already been dropped from <see cref="_segments"/>, so
    /// the cost of leaving it undeleted a while longer is disk space, not correctness.
    /// </summary>
    private void DeleteSegments(List<WalSegment> segments)
    {
        if (_pendingDeletes.Count > 0)
        {
            segments = [.. _pendingDeletes, .. segments];
            _pendingDeletes.Clear();
        }

        foreach (WalSegment segment in segments)
        {
            try
            {
                segment.Delete();
            }
            catch (IOException)
            {
                _pendingDeletes.Add(segment);
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask ResetToSnapshotAsync(
        long lastIncludedIndex,
        long lastIncludedTerm,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            List<WalSegment> doomed = [.. _segments];
            Volatile.Write(ref _segments, []);
            _active = null;

            Volatile.Write(ref _firstIndex, lastIncludedIndex + 1);
            Volatile.Write(ref _lastIndex, lastIncludedIndex);
            Volatile.Write(ref _lastTerm, lastIncludedTerm);

            // A snapshot is durable by the time it is installed, so everything it covers is durable too.
            Volatile.Write(ref _durableIndex, lastIncludedIndex);
            await SetBaseAsync(lastIncludedIndex, lastIncludedTerm, cancellationToken).ConfigureAwait(false);

            DeleteSegments(doomed);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        await _writeGate.WaitAsync().ConfigureAwait(false);
        try
        {
            _disposed = true;
            foreach (WalSegment segment in _segments)
            {
                segment.Dispose();
            }

            _segments = [];
            _active = null;
        }
        finally
        {
            // _writeGate is deliberately not disposed. Every append checks _disposed before waiting on the
            // gate, but the check and the wait are not atomic, so a caller could pass the check just before
            // this runs and then wait on a semaphore this method just disposed out from under it.
            // SemaphoreSlim.Dispose only matters if AvailableWaitHandle was used, which it never is here.
            _writeGate.Release();
        }
    }

    /// <summary>
    /// Records the index/term the log now starts after, in a small file next to the segments.
    /// It is fsynced: losing it would leave a compacted replica unable to answer for its own boundary, which
    /// is indistinguishable from a permanently divergent log.
    /// </summary>
    private async ValueTask SetBaseAsync(long index, long term, CancellationToken cancellationToken)
    {
        Volatile.Write(ref _baseIndex, index);
        Volatile.Write(ref _baseTerm, term);

        // The checksum algorithm is not written here: it lives in wal.cfg (LoadConfigAsync/SaveConfig) and is
        // validated once at open, before this file is ever read. Writing it a second time here, into a
        // record LoadBaseAsync never reads back, would only be a byte that looks load-bearing and is not.
        byte[] buffer = new byte[BaseRecordSize];
        BinaryPrimitives.WriteUInt64LittleEndian(buffer, BaseMagic);
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(8), index);
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(16), term);

        using SafeFileHandle handle = File.OpenHandle(
            BasePath,
            FileMode.OpenOrCreate,
            FileAccess.Write,
            FileShare.ReadWrite,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        await RandomAccess.WriteAsync(handle, buffer, 0, cancellationToken).ConfigureAwait(false);
        if (_options.FlushToDisk)
        {
            RandomAccess.FlushToDisk(handle);
        }
    }

    private async ValueTask LoadBaseAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(BasePath))
        {
            return;
        }

        byte[] buffer = new byte[BaseRecordSize];
        using SafeFileHandle handle = File.OpenHandle(BasePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, FileOptions.Asynchronous | FileOptions.SequentialScan);
        int read = await RandomAccess.ReadAsync(handle, buffer, 0, cancellationToken).ConfigureAwait(false);
        if (read < BaseRecordSize || BinaryPrimitives.ReadUInt64LittleEndian(buffer) != BaseMagic)
        {
            return;
        }

        Volatile.Write(ref _baseIndex, BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(8)));
        Volatile.Write(ref _baseTerm, BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(16)));
    }

    private async ValueTask LoadConfigAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(ConfigPath))
        {
            return;
        }

        byte[] buffer = new byte[1];
        using SafeFileHandle handle = File.OpenHandle(ConfigPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        int read = await RandomAccess.ReadAsync(handle, buffer, 0, cancellationToken).ConfigureAwait(false);
        if (read < 1)
        {
            return;
        }

        WalChecksumAlgorithm persisted = (WalChecksumAlgorithm)buffer[0];
        if (persisted != _options.ChecksumAlgorithm)
        {
            throw new InvalidOperationException(
                "Checksum algorithm mismatch: the log was created with " + persisted +
                " but the options specify " + _options.ChecksumAlgorithm + ".");
        }

        _baseFileWritten = true;
    }

    private void SaveConfig()
    {
        if (_baseFileWritten)
        {
            return;
        }

        byte[] buffer = [(byte)_options.ChecksumAlgorithm];

        // FileMode.Create, not CreateNew: a crash between creating wal.cfg and writing its one byte leaves
        // an empty file that LoadConfigAsync treats as "not written yet" (read < 1), so this runs again on
        // the next append. Overwriting a single byte is harmless; CreateNew would throw on the existing file
        // and leave the log permanently unwritable.
        using SafeFileHandle handle = File.OpenHandle(
            ConfigPath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.ReadWrite);
        RandomAccess.Write(handle, buffer, 0);
        RandomAccess.FlushToDisk(handle);
        _baseFileWritten = true;
    }

    private void EnsureDense(long index)
    {
        long expected = _lastIndex + 1;
        if (index != expected)
        {
            throw new InvalidOperationException(
                "Log is dense: expected index " + expected.ToString(CultureInfo.InvariantCulture) +
                ", got " + index.ToString(CultureInfo.InvariantCulture) + ".");
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Advance(long index, long term)
    {
        Volatile.Write(ref _lastIndex, index);
        Volatile.Write(ref _lastTerm, term);
    }

    private WalSegment RollIfNeeded(long nextIndex, int recordSize)
    {
        if (_active is { } active && (active.Count == 0 || active.Length + recordSize <= _options.SegmentSizeBytes))
        {
            return active;
        }

        _active?.SetActive(false);
        WalSegment segment = WalSegment.Create(_directory, nextIndex, _options);
        Volatile.Write(ref _segments, [.. _segments, segment]);
        _active = segment;
        if (_segments.Length == 1)
        {
            Volatile.Write(ref _firstIndex, nextIndex);
        }

        return segment;
    }

    private static WalSegment? Locate(WalSegment[] segments, long index)
    {
        // Segments are index-ordered and few, so a descending linear scan beats a binary search's branches
        // for the overwhelmingly common case of reading from the newest segment.
        for (int i = segments.Length - 1; i >= 0; i--)
        {
            if (segments[i].Count > 0 && segments[i].FirstIndex <= index && index <= segments[i].LastIndex)
            {
                return segments[i];
            }
        }

        return null;
    }

    private async ValueTask RecoverAsync(CancellationToken cancellationToken)
    {
        string[] files = Directory.GetFiles(_directory, "*" + SegmentExtension);
        Array.Sort(files, StringComparer.Ordinal);

        var recovered = new List<WalSegment>(files.Length);
        bool torn = false;
        foreach (string file in files)
        {
            if (torn)
            {
                // Everything after the first torn record is unreachable: the log must stay dense.
                File.Delete(file);
                continue;
            }

            WalSegment segment = await WalSegment.RecoverAsync(file, _options, _checksum, cancellationToken).ConfigureAwait(false);
            bool contiguous = recovered.Count == 0 || segment.FirstIndex == recovered[^1].LastIndex + 1;
            if (segment.Count == 0 || !contiguous)
            {
                segment.Delete();
                torn = true;
                continue;
            }

            recovered.Add(segment);
            torn = segment.Torn;
        }

        Volatile.Write(ref _segments, [.. recovered]);
        if (recovered.Count > 0)
        {
            _active = recovered[^1];
            _active.SetActive(true);
            Volatile.Write(ref _firstIndex, recovered[0].FirstIndex);
            Volatile.Write(ref _lastIndex, _active.LastIndex);
            Volatile.Write(ref _lastTerm, _active.TermAt(_active.LastIndex));
        }
        else if (Volatile.Read(ref _baseIndex) > 0)
        {
            // Compacted down to nothing — a queue that drained, or a replica that has only ever had a
            // snapshot. The log is empty but it does not start at 1, and forgetting that would make the
            // next append collide with indices the snapshot already covers.
            long baseIndex = Volatile.Read(ref _baseIndex);
            Volatile.Write(ref _firstIndex, baseIndex + 1);
            Volatile.Write(ref _lastIndex, baseIndex);
            Volatile.Write(ref _lastTerm, Volatile.Read(ref _baseTerm));
        }

        // Anything that survived recovery was on disk before the process started, so it is durable by
        // definition — that is what makes DurableIndex meaningful across a restart.
        Volatile.Write(ref _durableIndex, _lastIndex);
    }
}
