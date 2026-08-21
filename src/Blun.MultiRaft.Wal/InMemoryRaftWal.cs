// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Runtime.CompilerServices;

namespace Blun.MultiRaft.Wal;

/// <summary>
/// Volatile log. For tests, benchmarks and the transient consumer-queue groups that are meant to die with
/// their consumer — not for anything whose durability is claimed to a client.
/// </summary>
/// <remarks>
/// It exists as much to keep <see cref="IRaftWal"/> honest as to be used: an interface with one
/// implementation is a wrapper class, and the consensus core is only genuinely storage-agnostic if two
/// unrelated stores satisfy the same test suite.
/// </remarks>
/// <remarks>
/// Concurrency model: appends/truncations are serialized against each other by <c>_writeGate</c> (Raft only
/// ever has one writer in flight per group), but readers never take a lock. Every mutation builds a new,
/// fully-populated <see cref="LogState"/> snapshot and publishes it with a single <c>Volatile.Write</c>;
/// readers grab the current snapshot with one <c>Volatile.Read</c> and index into it. Because the
/// snapshot is only ever handed out after it is complete, a reader either sees the old state or the new one,
/// never a torn mix — the array itself is append-only within a snapshot's lifetime (writer appends into the
/// unused tail past <c>Count</c> before publishing the incremented count), so slots a published snapshot
/// already claims are never mutated again.
/// </remarks>
public sealed class InMemoryRaftWal : IRaftWal
{
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private LogState _state = LogState.Empty;

    private long _durableIndex;

    /// <inheritdoc />
    public long FirstIndex => Volatile.Read(ref _state).FirstIndex;

    /// <inheritdoc />
    public long LastIndex
    {
        get
        {
            LogState state = Volatile.Read(ref _state);
            return state.FirstIndex + state.Count - 1;
        }
    }

    /// <inheritdoc />
    public long LastTerm
    {
        get
        {
            LogState state = Volatile.Read(ref _state);
            return state.Count > 0 ? state.Entries[state.Offset + state.Count - 1].Term : state.BaseTerm;
        }
    }

    /// <inheritdoc />
    public long DurableIndex => Volatile.Read(ref _durableIndex);

    /// <inheritdoc />
    public async ValueTask<long> AppendAsync(
        RaftEntryHeader header,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken = default)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            LogState state = _state;
            EnsureDense(state, header.Index);

            // Copied on the way in: the caller's buffer is a slice of a socket read and is recycled the
            // moment this returns, which is exactly the lifetime bug a volatile store invites.
            var entry = new RaftLogEntry(in header, payload.ToArray());
            Volatile.Write(ref _state, state.Appended(entry));

            return header.Index;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask<long> AppendAsync(
        ReadOnlyMemory<RaftLogEntry> entries,
        CancellationToken cancellationToken = default)
    {
        if (entries.IsEmpty)
        {
            return LastIndex;
        }

        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            LogState state = _state;
            EnsureDense(state, entries.Span[0].Index);

            for (int i = 0; i < entries.Length; i++)
            {
                state = state.Appended(entries.Span[i].ToOwned());
            }

            Volatile.Write(ref _state, state);

            return entries.Span[^1].Index;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    /// <summary>
    /// Rejects a gap. The consistency check assumes a dense index space, so a hole would not fail loudly —
    /// it would quietly make the check compare the wrong entries.
    /// </summary>
    private static void EnsureDense(LogState state, long index)
    {
        long expected = state.FirstIndex + state.Count;
        if (index != expected)
        {
            throw new InvalidOperationException(
                FormattableString.Invariant($"Log is dense: expected index {expected}, got {index}."));
        }
    }

    /// <inheritdoc />
    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        Volatile.Write(ref _durableIndex, LastIndex);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<long> TermAtAsync(long index, CancellationToken cancellationToken = default)
    {
        if (index == 0)
        {
            return ValueTask.FromResult(0L);
        }

        LogState state = Volatile.Read(ref _state);

        // The boundary index answers for itself even though its entry is gone: the next AppendEntries
        // runs its consistency check against exactly that point.
        if (index == state.BaseIndex)
        {
            return ValueTask.FromResult(state.BaseTerm);
        }

        int slot = (int)(index - state.FirstIndex);
        return ValueTask.FromResult((uint)slot < (uint)state.Count ? state.Entries[state.Offset + slot].Term : -1L);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RaftLogEntry> ReadFromAsync(
        long fromIndex,
        long toIndexInclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        LogState state = Volatile.Read(ref _state);
        for (long index = Math.Max(fromIndex, state.FirstIndex); index <= toIndexInclusive; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int slot = (int)(index - state.FirstIndex);
            if ((uint)slot >= (uint)state.Count)
            {
                yield break;
            }

            yield return state.Entries[state.Offset + slot];
        }

        await ValueTask.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RaftEntryHeader> ReadHeadersFromAsync(
        long fromIndex,
        long toIndexInclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        LogState state = Volatile.Read(ref _state);
        for (long index = Math.Max(fromIndex, state.FirstIndex); index <= toIndexInclusive; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int slot = (int)(index - state.FirstIndex);
            if ((uint)slot >= (uint)state.Count)
            {
                yield break;
            }

            yield return state.Entries[state.Offset + slot].Header;
        }

        await ValueTask.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask TruncateTailAsync(long fromIndex, CancellationToken cancellationToken = default)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            LogState state = _state;
            int slot = (int)(fromIndex - state.FirstIndex);
            if (slot < 0 || slot >= state.Count)
            {
                return;
            }

            Volatile.Write(ref _state, state.WithCount(slot));

            if (_durableIndex > fromIndex - 1)
            {
                Volatile.Write(ref _durableIndex, fromIndex - 1);
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
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            LogState state = _state;
            int drop = (int)(uptoIndexInclusive - state.FirstIndex + 1);
            if (drop <= 0)
            {
                return;
            }

            drop = Math.Min(drop, state.Count);

            // Capture the boundary before dropping it, for the same reason the segmented log does:
            // the compacted log still has to answer the consistency check at its own starting point.
            RaftLogEntry boundary = state.Entries[state.Offset + drop - 1];
            Volatile.Write(ref _state, state.WithHeadDropped(drop, boundary.Index, boundary.Term));
        }
        finally
        {
            _writeGate.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask ResetToSnapshotAsync(
        long lastIncludedIndex,
        long lastIncludedTerm,
        CancellationToken cancellationToken = default)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            Volatile.Write(ref _state, LogState.EmptyAt(lastIncludedIndex, lastIncludedTerm));

            // A snapshot is durable by the time it is installed, so everything it covers is durable too.
            Volatile.Write(ref _durableIndex, lastIncludedIndex);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _writeGate.Dispose();
        Volatile.Write(ref _state, LogState.Empty);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Immutable-once-published snapshot of the log's live window. <see cref="Entries"/> may have unused
    /// capacity past <see cref="Offset"/> + <see cref="Count"/> — that tail belongs to whichever writer built
    /// this snapshot and is only ever written into before being handed to a *new* snapshot via
    /// <see cref="Appended"/>; a snapshot already published to readers never has its claimed slots mutated.
    /// </summary>
    private sealed class LogState
    {
        public required RaftLogEntry[] Entries { get; init; }

        public required int Offset { get; init; }

        public required int Count { get; init; }

        public required long FirstIndex { get; init; }

        public required long BaseIndex { get; init; }

        public required long BaseTerm { get; init; }

        public static LogState Empty { get; } = new()
        {
            Entries = [],
            Offset = 0,
            Count = 0,
            FirstIndex = 1,
            BaseIndex = 0,
            BaseTerm = 0,
        };

        public static LogState EmptyAt(long lastIncludedIndex, long lastIncludedTerm) => new()
        {
            Entries = [],
            Offset = 0,
            Count = 0,
            FirstIndex = lastIncludedIndex + 1,
            BaseIndex = lastIncludedIndex,
            BaseTerm = lastIncludedTerm,
        };

        public LogState Appended(RaftLogEntry entry)
        {
            RaftLogEntry[] array = Entries;
            int offset = Offset;

            if (offset + Count >= array.Length)
            {
                // Compact the live window into a fresh array sized for future growth. Only the writer
                // (serialized by _writeGate) ever sees this array before it is published, so writing into
                // it here and publishing the new snapshot afterwards is race-free for readers.
                int newCapacity = Math.Max(4, Count * 2);
                var compacted = new RaftLogEntry[newCapacity];
                Array.Copy(array, offset, compacted, 0, Count);
                array = compacted;
                offset = 0;
            }

            array[offset + Count] = entry;

            return new LogState
            {
                Entries = array,
                Offset = offset,
                Count = Count + 1,
                FirstIndex = FirstIndex,
                BaseIndex = BaseIndex,
                BaseTerm = BaseTerm,
            };
        }

        public LogState WithCount(int newCount) => new()
        {
            Entries = Entries,
            Offset = Offset,
            Count = newCount,
            FirstIndex = FirstIndex,
            BaseIndex = BaseIndex,
            BaseTerm = BaseTerm,
        };

        public LogState WithHeadDropped(int drop, long boundaryIndex, long boundaryTerm) => new()
        {
            Entries = Entries,
            Offset = Offset + drop,
            Count = Count - drop,
            FirstIndex = FirstIndex + drop,
            BaseIndex = boundaryIndex,
            BaseTerm = boundaryTerm,
        };
    }
}

/// <summary>Hands out <see cref="InMemoryRaftWal"/> instances, one per group.</summary>
public sealed class InMemoryRaftWalFactory : IRaftWalFactory
{
    private readonly Dictionary<RaftGroupId, InMemoryRaftWal> _logs = [];

    /// <inheritdoc />
    public ValueTask<IRaftWal> OpenAsync(RaftGroupId group, CancellationToken cancellationToken = default)
    {
        lock (_logs)
        {
            if (!_logs.TryGetValue(group, out InMemoryRaftWal? wal))
            {
                wal = new InMemoryRaftWal();
                _logs[group] = wal;
            }

            return ValueTask.FromResult<IRaftWal>(wal);
        }
    }

    /// <inheritdoc />
    public ValueTask DeleteAsync(RaftGroupId group, CancellationToken cancellationToken = default)
    {
        lock (_logs)
        {
            _logs.Remove(group);
        }

        return ValueTask.CompletedTask;
    }
}
