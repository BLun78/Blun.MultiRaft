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
public sealed class InMemoryRaftWal : IRaftWal
{
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private readonly List<RaftLogEntry> _entries = [];

    private long _firstIndex = 1;
    private long _durableIndex;

    // The index/term the log starts after, once a prefix was compacted away or replaced by a snapshot.
    private long _baseIndex;
    private long _baseTerm;

    /// <inheritdoc />
    public long FirstIndex => Volatile.Read(ref _firstIndex);

    /// <inheritdoc />
    public long LastIndex => FirstIndex + EntryCount - 1;

    /// <inheritdoc />
    public long LastTerm
    {
        get
        {
            lock (_entries)
            {
                return _entries.Count > 0 ? _entries[^1].Term : Volatile.Read(ref _baseTerm);
            }
        }
    }

    /// <inheritdoc />
    public long DurableIndex => Volatile.Read(ref _durableIndex);

    private int EntryCount
    {
        get
        {
            lock (_entries)
            {
                return _entries.Count;
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask<long> AppendAsync(
        RaftEntryHeader header,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken = default)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureDense(header.Index);

            // Copied on the way in: the caller's buffer is a slice of a socket read and is recycled the
            // moment this returns, which is exactly the lifetime bug a volatile store invites.
            var entry = new RaftLogEntry(in header, payload.ToArray());
            lock (_entries)
            {
                _entries.Add(entry);
            }

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
            EnsureDense(entries.Span[0].Index);
            lock (_entries)
            {
                for (int i = 0; i < entries.Length; i++)
                {
                    _entries.Add(entries.Span[i].ToOwned());
                }
            }

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
    private void EnsureDense(long index)
    {
        long expected = LastIndex + 1;
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

        // The boundary index answers for itself even though its entry is gone: the next AppendEntries
        // runs its consistency check against exactly that point.
        if (index == Volatile.Read(ref _baseIndex))
        {
            return ValueTask.FromResult(Volatile.Read(ref _baseTerm));
        }

        lock (_entries)
        {
            int slot = (int)(index - _firstIndex);
            return ValueTask.FromResult((uint)slot < (uint)_entries.Count ? _entries[slot].Term : -1L);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RaftLogEntry> ReadFromAsync(
        long fromIndex,
        long toIndexInclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (long index = Math.Max(fromIndex, FirstIndex); index <= toIndexInclusive; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            RaftLogEntry entry;
            lock (_entries)
            {
                int slot = (int)(index - _firstIndex);
                if ((uint)slot >= (uint)_entries.Count)
                {
                    yield break;
                }

                entry = _entries[slot];
            }

            yield return entry;
        }

        await ValueTask.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RaftEntryHeader> ReadHeadersFromAsync(
        long fromIndex,
        long toIndexInclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (long index = Math.Max(fromIndex, FirstIndex); index <= toIndexInclusive; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            RaftEntryHeader header;
            lock (_entries)
            {
                int slot = (int)(index - _firstIndex);
                if ((uint)slot >= (uint)_entries.Count)
                {
                    yield break;
                }

                header = _entries[slot].Header;
            }

            yield return header;
        }

        await ValueTask.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask TruncateTailAsync(long fromIndex, CancellationToken cancellationToken = default)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            lock (_entries)
            {
                int slot = (int)(fromIndex - _firstIndex);
                if (slot < 0 || slot >= _entries.Count)
                {
                    return;
                }

                _entries.RemoveRange(slot, _entries.Count - slot);
            }

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
            lock (_entries)
            {
                int drop = (int)(uptoIndexInclusive - _firstIndex + 1);
                if (drop <= 0)
                {
                    return;
                }

                drop = Math.Min(drop, _entries.Count);

                // Capture the boundary before dropping it, for the same reason the segmented log does:
                // the compacted log still has to answer the consistency check at its own starting point.
                RaftLogEntry boundary = _entries[drop - 1];
                Volatile.Write(ref _baseIndex, boundary.Index);
                Volatile.Write(ref _baseTerm, boundary.Term);

                _entries.RemoveRange(0, drop);
                Volatile.Write(ref _firstIndex, _firstIndex + drop);
            }
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
            lock (_entries)
            {
                _entries.Clear();
            }

            Volatile.Write(ref _firstIndex, lastIncludedIndex + 1);
            Volatile.Write(ref _baseIndex, lastIncludedIndex);
            Volatile.Write(ref _baseTerm, lastIncludedTerm);

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
        lock (_entries)
        {
            _entries.Clear();
        }

        return ValueTask.CompletedTask;
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
