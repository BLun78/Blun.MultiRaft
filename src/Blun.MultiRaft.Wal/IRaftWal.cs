// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

namespace Blun.MultiRaft.Wal;

/// <summary>
/// The persistent log of one Raft group. One instance per group by design: a queue that drains wants to
/// throw its whole log away, and in a log shared between groups the slowest group pins the segments of
/// every other one. The cost of that choice is per-instance footprint, so implementations are expected to
/// keep it to a file handle and an index map, not a page cache.
/// </summary>
/// <remarks>
/// Index space is 1-based and dense. Index 0 is the "before the beginning" sentinel with term 0, which is
/// what <see cref="TermAtAsync"/> returns for it — that makes the very first <c>AppendEntries</c> consistency
/// check fall out of the general case instead of needing a branch.
/// </remarks>
public interface IRaftWal : IAsyncDisposable
{
    /// <summary>Lowest index still retained. 1 until <see cref="TruncateHeadAsync"/> discards a prefix.</summary>
    long FirstIndex { get; }

    /// <summary>Highest index appended, durable or not. 0 when the log is empty.</summary>
    long LastIndex { get; }

    /// <summary>Term of <see cref="LastIndex"/>, or 0 when the log is empty.</summary>
    long LastTerm { get; }

    /// <summary>Highest index known to have reached stable storage. Never exceeds <see cref="LastIndex"/>.</summary>
    long DurableIndex { get; }

    /// <summary>
    /// Appends one entry. Does not fsync — durability is a separate, batched step, because one fsync per
    /// entry is the difference between thousands and hundreds of thousands of appends per second.
    /// </summary>
    /// <returns>The index the entry was written at.</returns>
    ValueTask<long> AppendAsync(RaftEntryHeader header, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default);

    /// <summary>
    /// Appends a contiguous batch in one write. The batch is the unit that matters: a replication message
    /// carries many entries and they should cost one I/O, not one each.
    /// </summary>
    /// <returns>The index of the last entry written, or the unchanged <see cref="LastIndex"/> if empty.</returns>
    ValueTask<long> AppendAsync(ReadOnlyMemory<RaftLogEntry> entries, CancellationToken cancellationToken = default);

    /// <summary>Forces everything up to <see cref="LastIndex"/> to stable storage and advances <see cref="DurableIndex"/>.</summary>
    ValueTask FlushAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Term of the entry at <paramref name="index"/>. Returns 0 for index 0, and -1 when the index has been
    /// discarded by head truncation or was never written — the caller treats that as "cannot verify".
    /// </summary>
    ValueTask<long> TermAtAsync(long index, CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams entries in index order. The payloads handed out are slices of a buffer this log owns and
    /// recycles after each <c>MoveNextAsync</c>; a consumer that keeps one past the next iteration must
    /// call <see cref="RaftLogEntry.ToOwned"/>.
    /// </summary>
    IAsyncEnumerable<RaftLogEntry> ReadFromAsync(long fromIndex, long toIndexInclusive, CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams just the fixed-size headers in index order, without reading each entry's payload off disk.
    /// For a message queue rebuilding a priority or due-time index at leader-takeover or restart — where
    /// every entry needs a look, but only <see cref="RaftEntryHeader.ApplicationTag"/> and
    /// <see cref="RaftEntryHeader.Index"/> matter — this is the difference between an I/O cost proportional
    /// to <see cref="RaftEntryHeader.Size"/> per entry and one proportional to the average payload size.
    /// </summary>
    /// <remarks>
    /// A header read alone cannot verify the record's checksum, which covers the payload too — a header this
    /// call hands out may belong to an entry whose payload is corrupt. That is an acceptable trade for an
    /// index rebuild: the corruption still surfaces the first time something actually tries to deliver that
    /// entry through <see cref="ReadFromAsync"/>, which does verify. Building an index is not delivering a
    /// message.
    /// </remarks>
    IAsyncEnumerable<RaftEntryHeader> ReadHeadersFromAsync(long fromIndex, long toIndexInclusive, CancellationToken cancellationToken = default);

    /// <summary>
    /// Discards <paramref name="fromIndex"/> and everything after it. This is the operation a follower needs
    /// when the leader's log diverges from its own (Raft §5.3), and it is the reason an append-only store
    /// that can only truncate its head is a poor fit for a Raft log.
    /// </summary>
    ValueTask TruncateTailAsync(long fromIndex, CancellationToken cancellationToken = default);

    /// <summary>Discards everything at or below <paramref name="uptoIndexInclusive"/>, after a snapshot.</summary>
    ValueTask TruncateHeadAsync(long uptoIndexInclusive, CancellationToken cancellationToken = default);

    /// <summary>
    /// Discards the entire log and restarts the index space just past a snapshot, so that
    /// <see cref="LastIndex"/> becomes <paramref name="lastIncludedIndex"/> and <see cref="TermAtAsync"/>
    /// answers <paramref name="lastIncludedTerm"/> for exactly that index.
    /// </summary>
    /// <remarks>
    /// What a follower does after installing a snapshot. The term has to survive the entry that carried it:
    /// the very next <c>AppendEntries</c> runs its consistency check against this boundary index, and a log
    /// that had forgotten the term would reject every entry after it, forever.
    /// </remarks>
    ValueTask ResetToSnapshotAsync(
        long lastIncludedIndex,
        long lastIncludedTerm,
        CancellationToken cancellationToken = default);
}

/// <summary>Opens the log of a group. The seam that keeps the consensus core free of any storage dependency.</summary>
public interface IRaftWalFactory
{
    ValueTask<IRaftWal> OpenAsync(RaftGroupId group, CancellationToken cancellationToken = default);

    /// <summary>Removes a group's log entirely — the queue it belonged to was deleted.</summary>
    ValueTask DeleteAsync(RaftGroupId group, CancellationToken cancellationToken = default);
}
