// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using Blun.MultiRaft.Wal;
using Microsoft.Win32.SafeHandles;

namespace Blun.MultiRaft;

/// <summary>The two values Raft requires to survive a restart: the current term and the vote cast in it.</summary>
/// <param name="Term">Latest term this node has seen.</param>
/// <param name="VotedFor">Who it voted for in that term, or <see langword="null"/> if it has not voted.</param>
public readonly record struct RaftMeta(long Term, NodeId? VotedFor)
{
    /// <summary>The state of a node that has never participated in a term.</summary>
    public static readonly RaftMeta Initial = new(0, null);
}

/// <summary>
/// Persists <see cref="RaftMeta"/> for a group. Separate from the log because it is overwritten in place
/// rather than appended to, and because it must be fsynced <em>before</em> a vote is granted — losing it is
/// how a node ends up voting twice in one term and electing two leaders.
/// </summary>
public interface IRaftMetaStore
{
    ValueTask<RaftMeta> ReadAsync(RaftGroupId group, CancellationToken cancellationToken = default);

    ValueTask WriteAsync(RaftGroupId group, RaftMeta meta, CancellationToken cancellationToken = default);

    ValueTask DeleteAsync(RaftGroupId group, CancellationToken cancellationToken = default);
}

/// <summary>Volatile meta store. Tests and transient groups only — with it, a restart can violate safety.</summary>
public sealed class InMemoryRaftMetaStore : IRaftMetaStore
{
    private readonly ConcurrentDictionary<RaftGroupId, RaftMeta> _meta = new();

    /// <inheritdoc />
    public ValueTask<RaftMeta> ReadAsync(RaftGroupId group, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(_meta.TryGetValue(group, out RaftMeta meta) ? meta : RaftMeta.Initial);

    /// <inheritdoc />
    public ValueTask WriteAsync(RaftGroupId group, RaftMeta meta, CancellationToken cancellationToken = default)
    {
        _meta[group] = meta;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask DeleteAsync(RaftGroupId group, CancellationToken cancellationToken = default)
    {
        _meta.TryRemove(group, out _);
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// One tiny fixed-size file per group, written and fsynced in place.
/// Fixed size and single-sector means the write is atomic in practice, so no double-buffering is needed.
/// </summary>
public sealed class FileRaftMetaStore : IRaftMetaStore
{
    private const int RecordSize = 24;
    private const ulong Magic = 0x424C554E4D455441; // "BLUNMETA"

    private readonly string _root;

    public FileRaftMetaStore(string root)
    {
        ArgumentException.ThrowIfNullOrEmpty(root);
        _root = root;
        Directory.CreateDirectory(root);
    }

    /// <inheritdoc />
    public async ValueTask<RaftMeta> ReadAsync(RaftGroupId group, CancellationToken cancellationToken = default)
    {
        string path = PathFor(group);
        if (!File.Exists(path))
        {
            return RaftMeta.Initial;
        }

        byte[] buffer = new byte[RecordSize];
        using SafeFileHandle handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        int read = await RandomAccess.ReadAsync(handle, buffer, 0, cancellationToken).ConfigureAwait(false);
        if (read < RecordSize || BinaryPrimitives.ReadUInt64LittleEndian(buffer) != Magic)
        {
            // A missing file is legitimate -- a node that has never voted. A file that exists but is short
            // or has the wrong magic is corruption, and must not be treated the same way: silently resetting
            // to term 0 / no vote is exactly how a node ends up voting twice in a term it already voted in.
            throw new InvalidOperationException(
                $"The Raft metadata for group {group.Value} is unreadable. Refusing to start rather than "
                + "silently resetting term and vote, which would allow a second vote in a term already voted in.");
        }

        long term = BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(8));
        ulong voted = BinaryPrimitives.ReadUInt64LittleEndian(buffer.AsSpan(16));
        return new RaftMeta(term, voted == ulong.MaxValue ? null : new NodeId(voted));
    }

    /// <inheritdoc />
    public async ValueTask WriteAsync(RaftGroupId group, RaftMeta meta, CancellationToken cancellationToken = default)
    {
        byte[] buffer = new byte[RecordSize];
        BinaryPrimitives.WriteUInt64LittleEndian(buffer, Magic);
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(8), meta.Term);
        BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(16), meta.VotedFor?.Value ?? ulong.MaxValue);

        using SafeFileHandle handle = File.OpenHandle(
            PathFor(group),
            FileMode.OpenOrCreate,
            FileAccess.Write,
            FileShare.None,
            FileOptions.Asynchronous);
        await RandomAccess.WriteAsync(handle, buffer, 0, cancellationToken).ConfigureAwait(false);

        // Synchronous fsync on purpose: this must be on disk before the caller acts on the new term or vote.
        RandomAccess.FlushToDisk(handle);
    }

    /// <inheritdoc />
    public ValueTask DeleteAsync(RaftGroupId group, CancellationToken cancellationToken = default)
    {
        string path = PathFor(group);
        if (File.Exists(path))
        {
            File.Delete(path);
        }

        return ValueTask.CompletedTask;
    }

    private string PathFor(RaftGroupId group)
        => Path.Combine(_root, "g" + group.Value.ToString("D20", CultureInfo.InvariantCulture) + ".meta");
}
