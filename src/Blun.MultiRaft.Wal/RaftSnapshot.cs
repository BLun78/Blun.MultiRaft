// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;

namespace Blun.MultiRaft.Wal;

/// <summary>
/// What a snapshot replaces. The index and term are the log position it stands for; the follower installing
/// it resumes as if it had received every entry up to and including that point.
/// </summary>
/// <param name="LastIncludedIndex">The highest log index the snapshot's contents account for.</param>
/// <param name="LastIncludedTerm">The term of that entry, needed for the next consistency check.</param>
/// <param name="Configuration">
/// The group configuration in force at that index, serialized. It travels with the snapshot because a
/// follower that discarded its log would otherwise have no way to learn who is in the group — including,
/// possibly, that it is no longer in it.
/// </param>
public readonly record struct RaftSnapshotMetadata(
    long LastIncludedIndex,
    long LastIncludedTerm,
    ReadOnlyMemory<byte> Configuration)
{
    /// <summary>Size of the fixed header written in front of a stored snapshot.</summary>
    internal const int HeaderSize = 28;

    internal const ulong Magic = 0x424C554E534E4150; // "BLUNSNAP"
}

/// <summary>
/// Where a group's snapshot lives. Separate from <see cref="IRaftWal"/> because it is written whole and read
/// whole, has no index space, and is replaced rather than appended to.
/// </summary>
public interface IRaftSnapshotStore
{
    /// <summary>Metadata of the stored snapshot, or <see langword="null"/> when the group has none.</summary>
    ValueTask<RaftSnapshotMetadata?> ReadMetadataAsync(RaftGroupId group, CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams the snapshot body in chunks. Chunked rather than handed over whole because a snapshot is the
    /// one thing in this system with no size bound — it is as large as the state machine is.
    /// </summary>
    IAsyncEnumerable<ReadOnlyMemory<byte>> ReadAsync(RaftGroupId group, CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces the stored snapshot. The write is staged and swapped in at the end, so a crash halfway
    /// through leaves the previous snapshot intact rather than a half-written one that looks complete.
    /// </summary>
    ValueTask WriteAsync(
        RaftGroupId group,
        RaftSnapshotMetadata metadata,
        IAsyncEnumerable<ReadOnlyMemory<byte>> body,
        CancellationToken cancellationToken = default);

    /// <summary>Removes a group's snapshot.</summary>
    ValueTask DeleteAsync(RaftGroupId group, CancellationToken cancellationToken = default);
}

/// <summary>Volatile snapshot store. Tests, and groups whose state does not outlive the process.</summary>
public sealed class InMemoryRaftSnapshotStore : IRaftSnapshotStore
{
    private readonly ConcurrentDictionary<RaftGroupId, (RaftSnapshotMetadata Metadata, byte[] Body)> _snapshots = new();

    /// <inheritdoc />
    public ValueTask<RaftSnapshotMetadata?> ReadMetadataAsync(
        RaftGroupId group,
        CancellationToken cancellationToken = default)
        => ValueTask.FromResult<RaftSnapshotMetadata?>(
            _snapshots.TryGetValue(group, out var stored) ? stored.Metadata : null);

    /// <inheritdoc />
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadAsync(
        RaftGroupId group,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (_snapshots.TryGetValue(group, out var stored))
        {
            yield return stored.Body;
        }

        await ValueTask.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask WriteAsync(
        RaftGroupId group,
        RaftSnapshotMetadata metadata,
        IAsyncEnumerable<ReadOnlyMemory<byte>> body,
        CancellationToken cancellationToken = default)
    {
        using var buffer = new MemoryStream();
        await foreach (ReadOnlyMemory<byte> chunk in body.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            await buffer.WriteAsync(chunk, cancellationToken).ConfigureAwait(false);
        }

        // Swapped in only once the whole body has been consumed, so a failure mid-stream leaves the previous
        // snapshot in place rather than a truncated one.
        _snapshots[group] = (metadata with { Configuration = metadata.Configuration.ToArray() }, buffer.ToArray());
    }

    /// <inheritdoc />
    public ValueTask DeleteAsync(RaftGroupId group, CancellationToken cancellationToken = default)
    {
        _snapshots.TryRemove(group, out _);
        return ValueTask.CompletedTask;
    }
}

/// <summary>One file per group, written to a temporary name and moved into place.</summary>
public sealed class FileRaftSnapshotStore : IRaftSnapshotStore
{
    private const int ChunkBytes = 64 * 1024;

    private readonly string _root;

    public FileRaftSnapshotStore(string root)
    {
        ArgumentException.ThrowIfNullOrEmpty(root);
        _root = root;
        Directory.CreateDirectory(root);
    }

    /// <inheritdoc />
    public async ValueTask<RaftSnapshotMetadata?> ReadMetadataAsync(
        RaftGroupId group,
        CancellationToken cancellationToken = default)
    {
        string path = PathFor(group);
        if (!File.Exists(path))
        {
            return null;
        }

        await using FileStream stream = File.OpenRead(path);
        byte[] header = new byte[RaftSnapshotMetadata.HeaderSize];
        if (await stream.ReadAtLeastAsync(header, header.Length, throwOnEndOfStream: false, cancellationToken)
                .ConfigureAwait(false) < header.Length
            || BinaryPrimitives.ReadUInt64LittleEndian(header) != RaftSnapshotMetadata.Magic)
        {
            return null;
        }

        long index = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(8));
        long term = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(16));
        int configLength = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(24));

        byte[] configuration = new byte[configLength];
        if (configLength > 0)
        {
            await stream.ReadExactlyAsync(configuration, cancellationToken).ConfigureAwait(false);
        }

        return new RaftSnapshotMetadata(index, term, configuration);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadAsync(
        RaftGroupId group,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        string path = PathFor(group);
        if (!File.Exists(path))
        {
            yield break;
        }

        RaftSnapshotMetadata? metadata = await ReadMetadataAsync(group, cancellationToken).ConfigureAwait(false);
        if (metadata is null)
        {
            yield break;
        }

        await using FileStream stream = File.OpenRead(path);
        stream.Seek(RaftSnapshotMetadata.HeaderSize + metadata.Value.Configuration.Length, SeekOrigin.Begin);

        byte[] chunk = new byte[ChunkBytes];
        while (true)
        {
            int read = await stream.ReadAsync(chunk, cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                yield break;
            }

            yield return chunk.AsMemory(0, read);
        }
    }

    /// <inheritdoc />
    public async ValueTask WriteAsync(
        RaftGroupId group,
        RaftSnapshotMetadata metadata,
        IAsyncEnumerable<ReadOnlyMemory<byte>> body,
        CancellationToken cancellationToken = default)
    {
        string path = PathFor(group);
        string staging = path + ".tmp";

        await using (FileStream stream = File.Create(staging))
        {
            byte[] header = new byte[RaftSnapshotMetadata.HeaderSize];
            BinaryPrimitives.WriteUInt64LittleEndian(header, RaftSnapshotMetadata.Magic);
            BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(8), metadata.LastIncludedIndex);
            BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(16), metadata.LastIncludedTerm);
            BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(24), metadata.Configuration.Length);

            await stream.WriteAsync(header, cancellationToken).ConfigureAwait(false);
            await stream.WriteAsync(metadata.Configuration, cancellationToken).ConfigureAwait(false);

            await foreach (ReadOnlyMemory<byte> chunk in body.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                await stream.WriteAsync(chunk, cancellationToken).ConfigureAwait(false);
            }

            stream.Flush(flushToDisk: true);
        }

        // The move is the commit point. Writing in place would leave a snapshot that looks complete but is
        // not if the process died mid-write — and a replica would install it and silently lose state.
        File.Move(staging, path, overwrite: true);
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
        => Path.Combine(_root, "g" + group.Value.ToString("D20", CultureInfo.InvariantCulture) + ".snap");
}
