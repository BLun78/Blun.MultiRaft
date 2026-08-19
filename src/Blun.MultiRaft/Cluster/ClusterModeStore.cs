// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Buffers.Binary;
using Microsoft.Win32.SafeHandles;

namespace Blun.MultiRaft.Cluster;

/// <summary>What a node recorded about itself on a previous run.</summary>
/// <param name="Mode">The mode it ran in.</param>
/// <param name="Self">Which node it was. A marker belonging to a different node is a misplaced data directory.</param>
public readonly record struct ClusterModeMarker(ClusterMode Mode, NodeId Self);

/// <summary>
/// Remembers, across restarts, which mode this node ran in — the only way "the mode cannot change at
/// runtime" can mean anything beyond the lifetime of one process.
/// </summary>
/// <remarks>
/// One direction is allowed and one is not, and the asymmetry is about where the data is.
/// <para>
/// <b>Single-node to replicated is allowed</b>, and needs no mechanism beyond letting it happen. The node
/// that ran alone has the log; the nodes joining it are new and have none. Raft's election restriction
/// (§5.4.1) settles it without being asked: on equal terms the longer log wins, and against an empty log
/// every log is longer. The newcomers join as learners, are caught up from the existing log, and are
/// promoted — the ordinary single-server change path, with nothing special about it.
/// </para>
/// <para>
/// <b>Replicated to single-node is refused.</b> A node pulled out of a cluster still holds entries the rest
/// of that cluster committed, and letting it run alone makes it a second authority on the same group ids.
/// Both sides would then have real, committed, divergent histories, and there is no rule that reconciles
/// them — which is precisely why this has to be refused up front rather than resolved later.
/// </para>
/// </remarks>
public sealed class ClusterModeStore
{
    private const int RecordSize = 24;
    private const ulong Magic = 0x424C554E4D4F4445; // "BLUNMODE"

    private readonly string? _path;
    private ClusterModeMarker? _memory;

    /// <summary>Creates a store. A null or empty directory keeps the marker in memory only.</summary>
    public ClusterModeStore(string? directory)
    {
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
            _path = Path.Combine(directory, "cluster.mode");
        }
    }

    /// <summary>Whether the marker survives a restart. False for the in-memory store.</summary>
    public bool IsDurable => _path is not null;

    /// <summary>Reads the marker, or <see langword="null"/> when this node has never run before.</summary>
    public async ValueTask<ClusterModeMarker?> ReadAsync(CancellationToken cancellationToken = default)
    {
        if (_path is null)
        {
            return _memory;
        }

        if (!File.Exists(_path))
        {
            return null;
        }

        byte[] buffer = new byte[RecordSize];
        using SafeFileHandle handle = File.OpenHandle(_path, FileMode.Open, FileAccess.Read, FileShare.Read);
        int read = await RandomAccess.ReadAsync(handle, buffer, 0, cancellationToken).ConfigureAwait(false);
        if (read < RecordSize || BinaryPrimitives.ReadUInt64LittleEndian(buffer) != Magic)
        {
            return null;
        }

        return new ClusterModeMarker(
            (ClusterMode)BinaryPrimitives.ReadUInt64LittleEndian(buffer.AsSpan(8)),
            new NodeId(BinaryPrimitives.ReadUInt64LittleEndian(buffer.AsSpan(16))));
    }

    /// <summary>Writes the marker, fsynced before returning.</summary>
    public async ValueTask WriteAsync(ClusterModeMarker marker, CancellationToken cancellationToken = default)
    {
        if (_path is null)
        {
            _memory = marker;
            return;
        }

        byte[] buffer = new byte[RecordSize];
        BinaryPrimitives.WriteUInt64LittleEndian(buffer, Magic);
        BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(8), (ulong)marker.Mode);
        BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(16), marker.Self.Value);

        using SafeFileHandle handle = File.OpenHandle(
            _path,
            FileMode.OpenOrCreate,
            FileAccess.Write,
            FileShare.None,
            FileOptions.Asynchronous);
        await RandomAccess.WriteAsync(handle, buffer, 0, cancellationToken).ConfigureAwait(false);

        // Synchronous fsync, same as the Raft meta store: the node is about to act on this, and a marker that
        // is only in the page cache would not survive the crash it exists to guard against.
        RandomAccess.FlushToDisk(handle);
    }

    /// <summary>
    /// Checks the intended mode against what was recorded, and returns whether this start is the one-way
    /// single-node-to-replicated promotion.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The data directory belongs to another node, or the transition is the one that cannot be made safely.
    /// </exception>
    public static bool ValidateTransition(ClusterModeMarker? previous, ClusterMode intended, NodeId self)
    {
        if (previous is not { } marker)
        {
            return false;
        }

        if (marker.Self != self)
        {
            throw new InvalidOperationException(
                "This data directory was last used by node " + marker.Self + ", but this node is " + self
                + ". Pointing a node at another node's data would make two nodes authoritative for the same "
                + "log.");
        }

        if (marker.Mode == ClusterMode.Replicated && intended == ClusterMode.SingleNode)
        {
            throw new InvalidOperationException(
                "Node " + self + " last ran as part of a replicated cluster and cannot be restarted as a "
                + "single-node cluster. Its log holds entries the rest of that cluster committed; running it "
                + "alone would make it a second authority on the same groups, with no way to reconcile the "
                + "two histories afterwards.");
        }

        return marker.Mode == ClusterMode.SingleNode && intended == ClusterMode.Replicated;
    }
}
