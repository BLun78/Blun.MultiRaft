// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Globalization;

namespace Blun.MultiRaft.Wal;

/// <summary>
/// Gives every group its own directory under a common root. One directory per group is what makes deleting
/// a queue's log an <c>rmdir</c> instead of a compaction.
/// </summary>
public sealed class SegmentedRaftWalFactory : IRaftWalFactory
{
    private readonly string _root;
    private readonly SegmentedRaftWalOptions _options;

    public SegmentedRaftWalFactory(string root, SegmentedRaftWalOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(root);
        _root = root;
        _options = options ?? new SegmentedRaftWalOptions();
    }

    /// <inheritdoc />
    public async ValueTask<IRaftWal> OpenAsync(RaftGroupId group, CancellationToken cancellationToken = default)
        => await SegmentedRaftWal.OpenAsync(DirectoryFor(group), _options, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public ValueTask DeleteAsync(RaftGroupId group, CancellationToken cancellationToken = default)
    {
        string directory = DirectoryFor(group);
        if (Directory.Exists(directory))
        {
            Directory.Delete(directory, recursive: true);
        }

        return ValueTask.CompletedTask;
    }

    private string DirectoryFor(RaftGroupId group)
        => Path.Combine(_root, "g" + group.Value.ToString("D20", CultureInfo.InvariantCulture));
}
