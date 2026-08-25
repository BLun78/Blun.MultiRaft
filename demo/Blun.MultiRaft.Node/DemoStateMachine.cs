// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;

using Blun.MultiRaft.Core;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Node;

/// <summary>
/// The demo's only "application": it does not enqueue anything, it just counts what it has seen, so that
/// <see cref="RaftGroupInstance.TakeSnapshotAsync"/> has a state to capture. Real hosts (Blun.MQ) apply
/// committed entries to actual queue state; this exists purely so the "take a snapshot" button in the
/// observer has something behind it to compact.
/// </summary>
internal sealed class DemoStateMachine : IRaftSnapshotableStateMachine
{
    private long _lastAppliedIndex;
    private long _bytesApplied;

    /// <inheritdoc />
    public ValueTask ApplyAsync(RaftGroupId group, RaftLogEntry entry, CancellationToken cancellationToken = default)
    {
        Interlocked.Exchange(ref _lastAppliedIndex, entry.Index);
        Interlocked.Add(ref _bytesApplied, entry.Payload.Length);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> CaptureAsync(
        RaftGroupId group,
        long lastAppliedIndex,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        byte[] body = new byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(body, lastAppliedIndex);
        BinaryPrimitives.WriteInt64LittleEndian(body.AsSpan(8), Interlocked.Read(ref _bytesApplied));
        yield return body;

        await ValueTask.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask RestoreAsync(
        RaftGroupId group,
        IAsyncEnumerable<ReadOnlyMemory<byte>> body,
        CancellationToken cancellationToken = default)
    {
        long bytesApplied = 0;

        await foreach (ReadOnlyMemory<byte> chunk in body.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (chunk.Length >= 16)
            {
                bytesApplied = BinaryPrimitives.ReadInt64LittleEndian(chunk.Span.Slice(8, 8));
            }
        }

        Volatile.Write(ref _bytesApplied, bytesApplied);
    }
}
