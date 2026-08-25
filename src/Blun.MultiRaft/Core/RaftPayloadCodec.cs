// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Buffers;
using System.Buffers.Binary;
using Blun.MultiRaft.Wal;
using K4os.Compression.LZ4;
using K4os.Compression.LZ4.Internal;

namespace Blun.MultiRaft.Core;

/// <summary>
/// Compresses payloads on the way into the log and expands them on the way out to the state machine.
/// Everything between those two points — the WAL, the transport, the follower's log — carries the compressed
/// bytes and never needs to know it, because <see cref="RaftEntryHeader.Compression"/> travels with them.
/// </summary>
/// <remarks>
/// Rationale for the level and the try-then-keep rule is in <c>doc/raft-payload-compression.md</c>, backed by
/// the measurements in <c>doc/compression-level-benchmark.md</c>.
/// <para>
/// Uses the raw LZ4 block codec with a four-byte length prefix rather than the LZ4 frame format the benchmark
/// compared. The frame's advantage is that it is self-describing, which is worth nothing here — the header
/// already says whether an entry is compressed, and both ends of this are the same library version. Its
/// content-length field, the one part that would have been useful (sizing the destination for a decode), is
/// not implemented by K4os for span targets at all: <c>LZ4EncoderSettings.ContentLength</c> throws
/// <see cref="NotImplementedException"/> from <c>ByteSpanLZ4FrameWriter</c>. Writing the length ourselves
/// costs four bytes instead of the frame's header and end mark, and makes the decode a single call into an
/// exactly-sized buffer.
/// </para>
/// </remarks>
internal static class RaftPayloadCodec
{
    /// <summary>Bytes of little-endian original length written ahead of the compressed block.</summary>
    private const int LengthPrefixBytes = sizeof(int);

    static RaftPayloadCodec()
    {
        // K4os pools its internal blocks in *pinned* memory, sized for its documented common case: short-lived
        // processes where nothing lives long enough for the pinning to matter. This library is the opposite —
        // one host process holding thousands of groups for weeks — and pinned blocks are exactly what the GC
        // cannot compact around. Capping the pool at the largest payload this library will ever hand to LZ4
        // (the 1024 KB message limit) plus a little room keeps the pooling benefit for the ordinary small
        // append while refusing to pin anything larger. Process-global by nature, set here rather than in
        // MultiRaftHost so that it also covers a RaftGroupInstance used directly, and only once something
        // actually compresses.
        PinnedMemory.MaxPooledSize = Mem.M1 + 128;
    }

    /// <summary>
    /// Compresses when it is worth it. Returns the original <paramref name="payload"/> and
    /// <see cref="RaftPayloadCompression.None"/> whenever compression is off, cannot apply, or did not
    /// actually make the payload smaller — so a caller can write the result without checking which happened.
    /// </summary>
    /// <remarks>
    /// When <paramref name="rented"/> comes back non-null the returned memory points into a pooled array and
    /// the caller must return it once the write is done. Handing it back rather than releasing it internally
    /// is what keeps the payload valid for the whole append, which outlives this call.
    /// </remarks>
    public static ReadOnlyMemory<byte> Compress(
        ReadOnlyMemory<byte> payload,
        RaftEntryKind kind,
        RaftPayloadCompression configured,
        out RaftPayloadCompression applied,
        out byte[]? rented)
    {
        applied = RaftPayloadCompression.None;
        rented = null;

        // Membership entries are read straight off the log by the replay that rebuilds configuration at
        // startup, which runs before any state machine exists and so has nowhere to decompress them. They are
        // also a handful of bytes, so there is nothing to win. NoOp carries no payload at all.
        if (configured == RaftPayloadCompression.None || kind != RaftEntryKind.Command || payload.IsEmpty)
        {
            return payload;
        }

        byte[] buffer = ArrayPool<byte>.Shared.Rent(LengthPrefixBytes + LZ4Codec.MaximumOutputSize(payload.Length));
        try
        {
            int written = LZ4Codec.Encode(
                payload.Span,
                buffer.AsSpan(LengthPrefixBytes),
                LZ4Level.L00_FAST);

            int total = LengthPrefixBytes + written;

            // Not smaller once the prefix is counted: random, already-compressed or encrypted bytes. Storing
            // it would cost space and force a pointless decode on every read. LZ4Codec.Encode also returns a
            // non-positive value when the block does not fit the destination, which lands here too.
            if (written <= 0 || total >= payload.Length)
            {
                ArrayPool<byte>.Shared.Return(buffer);
                return payload;
            }

            BinaryPrimitives.WriteInt32LittleEndian(buffer, payload.Length);
            applied = RaftPayloadCompression.Lz4Fast;
            rented = buffer;
            return buffer.AsMemory(0, total);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }
    }

    /// <summary>
    /// Reverses <see cref="Compress"/>. Returns <paramref name="entry"/> unchanged when it was never
    /// compressed, and otherwise an entry whose payload is a fresh array — which is also what makes it safe to
    /// hand to a state machine that outlives the WAL's recycled read buffer.
    /// </summary>
    public static RaftLogEntry Expand(in RaftLogEntry entry)
    {
        if (entry.Header.Compression == RaftPayloadCompression.None)
        {
            return entry;
        }

        ReadOnlySpan<byte> stored = entry.Payload.Span;
        if (stored.Length < LengthPrefixBytes)
        {
            throw new InvalidOperationException(
                $"Entry {entry.Index} of term {entry.Term} is marked compressed but is too short to carry a "
                + "length prefix.");
        }

        int originalLength = BinaryPrimitives.ReadInt32LittleEndian(stored);
        if (originalLength < 0)
        {
            throw new InvalidOperationException(
                $"Entry {entry.Index} of term {entry.Term} declares a negative payload length of {originalLength}.");
        }

        var destination = new byte[originalLength];
        int decoded = LZ4Codec.Decode(stored[LengthPrefixBytes..], destination);
        if (decoded != originalLength)
        {
            throw new InvalidOperationException(
                $"Entry {entry.Index} of term {entry.Term} declared {originalLength} bytes of payload but its "
                + $"compressed block yielded {decoded}.");
        }

        var header = new RaftEntryHeader(
            entry.Header.Term,
            entry.Header.Index,
            entry.Header.Kind,
            originalLength,
            entry.Header.TimestampTicks,
            entry.Header.ApplicationTag);

        return new RaftLogEntry(in header, destination);
    }
}
