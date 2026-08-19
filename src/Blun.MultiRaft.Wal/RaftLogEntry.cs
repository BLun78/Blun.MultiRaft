// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Blun.MultiRaft.Wal;

/// <summary>What a log record carries. A byte-wide enum so it costs one byte in the header and never boxes.</summary>
public enum RaftEntryKind : byte
{
    /// <summary>Opaque application payload — for Blun.MQ, an enqueue/ack/settle command.</summary>
    Command = 0,

    /// <summary>The empty entry a fresh leader appends to commit entries from previous terms (Raft §5.4.2).</summary>
    NoOp = 1,

    /// <summary>Single-server membership change (Raft §4.1). Payload is a <c>MembershipChangePayload</c>.</summary>
    Membership = 2,
}

/// <summary>
/// Fixed-size, blittable record header. Laid out explicitly at 32 bytes so it can be written to and read
/// from the WAL with <see cref="MemoryMarshal"/> — no serializer, no intermediate buffer, and the payload
/// can follow it as a second span in the same scatter-gather write.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 8, Size = Size)]
public readonly struct RaftEntryHeader : IEquatable<RaftEntryHeader>
{
    /// <summary>Serialized size in bytes. Must stay a multiple of 8 to keep the payload 8-byte aligned.</summary>
    public const int Size = 32;

    public readonly long Term;
    public readonly long Index;
    public readonly long TimestampTicks;
    public readonly int PayloadLength;
    public readonly RaftEntryKind Kind;

    /// <summary>
    /// Opaque to this library — never read or interpreted below <see cref="IRaftWal"/>. For Blun.MQ this
    /// carries a priority bucket (0-9) or an SMQ due-time bucket, put here specifically so a priority-index
    /// rebuild at leader-takeover or restart can use <see cref="IRaftWal.ReadHeadersFromAsync"/> and pay for
    /// <see cref="Size"/> bytes per entry instead of the whole record: the field that decides which bucket an
    /// entry belongs in travels with the header, not buried in a payload that would otherwise have to be
    /// deserialized just to find it.
    /// </summary>
    public readonly byte ApplicationTag;

    private readonly byte _reserved1;
    private readonly byte _reserved2;

    public RaftEntryHeader(
        long term,
        long index,
        RaftEntryKind kind,
        int payloadLength,
        long timestampTicks,
        byte applicationTag = 0)
    {
        Term = term;
        Index = index;
        TimestampTicks = timestampTicks;
        PayloadLength = payloadLength;
        Kind = kind;
        ApplicationTag = applicationTag;
        _reserved1 = _reserved2 = 0;
    }

    /// <summary>
    /// Reinterprets a header as bytes without copying — the span aliases the caller.s struct.
    /// Static, because a struct member may not hand out a reference to <c>this</c>; taking the header as an
    /// <c>in</c> parameter puts the lifetime where it belongs, with the caller.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlySpan<byte> AsBytes(ref readonly RaftEntryHeader header)
        => MemoryMarshal.AsBytes(new ReadOnlySpan<RaftEntryHeader>(in header));

    /// <summary>Reads a header straight out of a buffer. No allocation; the caller keeps owning the buffer.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static RaftEntryHeader Read(ReadOnlySpan<byte> source)
        => MemoryMarshal.Read<RaftEntryHeader>(source);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(Span<byte> destination) => MemoryMarshal.Write(destination, in this);

    public bool Equals(RaftEntryHeader other)
        => Term == other.Term && Index == other.Index && Kind == other.Kind
           && PayloadLength == other.PayloadLength && TimestampTicks == other.TimestampTicks
           && ApplicationTag == other.ApplicationTag;

    public override bool Equals(object? obj) => obj is RaftEntryHeader other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(Term, Index, (byte)Kind, PayloadLength, ApplicationTag);

    public static bool operator ==(RaftEntryHeader left, RaftEntryHeader right) => left.Equals(right);

    public static bool operator !=(RaftEntryHeader left, RaftEntryHeader right) => !left.Equals(right);
}

/// <summary>
/// One Raft log entry. A struct, so a batch of them lives in one array with no per-entry object.
/// <see cref="Payload"/> is a <em>slice of someone else's buffer</em> — the socket read buffer on the receive
/// path, a pooled buffer on the WAL read path — which is what keeps the replication path copy-free. It is
/// therefore only valid for as long as its owner says; anything that outlives the call must copy explicitly.
/// </summary>
public readonly struct RaftLogEntry
{
    public readonly RaftEntryHeader Header;
    public readonly ReadOnlyMemory<byte> Payload;

    public RaftLogEntry(in RaftEntryHeader header, ReadOnlyMemory<byte> payload)
    {
        Header = header;
        Payload = payload;
    }

    public RaftLogEntry(long term, long index, RaftEntryKind kind, ReadOnlyMemory<byte> payload, byte applicationTag = 0)
        : this(new RaftEntryHeader(term, index, kind, payload.Length, DateTime.UtcNow.Ticks, applicationTag), payload)
    {
    }

    public long Term => Header.Term;

    public long Index => Header.Index;

    public RaftEntryKind Kind => Header.Kind;

    /// <summary>True for the default instance — used as the "no such entry" sentinel instead of a nullable.</summary>
    public bool IsEmpty => Header.Index == 0 && Header.Term == 0 && Payload.IsEmpty;

    /// <summary>Deep-copies the payload so the entry can outlive the buffer it was parsed from.</summary>
    public RaftLogEntry ToOwned() => new(in Header, Payload.ToArray());
}
