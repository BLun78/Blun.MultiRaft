// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Exporters.Csv;
using K4os.Compression.LZ4;
using K4os.Compression.LZ4.Streams;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Benchmarks;

/// <summary>Payload shape, because compressibility is the whole question and depends on content, not just size.</summary>
public enum PayloadContentType
{
    /// <summary>Worst case: <see cref="Random"/> bytes, nothing for LZ4 to find.</summary>
    Random,

    /// <summary>Best-case approximation of a queue command: a JSON-like pattern repeated to fill the length.</summary>
    Repetitive,
}

/// <summary>
/// Which K4os entry point does the compressing. <see cref="Pickler"/> is the one-shot in-memory block API
/// already used by the size/level sweep; <see cref="Stream"/> and <see cref="Frame"/> are the two other
/// public encode paths K4os exposes, both producing the (self-describing, checksummed) LZ4 frame format
/// rather than <see cref="Pickler"/>'s bespoke block format. The question this axis exists to answer: past
/// what payload size, if any, does the extra frame bookkeeping in <see cref="Stream"/>/<see cref="Frame"/>
/// stop being what dominates, and which of the two wins once it doesn't.
/// </summary>
public enum CompressionApi
{
    /// <summary><see cref="LZ4Pickler.Pickle(byte[], LZ4Level)"/> — one call, one allocation, block format.</summary>
    Pickler,

    /// <summary>
    /// <see cref="LZ4Stream.Encode(System.IO.Stream, LZ4Level, int, bool)"/> — a chunked <see cref="System.IO.Stream"/>
    /// wrapper over a <see cref="System.IO.MemoryStream"/>. Pays for the <see cref="System.IO.Stream"/> abstraction
    /// (virtual calls, internal buffering) on every write; the API this library would use if it ever needs to
    /// compress something too large to hold as one contiguous span.
    /// </summary>
    Stream,

    /// <summary>
    /// <see cref="LZ4Frame.Encode(System.Span{byte}, System.Span{byte}, LZ4Level, int)"/> — span-to-span, no
    /// <see cref="System.IO.Stream"/> in the path, same frame format as <see cref="Stream"/>. The API this
    /// library would use for an in-memory payload that fits in one buffer, which is every WAL entry today.
    /// </summary>
    Frame,
}

/// <summary>
/// The compression choice under test, standing in for the <c>RaftPayloadCompression</c> enum this benchmark
/// exists to size. <see cref="None"/> is the required baseline; the rest mirror every level K4os exposes.
/// </summary>
public enum CompressionLevelOption
{
    None,
    L00_FAST,
    L03_HC,
    L04_HC,
    L05_HC,
    L06_HC,
    L07_HC,
    L08_HC,
    L09_HC,
    L10_OPT,
    L11_OPT,
    L12_MAX,
}

/// <summary>
/// Compress + WAL append + flush, end to end, across payload size and LZ4 level. Answers one question: past
/// what payload size does compression start paying for itself once fsync — not just the append — is in the
/// bill. See <c>doc/compression-level-benchmark.md</c> for the brief and, once run, the conclusion.
/// </summary>
/// <remarks>
/// Unlike <see cref="WalAppendBenchmarks"/>, flush is deliberately ON here (<c>FlushToDisk = true</c>). That
/// benchmark turns it off because fsync would bury the comparison it exists to make; this one measures fsync
/// on purpose, because "performance beats everything for the WAL" was the explicit brief — the number that
/// matters is what a caller actually waits for, not the append alone.
/// </remarks>
[MemoryDiagnoser]
[CsvExporter]
public class CompressionLevelBenchmarks
{
    /// <summary>
    /// Log-spaced stand-ins for a continuous 100 B - 1 MiB sweep. A full 100-byte-step sweep combined with 12
    /// levels and 2 content types is on the order of 250,000 BenchmarkDotNet cases, which does not run in any
    /// practical time; these 13 points cover the same span without that blow-up. Narrow further only around
    /// whatever threshold this run turns up.
    /// </summary>
    [Params(100, 500, 1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 128_000, 256_000, 512_000, 1_048_576)]
    public int PayloadBytes { get; set; }

    [ParamsAllValues]
    public CompressionLevelOption Compression { get; set; }

    [ParamsAllValues]
    public PayloadContentType ContentType { get; set; }

    /// <summary>
    /// Irrelevant when <see cref="Compression"/> is <see cref="CompressionLevelOption.None"/> — all three
    /// values run anyway (the case is cheap; keeping the matrix rectangular is simpler than special-casing
    /// it out) and should agree with each other within noise, which is itself a sanity check on the harness.
    /// </summary>
    [ParamsAllValues]
    public CompressionApi Api { get; set; }

    private string _root = string.Empty;
    private IRaftWalFactory _factory = null!;
    private IRaftWal _wal = null!;
    private byte[] _payload = [];
    private long _index;

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _root = Path.Combine(Path.GetTempPath(), "blun-compression-bench-" + Guid.NewGuid().ToString("N"));

        _factory = new SegmentedRaftWalFactory(
            _root,
            new SegmentedRaftWalOptions
            {
                // The point of this benchmark: fsync is part of what gets measured, not excluded from it.
                FlushToDisk = true,
                SegmentAccess = WalSegmentAccess.RandomAccess,

                // Large enough that no run rolls a segment mid-measurement.
                SegmentSizeBytes = 512L * 1024 * 1024,
            });

        _wal = await _factory.OpenAsync(new RaftGroupId(1)).ConfigureAwait(false);
        _payload = BuildPayload(PayloadBytes, ContentType);
        _index = _wal.LastIndex;
    }

    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        await _wal.DisposeAsync().ConfigureAwait(false);
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    /// <summary>Compress (if requested), append, flush. The full cost a caller actually waits for.</summary>
    [Benchmark]
    public async Task<long> CompressAppendFlush()
    {
        ReadOnlyMemory<byte> onWire = Compress();

        long index = ++_index;
        var header = new RaftEntryHeader(
            term: 1,
            index: index,
            RaftEntryKind.Command,
            onWire.Length,
            DateTime.UtcNow.Ticks);

        long result = await _wal.AppendAsync(header, onWire).ConfigureAwait(false);
        await _wal.FlushAsync().ConfigureAwait(false);
        return result;
    }

    private ReadOnlyMemory<byte> Compress()
    {
        if (Compression == CompressionLevelOption.None)
        {
            return _payload;
        }

        LZ4Level level = ToLz4Level(Compression);
        return Api switch
        {
            CompressionApi.Pickler => LZ4Pickler.Pickle(_payload, level),
            CompressionApi.Stream => CompressWithStream(_payload, level),
            CompressionApi.Frame => CompressWithFrame(_payload, level),
            _ => throw new ArgumentOutOfRangeException(nameof(Api), Api, message: null),
        };
    }

    /// <summary>
    /// The <see cref="System.IO.Stream"/>-based encoder: what this library would reach for if a payload did
    /// not fit in one contiguous buffer. Here it always does, so this measures pure API overhead against
    /// <see cref="CompressWithFrame"/> at the same level and size.
    /// </summary>
    private static byte[] CompressWithStream(byte[] payload, LZ4Level level)
    {
        using var target = new MemoryStream(capacity: LZ4Codec.MaximumOutputSize(payload.Length));
        using (var encoder = LZ4Stream.Encode(target, level, extraMemory: 0, leaveOpen: true))
        {
            encoder.Write(payload, 0, payload.Length);
        }

        return target.ToArray();
    }

    /// <summary>
    /// The span-to-span frame encoder: no <see cref="System.IO.Stream"/> in the path, one caller-owned
    /// buffer. The most direct fit for a WAL entry, which already lives as one contiguous span.
    /// </summary>
    private static ReadOnlyMemory<byte> CompressWithFrame(byte[] payload, LZ4Level level)
    {
        // Frame format adds a header/footer (magic, descriptor, content checksum) on top of the LZ4 block
        // bound; 64 bytes of slack comfortably covers it regardless of payload size.
        var buffer = new byte[LZ4Codec.MaximumOutputSize(payload.Length) + 64];
        int written = LZ4Frame.Encode(payload.AsSpan(), buffer.AsSpan(), level, extraMemory: 0);
        return buffer.AsMemory(0, written);
    }

    private static LZ4Level ToLz4Level(CompressionLevelOption option) => option switch
    {
        CompressionLevelOption.L00_FAST => LZ4Level.L00_FAST,
        CompressionLevelOption.L03_HC => LZ4Level.L03_HC,
        CompressionLevelOption.L04_HC => LZ4Level.L04_HC,
        CompressionLevelOption.L05_HC => LZ4Level.L05_HC,
        CompressionLevelOption.L06_HC => LZ4Level.L06_HC,
        CompressionLevelOption.L07_HC => LZ4Level.L07_HC,
        CompressionLevelOption.L08_HC => LZ4Level.L08_HC,
        CompressionLevelOption.L09_HC => LZ4Level.L09_HC,
        CompressionLevelOption.L10_OPT => LZ4Level.L10_OPT,
        CompressionLevelOption.L11_OPT => LZ4Level.L11_OPT,
        CompressionLevelOption.L12_MAX => LZ4Level.L12_MAX,
        _ => throw new ArgumentOutOfRangeException(nameof(option), option, message: null),
    };

    private static byte[] BuildPayload(int length, PayloadContentType contentType)
    {
        var buffer = new byte[length];
        if (contentType == PayloadContentType.Random)
        {
            Random.Shared.NextBytes(buffer);
            return buffer;
        }

        const string pattern = "{\"type\":\"enqueue\",\"queue\":\"orders\",\"priority\":3,\"body\":\"payload-";
        byte[] patternBytes = System.Text.Encoding.UTF8.GetBytes(pattern);
        for (int written = 0; written < length;)
        {
            int chunk = Math.Min(patternBytes.Length, length - written);
            patternBytes.AsSpan(0, chunk).CopyTo(buffer.AsSpan(written));
            written += chunk;
        }

        return buffer;
    }
}
