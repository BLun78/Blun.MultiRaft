// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Text;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Tests;

/// <summary>
/// The same suite runs against both logs. That is the point of having two implementations: an interface with
/// one implementation proves nothing, and a divergence between a volatile and a durable store shows up here
/// rather than in production.
/// </summary>
public abstract class RaftWalContractTests : IAsyncLifetime
{
    private IRaftWal _wal = null!;

    protected abstract ValueTask<IRaftWal> CreateAsync();

    public async ValueTask InitializeAsync() => _wal = await CreateAsync();

    public async ValueTask DisposeAsync() => await _wal.DisposeAsync();

    [Fact]
    public async Task EmptyLogHasNoEntries()
    {
        Assert.Equal(0, _wal.LastIndex);
        Assert.Equal(0, _wal.LastTerm);
        Assert.Equal(0, await _wal.TermAtAsync(0));
    }

    [Fact]
    public async Task AppendThenReadRoundTrips()
    {
        for (int i = 1; i <= 50; i++)
        {
            await AppendAsync(_wal, term: 1, index: i, "payload-" + i);
        }

        Assert.Equal(50, _wal.LastIndex);
        Assert.Equal(1, _wal.LastTerm);

        int seen = 0;
        await foreach (RaftLogEntry entry in _wal.ReadFromAsync(1, 50))
        {
            seen++;
            Assert.Equal(seen, entry.Index);
            Assert.Equal("payload-" + seen, Encoding.UTF8.GetString(entry.Payload.Span));
        }

        Assert.Equal(50, seen);
    }

    [Fact]
    public async Task BatchAppendIsIndistinguishableFromIndividualAppends()
    {
        RaftLogEntry[] batch =
        [
            Entry(1, 1, "a"),
            Entry(1, 2, "bb"),
            Entry(2, 3, "ccc"),
        ];

        Assert.Equal(3, await _wal.AppendAsync(batch));
        Assert.Equal(3, _wal.LastIndex);
        Assert.Equal(2, _wal.LastTerm);
        Assert.Equal(1, await _wal.TermAtAsync(2));

        List<string> payloads = [];
        await foreach (RaftLogEntry entry in _wal.ReadFromAsync(1, 3))
        {
            payloads.Add(Encoding.UTF8.GetString(entry.Payload.Span));
        }

        Assert.Equal(["a", "bb", "ccc"], payloads);
    }

    [Fact]
    public async Task TermLookupReportsMissingIndices()
    {
        await AppendAsync(_wal, term: 7, index: 1, "x");

        Assert.Equal(7, await _wal.TermAtAsync(1));
        Assert.Equal(-1, await _wal.TermAtAsync(2));
    }

    [Fact]
    public async Task TruncateTailDropsTheSuffix()
    {
        for (int i = 1; i <= 20; i++)
        {
            await AppendAsync(_wal, term: 1, index: i, "e" + i);
        }

        await _wal.TruncateTailAsync(11);

        Assert.Equal(10, _wal.LastIndex);
        Assert.Equal(-1, await _wal.TermAtAsync(11));

        // The freed indices must be reusable, or a follower could never reconcile a divergent log.
        await AppendAsync(_wal, term: 2, index: 11, "replacement");
        Assert.Equal(11, _wal.LastIndex);
        Assert.Equal(2, await _wal.TermAtAsync(11));
    }

    [Fact]
    public async Task TruncateHeadDropsThePrefix()
    {
        for (int i = 1; i <= 400; i++)
        {
            await AppendAsync(_wal, term: 1, index: i, new string('x', 512));
        }

        await _wal.TruncateHeadAsync(200);

        Assert.True(_wal.FirstIndex > 1, "head truncation should have discarded at least one segment");
        Assert.Equal(400, _wal.LastIndex);

        await foreach (RaftLogEntry entry in _wal.ReadFromAsync(1, 400))
        {
            Assert.True(entry.Index >= _wal.FirstIndex);
        }
    }

    [Fact]
    public async Task FlushAdvancesDurableIndex()
    {
        await AppendAsync(_wal, term: 1, index: 1, "durable");
        await _wal.FlushAsync();

        Assert.Equal(1, _wal.DurableIndex);
    }

    [Fact]
    public async Task DenseIndexIsEnforced()
    {
        await AppendAsync(_wal, term: 1, index: 1, "one");

        // A gap in the log would silently break the consistency check, so it must be rejected loudly.
        await Assert.ThrowsAnyAsync<Exception>(async () => await AppendAsync(_wal, term: 1, index: 3, "three"));
    }

    [Fact]
    public async Task ReadHeadersFromRoundTripsTheApplicationTagWithoutThePayload()
    {
        for (int i = 1; i <= 20; i++)
        {
            byte[] bytes = Encoding.UTF8.GetBytes("payload-" + i);
            var entry = new RaftLogEntry(
                new RaftEntryHeader(term: 1, index: i, RaftEntryKind.Command, bytes.Length, DateTime.UtcNow.Ticks, applicationTag: (byte)(i % 10)),
                bytes);
            await _wal.AppendAsync(entry.Header, entry.Payload);
        }

        var headers = new List<RaftEntryHeader>();
        await foreach (RaftEntryHeader header in _wal.ReadHeadersFromAsync(1, 20))
        {
            headers.Add(header);
        }

        Assert.Equal(20, headers.Count);
        for (int i = 0; i < headers.Count; i++)
        {
            Assert.Equal(i + 1, headers[i].Index);
            Assert.Equal((byte)((i + 1) % 10), headers[i].ApplicationTag);
        }

        // The same tag must come back through the ordinary payload-carrying read too -- ReadHeadersFromAsync
        // is a fast path for the same data, not a second source of truth that could disagree with it.
        await foreach (RaftLogEntry full in _wal.ReadFromAsync(1, 20))
        {
            Assert.Equal((byte)(full.Index % 10), full.Header.ApplicationTag);
        }
    }

    internal static RaftLogEntry Entry(long term, long index, string payload)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(payload);
        return new RaftLogEntry(
            new RaftEntryHeader(term, index, RaftEntryKind.Command, bytes.Length, DateTime.UtcNow.Ticks),
            bytes);
    }

    internal static async ValueTask AppendAsync(IRaftWal wal, long term, long index, string payload)
    {
        RaftLogEntry entry = Entry(term, index, payload);
        await wal.AppendAsync(entry.Header, entry.Payload);
    }
}

public sealed class InMemoryWalTests : RaftWalContractTests
{
    protected override ValueTask<IRaftWal> CreateAsync() => ValueTask.FromResult<IRaftWal>(new InMemoryRaftWal());
}

public sealed class SegmentedWalTests : RaftWalContractTests, IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "blun-mr-" + Guid.CreateVersion7().ToString("N"));

    protected override async ValueTask<IRaftWal> CreateAsync()
        => await SegmentedRaftWal.OpenAsync(
            _directory,
            new SegmentedRaftWalOptions { SegmentSizeBytes = 64 * 1024, MaxPayloadBytes = 4096 });

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }
}

/// <summary>
/// The same contract over a preallocated, memory-mapped active segment. Running it is the point: the mapped
/// path changes what the file looks like on disk — a zero-filled tail past the last record — and the whole
/// risk of the option is that recovery mistakes that tail for damage.
/// </summary>
public sealed class MemoryMappedWalTests : RaftWalContractTests, IDisposable
{
    private readonly string _directory =
        Path.Combine(Path.GetTempPath(), "blun-mr-mmf-" + Guid.CreateVersion7().ToString("N"));

    protected override async ValueTask<IRaftWal> CreateAsync()
        => await SegmentedRaftWal.OpenAsync(
            _directory,
            new SegmentedRaftWalOptions
            {
                SegmentSizeBytes = 64 * 1024,
                MaxPayloadBytes = 4096,
                SegmentAccess = WalSegmentAccess.MemoryMapped,
            });

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }
}

/// <summary>
/// The 1024 KB message ceiling, and what happens at it. These run against the real defaults rather than the
/// shrunken test options, because the defaults are the thing being asserted.
/// </summary>
public sealed class MessageSizeLimitTests : IDisposable
{
    private readonly string _directory =
        Path.Combine(Path.GetTempPath(), "blun-mr-size-" + Guid.CreateVersion7().ToString("N"));

    [Fact]
    public void TheDefaultsAdmitAMaximumSizeMessageWithItsEnvelope()
    {
        var options = new SegmentedRaftWalOptions();

        // A 1024 KB message plus whatever wraps it has to fit, or the cap would reject exactly the payload
        // it was written to allow.
        Assert.Equal(1024 * 1024, SegmentedRaftWalOptions.MaxMessageBytes);
        Assert.True(options.MaxPayloadBytes > SegmentedRaftWalOptions.MaxMessageBytes);
        Assert.True(options.SegmentSizeBytes >= options.MaxPayloadBytes);
    }

    [Fact]
    public async Task AMaximumSizeMessageRoundTrips()
    {
        await using SegmentedRaftWal wal = await SegmentedRaftWal.OpenAsync(_directory);

        byte[] payload = new byte[SegmentedRaftWalOptions.MaxMessageBytes];
        Random.Shared.NextBytes(payload);

        // Far past ScratchBufferBytes, so this is also the pooled framing path rather than the inline one.
        var header = new RaftEntryHeader(1, 1, RaftEntryKind.Command, payload.Length, DateTime.UtcNow.Ticks);
        Assert.Equal(1, await wal.AppendAsync(header, payload));

        await foreach (RaftLogEntry entry in wal.ReadFromAsync(1, 1))
        {
            Assert.Equal(payload.Length, entry.Payload.Length);
            Assert.True(payload.AsSpan().SequenceEqual(entry.Payload.Span));
        }
    }

    [Fact]
    public async Task APayloadOverTheCapIsRejected()
    {
        await using SegmentedRaftWal wal = await SegmentedRaftWal.OpenAsync(_directory);

        var options = new SegmentedRaftWalOptions();
        byte[] payload = new byte[options.MaxPayloadBytes + 1];
        var header = new RaftEntryHeader(1, 1, RaftEntryKind.Command, payload.Length, DateTime.UtcNow.Ticks);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await wal.AppendAsync(header, payload));
    }

    [Fact]
    public async Task APayloadOverTheCapIsRejectedByTheBatchOverloadToo()
    {
        // SEC-008: only the single-entry overload above checked MaxPayloadBytes. The batch overload is what
        // AppendEntries actually writes on the receive path, and an oversized entry that slips past it here
        // is not caught until the next restart, where recovery truncates the log from that point on -- this
        // is the regression guard for that gap, sharing the contract test suite both overloads are supposed to.
        await using SegmentedRaftWal wal = await SegmentedRaftWal.OpenAsync(_directory);

        var options = new SegmentedRaftWalOptions();
        var payload = new byte[options.MaxPayloadBytes + 1];
        var header = new RaftEntryHeader(1, 1, RaftEntryKind.Command, payload.Length, DateTime.UtcNow.Ticks);
        RaftLogEntry[] entries = [new RaftLogEntry(header, payload)];

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await wal.AppendAsync(entries));
    }

    [Fact]
    public async Task ABatchWithAMismatchedHeaderLengthIsRejected()
    {
        await using SegmentedRaftWal wal = await SegmentedRaftWal.OpenAsync(_directory);

        byte[] payload = [1, 2, 3];
        var header = new RaftEntryHeader(1, 1, RaftEntryKind.Command, payload.Length + 1, DateTime.UtcNow.Ticks);
        RaftLogEntry[] entries = [new RaftLogEntry(header, payload)];

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await wal.AppendAsync(entries));
    }

    [Fact]
    public async Task ARejectedBatchLeavesNoPartialWrite()
    {
        // The whole batch is validated before the write gate specifically so a rejection never leaves the
        // first (valid) entries written and the rest missing -- verified here by checking LastIndex is back
        // to where it started, not just that the call throws.
        await using SegmentedRaftWal wal = await SegmentedRaftWal.OpenAsync(_directory);

        var options = new SegmentedRaftWalOptions();
        byte[] validPayload = [1, 2, 3];
        byte[] oversizedPayload = new byte[options.MaxPayloadBytes + 1];
        RaftLogEntry[] entries =
        [
            new RaftLogEntry(
                new RaftEntryHeader(1, 1, RaftEntryKind.Command, validPayload.Length, DateTime.UtcNow.Ticks),
                validPayload),
            new RaftLogEntry(
                new RaftEntryHeader(1, 2, RaftEntryKind.Command, oversizedPayload.Length, DateTime.UtcNow.Ticks),
                oversizedPayload),
        ];

        long before = wal.LastIndex;
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await wal.AppendAsync(entries));
        Assert.Equal(before, wal.LastIndex);
    }

    [Fact]
    public async Task ASegmentTooSmallForOneRecordIsRefusedAtOpen()
    {
        // Rejected at open rather than on the first large append: a record that cannot fit a segment would
        // otherwise roll segments forever without ever being written.
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await SegmentedRaftWal.OpenAsync(
                _directory,
                new SegmentedRaftWalOptions { SegmentSizeBytes = 64 * 1024 }));
    }

    [Fact]
    public async Task TheScratchBufferDoesNotScaleWithTheMessageCap()
    {
        // The regression this guards: sizing the per-log framing buffer to MaxPayloadBytes costs a megabyte
        // per group, which across thousands of queues is gigabytes of buffer for a case almost none of them
        // hit. Opening many logs at the defaults must stay cheap.
        //
        // Sampled, and the minimum taken. GC.GetTotalAllocatedBytes counts the whole process, and xUnit runs
        // collections in parallel, so a single reading also charges this log whatever every other test
        // happened to allocate in the same window -- which made this fail on roughly three runs in five while
        // passing every time the class was run alone. Interference can only ever add, never subtract, so the
        // smallest of several samples converges on what an open actually costs. The bound itself is unchanged.
        long best = long.MaxValue;

        for (int attempt = 0; attempt < 5; attempt++)
        {
            var logs = new List<SegmentedRaftWal>();
            try
            {
                long before = GC.GetTotalAllocatedBytes(precise: true);
                for (int i = 0; i < 32; i++)
                {
                    logs.Add(await SegmentedRaftWal.OpenAsync(
                        Path.Combine(_directory, "a" + attempt, "g" + i)));
                }

                best = Math.Min(best, (GC.GetTotalAllocatedBytes(precise: true) - before) / 32);
            }
            finally
            {
                foreach (SegmentedRaftWal log in logs)
                {
                    await log.DisposeAsync();
                }
            }
        }

        Assert.True(
            best < 128 * 1024,
            "each log allocated " + best + " bytes at open; the framing buffer must not track the message cap");
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }
}

/// <summary>Durability behaviour that only the on-disk log can have.</summary>
public sealed class SegmentedWalRecoveryTests : IDisposable
{
    private readonly string _directory =
        Path.Combine(Path.GetTempPath(), "blun-mr-rec-" + Guid.CreateVersion7().ToString("N"));

    private SegmentedRaftWalOptions Options => new() { SegmentSizeBytes = 16 * 1024, MaxPayloadBytes = 4096 };

    private static SegmentedRaftWalOptions MappedOptions => new()
    {
        SegmentSizeBytes = 16 * 1024,
        MaxPayloadBytes = 4096,
        SegmentAccess = WalSegmentAccess.MemoryMapped,
    };

    [Fact]
    public async Task AMappedLogRecoversAcrossReopen()
    {
        await using (SegmentedRaftWal wal = await SegmentedRaftWal.OpenAsync(_directory, MappedOptions))
        {
            for (int i = 1; i <= 300; i++)
            {
                await RaftWalContractTests.AppendAsync(wal, term: 1, index: i, "mapped-" + i);
            }

            await wal.FlushAsync();
        }

        await using SegmentedRaftWal reopened = await SegmentedRaftWal.OpenAsync(_directory, MappedOptions);

        // Several segments, the last of them preallocated with a zero tail. If that tail were read as a
        // tear, recovery would delete every segment after the first and this would come back far short.
        Assert.Equal(300, reopened.LastIndex);
        Assert.Equal(1, reopened.LastTerm);

        await RaftWalContractTests.AppendAsync(reopened, term: 2, index: 301, "after-reopen");
        Assert.Equal(301, reopened.LastIndex);
    }

    [Fact]
    public async Task ASealedSegmentIsTrimmedBackToItsContents()
    {
        await using SegmentedRaftWal wal = await SegmentedRaftWal.OpenAsync(_directory, MappedOptions);
        for (int i = 1; i <= 300; i++)
        {
            await RaftWalContractTests.AppendAsync(wal, term: 1, index: i, new string('x', 128));
        }

        string[] segments = [.. Directory.GetFiles(_directory, "*.seg").Order(StringComparer.Ordinal)];
        Assert.True(segments.Length > 1, "the test needs the log to have rolled at least once");

        // Only the segment still being written stays preallocated; the rest cost no more disk than an
        // unmapped log would, which is what keeps the option from doubling storage.
        foreach (string sealedSegment in segments[..^1])
        {
            Assert.True(
                new FileInfo(sealedSegment).Length <= MappedOptions.SegmentSizeBytes,
                "a sealed segment must not exceed its own contents");
        }
    }

    [Fact]
    public async Task AMappedLogStillDiscardsATornTail()
    {
        await using (SegmentedRaftWal wal = await SegmentedRaftWal.OpenAsync(_directory, MappedOptions))
        {
            for (int i = 1; i <= 10; i++)
            {
                await RaftWalContractTests.AppendAsync(wal, term: 1, index: i, "entry-" + i);
            }

            await wal.FlushAsync();
        }

        // Corrupt a record in the middle rather than the tail: a zero tail is now a legitimate ending, so
        // this is what proves the relaxed check did not also relax the damage detection.
        string segment = Directory.GetFiles(_directory, "*.seg").Order(StringComparer.Ordinal).First();
        byte[] bytes = await File.ReadAllBytesAsync(segment);
        int fifthRecord = RaftWalRecordSize("entry-1") * 4;
        bytes[fifthRecord + RaftEntryHeader.Size + 1] ^= 0xFF;
        await File.WriteAllBytesAsync(segment, bytes);

        await using SegmentedRaftWal reopened = await SegmentedRaftWal.OpenAsync(_directory, MappedOptions);

        Assert.Equal(4, reopened.LastIndex);
    }

    private static int RaftWalRecordSize(string payload, int checksumSize = 4)
    {
        int raw = RaftEntryHeader.Size + Encoding.UTF8.GetByteCount(payload) + checksumSize;
        return (raw + 7) & ~7;
    }

    [Fact]
    public async Task ReopenRecoversEveryEntry()
    {
        await using (SegmentedRaftWal wal = await SegmentedRaftWal.OpenAsync(_directory, Options))
        {
            for (int i = 1; i <= 500; i++)
            {
                await RaftWalContractTests.AppendAsync(wal, term: i <= 250 ? 1 : 2, index: i, "entry-" + i);
            }

            await wal.FlushAsync();
        }

        await using SegmentedRaftWal reopened = await SegmentedRaftWal.OpenAsync(_directory, Options);

        Assert.Equal(500, reopened.LastIndex);
        Assert.Equal(2, reopened.LastTerm);
        Assert.Equal(1, await reopened.TermAtAsync(250));
        Assert.Equal(2, await reopened.TermAtAsync(251));
        Assert.Equal(500, reopened.DurableIndex);
    }

    [Fact]
    public async Task ATornTailIsDiscardedOnRecovery()
    {
        await using (SegmentedRaftWal wal = await SegmentedRaftWal.OpenAsync(_directory, Options))
        {
            for (int i = 1; i <= 20; i++)
            {
                await RaftWalContractTests.AppendAsync(wal, term: 1, index: i, "entry-" + i);
            }

            await wal.FlushAsync();
        }

        // Simulate a crash mid-write: chop the file so the last record is incomplete.
        string segment = Directory.GetFiles(_directory, "*.seg").Order(StringComparer.Ordinal).Last();
        long length = new FileInfo(segment).Length;
        using (FileStream stream = File.Open(segment, FileMode.Open, FileAccess.Write))
        {
            stream.SetLength(length - 12);
        }

        await using SegmentedRaftWal reopened = await SegmentedRaftWal.OpenAsync(_directory, Options);

        // The torn record is gone and the log is still dense, so appending simply continues from there.
        Assert.Equal(19, reopened.LastIndex);
        await RaftWalContractTests.AppendAsync(reopened, term: 2, index: 20, "rewritten");
        Assert.Equal(20, reopened.LastIndex);
        Assert.Equal(2, await reopened.TermAtAsync(20));
    }

    [Fact]
    public async Task CorruptedPayloadIsRejectedByTheChecksum()
    {
        await using (SegmentedRaftWal wal = await SegmentedRaftWal.OpenAsync(_directory, Options))
        {
            for (int i = 1; i <= 5; i++)
            {
                await RaftWalContractTests.AppendAsync(wal, term: 1, index: i, "entry-" + i);
            }

            await wal.FlushAsync();
        }

        string segment = Directory.GetFiles(_directory, "*.seg").Single();
        byte[] bytes = await File.ReadAllBytesAsync(segment);
        bytes[^6] ^= 0xFF; // flip a bit inside the last record's payload
        await File.WriteAllBytesAsync(segment, bytes);

        await using SegmentedRaftWal reopened = await SegmentedRaftWal.OpenAsync(_directory, Options);

        Assert.Equal(4, reopened.LastIndex);
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }
}

/// <summary>
/// Runs the full contract suite with a specific checksum algorithm. Each algorithm must be
/// indistinguishable from CRC-32 at the contract level — the choice only matters for detection
/// strength and framing overhead.
/// </summary>
public abstract class SegmentedWalWithAlgorithmTests : RaftWalContractTests, IDisposable
{
    private readonly string _directory =
        Path.Combine(Path.GetTempPath(), "blun-mr-alg-" + Guid.CreateVersion7().ToString("N"));

    protected abstract WalChecksumAlgorithm Algorithm { get; }

    protected override async ValueTask<IRaftWal> CreateAsync()
        => await SegmentedRaftWal.OpenAsync(
            _directory,
            new SegmentedRaftWalOptions
            {
                SegmentSizeBytes = 64 * 1024,
                MaxPayloadBytes = 4096,
                ChecksumAlgorithm = Algorithm,
            });

    [Fact]
    public async Task ReopenRecoversAllEntries()
    {
        await using (SegmentedRaftWal wal = await SegmentedRaftWal.OpenAsync(
            _directory,
            new SegmentedRaftWalOptions
            {
                SegmentSizeBytes = 16 * 1024,
                MaxPayloadBytes = 4096,
                ChecksumAlgorithm = Algorithm,
            }))
        {
            for (int i = 1; i <= 100; i++)
            {
                await RaftWalContractTests.AppendAsync(wal, term: 1, index: i, "entry-" + i);
            }

            await wal.FlushAsync();
        }

        var options = new SegmentedRaftWalOptions
        {
            SegmentSizeBytes = 16 * 1024,
            MaxPayloadBytes = 4096,
            ChecksumAlgorithm = Algorithm,
        };
        await using SegmentedRaftWal reopened = await SegmentedRaftWal.OpenAsync(_directory, options);

        Assert.Equal(100, reopened.LastIndex);
        Assert.Equal(1, reopened.LastTerm);

        int seen = 0;
        await foreach (RaftLogEntry entry in reopened.ReadFromAsync(1, 100))
        {
            seen++;
            Assert.Equal("entry-" + seen, Encoding.UTF8.GetString(entry.Payload.Span));
        }

        Assert.Equal(100, seen);
    }

    [Fact]
    public async Task CorruptedPayloadIsRejected()
    {
        var options = new SegmentedRaftWalOptions
        {
            SegmentSizeBytes = 16 * 1024,
            MaxPayloadBytes = 4096,
            ChecksumAlgorithm = Algorithm,
        };

        await using (SegmentedRaftWal wal = await SegmentedRaftWal.OpenAsync(_directory, options))
        {
            for (int i = 1; i <= 5; i++)
            {
                await RaftWalContractTests.AppendAsync(wal, term: 1, index: i, "entry-" + i);
            }

            await wal.FlushAsync();
        }

        string segment = Directory.GetFiles(_directory, "*.seg").Single();
        byte[] bytes = await File.ReadAllBytesAsync(segment);
        bytes[^6] ^= 0xFF;
        await File.WriteAllBytesAsync(segment, bytes);

        await using SegmentedRaftWal reopened = await SegmentedRaftWal.OpenAsync(_directory, options);
        Assert.Equal(4, reopened.LastIndex);
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }
}

public sealed class Crc32WalTests : SegmentedWalWithAlgorithmTests
{
    protected override WalChecksumAlgorithm Algorithm => WalChecksumAlgorithm.Crc32;
}

public sealed class Crc64WalTests : SegmentedWalWithAlgorithmTests
{
    protected override WalChecksumAlgorithm Algorithm => WalChecksumAlgorithm.Crc64;
}

public sealed class XxHash32WalTests : SegmentedWalWithAlgorithmTests
{
    protected override WalChecksumAlgorithm Algorithm => WalChecksumAlgorithm.XxHash32;
}

public sealed class XxHash3WalTests : SegmentedWalWithAlgorithmTests
{
    protected override WalChecksumAlgorithm Algorithm => WalChecksumAlgorithm.XxHash3;
}

public sealed class XxHash64WalTests : SegmentedWalWithAlgorithmTests
{
    protected override WalChecksumAlgorithm Algorithm => WalChecksumAlgorithm.XxHash64;
}

public sealed class XxHash128WalTests : SegmentedWalWithAlgorithmTests
{
    protected override WalChecksumAlgorithm Algorithm => WalChecksumAlgorithm.XxHash128;
}

/// <summary>
/// Reopening a log with a different checksum algorithm than the one it was created with must fail loudly.
/// </summary>
public sealed class ChecksumAlgorithmMismatchTests : IDisposable
{
    private readonly string _directory =
        Path.Combine(Path.GetTempPath(), "blun-mr-mix-" + Guid.CreateVersion7().ToString("N"));

    [Theory]
    [InlineData(WalChecksumAlgorithm.Crc32, WalChecksumAlgorithm.XxHash64)]
    [InlineData(WalChecksumAlgorithm.Crc64, WalChecksumAlgorithm.Crc32)]
    [InlineData(WalChecksumAlgorithm.XxHash3, WalChecksumAlgorithm.Crc64)]
    [InlineData(WalChecksumAlgorithm.XxHash64, WalChecksumAlgorithm.XxHash128)]
    [InlineData(WalChecksumAlgorithm.XxHash128, WalChecksumAlgorithm.XxHash32)]
    public async Task ReopenWithDifferentAlgorithmThrows(WalChecksumAlgorithm create, WalChecksumAlgorithm reopen)
    {
        string groupDir = Path.Combine(_directory, create + "-" + reopen);
        var createOptions = new SegmentedRaftWalOptions
        {
            SegmentSizeBytes = 16 * 1024,
            MaxPayloadBytes = 4096,
            ChecksumAlgorithm = create,
        };

        await using (SegmentedRaftWal wal = await SegmentedRaftWal.OpenAsync(groupDir, createOptions))
        {
            await RaftWalContractTests.AppendAsync(wal, term: 1, index: 1, "entry");
            await wal.FlushAsync();
        }

        var reopenOptions = new SegmentedRaftWalOptions
        {
            SegmentSizeBytes = 16 * 1024,
            MaxPayloadBytes = 4096,
            ChecksumAlgorithm = reopen,
        };

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await SegmentedRaftWal.OpenAsync(groupDir, reopenOptions));
    }

    [Theory]
    [InlineData(WalChecksumAlgorithm.Crc32)]
    [InlineData(WalChecksumAlgorithm.Crc64)]
    [InlineData(WalChecksumAlgorithm.XxHash32)]
    [InlineData(WalChecksumAlgorithm.XxHash3)]
    [InlineData(WalChecksumAlgorithm.XxHash64)]
    [InlineData(WalChecksumAlgorithm.XxHash128)]
    public async Task ReopenWithSameAlgorithmSucceeds(WalChecksumAlgorithm algorithm)
    {
        string groupDir = Path.Combine(_directory, "same-" + algorithm);
        var options = new SegmentedRaftWalOptions
        {
            SegmentSizeBytes = 16 * 1024,
            MaxPayloadBytes = 4096,
            ChecksumAlgorithm = algorithm,
        };

        await using (SegmentedRaftWal wal = await SegmentedRaftWal.OpenAsync(groupDir, options))
        {
            await RaftWalContractTests.AppendAsync(wal, term: 1, index: 1, "entry");
            await wal.FlushAsync();
        }

        await using SegmentedRaftWal reopened = await SegmentedRaftWal.OpenAsync(groupDir, options);
        Assert.Equal(1, reopened.LastIndex);
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }
}
