// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Benchmarks;

/// <summary>
/// The append path, which is the only part of this library that runs per message.
/// </summary>
/// <remarks>
/// fsync is off. Not to flatter the numbers but to make them mean something: a real
/// <see cref="RandomAccess.FlushToDisk"/> costs whatever the drive costs, which on a consumer SSD is one to
/// two orders of magnitude more than everything measured here put together, and it would bury the difference
/// between the two segment access modes — which is the entire question this benchmark exists to answer.
/// Durability is a platform property, and it is tested elsewhere; this measures the code.
/// <para>
/// Read the ratios rather than the absolutes. These run under <c>--job short</c>, where the variance on the
/// batched 4 KB figures alone is in the tens of microseconds.
/// </para>
/// </remarks>
[MemoryDiagnoser]
public class WalAppendBenchmarks
{
    private const int BatchSize = 32;

    private string _root = string.Empty;
    private IRaftWalFactory _factory = null!;
    private IRaftWal _wal = null!;
    private byte[] _payload = [];
    private RaftLogEntry[] _batch = [];
    private long _index;

    /// <summary>64 bytes is a queue command; 4096 is a message that is actually carrying something.</summary>
    [Params(64, 4096)]
    public int PayloadBytes { get; set; }

    /// <summary>
    /// The comparison the whole benchmark is for: one syscall per append against a preallocated mapped view.
    /// </summary>
    [Params(WalSegmentAccess.RandomAccess, WalSegmentAccess.MemoryMapped)]
    public WalSegmentAccess Access { get; set; }

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _root = Path.Combine(Path.GetTempPath(), "blun-wal-bench-" + Guid.NewGuid().ToString("N"));

        _factory = new SegmentedRaftWalFactory(
            _root,
            new SegmentedRaftWalOptions
            {
                FlushToDisk = false,
                SegmentAccess = Access,

                // Large enough that a run does not spend its time rolling segments, which is a different
                // operation with a different cost and is not what is being measured here.
                SegmentSizeBytes = 512L * 1024 * 1024,
            });

        _wal = await _factory.OpenAsync(new RaftGroupId(1)).ConfigureAwait(false);

        _payload = new byte[PayloadBytes];
        Random.Shared.NextBytes(_payload);

        _batch = new RaftLogEntry[BatchSize];
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

    /// <summary>One entry, one append. What a queue with no traffic to coalesce actually pays.</summary>
    [Benchmark(Baseline = true)]
    public async Task<long> AppendSingle()
    {
        long index = ++_index;
        var header = new RaftEntryHeader(
            term: 1,
            index: index,
            RaftEntryKind.Command,
            _payload.Length,
            DateTime.UtcNow.Ticks);

        return await _wal.AppendAsync(header, _payload).ConfigureAwait(false);
    }

    /// <summary>
    /// Thirty-two entries in one call, reported per entry. Batching and mapping are substitutes rather than
    /// complements — both remove the same syscall — so this is only worth what the unmapped path pays for it.
    /// </summary>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    public async Task<long> AppendBatch()
    {
        for (int i = 0; i < BatchSize; i++)
        {
            long index = ++_index;
            _batch[i] = new RaftLogEntry(
                new RaftEntryHeader(
                    term: 1,
                    index: index,
                    RaftEntryKind.Command,
                    _payload.Length,
                    DateTime.UtcNow.Ticks),
                _payload);
        }

        return await _wal.AppendAsync(_batch).ConfigureAwait(false);
    }
}

/// <summary>
/// The in-memory log on the same path, as the control. It copies every payload deliberately, having no
/// buffer of its own, which is why its allocation figure tracks payload size where the segmented log's
/// does not.
/// </summary>
[MemoryDiagnoser]
public class InMemoryWalAppendBenchmarks
{
    private IRaftWal _wal = null!;
    private byte[] _payload = [];
    private long _index;

    [Params(64, 4096)]
    public int PayloadBytes { get; set; }

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _wal = await new InMemoryRaftWalFactory().OpenAsync(new RaftGroupId(1)).ConfigureAwait(false);
        _payload = new byte[PayloadBytes];
        Random.Shared.NextBytes(_payload);
        _index = _wal.LastIndex;
    }

    [GlobalCleanup]
    public async Task CleanupAsync() => await _wal.DisposeAsync().ConfigureAwait(false);

    [Benchmark]
    public async Task<long> AppendSingle()
    {
        long index = ++_index;
        var header = new RaftEntryHeader(
            term: 1,
            index: index,
            RaftEntryKind.Command,
            _payload.Length,
            DateTime.UtcNow.Ticks);

        return await _wal.AppendAsync(header, _payload).ConfigureAwait(false);
    }
}

/// <summary>Entry point.</summary>
public static class Program
{
    /// <summary>
    /// Runs the suite. With no arguments it supplies <c>--runtimes net10.0 net11.0</c> itself.
    /// </summary>
    /// <remarks>
    /// The default is the point of the project: a figure from one runtime says how fast the code is on that
    /// runtime, and nothing about whether a change helped or the runtime did. Making the comparison the
    /// default means the interesting number is the one you get without having to remember to ask for it.
    /// </remarks>
    public static void Main(string[] args)
    {
        string[] effective = args.Length == 0
            ? ["--runtimes", "net10.0", "net11.0"]
            : args;

        BenchmarkSwitcher
            .FromAssembly(typeof(Program).Assembly)
            .Run(effective, DefaultConfig.Instance.WithOptions(ConfigOptions.DisableOptimizationsValidator));
    }
}
