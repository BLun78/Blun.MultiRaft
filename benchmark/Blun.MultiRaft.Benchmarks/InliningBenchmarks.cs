// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Buffers.Binary;
using System.IO.Hashing;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Benchmarks;

/// <summary>
/// Whether <c>[MethodImpl(MethodImplOptions.AggressiveInlining)]</c> buys anything on the shapes of method
/// this library actually has.
/// </summary>
/// <remarks>
/// <para>
/// These are <em>copies</em> of the shapes found in the WAL, not the shipped methods. They have to be: the
/// attribute is baked into metadata at compile time, so there is no way to measure one method with and
/// without it — only two methods that differ by it. Keep the bodies of each triplet identical, or the
/// comparison stops meaning anything.
/// </para>
/// <para>
/// The <c>Never</c> arm is the ruler, and without it the other two cannot be read at all. It forces a real
/// call, so it prices one. Then:
/// </para>
/// <list type="bullet">
///   <item><description><c>JitDecides</c> ≈ <c>Aggressive</c>, both well under <c>Never</c> — the JIT was
///   already inlining and the attribute is a no-op.</description></item>
///   <item><description><c>JitDecides</c> ≈ <c>Never</c>, <c>Aggressive</c> below both — the JIT declined
///   and the attribute genuinely changed the generated code.</description></item>
/// </list>
/// <para>
/// Each loop is a serial dependency chain rather than a sum of independent calls, deliberately: an
/// independent chain lets the JIT vectorise or hoist the inlined arm while the <c>Never</c> arm stays
/// scalar, which manufactures a difference that has nothing to do with the call. What is measured here is
/// latency per call, and the numbers are nanoseconds — read the ratios, and do not trust a difference
/// smaller than the run-to-run spread.
/// </para>
/// <para>
/// Timing answers "did it cost less"; it does not answer "was it inlined". For that, run with <c>--disasm</c>
/// (add <c>--disasmDepth 3</c> to follow the call into the callee) and look for whether the callee's body
/// appears in the caller or a <c>call</c> instruction survives. That is a fact rather than a measurement,
/// and it is the faster way to settle the question.
/// </para>
/// </remarks>
[MemoryDiagnoser(false)]
public class InliningBenchmarks
{
    /// <summary>
    /// Calls per invocation. A single call of these methods is around a nanosecond, which is below what
    /// BenchmarkDotNet can resolve per invocation; <c>OperationsPerInvoke</c> divides the loop back out so
    /// the reported figure is still per call.
    /// </summary>
    private const int Ops = 512;

    /// <summary>Fewer iterations for the arms that actually hash, since each one costs real work.</summary>
    private const int HashOps = 64;

    private const int HeaderSize = 32;
    private const int Alignment = 8;

    private readonly IMix _mixPlain = new PlainMix();
    private readonly IMix _mixAggressive = new AggressiveMix();
    private readonly PlainMix _mixDirect = new();

    private readonly IChecksum _checksumPlain = new PlainCrc32();
    private readonly IChecksum _checksumAggressive = new AggressiveCrc32();

    private byte[] _payload = [];
    private byte[] _scratch = [];

    /// <summary>A record-sized payload, the size the framing path sees for an ordinary queue command.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _payload = new byte[64];
        _scratch = new byte[8];
        Random.Shared.NextBytes(_payload);
    }

    // ---------------------------------------------------------------------------------------------------
    // Shape 1: trivial arithmetic, a handful of IL bytes. RaftWalRecord.SizeOf is this.
    // ---------------------------------------------------------------------------------------------------

    /// <summary>The shape the JIT is expected to inline unasked.</summary>
    [Benchmark(OperationsPerInvoke = Ops, Baseline = true)]
    [BenchmarkCategory("SizeOf")]
    public int SizeOf_JitDecides()
    {
        int acc = 0;
        for (int i = 0; i < Ops; i++)
        {
            acc = SizeOfDefault(acc & 4095, 4);
        }

        return acc;
    }

    /// <summary>Same body, asked for explicitly.</summary>
    [Benchmark(OperationsPerInvoke = Ops)]
    [BenchmarkCategory("SizeOf")]
    public int SizeOf_Aggressive()
    {
        int acc = 0;
        for (int i = 0; i < Ops; i++)
        {
            acc = SizeOfAggressive(acc & 4095, 4);
        }

        return acc;
    }

    /// <summary>Same body, forced to stay a call. This is what one call costs.</summary>
    [Benchmark(OperationsPerInvoke = Ops)]
    [BenchmarkCategory("SizeOf")]
    public int SizeOf_Never()
    {
        int acc = 0;
        for (int i = 0; i < Ops; i++)
        {
            acc = SizeOfNever(acc & 4095, 4);
        }

        return acc;
    }

    // ---------------------------------------------------------------------------------------------------
    // Shape 2: a switch over six arms ending in a throw -- well past the inline budget. This is
    // WalChecksumStrategy.ChecksumSizeFor, which is the one place the attribute was plausibly doing
    // something. (In the library it is called twice, both times cold, which is why it lost the attribute.)
    // ---------------------------------------------------------------------------------------------------

    /// <summary>The shape the JIT is expected to decline.</summary>
    [Benchmark(OperationsPerInvoke = Ops)]
    [BenchmarkCategory("ChecksumSize")]
    public int ChecksumSize_JitDecides()
    {
        int acc = 0;
        for (int i = 0; i < Ops; i++)
        {
            acc += ChecksumSizeDefault((WalChecksumAlgorithm)((acc + i) % 6));
        }

        return acc;
    }

    /// <summary>Same switch, forced inline. If the attribute ever pays, it pays here.</summary>
    [Benchmark(OperationsPerInvoke = Ops)]
    [BenchmarkCategory("ChecksumSize")]
    public int ChecksumSize_Aggressive()
    {
        int acc = 0;
        for (int i = 0; i < Ops; i++)
        {
            acc += ChecksumSizeAggressive((WalChecksumAlgorithm)((acc + i) % 6));
        }

        return acc;
    }

    /// <summary>Same switch, forced to stay a call.</summary>
    [Benchmark(OperationsPerInvoke = Ops)]
    [BenchmarkCategory("ChecksumSize")]
    public int ChecksumSize_Never()
    {
        int acc = 0;
        for (int i = 0; i < Ops; i++)
        {
            acc += ChecksumSizeNever((WalChecksumAlgorithm)((acc + i) % 6));
        }

        return acc;
    }

    // ---------------------------------------------------------------------------------------------------
    // Shape 3: the call goes through an interface, which is how every IWalChecksumStrategy call site
    // reaches its implementation. The attribute sits on the implementation, where an interface call cannot
    // use it until the JIT has devirtualised first (guarded devirtualisation off dynamic PGO). The direct
    // arm is the reference: it is the same body reached through the concrete type.
    // ---------------------------------------------------------------------------------------------------

    /// <summary>Interface call, implementation without the attribute.</summary>
    [Benchmark(OperationsPerInvoke = Ops)]
    [BenchmarkCategory("Dispatch")]
    public int Dispatch_Interface_Plain()
    {
        int acc = 0;
        for (int i = 0; i < Ops; i++)
        {
            acc = _mixPlain.Mix(acc + i);
        }

        return acc;
    }

    /// <summary>Interface call, implementation carrying the attribute.</summary>
    [Benchmark(OperationsPerInvoke = Ops)]
    [BenchmarkCategory("Dispatch")]
    public int Dispatch_Interface_Aggressive()
    {
        int acc = 0;
        for (int i = 0; i < Ops; i++)
        {
            acc = _mixAggressive.Mix(acc + i);
        }

        return acc;
    }

    /// <summary>The same implementation reached through the sealed type, with no interface in the way.</summary>
    [Benchmark(OperationsPerInvoke = Ops)]
    [BenchmarkCategory("Dispatch")]
    public int Dispatch_Direct()
    {
        int acc = 0;
        for (int i = 0; i < Ops; i++)
        {
            acc = _mixDirect.Mix(acc + i);
        }

        return acc;
    }

    // ---------------------------------------------------------------------------------------------------
    // Shape 4: shape 3 with the body the real strategies have. The point of this pair is the ratio against
    // the pair above -- whatever the call frame costs, this is what it is being compared against.
    // ---------------------------------------------------------------------------------------------------

    /// <summary>CRC-32 over 64 bytes through the interface, implementation without the attribute.</summary>
    [Benchmark(OperationsPerInvoke = HashOps)]
    [BenchmarkCategory("Checksum")]
    public int Checksum_Interface_Plain()
    {
        int acc = 0;
        for (int i = 0; i < HashOps; i++)
        {
            _payload[0] = (byte)acc;
            _checksumPlain.WriteChecksum(_payload, _scratch, 0);
            acc += _scratch[0];
        }

        return acc;
    }

    /// <summary>CRC-32 over 64 bytes through the interface, implementation carrying the attribute.</summary>
    [Benchmark(OperationsPerInvoke = HashOps)]
    [BenchmarkCategory("Checksum")]
    public int Checksum_Interface_Aggressive()
    {
        int acc = 0;
        for (int i = 0; i < HashOps; i++)
        {
            _payload[0] = (byte)acc;
            _checksumAggressive.WriteChecksum(_payload, _scratch, 0);
            acc += _scratch[0];
        }

        return acc;
    }

    // ---------------------------------------------------------------------------------------------------
    // The methods under test. Each triplet must stay byte-identical apart from the attribute.
    // ---------------------------------------------------------------------------------------------------

    private static int SizeOfDefault(int payloadLength, int checksumSize)
    {
        int raw = HeaderSize + payloadLength + checksumSize;
        return (raw + (Alignment - 1)) & ~(Alignment - 1);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SizeOfAggressive(int payloadLength, int checksumSize)
    {
        int raw = HeaderSize + payloadLength + checksumSize;
        return (raw + (Alignment - 1)) & ~(Alignment - 1);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int SizeOfNever(int payloadLength, int checksumSize)
    {
        int raw = HeaderSize + payloadLength + checksumSize;
        return (raw + (Alignment - 1)) & ~(Alignment - 1);
    }

    private static int ChecksumSizeDefault(WalChecksumAlgorithm algorithm) => algorithm switch
    {
        WalChecksumAlgorithm.Crc32 => 4,
        WalChecksumAlgorithm.Crc64 => 8,
        WalChecksumAlgorithm.XxHash32 => 4,
        WalChecksumAlgorithm.XxHash3 => 8,
        WalChecksumAlgorithm.XxHash64 => 8,
        WalChecksumAlgorithm.XxHash128 => 16,
        _ => throw new ArgumentOutOfRangeException(nameof(algorithm), algorithm, "Unknown checksum algorithm."),
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ChecksumSizeAggressive(WalChecksumAlgorithm algorithm) => algorithm switch
    {
        WalChecksumAlgorithm.Crc32 => 4,
        WalChecksumAlgorithm.Crc64 => 8,
        WalChecksumAlgorithm.XxHash32 => 4,
        WalChecksumAlgorithm.XxHash3 => 8,
        WalChecksumAlgorithm.XxHash64 => 8,
        WalChecksumAlgorithm.XxHash128 => 16,
        _ => throw new ArgumentOutOfRangeException(nameof(algorithm), algorithm, "Unknown checksum algorithm."),
    };

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int ChecksumSizeNever(WalChecksumAlgorithm algorithm) => algorithm switch
    {
        WalChecksumAlgorithm.Crc32 => 4,
        WalChecksumAlgorithm.Crc64 => 8,
        WalChecksumAlgorithm.XxHash32 => 4,
        WalChecksumAlgorithm.XxHash3 => 8,
        WalChecksumAlgorithm.XxHash64 => 8,
        WalChecksumAlgorithm.XxHash128 => 16,
        _ => throw new ArgumentOutOfRangeException(nameof(algorithm), algorithm, "Unknown checksum algorithm."),
    };

    /// <summary>Stands in for <c>IWalChecksumStrategy</c>, which is internal to the WAL project.</summary>
    private interface IMix
    {
        int Mix(int x);
    }

    private sealed class PlainMix : IMix
    {
        public int Mix(int x) => (x * 31) ^ (x >>> 3);
    }

    private sealed class AggressiveMix : IMix
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Mix(int x) => (x * 31) ^ (x >>> 3);
    }

    /// <summary>The real <c>Crc32ChecksumStrategy.WriteChecksum</c> body, copied.</summary>
    private interface IChecksum
    {
        void WriteChecksum(ReadOnlySpan<byte> data, Span<byte> destination, int checksumOffset);
    }

    private sealed class PlainCrc32 : IChecksum
    {
        public void WriteChecksum(ReadOnlySpan<byte> data, Span<byte> destination, int checksumOffset)
        {
            uint crc = Crc32.HashToUInt32(data);
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(checksumOffset, 4), crc);
        }
    }

    private sealed class AggressiveCrc32 : IChecksum
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteChecksum(ReadOnlySpan<byte> data, Span<byte> destination, int checksumOffset)
        {
            uint crc = Crc32.HashToUInt32(data);
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(checksumOffset, 4), crc);
        }
    }
}
