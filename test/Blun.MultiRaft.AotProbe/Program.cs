// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

// AOT smoke test for Blun.MultiRaft.Wal. Two things are being checked, and they fail in different places:
//
//   1. That ILC compiles the WAL with no trim or AOT warnings. That failure happens at publish time, in CI,
//      before this file ever runs.
//   2. That the resulting native binary actually works. Reflection that ILC could not see, a generic
//      instantiation that was trimmed away, a static constructor ordering difference -- none of those show up
//      as a warning, only as wrong behaviour at runtime. Hence the assertions below rather than a bare
//      "it started, ship it".
//
// It is a console app rather than an xUnit test on purpose: the test suite runs on the CLR, so running it
// would prove nothing about NativeAOT no matter how thorough it got.

using System.Globalization;
using Blun.MultiRaft.Wal;

string root = Path.Combine(
    Path.GetTempPath(),
    "blun-aotprobe-" + Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(root);

// The two axes that pick different code paths inside the log: the checksum strategy (a switch over static
// singletons, which is exactly the shape a trimmer can get wrong) and the segment device (RandomAccess vs the
// unsafe memory-mapped one).
WalChecksumAlgorithm[] algorithms = Enum.GetValues<WalChecksumAlgorithm>();
WalSegmentAccess[] accessModes = Enum.GetValues<WalSegmentAccess>();

const int Written = 200;
const int TailFrom = 150;
const int HeadUpto = 20;

try
{
    foreach (WalChecksumAlgorithm algorithm in algorithms)
    {
        foreach (WalSegmentAccess access in accessModes)
        {
            await ProbeAsync(root, algorithm, access).ConfigureAwait(false);
        }
    }
}
finally
{
    try
    {
        Directory.Delete(root, recursive: true);
    }
    catch (IOException)
    {
        // A leftover temp directory is not worth failing the probe over.
    }
}

Console.WriteLine(string.Create(
    CultureInfo.InvariantCulture,
    $"AOT probe OK: {algorithms.Length} checksum algorithms x {accessModes.Length} segment access modes."));
return 0;

static async Task ProbeAsync(string root, WalChecksumAlgorithm algorithm, WalSegmentAccess access)
{
    SegmentedRaftWalOptions options = new()
    {
        ChecksumAlgorithm = algorithm,
        SegmentAccess = access,
        FlushToDisk = true,
        // Small enough that the 200 entries below roll the segment several times, so segment rolling and the
        // multi-segment read path are covered rather than assumed.
        SegmentSizeBytes = 1L * 1024 * 1024,
        MaxPayloadBytes = 64 * 1024,
    };

    SegmentedRaftWalFactory factory = new(
        Path.Combine(root, algorithm.ToString() + "-" + access.ToString()),
        options);
    RaftGroupId group = new(42);

    await using (IRaftWal wal = await factory.OpenAsync(group).ConfigureAwait(false))
    {
        for (int i = 1; i <= Written; i++)
        {
            byte[] payload = new byte[97];
            payload.AsSpan().Fill((byte)i);
            RaftEntryHeader header = new(
                term: 3,
                index: i,
                RaftEntryKind.Command,
                payload.Length,
                DateTime.UtcNow.Ticks,
                applicationTag: (byte)(i % 10));

            long at = await wal.AppendAsync(header, payload).ConfigureAwait(false);
            Expect(at == i, $"append returned index {at}, expected {i}");
        }

        await wal.FlushAsync().ConfigureAwait(false);
        Expect(wal.DurableIndex == Written, $"DurableIndex is {wal.DurableIndex} after flush, expected {Written}");

        await wal.TruncateTailAsync(TailFrom).ConfigureAwait(false);
        await wal.TruncateHeadAsync(HeadUpto).ConfigureAwait(false);
        Expect(wal.LastIndex == TailFrom - 1, $"LastIndex is {wal.LastIndex} after tail truncation");
    }

    // Reopening is the interesting half: it runs the recovery scan, which re-frames every record and verifies
    // its checksum. A trimmed-away hash implementation would surface right here.
    await using (IRaftWal wal = await factory.OpenAsync(group).ConfigureAwait(false))
    {
        Expect(wal.LastIndex == TailFrom - 1, $"LastIndex is {wal.LastIndex} after reopen");

        long expected = wal.FirstIndex;
        await foreach (RaftLogEntry entry in wal.ReadFromAsync(wal.FirstIndex, wal.LastIndex).ConfigureAwait(false))
        {
            Expect(entry.Index == expected, $"read index {entry.Index}, expected {expected}");
            Expect(entry.Term == 3, $"read term {entry.Term} at index {entry.Index}");
            // Each payload was filled with its own index, so a silently wrong buffer is caught rather than
            // just a wrong length.
            Expect(
                entry.Payload.Length == 97 && entry.Payload.Span[0] == (byte)entry.Index,
                $"payload contents wrong at index {entry.Index}");
            expected++;
        }

        Expect(expected == wal.LastIndex + 1, $"stream stopped at {expected - 1}, expected {wal.LastIndex}");

        long headers = 0;
        await foreach (RaftEntryHeader header in wal.ReadHeadersFromAsync(wal.FirstIndex, wal.LastIndex).ConfigureAwait(false))
        {
            Expect(header.ApplicationTag == (byte)(header.Index % 10), $"tag wrong at index {header.Index}");
            headers++;
        }

        Expect(headers == wal.LastIndex - wal.FirstIndex + 1, $"header stream yielded {headers}");

        long term = await wal.TermAtAsync(wal.LastIndex).ConfigureAwait(false);
        Expect(term == 3, $"TermAtAsync returned {term}");
    }

    await factory.DeleteAsync(group).ConfigureAwait(false);
}

static void Expect(bool condition, string message)
{
    if (!condition)
    {
        // The probe's whole contract with CI is its exit code, so this has to be a hard failure.
        throw new InvalidOperationException("AOT probe failed: " + message);
    }
}
