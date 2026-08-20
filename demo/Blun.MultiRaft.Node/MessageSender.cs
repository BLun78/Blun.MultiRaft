// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Collections.Concurrent;
using System.Buffers.Binary;
using System.Diagnostics;
using Blun.MultiRaft.Hosting;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Node;

/// <summary>What one run of the message generator has managed so far.</summary>
internal sealed class SendJob(int total, int size, TimeSpan interval)
{
    private long _sent;
    private long _failed;

    public int Total { get; } = total;

    public int Size { get; } = size;

    public TimeSpan Interval { get; } = interval;

    public long StartedTimestamp { get; } = Stopwatch.GetTimestamp();

    public long Sent => Interlocked.Read(ref _sent);

    public long Failed => Interlocked.Read(ref _failed);

    public bool Running { get; set; } = true;

    public string? Error { get; set; }

    public CancellationTokenSource Cancellation { get; } = new();

    public TimeSpan Elapsed { get; private set; }

    public void Progress() => Interlocked.Increment(ref _sent);

    public void Fail() => Interlocked.Increment(ref _failed);

    public void Finish()
    {
        Elapsed = Stopwatch.GetElapsedTime(StartedTimestamp);
        Running = false;
    }

    /// <summary>
    /// Entries per second, measured rather than assumed. A ten-millisecond interval is a request, not a
    /// promise: the platform's timer resolution decides what actually happens, and on Windows that has
    /// historically been coarser than ten milliseconds.
    /// </summary>
    public double RatePerSecond
    {
        get
        {
            TimeSpan elapsed = Running ? Stopwatch.GetElapsedTime(StartedTimestamp) : Elapsed;
            return elapsed.TotalSeconds <= 0 ? 0 : Sent / elapsed.TotalSeconds;
        }
    }
}

/// <summary>
/// Writes dummy traffic into a group, so the write-ahead log has something in it to look at.
/// </summary>
/// <remarks>
/// The appends go through <see cref="RaftGroupInstance.AppendAsync"/> like any other command, which means
/// they replicate, commit and land in every follower's log — the point is to watch a real log grow, not to
/// fabricate a number. One run per group at a time; a second request while one is going is refused rather
/// than interleaved, because two generators on one group would make the measured rate meaningless.
/// </remarks>
internal sealed class MessageSender(MultiRaftHost host, ILogger logger) : IAsyncDisposable
{
    private readonly ConcurrentDictionary<ulong, SendJob> _jobs = new();

    public SendJob? Job(ulong group) => _jobs.TryGetValue(group, out SendJob? job) ? job : null;

    public object Start(ulong group, int count, TimeSpan interval, int size)
    {
        if (!host.TryGetGroup(new RaftGroupId(group), out RaftGroupInstance? instance) || instance is null)
        {
            return new { group, started = false, error = "this node does not carry that group" };
        }

        if (!instance.IsLeader)
        {
            return new
            {
                group,
                started = false,
                error = "not the leader of this group",
                leader = instance.LeaderId?.Value,
            };
        }

        if (_jobs.TryGetValue(group, out SendJob? running) && running.Running)
        {
            return new { group, started = false, error = "a run is already in progress", sent = running.Sent, total = running.Total };
        }

        var job = new SendJob(count, size, interval);
        _jobs[group] = job;

        SendLog.Started(logger, group, count, (long)interval.TotalMilliseconds, size);
        _ = Task.Run(() => RunAsync(instance, job), CancellationToken.None);

        return new { group, started = true, total = count, intervalMs = interval.TotalMilliseconds, size };
    }

    public void Stop(ulong group)
    {
        if (_jobs.TryGetValue(group, out SendJob? job) && job.Running)
        {
            job.Cancellation.Cancel();
        }
    }

    private async Task RunAsync(RaftGroupInstance instance, SendJob job)
    {
        byte[] payload = new byte[job.Size];

        // Something recognisable rather than zeroes: a reader looking at a segment on disk should be able to
        // tell demo traffic from anything else, and a compressible run of zeroes would flatter the numbers.
        for (int i = 16; i < payload.Length; i++)
        {
            payload[i] = (byte)('a' + (i % 26));
        }

        using var timer = new PeriodicTimer(job.Interval);

        try
        {
            for (int i = 0; i < job.Total; i++)
            {
                if (job.Cancellation.IsCancellationRequested)
                {
                    break;
                }

                BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(0, 8), i);
                BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(8, 8), DateTimeOffset.UtcNow.Ticks);

                try
                {
                    await instance.AppendAsync(payload, applicationTag: 1, job.Cancellation.Token)
                        .ConfigureAwait(false);

                    job.Progress();
                }
                catch (NotLeaderException)
                {
                    // Leadership moved out from under the run. Reported rather than retried elsewhere: where
                    // the writes went is the interesting part of the answer.
                    job.Fail();
                    job.Error = "leadership moved away from this node";
                    break;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
#pragma warning disable CA1031 // A generator that dies silently is worse than one that reports anything.
                catch (Exception ex)
                {
                    job.Fail();
                    job.Error = ex.Message;
                    break;
                }
#pragma warning restore CA1031

                await timer.WaitForNextTickAsync(job.Cancellation.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Cancelled, either by request or by shutdown.
        }
        finally
        {
            job.Finish();
            SendLog.Finished(logger, instance.Group.Value, job.Sent, job.Failed, (long)job.Elapsed.TotalMilliseconds);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (SendJob job in _jobs.Values)
        {
            await job.Cancellation.CancelAsync().ConfigureAwait(false);
            job.Cancellation.Dispose();
        }

        _jobs.Clear();
    }
}

internal static partial class SendLog
{
    [LoggerMessage(
        EventId = 2010,
        Level = LogLevel.Information,
        Message = "Sending {Count} messages of {Size} bytes into group {Group}, one every {IntervalMs} ms.")]
    public static partial void Started(ILogger logger, ulong group, int count, long intervalMs, int size);

    [LoggerMessage(
        EventId = 2011,
        Level = LogLevel.Information,
        Message = "Group {Group}: {Sent} messages sent, {Failed} failed, in {ElapsedMs} ms.")]
    public static partial void Finished(ILogger logger, ulong group, long sent, long failed, long elapsedMs);
}
