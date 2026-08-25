// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Collections.Concurrent;
using System.Threading.Channels;
using Blun.MultiRaft.Core;
using Blun.MultiRaft.Grpc.Protocol;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Blun.MultiRaft.Grpc;

/// <summary>
/// One multiplexed bidirectional stream between two nodes, carrying every Raft group that spans them.
/// </summary>
/// <remarks>
/// Symmetric on purpose. Once the stream is up there is no client and no server: both ends issue requests
/// and both answer them, and a correlation id pairs a reply with its request. That is what lets a single
/// connection serve thousands of groups — the alternative, a stream per group, gives each one its own
/// flow-control window and HPACK state and makes cross-group batching impossible.
/// <para>
/// All writes go through one channel and one writer loop. A gRPC stream permits exactly one writer at a
/// time, and funnelling through a channel gets that for free while leaving room to coalesce frames later.
/// </para>
/// </remarks>
internal sealed class RaftStreamSession : IAsyncDisposable
{
    private readonly IAsyncStreamWriter<RaftFrame> _writer;
    private readonly IAsyncStreamReader<RaftFrame> _reader;
    private readonly IRaftProtocolListener _listener;
    private readonly ILogger _logger;
    private readonly Channel<RaftFrame> _outbound;
    private readonly CancellationTokenSource _shutdown;

    private readonly ConcurrentDictionary<ulong, TaskCompletionSource<RaftFrame>> _pending = new();
    private readonly ConcurrentDictionary<ulong, InboundSnapshot> _inboundSnapshots = new();
    private readonly int _maxConcurrentInboundSnapshots;
    private readonly long _maxInboundSnapshotBytes;
    private readonly SemaphoreSlim _requestFanoutGate;

    private long _correlation;

    /// <summary>Default cap on simultaneously-open inbound snapshot transfers per session (SEC-003).</summary>
    public const int DefaultMaxConcurrentInboundSnapshots = 4;

    /// <summary>Default per-snapshot byte budget (SEC-003): a peer sending past this is rejected, not buffered.</summary>
    public const long DefaultMaxInboundSnapshotBytes = 512L * 1024 * 1024;

    /// <summary>
    /// Default cap on <see cref="HandleRequestAsync"/> calls in flight at once per session (SEC-004): the
    /// reader loop starts one uncounted task per inbound request frame so an fsync-bound answer never blocks
    /// the loop every group on this connection shares, which without a bound lets frames-per-second alone
    /// grow memory and thread-pool usage without limit.
    /// </summary>
    public const int DefaultMaxConcurrentRequests = 1024;

    /// <summary>
    /// Default capacity of the outbound frame channel (SEC-004). Generous — the goal is a backstop against an
    /// unbounded queue, not a latency target — but finite: without one, a writer loop slower than its
    /// producers grows the channel until the process runs out of memory.
    /// </summary>
    public const int DefaultOutboundCapacity = 65536;

    public RaftStreamSession(
        IAsyncStreamWriter<RaftFrame> writer,
        IAsyncStreamReader<RaftFrame> reader,
        IRaftProtocolListener listener,
        CancellationToken cancellationToken,
        ILogger? logger = null,
        NodeId? peerId = null,
        int maxConcurrentInboundSnapshots = DefaultMaxConcurrentInboundSnapshots,
        long maxInboundSnapshotBytes = DefaultMaxInboundSnapshotBytes,
        int maxConcurrentRequests = DefaultMaxConcurrentRequests,
        int outboundCapacity = DefaultOutboundCapacity)
    {
        _writer = writer;
        _reader = reader;
        _listener = listener;
        _logger = logger ?? NullLogger.Instance;
        PeerId = peerId;
        _maxConcurrentInboundSnapshots = maxConcurrentInboundSnapshots;
        _maxInboundSnapshotBytes = maxInboundSnapshotBytes;
        _requestFanoutGate = new SemaphoreSlim(maxConcurrentRequests, maxConcurrentRequests);
        _shutdown = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // Bounded with Wait, not Unbounded: a writer loop that falls behind its producers (replies,
        // outgoing calls) now applies backpressure to whoever is calling SendAsync instead of growing
        // without limit (SEC-004). SendAsync's callers are CallAsync (a public API call, already
        // cancellable) and HandleRequestAsync (already gated by _requestFanoutGate below) -- neither is
        // the reader loop itself, so waiting here cannot stall frame delivery to other groups.
        _outbound = Channel.CreateBounded<RaftFrame>(new BoundedChannelOptions(outboundCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait,
        });

        WriterLoop = Task.Run(PumpOutboundAsync, CancellationToken.None);
        ReaderLoop = Task.Run(PumpInboundAsync, CancellationToken.None);
    }

    /// <summary>Completes when the session stops writing — used by the server to hold the call open.</summary>
    public Task WriterLoop { get; }

    /// <summary>Completes when the peer closed the stream or the session was cancelled.</summary>
    public Task ReaderLoop { get; }

    /// <summary>
    /// The identity this connection authenticated as, when an <see cref="IRaftPeerAuthenticator"/> is
    /// configured on the receiving end; <c>null</c> otherwise (including on the client side, which asserts its
    /// own identity via the outbound header rather than validating one). When set, every inbound request frame
    /// carrying a claimed sender is checked against it in <see cref="HandleRequestAsync"/>.
    /// </summary>
    public NodeId? PeerId { get; }

    public async ValueTask<AppendEntriesResponse> AppendEntriesAsync(
        AppendEntriesRequest request,
        IAsyncEnumerable<RaftLogEntry> entries,
        CancellationToken cancellationToken)
    {
        AppendEntries message = RaftFrameCodec.ToProto(in request);

        // Entries go in the one frame rather than as a stream of their own. A replication round is already
        // bounded by MaxEntriesPerAppend and by the message cap, so one frame is one write.
        await foreach (RaftLogEntry entry in entries.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            message.Entries.Add(RaftFrameCodec.ToProto(in entry));
        }

        RaftFrame reply = await CallAsync(
            request.Group,
            frame => frame.AppendEntries = message,
            cancellationToken).ConfigureAwait(false);

        return reply.PayloadCase == RaftFrame.PayloadOneofCase.AppendEntriesReply
            ? RaftFrameCodec.ToDomain(reply.AppendEntriesReply)
            : new AppendEntriesResponse(0, Success: false, 0, 0);
    }

    public async ValueTask<VoteResponse> RequestVoteAsync(VoteRequest request, CancellationToken cancellationToken)
    {
        RaftFrame reply = await CallAsync(
            request.Group,
            frame => frame.Vote = RaftFrameCodec.ToProto(in request),
            cancellationToken).ConfigureAwait(false);

        return reply.PayloadCase == RaftFrame.PayloadOneofCase.VoteReply
            ? RaftFrameCodec.ToDomain(reply.VoteReply)
            : new VoteResponse(0, Granted: false);
    }

    public async ValueTask<ReadIndexResponse> ReadIndexAsync(ReadIndexRequest request, CancellationToken cancellationToken)
    {
        RaftFrame reply = await CallAsync(
            request.Group,
            frame => frame.ReadIndex = RaftFrameCodec.ToProto(in request),
            cancellationToken).ConfigureAwait(false);

        return reply.PayloadCase == RaftFrame.PayloadOneofCase.ReadIndexReply
            ? RaftFrameCodec.ToDomain(reply.ReadIndexReply)
            : new ReadIndexResponse(0, Success: false, 0, 0);
    }

    public async ValueTask<TimeoutNowResponse> TimeoutNowAsync(
        TimeoutNowRequest request,
        CancellationToken cancellationToken)
    {
        RaftFrame reply = await CallAsync(
            request.Group,
            frame => frame.TimeoutNow = RaftFrameCodec.ToProto(in request),
            cancellationToken).ConfigureAwait(false);

        return reply.PayloadCase == RaftFrame.PayloadOneofCase.TimeoutNowReply
            ? RaftFrameCodec.ToDomain(reply.TimeoutNowReply)
            : new TimeoutNowResponse(0, Accepted: false);
    }

    public async ValueTask ReportLoadAsync(NodeLoadReport report, CancellationToken cancellationToken)
    {
        // Carried on the cluster group's id: the report is about the node, but every frame needs a group and
        // this is the group the cluster leader is reachable through.
        await CallAsync(
            RaftGroupId.Cluster,
            frame => frame.NodeLoad = RaftFrameCodec.ToProto(in report),
            cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<LeaderTargetResponse> LeaderTargetAsync(
        LeaderTargetRequest request,
        CancellationToken cancellationToken)
    {
        RaftFrame reply = await CallAsync(
            request.Group,
            frame => frame.LeaderTarget = RaftFrameCodec.ToProto(in request),
            cancellationToken).ConfigureAwait(false);

        return reply.PayloadCase == RaftFrame.PayloadOneofCase.LeaderTargetReply
            ? RaftFrameCodec.ToDomain(reply.LeaderTargetReply)
            : new LeaderTargetResponse(default, LeaderTargetStatus.Unreachable, 0, Transferred: false);
    }

    public async ValueTask<InstallSnapshotResponse> InstallSnapshotAsync(
        InstallSnapshotRequest request,
        IAsyncEnumerable<ReadOnlyMemory<byte>> body,
        CancellationToken cancellationToken)
    {
        ulong correlation = NextCorrelation();
        var completion = new TaskCompletionSource<RaftFrame>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[correlation] = completion;

        try
        {
            await SendAsync(
                new RaftFrame
                {
                    CorrelationId = correlation,
                    GroupId = RaftGroupWireId.ToWire(request.Group),
                    InstallSnapshot = RaftFrameCodec.ToProto(in request),
                },
                cancellationToken).ConfigureAwait(false);

            // A snapshot body is the one payload with no size bound, so unlike entries it genuinely streams:
            // chunk frames sharing the correlation id, terminated by an empty one flagged last.
            await foreach (ReadOnlyMemory<byte> chunk in body.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                await SendAsync(
                    new RaftFrame
                    {
                        CorrelationId = correlation,
                        GroupId = RaftGroupWireId.ToWire(request.Group),

                        // Copied, not wrapped, for the reason set out on RaftFrameCodec: SendAsync only queues
                        // the frame, and FileRaftSnapshotStore.ReadAsync refills one chunk array per iteration,
                        // so wrapping raced the producer. Every chunk but the last would have been serialised
                        // from a buffer already overwritten by the next read — a snapshot larger than one chunk
                        // would install silently corrupt state on the receiver.
                        SnapshotChunk = new SnapshotChunk
                        {
                            Data = Google.Protobuf.ByteString.CopyFrom(chunk.Span),
                            Last = false,
                        },
                    },
                    cancellationToken).ConfigureAwait(false);
            }

            await SendAsync(
                new RaftFrame
                {
                    CorrelationId = correlation,
                    GroupId = RaftGroupWireId.ToWire(request.Group),
                    SnapshotChunk = new SnapshotChunk { Last = true },
                },
                cancellationToken).ConfigureAwait(false);

            RaftFrame reply = await completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return reply.PayloadCase == RaftFrame.PayloadOneofCase.InstallSnapshotReply
                ? RaftFrameCodec.ToDomain(reply.InstallSnapshotReply)
                : new InstallSnapshotResponse(0, Success: false);
        }
        finally
        {
            _pending.TryRemove(correlation, out _);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _shutdown.CancelAsync().ConfigureAwait(false);
        _outbound.Writer.TryComplete();

        foreach (TaskCompletionSource<RaftFrame> waiter in _pending.Values)
        {
            waiter.TrySetCanceled();
        }

        _pending.Clear();

        foreach (InboundSnapshot tracked in _inboundSnapshots.Values)
        {
            tracked.Channel.Writer.TryComplete();
        }

        _inboundSnapshots.Clear();

        // Both pumps read _shutdown.Token on every iteration, and HandleRequestAsync hands it to the
        // listener. Disposing the token source while either loop is still running turns its next token
        // access into an ObjectDisposedException that neither loop's catch list expects.
        try
        {
            await Task.WhenAll(ReaderLoop, WriterLoop).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is OperationCanceledException or RpcException or IOException or InvalidOperationException)
        {
            // Both loops end this way; that is the ordinary shutdown path.
        }

        _shutdown.Dispose();
    }

    private ulong NextCorrelation() => (ulong)Interlocked.Increment(ref _correlation);

    private async ValueTask<RaftFrame> CallAsync(
        RaftGroupId group,
        Action<RaftFrame> fill,
        CancellationToken cancellationToken)
    {
        ulong correlation = NextCorrelation();
        var completion = new TaskCompletionSource<RaftFrame>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[correlation] = completion;

        try
        {
            var frame = new RaftFrame { CorrelationId = correlation, GroupId = RaftGroupWireId.ToWire(group) };
            fill(frame);
            await SendAsync(frame, cancellationToken).ConfigureAwait(false);
            return await completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _pending.TryRemove(correlation, out _);
        }
    }

    private async ValueTask SendAsync(RaftFrame frame, CancellationToken cancellationToken)
        => await _outbound.Writer.WriteAsync(frame, cancellationToken).ConfigureAwait(false);

    private async Task PumpOutboundAsync()
    {
        try
        {
            await foreach (RaftFrame frame in _outbound.Reader.ReadAllAsync(_shutdown.Token).ConfigureAwait(false))
            {
                await _writer.WriteAsync(frame).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Session closing.
        }
        catch (Exception ex) when (ex is RpcException or InvalidOperationException or IOException)
        {
            // The peer went away. Every pending call has to fail rather than hang: a Raft group treats an
            // unreachable peer as a normal condition, but only if it is told.
            FailPending(ex);
        }
        finally
        {
            // Symmetric with the reader loop's completion below: without this, a writer that dies leaves
            // the outbound channel open and unbounded while nothing is reading from it, and IsAlive (which
            // only watches the reader) keeps calling the session healthy.
            _outbound.Writer.TryComplete();
        }
    }

    private async Task PumpInboundAsync()
    {
        try
        {
            while (await _reader.MoveNext(_shutdown.Token).ConfigureAwait(false))
            {
                RaftFrame frame = _reader.Current;
                switch (frame.PayloadCase)
                {
                    case RaftFrame.PayloadOneofCase.AppendEntriesReply:
                    case RaftFrame.PayloadOneofCase.VoteReply:
                    case RaftFrame.PayloadOneofCase.InstallSnapshotReply:
                    case RaftFrame.PayloadOneofCase.ReadIndexReply:
                    case RaftFrame.PayloadOneofCase.TimeoutNowReply:
                    case RaftFrame.PayloadOneofCase.NodeLoadReply:
                    case RaftFrame.PayloadOneofCase.LeaderTargetReply:
                        if (_pending.TryRemove(frame.CorrelationId, out TaskCompletionSource<RaftFrame>? waiter))
                        {
                            waiter.TrySetResult(frame);
                        }

                        break;

                    case RaftFrame.PayloadOneofCase.SnapshotChunk:
                        HandleSnapshotChunk(frame);
                        break;

                    default:
                        // Handled off the read loop: a request may take an fsync to answer, and blocking the
                        // reader would stall every other group sharing this stream behind it. The gate below
                        // is the SEC-004 backstop: waiting for a free slot *does* block the reader loop, but
                        // only once DefaultMaxConcurrentRequests answers are already outstanding -- the point
                        // where an unbounded fan-out would otherwise start growing the thread pool and memory
                        // without limit. HTTP/2 flow control then carries that backpressure to the sender.
                        await _requestFanoutGate.WaitAsync(_shutdown.Token).ConfigureAwait(false);
                        _ = HandleRequestAsync(frame);
                        break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Session closing.
        }
        catch (Exception ex) when (ex is RpcException or InvalidOperationException or IOException)
        {
            FailPending(ex);
        }
        finally
        {
            _outbound.Writer.TryComplete();
        }
    }

    /// <summary>
    /// True if <paramref name="claimed"/> may be dispatched: either this session is unauthenticated
    /// (<see cref="PeerId"/> is <c>null</c>, the pre-SEC-001 default), or it matches the authenticated
    /// identity. A frame claiming to be a different node than the one that opened this connection is dropped
    /// rather than trusted — accepting it would let an authenticated-but-malicious peer act as anyone.
    /// </summary>
    private bool IsClaimedSenderTrusted(ulong group, string payload, ulong claimed)
    {
        if (PeerId is not { } peerId || peerId.Value == claimed)
        {
            return true;
        }

        GrpcLog.PeerIdentityMismatch(_logger, group, payload, claimed, peerId.Value);
        return false;
    }

    private void HandleSnapshotChunk(RaftFrame frame)
    {
        if (!_inboundSnapshots.TryGetValue(frame.CorrelationId, out InboundSnapshot? tracked))
        {
            return;
        }

        if (frame.SnapshotChunk.Last)
        {
            tracked.Channel.Writer.TryComplete();
            return;
        }

        long total = Interlocked.Add(ref tracked.BytesWritten, frame.SnapshotChunk.Data.Length);
        if (total > _maxInboundSnapshotBytes)
        {
            // Budget exceeded (SEC-003): stop retaining chunks for this transfer and fault the channel so the
            // awaiting OnInstallSnapshotAsync call observes a definite failure instead of hanging or growing
            // without bound. Removed here rather than left for the handler's finally, so a chunk arriving
            // after this one finds no tracked entry and is dropped for free.
            tracked.BudgetExceeded = true;
            tracked.Channel.Writer.TryComplete(
                new InvalidOperationException($"Inbound snapshot exceeded the {_maxInboundSnapshotBytes}-byte budget."));
            _inboundSnapshots.TryRemove(frame.CorrelationId, out _);
            GrpcLog.SnapshotBudgetExceeded(_logger, RaftGroupWireId.FromWire(frame.GroupId).Value, _maxInboundSnapshotBytes);
            return;
        }

        tracked.Channel.Writer.TryWrite(frame.SnapshotChunk.Data.ToByteArray());
    }

    /// <summary>Tracks one inbound snapshot transfer's channel and running byte total (SEC-003).</summary>
    private sealed class InboundSnapshot(Channel<ReadOnlyMemory<byte>> channel)
    {
        public Channel<ReadOnlyMemory<byte>> Channel { get; } = channel;
        public long BytesWritten;
        public bool BudgetExceeded;
    }

    /// <summary>
    /// Releases the SEC-004 fan-out gate the reader loop acquired before dispatching this frame, then runs
    /// the actual handler. Split out so every exit path of <see cref="HandleRequestCoreAsync"/> — the early
    /// <c>return</c>s on a failed identity check included — releases the slot exactly once.
    /// </summary>
    private async Task HandleRequestAsync(RaftFrame frame)
    {
        try
        {
            await HandleRequestCoreAsync(frame).ConfigureAwait(false);
        }
        finally
        {
            _requestFanoutGate.Release();
        }
    }

    private async Task HandleRequestCoreAsync(RaftFrame frame)
    {
        var group = RaftGroupWireId.FromWire(frame.GroupId);
        try
        {
            RaftFrame response = new() { CorrelationId = frame.CorrelationId, GroupId = frame.GroupId };

            switch (frame.PayloadCase)
            {
                case RaftFrame.PayloadOneofCase.AppendEntries:
                {
                    if (!IsClaimedSenderTrusted(group.Value, "AppendEntries", frame.AppendEntries.Leader))
                    {
                        return;
                    }

                    AppendEntriesRequest request = RaftFrameCodec.ToDomain(group, frame.AppendEntries);
                    AppendEntriesResponse result = await _listener
                        .OnAppendEntriesAsync(request, Decode(frame.AppendEntries), _shutdown.Token)
                        .ConfigureAwait(false);
                    response.AppendEntriesReply = RaftFrameCodec.ToProto(in result);
                    break;
                }

                case RaftFrame.PayloadOneofCase.Vote:
                {
                    if (!IsClaimedSenderTrusted(group.Value, "Vote", frame.Vote.Candidate))
                    {
                        return;
                    }

                    VoteRequest request = RaftFrameCodec.ToDomain(group, frame.Vote);
                    VoteResponse result = await _listener
                        .OnRequestVoteAsync(request, _shutdown.Token)
                        .ConfigureAwait(false);
                    response.VoteReply = RaftFrameCodec.ToProto(in result);
                    break;
                }

                case RaftFrame.PayloadOneofCase.TimeoutNow:
                {
                    TimeoutNowRequest request = RaftFrameCodec.ToDomain(group, frame.TimeoutNow);
                    TimeoutNowResponse result = await _listener
                        .OnTimeoutNowAsync(request, _shutdown.Token)
                        .ConfigureAwait(false);
                    response.TimeoutNowReply = RaftFrameCodec.ToProto(in result);
                    break;
                }

                case RaftFrame.PayloadOneofCase.ReadIndex:
                {
                    ReadIndexRequest request = RaftFrameCodec.ToDomain(group, frame.ReadIndex);
                    ReadIndexResponse result = await _listener
                        .OnReadIndexAsync(request, _shutdown.Token)
                        .ConfigureAwait(false);
                    response.ReadIndexReply = RaftFrameCodec.ToProto(in result);
                    break;
                }

                case RaftFrame.PayloadOneofCase.NodeLoad:
                {
                    if (!IsClaimedSenderTrusted(group.Value, "NodeLoad", frame.NodeLoad.Node))
                    {
                        return;
                    }

                    // A transport whose listener does not do cluster management still answers, with an empty
                    // reply. The sender treats a lost report as normal anyway, and leaving the call to time
                    // out would be a worse way of saying the same thing.
                    if (_listener is IRaftClusterListener sink)
                    {
                        await sink
                            .OnLoadReportAsync(RaftFrameCodec.ToDomain(frame.NodeLoad), _shutdown.Token)
                            .ConfigureAwait(false);
                    }

                    response.NodeLoadReply = new NodeLoadReply();
                    break;
                }

                case RaftFrame.PayloadOneofCase.LeaderTarget:
                {
                    LeaderTargetResponse result = _listener is IRaftClusterListener target
                        ? await target
                            .OnLeaderTargetAsync(
                                RaftFrameCodec.ToDomain(group, frame.LeaderTarget),
                                _shutdown.Token)
                            .ConfigureAwait(false)
                        : new LeaderTargetResponse(default, LeaderTargetStatus.Unreachable, 0, Transferred: false);

                    response.LeaderTargetReply = RaftFrameCodec.ToProto(in result);
                    break;
                }

                case RaftFrame.PayloadOneofCase.InstallSnapshot:
                {
                    if (!IsClaimedSenderTrusted(group.Value, "InstallSnapshot", frame.InstallSnapshot.Leader))
                    {
                        return;
                    }

                    // A cap on simultaneous transfers, not on transfer size (SEC-003): an unbounded number of
                    // concurrent InstallSnapshot frames is itself a memory-DoS surface even before any chunk
                    // arrives, since each one holds a channel and an in-flight handler.
                    if (_inboundSnapshots.Count >= _maxConcurrentInboundSnapshots)
                    {
                        response.InstallSnapshotReply = new InstallSnapshotReply { Success = false };
                        break;
                    }

                    // The channel is registered before the handler runs, so chunks arriving while the handler
                    // is still starting up are buffered rather than dropped. Left unbounded deliberately: a
                    // bounded channel's blocking write would stall this connection's single reader loop, which
                    // every other group sharing it depends on (see SEC-003). The byte budget tracked in
                    // HandleSnapshotChunk is what actually bounds memory instead.
                    var body = Channel.CreateUnbounded<ReadOnlyMemory<byte>>(new UnboundedChannelOptions
                    {
                        SingleReader = true,
                        SingleWriter = true,
                    });
                    var tracked = new InboundSnapshot(body);
                    _inboundSnapshots[frame.CorrelationId] = tracked;

                    try
                    {
                        InstallSnapshotRequest request = RaftFrameCodec.ToDomain(group, frame.InstallSnapshot);
                        InstallSnapshotResponse result = await _listener
                            .OnInstallSnapshotAsync(request, body.Reader.ReadAllAsync(_shutdown.Token), _shutdown.Token)
                            .ConfigureAwait(false);
                        response.InstallSnapshotReply = RaftFrameCodec.ToProto(in result);
                    }
                    catch (InvalidOperationException) when (tracked.BudgetExceeded)
                    {
                        // HandleSnapshotChunk already logged and dropped the transfer; answer instead of
                        // leaving the sender to time out, since the rejection is definite, not transient.
                        response.InstallSnapshotReply = new InstallSnapshotReply { Success = false };
                    }
                    finally
                    {
                        _inboundSnapshots.TryRemove(frame.CorrelationId, out _);
                    }

                    break;
                }

                default:
                    return;
            }

            await SendAsync(response, _shutdown.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Session closing; the peer will time the call out.
        }
        catch (Exception ex) when (ex is RpcException or InvalidOperationException or IOException)
        {
            // Still not rethrown: one group's failed request must not tear down the stream that every other
            // group on this node pair is sharing. But it is no longer silent. An InvalidOperationException
            // here is this library's signal for an integrity violation — SegmentedRaftWal.EnsureDense raises
            // exactly that for a non-contiguous append — and a follower failing this way simply answers
            // nothing, so the leader sees a timeout and retries forever. Swallowing that without a word made
            // a whole class of follower-side failure invisible from both ends of the wire.
            GrpcLog.RequestFailed(_logger, ex, RaftGroupWireId.FromWire(frame.GroupId).Value, frame.PayloadCase.ToString());
        }
    }

    private static async IAsyncEnumerable<RaftLogEntry> Decode(AppendEntries message)
    {
        foreach (LogEntry entry in message.Entries)
        {
            yield return RaftFrameCodec.ToDomain(entry);
        }

        await ValueTask.CompletedTask.ConfigureAwait(false);
    }

    private void FailPending(Exception error)
    {
        foreach (KeyValuePair<ulong, TaskCompletionSource<RaftFrame>> pair in _pending)
        {
            if (_pending.TryRemove(pair.Key, out TaskCompletionSource<RaftFrame>? waiter))
            {
                waiter.TrySetException(new IOException("The Raft session failed.", error));
            }
        }
    }
}
