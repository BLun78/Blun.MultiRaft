// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Transport;

/// <summary>
/// An in-process cluster. Every node registers its listener here and messages are handed over directly,
/// so consensus can be exercised deterministically without a socket in the way.
/// </summary>
/// <remarks>
/// It also models the two failures that actually break Raft implementations — an unreachable node and a
/// one-way partition — because those are exactly the cases a socket-based test cannot reproduce on demand.
/// </remarks>
public sealed class InMemoryRaftCluster
{
    private readonly ConcurrentDictionary<NodeId, IRaftProtocolListener> _nodes = new();
    private readonly ConcurrentDictionary<(NodeId From, NodeId To), bool> _cuts = new();

    /// <summary>Registers a node's listener and hands back the transport it should use.</summary>
    public IRaftProtocolTransport Connect(NodeId node, IRaftProtocolListener listener)
    {
        _nodes[node] = listener;
        return new InMemoryRaftTransport(this, node);
    }

    /// <summary>Removes a node: every message to it now fails as if the host were down.</summary>
    public void Disconnect(NodeId node) => _nodes.TryRemove(node, out _);

    /// <summary>Cuts the link in one direction. One-way partitions are where election bugs hide.</summary>
    public void Cut(NodeId from, NodeId to) => _cuts[(from, to)] = true;

    /// <summary>Restores a link cut by <see cref="Cut"/>.</summary>
    public void Heal(NodeId from, NodeId to) => _cuts.TryRemove((from, to), out _);

    /// <summary>Restores every cut link.</summary>
    public void HealAll() => _cuts.Clear();

    internal IRaftProtocolListener Resolve(NodeId from, NodeId to)
    {
        if (_cuts.ContainsKey((from, to)) || !_nodes.TryGetValue(to, out IRaftProtocolListener? listener))
        {
            throw new IOException("Node " + to + " is unreachable from " + from + ".");
        }

        return listener;
    }

    private sealed class InMemoryRaftTransport(InMemoryRaftCluster cluster, NodeId self) : IRaftClusterTransport
    {
        public async ValueTask ReportLoadAsync(
            NodeId target,
            NodeLoadReport report,
            CancellationToken cancellationToken = default)
        {
            IRaftProtocolListener listener = cluster.Resolve(self, target);
            if (listener is IRaftClusterListener clusterListener)
            {
                await clusterListener.OnLoadReportAsync(report, cancellationToken).ConfigureAwait(false);
            }
        }

        public async ValueTask<LeaderTargetResponse> LeaderTargetAsync(
            NodeId target,
            LeaderTargetRequest request,
            CancellationToken cancellationToken = default)
        {
            IRaftProtocolListener listener = cluster.Resolve(self, target);
            return listener is IRaftClusterListener clusterListener
                ? await clusterListener.OnLeaderTargetAsync(request, cancellationToken).ConfigureAwait(false)
                : new LeaderTargetResponse(target, LeaderTargetStatus.Unreachable, 0, Transferred: false);
        }


        public async ValueTask<AppendEntriesResponse> AppendEntriesAsync(
            NodeId target,
            AppendEntriesRequest request,
            IAsyncEnumerable<RaftLogEntry> entries,
            CancellationToken cancellationToken = default)
        {
            IRaftProtocolListener listener = cluster.Resolve(self, target);

            // Entries are copied at the boundary. In-process there is no wire to force it, but a real
            // transport serializes here, and a test that shares the sender's buffers would quietly hide
            // every lifetime bug this library's zero-copy contract can produce.
            return await listener
                .OnAppendEntriesAsync(request, Detach(entries, cancellationToken), cancellationToken)
                .ConfigureAwait(false);
        }

        public async ValueTask<InstallSnapshotResponse> InstallSnapshotAsync(
            NodeId target,
            InstallSnapshotRequest request,
            IAsyncEnumerable<ReadOnlyMemory<byte>> body,
            CancellationToken cancellationToken = default)
        {
            IRaftProtocolListener listener = cluster.Resolve(self, target);
            return await listener
                .OnInstallSnapshotAsync(request, DetachChunks(body, cancellationToken), cancellationToken)
                .ConfigureAwait(false);
        }

        public async ValueTask<ReadIndexResponse> ReadIndexAsync(
            NodeId target,
            ReadIndexRequest request,
            CancellationToken cancellationToken = default)
        {
            IRaftProtocolListener listener = cluster.Resolve(self, target);
            return await listener.OnReadIndexAsync(request, cancellationToken).ConfigureAwait(false);
        }

        public async ValueTask<TimeoutNowResponse> TimeoutNowAsync(
            NodeId target,
            TimeoutNowRequest request,
            CancellationToken cancellationToken = default)
        {
            IRaftProtocolListener listener = cluster.Resolve(self, target);
            return await listener.OnTimeoutNowAsync(request, cancellationToken).ConfigureAwait(false);
        }

        public async ValueTask<VoteResponse> RequestVoteAsync(
            NodeId target,
            VoteRequest request,
            CancellationToken cancellationToken = default)
        {
            IRaftProtocolListener listener = cluster.Resolve(self, target);
            return await listener.OnRequestVoteAsync(request, cancellationToken).ConfigureAwait(false);
        }

        private static async IAsyncEnumerable<ReadOnlyMemory<byte>> DetachChunks(
            IAsyncEnumerable<ReadOnlyMemory<byte>> source,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (ReadOnlyMemory<byte> chunk in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                yield return chunk.ToArray();
            }
        }

        private static async IAsyncEnumerable<RaftLogEntry> Detach(
            IAsyncEnumerable<RaftLogEntry> source,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (RaftLogEntry entry in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                yield return entry.ToOwned();
            }
        }
    }
}
