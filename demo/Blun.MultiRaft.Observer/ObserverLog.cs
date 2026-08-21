// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

namespace Blun.MultiRaft.Observer;

internal static partial class ObserverLog
{
    [LoggerMessage(
        EventId = 2100,
        Level = LogLevel.Information,
        Message = "Observer watching {NodeCount} nodes every {IntervalMs} ms.")]
    public static partial void WatcherStarted(ILogger logger, int nodeCount, long intervalMs);

    [LoggerMessage(
        EventId = 2101,
        Level = LogLevel.Warning,
        Message = "A polling round failed; the observer keeps watching.")]
    public static partial void PollFailed(ILogger logger, Exception exception);

    [LoggerMessage(
        EventId = 2102,
        Level = LogLevel.Debug,
        Message = "The log stream for node {Node} dropped ({Reason}); reconnecting.")]
    public static partial void LogStreamDropped(ILogger logger, ulong node, string reason);

    [LoggerMessage(
        EventId = 2103,
        Level = LogLevel.Warning,
        Message = "The control plane did not answer a resource request ({Reason}).")]
    public static partial void ControlPlaneUnreachable(ILogger logger, string reason);
}
