// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using Microsoft.Extensions.Logging;

namespace Blun.MultiRaft.Grpc;

internal static partial class GrpcLog
{
    [LoggerMessage(
        EventId = 1300,
        Level = LogLevel.Error,
        Message = "Raft group {Group}: an inbound {Payload} could not be answered; the peer will see a timeout.")]
    public static partial void RequestFailed(ILogger logger, Exception exception, ulong group, string payload);

    [LoggerMessage(
        EventId = 1301,
        Level = LogLevel.Warning,
        Message = "Inbound Raft session rejected: the x-raft-auth header was missing or failed validation.")]
    public static partial void PeerAuthenticationFailed(ILogger logger);

    [LoggerMessage(
        EventId = 1302,
        Level = LogLevel.Warning,
        Message = "Raft group {Group}: a {Payload} claimed sender {Claimed} but the session is authenticated as "
            + "{Authenticated}; the frame was dropped rather than dispatched.")]
    public static partial void PeerIdentityMismatch(
        ILogger logger,
        ulong group,
        string payload,
        ulong claimed,
        ulong authenticated);
}
