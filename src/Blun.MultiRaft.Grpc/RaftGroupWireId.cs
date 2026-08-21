// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Grpc;

/// <summary>
/// Maps <see cref="RaftGroupId"/> to the value carried on <c>RaftFrame.group_id</c>.
/// </summary>
/// <remarks>
/// <see cref="RaftGroupId.Cluster"/> is <c>ulong.MaxValue</c> internally, deliberately: it has to sit outside
/// any real group id so an uninitialized <see cref="RaftGroupId"/> can never address the administrative
/// plane. On the wire that value is a varint with every bit set -- 10 bytes instead of 1 -- and it rides
/// every frame the cluster group sends, forever, since that group never stops heartbeating. Remapping it to
/// 0 here costs nothing (0 cannot collide with a real group id, since group ids <see cref="RaftGroupId.ReservedFloor"/>
/// and above are already refused elsewhere) and is purely a transport concern: <see cref="RaftGroupId"/>
/// itself, its sentinel value, and every rule built on it stay exactly as they are.
/// </remarks>
internal static class RaftGroupWireId
{
    public static ulong ToWire(RaftGroupId group)
        => group == RaftGroupId.Cluster ? 0UL : group.Value + 1;

    public static RaftGroupId FromWire(ulong wire)
        => wire == 0 ? RaftGroupId.Cluster : new RaftGroupId(wire - 1);
}
