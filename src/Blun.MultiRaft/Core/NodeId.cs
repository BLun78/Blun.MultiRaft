// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Globalization;

namespace Blun.MultiRaft.Core;

/// <summary>Identity of a physical node in the cluster. Value type: never boxed on the message path.</summary>
public readonly record struct NodeId(ulong Value) : IComparable<NodeId>
{
    public int CompareTo(NodeId other) => Value.CompareTo(other.Value);

    public override string ToString() => Value.ToString(CultureInfo.InvariantCulture);
}

