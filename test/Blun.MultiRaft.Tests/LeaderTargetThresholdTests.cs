// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Text;
using Blun.MultiRaft.Transport;
using Blun.MultiRaft.Wal;

namespace Blun.MultiRaft.Tests;

/// <summary>
/// What "fully in sync and healthy" means for a node proposed as a leadership target. Two independent
/// claims, deliberately answered by two independent checks: how far behind the log is, and whether the node
/// is still talking. A match index alone supports only the first, and on a quiet queue a node that died
/// moments ago is indistinguishable from one that is caught up.
/// </summary>
public sealed class LeaderTargetThresholdTests
{
    private static readonly RaftGroupId Group = new(700);

    [Fact]
    public async Task ASilentTargetIsRefusedEvenThoughItsLogLooksCurrent()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(Group, [1, 2, 3]);
        RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Group);

        NodeId silent = FirstFollower(cluster, leader);

        // Nothing is appended after this point, so the silent node's match index stays exactly where the
        // healthy ones are. Lag cannot tell the two apart -- that is the whole reason the liveness check
        // exists rather than being inferred from the numbers.
        cluster.Network.Cut(leader.Self, silent);
        cluster.Network.Cut(silent, leader.Self);

        LeaderTargetResponse response = await WaitForStatusAsync(
            leader,
            silent,
            LeaderTargetStatus.NotResponding,
            "the silent node to be judged as not responding");

        Assert.Equal(silent, response.Node);
        Assert.Equal(0, response.Lag);
        Assert.False(response.Transferred);
        Assert.True(leader.IsLeader, "refusing a target must never cost the group its leader");
    }

    [Fact]
    public async Task WhenTheLeaderPicksItPrefersANodeThatIsStillAnswering()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);
        await cluster.AddGroupEverywhereAsync(Group, [1, 2, 3]);
        RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Group);

        NodeId silent = FirstFollower(cluster, leader);
        cluster.Network.Cut(leader.Self, silent);
        cluster.Network.Cut(silent, leader.Self);

        // Both followers have the same match index here, so a picker that only ranked by match index could
        // legitimately return either -- and would return the dead one roughly half the time, or every time if
        // it happened to be the one that died furthest ahead.
        await WaitForStatusAsync(
            leader,
            silent,
            LeaderTargetStatus.NotResponding,
            "the silent node to be judged as not responding");

        LeaderTargetResponse chosen = await leader.EvaluateLeaderTargetAsync(
            new LeaderTargetRequest(Group, Candidate: null, Execute: false));

        Assert.Equal(LeaderTargetStatus.Valid, chosen.Status);
        Assert.NotEqual(silent, chosen.Node);
        Assert.NotEqual(leader.Self, chosen.Node);
    }

    [Fact]
    public async Task ALagVerdictIsGovernedByItsOwnThresholdAndNotByThePromotionOne()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);

        // PromotionCatchUpThreshold is 8 in FastOptions and LeaderTargetLagThreshold is left at its default,
        // which derives from MaxEntriesPerAppend (256). The backlog below sits deliberately between the two:
        // far past what would block a promotion, comfortably inside one replication round.
        await cluster.AddGroupEverywhereAsync(Group, [1, 2, 3]);
        RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Group);

        NodeId behind = FirstFollower(cluster, leader);
        cluster.Network.Cut(leader.Self, behind);
        cluster.Network.Cut(behind, leader.Self);

        await AppendAsync(leader, count: 40);

        // Not Lagging. Reusing the promotion threshold here -- which is what this used to do -- would report
        // a node 40 entries behind as too far behind to lead, on a group whose leader ships 256 entries in a
        // single round. The refusal it does get is about the silence, which is a different and true statement.
        LeaderTargetResponse response = await WaitForStatusAsync(
            leader,
            behind,
            LeaderTargetStatus.NotResponding,
            "a backlog inside one replication round to be judged on liveness rather than on lag");

        Assert.True(response.Lag >= 40, "the reported lag must be the real distance, not a threshold");
    }

    [Fact]
    public async Task ATargetBeyondTheConfiguredToleranceIsRefusedAsLagging()
    {
        await using TestCluster cluster = await new TestCluster().WithNodesAsync(1, 2, 3);

        var options = new RaftGroupOptions
        {
            ElectionTimeout = TimeSpan.FromMilliseconds(120),
            ElectionJitter = 1.0,
            HeartbeatInterval = TimeSpan.FromMilliseconds(30),
            LeadershipTransferCatchUpTimeout = TimeSpan.FromSeconds(10),

            // Deliberately inverted against the test above: there the promotion threshold was the strict one
            // and the target tolerance was loose, here it is the other way round. Both tests use the same
            // 40-entry backlog, so between them exactly one option can explain both verdicts, and it is not
            // PromotionCatchUpThreshold -- which is set wide enough here that it could not produce this
            // refusal on its own.
            PromotionCatchUpThreshold = 1000,
            LeaderTargetLagThreshold = 4,
        };

        await cluster.AddGroupEverywhereAsync(Group, [1, 2, 3], options: options);
        RaftGroupInstance leader = await cluster.WaitForLeaderAsync(Group);

        NodeId behind = FirstFollower(cluster, leader);
        cluster.Network.Cut(leader.Self, behind);
        cluster.Network.Cut(behind, leader.Self);

        await AppendAsync(leader, count: 40);

        // Lag is checked before liveness, so a node that is both silent and genuinely too far behind is
        // reported on the log distance -- the condition a caller can do something about.
        LeaderTargetResponse response = await WaitForStatusAsync(
            leader,
            behind,
            LeaderTargetStatus.Lagging,
            "a backlog past the configured tolerance to be refused as lagging");

        Assert.True(response.Lag > 4);
    }

    [Fact]
    public void TheDefaultsAreDerivedRatherThanPicked()
    {
        // Both null by default, and both meaningful only through what they derive from. Asserted here because
        // the derivation is the decision -- a constant would go stale the moment someone changed the batch
        // size or the election timeout, which is exactly what it is meant to track.
        var options = new RaftGroupOptions();

        Assert.Null(options.LeaderTargetLagThreshold);
        Assert.Null(options.LeaderTargetContactWindow);
        Assert.Equal(256, options.MaxEntriesPerAppend);
    }

    private static NodeId FirstFollower(TestCluster cluster, RaftGroupInstance leader)
        => cluster.GroupsOf(Group).First(g => g.Self != leader.Self).Self;

    private static async ValueTask AppendAsync(RaftGroupInstance leader, int count)
    {
        for (int i = 0; i < count; i++)
        {
            // LeaderBuffered would be the honest choice for a group that cannot reach a majority, but this
            // group can: only one of three followers is cut off, so each of these still commits.
            await leader.AppendAsync(Encoding.UTF8.GetBytes("entry-" + i));
        }
    }

    /// <summary>
    /// Polls rather than sleeping past the contact window. The window is derived from the election timeout,
    /// so a fixed wait here would silently stop testing anything the day that timeout changes.
    /// </summary>
    private static async ValueTask<LeaderTargetResponse> WaitForStatusAsync(
        RaftGroupInstance leader,
        NodeId candidate,
        LeaderTargetStatus expected,
        string what)
    {
        long deadline = System.Diagnostics.Stopwatch.GetTimestamp()
                        + (10 * System.Diagnostics.Stopwatch.Frequency);
        LeaderTargetResponse response = default;

        while (System.Diagnostics.Stopwatch.GetTimestamp() < deadline)
        {
            response = await leader.EvaluateLeaderTargetAsync(
                new LeaderTargetRequest(Group, candidate, Execute: false));

            if (response.Status == expected)
            {
                return response;
            }

            await Task.Delay(10);
        }

        Assert.Fail("Timed out waiting for " + what + "; last status was " + response.Status + ".");
        return response;
    }
}
