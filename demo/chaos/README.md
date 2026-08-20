# Chaos harness for the demo cluster

Two scripts, no dependencies beyond Node.js. They drive the running five-node demo and read what it says
about being driven. Start the demo first; both talk to the observer on 8300 and the app-host control plane
on 8200.

```bash
# n rounds of node loss and recovery, then a report
node demo/chaos/chaos.mjs demo/chaos/runs/run1 8

# what the nodes logged during that run, grouped by level and message
node demo/chaos/scan.mjs demo/chaos/runs/run1

# every button in the observer UI, in a real browser (needs the two installs first)
cd demo/chaos && npm install && npx playwright install chromium
node demo/chaos/ui.mjs
```

`ui.mjs` drives Chromium through the whole surface — theme, log tabs, filters, following, leadership transfer
by dropdown and by clicking a cell, membership, sending traffic into a group's log and watching every
replica's copy grow to the same size, stop, start, restart, and killing the cluster leader — and asserts what
the page shows afterwards. It writes screenshots to `shots/` and fails on any console error or failed request. Two of its
assertions deliberately reach outside the browser: a restart is confirmed against the app host's own resource
state, because the card reads `Running` before and after and cannot tell a restart from a click that did
nothing.

`chaos.mjs` writes each node's console output to `<out>/node-N.log` and a transcript to `<out>/run.log`, and
exits non-zero if any step failed to settle.

## What a round does

Rounds cycle through seven shapes: a hard kill and restart, the same to the *cluster leader*, two nodes out
and back, three out so the quorum is lost, a leadership transfer followed by killing the node that just
received it, a node taken out of the cluster group's configuration and re-adopted, and all five killed at
once and brought back from disk.

Hard kills go through the OS: the script finds the PID listening on the node's status port and terminates it,
so the node gets no shutdown path and its peers find out by not hearing from it. Graceful stops go through
the control plane, which is what the UI's buttons do.

## What it checks

After every step, that the set of answering nodes is the one the step implies, and — whenever a quorum
remains — that a leader exists for the cluster group and for every queue group, that the answering nodes
agree on who it is, and that the node they name is one that is actually answering. That last clause matters:
nodes go on naming the leader they last heard from until their election timeout expires, so "there is a
leader" is satisfied by a dead one and proves nothing.

Below quorum, none of that is required. The only assertion is that the observer still answers.

## Reading the log scan

`replication to node N failed`, carrying an `IOException`, is the designed behaviour when a peer is gone —
the replication loop treats `IOException` as retryable, which is why it is a warning and not an error. Expect
a lot of them in a run that kills processes.

What should never appear: anything at `fail:` or `crit:`, an unhandled exception, or an
`InvalidOperationException` — in this codebase that one means an integrity violation, not a transport
problem. `scan.mjs` reports those separately as suspicious lines.
