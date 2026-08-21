// Chaos harness for the five-node demo.
//
// Drives the cluster through node loss and recovery -- graceful stops, hard kills of the OS process,
// leader kills, and the quorum edge -- and after every step waits for the cluster to settle and checks
// that it did. Every node's console output is collected the whole time and scanned at the end.

import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const OBSERVER = 'http://127.0.0.1:8300';
const CONTROL = 'http://127.0.0.1:8200';
const NODES = [1, 2, 3, 4, 5];
const STATUS_PORT = (id) => 8100 + id;
const RESOURCE = (id) => `raft-node-${id}`;
const OUT = process.argv[2] ?? 'chaos-run';
const ROUNDS = Number(process.argv[3] ?? 8);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const snapshot = async () => (await fetch(`${OBSERVER}/api/cluster`)).json();

fs.mkdirSync(OUT, { recursive: true });
const report = fs.createWriteStream(path.join(OUT, 'run.log'), { flags: 'a' });
const say = (line) => {
  console.log(line);
  report.write(line + '\n');
};

// ---------------------------------------------------------------------------------------------------
// Log collection
// ---------------------------------------------------------------------------------------------------

const lines = new Map(NODES.map((id) => [id, []]));
const seen = new Map(NODES.map((id) => [id, new Set()]));
let collecting = true;

async function collect(id) {
  while (collecting) {
    try {
      const response = await fetch(`${CONTROL}/api/resources/${RESOURCE(id)}/logs`);
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (collecting) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        let index;

        while ((index = buffer.indexOf('\n\n')) >= 0) {
          const frame = buffer.slice(0, index);
          buffer = buffer.slice(index + 2);

          if (!frame.startsWith('data: ')) continue;

          const line = JSON.parse(frame.slice(6));
          const key = `${line.lineNumber}|${line.content}`;

          if (!seen.get(id).has(key)) {
            seen.get(id).add(key);
            lines.get(id).push(line);
          }
        }
      }
    } catch {
      // The stream ends when the resource stops. Reconnect and keep watching.
    }

    await sleep(500);
  }
}

NODES.forEach((id) => void collect(id));

// ---------------------------------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------------------------------

async function command(id, verb) {
  const response = await fetch(`${CONTROL}/api/resources/${RESOURCE(id)}/${verb}`, { method: 'POST' });
  return `${verb} ${RESOURCE(id)} -> ${response.status}`;
}

/** The PID listening on a node's status port -- the only way to tell five identical `dotnet run` apart. */
function pidOf(id) {
  const netstat = spawnSync('netstat', ['-ano', '-p', 'tcp'], { encoding: 'utf8' });
  const wanted = `:${STATUS_PORT(id)}`;

  for (const line of netstat.stdout.split('\n')) {
    const parts = line.trim().split(/\s+/);

    // The state column is localised ("ABHOEREN" on a German Windows), so the listening socket is
    // recognised by its remote address being the wildcard instead.
    if (
      parts.length >= 5 &&
      parts[0] === 'TCP' &&
      parts[1].endsWith(wanted) &&
      /^(0\.0\.0\.0:0|\[::\]:0)$/.test(parts[2])
    ) {
      return Number(parts[parts.length - 1]);
    }
  }

  return null;
}

/** A hard kill: no shutdown path, no goodbye to peers. What a crashed machine looks like from outside. */
function kill(id) {
  const pid = pidOf(id);
  if (pid === null) return `kill ${RESOURCE(id)} -> no listener found`;

  const result = spawnSync('taskkill', ['/F', '/PID', String(pid)], { encoding: 'utf8' });
  return `KILL -9 ${RESOURCE(id)} (pid ${pid}) -> ${result.status === 0 ? 'killed' : result.stderr.trim()}`;
}

// ---------------------------------------------------------------------------------------------------
// Invariants
// ---------------------------------------------------------------------------------------------------

/**
 * Waits for the cluster to reach the state the step implies, then reports what it found.
 * With a quorum there must be a cluster leader, every group must have one, and the answering nodes must
 * agree. Without a quorum, none of that is required -- only that the observer still answers.
 */
async function settle(expectedOnline, timeoutMs = 45000) {
  const started = Date.now();
  let last = null;

  while (Date.now() - started < timeoutMs) {
    const s = await snapshot();
    last = s;

    const onlineIds = s.nodes.filter((n) => n.online).map((n) => n.id);
    const rightSet =
      onlineIds.length === expectedOnline.length && expectedOnline.every((id) => onlineIds.includes(id));

    if (rightSet) {
      if (!s.hasQuorum) {
        return { ok: true, waited: Date.now() - started, snapshot: s, note: 'no quorum expected' };
      }

      // A leader that is not answering is not a settled leader. Nodes keep naming the one they last heard
      // from until their election timeout runs out, which is correct of them and useless as an assertion:
      // the cluster has recovered only once the leadership sits on a node that is actually there.
      const live = (id) => id !== null && onlineIds.includes(Number(id));
      const groupsLed = s.groups.every((g) => live(g.leader) && !g.disagreement);

      if (live(s.clusterLeader) && !s.leaderDisagreement && groupsLed) {
        return { ok: true, waited: Date.now() - started, snapshot: s };
      }
    }

    await sleep(500);
  }

  const onlineIds = last.nodes.filter((n) => n.online).map((n) => n.id);
  const missing = last.groups
    .filter((g) => g.leader === null || g.disagreement || !onlineIds.includes(Number(g.leader)))
    .map((g) => `${g.label}(leader=${g.leader ?? '-'}${g.disagreement ? ',disputed' : ''})`);

  return {
    ok: false,
    waited: Date.now() - started,
    snapshot: last,
    note:
      `expected online [${expectedOnline}], saw [${onlineIds}]; ` +
      `clusterLeader=${last.clusterLeader} disputed=${last.leaderDisagreement} ` +
      `groups without a settled leader: ${missing.length ? missing.join(', ') : 'none'}`,
  };
}

/** Polls until the snapshot satisfies a condition, or gives up. */
async function waitFor(condition, timeoutMs = 30000) {
  const started = Date.now();

  while (Date.now() - started < timeoutMs) {
    if (condition(await snapshot())) {
      return { ok: true, waited: Date.now() - started };
    }

    await sleep(500);
  }

  return { ok: false, waited: Date.now() - started };
}

function describe(s) {
  return (
    `online ${s.online}/${s.total} quorum=${s.hasQuorum} leader=${s.clusterLeader ?? '-'} | ` +
    s.groups.map((g) => `${g.label.replace('Group ', 'g')}=${g.leader ?? '-'}`).join(' ')
  );
}

let failures = 0;

async function step(title, action, expectedOnline, timeoutMs) {
  say(`\n--- ${title}`);
  if (action) say('    ' + (await action()));

  const result = await settle(expectedOnline, timeoutMs);
  say(`    ${result.ok ? 'OK' : 'FAILED'} after ${result.waited} ms: ${describe(result.snapshot)}`);

  if (result.note) say(`    note: ${result.note}`);
  if (!result.ok) failures++;

  return result;
}

// ---------------------------------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------------------------------

const all = [...NODES];
const without = (...ids) => all.filter((id) => !ids.includes(id));
const pick = (from) => from[Math.floor(Math.random() * from.length)];

say(`\n================ chaos run, ${ROUNDS} rounds, ${new Date().toISOString()} ================`);

// Whatever the last run left behind is not this run's subject. Bring everything up first -- but only what
// is actually down: starting a resource that is already running takes half a minute to report success.
say('\n--- repair: start whatever is down');
const states = await (await fetch(`${CONTROL}/api/resources`)).json();

for (const state of states.filter((s) => !s.running)) {
  say('    ' + (await command(Number(state.node), 'start')));
}

await step('baseline', null, all, 90000);

for (let round = 1; round <= ROUNDS; round++) {
  say(`\n######## round ${round} ########`);

  const before = await snapshot();
  const leader = Number(before.clusterLeader);
  const victim = pick(all);
  const other = pick(without(victim, leader));

  switch (round % 7) {
    case 1: {
      // A node crashes and comes back.
      await step(`hard kill node ${victim}`, () => kill(victim), without(victim));
      await step(`start node ${victim} again`, () => command(victim, 'start'), all);
      break;
    }

    case 2: {
      // The cluster leader crashes -- the case that has to re-elect.
      await step(`hard kill the cluster leader (node ${leader})`, () => kill(leader), without(leader));
      await step(`start node ${leader} again`, () => command(leader, 'start'), all);
      break;
    }

    case 3: {
      // Two out, one back: the quorum edge from above.
      await step(`stop node ${victim}`, () => command(victim, 'stop'), without(victim));
      await step(`hard kill node ${other}`, () => kill(other), without(victim, other));
      await step(`start node ${victim}`, () => command(victim, 'start'), without(other));
      await step(`start node ${other}`, () => command(other, 'start'), all);
      break;
    }

    case 4: {
      // Below quorum and back. Nothing may be claimed while it is down.
      const third = pick(without(victim, other));
      await step(`stop node ${victim}`, () => command(victim, 'stop'), without(victim));
      await step(`stop node ${other}`, () => command(other, 'stop'), without(victim, other));
      await step(
        `kill node ${third} -- quorum lost`,
        () => kill(third),
        without(victim, other, third),
      );
      await step(`start node ${victim}`, () => command(victim, 'start'), without(other, third));
      await step(`start node ${other}`, () => command(other, 'start'), without(third));
      await step(`start node ${third}`, () => command(third, 'start'), all, 60000);
      break;
    }

    case 5: {
      // Leadership moved on purpose, then the new holder is killed.
      const group = pick(['1', '2', '3']);
      const target = pick(without(leader));
      say(`\n--- move group ${group} to node ${target}`);
      const response = await fetch(`${OBSERVER}/api/groups/${group}/leader?node=${target}`, { method: 'POST' });
      say('    ' + (await response.text()));
      await step(`kill node ${target} right after the handover`, () => kill(target), without(target));
      await step(`start node ${target}`, () => command(target, 'start'), all);
      break;
    }

    case 6: {
      // Everything dies at once and comes back from disk. Nothing seeds a cluster that already exists, so
      // every node has to rebuild its configuration from its own log and stand for election off the back of
      // it. This is the case that found the learner-on-restart defect.
      await step(
        'kill all five at once',
        () => NODES.map((id) => kill(id)).join('\n    '),
        [],
        30000,
      );

      await step(
        'start all five again',
        async () => (await Promise.all(NODES.map((id) => command(id, 'start')))).join('\n    '),
        all,
        90000,
      );
      break;
    }

    case 0: {
      // A node out of the cluster group's configuration and back in -- the single-server change path, which
      // no amount of killing processes would otherwise exercise.
      const target = pick(without(leader));
      say(`\n--- take node ${target} out of the cluster group`);

      const out = await fetch(`${OBSERVER}/api/cluster/nodes/${target}`, { method: 'DELETE' });
      const outBody = await out.text();
      say(`    -> ${out.status} ${outBody}`);

      if (JSON.parse(outBody).applied) {
        // The coordinator adopts any configured node it finds missing, and promotes one per pass, so the
        // node has to come back on its own. If it does not, the reconcile loop has stopped working.
        const back = await waitFor(
          (s) => s.nodes.find((n) => n.id === target)?.voters?.includes(target) === true,
          60000,
        );

        say(`    ${back.ok ? 'OK' : 'FAILED'}: node ${target} was ${back.ok ? 're-adopted and promoted' : 'never taken back'} after ${back.waited} ms`);
        if (!back.ok) failures++;
      }

      await step(`settle after the membership change`, null, all);
      break;
    }

    default: {
      // Restart, the ordinary case.
      await step(`restart node ${victim}`, () => command(victim, 'restart'), all, 60000);
      break;
    }
  }
}

await step('final settle', null, all, 60000);

// ---------------------------------------------------------------------------------------------------
// What the nodes said about it
// ---------------------------------------------------------------------------------------------------

await sleep(3000);
collecting = false;

const PATTERNS = [
  ['unhandled', /unhandled exception/i],
  ['fail-level', /^\s*fail:/im],
  ['crit-level', /^\s*crit:/im],
  ['warn-level', /^\s*warn:/im],
  ['exception', /\b[A-Za-z.]*Exception\b/],
  ['stack frame', /^\s+at [A-Za-z]/m],
];

const buckets = new Map();

for (const id of NODES) {
  fs.writeFileSync(
    path.join(OUT, `node-${id}.log`),
    lines.get(id).map((l) => l.content).join('\n'),
  );

  for (const line of lines.get(id)) {
    for (const [name, pattern] of PATTERNS) {
      if (pattern.test(line.content)) {
        const key = `${name} :: ${line.content.replace(/^\S+Z\s*/, '').slice(0, 140)}`;
        buckets.set(key, (buckets.get(key) ?? 0) + 1);
        break;
      }
    }
  }
}

say(`\n================ log scan ================`);
say(`collected ${NODES.map((id) => `node ${id}: ${lines.get(id).length}`).join(', ')} lines`);

if (buckets.size === 0) {
  say('no matching lines');
} else {
  for (const [key, count] of [...buckets].sort((a, b) => b[1] - a[1])) {
    say(`${String(count).padStart(4)}  ${key}`);
  }
}

say(`\n================ verdict: ${failures} failed step(s) ================`);
report.end();
process.exit(failures === 0 ? 0 : 1);
