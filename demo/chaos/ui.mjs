// Drives the observer UI in a real browser: every button, every tab, and the two things the UI is for --
// showing a leadership change and letting one be asked for.

import { chromium } from 'playwright';
import fs from 'node:fs';

const URL = 'http://127.0.0.1:4200';
const SHOTS = 'shots';
fs.mkdirSync(SHOTS, { recursive: true });

const problems = [];
const results = [];

const check = (name, ok, detail = '') => {
  results.push({ name, ok });
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? '  — ' + detail : ''}`);
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitUntil(fn, timeoutMs, label) {
  const started = Date.now();

  while (Date.now() - started < timeoutMs) {
    try {
      if (await fn()) return { ok: true, waited: Date.now() - started };
    } catch {
      // The DOM moved under us; try again.
    }

    await sleep(300);
  }

  return { ok: false, waited: Date.now() - started, label };
}

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1600, height: 1400 } });

page.on('console', (m) => {
  if (m.type() === 'error') problems.push('console.error: ' + m.text());
});
page.on('pageerror', (e) => problems.push('pageerror: ' + e.message));
page.on('requestfailed', (r) => {
  // The SSE streams are aborted when the page closes; that is not a failure worth reporting.
  if (!r.url().includes('/logs') && !r.url().includes('/stream')) {
    problems.push(`requestfailed: ${r.url()} ${r.failure()?.errorText}`);
  }
});

const text = (id) => page.getByTestId(id).innerText();
const testId = (id) => page.getByTestId(id);

console.log('=== load ===');
await page.goto(URL, { waitUntil: 'domcontentloaded' });

const loaded = await waitUntil(async () => /^\d+$/.test((await text('cluster-leader')).trim()), 30000, 'leader');
check('the page loads and names a cluster leader', loaded.ok, `after ${loaded.waited} ms`);

const nodes = [1, 2, 3, 4, 5];
let allCards = true;

for (const id of nodes) {
  if ((await testId(`node-${id}`).count()) !== 1) allCards = false;
}

check('five node cards are rendered', allCards);

const quorum = await text('quorum');
check('the quorum box reads 5 / 5 and holds', /5 \/ 5 answering/.test(quorum) && /holds/.test(quorum), quorum.replace(/\n/g, ' '));

const rows = await page.locator('tr[data-testid^="group-"]').count();
check('the matrix has a row per group plus the cluster group', rows === 4, `${rows} rows`);

const clusterRow = await testId('group-18446744073709551615-leader').count();
check('the cluster group row addresses the full ulong id', clusterRow === 1);

await page.screenshot({ path: `${SHOTS}/01-loaded.png`, fullPage: true });

console.log('\n=== theme toggle ===');
const before = await page.evaluate(() => document.documentElement.classList.contains('dark'));
await testId('theme-toggle').click();
await sleep(400);
const after = await page.evaluate(() => document.documentElement.classList.contains('dark'));
check('the theme toggle flips the theme', before !== after);
await page.screenshot({ path: `${SHOTS}/02-theme.png` });
await testId('theme-toggle').click();
await sleep(400);

console.log('\n=== logs ===');
const linesArrived = await waitUntil(async () => {
  const count = Number((await text('log-count')).replace(/\D/g, ''));
  return count > 0;
}, 30000, 'log lines');

check('log lines arrive over SSE', linesArrived.ok, (await text('log-count')));

let tabsWork = true;

for (const id of nodes) {
  await testId(`log-tab-${id}`).click();
  await sleep(700);
  const count = Number((await text('log-count')).replace(/\D/g, ''));
  if (count === 0) {
    tabsWork = false;
    problems.push(`log tab for node ${id} showed no lines`);
  }
}

check('every per-node log tab shows that node\'s lines', tabsWork);

await testId('log-tab-all').click();
await sleep(700);
const merged = Number((await text('log-count')).replace(/\D/g, ''));

await testId('log-filter').fill('Raft group');
await sleep(700);
const filtered = Number((await text('log-count')).replace(/\D/g, ''));
check('the filter narrows the log', filtered > 0 && filtered < merged, `${merged} → ${filtered}`);

await testId('log-filter').fill('zzz-nothing-matches-this');
await sleep(700);
const empty = Number((await text('log-count')).replace(/\D/g, ''));
check('a filter that matches nothing shows nothing', empty === 0);

await testId('log-filter').fill('');
await sleep(500);

await testId('log-stderr').check();
await sleep(700);
const stderrOnly = Number((await text('log-count')).replace(/\D/g, ''));
check('the stderr filter is applied', stderrOnly <= merged, `${stderrOnly} stderr lines`);
await testId('log-stderr').uncheck();
await sleep(500);

// Scrolling up must stop the follow and offer to resume.
await testId('log-scroller').evaluate((element) => element.scrollTo({ top: 0 }));
await sleep(600);
const paused = await testId('log-follow').count();
check('scrolling up pauses the follow', paused === 1);

if (paused === 1) {
  await testId('log-follow').click();
  await sleep(600);
  check('follow again resumes it', (await testId('log-follow').count()) === 0);
}

await page.screenshot({ path: `${SHOTS}/03-logs.png`, fullPage: true });

console.log('\n=== write-ahead log ===');
const walOnCard = (await text('node-1-wal')).trim();
check('the node card reports its log size on disk', /^[\d.]+ (B|KB|MB|GB)$/.test(walOnCard) && walOnCard !== '0 B', walOnCard);

let walCells = true;

for (const id of nodes) {
  if ((await testId(`wal-3-node-${id}`).count()) !== 1) walCells = false;
}

check('the WAL panel shows every node\'s copy of group 3', walCells);

const sizeOf = async (group, node) => (await text(`wal-${group}-node-${node}`)).trim();
const beforeSend = await sizeOf('3', 1);

check('the 1k and 10k buttons are offered too',
  (await testId('wal-3-send-1000').isEnabled()) && (await testId('wal-3-send-10000').isEnabled()));

await testId('wal-3-send-100').click();

const progressed = await waitUntil(async () => {
  if ((await testId('wal-3-progress').count()) !== 1) return false;
  return /\/ 100/.test(await text('wal-3-progress'));
}, 25000, 'send progress');

check('sending 100 messages reports progress', progressed.ok,
  progressed.ok ? (await text('wal-3-progress')).replace(/\s+/g, ' ').trim() : '');

check('the send buttons are disabled while a run is going',
  await testId('wal-3-send-100').isDisabled());

const grew = await waitUntil(async () => (await sizeOf('3', 1)) !== beforeSend, 60000, 'wal grew');
check('every node\'s copy of the log grows with it', grew.ok, `node 1: ${beforeSend} → ${await sizeOf('3', 1)}`);

const finished = await waitUntil(async () => /100 \/ 100/.test(await text('wal-3-progress')), 60000, 'run finished');
check('the run reaches its total', finished.ok, (await text('wal-3-progress')).replace(/\s+/g, ' ').trim());

// Replication means every copy is the same size. A divergence here would be a real one.
const sizes = [];
for (const id of nodes) sizes.push(await sizeOf('3', id));
check('all five copies are the same size', new Set(sizes).size === 1, sizes.join(' '));

await page.screenshot({ path: `${SHOTS}/09-wal.png`, fullPage: true });

console.log('\n=== leadership transfer, the thing the UI is for ===');
const leaderOf = async (group) => (await text(`group-${group}-leader`)).trim();

const groupOne = await leaderOf('1');
const target = nodes.find((id) => String(id) !== groupOne);

await testId('group-1-select').selectOption(String(target));
await testId('group-1-request').click();

const answered = await waitUntil(async () => (await testId('group-1-result').count()) === 1, 20000, 'result');
check('the request reports the server\'s answer next to the button', answered.ok,
  answered.ok ? await text('group-1-result') : '');

const moved = await waitUntil(async () => (await leaderOf('1')) === String(target), 25000, 'leader moved');
check(`the matrix shows group 1 moving from node ${groupOne} to node ${target}`, moved.ok,
  `after ${moved.waited} ms, now ${await leaderOf('1')}`);

const flashed = await testId('group-1').innerText();
check('the row is marked as just moved', /just moved/.test(flashed));

await page.screenshot({ path: `${SHOTS}/04-transfer.png`, fullPage: true });

// The row that used to answer 404 because its id does not fit in a long.
await testId('group-18446744073709551615-request').click();
const clusterAnswer = await waitUntil(
  async () => (await testId('group-18446744073709551615-result').count()) === 1,
  20000,
  'cluster group answer',
);

const clusterText = clusterAnswer.ok ? await text('group-18446744073709551615-result') : '';
check('the cluster group accepts a leadership request', clusterAnswer.ok && !/404/.test(clusterText), clusterText);

console.log('\n=== moving leadership by clicking a cell ===');
const groupTwo = await leaderOf('2');
const cellTarget = nodes.find((id) => String(id) !== groupTwo);

check(
  `the cell of node ${groupTwo}, which already leads group 2, is not clickable`,
  await testId(`group-2-cell-${groupTwo}`).isDisabled(),
);

await testId(`group-2-cell-${cellTarget}`).click();

const cellMoved = await waitUntil(async () => (await leaderOf('2')) === String(cellTarget), 25000, 'cell move');
check(
  `clicking node ${cellTarget}'s cell moves group 2 there`,
  cellMoved.ok,
  `${groupTwo} → ${await leaderOf('2')} after ${cellMoved.waited} ms`,
);

check('the click reports the server\'s answer', (await testId('group-2-result').count()) === 1,
  (await testId('group-2-result').count()) === 1 ? await text('group-2-result') : '');

await page.screenshot({ path: `${SHOTS}/08-cell-move.png`, fullPage: true });

console.log('\n=== membership: out of the cluster and back ===');
const clusterLeaderNow = (await text('cluster-leader')).trim();
const spare = nodes.find((id) => String(id) !== clusterLeaderNow);

await testId(`node-${spare}-out`).click();
const removed = await waitUntil(async () => /applied/.test(await text('last-result')), 20000, 'removal');
check(`node ${spare} can be taken out of the cluster group`, removed.ok, removed.ok ? await text('last-result') : '');

await testId(`node-${spare}-in`).click();
await sleep(2000);
check(`node ${spare} can be put back in`, /applied/.test(await text('last-result')), await text('last-result'));

console.log('\n=== process control ===');
const leaderForVictim = (await text('cluster-leader')).trim();
const victim = nodes.find((id) => String(id) !== leaderForVictim);

check('start is disabled while the node runs', await testId(`node-${victim}-start`).isDisabled());
check('stop is enabled while the node runs', await testId(`node-${victim}-stop`).isEnabled());

// Two clicks: the first arms it, the second means it.
await testId(`node-${victim}-stop`).click();
await sleep(300);
const armed = await testId(`node-${victim}-stop`).innerText();
check('the first click asks for confirmation', /confirm/i.test(armed), armed.trim());

await testId(`node-${victim}-stop`).click();

const wentOffline = await waitUntil(
  async () => /offline/.test(await text(`node-${victim}`)),
  60000,
  'node offline',
);

check(`node ${victim} goes offline in the UI after being stopped`, wentOffline.ok, `after ${wentOffline.waited} ms`);
await page.screenshot({ path: `${SHOTS}/05-stopped.png`, fullPage: true });

const quorumAfter = await text('quorum');
check('the quorum box counts the loss', /4 \/ 5 answering/.test(quorumAfter), quorumAfter.replace(/\n/g, ' '));

check(
  `the matrix cells of the stopped node ${victim} are not clickable`,
  await testId(`group-1-cell-${victim}`).isDisabled(),
);

const startEnabled = await waitUntil(async () => await testId(`node-${victim}-start`).isEnabled(), 20000, 'start enabled');
check('start becomes available once the node is down', startEnabled.ok);

await testId(`node-${victim}-start`).click();
await sleep(300);
await testId(`node-${victim}-start`).click();

const cameBack = await waitUntil(
  async () => !/offline/.test(await text(`node-${victim}`)),
  90000,
  'node back online',
);

check(`node ${victim} comes back online in the UI`, cameBack.ok, `after ${cameBack.waited} ms`);

// Restart, on a node that is running. The card reads Running before and after, so it cannot tell a restart
// from a click that did nothing; the app host is asked directly instead, which is a channel the button does
// not go through and cannot fake.
const stateOf = async (id) => {
  const states = await (await fetch('http://127.0.0.1:8200/api/resources')).json();
  return states.find((s) => Number(s.node) === id)?.state;
};

const restartWatch = (async () => {
  const left = await waitUntil(async () => (await stateOf(victim)) !== 'Running', 60000, 'left Running');
  const back = await waitUntil(async () => (await stateOf(victim)) === 'Running', 90000, 'back to Running');
  return left.ok && back.ok;
})();

await testId(`node-${victim}-restart`).click();
await sleep(300);
await testId(`node-${victim}-restart`).click();

check(`the restart button really restarts node ${victim}`, await restartWatch,
  'the app host saw it leave Running and come back');

const settled = await waitUntil(async () => /5 \/ 5 answering/.test(await text('quorum')), 90000, 'all five back');
check('the cluster returns to five of five', settled.ok);

console.log('\n=== killing the cluster leader, watched from the browser ===');
const leaderBefore = (await text('cluster-leader')).trim();

await testId(`node-${leaderBefore}-stop`).click();
await sleep(300);
await testId(`node-${leaderBefore}-stop`).click();

// The nodes go on naming the leader they last heard from until their election timeout runs out. Whether the
// browser catches that window is a matter of timing, so it is reported rather than asserted.
let sawStale = false;

const reElected = await waitUntil(async () => {
  const header = await page.getByTestId('cluster-leader').locator('..').innerText();
  if (/not answering/.test(header)) sawStale = true;

  const now = (await text('cluster-leader')).trim();
  return now !== leaderBefore && /^\d+$/.test(now) && !/not answering/.test(header);
}, 60000, 're-election');

check(
  `the UI shows a new cluster leader after node ${leaderBefore} was stopped`,
  reElected.ok,
  `${leaderBefore} → ${(await text('cluster-leader')).trim()} after ${reElected.waited} ms`,
);

console.log(`  (the "not answering" marker was ${sawStale ? 'seen' : 'not caught'} during the window)`);
await page.screenshot({ path: `${SHOTS}/07-reelected.png`, fullPage: true });

const offlineCard = /offline/.test(await text(`node-${leaderBefore}`));
check(`the stopped node ${leaderBefore} shows as offline`, offlineCard);

await testId(`node-${leaderBefore}-start`).click();
await sleep(300);
await testId(`node-${leaderBefore}-start`).click();

const restored = await waitUntil(async () => /5 \/ 5 answering/.test(await text('quorum')), 90000, 'restored');
check('the cluster is whole again at the end', restored.ok);

await page.screenshot({ path: `${SHOTS}/06-final.png`, fullPage: true });

console.log('\n=== browser problems ===');
if (problems.length === 0) {
  console.log('none');
} else {
  for (const problem of [...new Set(problems)].slice(0, 20)) console.log('  ' + problem);
}

const failed = results.filter((r) => !r.ok);
console.log(`\n=== ${results.length - failed.length}/${results.length} checks passed, ${problems.length} browser problems ===`);

await browser.close();
process.exit(failed.length === 0 && problems.length === 0 ? 0 : 1);
