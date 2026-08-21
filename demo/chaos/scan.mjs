// Reads the node logs a chaos run collected and reports what the nodes actually said, grouped by level and
// by message rather than by line -- an exception's stack trace is fifty lines of one event.

import fs from 'node:fs';
import path from 'node:path';

const dir = process.argv[2] ?? 'run2';
const ANSI = new RegExp('\\u001b\\[[0-9;]*[A-Za-z]', 'g');

const LEVEL = /^(trce|dbug|info|warn|fail|crit):\s*(\S+)/;

/** Problems that are not the cluster doing its job: an integrity violation, a crash, a hang. */
const SUSPICIOUS = [
  /InvalidOperationException/,
  /NullReferenceException/,
  /ObjectDisposedException/,
  /Unhandled exception/i,
  /StackOverflow/,
  /Deadlock/i,
  /corrupt/i,
  /checksum/i,
];

const byLevel = new Map();
const messages = new Map();
const suspicious = [];

for (const file of fs.readdirSync(dir).filter((f) => f.startsWith('node-'))) {
  const text = fs.readFileSync(path.join(dir, file), 'utf8').replace(ANSI, '');
  const lines = text.split('\n').map((l) => l.replace(/^\S+Z\s?/, ''));

  for (let i = 0; i < lines.length; i++) {
    const match = LEVEL.exec(lines[i].trim());

    if (match) {
      const [, level, source] = match;
      byLevel.set(level, (byLevel.get(level) ?? 0) + 1);

      const body = (lines[i + 1] ?? '').trim().replace(/\b\d+\b/g, 'N');
      const key = `${level} ${source} :: ${body.slice(0, 110)}`;
      messages.set(key, (messages.get(key) ?? 0) + 1);
    }

    for (const pattern of SUSPICIOUS) {
      if (pattern.test(lines[i])) {
        suspicious.push(`${file}: ${lines[i].trim().slice(0, 160)}`);
        break;
      }
    }
  }
}

console.log('=== lines by level ===');
for (const [level, count] of [...byLevel].sort((a, b) => b[1] - a[1])) {
  console.log(`${String(count).padStart(5)}  ${level}`);
}

console.log('\n=== distinct messages (numbers normalised) ===');
for (const [key, count] of [...messages].sort((a, b) => b[1] - a[1])) {
  console.log(`${String(count).padStart(5)}  ${key}`);
}

console.log(`\n=== suspicious lines: ${suspicious.length} ===`);
for (const line of suspicious.slice(0, 20)) {
  console.log('  ' + line);
}
