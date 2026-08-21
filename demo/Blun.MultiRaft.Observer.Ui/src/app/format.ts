/** Byte sizes and counts, formatted the same way wherever they appear. */

const UNITS = ['B', 'KB', 'MB', 'GB'];

export function formatBytes(bytes: number): string {
  if (!bytes) return '0 B';

  let value = bytes;
  let unit = 0;

  while (value >= 1024 && unit < UNITS.length - 1) {
    value /= 1024;
    unit++;
  }

  // Whole bytes stay whole; anything scaled keeps one decimal, which is enough to watch a log grow without
  // the number jittering in the last digit every poll.
  return unit === 0 ? `${value} B` : `${value.toFixed(1)} ${UNITS[unit]}`;
}

export function formatCount(value: number): string {
  return value.toLocaleString('en-US');
}
