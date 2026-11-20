// Parse a captured ESP serial log for the stability gate.
// Usage: node check_log.mjs <logfile>
import fs from 'fs';

const file = process.argv[2];
if (!file) { console.error('usage: node check_log.mjs <logfile>'); process.exit(2); }
const text = fs.readFileSync(file, 'utf8');

const forbidden = [
  'send stalled',
  'chunk send failed',
  'recv failed',
  'Guru Meditation',
  'STACK OVERFLOW',
  'task watchdog',
];
const hits = forbidden.filter((p) => text.includes(p));

const health = [...text.matchAll(/health: tick_max=(\d+)ms heap=(\d+) psram=(\d+) net_hw=(\d+) console_hw=(\d+) clients=(\d+) queue_max=(\d+) low_drops=(\d+) coalesces=(\d+) nvs_fail=(\d+)/g)];
const maxTick = health.length ? Math.max(...health.map((m) => Number(m[1]))) : -1;

const fails = [];
if (hits.length) fails.push(`forbidden log patterns: ${hits.join(', ')}`);
if (health.length < 2) fails.push(`need >=2 health lines, got ${health.length}`);
if (maxTick > 100) fails.push(`tick_max ${maxTick}ms > 100ms`);

if (fails.length) {
  console.log(`FAIL: ${fails.join('; ')}`);
  process.exit(1);
}
console.log(`PASS: health lines=${health.length}, max tick=${maxTick}ms, forbidden patterns=0`);
