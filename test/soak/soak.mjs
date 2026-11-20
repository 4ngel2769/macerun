// 4-client stability soak. Each bot joins, chats, digs/place, moves, and
// periodically disconnects/reconnects. Prints a single PASS/FAIL summary.
import mc from 'minecraft-protocol';

const HOST = process.env.SOAK_HOST || '192.168.101.117';
const PORT = 25565;
const DURATION_MS = Number(process.env.SOAK_MS || 30 * 60 * 1000);
const RECONNECT_MS = 60 * 1000;

const stats = { joins: 0, chats: 0, digs: 0, moves: 0, reconnects: 0, errors: 0, ends: 0 };

function makeBot(name) {
  const bot = mc.createClient({ host: HOST, port: PORT, username: name, version: '1.16.5', keepAlive: true });
  bot.on('login', () => {
    stats.joins++;
    bot.write('chat', { message: `soak ${name}` });
  });
  bot.on('packet', (d, m) => {
    if (m.name === 'chat') stats.chats++;
    if (m.name === 'block_change') stats.digs++;
  });
  bot.on('error', (e) => { stats.errors++; console.error(`[${name}] error: ${e.message}`); });
  bot.on('end', (r) => { stats.ends++; console.error(`[${name}] end: ${r}`); });
  return bot;
}

const names = ['SoakA', 'SoakB', 'SoakC', 'SoakD'];
let bots = names.map(makeBot);

const actions = setInterval(() => {
  for (let i = 0; i < bots.length; i++) {
    const bot = bots[i];
    if (bot.state !== 'play') continue;
    const x = (i * 3) % 16;
    const y = 60, z = 4;
    bot.write('block_dig', { status: 0, location: { x, y, z }, face: 1 });
    bot.write('block_dig', { status: 2, location: { x, y, z }, face: 1 });
    stats.digs++;
    bot.write('position', { x: x + 0.5, y, z: z + 0.5, onGround: true });
    stats.moves++;
  }
}, 2000);

const reconnector = setInterval(() => {
  const i = Math.floor(Math.random() * bots.length);
  const old = bots[i];
  const nm = `${names[i].replace(/[0-9]/g, '')}${Date.now() % 100000}`;
  try { old.end(); } catch (e) {}
  bots[i] = makeBot(nm);
  stats.reconnects++;
}, RECONNECT_MS);

setTimeout(() => {
  clearInterval(actions);
  clearInterval(reconnector);
  bots.forEach((b) => { try { b.end(); } catch (e) {} });
  const unexpectedEnds = stats.ends - stats.reconnects;
  const pass = stats.errors === 0 && unexpectedEnds <= 0 && stats.joins > 0;
  console.log(JSON.stringify({ ...stats, unexpectedEnds, verdict: pass ? 'PASS' : 'FAIL' }));
  process.exit(pass ? 0 : 1);
}, DURATION_MS);
