import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { CONTRACT } from './contract-fixture.js';
import { startUdpServer, hostCandidates } from '../src/udp-server.js';

const CLOSE_CODE = { NORMAL: 0, IDLE: 1, PROTOCOL: 2, VERSION: 3, REJECTED: 4, KICKED: 5, SHUTDOWN: 6, APPLICATION: 7 };
// The engine endpoint's shape, without sockets: the server's hooks and the
// sessions it would hand over.
class FakeEndpoint extends EventEmitter {
  constructor({ server, maxSessionsPerPrefix }) { super(); this.hooks = server; this.maxSessionsPerPrefix = maxSessionsPerPrefix; this.closed = false; }
  async listen({ port }) { return { port: port || 40001, families: ['IPv4', 'IPv6'] }; }
  stats() { return { sessions: 0 }; }
  close() { this.closed = true; }
}
const transport = { UdpEndpoint: FakeEndpoint, identityKeys: keys => keys, CLOSE_CODE };
const hello = { protocol: 3, profile: 'snapshot-delta-v1', simulation: 'bfe-sim-5', content: 'abc' };
function session(id, extra = {}) {
  const value = new EventEmitter();
  Object.assign(value, { id, publicKey: 'k', hello: { ...hello, ...extra }, rtt: 10, closes: [], sent: [], connection: { path: null },
    path: { kind: 'direct', family: 'IPv4' }, send(lane, payload) { this.sent.push([lane, payload]); return true; },
    close(code, reason) { this.closes.push([code, reason]); this.emit('close', { code, reason, remote: false }); }, stats: () => ({}) });
  return value;
}
const world = () => { const value = new EventEmitter(); value.networkContract = CONTRACT; value.sent = []; value.postMessage = m => value.sent.push(m); return value; };

test('admission waits for a battle, then checks version, bans and capacity', async () => {
  const udp = await startUdpServer({ transport, keys: {}, port: 0 });
  const { hooks } = udp.endpoint;
  assert.equal(hooks.admit({ client: { id: 'a', hello } }).code, CLOSE_CODE.REJECTED, 'nothing to join while the next battle loads');
  assert.equal(udp.endpoint.maxSessionsPerPrefix, 8, 'one network holds at most eight sessions by default');
  const banned = new Set(['bad']);
  udp.setBattle({ world: world(), content: 'abc', isBanned: id => banned.has(id), maxPlayers: 0 });
  assert.deepEqual(hooks.hello(), { protocol: 3, profile: 'snapshot-delta-v1', simulation: 'bfe-sim-5', content: 'abc', name: '' });
  udp.setBattle({ world: world(), content: 'abc', name: () => 'x'.repeat(2000) });
  assert.equal(hooks.hello().name.length, 100, 'the hello stays inside one handshake datagram');
  udp.setBattle({ world: world(), content: 'abc', isBanned: id => banned.has(id), maxPlayers: 0 });
  assert.match(hooks.admit({ client: { id: 'a', hello: { ...hello, protocol: 2 } } }).reason, /Update required: protocol 3/);
  assert.equal(hooks.admit({ client: { id: 'a', hello: { ...hello, simulation: 'bfe-sim-4' } } }).code, CLOSE_CODE.VERSION);
  assert.match(hooks.admit({ client: { id: 'bad', hello } }).reason, /banned/);
  assert.equal(hooks.admit({ client: { id: 'a', hello } }).accept, true);
  for (let i = 0; i < 16; i++) udp.endpoint.emit('session', session('p' + i));
  assert.match(hooks.admit({ client: { id: 'a', hello } }).reason, /full/);
  assert.equal(hooks.admit({ client: { id: 'p1', hello } }).accept, true, 'a reconnecting player is never refused as full');
  await udp.close();
});

test('sessions join the battle; a reconnect replaces the stale one; kick, match and battle end close them', async () => {
  const udp = await startUdpServer({ transport, keys: {}, port: 0 }), battle = world();
  udp.setBattle({ world: battle, content: 'abc', welcome: () => ({ serverId: 's' }) });
  const first = session('a'.repeat(64), { name: 'Josh' });
  udp.endpoint.emit('session', first);
  assert.deepEqual(battle.sent.map(m => [m.type, m.client, m.name]), [['join', 'a'.repeat(64), 'Josh']]);
  assert.deepEqual(udp.players(), ['a'.repeat(64)]);
  const second = session('a'.repeat(64));
  udp.endpoint.emit('session', second);
  assert.deepEqual(first.closes, [[CLOSE_CODE.NORMAL, 'replaced']]);
  assert.deepEqual(battle.sent.map(m => m.type), ['join', 'leave', 'join'], 'the stale session leaves before the new one joins');
  udp.match({ query: '?map=bes2', delayMs: 100 });
  assert.equal(JSON.parse(second.sent.at(-1)[1]).type, 'match');
  udp.kick('a'.repeat(64));
  assert.deepEqual(second.closes, [[CLOSE_CODE.KICKED, 'Kicked by the server operator.']]);
  const third = session('b'.repeat(64));
  udp.endpoint.emit('session', third);
  udp.clearBattle();
  assert.equal(third.closes[0][0], CLOSE_CODE.SHUTDOWN);
  const late = session('c'.repeat(64));
  udp.endpoint.emit('session', late);
  assert.equal(late.closes[0][0], CLOSE_CODE.REJECTED);
  await udp.close();
  assert.equal(udp.endpoint.closed, true);
});

test('host candidates skip internal and link-local addresses', () => {
  for (const candidate of hostCandidates(18451)) {
    assert.equal(candidate.port, 18451);
    assert.ok(!/^fe80:/i.test(candidate.address) && candidate.address !== '127.0.0.1' && candidate.address !== '::1');
  }
});
