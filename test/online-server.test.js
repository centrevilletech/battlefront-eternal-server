import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { CONTRACT } from './contract-fixture.js';
import { OnlineServer, publicAddress, parseCandidates } from '../src/online-server.js';

const player = 'b'.repeat(64), nonce = 'c'.repeat(32);
async function setup(t, { relayPolicy = 'auto', reflexive = { IPv4: { address: '203.0.113.9', port: 40001 } }, hosts = [{ address: '192.168.1.5', port: 18451 }] } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'bfe-online-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const world = new EventEmitter(); world.networkContract = CONTRACT;
  const punched = [], relays = [];
  const udp = { players: () => [], candidates: () => hosts,
    endpoint: { punch: list => punched.push(list), reflexive: async () => reflexive,
      allocateRelay: async options => { const relay = { options, relayed: { address: '198.51.100.7', port: 49170, family: 'IPv4' }, permitted: [], permit: async list => relay.permitted.push(...list), close() { relay.closed = true; } }; relays.push(relay); return relay; } } };
  const server = new OnlineServer({ config: { name: 'test', master: 'https://master.invalid', onlineNoticeAccepted: true, relayPolicy, rotation: ['swbf1/nab2:rep:conquest'], maxPlayers: 8 },
    directory, world, query: '?map=nab2&match=' + 'd'.repeat(32), udp });
  const signals = [];
  server.master = Object.assign(new EventEmitter(), { signal: (to, message) => { signals.push({ to, ...message }); return true; },
    credentials: async () => [{ hostname: 'relay.test', port: 3478, username: 'u', password: 'p', relayType: 'TurnUdp' }, { hostname: 'relay.test', port: 5349, username: 'u', password: 'p', relayType: 'TurnTls' }] });
  server.reflexive = reflexive;
  return { server, signals, punched, relays };
}
const offer = candidates => ({ from: player, type: 'offer', payload: JSON.stringify({ udp: 1, nonce, candidates }) });

test('an online offer is answered with public then reflexive candidates, and punched at public addresses only', async t => {
  const { server, signals, punched, relays } = await setup(t, { hosts: [{ address: '192.168.1.5', port: 18451 }, { address: '2001:db8::5', port: 18451 }], reflexive: { IPv4: { address: '203.0.113.9', port: 18451 } } });
  await server.signal(offer([{ address: '198.51.100.20', port: 50000, kind: 'srflx' }, { address: '127.0.0.1', port: 50000, kind: 'host' }, { address: '169.254.169.254', port: 80, kind: 'host' }]));
  assert.deepEqual(signals[0], { to: player, type: 'answer', udp: 1, nonce, candidates: [
    { address: '2001:db8::5', port: 18451, kind: 'host' }, { address: '203.0.113.9', port: 18451, kind: 'srflx' }] }, 'private addresses are not disclosed to a player on another network');
  assert.deepEqual(punched, [[{ address: '198.51.100.20', port: 50000, kind: 'srflx' }]], 'no punches at loopback, private or link-local addresses');
  // IPv4 is behind NAT (no interface holds the reflexive address), so auto
  // also allocates a relay and trickles it after the direct answer.
  assert.equal(relays.length, 1);
  assert.deepEqual(signals.map(signal => signal.type), ['answer', 'candidate']);
});

test('a player behind the host\'s own NAT also gets its private addresses, last', async t => {
  const { server, signals } = await setup(t, { hosts: [{ address: '192.168.1.5', port: 18451 }], reflexive: { IPv4: { address: '203.0.113.9', port: 18451 } } });
  await server.signal(offer([{ address: '203.0.113.9', port: 50001, kind: 'srflx' }]));
  assert.deepEqual(signals[0].candidates, [{ address: '203.0.113.9', port: 18451, kind: 'srflx' }, { address: '192.168.1.5', port: 18451, kind: 'host' }]);
});

test('relays are capped and released when their player never joins', async t => {
  const { server, relays } = await setup(t);
  const faults = [];
  server.on('fault', message => faults.push(message));
  const from = i => ({ ...offer([{ address: '198.51.100.20', port: 50000 + i, kind: 'srflx' }]), from: i.toString(16).padStart(64, '0') });
  for (let i = 0; i < 16; i++) await server.signal(from(i));
  // The next offer window: offers expire after 10 s, relays live longer.
  server.offers.clear();
  for (let i = 16; i < 24; i++) await server.signal(from(i));
  assert.equal(relays.length, 16, 'maxPlayers (8) plus 8');
  assert.ok(faults.some(message => /limit reached/.test(message)));
  for (const entry of server.relays.values()) entry.created -= 31000;
  server.sweep();
  assert.equal(server.relays.size, 0);
  assert.ok(relays.every(relay => relay.closed));
});

test('a NAT host trickles a per-player relay candidate permitted for that player only', async t => {
  const { server, signals, relays } = await setup(t);
  await server.signal(offer([{ address: '198.51.100.20', port: 50000, kind: 'srflx' }, { address: '10.0.0.4', port: 50000, kind: 'host' }]));
  assert.deepEqual(signals.map(signal => signal.type), ['answer', 'candidate']);
  assert.deepEqual(signals[1].candidates, [{ address: '198.51.100.7', port: 49170, family: 'IPv4', kind: 'relay' }]);
  assert.equal(relays[0].options.host, 'relay.test', 'the UDP relay, not TCP/TLS');
  assert.deepEqual(relays[0].permitted, ['198.51.100.20'], 'private player addresses are never permitted on the public relay');
});

test('relay policy always advertises only the relay; never allocates none', async t => {
  const always = await setup(t, { relayPolicy: 'always' });
  await always.server.signal(offer([{ address: '198.51.100.20', port: 50000 }]));
  assert.deepEqual(always.signals.map(signal => [signal.type, signal.candidates.map(c => c.kind)]), [['answer', ['relay']]]);
  assert.deepEqual(always.punched, [], 'no direct packets reveal the host');
  const never = await setup(t, { relayPolicy: 'never' });
  await never.server.signal(offer([{ address: '198.51.100.20', port: 50000 }]));
  assert.deepEqual(never.signals.map(signal => signal.type), ['answer']);
  assert.equal(never.relays.length, 0);
});

test('malformed, repeated and banned offers are ignored; readiness probes still answer', async t => {
  const { server, signals } = await setup(t, { reflexive: { IPv4: { address: '192.168.1.5', port: 18451 } } });
  await server.signal({ from: player, type: 'offer', payload: JSON.stringify({ sdp: 'v=0' }) });
  assert.equal(signals.length, 0, 'a WebRTC offer from an old client gets nothing');
  await server.signal(offer([]));
  await server.signal(offer([]));
  assert.equal(signals.length, 1, 'one answer per player per second');
  server.offers.clear();
  await assert.rejects(server.signal(offer([{ address: 'not-an-ip', port: 1 }])), /invalid candidate/);
  await server.signal(offer([]));
  assert.equal(signals.length, 1, 'a malformed offer still spends the player\'s slot');
  server.isBanned = id => id === player;
  server.offers.clear();
  await server.signal(offer([]));
  assert.equal(signals.length, 1);
  server.isBanned = () => false;
  await server.signal({ from: player, type: 'matchProbe', payload: JSON.stringify({ type: 'matchProbe', matchProbe: 'd'.repeat(32), nonce }) });
  assert.equal(signals.at(-1).type, 'matchReady');
});

test('address helpers', () => {
  for (const address of ['10.1.2.3', '192.168.0.1', '172.20.0.1', '127.0.0.1', '169.254.1.1', '100.64.0.1', 'fe80::1', 'fd00::1', '::1']) assert.equal(publicAddress(address), false, address);
  for (const address of ['203.0.113.9', '8.8.8.8', '2001:db8::1', '::ffff:203.0.113.9']) assert.equal(publicAddress(address), true, address);
  assert.throws(() => parseCandidates(Array(9).fill({ address: '203.0.113.9', port: 1 })), /invalid candidates/);
  assert.deepEqual(parseCandidates([{ address: '203.0.113.9', port: 5, kind: 'weird' }]), [{ address: '203.0.113.9', port: 5, kind: 'host' }]);
});
