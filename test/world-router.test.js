import { CONTRACT } from './contract-fixture.js';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import dgram from 'node:dgram';
import { GameSession, SESSION_SILENT_MS, SESSION_JOIN_SILENT_MS } from '../src/game-session.js';
import { netMetrics } from '../src/net-metrics.js';

// The network thread's side of the world port: the handler only enqueues,
// and the drain bounds what it keeps (newest snapshot and ACK per session)
// and how long it holds the thread.
const flush = async () => { for (let i = 0; i < 8; i++) await new Promise(r => setImmediate(r)); };
const datagram = (magic, size = 100) => { const bytes = new Uint8Array(size); new DataView(bytes.buffer).setUint32(0, magic, true); return bytes.buffer; };
const SHARED = 0x31534e42, OWN = 0x314f4e42;
function mkPeer() {
  const peer = new EventEmitter(); peer.remote = { protocol: 3, simulation: 'bfe-sim-5', profile: 'snapshot-delta-v1' };
  peer.sent = []; peer.state = []; peer.encoded = []; peer.closedWith = null;
  peer.send = (lane, m) => { peer.sent.push({ lane, m }); return true; };
  peer.sendBinary = bytes => { peer.state.push(Buffer.from(bytes)); return true; };
  peer.close = reason => { if (peer.closedWith) return; peer.closedWith = reason; peer.emit('closed'); };
  return peer;
}
function mkWorld() { const world = new EventEmitter(); world.networkContract = CONTRACT; world.sent = []; world.postMessage = m => world.sent.push(m); return world; }
// Captured "in the future" so a stall of this shared test machine cannot
// age these publications past STATE_STALE_MS (staleness has its own tests).
const publication = (tick, clients, own = clients) => ({ type: 'snapshotPublication',
  shared: [{ tick, capturedAtUnixMs: Date.now() + 60000, clients, parts: [datagram(SHARED), datagram(SHARED)] }],
  own: own.map(client => ({ client, tick, capturedAtUnixMs: Date.now() + 60000, buffer: datagram(OWN) })) });

test('only each session\'s newest snapshot is sent; an older unsent one is skipped whole', async () => {
  const world = mkWorld(), pa = mkPeer(), pb = mkPeer();
  const a = new GameSession(pa, world, 'alpha', {}), b = new GameSession(pb, world, 'beta', {});
  try {
    const superseded = netMetrics().supersededSnapshots;
    world.emit('message', publication(10, ['alpha', 'beta']));
    world.emit('message', publication(12, ['alpha'], []));
    await flush();
    // alpha: tick 12's parts only (its own datagram for 12 was not sent);
    // beta was not in tick 12, so it still gets all of tick 10.
    assert.deepEqual(pa.state.map(bytes => bytes.readUInt32LE(0)), [SHARED, SHARED]);
    assert.deepEqual(pb.state.map(bytes => bytes.readUInt32LE(0)), [SHARED, SHARED, OWN]);
    assert.equal(a.transportDiagnostics().stateSkippedSnapshots, 1);
    assert.equal(b.transportDiagnostics().stateSkippedSnapshots, 0);
    assert.equal(netMetrics().supersededSnapshots, superseded + 1);
  } finally { a.close(); b.close(); }
});

test('a deep backlog is compacted: per-session work stays bounded and nothing is retained after the drain', async () => {
  const world = mkWorld(), peers = [], sessions = [];
  for (let i = 0; i < 16; i++) { const peer = mkPeer(); peers.push(peer); sessions.push(new GameSession(peer, world, 'c' + i, {})); }
  try {
    const ids = sessions.map(session => session.id), router = sessions[0].router;
    for (let tick = 2; tick <= 400; tick += 2) {
      world.emit('message', publication(tick, ids));
      world.emit('message', { type: 'inputAck', recipients: ids.map(client => ({ client, epoch: 1, generation: 1, serverTick: tick, ack: tick, ackTick: tick })) });
    }
    assert.equal(router.inbox.length, 400, 'the handler only enqueued');
    await new Promise(r => setImmediate(r));
    assert.ok(router.snapshots.size <= ids.length && router.acks.size <= ids.length, 'at most one snapshot and one ACK per session');
    const busy = () => router.inbox.length || router.snapshots.size || router.acks.size;
    for (let i = 0; i < 200 && busy(); i++) await flush();
    assert.equal(router.inbox.length, 0); assert.equal(router.head, 0);
    assert.equal(router.snapshots.size + router.acks.size + router.edges.size, 0);
    for (const peer of peers) {
      // Each session got the newest snapshot it was due and the newest ACK.
      const acks = peer.sent.filter(entry => entry.m.type === 'input-ack');
      // At most one per drain slice, not one per world message (200).
      assert.ok(acks.length >= 1 && acks.length <= 20, `ACKs coalesced (${acks.length})`);
      assert.equal(acks.at(-1).m.serverTick, 400);
      assert.ok(peer.state.length <= 20 * 3, `snapshots coalesced (${peer.state.length} datagrams of 600)`);
    }
  } finally { for (const session of sessions) session.close(); }
});

test('gameplay events cross once with their recipients, are encoded once and split when too large', async () => {
  const world = mkWorld(), pa = mkPeer(), pb = mkPeer();
  for (const peer of [pa, pb]) peer.sendEncoded = (lane, data) => { peer.encoded.push({ lane, data }); return true; };
  const a = new GameSession(pa, world, 'alpha', {}), b = new GameSession(pb, world, 'beta', {});
  try {
    const record = { kind: 1, payload: new Uint8Array([1, 2, 3]) };
    world.emit('message', { type: 'gameplayEvents', tick: 8, records: [record], clients: ['alpha', 'beta', 'ghost'] });
    await flush();
    assert.equal(pa.encoded.length, 1); assert.equal(pb.encoded.length, 1);
    assert.equal(pa.encoded[0].data, pb.encoded[0].data, 'one serialization shared by every recipient');
    assert.deepEqual(JSON.parse(pa.encoded[0].data), { type: 'gameplay-events', tick: 8, records: [{ kind: 1, payload: 'AQID' }] });
    const big = Array.from({ length: 40 }, (_, i) => ({ kind: i, payload: new Uint8Array(1000) }));
    world.emit('message', { type: 'gameplayEvents', tick: 10, records: big, clients: ['alpha'] });
    await flush();
    const pieces = pa.encoded.slice(1).map(entry => JSON.parse(entry.data));
    assert.ok(pieces.length >= 2, 'an oversized batch is split, not dropped');
    assert.ok(pa.encoded.slice(1).every(entry => Buffer.byteLength(entry.data) <= 32768));
    assert.deepEqual(pieces.flatMap(piece => piece.records.map(r => r.kind)), big.map(r => r.kind), 'every record, in order');
  } finally { a.close(); b.close(); }
});

test('one session failing closes that session only', async () => {
  const world = mkWorld(), pa = mkPeer(), pb = mkPeer();
  pa.send = () => { throw Error('boom'); };
  const a = new GameSession(pa, world, 'alpha', {}), b = new GameSession(pb, world, 'beta', {});
  const warn = console.warn; console.warn = () => {};
  try {
    world.emit('message', { type: 'entityRegistry', client: 'alpha', registry: {} });
    world.emit('message', { type: 'entityRegistry', client: 'beta', registry: {} });
    await flush();
    assert.equal(pa.closedWith, 'session-failed'); assert.ok(a.closed);
    assert.ok(world.sent.some(m => m.type === 'leave' && m.client === 'alpha'));
    assert.equal(pb.sent.length, 1); assert.ok(!b.closed);
  } finally { console.warn = warn; a.close(); b.close(); }
});

test('a silent session is closed, and so is one the world evicted', async () => {
  const world = mkWorld(), pa = mkPeer(), pb = mkPeer(), pc = mkPeer();
  const a = new GameSession(pa, world, 'alpha', {}), b = new GameSession(pb, world, 'beta', {}), c = new GameSession(pc, world, 'gamma', {});
  try {
    a.started = b.started = true;
    a.heardAt -= SESSION_SILENT_MS + 1;
    b.heardAt -= SESSION_SILENT_MS + 1;
    const input = Buffer.alloc(10); input.writeUInt32LE(0x31414e42);
    pb.emit('binary', input, 'state');
    a.router.checkLiveness();
    assert.equal(pa.closedWith, 'idle'); assert.equal(pb.closedWith, null, 'an ack keepalive counts as life');
    world.emit('message', { type: 'clientEvicted', client: 'gamma', reason: 'stale' });
    await flush();
    assert.equal(pc.closedWith, 'idle');
    assert.ok(['alpha', 'gamma'].every(client => world.sent.some(m => m.type === 'leave' && m.client === client)));
  } finally { a.close(); b.close(); c.close(); }
});

test('a client still loading its battle has longer before a silence close', () => {
  const world = mkWorld(), pa = mkPeer();
  const a = new GameSession(pa, world, 'alpha', {});
  try {
    a.heardAt -= SESSION_SILENT_MS + 1;
    a.router.checkLiveness();
    assert.equal(pa.closedWith, null, 'loading, not yet started');
    pa.emit('message', 'events', { type: 'call', id: 1, method: 'start' });
    assert.ok(a.started);
    a.heardAt -= SESSION_SILENT_MS + 1;
    a.router.checkLiveness();
    assert.equal(pa.closedWith, 'idle');
  } finally { a.close(); }
  const pb = mkPeer(), b = new GameSession(pb, world, 'beta', {});
  try {
    b.heardAt -= SESSION_JOIN_SILENT_MS + 1;
    b.router.checkLiveness();
    assert.equal(pb.closedWith, 'idle');
  } finally { b.close(); }
});

// After /tmp/bfe-opt-network/starve.mjs: a backlog of costly world messages
// must not stop this thread's timers or socket reads while it drains.
test('timers and socket reads keep running while a world backlog drains', async () => {
  const world = mkWorld(), peer = mkPeer();
  const spin = ms => { const end = performance.now() + ms; while (performance.now() < end); };
  peer.send = () => { spin(0.4); return true; };
  const session = new GameSession(peer, world, 'alpha', {});
  const socket = dgram.createSocket('udp4'), blaster = dgram.createSocket('udp4');
  await new Promise(r => socket.bind(0, '127.0.0.1', r));
  let reads = 0, ticks = 0, handled = 0;
  socket.on('message', () => reads++);
  const port = socket.address().port;
  const blast = setInterval(() => { for (let i = 0; i < 10; i++) blaster.send(Buffer.alloc(64), port, '127.0.0.1'); }, 5);
  const timer = setInterval(() => ticks++, 10);
  try {
    await new Promise(r => setTimeout(r, 30));
    // About 600 ms of work arrives at once, as a port backlog does.
    const count = 1500;
    const emitted = performance.now();
    for (let i = 0; i < count; i++) world.emit('message', { type: 'chat', client: 'alpha', text: 'x' });
    assert.ok(performance.now() - emitted < 50, 'the handler does no per-message work');
    const readsBefore = reads, ticksBefore = ticks;
    const original = peer.send; peer.send = (...args) => { handled++; return original(...args); };
    while (handled < count) await new Promise(r => setTimeout(r, 10));
    const readsDuring = reads - readsBefore, ticksDuring = ticks - ticksBefore;
    assert.ok(ticksDuring >= 20, `timers kept firing during the drain (${ticksDuring})`);
    assert.ok(readsDuring >= 200, `socket reads kept running during the drain (${readsDuring})`);
  } finally { clearInterval(blast); clearInterval(timer); socket.close(); blaster.close(); session.close(); }
});

test('each client gets only its own input ack and edge results, not every recipient', async () => {
  const world = mkWorld(), pa = mkPeer(), pb = mkPeer();
  const a = new GameSession(pa, world, 'alpha', {}), b = new GameSession(pb, world, 'beta', {});
  try {
    const recipients = client => ({ client, received: [1], results: [{ id: 1, status: 'consumed', tick: 5 }] });
    world.emit('message', { type: 'entityEdgeResults', epoch: 1, generation: 0, recipients: [recipients('alpha'), recipients('beta')] });
    world.emit('message', { type: 'inputAck', epoch: 1, generation: 0, serverTick: 10, recipients: [{ client: 'alpha', sequence: 3 }, { client: 'beta', sequence: 4 }] });
    await flush();
    for (const [peer, client] of [[pa, 'alpha'], [pb, 'beta']]) {
      const sent = peer.sent.map(({ m }) => m);
      const results = sent.find(m => m.type === 'entity-edge-results'), ack = sent.find(m => m.type === 'input-ack');
      assert.ok(results && ack, `${client} got both`);
      assert.equal(results.recipients, undefined); assert.equal(ack.recipients, undefined);
      assert.deepEqual(results.received, [1]);
      assert.equal(ack.sequence, client === 'alpha' ? 3 : 4);
    }
  } finally { a.close(); b.close(); }
});
