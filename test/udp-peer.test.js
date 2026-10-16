import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { CONTRACT } from './contract-fixture.js';
import { UdpPeer } from '../src/udp-peer.js';

const CLOSE_CODE = { NORMAL: 0, IDLE: 1, PROTOCOL: 2, VERSION: 3, REJECTED: 4, KICKED: 5, SHUTDOWN: 6, APPLICATION: 7 };
// A transport session as the engine's endpoint hands it over.
function session(hello = { protocol: 3, profile: 'snapshot-delta-v1', simulation: 'bfe-sim-5', content: 'abc', name: 'Josh' }) {
  const value = new EventEmitter();
  Object.assign(value, { id: 'a'.repeat(64), publicKey: 'key', hello, rtt: 42, sent: [], closes: [],
    connection: { smoothedRtt: 40, minRtt: 30, path: { socket: { getSendQueueSize: () => 0 } } },
    path: { kind: 'direct', family: 'IPv6', address: '2001:db8::1', port: 5 },
    send(lane, payload) { this.sent.push([lane, payload]); return true; },
    close(code, reason) { this.closes.push([code, reason]); this.emit('close', { code, reason, remote: false }); },
    stats() { return { rtt: 42 }; } });
  return value;
}
const peerFor = value => new UdpPeer(value, { contract: CONTRACT, content: 'abc', closeCodes: CLOSE_CODE });

test('a UDP peer is born authenticated with the handshake hello and identity', () => {
  const value = session(), peer = peerFor(value);
  assert.equal(peer.authenticated, true);
  assert.equal(peer.identity, 'a'.repeat(64));
  assert.deepEqual(peer.remote, { ...value.hello, publicKey: 'key' });
  assert.equal(peer.spectator, false);
  assert.equal(peerFor(session({ ...value.hello, content: 'other' })).spectator, true, 'different content spectates');
  assert.deepEqual(peer.path(), { kind: 'direct', family: 'ipv6' });
  peer.close();
});

test('inbound state is bounded binary input or sequenced JSON; anything else closes', () => {
  const value = session(), peer = peerFor(value), binary = [], messages = [];
  peer.on('binary', (bytes, lane) => binary.push([lane, bytes.length]));
  peer.on('message', (lane, message) => messages.push([lane, message.type, message.sequence]));
  const input = Buffer.alloc(40); input.writeUInt32LE(0x31494e42);
  const keepalive = Buffer.alloc(10); keepalive.writeUInt32LE(0x31414e42);
  value.emit('message', 'state', input);
  value.emit('message', 'state', keepalive);
  value.emit('message', 'state', JSON.stringify({ type: 'entity-edge-ack', sequence: 2 }));
  value.emit('message', 'state', JSON.stringify({ type: 'entity-edge-ack', sequence: 1 }));
  value.emit('message', 'events', JSON.stringify({ type: 'chat', text: 'hi' }));
  value.emit('message', 'events', JSON.stringify({ type: 'pong', time: 1 }));
  assert.deepEqual(binary, [['state', 40], ['state', 10]]);
  assert.deepEqual(messages, [['state', 'entity-edge-ack', 2], ['events', 'chat', undefined]], 'stale sequence and pongs are dropped');
  const snapshot = Buffer.alloc(40); snapshot.writeUInt32LE(0x31534e42);
  value.emit('message', 'state', snapshot);
  assert.equal(peer.closed, true, 'a client cannot send server snapshot datagrams');
  assert.deepEqual(value.closes, [[CLOSE_CODE.PROTOCOL, 'invalid-message']]);
});

test('sends: JSON state gains a sequence; snapshot frames go out as state datagrams', () => {
  const value = session(), peer = peerFor(value);
  peer.send('state', { type: 'input-ack' }); peer.send('state', { type: 'input-ack' });
  peer.send('events', { type: 'welcome' });
  assert.deepEqual(value.sent.map(([lane, payload]) => [lane, JSON.parse(payload).sequence]), [['state', 1], ['state', 2], ['events', undefined]]);
  const part = new Uint8Array(700), own = new Uint8Array(200);
  assert.equal(peer.sendStatePacket([part, own]), true);
  assert.deepEqual(value.sent.slice(3).map(([lane, payload]) => [lane, payload.length]), [['state', 700], ['state', 200]]);
  assert.throws(() => peer.send('events', { text: 'x'.repeat(40000) }), /too large/);
  peer.close('kicked', 'Kicked by the server operator.');
  assert.deepEqual(value.closes, [[CLOSE_CODE.KICKED, 'Kicked by the server operator.']]);
  assert.equal(peer.send('events', { type: 'late' }), false);
});

test('queueing delay skips alternate snapshots until it clears', () => {
  const value = session(), peer = peerFor(value), connection = value.connection;
  assert.equal(peer.stateBacklog(), 0);
  connection.smoothedRtt = 30 + 150;
  const decisions = Array.from({ length: 4 }, () => peer.stateBacklog() > 32768);
  assert.deepEqual(decisions, [true, false, true, false]);
  connection.smoothedRtt = 30 + 80;
  assert.equal([peer.stateBacklog(), peer.stateBacklog()].some(value => value > 32768), true, 'hysteresis holds until well clear');
  connection.smoothedRtt = 30 + 20;
  assert.deepEqual([peer.stateBacklog(), peer.stateBacklog()], [0, 0]);
  assert.equal(peer.transportStats().congested, false);
  peer.close();
});

test('a remote close finishes the peer once with its reason', () => {
  const value = session(), peer = peerFor(value), reasons = [];
  peer.on('closed', reason => reasons.push(reason));
  value.emit('close', { code: CLOSE_CODE.IDLE, reason: 'idle timeout', remote: false });
  peer.close();
  assert.deepEqual(reasons, ['idle timeout']);
});
