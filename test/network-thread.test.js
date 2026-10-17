import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { loadIdentity, keyId, fingerprint } from '../src/identity.js';
import { startNetworkThread } from '../src/udp-server.js';
import { startLocalServer } from '../src/local-server.js';
import { validateNetworkContract } from '../src/network-contract.js';

// The game port on its own thread: an engine client joins, plays and is
// kicked while the per-packet path runs between the network thread and the
// world's port, and only bans and control cross to this thread.
const engine = process.env.BFE_ENGINE_DIR && resolve(process.env.BFE_ENGINE_DIR);
const waitFor = async (condition, ms = 5000) => { const end = Date.now() + ms; while (!(await condition())) { if (Date.now() > end) throw Error('timed out'); await new Promise(r => setTimeout(r, 5)); } };

test('the network thread runs sessions against the world port and asks this thread only about bans', { skip: !engine && 'set BFE_ENGINE_DIR to an engine checkout' }, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'bfe-net-thread-'));
  const { UdpEndpoint } = await import(pathToFileURL(join(engine, 'src/net/endpoint.js')));
  const { identityKeys } = await import(pathToFileURL(join(engine, 'src/net/crypto.js')));
  const { NETWORK_CONTRACT } = await import(pathToFileURL(join(engine, 'src/runtime/network-contract.js')));
  const keys = loadIdentity(join(directory, 'server.json'));
  const udp = await startNetworkThread({ engine, keys, port: 0, host: '127.0.0.1' });
  // A stand-in world: the battle hands it a port, and client traffic for
  // the battle flows over that port, not through this thread.
  const world = new EventEmitter(), sent = [], direct = [];
  let port = null;
  world.networkContract = validateNetworkContract(NETWORK_CONTRACT);
  world.postMessage = message => { if (message.type === 'attachNetwork') { port = message.port; port.on('message', m => direct.push(m)); } else sent.push(message); };
  const banned = loadIdentity(join(directory, 'banned.json')), bannedId = identityKeys(banned).id;
  const local = await startLocalServer({ config: { name: 'threaded', discovery: false, bind: '127.0.0.1', content: 'content' }, world, query: '?map=nab2', udp,
    isBanned: id => id === bannedId, identity: { keys, id: keyId(keys.publicKey), fingerprint: fingerprint(keys) } });
  const client = new UdpEndpoint({ identity: identityKeys(loadIdentity(join(directory, 'client.json'))) });
  await client.listen({ families: ['IPv4'] });
  t.after(async () => { client.close(); port?.close(); await local.close(); await udp.close(); await rm(directory, { recursive: true, force: true }); });
  assert.ok(port, 'the battle attached the world port');
  const info = await (await fetch(`http://127.0.0.1:${udp.port}/info`)).json();
  const hello = { protocol: NETWORK_CONTRACT.protocol, profile: NETWORK_CONTRACT.profile, simulation: NETWORK_CONTRACT.simulation, content: 'content', name: 'Josh' };
  const session = await client.connect({ serverId: info.id, candidates: [{ address: '127.0.0.1', port: info.port }], hello });
  const received = [];
  session.on('message', (lane, payload) => received.push([lane, typeof payload === 'string' ? JSON.parse(payload) : payload]));
  session.resume();
  await waitFor(() => direct.some(m => m.type === 'join'));
  const joined = direct.find(m => m.type === 'join');
  assert.equal(joined.name, 'Josh');
  assert.equal(sent.length, 0, 'nothing for the battle crossed to this thread');
  await waitFor(() => udp.players().includes(joined.client));
  // The world answers over its port, as the real world worker does.
  port.postMessage({ type: 'entityRegistry', client: joined.client, registry: { contentEpoch: 1 } });
  port.postMessage({ type: 'joined', client: joined.client, player: { unitId: 5, generation: 1, snapshotMode: 'snapshot-delta-v1' }, tick: 3 });
  const snapshot = new Uint8Array(400); new DataView(snapshot.buffer).setUint32(0, 0x31534e42, true);
  port.postMessage({ type: 'snapshotPublication', shared: [{ tick: 4, capturedAtUnixMs: Date.now(), clients: [joined.client], parts: [snapshot.buffer] }], own: [], publishedAt: performance.timeOrigin + performance.now() }, [snapshot.buffer]);
  await waitFor(() => received.length >= 3);
  assert.deepEqual(received.slice(0, 2).map(([, m]) => m.type), ['entity-registry', 'welcome']);
  assert.equal(received[1][1].serverId, info.id);
  assert.equal(received.find(([lane]) => lane === 'state')[1].length, 400);
  const input = Buffer.alloc(24); input.writeUInt32LE(0x31494e42);
  session.send('state', input);
  session.send('events', JSON.stringify({ type: 'chat', text: 'off the main thread' }));
  await waitFor(() => direct.some(m => m.type === 'entityInput') && direct.some(m => m.type === 'chat'));
  assert.ok(Number.isFinite(direct.find(m => m.type === 'entityInput').receivedAt), 'inputs carry their network receipt time');
  const metrics = await udp.metrics();
  assert.ok(metrics.sendHopMs.count >= 1 && Number.isFinite(metrics.eventLoopMs.p99));
  // Bans are this thread's policy, asked for across threads.
  const outcast = new UdpEndpoint({ identity: identityKeys(banned) });
  await outcast.listen({ families: ['IPv4'] });
  t.after(() => outcast.close());
  await assert.rejects(outcast.connect({ serverId: info.id, candidates: [{ address: '127.0.0.1', port: info.port }], hello }), /banned/);
  await waitFor(() => udp.stats()?.sessions >= 1);
  local.kick(joined.client);
  await waitFor(() => session.closed);
  await waitFor(() => direct.some(m => m.type === 'leave') && !udp.players().includes(joined.client));
});

// Aldine reaches every player through a TURN relay on its own host: online
// rendezvous allocates and permits it on the network thread
// (online-server.js), and a desktop join races and probes ports through it.
test('relays allocated on the network thread carry a raced, probed session', { skip: !engine && 'set BFE_ENGINE_DIR to an engine checkout' }, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'bfe-net-thread-'));
  const { UdpEndpoint } = await import(pathToFileURL(join(engine, 'src/net/endpoint.js')));
  const { identityKeys } = await import(pathToFileURL(join(engine, 'src/net/crypto.js')));
  const { NETWORK_CONTRACT } = await import(pathToFileURL(join(engine, 'src/runtime/network-contract.js')));
  const { turnServer } = await import(pathToFileURL(join(engine, 'tests/net-udp-fixtures.js')));
  const turn = await turnServer(), keys = loadIdentity(join(directory, 'server.json'));
  const udp = await startNetworkThread({ engine, keys, port: 0, host: '127.0.0.1' });
  const world = new EventEmitter(), direct = [];
  let port = null;
  world.networkContract = validateNetworkContract(NETWORK_CONTRACT);
  world.postMessage = message => { if (message.type === 'attachNetwork') { port = message.port; port.on('message', m => direct.push(m)); } };
  const client = new UdpEndpoint({ identity: identityKeys(loadIdentity(join(directory, 'client.json'))), raceAddress: () => true });
  await client.listen({ families: ['IPv4'] });
  t.after(async () => { client.close(); port?.close(); await udp.close(); turn.close(); await rm(directory, { recursive: true, force: true }); });
  udp.setBattle({ world, content: 'content', name: () => 'relayed', welcome: () => ({}), maxPlayers: 8 });
  const credentials = { host: '127.0.0.1', port: turn.port, username: turn.username, password: turn.password };
  await assert.rejects(udp.endpoint.allocateRelay({ ...credentials, password: 'wrong' }), /401/, 'a refused allocation fails across threads');
  const relay = await udp.endpoint.allocateRelay(credentials);
  assert.equal(relay.relayed.address, '127.0.0.1');
  await relay.permit(['127.0.0.1']);
  const hello = { protocol: NETWORK_CONTRACT.protocol, profile: NETWORK_CONTRACT.profile, simulation: NETWORK_CONTRACT.simulation, content: 'content', name: 'Josh' };
  const probed = new Promise(resolve => client.once('path-probed', (session, report) => resolve(report)));
  const session = await client.connect({ serverId: identityKeys(keys).id, candidates: [{ ...relay.relayed, kind: 'relay' }], hello, ports: 4 });
  const received = [];
  session.on('message', (lane, payload) => received.push([lane, typeof payload === 'string' ? JSON.parse(payload) : payload]));
  session.resume();
  assert.equal(session.path.kind, 'relay');
  assert.equal((await probed).ports.length, 16, 'every port was timed through the relay');
  // Traffic both ways once probing is over (the session may have moved).
  await waitFor(() => direct.some(m => m.type === 'join'));
  const joined = direct.find(m => m.type === 'join');
  port.postMessage({ type: 'entityRegistry', client: joined.client, registry: { contentEpoch: 1 } });
  port.postMessage({ type: 'joined', client: joined.client, player: { unitId: 5, generation: 1, snapshotMode: 'snapshot-delta-v1' }, tick: 3 });
  await waitFor(() => received.length >= 2);
  assert.deepEqual(received.slice(0, 2).map(([, m]) => m.type), ['entity-registry', 'welcome']);
  session.send('events', JSON.stringify({ type: 'chat', text: 'through the relay' }));
  await waitFor(() => direct.some(m => m.type === 'chat'));
  assert.ok(turn.stats.relayedUp > 0 && turn.stats.relayedDown > 0);
  relay.close();
  await waitFor(() => udp.stats()?.relays === 0, 3000);
});
