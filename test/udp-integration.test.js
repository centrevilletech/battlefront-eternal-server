import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { loadIdentity, keyId, fingerprint } from '../src/identity.js';
import { startUdpServer } from '../src/udp-server.js';
import { startLocalServer } from '../src/local-server.js';
import { validateNetworkContract } from '../src/network-contract.js';

// The engine's real transport against this server's listener, game session
// and world routing. Needs an engine checkout (BFE_ENGINE_DIR); the public
// CI has none, so it skips there.
const engine = process.env.BFE_ENGINE_DIR && resolve(process.env.BFE_ENGINE_DIR);
const waitFor = async (condition, ms = 5000) => { const end = Date.now() + ms; while (!condition()) { if (Date.now() > end) throw Error('timed out'); await new Promise(r => setTimeout(r, 5)); } };

test('an engine client joins the UDP listener and plays through GameSession', { skip: !engine && 'set BFE_ENGINE_DIR to an engine checkout' }, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'bfe-udp-integration-'));
  const { UdpEndpoint } = await import(pathToFileURL(join(engine, 'src/net/endpoint.js')));
  const { identityKeys } = await import(pathToFileURL(join(engine, 'src/net/crypto.js')));
  const { NETWORK_CONTRACT } = await import(pathToFileURL(join(engine, 'src/runtime/network-contract.js')));
  const keys = loadIdentity(join(directory, 'server.json'));
  const udp = await startUdpServer({ engine, keys, port: 0, host: '127.0.0.1' });
  const world = new EventEmitter(); world.networkContract = validateNetworkContract(NETWORK_CONTRACT); world.sent = []; world.postMessage = m => world.sent.push(m);
  const local = await startLocalServer({ config: { name: 'integration', discovery: false, bind: '127.0.0.1', content: 'content' }, world, query: '?map=nab2', udp,
    identity: { keys, id: keyId(keys.publicKey), fingerprint: fingerprint(keys) } });
  const client = new UdpEndpoint({ identity: identityKeys(loadIdentity(join(directory, 'client.json'))) });
  await client.listen({ families: ['IPv4'] });
  t.after(async () => { client.close(); await local.close(); await udp.close(); await rm(directory, { recursive: true, force: true }); });
  const info = await (await fetch(`http://127.0.0.1:${udp.port}/info`)).json();
  assert.equal(info.transport, 'udp');
  assert.equal(info.protocol, 3);
  const hello = { protocol: NETWORK_CONTRACT.protocol, profile: NETWORK_CONTRACT.profile, simulation: NETWORK_CONTRACT.simulation, content: 'content', name: 'Josh' };
  const session = await client.connect({ serverId: info.id, candidates: [{ address: '127.0.0.1', port: info.port }], hello });
  const received = [];
  session.on('message', (lane, payload) => received.push([lane, typeof payload === 'string' ? JSON.parse(payload) : payload]));
  session.resume();
  await waitFor(() => world.sent.some(m => m.type === 'join'));
  const joined = world.sent.find(m => m.type === 'join');
  assert.equal(joined.name, 'Josh');
  assert.equal(joined.spectator, false);
  // The world's registry, welcome and a snapshot reach the client.
  world.emit('message', { type: 'entityRegistry', client: joined.client, registry: { contentEpoch: 1 } });
  world.emit('message', { type: 'joined', client: joined.client, player: { unitId: 5, generation: 1, snapshotMode: 'snapshot-delta-v1' }, tick: 3 });
  const snapshot = new Uint8Array(400); new DataView(snapshot.buffer).setUint32(0, 0x31534e42, true);
  world.emit('message', { type: 'snapshotPublication', shared: [{ tick: 4, capturedAtUnixMs: Date.now(), clients: [joined.client], parts: [snapshot.buffer] }], own: [] });
  await waitFor(() => received.length >= 3);
  assert.deepEqual(received.slice(0, 2).map(([, m]) => m.type), ['entity-registry', 'welcome']);
  assert.equal(received[1][1].serverId, info.id);
  assert.equal(received.find(([lane]) => lane === 'state')[1].length, 400);
  // Input and chat flow back to the world.
  const input = Buffer.alloc(24); input.writeUInt32LE(0x31494e42);
  session.send('state', input);
  session.send('events', JSON.stringify({ type: 'chat', text: 'over udp' }));
  await waitFor(() => world.sent.some(m => m.type === 'entityInputs') && world.sent.some(m => m.type === 'chat'));
  assert.equal(world.sent.find(m => m.type === 'entityInputs').inputs[0].buffer.byteLength, 24);
  assert.equal(world.sent.find(m => m.type === 'chat').text, 'over udp');
  // An old client is told to update, by the handshake itself.
  const stale = new UdpEndpoint({ identity: identityKeys(loadIdentity(join(directory, 'stale.json'))) });
  await stale.listen({ families: ['IPv4'] });
  t.after(() => stale.close());
  await assert.rejects(stale.connect({ serverId: info.id, candidates: [{ address: '127.0.0.1', port: info.port }], hello: { ...hello, protocol: 2 } }), /Update required: protocol 3/);
  local.kick(joined.client);
  await waitFor(() => session.closed);
  assert.ok(world.sent.some(m => m.type === 'leave'));
});
