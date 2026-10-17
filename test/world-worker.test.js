import { CONTRACT } from './contract-fixture.js';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Worker } from 'node:worker_threads';

// The world worker's session bookkeeping against a stand-in engine whose
// world and snapshot host report what the worker asks of them.
const modules = {
  'package.json': '{"type":"module"}',
  'src/runtime/network-contract.js': 'export const NETWORK_CONTRACT=' + JSON.stringify(CONTRACT) + ';',
  'src/runtime/network-entity-player.js': 'export const OWN_PLAYER_SCHEMA=8;',
  'src/runtime/world-state.js': 'export function worldState(){return {}}',
  'src/runtime/content-hash.js': 'export async function contentHash(){return "fixture"}',
  'src/game.js': `export async function createGame(){return {playerUnit:{generation:1},worldClock:{tick:0},battle:{finished:false},units:{all:()=>[]}}}`,
  'src/runtime/multiplayer-world.js': `import { parentPort, workerData } from 'node:worker_threads';
const fake=workerData.fake??{};
const log=(event,id)=>parentPort.postMessage({type:'fake',event,id});
export function createMultiplayerWorld(game){
  const players=new Map();
  return { players,
    async addPlayer(id){ log('addPlayer',id); await new Promise(r=>setTimeout(r,fake.addMs??0)); const existing=players.get(id); if(existing){existing.disconnectedAt=null;return;} players.set(id,{id,character:{name:'P'},unit:{generation:1}}); log('added',id); },
    removePlayer(id){ const p=players.get(id); log('removePlayer',id); if(p)p.disconnectedAt=game.worldClock.tick+1; },
    player(id){ return players.has(id)?{unitId:1,generation:1}:null; },
    step(){ const end=performance.now()+(fake.stepMs??0); while(performance.now()<end); return ++game.worldClock.tick; },
  };
}`,
  'src/runtime/snapshot-host.js': `import { parentPort, workerData } from 'node:worker_threads';
const fake=workerData.fake??{};
const log=(event,id,extra)=>parentPort.postMessage({type:'fake',event,id,...extra});
export function createSnapshotHost({game,stateBytesPerSecond}){
  log('host',null,{stateBytesPerSecond});
  const clients=new Set();
  const host={ descriptor:()=>({}), addClient(id){clients.add(id);log('addClient',id);}, removeClient(id){clients.delete(id);log('removeClient',id);},
    setReady(){}, setStarted(){}, advanceEdges:()=>[], consumedEdges:()=>[], input(){},
    publish(){ log('publish',null,{tick:game.worldClock.tick}); return {shared:[],ownPackets:[],inputAcks:[],edgeResults:[],reliableEvents:[],diagnostics:[],faults:[]}; } };
  if(fake.staleClients)host.staleClients=seconds=>[...clients].filter(id=>fake.staleClients.includes(id)&&seconds===30);
  return host;
}`,
};
async function startWorld(t, fake) {
  const root = await mkdtemp(join(tmpdir(), 'bfe-world-worker-'));
  await mkdir(join(root, 'src/runtime'), { recursive: true });
  await Promise.all(Object.entries(modules).map(([name, body]) => writeFile(join(root, name), body)));
  const worker = new Worker(new URL('../src/world-worker.js', import.meta.url), { workerData: { engine: root, data: root, stateBytesPerSecond: 65000, fake } });
  const messages = [];
  worker.on('message', m => messages.push(m));
  t.after(async () => { await worker.terminate(); await rm(root, { recursive: true, force: true }); });
  await waitFor(() => messages.some(m => m.type === 'ready' || m.type === 'error'));
  assert.ok(messages.some(m => m.type === 'ready'), JSON.stringify(messages.find(m => m.type === 'error')));
  const events = (event, id) => messages.filter(m => m.type === 'fake' && m.event === event && (id === undefined || m.id === id));
  return { worker, messages, events };
}
const waitFor = async (condition, ms = 5000) => { const end = Date.now() + ms; while (!condition()) { if (Date.now() > end) throw Error('timed out'); await new Promise(r => setTimeout(r, 5)); } };
const sleep = ms => new Promise(r => setTimeout(r, ms));

test('a leave while the world is still adding the player cancels the join', async t => {
  const { worker, messages, events } = await startWorld(t, { addMs: 80 });
  worker.postMessage({ type: 'join', client: 'ghost', name: 'Ghost' });
  await waitFor(() => events('addPlayer', 'ghost').length);
  worker.postMessage({ type: 'leave', client: 'ghost' });
  await waitFor(() => events('added', 'ghost').length);
  await waitFor(() => events('removePlayer', 'ghost').length);
  await sleep(50);
  assert.equal(events('addClient', 'ghost').length, 0, 'no snapshot recipient for a session that is gone');
  assert.ok(!messages.some(m => m.type === 'joined' || m.type === 'entityRegistry'), 'no welcome for a cancelled join');
  assert.ok(!messages.some(m => m.type === 'reply' && m.error), 'a cancelled join is not an error');
  worker.postMessage({ type: 'call', client: 'ghost', id: 1, method: 'init' });
  await waitFor(() => messages.some(m => m.type === 'reply' && m.id === 1));
  assert.equal(messages.find(m => m.type === 'reply' && m.id === 1).error, 'not joined');
});

test('a reconnect (join, leave, join) waits for the cancelled join and then joins once', async t => {
  const { worker, messages, events } = await startWorld(t, { addMs: 60 });
  worker.postMessage({ type: 'join', client: 'p' });
  worker.postMessage({ type: 'leave', client: 'p' });
  worker.postMessage({ type: 'join', client: 'p' });
  await waitFor(() => messages.some(m => m.type === 'joined'));
  await sleep(100);
  assert.equal(messages.filter(m => m.type === 'joined').length, 1);
  assert.equal(events('addClient', 'p').length, 1);
  assert.ok(!messages.some(m => m.type === 'reply' && m.error), JSON.stringify(messages.filter(m => m.type === 'reply')));
  // Leaving twice (an eviction, then the session's own leave) removes once.
  worker.postMessage({ type: 'leave', client: 'p' });
  worker.postMessage({ type: 'leave', client: 'p' });
  await sleep(50);
  assert.equal(events('removePlayer', 'p').length, 2, 'the cancelled join and the real leave each removed once');
});

test('the snapshot host gets the state budget and evicts clients it has not heard from', async t => {
  const { worker, messages, events } = await startWorld(t, { staleClients: ['ghost'] });
  worker.postMessage({ type: 'join', client: 'ghost' });
  worker.postMessage({ type: 'join', client: 'alive' });
  await waitFor(() => messages.filter(m => m.type === 'joined').length === 2);
  assert.equal(events('host')[0].stateBytesPerSecond, 65000);
  worker.postMessage({ type: 'start' });
  await waitFor(() => messages.some(m => m.type === 'clientEvicted'), 3000);
  const evicted = messages.filter(m => m.type === 'clientEvicted');
  assert.deepEqual(evicted.map(m => m.client), ['ghost']);
  assert.equal(events('removeClient', 'ghost').length, 1);
  assert.equal(events('removePlayer', 'ghost').length, 1);
  assert.equal(events('removeClient', 'alive').length, 0);
  // The network thread restarting drops everyone through the same path.
  worker.postMessage({ type: 'dropClients' });
  await waitFor(() => events('removeClient', 'alive').length === 1);
  worker.postMessage({ type: 'stop' });
});

test('a catch-up batch publishes once, and the tick reaches the main thread about once a second', async t => {
  // Steps cost 30 ms against a 16.7 ms budget: the loop falls behind and
  // runs batches of several steps.
  const { worker, messages, events } = await startWorld(t, { stepMs: 30, staleClients: [] });
  worker.postMessage({ type: 'join', client: 'p' });
  await waitFor(() => messages.some(m => m.type === 'joined'));
  worker.postMessage({ type: 'start' });
  await sleep(700);
  worker.postMessage({ type: 'stop' });
  await sleep(50);
  const publishes = events('publish').length, ticks = Math.max(...events('publish').map(m => m.tick));
  assert.ok(ticks >= 12, `the world kept stepping (${ticks})`);
  assert.ok(publishes <= ticks / 2, `${publishes} publications for ${ticks} ticks`);
  assert.ok(messages.filter(m => m.type === 'tick').length <= 2, 'tick reports are throttled');
});

test('with an engine that captures snapshots on even ticks only, a batch also publishes its last even tick', async t => {
  const { worker, messages, events } = await startWorld(t, { stepMs: 30 });
  worker.postMessage({ type: 'join', client: 'p' });
  await waitFor(() => messages.some(m => m.type === 'joined'));
  worker.postMessage({ type: 'start' });
  await sleep(700);
  worker.postMessage({ type: 'stop' });
  await sleep(50);
  const ticks = events('publish').map(m => m.tick), evens = ticks.filter(tick => tick % 2 === 0);
  assert.ok(ticks.length < ticks.at(-1) * 0.8, `fewer publications than ticks (${ticks.length} for ${ticks.at(-1)})`);
  for (let i = 1; i < evens.length; i++) assert.ok(evens[i] - evens[i - 1] <= 4, `no snapshot gap past a batch: ${evens.join(',')}`);
});
