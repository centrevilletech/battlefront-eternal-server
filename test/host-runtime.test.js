import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { Worker } from 'node:worker_threads';
import { once } from 'node:events';
import { loadHostRuntime, validateHostManifest } from '../src/host-runtime.js';
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const identity = 'a'.repeat(64);
const wire = {manifestVersion:1,serverAdapterABI:4,protocol:3,profile:'snapshot-delta-v1',simulation:'bfe-sim-1',ownCorrectionSchema:11};
function manifest() {
  return { format: 1, wire, serverAdapterABI: 4, addonABI: 1, storeABI: 1, schema: identity, engineBuild: identity,
    serverRevision: 'b'.repeat(40), engineRevision: 'c'.repeat(40), worldBackend: 'wasm', targets: [], artifacts: {} };
}
async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), 'bfe-host-bundle-')), engine = join(root, 'engine');
  t.after(() => rm(root, { recursive: true, force: true }));
  const modules = {
    'engine/package.json': '{"type":"module","bfeHostBundle":1}',
    'engine/src/game.js': 'throw Error("GAMEPLAY MUST NOT LOAD");',
    'engine/src/sim-wasm.js': '',
    'engine/src/sim/host-schema.js': '',
    'engine/src/sim/host-runtime.js': 'export function hostSimulation(m){return ()=>m.engineBuild;}',
    'server/src/world-worker.js': await readFile(new URL('../src/world-worker.js', import.meta.url), 'utf8'),
    'server/src/host-runtime.js': await readFile(new URL('../src/host-runtime.js', import.meta.url), 'utf8'),
    'engine/src/runtime/network-contract.js': 'export const NETWORK_CONTRACT=' + JSON.stringify(wire),
  };
  const data = manifest();
  for (const [path, content] of Object.entries(modules)) {
    await mkdir(join(root, path, '..'), { recursive: true });
    await writeFile(join(root, path), content); data.artifacts[path] = sha(content);
  }
  const save = () => writeFile(join(engine, 'host-runtime.json'), JSON.stringify(data));
  await save();
  return { root, engine, data, save };
}

test('host ABI validation rejects incompatible versions and identities', () => {
  for (const field of ['format', 'serverAdapterABI', 'addonABI', 'storeABI', 'schema', 'engineBuild', 'serverRevision', 'engineRevision', 'worldBackend']) {
    assert.throws(() => validateHostManifest({ ...manifest(), [field]: 99 }), /host|Host/);
  }
});
test('unadvertised target uses the validated WASM factory', async t => {
  const { engine } = await fixture(t), host = await loadHostRuntime(engine);
  assert.equal(host.simulationFactory(), identity);
  assert.equal(host.manifest.worldBackend, 'wasm');
});
test('missing, corrupt, incomplete and escaping artifacts stop startup', async t => {
  for (const mode of ['missing manifest', 'corrupt artifact', 'missing artifact', 'incomplete manifest', 'escape']) {
    const f = await fixture(t);
    if (mode === 'missing manifest') await rm(join(f.engine, 'host-runtime.json'));
    if (mode === 'corrupt artifact') await writeFile(join(f.engine, 'src/game.js'), 'corrupt');
    if (mode === 'missing artifact') await rm(join(f.engine, 'src/game.js'));
    if (mode === 'incomplete manifest') { delete f.data.artifacts['engine/src/game.js']; await f.save(); }
    if (mode === 'escape') { f.data.artifacts['engine/../escape'] = identity; await f.save(); }
    await assert.rejects(loadHostRuntime(f.engine), /host|Host|ENOENT/);
  }
});
test('advertised addon mismatch and failed load never select WASM', async t => {
  for (const mode of ['missing', 'bad ABI', 'bad schema', 'bad build', 'native world']) {
    const f = await fixture(t), artifact = 'engine/fixture.cjs';
    const hello = { addonAbi: 1, storeAbi: 1, schema: identity, engineBuild: identity, napiMinimum: 8, nativeWorld: false, nativePublication: false };
    if (mode === 'bad ABI') hello.addonAbi++;
    if (mode === 'bad schema') hello.schema = 'd'.repeat(64);
    if (mode === 'bad build') hello.engineBuild = 'd'.repeat(64);
    if (mode === 'native world') hello.nativeWorld = true;
    const content = 'exports.handshake=()=>(' + JSON.stringify(hello) + ');';
    await writeFile(join(f.root, artifact), content);
    f.data.artifacts[artifact] = sha(content);
    f.data.targets = [{ target: 'linux-x64-gnu', artifact, napiMinimum: 8 }];
    if (mode === 'missing') await rm(join(f.root, artifact));
    await f.save();
    await assert.rejects(loadHostRuntime(f.engine), /handshake|ENOENT/);
  }
});
test('worker reports bundle ABI mismatch before importing gameplay', async t => {
  const f = await fixture(t);
  await mkdir(join(f.engine, 'src/runtime'), { recursive: true });
  const contract = {manifestVersion:1,serverAdapterABI:4,protocol:3,profile:'snapshot-delta-v1',simulation:'bfe-sim-1',ownCorrectionSchema:11};
  await writeFile(join(f.engine, 'src/runtime/network-contract.js'), 'export const NETWORK_CONTRACT=' + JSON.stringify(contract));
  f.data.addonABI++;
  await f.save();
  const worker = new Worker(new URL('../src/world-worker.js', import.meta.url), {workerData:{engine:f.engine,data:f.engine}});
  t.after(() => worker.terminate());
  const [message] = await once(worker, 'message');
  assert.equal(message.type, 'error');
  assert.match(message.message, /Host bundle ABI mismatch/);
  assert.doesNotMatch(message.message, /GAMEPLAY/);
});
