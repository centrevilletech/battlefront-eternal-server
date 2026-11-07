// Validate the packaged pair before importing gameplay or opening a listener.
// Source checkouts have no manifest; packaged engines always carry one.
import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import { loadNetworkContract, validateNetworkContract } from './network-contract.js';
import { pathToFileURL } from 'node:url';

export function validateHostManifest(manifest) {
  if (manifest.format !== 1 || manifest.serverAdapterABI !== 4 || manifest.addonABI !== 1 || manifest.storeABI !== 1) throw Error('Host bundle ABI mismatch');
  if (!/^[a-f0-9]{64}$/.test(manifest.schema) || !/^[a-f0-9]{64}$/.test(manifest.engineBuild)) throw Error('Invalid host build identity');
  if (!/^[a-f0-9]{40}$/.test(manifest.serverRevision) || !/^[a-f0-9]{40}$/.test(manifest.engineRevision)) throw Error('Invalid host revision');
  if (manifest.worldBackend !== 'wasm' || !Array.isArray(manifest.targets) || !manifest.artifacts || typeof manifest.artifacts !== 'object') throw Error('Invalid host capabilities');
  if (new Set(manifest.targets.map(target => target.target)).size !== manifest.targets.length) throw Error('Duplicate host target');
  validateNetworkContract(manifest.wire);
  return manifest;
}
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
export async function loadHostRuntime(engine) {
  let bytes;
  try { bytes = await readFile(join(engine, 'host-runtime.json')); }
  catch (error) {
    if (error.code !== 'ENOENT') throw error;
    const pkg = JSON.parse(await readFile(join(engine, 'package.json')));
    if (pkg.bfeHostBundle) throw Error('Missing host bundle manifest');
    return { simulationFactory: null, manifest: null };
  }
  const manifest = validateHostManifest(JSON.parse(bytes));
  const root = dirname(resolve(engine));
  // All executable payload files, not just the addon, are hashed. Relative
  // manifest names cannot escape the package or silently omit core entrypoints.
  for (const required of ['engine/src/game.js', 'engine/src/sim-wasm.js', 'engine/src/sim/host-runtime.js', 'engine/src/sim/host-schema.js', 'server/src/world-worker.js', 'server/src/host-runtime.js']) {
    if (!Object.hasOwn(manifest.artifacts, required)) throw Error('Missing host artifact: ' + required);
  }
  await Promise.all(Object.entries(manifest.artifacts).map(async ([path, hash]) => {
    if (!/^(engine|server|update)\/[a-zA-Z0-9_.\/-]+$/.test(path) || path.split('/').includes('..') || !/^[a-f0-9]{64}$/.test(hash)) throw Error('Invalid host artifact');
    if (sha256(await readFile(join(root, path))) !== hash) throw Error('Host artifact hash mismatch: ' + path);
  }));
  // Verify this running adapter too, rather than accepting a manifest for an
  // adjacent server tree while executing a different checkout's worker.
  for (const file of ['host-runtime.js', 'world-worker.js']) {
    if (sha256(await readFile(new URL('./' + file, import.meta.url))) !== manifest.artifacts['server/src/' + file]) throw Error('Running server does not match host bundle');
  }
  const wire = await loadNetworkContract(engine);
  if (JSON.stringify(wire) !== JSON.stringify(validateNetworkContract(manifest.wire))) throw Error('Host wire contract mismatch');
  const target = process.platform === 'linux' && process.arch === 'x64' && process.report.getReport().header.glibcVersionRuntime ? 'linux-x64-gnu' : `${process.platform}-${process.arch}`;
  const entry = manifest.targets.find(entry => entry.target === target);
  // An advertised addon must load and match. Its foundation capability does
  // not advertise a native world; there is no retry after a failed native load.
  if (entry) {
    if (!Object.hasOwn(manifest.artifacts, entry.artifact) || !Number.isInteger(entry.napiMinimum) || Number(process.versions.napi) < entry.napiMinimum) throw Error('Unsupported host addon runtime');
    const addon = createRequire(import.meta.url)(join(root, entry.artifact));
    const hello = addon.handshake();
    if (hello.addonAbi !== manifest.addonABI || hello.storeAbi !== manifest.storeABI || hello.schema !== manifest.schema || hello.engineBuild !== manifest.engineBuild || hello.napiMinimum !== entry.napiMinimum || hello.nativeWorld !== false || hello.nativePublication !== false) throw Error('Host addon handshake mismatch');
  }
  const { hostSimulation } = await import(pathToFileURL(join(engine, 'src/sim/host-runtime.js')));
  return { manifest, simulationFactory: hostSimulation(manifest) };
}
