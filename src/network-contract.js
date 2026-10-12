import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';

// ABI 1 is implemented by this server adapter, not negotiated with clients.
export function validateNetworkContract(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw Error('Missing engine network contract');
  if (value.manifestVersion !== 1) throw Error('Unsupported engine network manifest version');
  if (value.serverAdapterABI !== 1) throw Error('Unsupported engine server adapter ABI');
  if (value.protocol !== 2 || value.profile !== 'entity-state-v1') throw Error('Unsupported engine transport contract');
  if (typeof value.simulation !== 'string' || !/^bfe-sim-[1-9][0-9]{0,5}$/.test(value.simulation)) throw Error('Invalid engine simulation revision');
  if (!Number.isInteger(value.ownCorrectionSchema) || value.ownCorrectionSchema < 1 || value.ownCorrectionSchema > 65535) throw Error('Invalid engine own correction schema');
  return Object.freeze(Object.fromEntries(['manifestVersion','protocol','profile','simulation','serverAdapterABI','ownCorrectionSchema'].map(key=>[key,value[key]])));
}
export async function loadNetworkContract(engine) {
  try {
    const module=await import(pathToFileURL(resolve(engine,'src/runtime/network-contract.js')));
    return validateNetworkContract(module.NETWORK_CONTRACT);
  } catch (error) { throw new Error('Mandatory multiplayer engine contract unavailable: '+error.message,{cause:error}); }
}
export function wireCompatibility(contract) {
  const {protocol,profile,simulation}=validateNetworkContract(contract);
  return {protocol,profile,simulation};
}
