import { CONTRACT } from './contract-fixture.js';
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Worker} from 'node:worker_threads';
import {once} from 'node:events';

for(const scenario of ['missing module','old correction schema'])test(`mandatory engine readiness rejects ${scenario}`,{timeout:5000},async()=>{
 const root=await mkdtemp(join(tmpdir(),'bfe-engine-contract-'));
 let worker;
 try{
  await mkdir(join(root,'src/runtime'),{recursive:true});
  const modules={
   'package.json':'{"type":"module"}',
   'src/runtime/network-contract.js':'export const NETWORK_CONTRACT='+JSON.stringify(CONTRACT)+';',
   'src/game.js':'export async function createGame(){return {}}',
   'src/runtime/multiplayer-world.js':'export function createMultiplayerWorld(){return {}}',
   'src/runtime/world-state.js':'export function worldState(){}',
   'src/runtime/shared-packet.js':'export function compactMetadata(){}',
   'src/runtime/content-hash.js':'export async function contentHash(){return "fixture"}'
  };
  if(scenario==='old correction schema'){
   for(const name of ['capture','registry','codec','stream','input','edges','shots','combat','summary'])modules[`src/runtime/network-entity-${name}.js`]='export {}';
   modules['src/runtime/network-entity-player.js']='export const OWN_PLAYER_SCHEMA=7;';
  }
  await Promise.all(Object.entries(modules).map(([name,body])=>writeFile(join(root,name),body)));
  worker=new Worker(new URL('../src/world-worker.js',import.meta.url),{workerData:{engine:root,data:root}});
  const [message]=await once(worker,'message');
  assert.equal(message.type,'error');
  assert.match(message.message,/Mandatory multiplayer engine runtime unavailable/);
  assert.match(message.message,scenario==='missing module'?/network-entity-capture/:/requires own-player correction schema 8/);
 }finally{await worker?.terminate();await rm(root,{recursive:true,force:true});}
});

test('a newer correction schema is accepted when the engine manifest and implementation agree',async()=>{
 const root=await mkdtemp(join(tmpdir(),'bfe-engine-contract-next-'));let worker;
 try{
  await mkdir(join(root,'src/runtime'),{recursive:true});
  const modules={
   'package.json':'{"type":"module"}',
   'src/game.js':'export async function createGame(){return {playerUnit:{generation:1}}}',
   'src/runtime/network-contract.js':'export const NETWORK_CONTRACT='+JSON.stringify({...CONTRACT,simulation:'bfe-sim-999',ownCorrectionSchema:9}),
   'src/runtime/multiplayer-world.js':'export function createMultiplayerWorld(){return {}}',
   'src/runtime/world-state.js':'export function worldState(){}',
   'src/runtime/shared-packet.js':'export function compactMetadata(){}',
   'src/runtime/content-hash.js':'export async function contentHash(){return "fixture"}',
   'src/runtime/network-entity-player.js':'export const OWN_PLAYER_SCHEMA=9;',
   'src/runtime/entity-host.js':'export function createEntityHost(){}'
  };
  for(const name of ['capture','registry','codec','stream','input','edges','shots','combat','summary'])modules[`src/runtime/network-entity-${name}.js`]='export {}';
  await Promise.all(Object.entries(modules).map(([name,body])=>writeFile(join(root,name),body)));
  worker=new Worker(new URL('../src/world-worker.js',import.meta.url),{workerData:{engine:root,data:root}});
  const [message]=await once(worker,'message');assert.equal(message.type,'ready');assert.equal(message.networkContract.simulation,'bfe-sim-999');assert.equal(message.networkContract.ownCorrectionSchema,9);
 }finally{await worker?.terminate();await rm(root,{recursive:true,force:true});}
});
