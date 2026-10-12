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
