import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { Worker } from 'node:worker_threads';
import { once } from 'node:events';
import { mkdtemp,mkdir,writeFile,rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { CONTRACT } from './contract-fixture.js';
import { validateNetworkContract,wireCompatibility } from '../src/network-contract.js';
import { startLocalServer } from '../src/local-server.js';
import { GameSession } from '../src/game-session.js';

test('contract validation rejects missing, unsupported and invalid manifests without defaults',()=>{
 for(const value of [undefined,null,{}, {...CONTRACT,manifestVersion:2},{...CONTRACT,serverAdapterABI:1},{...CONTRACT,protocol:3},{...CONTRACT,profile:'legacy'}, {...CONTRACT,simulation:'test'},{...CONTRACT,ownCorrectionSchema:0},{...CONTRACT,ownCorrectionSchema:1.5}])assert.throws(()=>validateNetworkContract(value));
 const next=validateNetworkContract({...CONTRACT,simulation:'bfe-sim-999',ownCorrectionSchema:9});
 assert.equal(next.simulation,'bfe-sim-999');assert.equal(next.ownCorrectionSchema,9);assert.ok(Object.isFrozen(next));
});

for(const [name,value]of [['missing',undefined],['invalid',{...CONTRACT,simulation:'bad'}],['ABI mismatch',{...CONTRACT,serverAdapterABI:1}]])test(`worker rejects ${name} manifest before creating gameplay or becoming ready`,async()=>{
 const engine=await mkdtemp(join(tmpdir(),'bfe-manifest-test-'));let worker;
 try{
  await mkdir(join(engine,'src/runtime'),{recursive:true});await writeFile(join(engine,'package.json'),'{"type":"module"}');
  await writeFile(join(engine,'src/game.js'),'throw Error("GAMEPLAY MUST NOT LOAD")');
  if(value)await writeFile(join(engine,'src/runtime/network-contract.js'),'export const NETWORK_CONTRACT='+JSON.stringify(value));
  worker=new Worker(new URL('../src/world-worker.js',import.meta.url),{workerData:{engine,data:engine}});
  const [message]=await once(worker,'message');assert.equal(message.type,'error');assert.match(message.message,/Mandatory multiplayer engine contract unavailable/);assert.doesNotMatch(message.message,/GAMEPLAY MUST NOT LOAD/);
 }finally{await worker?.terminate();await rm(engine,{recursive:true,force:true});}
});

test('LAN advertisement follows the mounted engine and cannot start without a validated contract',async()=>{
 const args={config:{name:'contract',gamePort:0,discovery:false},identity:{id:'a'.repeat(64)},certificate:{fingerprint:'test'},world:new EventEmitter(),query:'?map=nab2'};
 await assert.rejects(startLocalServer(args),/Missing engine network contract/);
 args.world.networkContract=validateNetworkContract({...CONTRACT,simulation:'bfe-sim-999'});
 const server=await startLocalServer(args);
 try {for(const [key,value]of Object.entries(wireCompatibility(args.world.networkContract)))assert.equal(server.info()[key],value);}
 finally{await server.close();}
});

test('session rejects a peer advertising an older contract even when transport and own schema match',()=>{
 const world=Object.assign(new EventEmitter(),{networkContract:{...CONTRACT,simulation:'bfe-sim-999'}});
 const peer={remote:wireCompatibility(CONTRACT),sendBinary(){}};
 assert.throws(()=>new GameSession(peer,world,'id',{}),/Update required/);
});
