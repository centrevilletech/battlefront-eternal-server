import { CONTRACT } from './contract-fixture.js';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, rm } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

test('finished matches replace the world and advance the configured playlist', {timeout:15000}, async () => {
  const root = await mkdtemp(join(tmpdir(),'bfe-rotation-'));
  const engine=join(root,'engine'), data=join(root,'data'), state=join(root,'state'), log=join(root,'matches');
  await Promise.all([mkdir(join(engine,'src/runtime'),{recursive:true}),mkdir(data),mkdir(state)]);
  await writeFile(join(engine,'package.json'),' {"type":"module"}');
  await writeFile(join(engine,'src/game.js'),`import {appendFileSync} from 'node:fs';export async function createGame({query}){appendFileSync(${JSON.stringify(log)},query+'\\n');return {playerUnit:{generation:1},worldClock:{tick:0},battle:{finished:false}}}`);
  await writeFile(join(engine,'src/runtime/content-hash.js'),"export async function contentHash(){return 'fixture-content'}");
  await writeFile(join(engine,'src/runtime/world-state.js'),"export function worldState(){return {}}");
  await writeFile(join(engine,'src/runtime/multiplayer-world.js'),"export function createMultiplayerWorld(game){return {step(){if(++game.worldClock.tick===12)game.battle.finished=true;return game.worldClock.tick},snapshot(){}}}");
  // Rotation exercises no players, but the mandatory engine API must import.
  await writeFile(join(engine,'src/runtime/network-entity-player.js'),'export const OWN_PLAYER_SCHEMA=8;');
  await writeFile(join(engine,'src/runtime/snapshot-host.js'),'export function createSnapshotHost(){}');
  await writeFile(join(engine,'src/runtime/network-contract.js'),'export const NETWORK_CONTRACT='+JSON.stringify(CONTRACT)+';');
  // The engine's native UDP transport, reduced to the endpoint shape bfeserver binds.
  await mkdir(join(engine,'src/net'),{recursive:true});
  await writeFile(join(engine,'src/net/endpoint.js'),"import {EventEmitter} from 'node:events';export class UdpEndpoint extends EventEmitter{async listen(){return {port:40000,families:['IPv4']}}stats(){return {}}close(){}}");
  await writeFile(join(engine,'src/net/crypto.js'),'export const identityKeys=keys=>keys;');
  await writeFile(join(engine,'src/net/connection.js'),'export const CLOSE_CODE={NORMAL:0,REJECTED:4,VERSION:3,KICKED:5,SHUTDOWN:6};');
  await writeFile(join(state,'config.json'),JSON.stringify({engine,data,rotation:['swbf1/nab2:rep:conquest','swbf1/bes2:all:conquest'],gamePort:0,discovery:false,rotationDelayMs:300}));
  const child=spawn(process.execPath,['src/main.js','--serve'],{cwd:new URL('..',import.meta.url),env:{...process.env,BFE_STATE_DIR:state,PORT:'0'},stdio:['ignore','pipe','pipe']});
  let output=''; child.stdout.on('data',d=>output+=d);child.stderr.on('data',d=>output+=d);
  try {
    let matches=[];
    for(let n=0;n<100;n++){assert.equal(child.exitCode,null,output);try{matches=(await readFile(log,'utf8')).trim().split('\n')}catch{}if(matches.length>=3)break;await new Promise(r=>setTimeout(r,100));}
    assert(matches.length>=3,output);
    assert.match(matches[0],/map=nab2&side=rep/);
    assert.match(matches[1],/map=bes2&side=all/);
    assert.match(matches[2],/map=nab2&side=rep/);
    const tokens=matches.slice(0,3).map(query=>new URLSearchParams(query).get('match'));
    for(const token of tokens)assert.match(token,/^[a-f0-9]{32}$/);
    assert.equal(new Set(tokens).size,3,'returning to the same map still creates a distinct launch');
  } finally { if(child.exitCode===null&&child.signalCode===null){const exited=once(child,'exit');child.kill('SIGTERM');await exited;}await rm(root,{recursive:true,force:true}); }
});
