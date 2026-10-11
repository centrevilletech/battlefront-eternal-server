import { Worker } from 'node:worker_threads';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { once } from 'node:events';
import assert from 'node:assert/strict';
import rtc from 'node-datachannel';
import { Peer } from '../src/peer.js';
import { loadIdentity } from '../src/identity.js';
import { GameSession } from '../src/game-session.js';
const directory = await mkdtemp(join(tmpdir(),'bfe-world-peers-'));
const query = '?map=nab2&side=rep&ai=classic&seed=1';
const worker = new Worker(new URL('../src/world-worker.js',import.meta.url),{workerData:{engine:resolve(process.env.BFE_ENGINE_DIR ?? '../battlefront-eternal'),data:resolve(process.env.BFE_DATA_DIR ?? '../battlefront-eternal/public'),query}});
const peers=[]; const timer=setTimeout(()=>{console.error('world peer test timed out');process.exit(1)},120000);
function message(source,predicate) { return new Promise((resolve,reject)=>{const listener=m=>{if(m.type==='error'){source.off('message',listener);reject(Error(m.message));}else if(predicate(m)){source.off('message',listener);resolve(m);}};source.on('message',listener);}); }
try {
  await message(worker,m=>m.type==='ready');
  const serverKeys=loadIdentity(join(directory,'server'));
  const clients=[];
  for(let i=0;i<2;i++) {
    const a=new Peer({name:'client'+i,keys:loadIdentity(join(directory,'client'+i)),initiator:true,simulation:'bfe-sim-1',content:'bfe-content-1'});
    const b=new Peer({name:'server'+i,keys:serverKeys,simulation:'bfe-sim-1',content:'bfe-content-1'});peers.push(a,b);
    a.on('signal',s=>b.signal(s));b.on('signal',s=>a.signal(s));
    const welcome=new Promise(r=>a.on('message',(_,m)=>{if(m.type==='welcome')r(m);}));
    b.once('ready',r=>new GameSession(b,worker,r.id,{query}));
    await once(a,'ready'); const w=await welcome;
    const packets=[]; a.on('message',(_,m)=>{if(m.type==='ticks'||m.type==='packet')packets.push(m);});
    clients.push({peer:a,w,packets});
    const reply=new Promise(r=>a.on('message',(_,m)=>{if(m.id===1&&m.type==='reply')r(m);}));
    a.send('events',{type:'call',id:1,method:'start',args:{}}); await reply;
  }
  assert.notEqual(clients[0].w.playerId,clients[1].w.playerId);
  for(let tick=1;tick<=90;tick++) {
    for(const [i,c]of clients.entries())c.peer.send('state',{type:'input',command:{epoch:1,sequence:tick,tick,generation:c.w.generation,down:[i?'moveBackward':'moveForward'],actions:[],look:[]}});
    await new Promise(r=>setTimeout(r,2));
    const state=message(worker,m=>m.type==='state');worker.postMessage({type:'step',count:1});await state;
  }
  assert(clients.every(c=>c.packets.length>10),'both peers receive authoritative packets');
  console.log(JSON.stringify({ok:true,players:clients.map(c=>c.w.playerId),packets:clients.map(c=>c.packets.length),ticks:90}));
} finally {clearTimeout(timer);for(const p of peers)p.close();await worker.terminate();rtc.cleanup();await rm(directory,{recursive:true,force:true});}
