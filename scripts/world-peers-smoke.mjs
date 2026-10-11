import { Worker } from 'node:worker_threads';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { once } from 'node:events';
import assert from 'node:assert/strict';
import rtc from 'node-datachannel';
import { Peer } from '../src/peer.js';
import { loadIdentity } from '../src/identity.js';
import { OnlineServer } from '../src/online-server.js';
import { MasterClient } from '../src/master-client.js';
import { GameSession } from '../src/game-session.js';
const directory = await mkdtemp(join(tmpdir(),'bfe-world-peers-'));
const query = '?map=nab2&side=rep&ai=classic&seed=1';
const worker = new Worker(new URL('../src/world-worker.js',import.meta.url),{workerData:{engine:resolve(process.env.BFE_ENGINE_DIR ?? '../battlefront-eternal'),data:resolve(process.env.BFE_DATA_DIR ?? '../battlefront-eternal/public'),query}});
const peers=[]; const masters=[]; let online; const relay=process.argv.includes('--relay'); const timer=setTimeout(()=>{console.error('world peer test timed out');process.exit(1)},120000);
function message(source,predicate) { return new Promise((resolve,reject)=>{const listener=m=>{if(m.type==='error'){source.off('message',listener);reject(Error(m.message));}else if(predicate(m)){source.off('message',listener);resolve(m);}};source.on('message',listener);}); }
try {
  const {content}=await message(worker,m=>m.type==='ready');
  if(relay) { online=new OnlineServer({config:{name:'Temporary relay gameplay acceptance',master:'https://online.battlefronteternal.com',onlineNoticeAccepted:true,relayPolicy:'always',rotation:['swbf1/nab2:rep:conquest'],maxPlayers:4,content},directory,world:worker,query}); online.on('fault',e=>console.error('online:',e)); await online.start(); }
  const serverKeys=loadIdentity(join(directory,'server'));
  const clients=[];
  for(let i=0;i<2;i++) {
    const keys=loadIdentity(join(directory,'client'+i));
    let master;
    if(relay) {master=new MasterClient({url:'https://online.battlefronteternal.com',noticeAccepted:true,keys});masters.push(master);await master.connect();}
    const iceServers=relay?await master.credentials(online.id):[];
    const a=new Peer({name:'client'+i,keys,initiator:true,simulation:'bfe-sim-1',content,iceServers,relay,portable:relay,expectedKey:online?.id}); peers.push(a);
    const welcome=new Promise(r=>a.on('message',(_,m)=>{if(m.type==='welcome')r(m);}));
    if(relay) {a.on('signal',s=>master.signal(online.id,s));master.on('signal',m=>a.signal(JSON.parse(m.payload)));}
    else {const b=new Peer({name:'server'+i,keys:serverKeys,simulation:'bfe-sim-1',content});peers.push(b);a.on('signal',s=>b.signal(s));b.on('signal',s=>a.signal(s));b.once('ready',r=>new GameSession(b,worker,r.id,{query}));}
    await once(a,'ready'); const w=await welcome;
    const packets=[]; a.on('message',(_,m)=>{if(m.type==='ticks'||m.type==='packet')packets.push(m);});
    clients.push({peer:a,w,packets});
    const reply=new Promise(r=>a.on('message',(_,m)=>{if(m.id===1&&m.type==='reply')r(m);}));
    a.send('events',{type:'call',id:1,method:'start',args:{}}); await reply;
  }
  assert.notEqual(clients[0].w.playerId,clients[1].w.playerId);
  const chats=clients.map(()=>[]);
  clients.forEach((client,i)=>client.peer.on('message',(_,m)=>{if(m.type==='chat')chats[i].push(m);}));
  clients[0].peer.send('events',{type:'chat',text:'global acceptance',team:false});
  clients[0].peer.send('events',{type:'chat',text:'team acceptance',team:true});
  for(let wait=0;wait<50 && (chats[0].length<2 || chats[1].length<1);wait++)await new Promise(r=>setTimeout(r,100));
  assert.deepEqual(chats[0].map(m=>m.text),['global acceptance','team acceptance']);
  assert.deepEqual(chats[1].map(m=>m.text),['global acceptance']);
  let first, last;
  for(let tick=1;tick<=90;tick++) {
    for(const [i,c]of clients.entries())c.peer.send('state',{type:'input',command:{epoch:1,sequence:tick,tick,generation:c.w.generation,down:[i?'KeyS':'KeyW'],actions:[],look:[]}});
    await new Promise(r=>setTimeout(r,relay?60:2));
    const state=message(worker,m=>m.type==='state');worker.postMessage({type:'step',count:1});last=(await state).state;first ??= last;
  }
  for(const c of clients){const a=first.units.find(u=>u[0]===c.w.playerId),b=last.units.find(u=>u[0]===c.w.playerId);assert(Math.hypot(b[4]-a[4],b[6]-a[6])>1,'player moves independently');}
  assert(clients.every(c=>c.packets.length>10),'both peers receive authoritative packets');
  console.log(JSON.stringify({steadyBytesPerSecond:clients.map(c=>Math.round(c.packets.slice(1).reduce((sum,m)=>sum+JSON.stringify(m).length,0)/(89/60))),ok:true,paths:clients.map(c=>c.peer.path()),relay,players:clients.map(c=>c.w.playerId),packets:clients.map(c=>c.packets.length),ticks:90,wireBytes:clients.map(c=>c.packets.reduce((sum,m)=>sum+JSON.stringify(m).length,0))}));
} finally {clearTimeout(timer);for(const p of peers)p.close();for(const m of masters)m.close();await online?.close();await worker.terminate();rtc.cleanup();await rm(directory,{recursive:true,force:true});}
