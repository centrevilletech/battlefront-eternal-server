import { CONTRACT } from './contract-fixture.js';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { GameSession, INPUT_BURST, INPUT_RATE_PER_SECOND } from '../src/game-session.js';
import { netMetrics } from '../src/net-metrics.js';
// World messages are drained in setImmediate slices, never in the port's handler.
const flush=async()=>{for(let i=0;i<5;i++)await new Promise(r=>setImmediate(r));};
test('entity registry and welcome establish the session without a replica bootstrap',async()=>{
 const {peer,world,session}=setup();const registry={classes:[[1,'rep_inf_trooper']]};
 try {
  world.emit('message',{type:'entityRegistry',client:'player',registry});
  world.emit('message',{type:'joined',client:'player',player:{unitId:7,generation:2,snapshotMode:'snapshot-delta-v1'},tick:30});
  assert.deepEqual(peer.sent,[],'the port handler only enqueues');await flush();
  assert.deepEqual(peer.sent,[{lane:'events',m:{type:'entity-registry',registry}},{lane:'events',m:{type:'welcome',unitId:7,generation:2,snapshotMode:'snapshot-delta-v1',playerId:7,tick:30}}]);
  const count=world.sent.length;
  peer.emit('message','state',{type:'snapshot-ack',epoch:1,tick:30});
  peer.emit('message','state',{type:'snapshot-resync'});
  assert.equal(world.sent.length,count,'retired snapshot traffic cannot drive the world');
 } finally {session.close();}
});

test('snapshot datagrams are unfragmented; a congested snapshot is skipped whole with no host round-trip',async()=>{
 const peer=new EventEmitter();peer.remote={protocol:3,simulation:'bfe-sim-5',profile:'snapshot-delta-v1'};peer.send=()=>true;peer.sendBinary=()=>true;peer.close=()=>peer.emit('closed');let buffered=40000,accept=true,sends=0;
 peer.channels=new Map([['state',{bufferedAmount:()=>buffered}]]);peer.sendStatePacket=(frames,options)=>{assert.equal(frames.length,1);assert.equal(options.maxBuffered,Number.MAX_SAFE_INTEGER);sends++;return accept;};
 const world=new EventEmitter();world.networkContract=CONTRACT;world.sent=[];world.postMessage=m=>world.sent.push(m);const session=new GameSession(peer,world,'player',{});
 try {
  assert(world.sent[0].entity);const buffer=Buffer.alloc(1100);buffer.writeUInt32LE(0x31534e42);
  const message=tick=>({type:'entityPacket',client:'player',epoch:1,tick,buffer});
  world.emit('message',message(2));await flush();assert.equal(sends,0,'backlog over 32 KB skips the snapshot');
  buffered=0;world.emit('message',message(2));await flush();assert.equal(sends,0,'the rest of a skipped snapshot is skipped too');
  accept=false;world.emit('message',message(4));await flush();assert.equal(sends,1,'a refused send drops one datagram');
  accept=true;world.emit('message',message(6));await flush();assert.equal(sends,2);
  assert.equal(world.sent.length,1,'nothing about state delivery is reported back to the host');
  assert.equal(session.transportDiagnostics().stateSkippedSnapshots,1);
 } finally {session.close();}
});

function setup(){const peer=new EventEmitter();peer.remote={protocol:3,simulation:'bfe-sim-5',profile:'snapshot-delta-v1'};peer.sent=[];peer.send=(lane,m)=>{peer.sent.push({lane,m});return true;};peer.sendBinary=()=>true;peer.close=()=>peer.emit('closed');const world=new EventEmitter();world.networkContract=CONTRACT;world.sent=[];world.postMessage=m=>world.sent.push(m);const session=new GameSession(peer,world,'player',{});return {peer,world,session};}
test('one world listener routes by client and recipients; closed sessions stop receiving',async()=>{
 const mkPeer=()=>{const peer=new EventEmitter();peer.remote={protocol:3,simulation:'bfe-sim-5',profile:'snapshot-delta-v1'};peer.sent=[];peer.send=(lane,m)=>{peer.sent.push({lane,m});return true;};peer.sendBinary=()=>true;peer.close=()=>peer.emit('closed');return peer;};
 const world=new EventEmitter();world.networkContract=CONTRACT;world.sent=[];world.postMessage=m=>world.sent.push(m);
 const pa=mkPeer(),pb=mkPeer();
 const a=new GameSession(pa,world,'alpha',{}),b=new GameSession(pb,world,'beta',{});
 try{
  assert.equal(world.listenerCount('message'),1,'sessions share one world listener, not one each');
  world.emit('message',{type:'entityRegistry',client:'alpha',registry:{marker:1}});await flush();
  assert.equal(pa.sent.length,1);assert.equal(pb.sent.length,0,'client-addressed messages reach only their session');
  world.emit('message',{type:'inputAck',recipients:[{client:'beta',epoch:1,generation:1,dropped:0,serverTick:9,ack:9,ackTick:9}]});await flush();
  assert.equal(pa.sent.length,1);assert.equal(pb.sent.length,1,'recipients batches merge and route per recipient');
  assert.equal(pb.sent[0].m.ack,9);
  world.emit('message',{type:'scores',players:[]});await flush();
  assert.equal(pa.sent.length+pb.sent.length,2,'unaddressed world messages reach no session');
  b.close();
  world.emit('message',{type:'entityRegistry',client:'beta',registry:{marker:2}});await flush();
  assert.equal(pb.sent.length,1,'a closed session receives nothing further');
 }finally{a.close();b.close();}
});
test('a snapshot publication fans shared parts out to every listed session, parts before own',async()=>{
 const mkPeer=()=>{const peer=new EventEmitter();peer.remote={protocol:3,simulation:'bfe-sim-5',profile:'snapshot-delta-v1'};peer.sent=[];peer.state=[];peer.send=(lane,m)=>{peer.sent.push({lane,m});return true;};peer.sendBinary=bytes=>{peer.state.push(Buffer.from(bytes).readUInt32LE(0));return true;};peer.close=()=>peer.emit('closed');return peer;};
 const datagram=magic=>{const bytes=new Uint8Array(100);new DataView(bytes.buffer).setUint32(0,magic,true);return bytes.buffer;};
 const world=new EventEmitter();world.networkContract=CONTRACT;world.sent=[];world.postMessage=m=>world.sent.push(m);
 const pa=mkPeer(),pb=mkPeer();
 const a=new GameSession(pa,world,'alpha',{}),b=new GameSession(pb,world,'beta',{});
 try{
  world.emit('message',{type:'snapshotPublication',
   shared:[{tick:10,capturedAtUnixMs:Date.now(),clients:['alpha','beta','ghost'],parts:[datagram(0x31534e42)]}],
   own:[{client:'alpha',tick:10,capturedAtUnixMs:Date.now(),buffer:datagram(0x314f4e42)}]});
  assert.deepEqual(pa.state,[],'fan-out yields the thread instead of sending synchronously');
  await flush();
  assert.deepEqual(pa.state,[0x31534e42,0x314f4e42],'shared part reaches alpha before its own datagram');
  assert.deepEqual(pb.state,[0x31534e42],'the same shared part reaches beta; an unknown client is ignored');
  const stale=netMetrics().stalePublications;
  world.emit('message',{type:'snapshotPublication',
   shared:[{tick:12,capturedAtUnixMs:Date.now()-1000,clients:['alpha','beta'],parts:[datagram(0x31534e42)]}],
   own:[{client:'beta',tick:12,capturedAtUnixMs:Date.now()-1000,buffer:datagram(0x314f4e42)}]});
  assert.equal(netMetrics().stalePublications,stale+1,'a stale publication is dropped whole at enqueue');
  await flush();
  assert.deepEqual(pa.state,[0x31534e42,0x314f4e42]);
  assert.deepEqual(pb.state,[0x31534e42]);
  // Fresh at enqueue but stale by the time it is sent: the capture-age
  // rule still skips it whole.
  world.emit('message',{type:'snapshotPublication',
   shared:[{tick:14,capturedAtUnixMs:Date.now()-1000,clients:['alpha'],parts:[datagram(0x31534e42)]}],own:[],publishedAt:performance.timeOrigin+performance.now()});
  await flush();
  assert.equal(pa.state.length,2);assert.equal(a.transportDiagnostics().stateSkippedSnapshots,1);
 }finally{a.close();b.close();}
});
test('a stale snapshot is skipped whole, never paced out late',()=>{
 const {world,session,peer}=setup();const buffer=Buffer.alloc(1000);buffer.writeUInt32LE(0x31534e42);let sends=0;
 peer.sendStatePacket=()=>{sends++;return true;};
 try{
  session.entityPacket({type:'entityPacket',client:'player',epoch:1,tick:4,capturedAtUnixMs:Date.now()-1000,buffer});
  assert.equal(sends,0);assert.equal(session.transportDiagnostics().stateSkippedSnapshots,1);
  session.entityPacket({type:'entityPacket',client:'player',epoch:1,tick:6,capturedAtUnixMs:Date.now(),buffer});
  assert.equal(sends,1,'the next fresh snapshot goes out at once');
 }finally{session.close();}
});
test('legacy peers cannot construct a session',()=>{assert.throws(()=>new GameSession({remote:{protocol:1}},Object.assign(new EventEmitter(),{networkContract:CONTRACT}),'player',{}),/Update required/);});
test('fixed profile has no capability negotiation and rejects JSON gameplay',()=>{const {peer,world,session}=setup();try{assert.deepEqual(world.sent[0],{type:'join',client:'player',spectator:undefined,name:'Player',entity:true});peer.emit('message','state',{type:'inputs',commands:[]});assert(session.closed);}finally{session.close();}});
test('compact input and edge ACKs bind authenticated identity',async()=>{const {peer,world,session}=setup();try{const b=Buffer.alloc(16);b.writeUInt32LE(0x31494e42);peer.emit('binary',b,'state');await flush();assert.equal(world.sent.at(-1).type,'entityInputs');assert.equal(world.sent.at(-1).inputs[0].client,'player');peer.emit('message','state',{type:'entity-edge-ack',epoch:1,generation:3,ids:[9],client:'victim'});assert.deepEqual(world.sent.at(-1),{type:'entityEdgeAck',client:'player',epoch:1,generation:3,ids:[9]});}finally{session.close();}});
test('chat and inspection control remain bounded and identity scoped',()=>{const {peer,world,session}=setup();try{for(let i=0;i<6;i++)peer.emit('message','events',{type:'chat',text:'hello',client:'victim',sender:'Admin'});const messages=world.sent.filter(m=>m.type==='chat');assert.equal(messages.length,4);assert(messages.every(m=>m.client==='player'&&!m.sender));peer.emit('message','events',{type:'call',id:3,method:'stop'});assert.equal(peer.sent.at(-1).m.error,'Unsupported operation');}finally{session.close();}});
test('independent ACK preserves consumed and discarded watermarks separately',async()=>{const {peer,world,session}=setup();try{world.emit('message',{type:'inputAck',client:'player',ack:7,ackTick:20,discardedInput:9,discardedActions:11});await flush();assert.equal(peer.sent.at(-1).lane,'state');assert.equal(peer.sent.at(-1).m.ack,7);assert.equal(peer.sent.at(-1).m.discardedInput,9);}finally{session.close();}});
test('input timing heartbeat keeps latest progress at 20Hz and immediately reports life/health changes',()=>{
 const {peer,world,session}=setup();try{
  for(let serverTick=1;serverTick<=60;serverTick++)session.inputAck({type:'inputAck',client:'player',epoch:1,generation:1,dropped:0,serverTick,ack:serverTick,ackTick:serverTick});
  assert.equal(peer.sent.length,20);assert.equal(peer.sent.at(-1).m.ack,58);assert.equal(peer.sent.at(-1).m.client,undefined);
  session.inputAck({type:'inputAck',client:'player',epoch:1,generation:2,dropped:0,serverTick:59,ack:59,ackTick:59});assert.equal(peer.sent.length,21);
  // A growing dropped-tick total (the server behind) waits for the heartbeat.
  session.inputAck({type:'inputAck',client:'player',epoch:1,generation:2,dropped:1,serverTick:60,ack:60,ackTick:60});assert.equal(peer.sent.length,21);
  session.inputAck({type:'inputAck',client:'player',epoch:1,generation:2,dropped:2,serverTick:62,ack:62,ackTick:62});assert.equal(peer.sent.length,22);assert.equal(peer.sent.at(-1).m.dropped,2);
 }finally{session.close();}
});
test('new edge receipts and execution outcomes bypass identical-feedback retry throttle',async()=>{
 const {peer,world,session}=setup();try{
  const message={type:'entityEdgeResults',client:'player',epoch:1,generation:1,received:[1],results:[]};
  world.emit('message',message);await flush();world.emit('message',message);await flush();assert.equal(peer.sent.length,1);
  world.emit('message',{...message,results:[{id:1,status:'consumed',tick:5}]});await flush();assert.equal(peer.sent.length,2);
  session.lastEdgeFeedback.at-=51;world.emit('message',{...message,results:[{id:1,status:'consumed',tick:5}]});await flush();assert.equal(peer.sent.length,3,'lost outcome/ACK must be retried');
 }finally{session.close();}
});
test('a token bucket drops input over its rate and never closes on rate alone',async()=>{
 const {peer,world,session}=setup();const oldWarn=console.warn,logs=[];console.warn=(...args)=>logs.push(args);
 try{
  const b=Buffer.alloc(16);b.writeUInt32LE(0x31494e42);
  // A queue-drain burst far past the bucket: the burst is admitted, the
  // excess dropped, and the peer stays.
  for(let i=0;i<INPUT_BURST+200;i++)peer.emit('binary',b,'state');
  await flush();
  const admitted=()=>world.sent.filter(m=>m.type==='entityInputs').reduce((n,m)=>n+m.inputs.length,0);
  assert(!session.closed,'a burst never closes the peer');
  assert(admitted()>=INPUT_BURST&&admitted()<INPUT_BURST+5,`about the burst is admitted (${admitted()})`);
  assert(session.transportDiagnostics().inputsDropped>=195);
  assert.equal(logs.filter(l=>l[0]==='entity-input-rate').length,1,'drops are logged once, not per datagram');
  // Sustained far over the rate for longer than a window: still no kick,
  // and admission follows the refill rate.
  const before=admitted();session.inputRefilledAt-=1000;
  for(let i=0;i<500;i++)peer.emit('binary',b,'state');
  await flush();
  assert(!session.closed);
  const refilled=admitted()-before;assert(refilled>=INPUT_RATE_PER_SECOND&&refilled<=INPUT_RATE_PER_SECOND+5,`one second refills the rate (${refilled})`);
 }finally{console.warn=oldWarn;session.close();}
});
test('inputs cross to the world batched, as exact-size transferred buffers',async()=>{
 const {peer,world,session}=setup();const posted=[];world.postMessage=(m,transfer)=>{world.sent.push(m);posted.push({m,transfer});};
 try{
  // A 16-byte input viewed in a large shared buffer (the transport's
  // datagram or Buffer pool) must not drag its backing store along.
  const pool=Buffer.alloc(8192);pool.writeUInt32LE(0x31494e42,100);const input=pool.subarray(100,116);
  const ack=Buffer.alloc(10);ack.writeUInt32LE(0x31414e42);
  peer.emit('binary',input,'state');peer.emit('binary',ack,'state');
  assert.equal(posted.length,0,'nothing crosses until the pass ends');
  await flush();
  const batch=posted.find(p=>p.m.type==='entityInputs');
  assert.equal(batch.m.inputs.length,2,'one message per pass');
  assert.deepEqual(batch.m.inputs.map(i=>i.buffer.byteLength),[16,10]);
  assert(batch.m.inputs.every(i=>i.buffer instanceof ArrayBuffer&&batch.transfer.includes(i.buffer)),'each buffer is in the transfer list');
  assert.equal(new DataView(batch.m.inputs[0].buffer).getUint32(0,true),0x31494e42);
  // Anything else the session posts goes after the inputs it queued.
  peer.emit('binary',input,'state');peer.emit('message','events',{type:'chat',text:'hi'});
  assert.deepEqual(posted.slice(-2).map(p=>p.m.type),['entityInputs','chat']);
 }finally{session.close();}
});
test('a closed UDP peer refuses state without escaping the worker listener',async()=>{
 const {UdpPeer}=await import('../src/udp-peer.js');const {world}=setup();
 const transport=Object.assign(new EventEmitter(),{id:'player',hello:{protocol:3,simulation:'bfe-sim-5',profile:'snapshot-delta-v1',content:'c'},rtt:5,connection:{path:null},
  send(){return true;},close(code,reason){this.emit('close',{code,reason,remote:false});},stats:()=>({})});
 const peer=new UdpPeer(transport,{contract:CONTRACT,content:'c',closeCodes:{}});const session=new GameSession(peer,world,'udp-player',{});
 transport.emit('close',{code:1,reason:'idle timeout',remote:false});
 const bytes=Buffer.alloc(30);bytes.writeUInt32LE(0x31534e42);
 assert.doesNotThrow(()=>world.emit('message',{type:'entityPacket',client:'udp-player',epoch:1,tick:5,buffer:bytes}));await flush();
 assert(peer.closed);assert(session.closed);assert(world.sent.some(m=>m.type==='leave'));
});

test('a peer\'s offered name is cleaned and bounded before it reaches the roster',async()=>{
 const {playerName}=await import('../src/game-session.js');
 assert.equal(playerName(undefined),'Player');assert.equal(playerName('  Josh\x00 Lambert  '),'Josh Lambert');assert.equal(playerName('x'.repeat(50)).length,32);assert.equal(playerName('\t\n'),'Player');
 const peer=new EventEmitter();peer.remote={protocol:3,simulation:'bfe-sim-5',profile:'snapshot-delta-v1',name:'  Josh  '};peer.send=()=>true;peer.sendBinary=()=>true;peer.close=()=>peer.emit('closed');
 const world=new EventEmitter();world.networkContract=CONTRACT;world.sent=[];world.postMessage=m=>world.sent.push(m);const session=new GameSession(peer,world,'player',{});
 try{assert.equal(world.sent[0].name,'Josh');}finally{session.close();}
});
test('each measured round trip reaches the world with the path, for the roster the players see',()=>{
 const {peer,world}=setup();peer.path=()=>({kind:'relay',family:'ipv6'});
 peer.emit('ping',42);const link=world.sent.find(m=>m.type==='playerLink');
 assert.deepEqual(link,{type:'playerLink',client:'player',rtt:42,path:{kind:'relay',family:'ipv6'}});
});
