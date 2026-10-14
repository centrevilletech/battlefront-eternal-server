import { CONTRACT } from './contract-fixture.js';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { GameSession } from '../src/game-session.js';
test('entity registry and welcome establish the session without a replica bootstrap',()=>{
 const {peer,world,session}=setup();const registry={classes:[[1,'rep_inf_trooper']]};
 try {
  world.emit('message',{type:'entityRegistry',client:'player',registry});
  world.emit('message',{type:'joined',client:'player',player:{unitId:7,generation:2,snapshotMode:'snapshot-delta-v1'},tick:30});
  assert.deepEqual(peer.sent,[{lane:'events',m:{type:'entity-registry',registry}},{lane:'events',m:{type:'welcome',unitId:7,generation:2,snapshotMode:'snapshot-delta-v1',playerId:7,tick:30}}]);
  const count=world.sent.length;
  peer.emit('message','state',{type:'snapshot-ack',epoch:1,tick:30});
  peer.emit('message','state',{type:'snapshot-resync'});
  assert.equal(world.sent.length,count,'retired snapshot traffic cannot drive the world');
 } finally {session.close();}
});

test('snapshot datagrams are unfragmented; a congested snapshot is skipped whole with no host round-trip',()=>{
 const peer=new EventEmitter();peer.remote={protocol:2,simulation:'bfe-sim-5',profile:'snapshot-delta-v1'};peer.send=()=>true;peer.sendBinary=()=>true;peer.close=()=>peer.emit('closed');let buffered=40000,accept=true,sends=0;
 peer.channels=new Map([['state',{bufferedAmount:()=>buffered}]]);peer.sendStatePacket=(frames,options)=>{assert.equal(frames.length,1);assert.equal(options.maxBuffered,Number.MAX_SAFE_INTEGER);sends++;return accept;};
 const world=new EventEmitter();world.networkContract=CONTRACT;world.sent=[];world.postMessage=m=>world.sent.push(m);const session=new GameSession(peer,world,'player',{});
 try {
  assert(world.sent[0].entity);const buffer=Buffer.alloc(1100);buffer.writeUInt32LE(0x31534e42);
  const message=tick=>({type:'entityPacket',client:'player',epoch:1,tick,buffer});
  world.emit('message',message(2));assert.equal(sends,0,'backlog over 32 KB skips the snapshot');
  buffered=0;world.emit('message',message(2));assert.equal(sends,0,'the rest of a skipped snapshot is skipped too');
  accept=false;world.emit('message',message(4));assert.equal(sends,1,'a refused send drops one datagram');
  accept=true;world.emit('message',message(6));assert.equal(sends,2);
  assert.equal(world.sent.length,1,'nothing about state delivery is reported back to the host');
  assert.equal(session.transportDiagnostics().stateSkippedSnapshots,1);
 } finally {session.close();}
});

function setup(){const peer=new EventEmitter();peer.remote={protocol:2,simulation:'bfe-sim-5',profile:'snapshot-delta-v1'};peer.sent=[];peer.send=(lane,m)=>{peer.sent.push({lane,m});return true;};peer.sendBinary=()=>true;peer.close=()=>peer.emit('closed');const world=new EventEmitter();world.networkContract=CONTRACT;world.sent=[];world.postMessage=m=>world.sent.push(m);const session=new GameSession(peer,world,'player',{});return {peer,world,session};}
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
test('compact input and edge ACKs bind authenticated identity',()=>{const {peer,world,session}=setup();try{const b=Buffer.alloc(16);b.writeUInt32LE(0x31494e42);peer.emit('binary',b,'state');assert.equal(world.sent.at(-1).type,'entityInput');assert.equal(world.sent.at(-1).client,'player');peer.emit('message','state',{type:'entity-edge-ack',epoch:1,generation:3,ids:[9],client:'victim'});assert.deepEqual(world.sent.at(-1),{type:'entityEdgeAck',client:'player',epoch:1,generation:3,ids:[9]});}finally{session.close();}});
test('chat and inspection control remain bounded and identity scoped',()=>{const {peer,world,session}=setup();try{for(let i=0;i<6;i++)peer.emit('message','events',{type:'chat',text:'hello',client:'victim',sender:'Admin'});const messages=world.sent.filter(m=>m.type==='chat');assert.equal(messages.length,4);assert(messages.every(m=>m.client==='player'&&!m.sender));peer.emit('message','events',{type:'call',id:3,method:'stop'});assert.equal(peer.sent.at(-1).m.error,'Unsupported operation');}finally{session.close();}});
test('independent ACK preserves consumed and discarded watermarks separately',()=>{const {peer,world,session}=setup();try{world.emit('message',{type:'inputAck',client:'player',ack:7,ackTick:20,discardedInput:9,discardedActions:11});assert.equal(peer.sent.at(-1).lane,'state');assert.equal(peer.sent.at(-1).m.ack,7);assert.equal(peer.sent.at(-1).m.discardedInput,9);}finally{session.close();}});
test('input timing heartbeat keeps latest progress at 20Hz and immediately reports life/health changes',()=>{
 const {peer,world,session}=setup();try{
  for(let serverTick=1;serverTick<=60;serverTick++)world.emit('message',{type:'inputAck',client:'player',epoch:1,generation:1,dropped:0,serverTick,ack:serverTick,ackTick:serverTick});
  assert.equal(peer.sent.length,20);assert.equal(peer.sent.at(-1).m.ack,58);assert.equal(peer.sent.at(-1).m.client,undefined);
  world.emit('message',{type:'inputAck',client:'player',epoch:1,generation:2,dropped:0,serverTick:59,ack:59,ackTick:59});assert.equal(peer.sent.length,21);
  world.emit('message',{type:'inputAck',client:'player',epoch:1,generation:2,dropped:1,serverTick:60,ack:60,ackTick:60});assert.equal(peer.sent.length,22);
 }finally{session.close();}
});
test('new edge receipts and execution outcomes bypass identical-feedback retry throttle',()=>{
 const {peer,world,session}=setup();try{
  const message={type:'entityEdgeResults',client:'player',epoch:1,generation:1,received:[1],results:[]};
  world.emit('message',message);world.emit('message',message);assert.equal(peer.sent.length,1);
  world.emit('message',{...message,results:[{id:1,status:'consumed',tick:5}]});assert.equal(peer.sent.length,2);
  session.lastEdgeFeedback.at-=51;world.emit('message',{...message,results:[{id:1,status:'consumed',tick:5}]});assert.equal(peer.sent.length,3,'lost outcome/ACK must be retried');
 }finally{session.close();}
});
test('input flood guard retains its bound and emits bounded receipt timing diagnostics',()=>{const {peer,world,session}=setup();const oldWarn=console.warn,logs=[];console.warn=(...args)=>logs.push(args);try{const b=Buffer.alloc(16);b.writeUInt32LE(0x31494e42);for(let i=0;i<121;i++)peer.emit('binary',b,'state');assert(session.closed);assert.equal(world.sent.filter(m=>m.type==='entityInput').length,120);const value=JSON.parse(logs[0][1]);assert.equal(value.count,121);assert.equal(value.receiptOffsetsMs.length,121);assert(value.windowMs<1000);}finally{console.warn=oldWarn;session.close();}});
test('a native state send close race cannot escape the worker listener or acknowledge delivery',async()=>{
 const {Peer}=await import('../src/peer.js');const {peer,world,session}=setup();
 Object.setPrototypeOf(peer,Peer.prototype);delete peer.close;peer.authenticated=true;peer.pc={close(){}};
 peer.channels=new Map([['state',{isOpen:()=>true,bufferedAmount:()=>0,sendMessageBinary(){throw Error('errno=104');},close(){}}]]);
 const bytes=Buffer.alloc(30);bytes.writeUInt32LE(0x31534e42);
 assert.doesNotThrow(()=>world.emit('message',{type:'entityPacket',client:'player',epoch:1,tick:5,buffer:bytes}));
 assert(peer.closed);assert(session.closed);assert(!world.sent.some(m=>m.type==='entitySent'));assert(world.sent.some(m=>m.type==='leave'));
});

test('a peer\'s offered name is cleaned and bounded before it reaches the roster',async()=>{
 const {playerName}=await import('../src/game-session.js');
 assert.equal(playerName(undefined),'Player');assert.equal(playerName('  Josh\x00 Lambert  '),'Josh Lambert');assert.equal(playerName('x'.repeat(50)).length,32);assert.equal(playerName('\t\n'),'Player');
 const peer=new EventEmitter();peer.remote={protocol:2,simulation:'bfe-sim-5',profile:'snapshot-delta-v1',name:'  Josh  '};peer.send=()=>true;peer.sendBinary=()=>true;peer.close=()=>peer.emit('closed');
 const world=new EventEmitter();world.networkContract=CONTRACT;world.sent=[];world.postMessage=m=>world.sent.push(m);const session=new GameSession(peer,world,'player',{});
 try{assert.equal(world.sent[0].name,'Josh');}finally{session.close();}
});
test('each measured round trip reaches the world with the path, for the roster the players see',()=>{
 const {peer,world}=setup();peer.path=()=>({kind:'relay',family:'ipv6'});
 peer.emit('ping',42);const link=world.sent.find(m=>m.type==='playerLink');
 assert.deepEqual(link,{type:'playerLink',client:'player',rtt:42,path:{kind:'relay',family:'ipv6'}});
});
