import { randomBytes } from 'node:crypto';
import { inflateSync } from 'node:zlib';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { decodeBulkFrame } from '../src/bulk-frame.js';
import { GameSession, compressionStats } from '../src/game-session.js';
test('mandatory entity initial join admits only after all reliable fragments and cannot repeat',async()=>{
 const peer=new EventEmitter();peer.remote={protocol:2,simulation:'bfe-sim-4',profile:'entity-state-v1'};peer.send=()=>true;
 let accepted=0,blocked=true;peer.sendBinary=()=>{if(blocked&&accepted===1)return false;accepted++;return true;};peer.close=()=>peer.emit('closed');
 const world=new EventEmitter();world.sent=[];world.postMessage=m=>world.sent.push(m);const session=new GameSession(peer,world,'player',{});
 try {
  const initial={type:'journalPacket',client:'player',reliable:true,initialBootstrap:true,snapshotEpoch:1,snapshotTick:3,buffer:randomBytes(50000),sharedId:'join'};
  world.emit('message',initial);
  for(let n=0;n<100&&session.draining;n++)await new Promise(r=>setTimeout(r,2));
  assert.equal(accepted,1);assert(!world.sent.some(m=>m.type==='snapshotSent'));
  blocked=false;await drained(session);
  assert.deepEqual(world.sent.at(-1),{type:'snapshotSent',client:'player',epoch:1,tick:3,initialBootstrap:true});
  const sent=accepted;world.emit('message',initial);await drained(session);assert.equal(accepted,sent);
  const recovery=[];session.unorderedSnapshot=m=>recovery.push(m);
  world.emit('message',{...initial,initialBootstrap:false,snapshotEpoch:2});assert.equal(recovery.length,0);
 } finally {session.close();}
});

test('semantic packets are unfragmented and only native admission authorizes exact membership ACK',()=>{
 const peer=new EventEmitter();peer.remote={protocol:2,simulation:'bfe-sim-4',profile:'entity-state-v1'};peer.send=()=>true;peer.sendBinary=()=>true;peer.close=()=>peer.emit('closed');let buffered=1,accept=true,sends=0;
 peer.channels=new Map([['state',{bufferedAmount:()=>buffered}]]);peer.sendStatePacket=(frames,options)=>{assert.equal(frames.length,1);assert.equal(options.maxBuffered,0);sends++;return accept;};
 const world=new EventEmitter();world.sent=[];world.postMessage=m=>world.sent.push(m);const session=new GameSession(peer,world,'player',{});
 try {
  assert(world.sent[0].entity);const buffer=Buffer.alloc(1100);buffer.writeUInt32LE(0x31454e42);const message={type:'entityPacket',client:'player',epoch:1,packetId:2,buffer};
  world.emit('message',message);assert.equal(sends,0);assert.equal(world.sent.length,1);
  buffered=0;accept=false;world.emit('message',message);assert.equal(world.sent.length,1);
  accept=true;world.emit('message',message);assert.deepEqual(world.sent.at(-1),{type:'entitySent',client:'player',epoch:1,packetId:2,ownCorrection:false,tick:0});
  peer.emit('message','state',{type:'entity-ack',epoch:1,packetId:2,client:'victim'});
  assert(world.sent.some(m=>m.type==='entityAck'&&m.client==='player'&&m.packetId===2));
  const n=world.sent.length;peer.emit('message','events',{type:'entity-ack',epoch:1,packetId:2});assert.equal(world.sent.length,n);
 } finally {session.close();}
});

async function drained(session){for(let n=0;n<400;n++){session.flush();if(!session.draining&&!session.pending.length&&!session.queue.length)return;await new Promise(r=>setTimeout(r,5));}throw Error('compression drain timed out');}
function setup(){const peer=new EventEmitter();peer.remote={protocol:2,simulation:'bfe-sim-4',profile:'entity-state-v1'};peer.sent=[];peer.send=(lane,m)=>{peer.sent.push({lane,m});return true;};peer.sendBinary=()=>true;peer.close=()=>peer.emit('closed');const world=new EventEmitter();world.sent=[];world.postMessage=m=>world.sent.push(m);const session=new GameSession(peer,world,'player',{});return {peer,world,session};}
test('legacy peers cannot construct a session',()=>{assert.throws(()=>new GameSession({remote:{protocol:1}},new EventEmitter(),'player',{}),/Update required/);});
test('fixed profile has no capability negotiation and rejects JSON gameplay',()=>{const {peer,world,session}=setup();try{assert.deepEqual(world.sent[0],{type:'join',client:'player',spectator:undefined,entity:true,journal:true,fresh:true,transientSnapshots:false,relativeSnapshots:false,linearProjectiles:true,separateActions:true,compactMetadata:true});peer.emit('message','state',{type:'inputs',commands:[]});assert(session.closed);}finally{session.close();}});
test('compact input and edge ACKs bind authenticated identity',()=>{const {peer,world,session}=setup();try{const b=Buffer.alloc(16);b.writeUInt32LE(0x31494e42);peer.emit('binary',b,'state');assert.equal(world.sent.at(-1).type,'entityInput');assert.equal(world.sent.at(-1).client,'player');peer.emit('message','state',{type:'entity-edge-ack',epoch:1,generation:3,ids:[9],client:'victim'});assert.deepEqual(world.sent.at(-1),{type:'entityEdgeAck',client:'player',epoch:1,generation:3,ids:[9]});}finally{session.close();}});
test('chat and inspection control remain bounded and identity scoped',()=>{const {peer,world,session}=setup();try{for(let i=0;i<6;i++)peer.emit('message','events',{type:'chat',text:'hello',client:'victim',sender:'Admin'});const messages=world.sent.filter(m=>m.type==='chat');assert.equal(messages.length,4);assert(messages.every(m=>m.client==='player'&&!m.sender));peer.emit('message','events',{type:'call',id:3,method:'stop'});assert.equal(peer.sent.at(-1).m.error,'Unsupported operation');}finally{session.close();}});
test('independent ACK preserves consumed and discarded watermarks separately',()=>{const {peer,world,session}=setup();try{world.emit('message',{type:'inputAck',client:'player',ack:7,ackTick:20,discardedInput:9,discardedActions:11});assert.equal(peer.sent.at(-1).lane,'state');assert.equal(peer.sent.at(-1).m.ack,7);assert.equal(peer.sent.at(-1).m.discardedInput,9);}finally{session.close();}});
test('input flood guard retains its bound and emits bounded receipt timing diagnostics',()=>{const {peer,world,session}=setup();const oldWarn=console.warn,logs=[];console.warn=(...args)=>logs.push(args);try{const b=Buffer.alloc(16);b.writeUInt32LE(0x31494e42);for(let i=0;i<121;i++)peer.emit('binary',b,'state');assert(session.closed);assert.equal(world.sent.filter(m=>m.type==='entityInput').length,120);const value=JSON.parse(logs[0][1]);assert.equal(value.count,121);assert.equal(value.receiptOffsetsMs.length,121);assert(value.windowMs<1000);}finally{console.warn=oldWarn;session.close();}});
