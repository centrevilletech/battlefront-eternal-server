import { randomBytes } from 'node:crypto';
import { inflateSync } from 'node:zlib';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { decodeBulkFrame } from '../src/bulk-frame.js';
import { GameSession, compressionStats } from '../src/game-session.js';
test('mandatory entity initial join admits only after all reliable fragments and cannot repeat',async()=>{
 const peer=new EventEmitter();peer.remote={protocol:2,simulation:'bfe-sim-5',profile:'entity-state-v1'};peer.send=()=>true;
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
 const peer=new EventEmitter();peer.remote={protocol:2,simulation:'bfe-sim-5',profile:'entity-state-v1'};peer.send=()=>true;peer.sendBinary=()=>true;peer.close=()=>peer.emit('closed');let buffered=1,accept=true,sends=0;
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
function setup(){const peer=new EventEmitter();peer.remote={protocol:2,simulation:'bfe-sim-5',profile:'entity-state-v1'};peer.sent=[];peer.send=(lane,m)=>{peer.sent.push({lane,m});return true;};peer.sendBinary=()=>true;peer.close=()=>peer.emit('closed');const world=new EventEmitter();world.sent=[];world.postMessage=m=>world.sent.push(m);const session=new GameSession(peer,world,'player',{});return {peer,world,session};}
test('legacy peers cannot construct a session',()=>{assert.throws(()=>new GameSession({remote:{protocol:1}},new EventEmitter(),'player',{}),/Update required/);});
test('fixed profile has no capability negotiation and rejects JSON gameplay',()=>{const {peer,world,session}=setup();try{assert.deepEqual(world.sent[0],{type:'join',client:'player',spectator:undefined,entity:true,journal:true,fresh:true,transientSnapshots:false,relativeSnapshots:false,linearProjectiles:true,separateActions:true,compactMetadata:true});peer.emit('message','state',{type:'inputs',commands:[]});assert(session.closed);}finally{session.close();}});
test('compact input and edge ACKs bind authenticated identity',()=>{const {peer,world,session}=setup();try{const b=Buffer.alloc(16);b.writeUInt32LE(0x31494e42);peer.emit('binary',b,'state');assert.equal(world.sent.at(-1).type,'entityInput');assert.equal(world.sent.at(-1).client,'player');peer.emit('message','state',{type:'entity-edge-ack',epoch:1,generation:3,ids:[9],client:'victim'});assert.deepEqual(world.sent.at(-1),{type:'entityEdgeAck',client:'player',epoch:1,generation:3,ids:[9]});}finally{session.close();}});
test('chat and inspection control remain bounded and identity scoped',()=>{const {peer,world,session}=setup();try{for(let i=0;i<6;i++)peer.emit('message','events',{type:'chat',text:'hello',client:'victim',sender:'Admin'});const messages=world.sent.filter(m=>m.type==='chat');assert.equal(messages.length,4);assert(messages.every(m=>m.client==='player'&&!m.sender));peer.emit('message','events',{type:'call',id:3,method:'stop'});assert.equal(peer.sent.at(-1).m.error,'Unsupported operation');}finally{session.close();}});
test('independent ACK preserves consumed and discarded watermarks separately',()=>{const {peer,world,session}=setup();try{world.emit('message',{type:'inputAck',client:'player',ack:7,ackTick:20,discardedInput:9,discardedActions:11});assert.equal(peer.sent.at(-1).lane,'state');assert.equal(peer.sent.at(-1).m.ack,7);assert.equal(peer.sent.at(-1).m.discardedInput,9);}finally{session.close();}});
test('input flood guard retains its bound and emits bounded receipt timing diagnostics',()=>{const {peer,world,session}=setup();const oldWarn=console.warn,logs=[];console.warn=(...args)=>logs.push(args);try{const b=Buffer.alloc(16);b.writeUInt32LE(0x31494e42);for(let i=0;i<121;i++)peer.emit('binary',b,'state');assert(session.closed);assert.equal(world.sent.filter(m=>m.type==='entityInput').length,120);const value=JSON.parse(logs[0][1]);assert.equal(value.count,121);assert.equal(value.receiptOffsetsMs.length,121);assert(value.windowMs<1000);}finally{console.warn=oldWarn;session.close();}});
test('a native state send close race cannot escape the worker listener or acknowledge delivery',async()=>{
 const {Peer}=await import('../src/peer.js');const {peer,world,session}=setup();
 Object.setPrototypeOf(peer,Peer.prototype);delete peer.close;peer.authenticated=true;peer.pc={close(){}};
 peer.channels=new Map([['state',{isOpen:()=>true,bufferedAmount:()=>0,sendMessageBinary(){throw Error('errno=104');},close(){}}]]);
 const bytes=Buffer.alloc(30);bytes.writeUInt32LE(0x31454e42);
 assert.doesNotThrow(()=>world.emit('message',{type:'entityPacket',client:'player',epoch:1,packetId:5,buffer:bytes}));
 assert(peer.closed);assert(session.closed);assert(!world.sent.some(m=>m.type==='entitySent'));assert(world.sent.some(m=>m.type==='leave'));
});
test('batched independent input ACKs expose only each authenticated recipient and retain edge feedback',()=>{
 const {peer,world,session}=setup();try{world.emit('message',{type:'inputAck',inputAckBatchId:7,recipients:[{client:'other',ack:9},{client:'player',ack:4,edgeFeedback:{epoch:1,generation:2,received:[3],results:[]}}]});
 assert.equal(peer.sent.length,2);assert.equal(peer.sent[0].m.ack,4);assert.equal(peer.sent[0].m.type,'input-ack');assert.equal(peer.sent[0].m.recipients,undefined);assert.equal(peer.sent[0].m.edgeFeedback,undefined);assert.equal(peer.sent[0].m.inputAckBatchId,undefined);assert.deepEqual(peer.sent[1].m.received,[3]);assert.equal(peer.sent[1].m.type,'entity-edge-results');
 }finally{session.close();}
});
test('publication diagnostics distinguish existing stale-age and successful native admission',()=>{const {peer,world,session}=setup();try{const buffer=Buffer.alloc(30);buffer.writeUInt32LE(0x31454e42);world.emit('message',{type:'entityPacket',client:'player',epoch:1,packetId:1,sessionIncarnation:3,ownCorrection:true,capturedAtUnixMs:Date.now()-1000,buffer});assert.equal(world.sent.at(-1).type,'entityRejected');assert.equal(world.sent.at(-1).reason,'ownDroppedAge');assert.equal(world.sent.at(-1).sessionIncarnation,3);
 world.emit('message',{type:'entityPacket',client:'player',epoch:1,packetId:2,sessionIncarnation:3,ownCorrection:true,capturedAtUnixMs:Date.now(),buffer});assert.equal(world.sent.at(-1).type,'entitySent');assert.equal(world.sent.at(-1).sessionIncarnation,3);assert(world.sent.at(-1).ageMs<250);
 }finally{session.close();}});
test('parent-owned publication receives native admission directly before completion credit',()=>{const {peer,world,session}=setup();try{const results=[],receipts=[];world.publication={packetResult:m=>results.push(m),receipt:(type,m)=>receipts.push({type,m})};const buffer=Buffer.alloc(30);buffer.writeUInt32LE(0x31454e42);world.emit('message',{type:'entityPacket',client:'player',epoch:1,packetId:2,sessionIncarnation:3,ownCorrection:true,capturedAtUnixMs:Date.now(),buffer});assert.equal(results.length,1);assert.equal(receipts[0].type,'admitted');assert.equal(receipts[0].m.packetId,2);assert(!world.sent.some(m=>m.type==='entitySent'));peer.emit('message','state',{type:'entity-ack',epoch:1,packetId:2});assert.equal(receipts[1].type,'acks');assert.equal(receipts[1].m.packetId,2);}finally{session.close();}});
test('reconnect incarnation isolates old output, inputs and late close from the new authenticated session',async()=>{
 const {acceptsPublicationCommand}=await import('../src/publication-bridge.js');const world=new EventEmitter();world.sent=[];world.postMessage=m=>world.sent.push(m);world.publication={leave(){}};
 const peer=()=>{const p=new EventEmitter();p.remote={protocol:2,simulation:'bfe-sim-5',profile:'entity-state-v1'};p.sent=[];p.send=(lane,m)=>{p.sent.push(m);return true;};p.sendBinary=()=>true;return p;};const p1=peer(),p2=peer(),old=new GameSession(p1,world,'same',{}),current=new GameSession(p2,world,'same',{});
 try{assert.equal(old.publicationIncarnation,1);assert.equal(current.publicationIncarnation,2);world.emit('message',{type:'inputAck',client:'same',sessionIncarnation:1,ack:7});assert.equal(p2.sent.length,0);world.emit('message',{type:'inputAck',client:'same',sessionIncarnation:2,ack:8});assert.equal(p2.sent[0].ack,8);old.close();assert.equal(world.publicationSessions.get('same'),current);const leave=world.sent.at(-1);assert.equal(leave.type,'leave');assert.equal(leave.publicationIncarnation,1);const states=new Map([['same',{publicationIncarnation:2}]]);assert.equal(acceptsPublicationCommand(states,leave),false);for(const type of ['entityInput','entityEdgeAck','snapshotAck'])assert.equal(acceptsPublicationCommand(states,{type,client:'same',publicationIncarnation:1}),false);assert.equal(acceptsPublicationCommand(states,{type:'entityInput',client:'same',publicationIncarnation:2}),true);
 }finally{old.close();current.close();}assert.equal(world.publicationSessions.size,0);
});
