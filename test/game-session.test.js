import { randomBytes } from 'node:crypto';
import { inflateSync } from 'node:zlib';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { decodeBulkFrame } from '../src/bulk-frame.js';
import { GameSession, compressionStats } from '../src/game-session.js';
function setup(){const peer=new EventEmitter();peer.sent=[];peer.send=(channel,m)=>{peer.sent.push([channel,m]);return true;};peer.close=()=>peer.emit('closed');const world=new EventEmitter();world.sent=[];world.postMessage=m=>world.sent.push(m);const session=new GameSession(peer,world,'player',{});return {peer,world,session};}
async function drained(session){for(let n=0;n<400;n++){session.flush();if(!session.draining&&!session.pending.length&&!session.queue.length)return;await new Promise(r=>setTimeout(r,5));}throw Error('compression drain timed out');}
test('large initial snapshots are chunked, ordered and bounded',async()=>{const {peer,world,session}=setup();try{const buffer=randomBytes(93610);world.emit('message',{type:'packet',client:'player',buffer});await drained(session);const chunks=peer.sent.map(([,m])=>m);assert(chunks.length>4);assert(chunks.every((m,i)=>m.index===i&&JSON.stringify(m).length<32768));assert.deepEqual(inflateSync(Buffer.from(chunks.map(m=>m.data).join(''),'base64')),buffer);}finally{session.close();}});
test('peer input cannot address another player or invoke arbitrary worker commands',()=>{const {peer,world,session}=setup();try{peer.emit('message','state',{type:'inputs',client:'victim',commands:[{tick:1}]});assert.equal(world.sent.at(-1).client,'player');peer.emit('message','events',{type:'call',id:3,method:'stop',args:{}});assert.equal(peer.sent.at(-1)[1].error,'Unsupported operation');assert(!world.sent.some(m=>m.method==='stop'));}finally{session.close();}});
test('reliable backlog bounds memory and disconnects a stalled client',()=>{const {peer,world,session}=setup();peer.send=()=>false;let closed=false;peer.once('closed',()=>closed=true);for(let i=0;i<8&&!closed;i++)world.emit('message',{type:'packet',client:'player',buffer:randomBytes(600000)});assert(closed);assert.equal(session.queue.length,0);});

test('raw world inspection cannot exceed channel limits or expose another client state',()=>{const {peer,world,session}=setup();try{peer.emit('message','events',{type:'call',id:4,method:'worldState'});assert.equal(peer.sent.at(-1)[1].error,'Unsupported operation');assert.equal(world.sent.length,1);}finally{session.close();}});

test('chat is bounded, rate limited and never trusts sender identity',()=>{const {peer,world,session}=setup();try{for(let i=0;i<6;i++)peer.emit('message','events',{type:'chat',text:'hello\u0000',client:'victim',sender:'Admin',team:true});const messages=world.sent.filter(m=>m.type==='chat');assert.equal(messages.length,4);assert(messages.every(m=>m.client==='player'&&m.text==='hello'&&m.team===true&&!m.sender));}finally{session.close();}});

test('stale reliable backlog resets the delta chain and waits for a fresh keyframe',async()=>{
  const {peer,world,session}=setup();
  try {
    peer.send=()=>false;
    session.queue.push({type:'ticks',buffers:['old'],queuedAt:performance.now()-2100});session.bytes=3;
    session.flush();assert(session.awaitKeyframe);assert.equal(world.sent.at(-1).type,'resync');assert.equal(session.queue[0].type,'reset');
    world.emit('message',{type:'packet',client:'player',buffer:Buffer.from('stale delta'),keyframe:false});assert.equal(session.queue.length,1);
    peer.sent=[];peer.send=(channel,message)=>{peer.sent.push(message);return true;};
    world.emit('message',{type:'packet',client:'player',buffer:Buffer.from('fresh keyframe'),keyframe:true});
    await drained(session);assert.equal(session.awaitKeyframe,false);assert.equal(peer.sent[0].type,'reset');assert.equal(peer.sent[1].type,'ticks');
    assert.equal(inflateSync(Buffer.from(peer.sent[1].buffers[0],'base64')).toString(),'fresh keyframe');assert.equal(session.bytes,0);
  }finally{session.close();}
});

test('shared world snapshots compress once while each recipient retains personal metadata',async()=>{
 const {peer,world,session}=setup();const second=new EventEmitter();second.sent=[];second.send=(channel,m)=>{second.sent.push(m);return true;};second.close=()=>second.emit('closed');const other=new GameSession(second,world,'second',{});
 try{const calls=compressionStats.calls;world.emit('message',{type:'packet',sharedId:1,buffer:Buffer.from('shared world'),recipients:[{client:'player',metadata:{playerId:82},keyframe:true},{client:'second',metadata:{playerId:83},keyframe:true}]});await Promise.all([drained(session),drained(other)]);assert.equal(compressionStats.calls-calls,1);assert.equal(peer.sent[0][1].metadata.playerId,82);assert.equal(second.sent[0].metadata.playerId,83);assert.equal(peer.sent[0][1].buffers[0],second.sent[0].buffers[0]);}finally{session.close();other.close();}
});

test('fire edges use reliable events, bind authenticated identity, and cap floods',()=>{
 const {peer,world,session}=setup();let closed=false;peer.once('closed',()=>closed=true);
 try{
  const edge={type:'fire',id:1,tick:42,generation:2,epoch:1,button:0,pressed:true,client:'victim',position:[1,2,3],aimYaw:1,viewTick:40};
  peer.emit('message','state',edge);assert.equal(world.sent.length,1);
  peer.emit('message','events',edge);const message=world.sent.at(-1);assert.equal(message.client,'player');assert.equal(message.edge.aimYaw,1);assert.equal(message.edge.viewTick,40);assert.equal(message.edge.position,undefined);assert.equal(message.edge.client,undefined);
  for(let i=2;i<=121;i++)peer.emit('message','events',{...edge,id:i});
  assert(closed);assert.equal(world.sent.filter(m=>m.type==='fire').length,120);
 }finally{session.close();}
});

test('input and resync floods cannot amplify worker load',()=>{
 const {peer,world,session}=setup();let closed=false;peer.once('closed',()=>closed=true);
 try{
  for(let i=0;i<20;i++)peer.emit('message','events',{type:'resync'});
  assert.equal(world.sent.filter(m=>m.type==='resync').length,1);
  for(let i=0;i<17;i++)peer.emit('message','state',{type:'inputs',commands:Array.from({length:32},()=>({tick:1}))});
  assert(closed);assert.equal(world.sent.filter(m=>m.type==='inputs').length,16);
 }finally{session.close();}
});

test('compact metadata is enabled only by the authenticated peer capability',()=>{
 const peer=new EventEmitter();peer.send=()=>true;peer.remote={capabilities:['unknown','compact-personal-v1']};const world=new EventEmitter();world.sent=[];world.postMessage=m=>world.sent.push(m);
 const session=new GameSession(peer,world,'authenticated',{});assert.equal(world.sent[0].compactMetadata,true);session.close();
 const plain=setup();assert.equal(plain.world.sent[0].compactMetadata,false);plain.session.close();
});

test('negotiated binary snapshots preserve compressed bytes and first-fragment metadata',async()=>{
 const peer=new EventEmitter();peer.remote={capabilities:['binary-bulk-v1']};peer.sent=[];peer.send=()=>true;peer.sendBinary=buffer=>{peer.sent.push(buffer);return true;};peer.close=()=>peer.emit('closed');
 const world=new EventEmitter();world.postMessage=()=>{};const session=new GameSession(peer,world,'player',{});
 try{const body=randomBytes(93610);world.emit('message',{type:'packet',client:'player',buffer:body,metadata:{playerId:82}});await drained(session);
 const frames=peer.sent.map(decodeBulkFrame);assert(frames.length>=4);assert(frames.every((f,i)=>f.index===i&&f.total===frames.length));assert.deepEqual(frames[0].metadata,{playerId:82});assert(frames.slice(1).every(f=>f.metadata===undefined));assert.deepEqual(inflateSync(Buffer.concat(frames.map(f=>f.data))),body);assert.equal(session.bytes,0);
 }finally{session.close();}
});

test('reliable actions bind identity independently of state packet sequence',()=>{
 const {peer,world,session}=setup();try{
 peer.emit('message','state',{type:'input',command:{sequence:90}});
 peer.emit('message','events',{type:'actions',client:'victim',epoch:1,id:10,tick:42,generation:1,actions:['jump'],human:[]});
 const action=world.sent.at(-1);assert.equal(action.type,'actions');assert.equal(action.client,'player');assert.equal(action.envelope.id,10);assert.deepEqual(action.envelope.actions,['jump']);
 }finally{session.close();}
});
