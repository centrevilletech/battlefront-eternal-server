import { randomBytes } from 'node:crypto';
import { inflateSync } from 'node:zlib';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { GameSession } from '../src/game-session.js';
function setup(){const peer=new EventEmitter();peer.sent=[];peer.send=(channel,m)=>{peer.sent.push([channel,m]);return true;};peer.close=()=>peer.emit('closed');const world=new EventEmitter();world.sent=[];world.postMessage=m=>world.sent.push(m);const session=new GameSession(peer,world,'player',{});return {peer,world,session};}
test('large initial snapshots are chunked, ordered and bounded',()=>{const {peer,world,session}=setup();try{const buffer=randomBytes(93610);world.emit('message',{type:'packet',client:'player',buffer});while(session.queue.length)session.flush();const chunks=peer.sent.map(([,m])=>m);assert(chunks.length>4);assert(chunks.every((m,i)=>m.index===i&&JSON.stringify(m).length<32768));assert.deepEqual(inflateSync(Buffer.from(chunks.map(m=>m.data).join(''),'base64')),buffer);}finally{session.close();}});
test('peer input cannot address another player or invoke arbitrary worker commands',()=>{const {peer,world,session}=setup();try{peer.emit('message','state',{type:'inputs',client:'victim',commands:[{tick:1}]});assert.equal(world.sent.at(-1).client,'player');peer.emit('message','events',{type:'call',id:3,method:'stop',args:{}});assert.equal(peer.sent.at(-1)[1].error,'Unsupported operation');assert(!world.sent.some(m=>m.method==='stop'));}finally{session.close();}});
test('reliable backlog bounds memory and disconnects a stalled client',()=>{const {peer,world,session}=setup();peer.send=()=>false;let closed=false;peer.once('closed',()=>closed=true);for(let i=0;i<8&&!closed;i++)world.emit('message',{type:'packet',client:'player',buffer:randomBytes(600000)});assert(closed);assert.equal(session.queue.length,0);});

test('raw world inspection cannot exceed channel limits or expose another client state',()=>{const {peer,world,session}=setup();try{peer.emit('message','events',{type:'call',id:4,method:'worldState'});assert.equal(peer.sent.at(-1)[1].error,'Unsupported operation');assert.equal(world.sent.length,1);}finally{session.close();}});

test('chat is bounded, rate limited and never trusts sender identity',()=>{const {peer,world,session}=setup();try{for(let i=0;i<6;i++)peer.emit('message','events',{type:'chat',text:'hello\u0000',client:'victim',sender:'Admin',team:true});const messages=world.sent.filter(m=>m.type==='chat');assert.equal(messages.length,4);assert(messages.every(m=>m.client==='player'&&m.text==='hello'&&m.team===true&&!m.sender));}finally{session.close();}});
