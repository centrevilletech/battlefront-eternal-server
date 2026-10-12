import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import rtc from "node-datachannel";
import { Peer } from "../src/peer.js";
import { loadIdentity, keyId } from "../src/identity.js";
import { EventEmitter } from "node:events";

test('state envelopes require their own increasing transport sequence',()=>{
 const peer=Object.assign(new EventEmitter(),{channels:new Map(),authenticated:true,lastSequence:0,capabilities:[]});
 Object.setPrototypeOf(peer,Peer.prototype);
 let receive;const channel={getLabel:()=> 'state',onError(){},onClosed(){},onOpen(){},onMessage:fn=>{receive=fn;},isOpen:()=>true};
 Peer.prototype.channel.call(peer,channel);
 const delivered=[];peer.on('message',(_,message)=>delivered.push(message));
 const input={type:'inputs',commands:[{sequence:99,tick:200}]};
 receive(JSON.stringify(input));
 receive(JSON.stringify({...input,sequence:1}));
 receive(JSON.stringify({...input,sequence:1}));
 receive(JSON.stringify({...input,sequence:0}));
 receive(JSON.stringify({...input,sequence:3}));
 receive(JSON.stringify({...input,sequence:2}));
 assert.deepEqual(delivered.map(message=>message.sequence),[1,3]);
 assert.equal(delivered[0].commands[0].sequence,99);
});

test('selected path treats mapped IPv4 addresses as IPv4',()=>{
 for(const [address,family] of [['192.0.2.1','ipv4'],['::ffff:192.0.2.1','ipv4'],['2001:db8::1','ipv6']]){
  const peer={pc:{getSelectedCandidatePair:()=>({local:{type:'host'},remote:{type:'host',address}})}};
  assert.deepEqual(Peer.prototype.path.call(peer),{kind:'direct',family});
 }
});
test(
  "identity proof binds encrypted session; content mismatch is spectator",
  { timeout: 15000 },
  async (t) => {
    const dir = await mkdtemp(join(tmpdir(), "bfe-peer-"));
    const ka = loadIdentity(join(dir, "a")),
      kb = loadIdentity(join(dir, "b"));
    const a = new Peer({
        name: "a",
        portable: true,
        keys: ka,
        initiator: true,
        expectedKey: keyId(kb.publicKey),
        simulation: "test",
        content: "one",
      }),
      b = new Peer({
        name: "b",
        portable: true,
        keys: kb,
        simulation: "test",
        content: "two",
      });
    t.after(async () => {
      a.close();
      b.close();
      rtc.cleanup();
      await rm(dir, { recursive: true, force: true });
    });
    a.on("signal", (s) => b.signal(s));
    b.on("signal", (s) => a.signal(s));
    const results = await Promise.all([
      new Promise((r) => a.once("ready", r)),
      new Promise((r) => b.once("ready", r)),
    ]);
    assert.equal(results[0].spectator, true);
    assert.equal(results[0].path.kind, "direct");
    const delivered = new Promise((r) => b.once("message", (_, m) => r(m)));
    assert.ok(a.send("events", { type: "chat", text: "hello" }));
    assert.equal((await delivered).text, "hello");
  },
);

test('native buffered sends are accepted once and complete state groups cross the soft limit',()=>{
 const sent=[];let buffered=0;const ch={isOpen:()=>true,bufferedAmount:()=>buffered,sendMessage:value=>{sent.push(value);return false;},sendMessageBinary:value=>{sent.push(value);buffered+=value.length;return false;}};
 const peer=Object.create(Peer.prototype);peer.authenticated=true;peer.sequence=0;peer.stateBufferLimit=1024;peer.channels=new Map([['events',ch],['bulk',ch],['state',ch]]);
 assert.equal(peer.send('events',{type:'test'}),true);assert.equal(sent.length,1);
 assert.equal(peer.sendBinary(Buffer.alloc(100)),true);assert.equal(sent.length,2);
 assert.equal(peer.sendStatePacket([Buffer.alloc(900),Buffer.alloc(900),Buffer.alloc(900)]),true);assert.equal(sent.length,5);
 assert.equal(peer.sendStatePacket([Buffer.alloc(900)]),false);assert.equal(sent.length,5);
});

test('semantic binary direction and initial bulk are authenticated and bounded',()=>{
 for(const initiator of [false,true]){
  const peer=Object.assign(new EventEmitter(),{initiator,channels:new Map(),authenticated:true,lastSequence:0,capabilities:initiator?['entity-state-v1']:[],remote:{capabilities:['entity-state-v1']},close(){this.closed=true;}});Object.setPrototypeOf(peer,Peer.prototype);peer.close=()=>{peer.closed=true;};
  let receive;const channel={getLabel:()=> 'state',onError(){},onClosed(){},onOpen(){},onMessage:fn=>{receive=fn;},isOpen:()=>true};Peer.prototype.channel.call(peer,channel);
  let delivered=0;peer.on('binary',()=>delivered++);const bytes=Buffer.alloc(40);bytes.writeUInt32LE(initiator?0x31454e42:0x31494e42);receive(bytes);assert.equal(delivered,1);
  bytes.writeUInt32LE(initiator?0x31494e42:0x31454e42);receive(bytes);assert(peer.closed);assert.equal(delivered,1);
  peer.closed=false;if(initiator){channel.getLabel=()=> 'bulk';Peer.prototype.channel.call(peer,channel);receive(Buffer.alloc(100));assert.equal(delivered,2);}
 }
});

test('old protocol receives an explicit version error before authentication',()=>{
 const sent=[];const peer=Object.create(Peer.prototype);peer.hello=()=>{};peer.compatibility={protocol:2,simulation:'bfe-sim-4',profile:'entity-state-v1'};peer.channels=new Map([['events',{sendMessage:m=>sent.push(JSON.parse(m))}]]);
 assert.throws(()=>peer.handshake({type:'hello',nonce:'a'.repeat(64),publicKey:'b'.repeat(60),protocol:1,simulation:'bfe-sim-2'}),/Update required/);
 assert.equal(sent[0].type,'version-error');assert.match(sent[0].message,/protocol 2/);assert(!peer.authenticated);
});

 test('previous correction schema revision is rejected before authentication',()=>{
 const sent=[];const peer=Object.create(Peer.prototype);peer.hello=()=>{};peer.compatibility={protocol:2,simulation:'bfe-sim-4',profile:'entity-state-v1'};peer.channels=new Map([['events',{sendMessage:m=>sent.push(JSON.parse(m))}]]);
 assert.throws(()=>peer.handshake({type:'hello',nonce:'a'.repeat(64),publicKey:'b'.repeat(60),protocol:2,simulation:'bfe-sim-3',profile:'entity-state-v1'}),/Update required/);
 assert.match(sent[0].message,/bfe-sim-4/);assert(!peer.authenticated);
 });
test('native readiness and send close races isolate the affected peer without admitting state',()=>{
 for(const failure of ['isOpen','bufferedAmount','sendMessage','sendMessageBinary']){
  const peer=new EventEmitter();Object.setPrototypeOf(peer,Peer.prototype);Object.assign(peer,{authenticated:true,sequence:0,stateBufferLimit:1024,pc:{close(){throw Error('already closed');}}});
  const channel={isOpen:()=>true,bufferedAmount:()=>0,sendMessage(){},sendMessageBinary(){},close(){throw Error('already closed');}};
  channel[failure]=()=>{throw Error('libdatachannel send failed errno=104');};peer.channels=new Map([['state',channel],['events',channel],['bulk',channel]]);
  let closes=0;peer.on('closed',()=>closes++);
  const send= failure==='sendMessage'?()=>peer.send('events',{type:'test'}):()=>peer.sendStatePacket([Buffer.alloc(30)],{maxBuffered:0});
  assert.equal(send(),false);assert.equal(peer.closed,true);assert.equal(closes,1);assert.equal(peer.sendBinary(Buffer.alloc(30)),false);peer.close();assert.equal(closes,1);
 }
 const peer=new EventEmitter();Object.setPrototypeOf(peer,Peer.prototype);peer.authenticated=true;peer.channels=new Map([['bulk',{isOpen:()=>true,bufferedAmount:()=>0,sendMessageBinary(){throw Error('errno=104');},close(){}}]]);peer.pc={close(){}};assert.equal(peer.sendBinary(Buffer.alloc(30)),false);assert(peer.closed);
});
