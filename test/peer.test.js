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
