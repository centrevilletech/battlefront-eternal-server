import test from 'node:test';
import assert from 'node:assert/strict';
import {StatePacer} from '../src/state-pacer.js';
function fixture(options={}){
 let time=0;const callbacks=new Map(),sent=[],dropped=[];let id=0;
 const pacer=new StatePacer({send:m=>sent.push({id:m.packetId,at:time}),drop:m=>dropped.push(m.packetId),now:()=>time,schedule:(fn,ms)=>{callbacks.set(++id,{fn,at:time+ms});return id;},cancel:id=>callbacks.delete(id),...options});
 return {pacer,sent,dropped,advance:ms=>{const target=time+ms;for(;;){const next=[...callbacks].sort((a,b)=>a[1].at-b[1].at)[0];if(!next||next[1].at>target)break;time=next[1].at;callbacks.delete(next[0]);next[1].fn();}time=target;}};
}
const packet=(id,ownCorrection=false)=>({packetId:id,ownCorrection,buffer:Buffer.alloc(1000),pacingBytesPerSecond:100000});
test('publication packets are spaced by bytes and own corrections pass queued remote state',()=>{
 const {pacer,sent,advance}=fixture();pacer.offer(packet(1));pacer.offer(packet(2));pacer.offer(packet(3));pacer.offer(packet(4,true));
 assert.deepEqual(sent,[{id:1,at:0}]);advance(9);assert.equal(sent.length,1);advance(21);
 assert.deepEqual(sent,[{id:1,at:0},{id:4,at:10},{id:2,at:20},{id:3,at:30}]);assert.equal(pacer.bytes,0);
});
test('queue pressure refunds oldest remote offers and close cancels timers',()=>{
 const {pacer,sent,dropped,advance}=fixture({maxBytes:2000});pacer.offer(packet(1));pacer.offer(packet(2));pacer.offer(packet(3,true));pacer.offer(packet(4));
 assert.deepEqual(dropped,[2]);assert.equal(pacer.bytes,2000);pacer.close();advance(100);
 assert.deepEqual(sent,[{id:1,at:0}]);assert.deepEqual(dropped,[2,3,4]);assert.equal(pacer.bytes,0);
 assert.equal(pacer.metrics.pacedPressureDropped,1);assert.equal(pacer.metrics.pacedClosed,2);assert.equal(pacer.metrics.pacedDropped,3);
});
test('stalled pacing expires obsolete offers instead of admitting a catch-up burst',()=>{
 let time=0;const dropped=[],sent=[];const pacer=new StatePacer({now:()=>time,schedule:()=>1,cancel(){},send:m=>sent.push(m.packetId),drop:m=>dropped.push(m.packetId)});
 pacer.offer(packet(1));pacer.offer(packet(2));time=200;pacer.flush();assert.deepEqual(sent,[1]);assert.deepEqual(dropped,[2]);
 pacer.offer(packet(3));assert.deepEqual(sent,[1,3]);
 assert.equal(pacer.metrics.pacedExpired,1);
});
test('new own correction replaces an unsent older correction and refunds its complete offer',()=>{
 const {pacer,sent,dropped,advance}=fixture();pacer.offer(packet(1));pacer.offer(packet(2,true));pacer.offer(packet(3));pacer.offer(packet(4,true));
 assert.deepEqual(dropped,[2]);advance(20);assert.deepEqual(sent.map(value=>value.id),[1,4,3]);
 assert.equal(pacer.metrics.pacedSuperseded,1);
});
