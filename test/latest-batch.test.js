import {test} from 'node:test';import assert from 'node:assert/strict';import {LatestBatch} from '../src/latest-batch.js';
test('input ACK lane replaces pending batches independently of state publication',()=>{const sent=[];const lane=new LatestBatch(m=>sent.push(m));lane.offer([{client:'a',ack:1}]);for(let ack=2;ack<=100;ack++)lane.offer([{client:'a',ack}]);assert.equal(sent.length,1);lane.processed(999);assert.equal(sent.length,1);lane.processed(1);assert.equal(sent.length,2);assert.equal(sent[1].recipients[0].ack,100);lane.close();});
test('closed and timed-out ACK lanes cannot be resurrected by offers or stale credits',async()=>{for(const timeout of [false,true]){const sent=[],faults=[];const lane=new LatestBatch(m=>sent.push(m),{timeoutMs:5,fatal:r=>faults.push(r)});lane.offer([1]);if(timeout)await new Promise(resolve=>setTimeout(resolve,20));else lane.close();lane.offer([2]);lane.processed(1);lane.dispatch();assert.equal(sent.length,1);assert.equal(faults.length,timeout?1:0);assert.equal(lane.pending,null);}});

test('queued credit gets one poll turn after deadline without closing a newer batch',()=>{
 const sent=[],checks=[],faults=[];const lane=new LatestBatch(m=>sent.push(m),{scheduleCheck:f=>{checks.push(f);return checks.length;},cancelCheck:()=>{},fatal:r=>faults.push(r)});
 try{lane.offer([1]);lane.offer([2]);lane.expire(1);assert.equal(lane.closed,undefined);assert.equal(sent.length,1);
  lane.processed(1);assert.equal(sent.length,2);checks.shift()();assert.equal(faults.length,0);assert.equal(lane.inflight,2);
  lane.expire(2);checks.shift()();assert.equal(faults.length,1);assert.equal(lane.closed,true);lane.offer([3]);assert.equal(sent.length,2);
 }finally{lane.close();}
});
