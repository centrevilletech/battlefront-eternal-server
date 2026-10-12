import {test} from 'node:test';import assert from 'node:assert/strict';import {LatestBatch} from '../src/latest-batch.js';
function fixture(){let now=0;const sent=[],states=[],faults=[],timers=[],checks=[];const lane=new LatestBatch(m=>sent.push(m),{incarnation:'lane',clock:()=>now,wallClock:()=>100000+now,onState:s=>states.push(s),fatal:s=>faults.push(s),setTimer:(fn,ms)=>{const timer={fn,at:now+ms};timers.push(timer);return timer;},clearTimer:t=>{if(t)t.cancelled=true;},scheduleCheck:fn=>{checks.push(fn);return fn;},cancelCheck:()=>{}});return {lane,sent,states,faults,timers,checks,time:n=>{now=n;},credit:id=>lane.processed(id,'lane')};}
test('three-second parent pause keeps bounded latest cumulative feedback and resumes by exact credit',()=>{
 const f=fixture();f.lane.offer([{ack:1}]);f.time(3000);for(let ack=2;ack<=100;ack++)f.lane.offer([{ack,edgeFeedback:{received:[1,2],results:[{sequence:1,result:'consumed'}]}}]);
 f.timers[0].fn();f.checks.shift()();assert.deepEqual(f.states,['stalled']);assert.equal(f.sent.length,2);assert.equal(f.sent[1].type,'inputAckProbe');assert.equal(f.timers[1].at,10000);
 f.lane.processed(1,'old');f.credit(99);f.lane.probeResult({...f.sent[1],processed:false});assert.equal(f.sent.length,2);
 f.time(3100);f.lane.probeResult({...f.sent[1],processed:true});assert.deepEqual(f.states,['stalled','running']);assert.equal(f.sent.length,3);assert.equal(f.sent[2].capturedAtUnixMs,103000);assert.equal(f.sent[2].recipients[0].ack,100);assert.equal(f.sent[2].recipients[0].edgeFeedback.results.length,1);f.lane.close();
});
test('fixed terminal deadline cannot be renewed or resurrected and probe occurs once',()=>{
 const f=fixture();f.lane.offer([1]);f.time(3000);f.lane.expire(1);f.checks.shift()();f.lane.expire(1);f.checks.shift()();assert.equal(f.sent.length,2);
 f.time(9999);f.lane.offer([2]);assert.equal(f.timers.at(-1).at,10000);f.time(10000);f.timers.at(-1).fn();assert.equal(f.faults.length,1);f.lane.offer([3]);f.credit(1);f.lane.dispatch();assert.equal(f.sent.length,2);assert.equal(f.lane.pending,null);
});
test('queued credit wins poll turn without closing a newer batch',()=>{const f=fixture();f.lane.offer([1]);f.lane.offer([2]);f.lane.expire(1);f.credit(1);f.checks.shift()();assert.equal(f.lane.inflight,2);assert.equal(f.states.length,0);f.lane.close();f.lane.offer([3]);assert.equal(f.sent.length,2);});
