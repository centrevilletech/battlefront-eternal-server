import test from 'node:test';
import assert from 'node:assert/strict';
import {FreshStateWindow} from '../src/fresh-state.js';

test('offered but unadmitted snapshots cannot authorize ACK baselines',()=>{
 const state=new FreshStateWindow();assert.deepEqual(state.plan(1,3,()=>true,0),{baseTick:null});
 assert(!state.acknowledge(1,3,()=>true));
 assert(state.admitted(1,3));assert(state.acknowledge(1,3,()=>true));
 assert.deepEqual(state.plan(1,6,()=>true,50),{baseTick:3});
 assert.deepEqual(state.plan(1,9,()=>true,100),{baseTick:3},'lost newer ACK never halts ordinary publication');
 assert(!state.acknowledge(1,3,()=>true),'duplicate cannot rewind');
});

test('lost bootstrap and expired bases recover with newest bounded-cadence state',()=>{
 const state=new FreshStateWindow();assert.deepEqual(state.plan(1,3,()=>true,0),{baseTick:null});
 assert.equal(state.plan(1,6,()=>true,50),null);
 assert.deepEqual(state.plan(1,18,()=>true,250),{baseTick:null});
 state.admitted(1,18);assert(!state.acknowledge(1,18,()=>false),'evicted materialization cannot become baseline');
 assert(state.acknowledge(1,18,()=>true));
 assert.deepEqual(state.plan(1,21,()=>true,300),{baseTick:18});
 assert.deepEqual(state.plan(1,150,()=>true,600),{baseTick:null});
 assert.equal(state.ack,null);
});

test('admission history is bounded and old epochs cannot advance current state',()=>{
 const state=new FreshStateWindow({maxSent:3,maxAgeTicks:6});state.plan(1,1,()=>true,0);
 for(let tick=1;tick<=10;tick++)state.admitted(1,tick);
 assert.equal(state.sent.size,3);assert(!state.acknowledge(1,1,()=>true));
 assert(state.acknowledge(1,9,()=>true));assert(!state.acknowledge(1,8,()=>true));
 state.plan(2,11,()=>true,100);assert.equal(state.sent.size,0);
 assert(!state.admitted(1,12));assert(!state.acknowledge(1,9,()=>true));
});
