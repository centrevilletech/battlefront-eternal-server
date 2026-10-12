import test from 'node:test';
import assert from 'node:assert/strict';
import {FreshStateWindow} from '../src/fresh-state.js';

test('offered but unadmitted snapshots cannot authorize ACK baselines',()=>{
 const state=new FreshStateWindow();assert.deepEqual(state.plan(1,3,()=>true,0),{baseTick:null,initialBootstrap:true});
 assert(!state.acknowledge(1,3,()=>true));
 assert(state.admitted(1,3));assert(state.acknowledge(1,3,()=>true));
 assert.deepEqual(state.plan(1,6,()=>true,50),{baseTick:3});
 assert.deepEqual(state.plan(1,9,()=>true,100),{baseTick:3},'lost newer ACK never halts ordinary publication');
 assert(!state.acknowledge(1,3,()=>true),'duplicate cannot rewind');
});

test('lost bootstrap and expired bases recover with newest bounded-cadence state',()=>{
 const state=new FreshStateWindow();assert.deepEqual(state.plan(1,3,()=>true,0),{baseTick:null,initialBootstrap:true});
 assert.equal(state.plan(1,6,()=>true,50),null);
 assert.deepEqual(state.plan(1,18,()=>true,250),{baseTick:null});
 state.admitted(1,18);assert(!state.acknowledge(1,18,()=>false),'evicted materialization cannot become baseline');
 assert(state.acknowledge(1,18,()=>true));
 assert.deepEqual(state.plan(1,21,()=>true,300),{baseTick:18});
 assert.deepEqual(state.plan(1,150,()=>false,600),{baseTick:null});
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


test('only initial join uses reliable bootstrap and its admitted baseline survives slow ACK',()=>{
 const state=new FreshStateWindow({maxSent:3,maxAgeTicks:6});
 assert.equal(state.plan(1,3,()=>true,0).initialBootstrap,true);
 state.admitted(1,3,true);
 for(let tick=6;tick<30;tick++)state.admitted(1,tick);
 assert(!state.sent.has(3));assert(state.acknowledge(1,3,()=>true));
 assert.deepEqual(state.plan(1,300,()=>true,5000),{baseTick:3},'pinned confirmed baseline remains usable');
 state.reset(1);assert.deepEqual(state.plan(1,303,()=>false,5100),{baseTick:null});
 assert.deepEqual(state.plan(2,306,()=>false,5200),{baseTick:null},'epoch reset never authorizes another reliable world transfer');
});
