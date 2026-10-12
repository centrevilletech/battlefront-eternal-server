import {test} from 'node:test';
import assert from 'node:assert/strict';
import {EntityEdges} from '../src/entity-edges.js';
class Receiver {
 reset(){this.queue=new Map();this.seen=new Set();}
 receive(edges){for(const edge of edges)if(!this.seen.has(edge.id)){this.seen.add(edge.id);this.queue.set(edge.id,edge);}return {receipts:edges.map(e=>e.id),rejected:[]};}
 take(){const items=[...this.queue.values()];this.queue.clear();return items;}
}
function setup(){
 const mailbox=()=>({received:0,outcomes:[],enableOutcomes(){},drainOutcomes(){return this.outcomes.splice(0);}});
 const actor={unit:{generation:2},actions:mailbox(),fire:mailbox()},calls=[];
 const world=Object.fromEntries(['actions','fire'].map(lane=>[lane,(client,message)=>{assert(message.id>actor[lane].received);actor[lane].received=message.id;calls.push({lane,message});return true;}]));
 const edges=new EntityEdges({Receiver,actor,world,client:'one',registry:{classes:new Map([[1,'soldier']])}});
 return {actor,calls,edges};
}
test('reordered external IDs translate to monotonic internal IDs and acceptance is not consumption',()=>{
 const {actor,calls,edges}=setup();
 for(const id of [9,8]){edges.receive({epoch:1,generation:2,edges:[{epoch:1,generation:2,id,tick:10,kind:'action',action:'jump'}]},10);edges.advance(11);}
 assert.deepEqual(calls.map(c=>c.message.id),[1,2]);assert.deepEqual(edges.feedback().received,[9,8]);assert.deepEqual(edges.feedback().results,[]);
 actor.actions.outcomes.push({id:1,status:'consumed'});edges.consumed();assert.deepEqual(edges.feedback().results,[{id:9,status:'consumed',tick:11}]);
 edges.acknowledge({epoch:1,generation:3,ids:[9]});assert.equal(edges.feedback().results.length,1);
 edges.acknowledge({epoch:1,generation:2,ids:[9]});assert.equal(edges.feedback().results.length,0);assert.deepEqual(edges.feedback().received,[8]);
 actor.actions.outcomes.push({id:2,status:'cleared'});edges.consumed();assert.deepEqual(edges.feedback().results,[{id:8,status:'cleared',tick:11}]);
});
test('duplicate accepted clicks do not fire twice and unknown spawn prerequisites are explicit',()=>{
 const {actor,calls,edges}=setup(),edge={epoch:1,generation:2,id:9,tick:10,kind:'fire',button:'firePrimary',pressed:true};
 for(let i=0;i<2;i++){edges.receive({epoch:1,generation:2,edges:[edge]},10);edges.advance(11);}
 assert.equal(calls.length,1);assert.equal(calls[0].message.button,0);assert.equal(calls[0].message.externalId,9);assert.equal(edges.feedback().results.length,0);
 actor.fire.outcomes.push({id:1,status:'expired',tick:11});edges.consumed();assert.deepEqual(edges.feedback().results,[{id:9,status:'expired',tick:11}]);
 edges.receive({epoch:1,generation:2,edges:[{epoch:1,generation:2,id:2,tick:11,kind:'spawn',classId:99}]},11);edges.advance(12);
 assert.equal(calls.length,1);assert.deepEqual(edges.feedback().results.at(-1),{id:2,status:'rejected',tick:12});
 edges.reset(1,3);assert.equal(edges.pending.size,0);assert.deepEqual(edges.feedback().results,[]);
});
