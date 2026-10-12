import {test} from 'node:test';import assert from 'node:assert/strict';import {EventEmitter} from 'node:events';
import {PublicationCoordinator} from '../src/publication-coordinator.js';
function fixture(options={}){const workers=[],sent=[];const coordinator=new PublicationCoordinator({...options,send:packet=>{if(packet.type!=='publicationBatchDone')sent.push(packet)},workerFactory:()=>{const worker=new EventEmitter();worker.messages=[];worker.postMessage=m=>worker.messages.push(m);worker.terminate=()=>{};workers.push(worker);return worker;}});return{coordinator,workers,sent};}
const complete=(worker,job,packets=[])=>worker.emit('message',{type:'result',id:job.id,incarnation:job.incarnation,packets,metrics:{jobs:1,totalMs:1,maxMs:1,cpu:{user:1,system:0}}});
test('stalled publication dispatch retains one latest capture while bounded receipts survive replacement',()=>{
 const {coordinator:c,workers}=fixture({maxReceipts:2});c.join('a',1);c.capture({tick:1,events:[1]});
 for(let tick=2;tick<=100;tick++)c.capture({tick,events:[1,2]});
 for(let packetId=1;packetId<=3;packetId++)c.receipt('acks',{client:'a',epoch:1,packetId});
 assert.equal(workers[0].messages.length,1);assert.equal(c.pending.tick,100);assert.equal(c.metrics.maxPending,1);assert.equal(c.metrics.superseded,98);
 complete(workers[0],workers[0].messages[0]);c.processed({publicationJobId:c.inflight.id,publicationWorkerIncarnation:c.inflight.incarnation});const latest=workers[0].messages[1];assert.equal(latest.capture.tick,100);assert.deepEqual(latest.capture.events,[1,2]);assert.deepEqual(latest.controls[0].acks.map(a=>a.packetId),[2,3]);assert.equal(c.metrics.receiptEvictions,1);c.close();
});
test('worker restart reserves new packet IDs and rejects old worker and reconnect results',()=>{
 const {coordinator:c,workers,sent}=fixture();c.join('a',1);c.capture({tick:1});const old=workers[0],job=old.messages[0];
 old.emit('error',Error('crash'));assert.equal(workers.length,2);const replacement=workers[1].messages[0];assert(replacement.packetBase>=job.packetBase+2);
 complete(old,job,[{client:'a',sessionIncarnation:1,buffer:new ArrayBuffer(1)}]);assert.equal(sent.length,0);
 c.leave('a');c.join('a',2);complete(workers[1],replacement,[{client:'a',sessionIncarnation:1,buffer:new ArrayBuffer(1)}]);assert.equal(sent.length,0);c.processed({publicationJobId:c.inflight.id,publicationWorkerIncarnation:c.inflight.incarnation});
 c.capture({tick:2});const fresh=workers[1].messages[1];complete(workers[1],fresh,[{client:'a',sessionIncarnation:2,buffer:new ArrayBuffer(1)}]);assert.equal(sent.length,1);assert.equal(c.metrics.restarts,1);c.close();
});
test('native admission and applied ACK controls remain separately bounded and ordered',()=>{
 const {coordinator:c,workers}=fixture({maxClients:1});c.join('a',1);assert.throws(()=>c.join('b',1),/capacity/);c.capture({tick:1});
 c.receipt('acks',{client:'a',epoch:1,packetId:2});c.receipt('admitted',{client:'a',epoch:1,packetId:2});c.capture({tick:2});
 complete(workers[0],workers[0].messages[0]);c.processed({publicationJobId:c.inflight.id,publicationWorkerIncarnation:c.inflight.incarnation});const controls=workers[0].messages[1].controls[0];assert.equal(controls.admitted[0].packetId,2);assert.equal(controls.acks[0].packetId,2);assert.equal(controls.incarnation,1);c.close();
});

test('completed packet batch waits for parent credit and rejects stale credit',()=>{
 const {coordinator:c,workers}=fixture();c.capture({tick:1});const job=workers[0].messages[0];complete(workers[0],job);for(let tick=2;tick<100;tick++)c.capture({tick});
 assert.equal(workers[0].messages.length,1);assert.equal(c.pending.tick,99);c.processed({publicationJobId:job.id,publicationWorkerIncarnation:999});assert.equal(workers[0].messages.length,1);
 c.processed({publicationJobId:job.id,publicationWorkerIncarnation:job.incarnation});assert.equal(workers[0].messages.length,2);c.close();
});
test('stale session receipts cannot acknowledge a reconnected client',()=>{const {coordinator:c}=fixture();c.join('a',2);c.receipt('acks',{client:'a',sessionIncarnation:1,packetId:3});assert.equal(c.clients.get('a').acks.size,0);c.close();});
test('hung workers restart then fail the match observably',async()=>{const failures=[];const {coordinator:c,workers}=fixture({timeoutMs:10,fatal:reason=>failures.push(reason)});c.capture({tick:1});await new Promise(resolve=>setTimeout(resolve,90));assert.equal(workers.length,4);assert.equal(c.closed,true);assert.equal(failures.length,1);});
test('stalled parent processing fails observably without queue growth',async()=>{const failures=[];const {coordinator:c,workers}=fixture({timeoutMs:10,fatal:reason=>failures.push(reason)});c.capture({tick:1});complete(workers[0],workers[0].messages[0]);c.capture({tick:2});await new Promise(resolve=>setTimeout(resolve,30));assert.equal(workers[0].messages.length,1);assert.match(failures[0],/transport/);});
