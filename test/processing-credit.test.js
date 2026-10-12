import test from 'node:test';
import assert from 'node:assert/strict';
import {returnProcessingCredit} from '../src/processing-credit.js';
test('deferred processing credit remains attached to originating match after rotation',()=>{
 const oldMessages=[],newMessages=[],pending=[];
 const oldWorld={postMessage:m=>oldMessages.push(m)},newWorld={postMessage:m=>newMessages.push(m)};
 let world=oldWorld;
 assert.equal(returnProcessingCredit(world,{type:'inputAck',inputAckBatchId:17,inputAckIncarnation:"old"},f=>pending.push(f)),true);
 world=newWorld;pending.shift()();
 assert.deepEqual(oldMessages,[{type:'inputAckBatchProcessed',inputAckBatchId:17,inputAckIncarnation:"old"}]);assert.deepEqual(newMessages,[]);
 assert.equal(returnProcessingCredit(oldWorld,{type:'publicationBatchDone',id:8}),true);
 assert.equal(oldMessages[1].type,'publicationBatchProcessed');assert.deepEqual(newMessages,[]);
 assert.equal(returnProcessingCredit(world,{type:'other'}),false);
});

test('probe only credits the exact completed incarnation after synchronous consumers',()=>{const sent=[],pending=[];const world={postMessage:m=>sent.push(m)};const batch={type:'inputAck',inputAckBatchId:1,inputAckIncarnation:'a'};returnProcessingCredit(world,batch,f=>pending.push(f));const probe={...batch,type:'inputAckProbe'};returnProcessingCredit(world,probe);assert.equal(sent.at(-1).processed,false);pending.shift()();returnProcessingCredit(world,probe);assert.equal(sent.at(-1).processed,true);returnProcessingCredit(world,{...probe,inputAckIncarnation:'b'});assert.equal(sent.at(-1).processed,false);});
