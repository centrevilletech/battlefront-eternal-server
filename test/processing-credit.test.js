import test from 'node:test';
import assert from 'node:assert/strict';
import {returnProcessingCredit} from '../src/processing-credit.js';
test('deferred processing credit remains attached to originating match after rotation',()=>{
 const oldMessages=[],newMessages=[],pending=[];
 const oldWorld={postMessage:m=>oldMessages.push(m)},newWorld={postMessage:m=>newMessages.push(m)};
 let world=oldWorld;
 assert.equal(returnProcessingCredit(world,{type:'inputAck',inputAckBatchId:17},f=>pending.push(f)),true);
 world=newWorld;pending.shift()();
 assert.deepEqual(oldMessages,[{type:'inputAckBatchProcessed',inputAckBatchId:17}]);assert.deepEqual(newMessages,[]);
 assert.equal(returnProcessingCredit(oldWorld,{type:'publicationBatchDone',id:8}),true);
 assert.equal(oldMessages[1].type,'publicationBatchProcessed');assert.deepEqual(newMessages,[]);
 assert.equal(returnProcessingCredit(world,{type:'other'}),false);
});
