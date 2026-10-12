// Real IPC diagnostic: deliberately pause only the parent for three seconds.
import assert from 'node:assert/strict';
import {Worker} from 'node:worker_threads';
import {returnProcessingCredit} from '../src/processing-credit.js';
const terminal=process.argv.includes('--terminal');
const worker=new Worker(`
 const {parentPort}=require('node:worker_threads');
 (async()=>{const {LatestBatch}=await import(${JSON.stringify(new URL('../src/latest-batch.js',import.meta.url).href)});
 const lane=new LatestBatch(m=>parentPort.postMessage(m),{onState:status=>parentPort.postMessage({type:'status',status}),fatal:error=>parentPort.postMessage({type:'fatal',error})});
 let ack=0;const timer=setInterval(()=>lane.offer([{client:'test',ack:++ack,edgeFeedback:{received:[7],results:[{id:7,status:'consumed',tick:1}]}}]),10);
 parentPort.on('message',m=>{if(m.type==='inputAckBatchProcessed')lane.processed(m.inputAckBatchId,m.inputAckIncarnation);if(m.type==='inputAckProbeResult')lane.probeResult(m);if(m.type==='stop'){clearInterval(timer);lane.close();parentPort.close();}});
 })();`,{eval:true});
let paused=false,stale=0,fresh=0,probes=0,lastAck=0,batches=0;const states=[],faults=[];const started=performance.now();
const deadline=setTimeout(()=>{worker.terminate();throw Error('pause smoke timed out');},15000);
await new Promise((resolve,reject)=>{
 worker.on('error',reject);
 worker.on('message',m=>{
  if(!terminal&&m.type==='inputAck'&&!paused){paused=true;Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,3000);}
  if(!terminal)returnProcessingCredit(worker,m);
  if(m.type==='inputAck'){batches++;assert.deepEqual(m.recipients[0].edgeFeedback.results,[{id:7,status:'consumed',tick:1}]);if(Date.now()-m.capturedAtUnixMs>250)stale++;else{fresh++;lastAck=m.recipients[0].ack;}}
  if(m.type==='inputAckProbe')probes++;
  if(m.type==='status')states.push(m.status);
  if(m.type==='fatal'){faults.push(m.error);if(terminal){worker.postMessage({type:'stop'});resolve();}}
  if(!terminal&&fresh>=3){worker.postMessage({type:'stop'});resolve();}
 });
});
clearTimeout(deadline);await worker.terminate();
if(terminal){assert.equal(batches,1);assert.equal(probes,1);assert.equal(faults.length,1);assert.deepEqual(states,['stalled','failed']);assert(performance.now()-started>=10000);console.log(JSON.stringify({terminalMs:performance.now()-started,batches,probes,states,faults}));process.exit(0);}
assert.equal(stale,1);assert.equal(probes,1);assert.deepEqual(states,['stalled','running']);assert.equal(faults.length,0);assert(lastAck>200);
console.log(JSON.stringify({pauseMs:3000,staleSkipped:stale,freshAfterResume:fresh,probes,states,lastAck,faults}));
