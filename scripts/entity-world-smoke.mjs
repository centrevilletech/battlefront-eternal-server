// Requires operator-owned content. Exercises the real worker and codecs,
// simulates transport admission; this is not an RTC or capacity benchmark.
import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
const engine=resolve(process.env.BFE_ENGINE_DIR??'../battlefront-eternal'),data=resolve(process.env.BFE_DATA_DIR??'../battlefront-eternal/public');
const load=name=>import(pathToFileURL(resolve(engine,'src/runtime',name)));
const {encodeEntityInput}=await load('network-entity-input.js');
import {Worker} from 'node:worker_threads';
const {SnapshotReceiver}=await load('snapshot-journal.js');
const {decodeEntityPacket}=await load('network-entity-codec.js');
let finished=false;
const receiver=new SnapshotReceiver(),counts={initial:0,entity:0,own:0,remote:0,received:[],consumed:[]};
const worker=new Worker(new URL('../src/world-worker.js',import.meta.url),{workerData:{engine,data,maxPlayers:2,armySize:2,query:'?map=nab2&side=rep&ai=classic&seed=1'}});
const post=m=>worker.postMessage(m),timeout=setTimeout(()=>{console.error('timeout',counts);worker.terminate();process.exitCode=1},20000);
worker.on('error',error=>{console.error(error);clearTimeout(timeout);process.exitCode=1});
worker.on('message',async m=>{
 if(finished)return;
 if(m.type==='ready')post({type:'join',client:'one',entity:true,journal:true,fresh:true,compactMetadata:true,separateActions:true,linearProjectiles:true});
 if(m.type==='joined'){post({type:'call',client:'one',id:1,method:'start'});post({type:'start'});}
 if(m.type==='journalPacket'){counts.initial++;const p=receiver.receive(m.buffer);if(!p?.buffer)throw Error('no initial snapshot');post({type:'snapshotSent',client:'one',epoch:m.snapshotEpoch,tick:m.snapshotTick,initialBootstrap:true});post({type:'snapshotAck',client:'one',epoch:m.snapshotEpoch,tick:m.snapshotTick});}
 if(m.type==='entityEdgeResults'){counts.received=[...new Set([...counts.received,...m.received])];counts.consumed=[...new Set([...counts.consumed,...m.results.filter(r=>r.status==='consumed').map(r=>r.id)])];}
 if(m.type==='entityPacket'){const packet=decodeEntityPacket(new Uint8Array(m.buffer));counts.entity++;const own=packet.records.find(r=>r.kind==='correction');counts[own?'own':'remote']++;if(own)post({type:'entityInput',client:'one',buffer:encodeEntityInput({epoch:1,generation:own.generation,commands:[{sequence:counts.entity,tick:packet.tick+1,down:['KeyW'],lookTotal:[0,0]}],edges:counts.own<=2?[{id:counts.own===1?9:8,tick:packet.tick+1,kind:'action',action:'reload'}]:[]})});post({type:'entitySent',client:'one',epoch:packet.epoch,packetId:packet.packetId});post({type:'entityAck',client:'one',epoch:packet.epoch,packetId:packet.packetId});if(counts.own===20){finished=true;if(!counts.consumed.includes(9)||!counts.consumed.includes(8))throw Error('missing reordered edge consumption');console.log(JSON.stringify(counts));clearTimeout(timeout);await worker.terminate();}}
 if(m.type==='error'||m.type==='snapshotFault'||m.error){console.error(JSON.stringify(m));clearTimeout(timeout);await worker.terminate();process.exitCode=1;}
});
