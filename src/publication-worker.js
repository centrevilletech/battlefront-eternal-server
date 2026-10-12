import {parentPort,workerData} from 'node:worker_threads';
import {pathToFileURL} from 'node:url';
import {resolve} from 'node:path';
import {EntityBudget} from './entity-budget.js';
import {entityImportance} from './entity-importance.js';
const root=workerData.engine;
const {EntityScheduler}=await import(pathToFileURL(resolve(root,'src/runtime/network-entity-stream.js')));
const {prepareEntityRecord}=await import(pathToFileURL(resolve(root,'src/runtime/network-entity-codec.js')));
const {decodeCombat}=await import(pathToFileURL(resolve(root,'src/runtime/network-entity-combat.js')));
const clients=new Map(),eventCache=new Map(),cpuStart=process.threadCpuUsage();
let jobs=0,totalMs=0,maxMs=0;
parentPort.on('message',message=>{
 if(message.type!=='capture')return;
 try{
  const begun=performance.now(),{capture,controls}=message,active=new Set(controls.map(c=>c.client));
  for(const id of clients.keys())if(!active.has(id))clients.delete(id);
  for(const control of controls){let state=clients.get(control.client);if(!state||state.incarnation!==control.incarnation){state={incarnation:control.incarnation,scheduler:new EntityScheduler({epoch:1}),budget:new EntityBudget()};clients.set(control.client,state);}
   for(const receipt of control.admitted)state.scheduler.admitted(receipt.packetId);
   for(const receipt of control.acks)state.scheduler.ack(receipt);
  }
  const records=capture.records.map(prepareEntityRecord),summaries=capture.summaries.map(prepareEntityRecord);
  const liveEvents=new Set(),events=capture.events.map(event=>{const r=event.record;liveEvents.add(r.id);const old=eventCache.get(r.id);if(old&&old.record.generation===r.generation&&old.record.tick===r.tick&&old.record.schema===r.schema)return old;const prepared={...event,prepared:prepareEntityRecord(r),view:decodeCombat(r)};eventCache.set(r.id,prepared);return prepared;});
  for(const id of eventCache.keys())if(!liveEvents.has(id))eventCache.delete(id);
  const packets=[];
  for(const recipient of capture.recipients){const state=clients.get(recipient.client);if(!state||state.incarnation!==recipient.sessionIncarnation)continue;
   const scheduler=state.scheduler;for(const prepared of records){if(!recipient.spectator&&prepared.record.kind==='soldier'&&prepared.record.id===recipient.unitId)continue;scheduler.update(prepared,entityImportance(prepared.record,recipient.interest));}
   for(const prepared of summaries)scheduler.update(prepared,4);
   for(const record of recipient.summaries)scheduler.update(record,4);
   for(const event of events)scheduler.updateEvent({...event,record:event.prepared},[event.view.ownerId,event.view.targetId,event.view.attackerId].includes(recipient.unitId)?24:entityImportance(event.view,recipient.interest));
   scheduler.serial=message.packetBase;
   const offer=(packet,ownCorrection)=>{if(!packet||!workerData.parentBudget&&!state.budget.take(packet.buffer.byteLength,begun))return false;const buffer=packet.buffer.buffer.slice(packet.buffer.byteOffset,packet.buffer.byteOffset+packet.buffer.byteLength);packets.push({type:'entityPacket',client:recipient.client,sessionIncarnation:state.incarnation,epoch:1,packetId:packet.packetId,ownCorrection,capturedAtUnixMs:capture.capturedAtUnixMs,buffer});return true;};
   const fields={tick:capture.tick,...recipient.fields},packet=scheduler.packet({...fields,own:recipient.own});
   if(offer(packet,!!recipient.own)){const maxBytes=workerData.parentBudget?1100:Math.min(1100,state.budget.available(begun));if(maxBytes>30)offer(scheduler.packet({...fields,maxBytes,excludePacketIds:[packet.packetId]}),false);}
  }
  const elapsed=performance.now()-begun;jobs++;totalMs+=elapsed;maxMs=Math.max(maxMs,elapsed);
  parentPort.postMessage({type:'result',id:message.id,incarnation:message.incarnation,packets,metrics:{jobs,totalMs,maxMs,cpu:process.threadCpuUsage(cpuStart),clients:clients.size}},packets.map(p=>p.buffer));
 }catch(error){parentPort.postMessage({type:'fault',incarnation:message.incarnation,message:error.message});}
});
