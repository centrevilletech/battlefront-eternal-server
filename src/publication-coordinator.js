import {Worker} from 'node:worker_threads';
// Dispatch credit bounds the native Worker queue: one executing capture and one
// replaceable pending capture. Lifecycle/receipts live outside that slot.
export class PublicationCoordinator {
 constructor({engine,send,report=()=>{},workerFactory=()=>new Worker(new URL('./publication-worker.js',import.meta.url),{workerData:{engine}}),maxClients=64,maxReceipts=128}){
  Object.assign(this,{send,report,workerFactory,maxClients,maxReceipts});this.clients=new Map();this.pending=null;this.inflight=null;this.serial=0;this.incarnation=0;this.closed=false;this.failures=0;this.metrics={dispatched:0,superseded:0,completed:0,staleResults:0,restarts:0,receiptEvictions:0,maxPending:0};this.start();
 }
 start(){const worker=this.worker=this.workerFactory(),incarnation=++this.incarnation;worker.on('message',message=>{
   if(this.closed||worker!==this.worker||message.incarnation!==incarnation){this.metrics.staleResults++;return;}
   if(message.type==='fault'){this.fail(worker,Error(message.message));return;}
   if(message.type!=='result'||message.id!==this.inflight?.id)return;
   this.metrics.completed++;const next=message.metrics,previous=this.lastWorkerMetrics;this.lastWorkerMetrics=next;const totals=this.metrics.worker??={jobs:0,totalMs:0,lifetimeMaxMs:0,cpu:{user:0,system:0}};for(const key of ['jobs','totalMs'])totals[key]+=Math.max(0,next[key]-(previous?.[key]??0));for(const key of ['user','system'])totals.cpu[key]+=Math.max(0,next.cpu[key]-(previous?.cpu?.[key]??0));totals.lifetimeMaxMs=Math.max(totals.lifetimeMaxMs,next.maxMs);totals.clients=next.clients;this.failures=0;
   for(const packet of message.packets){const client=this.clients.get(packet.client);if(client?.incarnation!==packet.sessionIncarnation)continue;this.send(packet,[packet.buffer]);}
   this.inflight=null;this.dispatch();
  });worker.on('error',error=>this.fail(worker,error));worker.on('exit',code=>{if(!this.closed&&worker===this.worker)this.fail(worker,Error(`publication worker exited ${code}`));});
 }
 fail(worker,error){if(this.closed||worker!==this.worker)return;this.worker=null;worker.terminate();this.report(String(error.message).slice(0,160));
  // The pending capture has the newest cumulative lifecycle/event facts.
  if(!this.pending&&this.inflight)this.pending=this.inflight.capture;
  this.inflight=null;this.lastWorkerMetrics=null;this.metrics.restarts++;if(++this.failures>3){this.closed=true;this.report('publication worker repeatedly failed; publication stopped');return;}this.start();this.dispatch();
 }
 join(client,incarnation){if(!this.clients.has(client)&&this.clients.size>=this.maxClients)throw Error('publication client capacity exceeded');this.clients.set(client,{incarnation,admitted:new Map(),acks:new Map()});}
 leave(client){this.clients.delete(client);}
 receipt(type,message){const client=this.clients.get(message.client);if(!client)return;const queue=client[type];if(!queue||!Number.isSafeInteger(message.packetId))return;queue.set(message.packetId,{...message,sessionIncarnation:client.incarnation});while(queue.size>this.maxReceipts){queue.delete(queue.keys().next().value);this.metrics.receiptEvictions++;}}
 capture(capture){if(this.closed)return;if(this.pending)this.metrics.superseded++;this.pending=capture;this.metrics.maxPending=Math.max(this.metrics.maxPending,1);this.dispatch();}
 dispatch(){if(this.closed||this.inflight||!this.pending)return;if(this.serial>=0x7ffffffe)throw Error('publication packet identity exhausted');
  const capture=this.pending;this.pending=null;const id=++this.serial;
  // Reserve two packet IDs per job and recipient, including work whose result
  // is lost during restart. No scheduler may reuse that range after restart.
  const controls=[...this.clients].map(([client,state])=>({client,incarnation:state.incarnation,admitted:[...state.admitted.values()],acks:[...state.acks.values()]}));
  for(const state of this.clients.values()){state.admitted.clear();state.acks.clear();}
  this.inflight={id,capture};this.metrics.dispatched++;this.worker.postMessage({type:'capture',id,packetBase:id*2,incarnation:this.incarnation,controls,capture});
 }
 resetMetrics(){this.baseline=structuredClone(this.metrics);}
 metricsSnapshot(){const out=structuredClone(this.metrics),base=this.baseline??{};for(const key of ['dispatched','superseded','completed','staleResults','restarts','receiptEvictions'])out[key]-=base[key]??0;if(out.worker){for(const key of ['jobs','totalMs'])out.worker[key]-=base.worker?.[key]??0;for(const key of ['user','system'])out.worker.cpu[key]-=base.worker?.cpu?.[key]??0;}return out;}
 close(){this.closed=true;this.pending=null;this.inflight=null;this.clients.clear();this.worker?.terminate();}
}
