import {Worker} from 'node:worker_threads';
// Dispatch credit bounds the native Worker queue: one executing capture and one
// replaceable pending capture. Lifecycle/receipts live outside that slot.
export class PublicationCoordinator {
 constructor({engine,send,report=()=>{},workerFactory=()=>new Worker(new URL('./publication-worker.js',import.meta.url),{workerData:{engine}}),maxClients=80,maxReceipts=128,timeoutMs=2000,fatal=()=>{}}){
  Object.assign(this,{send,report,workerFactory,maxClients,maxReceipts,timeoutMs,fatal});this.clients=new Map();this.pending=null;this.inflight=null;this.serial=0;this.incarnation=0;this.closed=false;this.failures=0;this.metrics={dispatched:0,superseded:0,completed:0,staleResults:0,restarts:0,receiptEvictions:0,maxPending:0};this.diagnostics={phases:{},clients:{}};this.start();
 }
 start(){let worker;try{worker=this.worker=this.workerFactory();}catch(error){this.terminal('publication worker creation failed: '+error.message);return;}const incarnation=++this.incarnation;worker.on('message',message=>{
   if(this.closed||worker!==this.worker||message.incarnation!==incarnation){this.metrics.staleResults++;return;}
   if(message.type==='fault'){this.fail(worker,Error(message.message));return;}
   if(message.type!=='result'||message.id!==this.inflight?.id)return;
   this.observePhase('dispatchToResultMs',performance.now()-this.inflight.dispatchedAt);this.observePhase('captureToResultMs',Date.now()-this.inflight.capture.capturedAtUnixMs);this.inflight.resultAt=performance.now();this.metrics.completed++;const next=message.metrics,previous=this.lastWorkerMetrics;this.lastWorkerMetrics=next;const totals=this.metrics.worker??={jobs:0,totalMs:0,lifetimeMaxMs:0,cpu:{user:0,system:0}};for(const key of ['jobs','totalMs'])totals[key]+=Math.max(0,next[key]-(previous?.[key]??0));for(const key of ['user','system'])totals.cpu[key]+=Math.max(0,next.cpu[key]-(previous?.cpu?.[key]??0));totals.lifetimeMaxMs=Math.max(totals.lifetimeMaxMs,next.maxMs);totals.clients=next.clients;this.failures=0;
   try{for(const packet of message.packets){const client=this.clients.get(packet.client);if(client?.incarnation!==packet.sessionIncarnation)continue;this.observe(packet.client,'ownOffered',packet.ownCorrection);this.send(packet,[packet.buffer]);}}catch(error){this.terminal('publication transport consumer failed: '+error.message);return;}
   clearTimeout(this.watchdog);this.inflight.phase='transport';this.armTimeout();this.send({type:'publicationBatchDone',publicationJobId:message.id,publicationWorkerIncarnation:incarnation});
  });worker.on('error',error=>this.fail(worker,error));worker.on('exit',code=>{if(!this.closed&&worker===this.worker)this.fail(worker,Error(`publication worker exited ${code}`));});
 }
 fail(worker,error){if(this.closed||worker!==this.worker)return;this.worker=null;worker.terminate();this.report(String(error.message).slice(0,160));
  // The pending capture has the newest cumulative lifecycle/event facts.
  clearTimeout(this.watchdog);if(this.inflight?.phase!=='transport'){if(!this.pending&&this.inflight)this.pending=this.inflight.capture;this.inflight=null;}this.lastWorkerMetrics=null;this.metrics.restarts++;if(++this.failures>3){this.terminal('publication worker repeatedly failed');return;}this.start();if(this.inflight)this.armTimeout();this.dispatch();
 }
 join(client,incarnation){if(!this.clients.has(client)&&this.clients.size>=this.maxClients)throw Error('publication client capacity exceeded');this.clients.set(client,{incarnation,admitted:new Map(),acks:new Map()});}
 leave(client){this.clients.delete(client);delete this.diagnostics.clients[client];}
 receipt(type,message){const client=this.clients.get(message.client);if(!client||(message.sessionIncarnation!==undefined&&message.sessionIncarnation!==client.incarnation))return;const queue=client[type];if(!queue||!Number.isSafeInteger(message.packetId))return;queue.set(message.packetId,{...message,sessionIncarnation:client.incarnation});while(queue.size>this.maxReceipts){queue.delete(queue.keys().next().value);this.metrics.receiptEvictions++;}}
 capture(capture){if(this.closed)return;for(const r of capture.recipients??[])this.observe(r.client,'ownCaptured',!!r.own);if(this.pending)this.metrics.superseded++;this.pending=capture;this.metrics.maxPending=Math.max(this.metrics.maxPending,1);this.dispatch();}
 dispatch(){if(this.closed||this.inflight||!this.pending)return;if(this.serial>=0x7ffffffe)throw Error('publication packet identity exhausted');
  const capture=this.pending;this.pending=null;const id=++this.serial;
  // Reserve two packet IDs per job and recipient, including work whose result
  // is lost during restart. No scheduler may reuse that range after restart.
  const controls=[...this.clients].map(([client,state])=>({client,incarnation:state.incarnation,admitted:[...state.admitted.values()],acks:[...state.acks.values()]}));
  for(const state of this.clients.values()){state.admitted.clear();state.acks.clear();}
  this.observePhase('captureToDispatchMs',Date.now()-capture.capturedAtUnixMs);this.inflight={id,capture,dispatchedAt:performance.now(),incarnation:this.incarnation,phase:'working'};this.metrics.dispatched++;this.armTimeout();try{this.worker.postMessage({type:'capture',id,packetBase:id*2,incarnation:this.incarnation,controls,capture});}catch(error){this.fail(this.worker,error);}
 }
 armTimeout(){clearTimeout(this.watchdog);this.watchdog=setTimeout(()=>{if(this.inflight?.phase==='transport')this.terminal('publication transport processing timed out');else this.fail(this.worker,Error('publication worker timed out'));},this.timeoutMs);this.watchdog.unref?.();}
 processed(message){const job=this.inflight;if(!job||job.phase!=='transport'||message.publicationJobId!==job.id||message.publicationWorkerIncarnation!==job.incarnation)return;this.observePhase('resultToParentProcessedMs',performance.now()-job.resultAt);this.observePhase('captureToParentProcessedMs',Date.now()-job.capture.capturedAtUnixMs);clearTimeout(this.watchdog);this.inflight=null;this.dispatch();}
 terminal(reason){this.close();this.report(reason);this.fatal(reason);}
 observePhase(name,value){if(!Number.isFinite(value))return;const values=this.diagnostics.phases[name]??=[];if(values.length<120000)values.push(Math.max(0,value));}
 observe(client,key,own=true){if(!own||!this.clients.has(client))return;const value=this.diagnostics.clients[client]??={};value[key]=(value[key]??0)+1;}
 packetResult(message){if(this.clients.get(message.client)?.incarnation!==message.sessionIncarnation)return;if(!message.ownCorrection)return;this.observe(message.client,message.reason??'ownAdmitted');this.observePhase('ownAdmissionAgeMs',message.ageMs);if(message.reason)return;const c=this.clients.get(message.client),now=Date.now();if(c.lastOwnAt)this.observePhase('ownNativeAdmissionGapMs',now-c.lastOwnAt);if(c.lastOwnTick!==undefined)this.observePhase('ownNativeAdmissionTickGap',message.tick-c.lastOwnTick);c.lastOwnAt=now;c.lastOwnTick=message.tick;}
 resetMetrics(){this.diagnostics={phases:{},clients:{}};this.baseline=structuredClone(this.metrics);}
 metricsSnapshot(){const out=structuredClone(this.metrics),base=this.baseline??{};for(const key of ['dispatched','superseded','completed','staleResults','restarts','receiptEvictions'])out[key]-=base[key]??0;if(out.worker){for(const key of ['jobs','totalMs'])out.worker[key]-=base.worker?.[key]??0;for(const key of ['user','system'])out.worker.cpu[key]-=base.worker?.cpu?.[key]??0;}out.diagnostics={clients:this.diagnostics.clients,phases:Object.fromEntries(Object.entries(this.diagnostics.phases).map(([name,values])=>{const a=[...values].sort((a,b)=>a-b);return[name,{count:a.length,p50:a[Math.floor(a.length*.5)]??null,p95:a[Math.floor(a.length*.95)]??null,p99:a[Math.floor(a.length*.99)]??null,max:a.at(-1)??null}];}))};return out;}
 close(){clearTimeout(this.watchdog);this.closed=true;this.pending=null;this.inflight=null;this.clients.clear();this.worker?.terminate();}
}
