import {ProcessingLane} from './processing-lane.js';
// Authority-to-parent capture credit is independent of remote encoding. Only
// pose captures are superseded; bounded exact receipts and the current roster
// are copied at dispatch, and each capture contains cumulative retained events.
export class PublicationBridge {
 constructor({send,fatal=()=>{},maxClients=192,maxReceipts=128,onState=()=>{},...laneOptions}){Object.assign(this,{send,fatal,maxClients,maxReceipts});this.clients=new Map();this.metrics={dispatched:0,superseded:0,receiptEvictions:0};this.lane=new ProcessingLane(message=>this.dispatchMessage(message),{...laneOptions,onState,fatal:reason=>this.terminal(reason),failureMessage:'publication capture parent processing timed out',describeBatch:({id,incarnation,recipients:capture})=>({type:'publicationCapture',id,incarnation,capture}),describeProbe:({id,incarnation})=>({type:'publicationCaptureProbe',id,incarnation})});}
 get inflight(){return this.lane.inflight;}
 get pending(){return this.lane.pending?.recipients;}

 join(client,incarnation){if(!this.clients.has(client)&&this.clients.size>=this.maxClients)throw Error('publication client capacity exceeded');this.clients.set(client,{incarnation,acks:new Map()});}
 leave(client){this.clients.delete(client);}
 receipt(type,message){if(type!=='acks')return;const c=this.clients.get(message.client);if(!c||(message.sessionIncarnation!==undefined&&message.sessionIncarnation!==c.incarnation))return;c.acks.set(message.packetId,{...message,sessionIncarnation:c.incarnation});while(c.acks.size>this.maxReceipts){c.acks.delete(c.acks.keys().next().value);this.metrics.receiptEvictions++;}}
 capture(capture){if(this.closed)return;if(this.lane.pending)this.metrics.superseded++;this.lane.offer(capture,capture.capturedAtUnixMs);}
 dispatchMessage(message){if(message.type==='publicationCapture'){message.controls=[...this.clients].map(([client,c])=>({client,incarnation:c.incarnation,acks:[...c.acks.values()]}));for(const c of this.clients.values())c.acks.clear();this.metrics.dispatched++;}try{this.send(message);}catch(error){this.terminal(error.message);}}
 processed(message){this.lane.processed(message.id,message.incarnation);}
 probeResult(message){if(message.processed===true)this.processed(message);}

 packetResult(){}
 resetMetrics(){this.metrics={dispatched:0,superseded:0,receiptEvictions:0};}
 metricsSnapshot(){return {...this.metrics};}
 terminal(reason){this.close();this.fatal(reason);}
 close(){this.closed=true;this.lane.close();this.clients.clear();}
}
export function acceptsPublicationCommand(clients,message){return message.type==='join'||message.publicationIncarnation===undefined||message.publicationIncarnation===clients.get(message.client)?.publicationIncarnation;}
