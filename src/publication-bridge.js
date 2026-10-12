// Authority-to-parent capture credit is independent of remote encoding. Only
// pose captures are superseded; bounded exact receipts and the current roster
// are copied at dispatch, and each capture contains cumulative retained events.
export class PublicationBridge {
 constructor({send,fatal=()=>{},maxClients=192,maxReceipts=128,timeoutMs=2000}){Object.assign(this,{send,fatal,maxClients,maxReceipts,timeoutMs});this.clients=new Map();this.serial=0;this.metrics={dispatched:0,superseded:0,receiptEvictions:0};}
 join(client,incarnation){if(!this.clients.has(client)&&this.clients.size>=this.maxClients)throw Error('publication client capacity exceeded');this.clients.set(client,{incarnation,acks:new Map()});}
 leave(client){this.clients.delete(client);}
 receipt(type,message){if(type!=='acks')return;const c=this.clients.get(message.client);if(!c||(message.sessionIncarnation!==undefined&&message.sessionIncarnation!==c.incarnation))return;c.acks.set(message.packetId,{...message,sessionIncarnation:c.incarnation});while(c.acks.size>this.maxReceipts){c.acks.delete(c.acks.keys().next().value);this.metrics.receiptEvictions++;}}
 capture(capture){if(this.closed)return;if(this.pending)this.metrics.superseded++;this.pending=capture;this.dispatch();}
 dispatch(){if(this.closed||this.inflight||!this.pending)return;const capture=this.pending;this.pending=null;const controls=[...this.clients].map(([client,c])=>({client,incarnation:c.incarnation,acks:[...c.acks.values()]}));for(const c of this.clients.values())c.acks.clear();this.inflight=++this.serial;this.metrics.dispatched++;this.timer=setTimeout(()=>this.terminal('publication capture parent processing timed out'),this.timeoutMs);this.timer.unref?.();try{this.send({type:'publicationCapture',id:this.inflight,capture,controls});}catch(error){this.terminal(error.message);}}
 processed(message){if(this.closed||message.id!==this.inflight)return;clearTimeout(this.timer);this.inflight=null;this.dispatch();}
 packetResult(){}
 resetMetrics(){this.metrics={dispatched:0,superseded:0,receiptEvictions:0};}
 metricsSnapshot(){return {...this.metrics};}
 terminal(reason){this.close();this.fatal(reason);}
 close(){this.closed=true;clearTimeout(this.timer);this.pending=null;this.inflight=null;this.clients.clear();}
}
export function acceptsPublicationCommand(clients,message){return message.type==='join'||message.publicationIncarnation===undefined||message.publicationIncarnation===clients.get(message.client)?.publicationIncarnation;}
