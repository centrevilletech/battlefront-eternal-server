// Independent latest-only control lane. Credit is parent IPC processing, never a
// client receipt or a publication-worker completion.
export class LatestBatch {
 constructor(send,{timeoutMs=2000,fatal=()=>{}}={}){Object.assign(this,{send,timeoutMs,fatal});this.serial=0;}
 offer(recipients){if(this.closed)return;this.pending=recipients;this.dispatch();}
 dispatch(){if(this.closed||this.inflight||!this.pending)return;const recipients=this.pending;this.pending=null;this.inflight=++this.serial;this.timer=setTimeout(()=>{this.close();this.fatal('input acknowledgement transport processing timed out');},this.timeoutMs);this.timer.unref?.();this.send({type:'inputAck',inputAckBatchId:this.inflight,recipients});}
 processed(id){if(this.closed||id!==this.inflight)return;clearTimeout(this.timer);this.inflight=null;this.dispatch();}
 close(){this.closed=true;clearTimeout(this.timer);this.pending=null;this.inflight=null;}
}
