// Independent latest-only control lane. Credit is parent IPC processing, never a
// client receipt or a publication-worker completion.
export class LatestBatch {
 constructor(send,{timeoutMs=2000,fatal=()=>{},scheduleCheck=setImmediate,cancelCheck=clearImmediate}={}){Object.assign(this,{send,timeoutMs,fatal,scheduleCheck,cancelCheck});this.serial=0;}
 offer(recipients){if(this.closed)return;this.pending=recipients;this.dispatch();}
 dispatch(){if(this.closed||this.inflight||!this.pending)return;const recipients=this.pending;this.pending=null;this.inflight=++this.serial;const id=this.inflight;this.timer=setTimeout(()=>this.expire(id),this.timeoutMs);this.timer.unref?.();this.send({type:'inputAck',inputAckBatchId:this.inflight,recipients});}
 // A deadline can run before already-queued IPC credit after a host pause.
 // One poll/check turn resolves that ordering race without another queued batch.
 expire(id){if(this.closed||this.inflight!==id)return;this.check=this.scheduleCheck(()=>{if(this.closed||this.inflight!==id)return;this.close();this.fatal('input acknowledgement transport processing timed out');});this.check?.unref?.();}
 processed(id){if(this.closed||id!==this.inflight)return;clearTimeout(this.timer);this.cancelCheck(this.check);this.check=null;this.inflight=null;this.dispatch();}
 close(){this.closed=true;clearTimeout(this.timer);this.cancelCheck(this.check);this.check=null;this.pending=null;this.inflight=null;}
}
