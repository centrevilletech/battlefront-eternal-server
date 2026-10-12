import {randomUUID} from 'node:crypto';
// Parent processing credit is independent of client receipts and publication.
export class LatestBatch {
 constructor(send,{timeoutMs=2000,terminalTimeoutMs=10000,fatal=()=>{},onState=()=>{},incarnation=randomUUID(),clock=()=>performance.now(),wallClock=Date.now,setTimer=setTimeout,clearTimer=clearTimeout,scheduleCheck=setImmediate,cancelCheck=clearImmediate}={}){
  Object.assign(this,{send,timeoutMs,terminalTimeoutMs,fatal,onState,incarnation,clock,wallClock,setTimer,clearTimer,scheduleCheck,cancelCheck});this.serial=0;this.stalled=false;
 }
 offer(recipients,capturedAtUnixMs=this.wallClock()){if(this.closed)return;this.pending={recipients,capturedAtUnixMs};this.dispatch();}
 dispatch(){if(this.closed||this.inflight||!this.pending)return;const batch=this.pending;this.pending=null;this.inflight=++this.serial;this.dispatchedAt=this.clock();const id=this.inflight;this.timer=this.setTimer(()=>this.expire(id),this.timeoutMs);this.timer?.unref?.();this.send({type:'inputAck',inputAckIncarnation:this.incarnation,inputAckBatchId:id,...batch});}
 expire(id){if(this.closed||this.inflight!==id)return;this.check=this.scheduleCheck(()=>{
  if(this.closed||this.inflight!==id||this.stalled)return;
  this.stalled=true;this.onState('stalled');
  const remaining=this.terminalTimeoutMs-(this.clock()-this.dispatchedAt);
  if(remaining<=0){this.fail();return;}
  this.timer=this.setTimer(()=>{if(!this.closed&&this.inflight===id)this.fail();},remaining);this.timer?.unref?.();
  this.send({type:'inputAckProbe',inputAckIncarnation:this.incarnation,inputAckBatchId:id});
 });this.check?.unref?.();}
 fail(){this.close();this.onState('failed');this.fatal('input acknowledgement transport processing timed out');}
 processed(id,incarnation){if(this.closed||incarnation!==this.incarnation||id!==this.inflight)return;this.clearTimer(this.timer);this.cancelCheck(this.check);this.check=null;this.inflight=null;if(this.stalled){this.stalled=false;this.onState('running');}this.dispatch();}
 probeResult(message){if(message.processed===true)this.processed(message.inputAckBatchId,message.inputAckIncarnation);}
 close(){this.closed=true;this.clearTimer(this.timer);this.cancelCheck(this.check);this.check=null;this.pending=null;this.inflight=null;}
}
