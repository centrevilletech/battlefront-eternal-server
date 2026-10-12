import {randomUUID} from 'node:crypto';
// Parent processing credit is independent of client receipts and publication.
export class ProcessingLane {
 constructor(send,{timeoutMs=2000,terminalTimeoutMs=10000,describeBatch=fields=>({type:'processingBatch',...fields}),describeProbe=fields=>({type:'processingProbe',...fields}),failureMessage='transport processing timed out',fatal=()=>{},onState=()=>{},incarnation=randomUUID(),clock=()=>performance.now(),wallClock=Date.now,setTimer=setTimeout,clearTimer=clearTimeout,scheduleCheck=setImmediate,cancelCheck=clearImmediate}={}){
  Object.assign(this,{send,describeBatch,describeProbe,failureMessage,timeoutMs,terminalTimeoutMs,fatal,onState,incarnation,clock,wallClock,setTimer,clearTimer,scheduleCheck,cancelCheck});this.serial=0;this.stalled=false;
 }
 offer(recipients,capturedAtUnixMs=this.wallClock()){if(this.closed)return;this.pending={recipients,capturedAtUnixMs};this.dispatch();}
 dispatch(){if(this.closed||this.inflight||!this.pending)return;const batch=this.pending;this.pending=null;this.inflight=++this.serial;this.dispatchedAt=this.clock();const id=this.inflight;this.timer=this.setTimer(()=>this.expire(id),this.timeoutMs);this.timer?.unref?.();this.send(this.describeBatch({incarnation:this.incarnation,id,...batch}));}
 expire(id){if(this.closed||this.inflight!==id)return;this.check=this.scheduleCheck(()=>{
  if(this.closed||this.inflight!==id||this.stalled)return;
  this.stalled=true;this.onState('stalled');
  const remaining=this.terminalTimeoutMs-(this.clock()-this.dispatchedAt);
  if(remaining<=0){this.fail();return;}
  this.timer=this.setTimer(()=>{if(!this.closed&&this.inflight===id)this.fail();},remaining);this.timer?.unref?.();
  this.send(this.describeProbe({incarnation:this.incarnation,id}));
 });this.check?.unref?.();}
 fail(){this.close();this.onState('failed');this.fatal(this.failureMessage);}
 processed(id,incarnation){if(this.closed||incarnation!==this.incarnation||id!==this.inflight)return;this.clearTimer(this.timer);this.cancelCheck(this.check);this.check=null;this.inflight=null;if(this.stalled){this.stalled=false;this.onState('running');}this.dispatch();}
 close(){this.closed=true;this.clearTimer(this.timer);this.cancelCheck(this.check);this.check=null;this.pending=null;this.inflight=null;}
}
