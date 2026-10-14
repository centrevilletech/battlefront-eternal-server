// Smooth one publication over its byte budget before entering SCTP. Admission
// still belongs to the transport; queued/expired offers never become ACK bases.
export class StatePacer {
  constructor({send,drop,now=()=>performance.now(),schedule=(fn,ms)=>setTimeout(fn,ms),cancel=clearTimeout,maxBytes=11000,maxAgeMs=100}={}) {
    Object.assign(this,{send,drop,now,schedule,cancel,maxBytes,maxAgeMs});
    this.queue=[];this.bytes=0;this.nextAt=0;this.timer=null;this.closed=false;
    this.metrics={pacedPackets:0,pacedDelayTotalMs:0,pacedDelayMaxMs:0,pacedDropped:0};
  }
  discard(entry){this.bytes-=entry.bytes;this.metrics.pacedDropped++;this.drop(entry.message);}
  offer(message){
    if(this.closed)return false;
    // An unsent correction is replaceable. Its other records have not gained
    // an admission/ACK baseline either, so the scheduler can offer them again.
    if(message.ownCorrection)for(let i=this.queue.length-1;i>=0;i--)if(this.queue[i].message.ownCorrection)this.discard(this.queue.splice(i,1)[0]);
    const entry={message,bytes:message.buffer.byteLength,at:this.now()};
    this.queue.push(entry);this.bytes+=entry.bytes;
    while(this.bytes>this.maxBytes){const index=Math.max(0,this.queue.findIndex(value=>!value.message.ownCorrection));this.discard(this.queue.splice(index,1)[0]);}
    this.flush();return true;
  }
  flush(){
    if(this.closed)return;
    if(this.timer!==null){this.cancel(this.timer);this.timer=null;}
    const now=this.now();
    for(let i=this.queue.length-1;i>=0;i--)if(now-this.queue[i].at>this.maxAgeMs)this.discard(this.queue.splice(i,1)[0]);
    if(!this.queue.length)return;
    if(now>=this.nextAt){
      const index=Math.max(0,this.queue.findIndex(entry=>entry.message.ownCorrection));
      const entry=this.queue.splice(index,1)[0];this.bytes-=entry.bytes;
      const rate=entry.message.pacingBytesPerSecond;
      this.nextAt=now+entry.bytes*1000/rate;
      this.metrics.pacedPackets++;this.metrics.pacedDelayTotalMs+=now-entry.at;
      this.metrics.pacedDelayMaxMs=Math.max(this.metrics.pacedDelayMaxMs,now-entry.at);
      this.send(entry.message);
    }
    if(this.queue.length){this.timer=this.schedule(()=>{this.timer=null;this.flush();},Math.max(1,this.nextAt-this.now()));this.timer?.unref?.();}
  }
  close(){if(this.closed)return;this.closed=true;if(this.timer!==null)this.cancel(this.timer);this.timer=null;for(const entry of this.queue)this.discard(entry);this.queue=[];}
}
