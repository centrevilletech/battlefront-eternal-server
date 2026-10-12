// Admission receipts, not worker offers, authorize immutable ACK baselines.
export class FreshStateWindow {
  constructor({maxSent=128,maxAgeTicks=120,recoveryMs=250}={}) {
    this.maxSent=maxSent;this.maxAgeTicks=maxAgeTicks;this.recoveryMs=recoveryMs;
    this.joinIssued=false;this.reset(null);
  }
  reset(epoch) {this.epoch=epoch;this.sent=new Map();this.ack=null;this.lastRecovery=-Infinity;this.join=null;}
  admitted(epoch,tick,initialBootstrap=false) {
    if(epoch!==this.epoch || !Number.isSafeInteger(tick) || tick<0)return false;
    if(initialBootstrap)this.join={epoch,tick};
    this.sent.set(tick,true);
    for(const candidate of this.sent.keys())if(tick-candidate>this.maxAgeTicks)this.sent.delete(candidate);
    while(this.sent.size>this.maxSent)this.sent.delete(this.sent.keys().next().value);
    return true;
  }
  acknowledge(epoch,tick,retain) {
    if(epoch!==this.epoch || !(this.sent.has(tick)||this.join?.epoch===epoch&&this.join.tick===tick) || tick<=(this.ack?.tick??-1) || !retain(tick))return false;
    this.ack={epoch,tick};this.join=null;return true;
  }
  plan(epoch,tick,hasBaseline,now=performance.now()) {
    if(epoch!==this.epoch)this.reset(epoch);
    const base=this.ack?.tick;
    if(base!==undefined && hasBaseline(base))return base===tick?null:{baseTick:base};
    this.ack=null;
    if(now-this.lastRecovery<this.recoveryMs)return null;
    this.lastRecovery=now;
    if(!this.joinIssued){this.joinIssued=true;return {baseTick:null,initialBootstrap:true};}
    return {baseTick:null};
  }
}
