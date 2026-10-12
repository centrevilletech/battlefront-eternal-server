// Fixed profile budget counts every offered application byte, including
// packets later dropped at native admission. It excludes SCTP/DTLS/IP overhead.
export class EntityBudget {
  constructor({bytesPerSecond=26000,burstBytes=2200,now=performance.now()}={}) {
    if(!Number.isFinite(bytesPerSecond)||bytesPerSecond<=0||!Number.isSafeInteger(burstBytes)||burstBytes<1100)throw Error('invalid entity budget');
    this.rate=bytesPerSecond;this.burst=burstBytes;this.tokens=burstBytes;this.at=now;this.offeredBytes=0;this.denied=0;
  }
  available(now=performance.now()) {
    if(!Number.isFinite(now))throw Error('invalid budget clock');
    this.tokens=Math.min(this.burst,this.tokens+Math.max(0,now-this.at)*this.rate/1000);this.at=Math.max(this.at,now);
    return Math.floor(this.tokens);
  }
  take(bytes,now=performance.now()) {
    if(!Number.isSafeInteger(bytes)||bytes<1||bytes>1100)throw Error('invalid entity message size');
    if(this.available(now)<bytes){this.denied++;return false;}
    this.tokens-=bytes;this.offeredBytes+=bytes;return true;
  }
}
