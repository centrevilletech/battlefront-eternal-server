// Fixed profile budget counts every offered application byte, including
// packets later dropped at native admission. It excludes SCTP/DTLS/IP overhead.
//
// The default is sized for the actual battle, not a dial-up link: a full
// refresh of a 32 versus 32 field (about 70 actor records of 50 bytes) costs
// about 3.5 KB, and every actor should refresh at the 20 Hz publication rate
// with headroom for combat events and the own correction. With the current
// record sizes a full field uses about 85 KB/s, so the default is 100 KB/s
// (800 kbit/s) per client; four players on an LTE hotspot need about
// 3.2 Mbit/s up. Operators with less upstream set `stateBytesPerSecond` in
// config.json. Compact delta records are the planned way down to the
// 200 kbit/s per client that comparable games spend.
export const DEFAULT_STATE_BYTES_PER_SECOND=100000;
export const MIN_STATE_BYTES_PER_SECOND=20000, MAX_STATE_BYTES_PER_SECOND=500000;
// Roughly one publication's worth of datagrams may be sent at once; a burst
// must still admit at least two 1,100-byte messages (own state and remotes).
export const burstFor=bytesPerSecond=>Math.max(2200,Math.min(11000,Math.round(bytesPerSecond/15)));
export function readStateBudget({stateBytesPerSecond}={}) {
  const rate=stateBytesPerSecond===undefined||stateBytesPerSecond===null?DEFAULT_STATE_BYTES_PER_SECOND:stateBytesPerSecond;
  if(!Number.isSafeInteger(rate)||rate<MIN_STATE_BYTES_PER_SECOND||rate>MAX_STATE_BYTES_PER_SECOND)throw Error(`stateBytesPerSecond must be an integer between ${MIN_STATE_BYTES_PER_SECOND} and ${MAX_STATE_BYTES_PER_SECOND}`);
  return {bytesPerSecond:rate,burstBytes:burstFor(rate)};
}
export class EntityBudget {
  constructor({bytesPerSecond=DEFAULT_STATE_BYTES_PER_SECOND,burstBytes=burstFor(bytesPerSecond),now=performance.now()}={}) {
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
  // Admission downstream (GameSession) can still drop an offered packet;
  // those bytes were never sent and must not stay charged to the budget.
  refund(bytes,now=performance.now()) {
    if(!Number.isSafeInteger(bytes)||bytes<1||bytes>1100)throw Error('invalid entity message size');
    this.available(now);this.tokens=Math.min(this.burst,this.tokens+bytes);this.offeredBytes=Math.max(0,this.offeredBytes-bytes);
  }
}
