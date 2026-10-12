// Bound work admitted from native receive callbacks while allowing network or
// scheduling delays to bunch otherwise normal 60 Hz traffic into a burst.
export class InputAdmission {
 constructor({capacity=120,rate=120,clock=()=>performance.now()}={}){this.capacity=capacity;this.rate=rate;this.clock=clock;this.tokens=capacity;this.last=clock();}
 take(){const now=Math.max(this.last,this.clock());this.tokens=Math.min(this.capacity,this.tokens+(now-this.last)*this.rate/1000);this.last=now;if(this.tokens<1)return false;this.tokens--;return true;}
}
