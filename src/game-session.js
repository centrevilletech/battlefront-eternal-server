import { deflate } from 'node:zlib';
const sharedCompression = new WeakMap();
export const compressionStats = { calls:0, durations:[] };
function compress(world, buffer, sharedId) {
  let cache=sharedCompression.get(world);
  if(!cache)sharedCompression.set(world,cache=new Map());
  if(sharedId!==undefined && cache.has(sharedId))return cache.get(sharedId);
  const begun=performance.now(); compressionStats.calls++;
  const promise=new Promise((resolve,reject)=>deflate(Buffer.from(buffer),{level:3},(error,result)=>{
    compressionStats.durations.push(performance.now()-begun);if(compressionStats.durations.length>4096)compressionStats.durations.shift();
    if(error)reject(error);else resolve(result.toString('base64'));
  }));
  if(sharedId!==undefined){cache.set(sharedId,promise);while(cache.size>4)cache.delete(cache.keys().next().value);}
  return promise;
}
// Application protocol above authenticated WebRTC. Reliable packets preserve the
// existing engine delta chain; inputs use the unordered state channel.
export class GameSession {
  constructor(peer, world, id, welcome) {
    this.peer = peer; this.world = world; this.id = id; this.queue = []; this.bytes = 0; this.serial = 0; this.started = false; this.pending=[]; this.rawBytes=0; this.compressionEpoch=0; this.closed=false;
    this.listener = (m) => {
      const recipient = m.recipients?.find(value=>value.client===id);
      if (m.client !== id && !recipient) return;
      if(recipient)m={...m,...recipient};
      if (m.type === 'packet') {
        if (this.awaitKeyframe && !m.keyframe) return;
        if (m.keyframe) this.awaitKeyframe = false;
        this.packet(m.buffer,m.metadata,m.sharedId);
      }
      else if (m.type === 'joined') peer.send('events', { type: 'welcome', ...welcome, ...m.player, playerId: m.player.unitId, tick: m.tick });
      else if (m.type === 'fireAck') peer.send('events',m);
      else if (m.type === 'chat') peer.send('events', m);
      else if (m.type === 'reply') { peer.send('events', m); if(m.error && m.id === undefined)peer.close(); }
    };
    world.setMaxListeners(Math.max(world.getMaxListeners(),256));
    world.on('message', this.listener);
    peer.on('message', (label, m) => this.message(label, m));
    peer.once('closed', () => this.close());
    this.timer = setInterval(() => this.flush(), 5); this.timer.unref();
    world.postMessage({ type: 'join', client: id, spectator: peer.spectator, compactMetadata:Array.isArray(peer.remote?.capabilities)&&peer.remote.capabilities.includes('compact-personal-v1') });
  }
  message(label, m) {
    if(m.type==='fire') {
      if(label!=='events')return;
      if(!Number.isSafeInteger(m.id)||!Number.isSafeInteger(m.tick)||!Number.isSafeInteger(m.generation)||![0,1].includes(m.button)||typeof m.pressed!=='boolean')return this.peer.close();
      const now=Date.now();if(now-(this.fireWindow??0)>=1000){this.fireWindow=now;this.fireCount=0;}
      if(++this.fireCount>120)return this.peer.close();
      const edge={id:m.id,tick:m.tick,generation:m.generation,epoch:m.epoch,button:m.button,pressed:m.pressed};
      for(const key of ['aimYaw','aimPitch','viewTick'])if(m[key]!==undefined)edge[key]=m[key];
      this.world.postMessage({type:'fire',client:this.id,edge});
    } else if (m.type === 'chat') {
      if (label !== 'events' || typeof m.text !== 'string' || m.text.length > 240) return;
      const now = Date.now();
      this.chatTimes = (this.chatTimes ?? []).filter(time => now-time < 5000);
      if (this.chatTimes.length >= 4) return;
      const text = m.text.replace(/[\x00-\x1f\x7f]/g, '').trim();
      if (!text) return;
      this.chatTimes.push(now); this.world.postMessage({ type:'chat', client:this.id, text, team:m.team === true });
    } else if (m.type === 'input' || m.type === 'inputs') {
      const commands = m.type === 'input' ? [m.command] : m.commands;
      if (!Array.isArray(commands) || commands.length > 32) return this.peer.close();
      const now=Date.now();if(now-(this.inputWindow??0)>=1000){this.inputWindow=now;this.inputCount=0;}
      this.inputCount+=commands.length;if(this.inputCount>512)return this.peer.close();
      this.world.postMessage({ type: 'inputs', client: this.id, commands });
    } else if (m.type === 'call' || m.type === 'rpc') {
      if (!Number.isSafeInteger(m.id) || !['init', 'start', 'suspend', 'inspect', 'resync'].includes(m.method)) {
        this.peer.send('events', { type: 'reply', id: m.id, error: 'Unsupported operation' }); return;
      }
      const now=Date.now();if(now-(this.callWindow??0)>=1000){this.callWindow=now;this.callCount=0;}
      if(++this.callCount>32)return this.peer.close();
      if(m.method==='resync'&&!this.allowResync())return;
      this.world.postMessage({ type: 'call', client: this.id, id: m.id, method: m.method, fields: m.fields ?? m.args ?? {} });
    } else if (m.type === 'resync' && this.allowResync()) this.world.postMessage({ type: 'resync', client: this.id });
  }
  allowResync() {const now=performance.now();if(now-(this.lastResync??-Infinity)<1000)return false;this.lastResync=now;return true;}
  enqueue(message) { Object.defineProperty(message,'queuedAt',{value:performance.now()}); this.queue.push(message); }
  packet(buffer, metadata, sharedId) {
    if(this.closed)return;
    if(buffer.byteLength>2*1024*1024 || this.rawBytes+this.bytes+buffer.byteLength>4*1024*1024)return this.peer.close('snapshot-memory-limit');
    const promise=compress(this.world,buffer,sharedId);
    promise.catch(()=>{});
    this.pending.push({promise,metadata,bytes:buffer.byteLength,queuedAt:performance.now(),epoch:this.compressionEpoch});
    this.rawBytes+=buffer.byteLength;
    this.drain();
  }
  async drain() {
    if(this.draining)return; this.draining=true;
    try {
      while(this.pending.length && !this.closed) {
        const item=this.pending.shift();this.inflight=item;
        const data=await item.promise;
        if(this.closed || item.epoch!==this.compressionEpoch)continue;
        this.rawBytes-=item.bytes;
        if(this.bytes+data.length>4*1024*1024){this.peer.close('compressed-memory-limit');break;}
        const id=++this.serial,total=Math.ceil(data.length/20000),extra=item.metadata?{metadata:item.metadata}:{};
        if(total===1)this.enqueue({type:'ticks',encoding:'deflate',buffers:[data],...extra});
        else for(let index=0;index<total;index++)this.enqueue({type:'packet',encoding:'deflate',id,index,total,data:data.slice(index*20000,(index+1)*20000),...(index===0?extra:{})});
        this.bytes+=data.length;this.inflight=null;this.flush();
      }
    } catch { this.peer.close('compression-or-send-error'); }
    finally {this.draining=false;this.inflight=null;}
  }
  flush() {
    if (performance.now() - Math.min(this.queue[0]?.queuedAt??Infinity,this.pending[0]?.queuedAt??Infinity,this.inflight?.queuedAt??Infinity) > 2000) {
      this.queue.length = 0; this.pending.length=0; this.inflight=null; this.rawBytes=0; this.compressionEpoch++; this.bytes = 0; this.awaitKeyframe = true;
      this.enqueue({type:'reset'});
      this.world.postMessage({type:'resync',client:this.id});
    }
    let count = 0;
    while (this.queue.length && count++ < 4) {
      const message = this.queue[0];
      if (!this.peer.send('bulk', message)) break;
      this.queue.shift(); this.bytes -= message.data?.length ?? message.buffers?.[0]?.length ?? 0;
    }
  }
  close() {
    if(this.closed)return;this.closed=true;this.compressionEpoch++;this.pending.length=0;this.rawBytes=0;
    clearInterval(this.timer); this.world.off('message', this.listener);
    this.world.postMessage({ type: 'leave', client: this.id }); this.queue.length = 0;
  }
}
