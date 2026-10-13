import { wireCompatibility } from './network-contract.js';
import {encodeBulkFrame,JOURNAL_MAGIC,EVENTS_MAGIC,TRANSIENTS_MAGIC} from './bulk-frame.js';
import { deflate } from 'node:zlib';
import { transportTuning } from './transport-tuning.js';
const sharedCompression = new WeakMap();
// Buffered state bytes beyond which a fresh entity packet is dropped rather
// than queued: one full publication (ten 1,100-byte datagrams), so a burst
// is not dropped by its own buffered amount before SCTP flushes any of it.
export const STATE_BACKLOG_BYTES=11000;
export const compressionStats = { calls:0, durations:[] };
function compress(world, buffer, sharedId) {
  let cache=sharedCompression.get(world);
  if(!cache)sharedCompression.set(world,cache=new Map());
  if(sharedId!==undefined && cache.has(sharedId))return cache.get(sharedId);
  const begun=performance.now(),compressionStartedAtUnixMs=Date.now(); compressionStats.calls++;
  const promise=new Promise((resolve,reject)=>deflate(Buffer.from(buffer),{level:6},(error,result)=>{
    compressionStats.durations.push(performance.now()-begun);if(compressionStats.durations.length>4096)compressionStats.durations.shift();
    if(error)reject(error);else resolve({buffer:result,base64:null,compressionStartedAtUnixMs,compressionFinishedAtUnixMs:Date.now()});
  }));
  if(sharedId!==undefined){cache.set(sharedId,promise);while(cache.size>128)cache.delete(cache.keys().next().value);}
  return promise;
}
// The name a peer offered in its hello, as the roster, the scores and chat
// will show it: printable, trimmed, bounded; absent means the generic name.
export function playerName(value) {
  const text = String(value ?? '').replace(/[\x00-\x1f\x7f]/g, '').replace(/\s+/g, ' ').trim().slice(0, 32);
  return text || 'Player';
}
// Fixed entity protocol above authenticated WebRTC: one reliable join baseline,
// then independent entity state and compact inputs on the unordered channel.
export class GameSession {
  constructor(peer, world, id, welcome, options={}) {
    this.journalFrameBytes=Math.max(256,Math.min(20000,options.journalFrameBytes??transportTuning.journalFrameBytes));
    this.peer = peer; this.world = world; this.id = id; this.queue = []; this.bytes = 0; this.serial = 0; this.started = false; this.pending=[]; this.rawBytes=0; this.compressionEpoch=0; this.closed=false;
    const wire=wireCompatibility(world.networkContract);
    if(Object.entries(wire).some(([key,value])=>peer.remote?.[key]!==value)||typeof peer.sendBinary!=='function')throw Error('Update required: entity-state-v1 protocol 2');
    this.entity=true;this.fresh=true;this.journal=true;this.binary=true;
    this.transportCounters={stateGroups:0,stateBytes:0,stateDropped:0,transientGroups:0,transientBytes:0,transientDropped:0,reliableBytes:0};
    this.listener = (m) => {
      const recipient = m.recipients?.find(value=>value.client===id);
      if (m.client !== id && !recipient) return;
      if(recipient)m={...m,...recipient};
      if(m.type==='snapshotFault'){peer.send('events',{type:'gameplay-error',message:String(m.reason).slice(0,240)});setTimeout(()=>peer.close(m.reason),50).unref();}
      else if(m.type==='entityDiagnostics')peer.send('events',{type:'entity-diagnostics',actors:m.actors});
      else if(m.type==='entityEdgeResults')peer.send('state',{...m,type:'entity-edge-results'});
      else if(m.type==='entityRegistry')peer.send('events',{type:'entity-registry',registry:m.registry});
      else if(m.type==='entityPacket')this.entityPacket(m);
      else if(m.type==='journalPacket'&&m.reliable&&m.initialBootstrap&&!this.joinBootstrapQueued){this.joinBootstrapQueued=true;this.packet(m.buffer,m.metadata,m.sharedId,JOURNAL_MAGIC,{epoch:m.snapshotEpoch,tick:m.snapshotTick,initialBootstrap:true});}
      else if (m.type === 'joined') peer.send('events', { type: 'welcome', ...welcome, binaryBulk:this.binary, ...m.player, playerId: m.player.unitId, tick: m.tick });
      else if(m.type==='inputAck'){if(this.fresh)peer.send('state',{...m,type:'input-ack'});}
      else if (m.type === 'fireAck') peer.send('events',m);
      else if (m.type === 'chat') peer.send('events', m);
      else if (m.type === 'reply') { peer.send('events', m.resultType==='inspection'?{...m,transport:this.transportDiagnostics()}:m); if(m.error && m.id === undefined)peer.close('invalid-input'); }
    };
    world.setMaxListeners(Math.max(world.getMaxListeners(),256));
    world.on('message', this.listener);
    peer.on('message', (label, m) => this.message(label, m));
    peer.on('binary',(buffer,label)=>{
      if(!this.entity||label!=='state'||this.closed)return;
      const bytes=Buffer.from(buffer);if(bytes.length>1100||bytes.length<16||bytes.readUInt32LE(0)!==0x31494e42)return peer.close('invalid-entity-input');
      const now=Date.now();if(now-(this.entityInputWindow??0)>=1000){this.entityInputWindow=now;this.entityInputCount=0;this.entityInputTimes=[];}
      this.entityInputTimes.push(now-this.entityInputWindow);
      if(++this.entityInputCount>120){console.warn('entity-input-flood',JSON.stringify({count:this.entityInputCount,windowMs:now-this.entityInputWindow,receiptOffsetsMs:this.entityInputTimes.slice(-121)}));return peer.close('entity-input-flood');}
      this.world.postMessage({type:'entityInput',client:this.id,buffer:bytes});
    });
    // Each measured round trip, with the selected path, for the roster the
    // world sends everyone (the Tab player list's PING and CONNECTION).
    peer.on('ping', rtt => { if (!this.closed) this.world.postMessage({ type: 'playerLink', client: id, rtt, path: peer.path?.() ?? null }); });
    peer.once('closed', () => this.close());
    this.timer = setInterval(() => this.flush(), 5); this.timer.unref();
    world.postMessage({type:'join',client:id,spectator:peer.spectator,name:playerName(peer.remote?.name),entity:true,journal:true,fresh:true,transientSnapshots:false,relativeSnapshots:false,linearProjectiles:true,separateActions:true,compactMetadata:true});
  }
  message(label, m) {
    if(this.closed)return;
    if(this.entity&&['input','inputs','actions','fire'].includes(m.type))return this.peer.close('entity-input-requires-binary');
    if(m.type==='entity-edge-ack'){
      if(!this.entity||label!=='state'||!Number.isSafeInteger(m.epoch)||!Number.isSafeInteger(m.generation)||!Array.isArray(m.ids)||m.ids.length>32||m.ids.some(id=>!Number.isSafeInteger(id)||id<1))return;
      const now=Date.now();if(now-(this.entityAckWindow??0)>=1000){this.entityAckWindow=now;this.entityAckCount=0;}if(++this.entityAckCount>120)return this.peer.close('entity-ack-flood');
      this.world.postMessage({type:'entityEdgeAck',client:this.id,epoch:m.epoch,generation:m.generation,ids:m.ids});
    }else if(m.type==='entity-ack'){
      if(!this.entity||label!=='state'||!Number.isSafeInteger(m.epoch)||m.epoch<1||!Number.isSafeInteger(m.packetId)||m.packetId<1)return;
      const now=Date.now();if(now-(this.entityAckWindow??0)>=1000){this.entityAckWindow=now;this.entityAckCount=0;}if(++this.entityAckCount>120)return this.peer.close('entity-ack-flood');
      this.world.postMessage({type:'entityAck',client:this.id,epoch:m.epoch,packetId:m.packetId});
    }else if(m.type==='snapshot-ack'){
      if(!this.journal||label!==(this.fresh?'state':'events')||!Number.isSafeInteger(m.epoch)||!Number.isSafeInteger(m.tick))return;
      const now=Date.now();if(now-(this.snapshotAckWindow??0)>=1000){this.snapshotAckWindow=now;this.snapshotAckCount=0;}if(++this.snapshotAckCount>120)return this.peer.close('snapshot-ack-flood');
      this.world.postMessage({type:'snapshotAck',client:this.id,epoch:m.epoch,tick:m.tick});
    }else if(m.type==='snapshot-resync'){
      if(this.journal&&label===(this.fresh?'state':'events')&&this.allowResync())this.world.postMessage({type:'snapshotResync',client:this.id});
    }else if(m.type==='actions'){
      if(label!=='events'||!Number.isSafeInteger(m.id)||!Number.isSafeInteger(m.tick)||!Number.isSafeInteger(m.generation)||!Array.isArray(m.actions)||m.actions.length>64||(m.human!==undefined&&!Array.isArray(m.human))||(m.human?.length??0)>8)return this.peer.close('invalid-actions');
      const now=Date.now();if(now-(this.actionWindow??0)>=1000){this.actionWindow=now;this.actionCount=0;}if(++this.actionCount>120)return this.peer.close('action-flood');
      this.world.postMessage({type:'actions',client:this.id,envelope:{epoch:m.epoch,id:m.id,tick:m.tick,generation:m.generation,actions:m.actions,human:m.human??[]}});
    }else if(m.type==='fire') {
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
      if(this.fresh && label==='state' && m.snapshotAck)this.message('state',{type:'snapshot-ack',...m.snapshotAck});
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
  transportDiagnostics() {
    const oldest=Math.min(this.queue[0]?.queuedAt??Infinity,this.pending[0]?.queuedAt??Infinity,this.inflight?.queuedAt??Infinity);
    return {...this.transportCounters,queuedReliableBytes:this.bytes+this.rawBytes,reliableAgeMs:Number.isFinite(oldest)?Math.max(0,performance.now()-oldest):0,bulkBuffered:this.peer.channels?.get('bulk')?.bufferedAmount()??0,stateBuffered:this.peer.channels?.get('state')?.bufferedAmount()??0};
  }
  enqueue(message) { Object.defineProperty(message,'queuedAt',{value:performance.now()}); this.queue.push(message); }
  entityPacket(m) {
    if(!this.entity||this.closed)return;
    // The worker already charged these bytes to the per-client budget when
    // it offered the packet; a drop here refunds them there.
    const refund=(bytes)=>this.world.postMessage({type:'entityDropped',client:this.id,bytes});
    if(Number.isFinite(m.capturedAtUnixMs)&&Date.now()-m.capturedAtUnixMs>250){this.transportCounters.stateDropped++;refund(m.buffer.byteLength);return;}
    const bytes=Buffer.from(m.buffer);
    if(bytes.length>1100||bytes.length<30||bytes.readUInt32LE(0)!==0x31454e42)return this.peer.close('invalid-entity-packet');
    // State is expendable: never queue it behind a congested channel. A few
    // datagrams still in the transport's hands (an asynchronous send, one
    // publication's burst) are not congestion and must not cost a refresh.
    try { if((this.peer.channels?.get('state')?.bufferedAmount()??0)>STATE_BACKLOG_BYTES){this.transportCounters.stateDropped++;refund(bytes.length);return;} }
    catch { this.peer.close('channel-state-send-failed');return; }
    const accepted=this.peer.sendStatePacket?.([bytes],{maxBuffered:STATE_BACKLOG_BYTES})??this.peer.sendBinary(bytes,'state');
    if(!accepted){this.transportCounters.stateDropped++;refund(bytes.length);return;}
    this.transportCounters.stateGroups++;this.transportCounters.stateBytes+=bytes.length;
    this.world.postMessage({type:'entitySent',client:this.id,epoch:m.epoch,packetId:m.packetId,ownCorrection:!!m.ownCorrection,tick:bytes.readUInt32LE(12)});
  }
  async unorderedSnapshot(m,magic=JOURNAL_MAGIC,versionSlot='snapshotVersion') {
    const version=this[versionSlot]=m.replaceKey??((this[versionSlot]??0)+1);
    const expendable=magic===TRANSIENTS_MAGIC;
    const counter=expendable?'transient':'state';
    const stateBusy=()=>this.fresh&&(this.peer.channels?.get('state')?.bufferedAmount()??0)>0;
    if(stateBusy()){this.transportCounters[counter+'Dropped']++;return;}
    const reliableBusy=()=>this.pending.length||this.queue.length||this.inflight||(this.peer.channels?.get('bulk')?.bufferedAmount()??0)>0;
    if(m.buffer.byteLength>2*1024*1024)return this.peer.close('snapshot-memory-limit');
    try {
      const compressed=await compress(this.world,m.buffer,m.sharedId);
      if(this.closed||version!==this[versionSlot]||stateBusy()||expendable&&reliableBusy()){this.transportCounters[counter+'Dropped']++;return;}
      if(this.journal)m={...m,metadata:{...m.metadata,transportTiming:{capturedAtUnixMs:m.capturedAtUnixMs,compressionStartedAtUnixMs:compressed.compressionStartedAtUnixMs,compressionFinishedAtUnixMs:compressed.compressionFinishedAtUnixMs,offeredAtUnixMs:Date.now()}}};
      const data=compressed.buffer,id=++this.serial,size=Math.max(256,Math.ceil(data.length/128),this.journalFrameBytes-Buffer.byteLength(JSON.stringify(m.metadata??{}))-16),total=Math.ceil(data.length/size);
      const frames=[];for(let index=0;index<total;index++)frames.push(encodeBulkFrame({magic,id:id>>>0,index,total,data:data.subarray(index*size,(index+1)*size),...(index===0?{metadata:m.metadata}:{})}));
      let accepted=true;
      if(this.peer.sendStatePacket)accepted=this.peer.sendStatePacket(frames,this.fresh?{maxBuffered:0}:{});else for(const frame of frames)if(!this.peer.sendBinary(frame,'state')){accepted=false;break;}
      if(accepted){this.transportCounters[counter+'Groups']++;this.transportCounters[counter+'Bytes']+=frames.reduce((n,frame)=>n+frame.length,0);if(this.fresh&&magic===JOURNAL_MAGIC)this.world.postMessage({type:'snapshotSent',client:this.id,epoch:m.snapshotEpoch,tick:m.snapshotTick});}else this.transportCounters[counter+'Dropped']++;
    }catch{this.peer.close('snapshot-compression-error');}
  }
  packet(buffer, metadata, sharedId, magic, admission) {
    if(this.closed)return;
    if(buffer.byteLength>2*1024*1024 || this.rawBytes+this.bytes+buffer.byteLength>4*1024*1024)return this.peer.close('snapshot-memory-limit');
    const promise=compress(this.world,buffer,sharedId);
    promise.catch(()=>{});
    this.pending.push({promise,metadata,magic,admission,bytes:buffer.byteLength,queuedAt:performance.now(),epoch:this.compressionEpoch});
    this.rawBytes+=buffer.byteLength;
    this.drain();
  }
  async drain() {
    if(this.draining)return; this.draining=true;
    try {
      while(this.pending.length && !this.closed) {
        const item=this.pending.shift();this.inflight=item;
        const compressed=await item.promise;
        const data=this.binary||item.magic?compressed.buffer:(compressed.base64??=compressed.buffer.toString('base64'));
        if(this.closed || item.epoch!==this.compressionEpoch)continue;
        this.rawBytes-=item.bytes;
        if(this.bytes+data.length>4*1024*1024){this.peer.close('compressed-memory-limit');break;}
        const id=++this.serial,total=Math.ceil(data.length/20000),extra=item.metadata?{metadata:item.metadata}:{};
        if(this.binary||item.magic){for(let index=0;index<total;index++)this.enqueue({binary:encodeBulkFrame({magic:item.magic,id:id>>>0,index,total,data:data.subarray(index*20000,(index+1)*20000),...(index===0?extra:{})}),admission:index===total-1?item.admission:undefined,payloadBytes:Math.min(20000,data.length-index*20000)});}
        else if(total===1)this.enqueue({type:'ticks',encoding:'deflate',buffers:[data],...extra});
        else for(let index=0;index<total;index++)this.enqueue({type:'packet',encoding:'deflate',id,index,total,data:data.slice(index*20000,(index+1)*20000),...(index===0?extra:{})});
        this.bytes+=data.length;this.inflight=null;this.flush();
      }
    } catch { this.peer.close('compression-or-send-error'); }
    finally {this.draining=false;this.inflight=null;}
  }
  flush() {
    if (performance.now() - Math.min(this.queue[0]?.queuedAt??Infinity,this.pending[0]?.queuedAt??Infinity,this.inflight?.queuedAt??Infinity) > 2000) {
      if(this.journal)return this.peer.close('reliable-event-backlog');
      this.queue.length = 0; this.pending.length=0; this.inflight=null; this.rawBytes=0; this.compressionEpoch++; this.bytes = 0; this.awaitKeyframe = true;
      this.enqueue({type:'reset'});
      this.world.postMessage({type:this.journal?'snapshotResync':'resync',client:this.id});
    }
    let count = 0;
    while (this.queue.length && count++ < 4) {
      const message = this.queue[0];
      if (!(message.binary?this.peer.sendBinary(message.binary):this.peer.send('bulk', message))) break;
      this.transportCounters.reliableBytes+=message.binary?.length??Buffer.byteLength(JSON.stringify(message));
      if(message.admission)this.world.postMessage({type:'snapshotSent',client:this.id,...message.admission});
      this.queue.shift(); this.bytes -= message.payloadBytes ?? message.data?.length ?? message.buffers?.[0]?.length ?? 0;
    }
  }
  close() {
    if(this.closed)return;this.closed=true;this.compressionEpoch++;this.pending.length=0;this.rawBytes=0;
    clearInterval(this.timer); this.world.off('message', this.listener);
    this.world.postMessage({ type: 'leave', client: this.id }); this.queue.length = 0;
  }
}
