import { wireCompatibility } from './network-contract.js';
import { recordHop, wallNow } from './net-metrics.js';
// A snapshot behind more than this is skipped whole, and the next snapshot
// is still encoded against the client's acknowledged baseline. The native
// UDP peer reports its socket queue plus a congestion hint (udp-peer.js).
export const STATE_BACKLOG_BYTES=32768;
// A snapshot captured more than this long ago is skipped whole at forward
// time: three publication intervals of queue is already visible lag, and
// the next snapshot still carries everything against the acknowledged
// baseline. This caps worker-to-main queueing at a tenth of a second no
// matter how far the main thread falls behind.
export const STATE_STALE_MS=100;
// One 'message' listener per world routes to its sessions by client id.
// The worker posts ~64 state messages per snapshot; when every session
// held its own listener, each message cost 64 listener calls of filter
// work on the main thread that also has to forward them.
const routers=new WeakMap();
function routerFor(world){
  let router=routers.get(world);
  if(!router){
    router=new Map();
    world.setMaxListeners(Math.max(world.getMaxListeners(),256));
    // Fan-out sends are drained in slices across macrotasks: expanding a
    // 64-client publication in one synchronous burst (hundreds of native
    // sends every 33 ms) starved RTC input callbacks on this thread,
    // which then arrived as flood-like clumps while client acks lagged
    // into empty-baseline encodes. Per-client parts-before-own order is
    // preserved by the single FIFO queue; stale slices are dropped whole
    // by entityPacket's capture-age rule.
    const fanoutQueue=[];let fanoutAt=0,fanoutDraining=false;
    const drainFanout=()=>{
      let sent=0;
      while(fanoutAt<fanoutQueue.length&&sent<32){const [session,packet,publishedAt]=fanoutQueue[fanoutAt++];session.entityPacket(packet,publishedAt);sent++;}
      if(fanoutAt>=fanoutQueue.length){fanoutQueue.length=0;fanoutAt=0;fanoutDraining=false;}
      else setImmediate(drainFanout);
    };
    const enqueueFanout=(session,packet,publishedAt)=>{fanoutQueue.push([session,packet,publishedAt]);if(!fanoutDraining){fanoutDraining=true;setImmediate(drainFanout);}};
    world.on('message',m=>{
      if(m.type==='snapshotPublication'){
        // The worker encoded and transferred each baseline group's parts
        // once; expand them here, parts before each client's own
        // datagram, through the same per-session skip rules. The shared
        // buffers are only read (the peer copies per send), never mutated.
        for(const group of m.shared??[])for(const client of group.clients){const sessions=router.get(client);if(sessions)for(const session of sessions)for(const part of group.parts)enqueueFanout(session,{tick:group.tick,capturedAtUnixMs:group.capturedAtUnixMs,buffer:part},m.publishedAt);}
        for(const packet of m.own??[]){const sessions=router.get(packet.client);if(sessions)for(const session of sessions)enqueueFanout(session,packet,m.publishedAt);}
      }
      else if(m.recipients){for(const recipient of m.recipients){const sessions=router.get(recipient.client);if(sessions)for(const session of sessions)session.handleWorld({...m,...recipient});}}
      else if(m.client!=null){const sessions=router.get(m.client);if(sessions)for(const session of sessions)session.handleWorld(m);}
    });
    routers.set(world,router);
  }
  return router;
}
// The name a peer offered in its hello, as the roster, the scores and chat
// will show it: printable, trimmed, bounded; absent means the generic name.
export function playerName(value) {
  const text = String(value ?? '').replace(/[\x00-\x1f\x7f]/g, '').replace(/\s+/g, ' ').trim().slice(0, 32);
  return text || 'Player';
}
// Independent entity records establish joins and live state on the same channel.
export class GameSession {
  constructor(peer, world, id, welcome, options={}) {
    this.peer=peer;this.world=world;this.id=id;this.closed=false;
    const wire=wireCompatibility(world.networkContract);
    if(Object.entries(wire).some(([key,value])=>peer.remote?.[key]!==value)||typeof peer.sendBinary!=='function')throw Error(`Update required: ${wire.profile} protocol ${wire.protocol}`);
    this.transportCounters={stateGroups:0,stateBytes:0,stateDropped:0,stateSkippedSnapshots:0};
    this.skippedTick=-1;this.lastPacketTick=-1;
    this.handleWorld = (m) => {
      if(m.type==='snapshotFault'){peer.send('events',{type:'gameplay-error',message:String(m.reason).slice(0,240)});setTimeout(()=>peer.close(m.reason),50).unref();}
      else if(m.type==='entityDiagnostics')peer.send('events',{type:'entity-diagnostics',actors:m.actors});
      else if(m.type==='entityEdgeResults')this.edgeFeedback(m);
      else if(m.type==='entityRegistry')peer.send('events',{type:'entity-registry',registry:m.registry});
      else if(m.type==='entityPacket')this.entityPacket(m);
      else if(m.type==='entityPackets')for(const packet of m.packets)this.entityPacket(packet);
      else if(m.type==='gameplayEvents')peer.send('events',{type:'gameplay-events',tick:m.tick,records:m.records.map(r=>({...r,payload:Buffer.from(r.payload).toString('base64')}))});
      else if (m.type === 'joined') peer.send('events', { type: 'welcome', ...welcome, ...m.player, playerId: m.player.unitId, tick: m.tick });
      else if(m.type==='inputAck')this.inputAck(m);
      else if (m.type === 'chat') peer.send('events', m);
      else if (m.type === 'reply') { peer.send('events', m.resultType==='inspection'?{...m,transport:this.transportDiagnostics()}:m); if(m.error && m.id === undefined)peer.close('invalid-input'); }
    };
    this.router=routerFor(world);
    let sessions=this.router.get(id);if(!sessions)this.router.set(id,sessions=new Set());
    sessions.add(this);
    peer.on('message', (label, m) => this.message(label, m));
    peer.on('binary',(buffer,label)=>{
      if(label!=='state'||this.closed)return;
      const bytes=Buffer.from(buffer);const magic=bytes.length>=4?bytes.readUInt32LE(0):0;
      if(bytes.length>1100||!((magic===0x31494e42&&bytes.length>=16)||(magic===0x31414e42&&bytes.length===10)))return peer.close('invalid-entity-input');
      const now=Date.now();if(now-(this.entityInputWindow??0)>=1000){this.entityInputWindow=now;this.entityInputCount=0;this.entityInputTimes=[];}
      this.entityInputTimes.push(now-this.entityInputWindow);
      // Arrival bursts are not sender floods: when this thread or the network
      // hitches, seconds of legitimately paced inputs land at once.
      // Drop the excess datagrams (the next input supersedes held state)
      // and only close on a rate sustained across most of a full window,
      // which a queue drain cannot produce twice in a row.
      if(++this.entityInputCount>120){
        if(now-this.entityInputWindow<900)return;
        console.warn('entity-input-flood',JSON.stringify({count:this.entityInputCount,windowMs:now-this.entityInputWindow,receiptOffsetsMs:this.entityInputTimes.slice(-121)}));return peer.close('entity-input-flood');
      }
      this.world.postMessage({type:'entityInput',client:this.id,buffer:bytes,receivedAt:wallNow()});
    });
    // Each measured round trip, with the selected path, for the roster the
    // world sends everyone (the Tab player list's PING and CONNECTION).
    peer.on('ping', rtt => { if (!this.closed) this.world.postMessage({ type: 'playerLink', client: id, rtt, path: peer.path?.() ?? null }); });
    peer.once('closed', () => this.close());
    world.postMessage({type:'join',client:id,spectator:peer.spectator,name:playerName(peer.remote?.name),entity:true});
  }
  message(label, m) {
    if(this.closed)return;
    if(['input','inputs','actions','fire'].includes(m.type))return this.peer.close('entity-input-requires-binary');
    if(m.type==='entity-edge-ack'){
      if(label!=='state'||!Number.isSafeInteger(m.epoch)||!Number.isSafeInteger(m.generation)||!Array.isArray(m.ids)||m.ids.length>32||m.ids.some(id=>!Number.isSafeInteger(id)||id<1))return;
      const now=Date.now();if(now-(this.entityAckWindow??0)>=1000){this.entityAckWindow=now;this.entityAckCount=0;}if(++this.entityAckCount>120)return this.peer.close('entity-ack-flood');
      this.world.postMessage({type:'entityEdgeAck',client:this.id,epoch:m.epoch,generation:m.generation,ids:m.ids});
    } else if (m.type === 'chat') {
      if (label !== 'events' || typeof m.text !== 'string' || m.text.length > 240) return;
      const now = Date.now();
      this.chatTimes = (this.chatTimes ?? []).filter(time => now-time < 5000);
      if (this.chatTimes.length >= 4) return;
      const text = m.text.replace(/[\x00-\x1f\x7f]/g, '').trim();
      if (!text) return;
      this.chatTimes.push(now); this.world.postMessage({ type:'chat', client:this.id, text, team:m.team === true });
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
  inputAck(message){
    const previous=this.lastInputAck;
    // Entity timestamps already drive arrival clocks. A 20 Hz independent
    // heartbeat retains execution timing/loss recovery without a 60 Hz stream
    // of tiny JSON messages and their acknowledgements.
    if(previous&&message.epoch===previous.epoch&&message.generation===previous.generation&&message.dropped===previous.dropped&&message.serverTick-previous.serverTick<3)return;
    const {client,type,...fields}=message;
    if(this.peer.send('state',{type:'input-ack',...fields}))this.lastInputAck=message;
  }
  edgeFeedback(message){
    const key=JSON.stringify([message.epoch,message.generation,message.received,message.results]),now=performance.now();
    if(key===this.lastEdgeFeedback?.key&&now-this.lastEdgeFeedback.at<50)return;
    const {client,type,...fields}=message;
    if(this.peer.send('state',{type:'entity-edge-results',...fields}))this.lastEdgeFeedback={key,at:now};
  }
  transportDiagnostics() {
    const stats=this.peer.transportStats?.();
    return {...this.transportCounters,stateBuffered:this.peer.queuedBytes?.()??this.peer.channels?.get('state')?.bufferedAmount?.()??0,...(stats?{rtt:stats.rtt,minRtt:stats.minRtt,loss:stats.loss,packetsSent:stats.packetsSent,packetsLost:stats.packetsLost,path:stats.path}:{})};
  }
  backlog() { return this.peer.stateBacklog?.() ?? this.peer.channels?.get('state')?.bufferedAmount?.() ?? 0; }
  entityPacket(m,publishedAt) {
    if(this.closed)return;
    const bytes=Buffer.from(m.buffer);
    const magic=bytes.length>=4?bytes.readUInt32LE(0):0;
    if(bytes.length>1100||bytes.length<20||(magic!==0x31534e42&&magic!==0x314f4e42))return this.peer.close('invalid-entity-packet');
    // Whole snapshots are the unit of loss: a congested or stale snapshot
    // is skipped entire (all its parts and its own datagram), never paced.
    if(m.tick!==this.lastPacketTick){
      this.lastPacketTick=m.tick;
      let backlog=0;try{backlog=this.backlog();}catch{this.peer.close('channel-state-send-failed');return;}
      if(backlog>STATE_BACKLOG_BYTES||(Number.isFinite(m.capturedAtUnixMs)&&Date.now()-m.capturedAtUnixMs>STATE_STALE_MS)){this.skippedTick=m.tick;this.transportCounters.stateSkippedSnapshots++;}
    }
    if(m.tick===this.skippedTick){this.transportCounters.stateDropped++;return;}
    // Once a snapshot is admitted, every datagram of it is attempted: a
    // backlog check between its parts would strand it incomplete, the
    // client could never ack it, and every later delta would grow.
    const accepted=this.peer.sendStatePacket?.([bytes],{maxBuffered:Number.MAX_SAFE_INTEGER})??this.peer.sendBinary(bytes,'state');
    if(!accepted){this.transportCounters.stateDropped++;return;}
    this.transportCounters.stateGroups++;this.transportCounters.stateBytes+=bytes.length;
    if(publishedAt)recordHop('sendHop',wallNow()-publishedAt);
  }
  close() {
    if(this.closed)return;this.closed=true;
    const sessions=this.router?.get(this.id);sessions?.delete(this);if(sessions&&!sessions.size)this.router.delete(this.id);
    this.world.postMessage({ type: 'leave', client: this.id });
  }
}
