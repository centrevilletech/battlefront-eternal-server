import { wireCompatibility } from './network-contract.js';
import { countNet, recordHop, wallNow } from './net-metrics.js';
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
// A session that has sent no application message (input, snapshot ack
// keepalive, call, chat) for this long is closed. Clients send at least a
// 10 Hz ack keepalive from their first snapshot on (spectators and paused
// players too), so this only catches a peer whose transport never timed
// out: a ghost that would otherwise hold a seat and its world recipient.
export const SESSION_SILENT_MS=30000;
// Before its first `start` a client is loading the battle and sends
// nothing: a slow machine loading a large map must not be closed for that.
export const SESSION_JOIN_SILENT_MS=180000;
// Inputs per session: a token bucket refilled at this rate up to this
// burst. Clients send one input datagram per 60 Hz tick (or a 10 Hz ack
// keepalive instead); arrival bursts after a server or network hitch land
// seconds of legitimate input at once. The excess is dropped (the next
// input supersedes held state), never kicked on: rate alone is not abuse.
export const INPUT_RATE_PER_SECOND=75,INPUT_BURST=360;
// The world thread's messages are handled in slices of about this long,
// yielding with setImmediate between them so the socket's reads and the
// transport's timers keep running however deep the backlog gets.
export const WORLD_SLICE_MS=4;
// One 'message' listener per world routes to its sessions by client id.
// The handler itself only enqueues (O(1)); a worker backlog of thousands of
// messages once starved this thread of socket reads and timers (inbound
// datagrams dropped by the kernel, idle timeouts never fired).
// The drain then:
// - routes client-addressed messages (welcome, replies, chat, events) in
//   arrival order;
// - keeps only each session's newest input ACK and edge results (both are
//   cumulative: the newer one carries everything the older did);
// - keeps only each session's newest snapshot (a snapshot is the unit of
//   loss and the next one is encoded against the acknowledged baseline),
//   in a map bounded by the number of sessions;
// - drops whole publications older than STATE_STALE_MS at enqueue.
// A publication's per-session work is only sent after every message that
// arrived before it, so a snapshot never overtakes an earlier welcome.
const routers=new WeakMap();
function routerFor(world){
  let router=routers.get(world);
  if(!router){
    router=new WorldRouter(world);
    routers.set(world,router);
  }
  return router;
}
const publicationAge=m=>{
  if(Number.isFinite(m.publishedAt))return wallNow()-m.publishedAt;
  const capturedAt=m.shared?.[0]?.capturedAtUnixMs??m.own?.[0]?.capturedAtUnixMs;
  return Number.isFinite(capturedAt)?Date.now()-capturedAt:0;
};
class WorldRouter{
  constructor(world){
    this.world=world;this.sessions=new Map();
    this.inbox=[];this.head=0;this.scheduled=false;
    this.snapshots=new Map();this.acks=new Map();this.edges=new Map();
    this.inputs=[];this.inputTransfer=[];this.inputsScheduled=false;
    this.liveness=null;
    this.drain=()=>this.drainSlice();
    this.flushInputs=()=>this.postInputs();
    world.setMaxListeners?.(Math.max(world.getMaxListeners?.()??10,256));
    world.on('message',m=>{
      if(m?.type==='snapshotPublication'&&publicationAge(m)>STATE_STALE_MS){countNet('stalePublications');return;}
      this.inbox.push(m);this.schedule();
    });
  }
  get(client){return this.sessions.get(client);}
  add(session){
    let sessions=this.sessions.get(session.id);if(!sessions)this.sessions.set(session.id,sessions=new Set());
    sessions.add(session);
    if(!this.liveness){this.liveness=setInterval(()=>this.checkLiveness(),5000);this.liveness.unref?.();}
  }
  remove(session){
    const sessions=this.sessions.get(session.id);sessions?.delete(session);if(sessions&&!sessions.size)this.sessions.delete(session.id);
    this.snapshots.delete(session);this.acks.delete(session);this.edges.delete(session);
    if(!this.sessions.size){clearInterval(this.liveness);this.liveness=null;}
  }
  checkLiveness(){
    const now=performance.now();
    for(const sessions of this.sessions.values())for(const session of sessions)if(now-session.heardAt>(session.started?SESSION_SILENT_MS:SESSION_JOIN_SILENT_MS))this.guard(session,()=>session.peer.close('idle','No messages from the client.'));
  }
  schedule(){if(!this.scheduled){this.scheduled=true;setImmediate(this.drain);}}
  // One session's failure closes that session, never the thread.
  guard(session,work){
    try{work();session.peer.flush?.();}
    catch(error){
      countNet('sessionFaults');console.warn('Session failed:',String(error?.stack??error).slice(0,400));
      try{session.peer.close('session-failed');}catch{}
      session.close();
    }
  }
  each(client,work){const sessions=this.sessions.get(client);if(sessions)for(const session of sessions)this.guard(session,()=>work(session));}
  route(m){
    if(m.type==='snapshotPublication'){
      if(publicationAge(m)>STATE_STALE_MS){countNet('stalePublications');return;}
      // The worker encoded and transferred each baseline group's parts
      // once; each listed session takes a reference (parts before own).
      // The shared buffers are only read (the peer copies per send).
      for(const group of m.shared??[])for(const client of group.clients){
        const sessions=this.sessions.get(client);if(!sessions)continue;
        for(const session of sessions)this.snapshot(session,group.tick,group.capturedAtUnixMs,m.publishedAt).parts=group.parts;
      }
      for(const packet of m.own??[]){
        const sessions=this.sessions.get(packet.client);if(!sessions)continue;
        for(const session of sessions){
          let work=this.snapshots.get(session);
          if(!work||work.tick!==packet.tick)work=this.snapshot(session,packet.tick,packet.capturedAtUnixMs,m.publishedAt);
          work.own.push(packet.buffer);
        }
      }
    }
    else if(m.type==='inputAck'&&m.recipients){for(const recipient of m.recipients)this.latest(this.acks,m,recipient);}
    else if(m.type==='entityEdgeResults'&&m.recipients){for(const recipient of m.recipients)this.latest(this.edges,m,recipient);}
    else if(m.type==='gameplayEvents'&&Array.isArray(m.clients))this.broadcastEvents(m);
    else if(m.recipients){for(const recipient of m.recipients)this.each(recipient.client,session=>session.handleWorld(WorldRouter.addressed(m,recipient)));}
    else if(m.client!=null)this.each(m.client,session=>session.handleWorld(m));
  }
  // The newest snapshot for a session replaces any it had not yet sent.
  snapshot(session,tick,capturedAtUnixMs,publishedAt){
    const previous=this.snapshots.get(session);
    if(previous){
      if(previous.tick===tick)return previous;
      countNet('supersededSnapshots');session.transportCounters.stateSkippedSnapshots++;this.snapshots.delete(session);
    }
    const work={tick,capturedAtUnixMs,publishedAt,parts:null,own:[]};
    this.snapshots.set(session,work);
    return work;
  }
  // One recipient's copy of a world message addressed to several: its own
  // fields only, never the whole recipients list (63 players' feedback in
  // every client's input ack or edge result, over 40 KB at 64 players).
  static addressed(m,recipient){const {recipients,...base}=m;return {...base,...recipient};}
  latest(map,m,recipient){const sessions=this.sessions.get(recipient.client);if(sessions)for(const session of sessions)map.set(session,[m,recipient]);}
  // Gameplay events cross from the world once with their recipients and
  // are serialized once for all of them. A batch over the transport's
  // message bound is split by records rather than dropped.
  broadcastEvents(m){
    const sessions=[];for(const client of m.clients){const set=this.sessions.get(client);if(set)for(const session of set)if(!session.closed)sessions.push(session);}
    if(!sessions.length)return;
    const encode=records=>JSON.stringify({type:'gameplay-events',tick:m.tick,records:records.map(r=>({...r,payload:Buffer.from(r.payload).toString('base64')}))});
    const pieces=[],pending=[m.records];
    while(pending.length){
      const records=pending.pop(),data=encode(records);
      if(Buffer.byteLength(data)<=32768||records.length<2)pieces.push(data);
      else{const half=Math.ceil(records.length/2);pending.push(records.slice(half),records.slice(0,half));}
    }
    for(const session of sessions)this.guard(session,()=>{for(const data of pieces)session.sendEvents(data);});
  }
  drainSlice(){
    this.scheduled=false;
    const begun=performance.now(),deadline=begun+WORLD_SLICE_MS,inboxDeadline=begun+WORLD_SLICE_MS/2;
    const inbox=this.inbox;
    while(this.head<inbox.length){
      const m=inbox[this.head];inbox[this.head++]=undefined;
      try{this.route(m);}catch(error){console.warn('World message failed:',String(error?.stack??error).slice(0,400));}
      if(performance.now()>=inboxDeadline)break;
    }
    // Compact: never retain handled entries behind the read position.
    if(this.head>=inbox.length){inbox.length=0;this.head=0;}
    else if(this.head>=1024){inbox.splice(0,this.head);this.head=0;}
    this.flush(this.acks,(session,[m,recipient])=>session.inputAck(WorldRouter.addressed(m,recipient)),deadline);
    this.flush(this.edges,(session,[m,recipient])=>session.edgeFeedback(WorldRouter.addressed(m,recipient)),deadline);
    this.flush(this.snapshots,(session,work)=>session.sendSnapshot(work),deadline);
    if(this.head<inbox.length||this.acks.size||this.edges.size||this.snapshots.size)this.schedule();
  }
  // At least one entry per slice, then as many as the deadline allows.
  flush(map,send,deadline){
    let first=true;
    for(const [session,value] of map){
      if(!first&&performance.now()>=deadline)return;
      first=false;map.delete(session);
      this.guard(session,()=>send(session,value));
    }
  }
  // Inputs reach the world batched: one message (exact-size buffers in
  // its transfer list) per event-loop pass instead of one per datagram.
  input(entry,buffer){
    this.inputs.push(entry);this.inputTransfer.push(buffer);
    if(!this.inputsScheduled){this.inputsScheduled=true;setImmediate(this.flushInputs);}
  }
  postInputs(){
    this.inputsScheduled=false;
    if(!this.inputs.length)return;
    const inputs=this.inputs,transfer=this.inputTransfer;this.inputs=[];this.inputTransfer=[];
    this.world.postMessage({type:'entityInputs',inputs},transfer);
  }
  // Anything else a session posts goes after the inputs it already queued.
  post(message){this.postInputs();this.world.postMessage(message);}
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
    this.transportCounters={stateGroups:0,stateBytes:0,stateDropped:0,stateSkippedSnapshots:0,inputsDropped:0};
    this.skippedTick=-1;this.lastPacketTick=-1;
    this.heardAt=performance.now();
    this.inputTokens=INPUT_BURST;this.inputRefilledAt=this.heardAt;
    this.handleWorld = (m) => {
      if(m.type==='snapshotFault'){peer.send('events',{type:'gameplay-error',message:String(m.reason).slice(0,240)});setTimeout(()=>peer.close(m.reason),50).unref();}
      // The world evicted this client (no acknowledgement for too long).
      else if(m.type==='clientEvicted')peer.close('idle',String(m.reason??'No messages from the client.').slice(0,120));
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
    this.router.add(this);
    peer.on('message', (label, m) => this.guarded(() => this.message(label, m)));
    peer.on('binary',(buffer,label)=>this.guarded(()=>this.binary(buffer,label)));
    // Each measured round trip, with the selected path, for the roster the
    // world sends everyone (the Tab player list's PING and CONNECTION).
    peer.on('ping', rtt => { if (!this.closed) this.router.post({ type: 'playerLink', client: id, rtt, path: peer.path?.() ?? null }); });
    peer.once('closed', () => this.close());
    this.router.post({type:'join',client:id,spectator:peer.spectator,name:playerName(peer.remote?.name),entity:true});
  }
  // A fault in one session's handling closes that session only.
  guarded(work){
    try{work();}
    catch(error){
      if(this.closed)return;
      countNet('sessionFaults');console.warn('Session failed:',String(error?.stack??error).slice(0,400));
      try{this.peer.close('session-failed');}catch{}
      this.close();
    }
  }
  // Token bucket: refill since the last datagram, then spend one.
  admitInput(now){
    this.inputTokens=Math.min(INPUT_BURST,this.inputTokens+(now-this.inputRefilledAt)*INPUT_RATE_PER_SECOND/1000);this.inputRefilledAt=now;
    if(this.inputTokens<1)return false;
    this.inputTokens--;return true;
  }
  binary(buffer,label){
    if(label!=='state'||this.closed)return;
    const now=performance.now();this.heardAt=now;
    // Over the bucket's rate the datagram is dropped before any copy.
    if(!this.admitInput(now)){
      this.transportCounters.inputsDropped++;countNet('inputsDropped');
      if(now-(this.inputDropLoggedAt??-Infinity)>=60000){this.inputDropLoggedAt=now;console.warn('entity-input-rate',JSON.stringify({client:String(this.id).slice(0,16),dropped:this.transportCounters.inputsDropped}));}
      return;
    }
    // A view of only this message's bytes: newer engines hand over a
    // Uint8Array view of the whole decrypted packet's buffer, so neither
    // the view's backing buffer nor a pool slice may cross as is.
    const bytes=Buffer.isBuffer(buffer)?buffer:ArrayBuffer.isView(buffer)?Buffer.from(buffer.buffer,buffer.byteOffset,buffer.byteLength):Buffer.from(buffer);
    const magic=bytes.length>=4?bytes.readUInt32LE(0):0;
    if(bytes.length>1100||!((magic===0x31494e42&&bytes.length>=16)||(magic===0x31414e42&&bytes.length===10)))return this.peer.close('invalid-entity-input');
    // An exact-size copy crosses to the world in the transfer list; a
    // Buffer view would clone its whole (8 KiB pool or datagram) backing
    // store with every input.
    const copy=new ArrayBuffer(bytes.length);new Uint8Array(copy).set(bytes);
    this.router.input({client:this.id,buffer:copy,receivedAt:wallNow()},copy);
  }
  message(label, m) {
    if(this.closed)return;
    this.heardAt=performance.now();
    if(['input','inputs','actions','fire'].includes(m.type))return this.peer.close('entity-input-requires-binary');
    if(m.type==='entity-edge-ack'){
      if(label!=='state'||!Number.isSafeInteger(m.epoch)||!Number.isSafeInteger(m.generation)||!Array.isArray(m.ids)||m.ids.length>32||m.ids.some(id=>!Number.isSafeInteger(id)||id<1))return;
      const now=Date.now();if(now-(this.entityAckWindow??0)>=1000){this.entityAckWindow=now;this.entityAckCount=0;}if(++this.entityAckCount>120)return this.peer.close('entity-ack-flood');
      this.router.post({type:'entityEdgeAck',client:this.id,epoch:m.epoch,generation:m.generation,ids:m.ids});
    } else if (m.type === 'chat') {
      if (label !== 'events' || typeof m.text !== 'string' || m.text.length > 240) return;
      const now = Date.now();
      this.chatTimes = (this.chatTimes ?? []).filter(time => now-time < 5000);
      if (this.chatTimes.length >= 4) return;
      const text = m.text.replace(/[\x00-\x1f\x7f]/g, '').trim();
      if (!text) return;
      this.chatTimes.push(now); this.router.post({ type:'chat', client:this.id, text, team:m.team === true });
    } else if (m.type === 'call' || m.type === 'rpc') {
      if (!Number.isSafeInteger(m.id) || !['init', 'start', 'suspend', 'inspect', 'resync'].includes(m.method)) {
        this.peer.send('events', { type: 'reply', id: m.id, error: 'Unsupported operation' }); return;
      }
      const now=Date.now();if(now-(this.callWindow??0)>=1000){this.callWindow=now;this.callCount=0;}
      if(++this.callCount>32)return this.peer.close();
      if(m.method==='resync'&&!this.allowResync())return;
      if(m.method==='start')this.started=true;
      this.router.post({ type: 'call', client: this.id, id: m.id, method: m.method, fields: m.fields ?? m.args ?? {} });
    } else if (m.type === 'resync' && this.allowResync()) this.router.post({ type: 'resync', client: this.id });
  }
  allowResync() {const now=performance.now();if(now-(this.lastResync??-Infinity)<1000)return false;this.lastResync=now;return true;}
  inputAck(message){
    const previous=this.lastInputAck;
    // Entity timestamps already drive arrival clocks. A 20 Hz independent
    // heartbeat retains execution timing/loss recovery without a 60 Hz stream
    // of tiny JSON messages and their acknowledgements.
    // The dropped-tick total is not a reason to send early: it grows on
    // every publication while the server is behind, which turned this
    // heartbeat back into one message per tick exactly when the server
    // could least afford it. The latest total rides the next heartbeat.
    if(previous&&message.epoch===previous.epoch&&message.generation===previous.generation&&message.serverTick-previous.serverTick<3)return;
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
  // One snapshot's datagrams, shared parts before own, through the
  // whole-snapshot skip rules of entityPacket.
  sendSnapshot(work){
    if(this.closed)return;
    for(const buffer of work.parts??[])this.entityPacket({tick:work.tick,capturedAtUnixMs:work.capturedAtUnixMs,buffer},work.publishedAt);
    for(const buffer of work.own)this.entityPacket({tick:work.tick,capturedAtUnixMs:work.capturedAtUnixMs,buffer},work.publishedAt);
  }
  // An events message already serialized for every recipient.
  sendEvents(data){
    if(this.closed)return false;
    if(typeof this.peer.sendEncoded==='function')return this.peer.sendEncoded('events',data,'gameplay-events');
    return this.peer.send('events',JSON.parse(data));
  }
  close() {
    if(this.closed)return;this.closed=true;
    this.router?.remove(this);
    this.router.post({ type: 'leave', client: this.id });
  }
}
