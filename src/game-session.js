import { wireCompatibility } from './network-contract.js';
import { StatePacer } from './state-pacer.js';
export const STATE_BACKLOG_BYTES=11000;
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
    if(Object.entries(wire).some(([key,value])=>peer.remote?.[key]!==value)||typeof peer.sendBinary!=='function')throw Error('Update required: entity-state-v1 protocol 2');
    this.transportCounters={stateGroups:0,stateBytes:0,stateDropped:0};
    this.pacer=new StatePacer({send:m=>this.entityPacket(m,true),drop:m=>this.dropEntityPacket(m)});
    this.listener = (m) => {
      const recipient = m.recipients?.find(value=>value.client===id);
      if (m.client !== id && !recipient) return;
      if(recipient)m={...m,...recipient};
      if(m.type==='snapshotFault'){peer.send('events',{type:'gameplay-error',message:String(m.reason).slice(0,240)});setTimeout(()=>peer.close(m.reason),50).unref();}
      else if(m.type==='entityDiagnostics')peer.send('events',{type:'entity-diagnostics',actors:m.actors});
      else if(m.type==='entityEdgeResults')this.edgeFeedback(m);
      else if(m.type==='entityRegistry')peer.send('events',{type:'entity-registry',registry:m.registry});
      else if(m.type==='entityPacket')this.entityPacket(m);
      else if (m.type === 'joined') peer.send('events', { type: 'welcome', ...welcome, ...m.player, playerId: m.player.unitId, tick: m.tick });
      else if(m.type==='inputAck')this.inputAck(m);
      else if (m.type === 'chat') peer.send('events', m);
      else if (m.type === 'reply') { peer.send('events', m.resultType==='inspection'?{...m,transport:this.transportDiagnostics()}:m); if(m.error && m.id === undefined)peer.close('invalid-input'); }
    };
    world.setMaxListeners(Math.max(world.getMaxListeners(),256));
    world.on('message', this.listener);
    peer.on('message', (label, m) => this.message(label, m));
    peer.on('binary',(buffer,label)=>{
      if(label!=='state'||this.closed)return;
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
    world.postMessage({type:'join',client:id,spectator:peer.spectator,name:playerName(peer.remote?.name),entity:true});
  }
  message(label, m) {
    if(this.closed)return;
    if(['input','inputs','actions','fire'].includes(m.type))return this.peer.close('entity-input-requires-binary');
    if(m.type==='entity-edge-ack'){
      if(label!=='state'||!Number.isSafeInteger(m.epoch)||!Number.isSafeInteger(m.generation)||!Array.isArray(m.ids)||m.ids.length>32||m.ids.some(id=>!Number.isSafeInteger(id)||id<1))return;
      const now=Date.now();if(now-(this.entityAckWindow??0)>=1000){this.entityAckWindow=now;this.entityAckCount=0;}if(++this.entityAckCount>120)return this.peer.close('entity-ack-flood');
      this.world.postMessage({type:'entityEdgeAck',client:this.id,epoch:m.epoch,generation:m.generation,ids:m.ids});
    }else if(m.type==='entity-ack'){
      if(label!=='state'||!Number.isSafeInteger(m.epoch)||m.epoch<1||!Number.isSafeInteger(m.packetId)||m.packetId<1)return;
      const now=Date.now();if(now-(this.entityAckWindow??0)>=1000){this.entityAckWindow=now;this.entityAckCount=0;}if(++this.entityAckCount>120)return this.peer.close('entity-ack-flood');
      this.world.postMessage({type:'entityAck',client:this.id,epoch:m.epoch,packetId:m.packetId});
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
    // of tiny JSON messages and their reverse-direction SCTP receipts.
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
    return {...this.transportCounters,...this.pacer.metrics,stateQueued:this.pacer.bytes,stateBuffered:this.peer.channels?.get('state')?.bufferedAmount()??0};
  }
  dropEntityPacket(m){this.transportCounters.stateDropped++;this.world.postMessage({type:'entityDropped',client:this.id,epoch:m.epoch,packetId:m.packetId});}
  entityPacket(m,paced=false) {
    if(this.closed)return;
    if(!paced&&Number.isInteger(m.pacingBytesPerSecond)&&m.pacingBytesPerSecond>=20000&&m.pacingBytesPerSecond<=500000){this.pacer.offer(m);return;}
    // The host refunds only this offer's charged bytes, once. A forced own
    // correction may not have spent budget at all.
    if(Number.isFinite(m.capturedAtUnixMs)&&Date.now()-m.capturedAtUnixMs>250){this.dropEntityPacket(m);return;}
    const bytes=Buffer.from(m.buffer);
    if(bytes.length>1100||bytes.length<30||bytes.readUInt32LE(0)!==0x31454e42)return this.peer.close('invalid-entity-packet');
    // State is expendable: never queue it behind a congested channel. A few
    // datagrams still in the transport's hands (an asynchronous send, one
    // publication's burst) are not congestion and must not cost a refresh.
    try { if((this.peer.channels?.get('state')?.bufferedAmount()??0)>STATE_BACKLOG_BYTES){this.dropEntityPacket(m);return;} }
    catch { this.peer.close('channel-state-send-failed');return; }
    const accepted=this.peer.sendStatePacket?.([bytes],{maxBuffered:STATE_BACKLOG_BYTES})??this.peer.sendBinary(bytes,'state');
    if(!accepted){this.dropEntityPacket(m);return;}
    this.transportCounters.stateGroups++;this.transportCounters.stateBytes+=bytes.length;
    this.world.postMessage({type:'entitySent',client:this.id,epoch:m.epoch,packetId:m.packetId,ownCorrection:!!m.ownCorrection,tick:bytes.readUInt32LE(12)});
  }
  close() {
    if(this.closed)return;this.closed=true;
    this.pacer.close();
    this.world.off('message', this.listener);
    this.world.postMessage({ type: 'leave', client: this.id });
  }
}
