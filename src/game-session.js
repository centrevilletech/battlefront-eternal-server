import { deflateSync } from 'node:zlib';
// Application protocol above authenticated WebRTC. Reliable packets preserve the
// existing engine delta chain; inputs use the unordered state channel.
export class GameSession {
  constructor(peer, world, id, welcome) {
    this.peer = peer; this.world = world; this.id = id; this.queue = []; this.bytes = 0; this.serial = 0; this.started = false;
    this.listener = (m) => {
      if (m.client !== id) return;
      if (m.type === 'packet') {
        if (this.awaitKeyframe && !m.keyframe) return;
        if (m.keyframe) this.awaitKeyframe = false;
        this.packet(m.buffer);
      }
      else if (m.type === 'joined') peer.send('events', { type: 'welcome', ...welcome, ...m.player, playerId: m.player.unitId, tick: m.tick });
      else if (m.type === 'chat') peer.send('events', m);
      else if (m.type === 'reply') { peer.send('events', m); if(m.error && m.id === undefined)peer.close(); }
    };
    world.on('message', this.listener);
    peer.on('message', (label, m) => this.message(label, m));
    peer.once('closed', () => this.close());
    this.timer = setInterval(() => this.flush(), 5); this.timer.unref();
    world.postMessage({ type: 'join', client: id, spectator: peer.spectator });
  }
  message(label, m) {
    if (m.type === 'chat') {
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
      this.world.postMessage({ type: 'inputs', client: this.id, commands });
    } else if (m.type === 'call' || m.type === 'rpc') {
      if (!Number.isSafeInteger(m.id) || !['init', 'start', 'suspend', 'inspect', 'resync'].includes(m.method)) {
        this.peer.send('events', { type: 'reply', id: m.id, error: 'Unsupported operation' }); return;
      }
      this.world.postMessage({ type: 'call', client: this.id, id: m.id, method: m.method, fields: m.fields ?? m.args ?? {} });
    } else if (m.type === 'resync') this.world.postMessage({ type: 'resync', client: this.id });
  }
  enqueue(message) { Object.defineProperty(message,'queuedAt',{value:performance.now()}); this.queue.push(message); }
  packet(buffer) {
    const data = deflateSync(Buffer.from(buffer), { level: 3 }).toString('base64');
    if (data.length > 2 * 1024 * 1024 || this.bytes + data.length > 4 * 1024 * 1024) return this.peer.close();
    const id = ++this.serial, total = Math.ceil(data.length / 20000);
    if (total === 1) this.enqueue({ type: 'ticks', encoding: 'deflate', buffers: [data] });
    else for (let index = 0; index < total; index++) this.enqueue({ type: 'packet', encoding: 'deflate', id, index, total, data: data.slice(index * 20000, (index + 1) * 20000) });
    this.bytes += data.length;
    this.flush();
  }
  flush() {
    if (this.queue.length && performance.now() - this.queue[0].queuedAt > 2000) {
      this.queue.length = 0; this.bytes = 0; this.awaitKeyframe = true;
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
    clearInterval(this.timer); this.world.off('message', this.listener);
    this.world.postMessage({ type: 'leave', client: this.id }); this.queue.length = 0;
  }
}
