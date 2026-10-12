import { EventEmitter } from 'node:events';
import { join } from 'node:path';
import { loadIdentity, keyId } from './identity.js';
import { loadCertificate } from './certificate.js';
import { MasterClient } from './master-client.js';
import { Peer } from './peer.js';
import { BanCache } from './bans.js';
import { GameSession } from './game-session.js';
export class OnlineServer extends EventEmitter {
  constructor({ config, directory, world, query, isBanned = () => false }) {
    super(); this.isBanned=isBanned; this.config = config; this.world = world; this.query = query; this.peers = new Map(); this.pending = new Map();
    this.keys = loadIdentity(join(directory, 'identity.json')); this.id = keyId(this.keys.publicKey);
    this.certificate = loadCertificate(directory);
    this.master = new MasterClient({ url: config.master, keys: this.keys, noticeAccepted: config.onlineNoticeAccepted });
    this.bans = new BanCache(this.master, join(directory, 'master-ban-key'));
    this.simulation = 'bfe-sim-6'; this.content = config.content ?? 'bfe-content-1';
    this.master.on('signal', m => this.signal(m).catch(e => this.emit('fault', e.message)));
    this.master.on('disconnected', () => { if (!this.closed) this.reconnect(); });
  }
  listing() {
    return { name: this.config.name, protocol: 2, profile: 'entity-state-v1', simulation: this.simulation, content: this.content,
      rotation: this.config.rotation, players: [...this.peers.values()].filter(p => p.authenticated).length,
      maxPlayers: this.config.maxPlayers ?? 64, relayPolicy: this.config.relayPolicy,
      endpoints: [], fingerprint: this.certificate.fingerprint };
  }
  async start() {
    if (this.config.honorGlobalBans) await this.bans.refresh();
    await this.master.request('/v1/servers', 'POST', this.listing());
    await this.master.connect();
    this.heartbeat = setInterval(() => this.master.request('/v1/servers/' + this.id, 'PUT', this.listing()).catch(e => this.emit('fault', e.message)), 20000);
    this.banTimer = setInterval(() => { if(this.config.honorGlobalBans)this.bans.refresh().then(()=>{for(const [id,p]of this.peers)if(this.bans.contains(id))p.close();}).catch(e=>this.emit('fault',e.message)); },120000);
    this.banTimer.unref(); this.heartbeat.unref(); this.emit('ready', { id: this.id });
  }
  reconnect() {
    clearTimeout(this.retry); this.retry = setTimeout(async () => {
      try { await this.master.request('/v1/servers', 'POST', this.listing()); await this.master.connect(); }
      catch { if (!this.closed) this.reconnect(); }
    }, 3000); this.retry.unref();
  }
  async signal(m) {
    if (!/^[a-f0-9]{64}$/.test(m.from) || typeof m.payload !== 'string') return;
    if (this.isBanned(m.from) || (this.config.honorGlobalBans && this.bans.contains(m.from))) return;
    const signal = JSON.parse(m.payload);
    let peer = this.peers.get(m.from);
    if (!peer) {
      if (signal.type !== 'offer') {
        const queue = this.pending.get(m.from); if (queue && queue.length < 32) queue.push(signal); return;
      }
      if (this.peers.size + this.pending.size >= (this.config.maxPlayers ?? 64)) return;
      if (this.pending.has(m.from)) return;
      this.pending.set(m.from, []);
      let iceServers = [];
      try { if (this.config.relayPolicy !== 'never') iceServers = await this.master.credentials(this.id, m.from); }
      catch (e) { if (this.config.relayPolicy === 'always') { this.pending.delete(m.from); throw e; } }
      const portable = process.env.BFE_PORTABLE_RTC === '1' || this.config.portableRTC === true;
      // A native host uses UDP to its relay; clients may independently use TCP/TLS.
      if (!portable) iceServers = iceServers.filter(server => !server.relayType || server.relayType === 'TurnUdp');
      peer = new Peer({ name: m.from, keys: this.keys, expectedKey: m.from,
        simulation: this.simulation, content: this.content, iceServers,
        relay: this.config.relayPolicy === 'always', portable, ...this.certificate });
      this.peers.set(m.from, peer);
      peer.on('signal', s => this.master.signal(m.from, s));
      peer.once('closed', reason => {this.peers.delete(m.from);if(reason && reason!=='closed')console.warn('Online peer disconnected:',reason);});
      peer.on('fault', error => this.emit('fault', error));
      peer.once('ready', () => { if(this.isBanned(m.from)){peer.close();return;} new GameSession(peer, this.world, m.from, {
        query: this.query, protocol: 2, profile: 'entity-state-v1', simulation: this.simulation, content: this.content, serverId: this.id,
      }); });
      peer.signal(signal);
      for (const candidate of this.pending.get(m.from) ?? []) peer.signal(candidate);
      this.pending.delete(m.from);
    } else peer.signal(signal);
  }
  kick(id) { this.peers.get(id)?.close(); }
  players() { return [...this.peers].filter(([,peer])=>peer.authenticated).map(([id])=>id); }
  match(value) { for (const peer of this.peers.values()) peer.send('events', { type:'match', ...value }); }
  async close() {
    this.closed = true; clearInterval(this.heartbeat); clearInterval(this.banTimer); clearTimeout(this.retry);
    for (const peer of this.peers.values()) peer.close(); this.master.close();
    await this.master.request('/v1/servers/' + this.id, 'DELETE').catch(() => {});
  }
}
