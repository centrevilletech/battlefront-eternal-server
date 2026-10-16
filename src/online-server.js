import { validateNetworkContract, wireCompatibility } from './network-contract.js';
import { EventEmitter } from 'node:events';
import { isIP } from 'node:net';
import { join } from 'node:path';
import { loadIdentity, keyId, fingerprint } from './identity.js';
import { MasterClient } from './master-client.js';
import { BanCache } from './bans.js';

// Private, loopback and link-local ranges are only useful on this LAN.
export function publicAddress(address) {
  const value = String(address).replace(/^::ffff:/i, '');
  if (isIP(value) === 4) {
    const [a, b] = value.split('.').map(Number);
    return !(a === 10 || a === 127 || a === 0 || (a === 100 && b >= 64 && b < 128) || (a === 169 && b === 254) || (a === 172 && b >= 16 && b < 32) || (a === 192 && b === 168) || a >= 224);
  }
  if (isIP(value) === 6) return !/^(::1?$|f[cd]|fe[89ab]|ff)/i.test(value);
  return false;
}
export function parseCandidates(value) {
  if (!Array.isArray(value) || value.length > 8) throw Error('invalid candidates');
  return value.map(candidate => {
    if (!candidate || typeof candidate.address !== 'string' || !isIP(candidate.address.split('%')[0]) || !Number.isInteger(candidate.port) || candidate.port < 1 || candidate.port > 65535) throw Error('invalid candidate');
    return { address: candidate.address, port: candidate.port, kind: ['host', 'srflx', 'relay'].includes(candidate.kind) ? candidate.kind : 'host' };
  });
}

// Directory registration and master rendezvous. Rendezvous exchanges UDP
// candidates (host, reflexive, relayed); the session itself is the game
// port's native UDP handshake, so a lost master never drops a battle.
export class OnlineServer extends EventEmitter {
  constructor({ config, directory, world, query, udp, isBanned = () => false }) {
    super(); this.contract=validateNetworkContract(world.networkContract);this.wire=wireCompatibility(this.contract); this.isBanned=isBanned; this.config = config; this.world = world; this.query = query; this.udp = udp;
    this.keys = loadIdentity(join(directory, 'identity.json')); this.id = keyId(this.keys.publicKey);
    this.master = new MasterClient({ url: config.master, keys: this.keys, noticeAccepted: config.onlineNoticeAccepted });
    this.bans = new BanCache(this.master, join(directory, 'master-ban-key'));
    this.simulation = this.contract.simulation; this.content = config.content ?? 'bfe-content-1';
    this.relays = new Map(); this.reflexive = {}; this.offers = new Map();
    this.master.on('signal', m => this.signal(m).catch(e => this.emit('fault', e.message)));
    this.master.on('disconnected', () => { if (!this.closed) this.reconnect(); });
  }
  listing() {
    return { name: this.config.name, ...this.wire, content: this.content,
      rotation: this.config.rotation, players: this.udp?.players().length ?? 0,
      maxPlayers: this.config.maxPlayers ?? 64, relayPolicy: this.config.relayPolicy,
      endpoints: [], fingerprint: fingerprint(this.keys) };
  }
  async start() {
    if (this.config.honorGlobalBans) await this.bans.refresh();
    await this.master.request('/v1/servers', 'POST', this.listing());
    await this.master.connect();
    await this.discover();
    this.heartbeat = setInterval(() => this.master.request('/v1/servers/' + this.id, 'PUT', this.listing()).catch(e => this.emit('fault', e.message)), 20000);
    this.banTimer = setInterval(() => { if(this.config.honorGlobalBans)this.bans.refresh().then(()=>{for(const id of this.udp?.players()??[])if(this.bans.contains(id))this.udp.kick(id,'You are banned from online play.');}).catch(e=>this.emit('fault',e.message)); },120000);
    // Reflexive addresses drift with NAT mappings; relays without a
    // session are released.
    this.maintenance = setInterval(() => { this.discover().catch(() => {}); this.sweep(); }, 60000);
    for (const timer of [this.banTimer, this.heartbeat, this.maintenance]) timer.unref();
    this.emit('ready', { id: this.id });
  }
  globallyBanned(id) { return Boolean(this.config.honorGlobalBans && this.bans.contains(id)); }
  stunServer() {
    const [host, port] = String(this.config.stun ?? 'relay.battlefronteternal.com:3478').split(/:(?=\d+$)/);
    return { host, port: Number(port) || 3478 };
  }
  // This host's game port as the internet sees it, per family.
  async discover() {
    if (!this.udp || this.config.relayPolicy === 'always') return;
    try { this.reflexive = await this.udp.endpoint.reflexive(this.stunServer()); } catch { this.reflexive = {}; }
  }
  // Behind NAT when no interface holds the reflexive address.
  natted() {
    const hosts = new Set((this.udp?.candidates() ?? []).map(candidate => candidate.address));
    const mapped = Object.values(this.reflexive);
    return !mapped.length || mapped.some(entry => !hosts.has(entry.address));
  }
  candidates(clientCandidates = []) {
    if (this.config.relayPolicy === 'always' || !this.udp) return [];
    const list = this.udp.candidates().filter(candidate => publicAddress(candidate.address)).map(({ address, port }) => ({ address, port, kind: 'host' }));
    for (const entry of Object.values(this.reflexive)) if (!list.some(candidate => candidate.address === entry.address && candidate.port === entry.port)) list.push({ ...entry, kind: 'srflx' });
    // Private addresses last, and only to a player behind this host's own
    // NAT (their reflexive address is ours): they only help a player on this
    // same network, and to anyone else they would disclose it.
    const mapped = new Set(Object.values(this.reflexive).map(entry => entry.address));
    if (clientCandidates.some(candidate => mapped.has(candidate.address)))
      for (const candidate of this.udp.candidates()) if (!publicAddress(candidate.address)) list.push({ address: candidate.address, port: candidate.port, kind: 'host' });
    return list.slice(0, 8);
  }
  // Relays are capped, and short-lived until their player is admitted:
  // identities are free, so offers alone must not hold relay allocations.
  relayLimit() { return (this.config.maxPlayers ?? 64) + 8; }
  reconnect() {
    clearTimeout(this.retry); this.retry = setTimeout(async () => {
      try { await this.master.request('/v1/servers', 'POST', this.listing()); await this.master.connect(); }
      catch { if (!this.closed) this.reconnect(); }
    }, 3000); this.retry.unref();
  }
  async signal(m) {
    if (!/^[a-f0-9]{64}$/.test(m.from) || typeof m.payload !== 'string') return;
    if (this.isBanned(m.from) || this.globallyBanned(m.from)) return;
    const signal = JSON.parse(m.payload);
    // Readiness uses the existing identity-authenticated signaling route.
    // Answer only for this launched world, without allocating anything.
    if (m.type === 'matchProbe' || signal?.matchProbe !== undefined) {
      if (signal && !Array.isArray(signal) && m.type === 'matchProbe' && signal.type === 'matchProbe' && Object.keys(signal).length === 3 &&
          typeof signal.matchProbe === 'string' && typeof signal.nonce === 'string' &&
          /^[a-f0-9]{32}$/.test(signal.matchProbe) && /^[a-f0-9]{32}$/.test(signal.nonce) &&
          signal.matchProbe === new URLSearchParams(this.query).get('match'))
        this.master.signal(m.from, { type: 'matchReady', matchReady: signal.matchProbe, nonce: signal.nonce });
      return;
    }
    if (m.type !== 'offer' || signal?.udp !== 1 || !/^[a-f0-9]{32}$/.test(signal.nonce ?? '')) return;
    const now = Date.now();
    for (const [id, at] of this.offers) if (now - at > 10000) this.offers.delete(id);
    if (this.offers.size >= (this.config.maxPlayers ?? 64) * 2 || now - (this.offers.get(m.from) ?? 0) < 1000) return;
    this.offers.set(m.from, now);
    const clientCandidates = parseCandidates(signal.candidates);
    const nonce = signal.nonce, policy = this.config.relayPolicy;
    // Direct candidates first, at once; a relay follows when it is ready.
    // Punches go to public addresses only: the server has no business
    // sending to a player-named loopback, private or link-local address.
    if (policy !== 'always') {
      this.master.signal(m.from, { type: 'answer', udp: 1, nonce, candidates: this.candidates(clientCandidates) });
      this.udp.endpoint.punch(clientCandidates.filter(candidate => publicAddress(candidate.address)));
    }
    this.sweep(30000);
    if ((policy === 'always' || (policy === 'auto' && this.natted())) && !this.relays.has(m.from) && this.relays.size >= this.relayLimit()) this.emit('fault', 'relay: limit reached');
    else if (policy === 'always' || (policy === 'auto' && this.natted())) {
      try {
        const relay = await this.relayFor(m.from, clientCandidates);
        this.master.signal(m.from, { type: policy === 'always' ? 'answer' : 'candidate', udp: 1, nonce, candidates: [{ ...relay.relayed, kind: 'relay' }] });
      } catch (error) { this.emit('fault', 'relay: ' + error.message); }
    }
  }
  // A TURN allocation for one player, permitted for that player's public
  // addresses only. Short-lived per-pair credentials come from the master.
  async relayFor(id, clientCandidates) {
    const existing = this.relays.get(id);
    if (existing && !existing.closed) { await existing.ready; await existing.relay.permit(clientCandidates.map(c => c.address).filter(publicAddress)); return existing.relay; }
    const entry = { created: Date.now(), closed: false };
    this.relays.set(id, entry);
    entry.ready = (async () => {
      const servers = await this.master.credentials(this.id, id);
      const server = servers.find(item => item.relayType === 'TurnUdp');
      if (!server) throw Error('no UDP relay offered');
      const relay = await this.udp.endpoint.allocateRelay({ host: server.hostname, port: server.port, username: server.username, password: server.password });
      entry.relay = relay;
      if (entry.closed) { relay.close(); throw Error('relay no longer needed'); }
      await relay.permit(clientCandidates.map(c => c.address).filter(publicAddress));
      return relay;
    })();
    try { return await entry.ready; }
    catch (error) { this.relays.delete(id); throw error; }
  }
  sweep(maxAge = 30000) {
    const playing = new Set(this.udp?.players() ?? []);
    for (const [id, entry] of this.relays) if (!playing.has(id) && Date.now() - entry.created > maxAge) { entry.closed = true; entry.relay?.close(); this.relays.delete(id); }
  }
  // Sessions belong to the UDP endpoint (udp-server.js), not to rendezvous.
  kick() {}
  players() { return []; }
  match() {}
  async close() {
    this.closed = true; clearInterval(this.heartbeat); clearInterval(this.banTimer); clearInterval(this.maintenance); clearTimeout(this.retry);
    for (const entry of this.relays.values()) { entry.closed = true; entry.relay?.close(); }
    this.relays.clear(); this.master.close();
    await this.master.request('/v1/servers/' + this.id, 'DELETE').catch(() => {});
  }
}
