import dgram from 'node:dgram';
import { randomBytes } from 'node:crypto';
import { networkInterfaces } from 'node:os';

const MAGIC = 'BFE-LAN/1';
export const LAN_PORT = 18452;
function parse(bytes) {
  if (bytes.length > 2048) return null;
  try { const value = JSON.parse(bytes); return value.magic === MAGIC && /^[a-f0-9]{32}$/.test(value.nonce) ? value : null; } catch { return null; }
}
export function lanTargets() {
  const targets = new Set(['255.255.255.255']);
  for (const entries of Object.values(networkInterfaces())) for (const entry of entries || []) {
    if (entry.family !== 'IPv4' || entry.internal) continue;
    const address = entry.address.split('.').map(Number), mask = entry.netmask.split('.').map(Number);
    targets.add(address.map((n, i) => n | (255 ^ mask[i])).join('.'));
  }
  return [...targets];
}
export async function startLanDiscovery({ port = LAN_PORT, host = '0.0.0.0', getInfo }) {
  const socket = dgram.createSocket('udp4'), recent = new Map();
  socket.on('message', (bytes, remote) => {
    const message = parse(bytes), now = Date.now();
    if (!message || message.type !== 'discover') return;
    if (now - (recent.get(remote.address) || 0) < 250) return;
    if (recent.size >= 1024) for (const [key, time] of recent) if (now - time > 1000) recent.delete(key);
    if (recent.size >= 1024 && !recent.has(remote.address)) return;
    recent.set(remote.address, now);
    const info = getInfo(); if (!info) return;
    const data = Buffer.from(JSON.stringify({ magic: MAGIC, type: 'server', nonce: message.nonce, info }));
    if (data.length <= 2048) socket.send(data, remote.port, remote.address, () => {});
  });
  await new Promise((resolve, reject) => { socket.once('error', reject); socket.bind(port, host, () => { socket.off('error', reject); resolve(); }); });
  socket.on('error', () => {});
  return { port: socket.address().port, close: () => new Promise(resolve => socket.close(resolve)) };
}
export async function discoverLan({ port = LAN_PORT, timeout = 750, targets = lanTargets() } = {}) {
  if (!Number.isInteger(port) || port < 1 || port > 65535 || !Number.isFinite(timeout) || timeout < 1 || timeout > 5000 || targets.length > 64) throw Error('Invalid discovery options');
  const socket = dgram.createSocket('udp4'), nonce = randomBytes(16).toString('hex'), found = new Map();
  await new Promise((resolve, reject) => { socket.once('error', reject); socket.bind(0, '0.0.0.0', () => { socket.off('error', reject); resolve(); }); });
  socket.setBroadcast(true); socket.on('error', () => {});
  socket.on('message', (bytes, remote) => {
    const message = parse(bytes), info = message?.info;
    if (message?.type !== 'server' || message.nonce !== nonce || !info || !/^[a-f0-9]{64}$/.test(info.id) || typeof info.name !== 'string' || info.name.length > 80 || !Number.isInteger(info.port) || info.port < 1 || info.port > 65535 || found.size >= 256) return;
    found.set(info.id, { ...info, address: remote.address });
  });
  const request = Buffer.from(JSON.stringify({ magic: MAGIC, type: 'discover', nonce }));
  for (const address of targets) socket.send(request, port, address, () => {});
  await new Promise(resolve => setTimeout(resolve, timeout));
  await new Promise(resolve => socket.close(resolve));
  return [...found.values()];
}
