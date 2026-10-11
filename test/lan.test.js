import test from 'node:test';
import assert from 'node:assert/strict';
import dgram from 'node:dgram';
import { startLanDiscovery, discoverLan } from '../src/lan.js';

test('LAN discovery is local and uses the observed source address', async () => {
  const info = { id: 'a'.repeat(64), name: 'Local test', port: 18451, address: 'untrusted', protocol: 1 };
  const server = await startLanDiscovery({ port: 0, host: '127.0.0.1', getInfo: () => info });
  try {
    const found = await discoverLan({ port: server.port, targets: ['127.0.0.1'], timeout: 100 });
    assert.equal(found.length, 1); assert.equal(found[0].address, '127.0.0.1');
    assert.equal(found[0].id, info.id); assert.equal(found[0].port, 18451);
  } finally { await server.close(); }
});

test('LAN discovery rejects unsolicited replies with the wrong nonce', async () => {
  const socket = dgram.createSocket('udp4');
  await new Promise(resolve => socket.bind(0, '127.0.0.1', resolve));
  socket.on('message', (_, remote) => socket.send(JSON.stringify({ magic: 'BFE-LAN/1', type: 'server', nonce: '0'.repeat(32), info: { id: 'a'.repeat(64), name: 'Spoof', port: 18451 } }), remote.port, remote.address));
  try { assert.deepEqual(await discoverLan({ port: socket.address().port, targets: ['127.0.0.1'], timeout: 100 }), []); }
  finally { socket.close(); }
});
