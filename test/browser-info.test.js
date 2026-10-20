import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { CONTRACT } from './contract-fixture.js';
import { browserInfo, httpsUrl, mapNames, sponsorOf } from '../src/browser-info.js';
import { OnlineServer } from '../src/online-server.js';

test('the server browser gets the description, sponsor, location, battle, rules and uptime', async t => {
  const data = await mkdtemp(join(tmpdir(), 'bfe-browser-'));
  t.after(() => rm(data, { recursive: true, force: true }));
  await mkdir(join(data, 'common', 'shell'), { recursive: true });
  await writeFile(join(data, 'common', 'shell', 'shell.json'), JSON.stringify({ maps: [{ id: 'bes2', name: 'BESPIN: CLOUD CITY' }] }));
  const config = { rotation: ['swbf1/bes2:imp:conquest', 'swbf1/bes2:rep:conquest'], autoAssignTeams: true, location: 'Alabama, US',
    description: 'Cloud City only, no bots.\u0007', sponsor: { name: 'Alabama Lightwave', url: 'https://alabamalightwave.com/', logo: 'https://alabamalightwave.com/images/al-logo-white.png' } };
  const info = browserInfo({ config, query: '?map=bes2&side=imp&ai=off&seed=1&match=' + 'a'.repeat(32), startedAt: 1000, now: 91000, names: mapNames(data) });
  assert.deepEqual(info.battle, { map: 'bes2', mapName: 'BESPIN: CLOUD CITY', side: 'imp', era: 'gcw', mode: 'conquest' });
  assert.deepEqual([info.bots, info.autoAssignTeams, info.uptime, info.location], [false, true, 90, 'Alabama, US']);
  assert.equal(info.description, 'Cloud City only, no bots.', 'control characters never reach a browser');
  assert.deepEqual(info.sponsor, { name: 'Alabama Lightwave', url: 'https://alabamalightwave.com/', logo: 'https://alabamalightwave.com/images/al-logo-white.png' });
  assert.equal(info.query, '?map=bes2&side=imp&ai=off&seed=1&match=' + 'a'.repeat(32));
  const plain = browserInfo({ config: { rotation: ['swbf1/nab2:rep:conquest'] }, query: '?map=nab2&side=rep&ai=classic&seed=1', startedAt: 0, now: 0 });
  assert.deepEqual([plain.description, plain.sponsor, plain.location, plain.bots, plain.autoAssignTeams, plain.battle.mapName, plain.battle.era], [null, null, null, true, false, null, 'cw']);
  for (const bad of ['http://example.com/', 'javascript:alert(1)', 'https://user:pass@example.com/', 'x'.repeat(600), 42]) assert.equal(httpsUrl(bad), null, String(bad).slice(0, 30));
  assert.equal(sponsorOf({ url: 'https://example.com/' }), null, 'a sponsor needs a name');
  assert.deepEqual(sponsorOf({ name: 'Me', url: 'http://insecure.example/', logo: 'https://example.com/logo.png' }), { name: 'Me', url: null, logo: 'https://example.com/logo.png' });
});

async function online(t, relayPolicy) {
  const directory = await mkdtemp(join(tmpdir(), 'bfe-online-browser-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const world = new EventEmitter(); world.networkContract = CONTRACT;
  const server = new OnlineServer({ config: { name: 'test', master: 'https://master.invalid', onlineNoticeAccepted: true, relayPolicy, rotation: ['swbf1/bes2:rep:conquest'], maxPlayers: 64, gamePort: 18452 },
    directory, world, query: '?map=bes2&side=rep&ai=off&seed=1', udp: { players: () => ['p'], port: 18452 }, browser: () => ({ description: 'Duels', bots: false }) });
  const sent = [];
  server.master = { request: async (path, method, value, options = {}) => { sent.push({ path, method, family: options.family ?? 0, listing: value }); if (server.failFamily && options.family === server.failFamily) throw Object.assign(Error('connect ENETUNREACH'), { code: 'ENETUNREACH' }); return {}; } };
  return { server, sent };
}

test('a direct server lists its game port and the browser fields, and its heartbeats take each address family in turn', async t => {
  const { server, sent } = await online(t, 'never');
  const listing = server.listing();
  assert.deepEqual(listing.endpoints, [{ port: 18452, transport: 'udp' }], 'the master adds the address each beat comes from');
  assert.deepEqual([listing.description, listing.bots, listing.players], ['Duels', false, 1]);
  for (let i = 0; i < 4; i++) await server.beat();
  assert.deepEqual(sent.map(item => item.family), [4, 6, 4, 6]);
  // A host without IPv6 to the master: that beat goes the usual way, and
  // IPv6 is left alone for a while.
  sent.length = 0; server.failFamily = 6;
  await server.beat(); await server.beat(); await server.beat(); await server.beat();
  assert.deepEqual(sent.map(item => item.family), [4, 6, 0, 4, 0], 'IPv6 failed once, then the heartbeats skip it');
  const relayed = await online(t, 'auto');
  assert.deepEqual(relayed.server.listing().endpoints, [], 'a server that may relay keeps its address private');
  await relayed.server.beat();
  assert.deepEqual(relayed.sent.map(item => item.family), [0]);
});
