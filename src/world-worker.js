import { randomUUID } from 'node:crypto';
import { parentPort, workerData } from 'node:worker_threads';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
import { dataReader } from './content.js';
globalThis.fetch = await dataReader(workerData.data);
try {
  const root = resolve(workerData.engine);
  const { createGame } = await import(pathToFileURL(resolve(root, 'src/game.js')));
  const { createMultiplayerWorld } = await import(pathToFileURL(resolve(root, 'src/runtime/multiplayer-world.js')));
  const { worldState } = await import(pathToFileURL(resolve(root, 'src/runtime/world-state.js')));
  const { contentHash } = await import(pathToFileURL(resolve(root, 'src/runtime/content-hash.js')));
  const content = await contentHash(workerData.query ?? '?map=nab2&side=rep&ai=classic&seed=1');
  const game = await createGame({ headless: true, query: workerData.query ?? '?map=nab2&side=rep&ai=classic&seed=1' });
  const world = createMultiplayerWorld(game, { maxPlayers: workerData.maxPlayers ?? 32 });
  const clients = new Map(); let timer = null, last = performance.now(), debt = 0;
  const publish = () => {
    for (const [client, state] of clients) if (state.started) {
      const keyframe = Boolean(state.keyframe);
      const buffer = world.snapshot(client, { keyframe });
      state.keyframe = false;
      if (buffer) parentPort.postMessage({ type: 'packet', client, buffer, keyframe }, [buffer]);
    }
  };
  // Run the simulation at 60 Hz and send accumulated changes at 20 Hz.
  const matchId = randomUUID();
  const report = () => parentPort.postMessage({type:'scores',match:matchId,players:[...(world.players?.values() ?? [])].map(p=>({id:p.id,name:p.character.name,kills:p.character.kills-2*p.character.teamKills,deaths:p.character.deaths,points:p.character.captures}))});
  let ended = false;
  const step = () => {
    const tick = world.step(); if (tick % 3 === 0) publish(); if (tick % 300 === 0) report();
    if (game.battle.finished && !ended) { ended = true; report(); parentPort.postMessage({ type: 'finished', tick }); }
  };
  parentPort.postMessage({ type: 'ready', generation: game.playerUnit.generation, content });
  parentPort.on('message', async m => {
    try {
      if (m.type === 'join') {
        if (clients.has(m.client)) throw Error('already joined');
        await world.addPlayer(m.client, { spectator: m.spectator });
        clients.set(m.client, { started: false });
        parentPort.postMessage({ type: 'joined', client: m.client, player: world.player(m.client), tick: game.worldClock.tick });
      } else if (m.type === 'leave') { report(); clients.delete(m.client); world.removePlayer(m.client); }
      else if (m.type === 'announcement') { for(const [client,state] of clients)if(state.started)parentPort.postMessage({type:'chat',client,text:m.text,sender:'Server',playerId:0,team:false,admin:true}); }
      else if (m.type === 'chat') {
        const sender = world.player(m.client);
        if (!sender || !clients.get(m.client)?.started) return;
        for (const [client, state] of clients) {
          if (!state.started || (m.team && world.player(client)?.team !== sender.team)) continue;
          parentPort.postMessage({ type:'chat',client,text:m.text,team:m.team,playerId:sender.unitId,sender:`Player ${sender.unitId}` });
        }
      } else if (m.type === 'inputs') {
        if (!clients.get(m.client)?.started) return;
        for (const command of m.commands.slice(0,32)) world.input(m.client, command);
      } else if (m.type === 'resync') clients.get(m.client) && (clients.get(m.client).keyframe = true);
      else if (m.type === 'call') {
        if (!clients.has(m.client)) throw Error('not joined');
        const player = world.player(m.client);
        let result = { epoch: 1, tick: game.worldClock.tick };
        if (m.method === 'init') result = { ...result, type: 'ready', generation: player.generation, playerId: player.unitId, shared: false };
        else if (m.method === 'start') { clients.get(m.client).started = true; clients.get(m.client) && (clients.get(m.client).keyframe = true); result.type = 'started'; }
        else if (m.method === 'suspend') { clients.get(m.client).started = false; result.type = 'suspended'; }
        else if (m.method === 'resync') { clients.get(m.client) && (clients.get(m.client).keyframe = true); }
        else if (m.method === 'worldState') result = { type: 'state', state: worldState(game) };
        else if (m.method === 'inspect') result = { ...result, type: 'inspection', player, players: clients.size, running: Boolean(timer) };
        parentPort.postMessage({ ...result, type: 'reply', resultType: result.type, id: m.id, client: m.client });
      } else if (m.type === 'step') {
        for (let i = 0; i < Math.min(m.count ?? 1, 600); i++) step();
        parentPort.postMessage({ type: 'state', state: worldState(game) });
      } else if (m.type === 'start' && !timer) {
        last = performance.now(); timer = setInterval(() => {
          try {
            const now = performance.now(); debt = Math.min(debt + (now-last)/1000, 8/60); last = now;
            let count = 0;
            while (debt >= 1/60 && count++ < 4) { step(); debt -= 1/60; }
            if (count) parentPort.postMessage({ type: 'tick', tick: game.worldClock.tick });
          } catch(e) { clearInterval(timer); parentPort.postMessage({ type: 'error', message: e.message, stack: e.stack }); }
        }, 5);
      } else if (m.type === 'stop') { clearInterval(timer); timer = null; }
    } catch(e) {
      parentPort.postMessage(m.client ? { type: 'reply', client: m.client, id: m.id, error: e.message } : { type: 'error', message: e.message, stack: e.stack });
    }
  });
} catch(e) { parentPort.postMessage({ type: 'error', message: e.message, stack: e.stack }); }
