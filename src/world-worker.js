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
  const game = await createGame({ headless: true, query: workerData.query ?? '?map=nab2&side=rep&ai=classic&seed=1' });
  const world = createMultiplayerWorld(game, { maxPlayers: workerData.maxPlayers ?? 32 });
  const clients = new Map(); let timer = null, last = performance.now(), debt = 0;
  const publish = () => {
    for (const [client, state] of clients) if (state.started) {
      const buffer = world.snapshot(client, { keyframe: state.keyframe });
      state.keyframe = false;
      if (buffer) parentPort.postMessage({ type: 'packet', client, buffer }, [buffer]);
    }
  };
  const step = () => { world.step(); publish(); };
  parentPort.postMessage({ type: 'ready', generation: game.playerUnit.generation });
  parentPort.on('message', async m => {
    try {
      if (m.type === 'join') {
        if (clients.has(m.client)) throw Error('already joined');
        await world.addPlayer(m.client, { spectator: m.spectator });
        clients.set(m.client, { started: false });
        parentPort.postMessage({ type: 'joined', client: m.client, player: world.player(m.client), tick: game.worldClock.tick });
      } else if (m.type === 'leave') { clients.delete(m.client); world.removePlayer(m.client); }
      else if (m.type === 'inputs') {
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
