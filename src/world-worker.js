import { loadNetworkContract } from './network-contract.js';
import { liveActorRoster } from './actor-roster.js';
import { Session } from 'node:inspector';
import { writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { parentPort, workerData } from 'node:worker_threads';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
import { dataReader } from './content.js';
globalThis.fetch = await dataReader(workerData.data);
try {
  const root = resolve(workerData.engine);
  const networkContract = await loadNetworkContract(root);
  const { createGame } = await import(pathToFileURL(resolve(root, 'src/game.js')));
  const { createMultiplayerWorld } = await import(pathToFileURL(resolve(root, 'src/runtime/multiplayer-world.js')));
  const { worldState } = await import(pathToFileURL(resolve(root, 'src/runtime/world-state.js')));
  const { contentHash } = await import(pathToFileURL(resolve(root, 'src/runtime/content-hash.js')));
  const content = await contentHash(workerData.query ?? '?map=nab2&side=rep&ai=classic&seed=1');
  const game = await createGame({ headless: true, query: workerData.query ?? '?map=nab2&side=rep&ai=classic&seed=1' });
  const world = createMultiplayerWorld(game, { maxPlayers: workerData.maxPlayers ?? 64, armySize: workerData.armySize ?? 32 });
  const { OWN_PLAYER_SCHEMA } = await import(pathToFileURL(resolve(root, 'src/runtime/network-entity-player.js')));
  if (OWN_PLAYER_SCHEMA !== networkContract.ownCorrectionSchema) throw Error('Engine own-player schema mismatch');
  const { createEntityHost } = await import(pathToFileURL(resolve(root, 'src/runtime/entity-host.js')));
  const clients = new Map(); let timer = null, last = performance.now(), debt = 0;
  let entityHost=null;
  const ensureEntityHost=()=>{
    if(!entityHost){
      entityHost=createEntityHost({game,world,stateBytesPerSecond:workerData.stateBytesPerSecond});
      process.once('exit',()=>entityHost?.dispose());
    }
    return entityHost;
  };
  let profiler;
  const profileCall = method => new Promise((resolve,reject)=>profiler.post(method,(error,result)=>error?reject(error):resolve(result)));
  if(workerData.profile){profiler=new Session();profiler.connect();await profileCall('Profiler.enable');await profileCall('Profiler.start');}
  let threadCpuStart=process.threadCpuUsage();
  const measurements = {sim:[],encode:[],total:[],droppedTicks:0,fireAccepted:0,fireRejected:0};
  const measure = (name,value) => { if(workerData.metrics && measurements[name].length<120000)measurements[name].push(value); };
  const publish = () => {
    const begun=performance.now();
    if(entityHost){
      const out=entityHost.publish({begun,droppedTicks:Math.floor(measurements.droppedTicks)});
      for(const ack of out.inputAcks)parentPort.postMessage({type:'inputAck',...ack});
      for(const result of out.edgeResults)parentPort.postMessage({type:'entityEdgeResults',...result});
      for(const diagnostic of out.diagnostics)parentPort.postMessage({type:'entityDiagnostics',...diagnostic});
      for(const packet of out.packets)parentPort.postMessage({type:'entityPacket',...packet},[packet.buffer]);
      for(const fault of out.faults)parentPort.postMessage({type:'snapshotFault',client:fault.client,reason:'entity-prerequisite-failed:'+String(fault.error.message).slice(0,100)});
    }
    measure('encode',performance.now()-begun);
  };
  // Run the simulation at 60 Hz and send accumulated changes at 20 Hz.
  const matchId = randomUUID();
  const report = () => parentPort.postMessage({type:'scores',match:matchId,players:[...(world.players?.values() ?? [])].filter(p=>!p.spectator).map(p=>({id:p.id,name:p.character.name,kills:p.character.kills-2*p.character.teamKills,deaths:p.character.deaths,points:p.character.captures}))});
  let ended = false;
  const reportEdgeFaults=faults=>{
    for(const {client,error} of faults){
      const state=clients.get(client);if(state)state.started=false;entityHost?.setStarted(client,false);
      parentPort.postMessage({type:'snapshotFault',client,reason:'entity-edge-failed:'+String(error.message).slice(0,100)});
    }
  };
  const step = () => {
    const begun=performance.now();
    reportEdgeFaults(entityHost?.advanceEdges()??[]);
    const tick=world.step();
    reportEdgeFaults(entityHost?.consumedEdges(tick)??[]);
    measure('sim',performance.now()-begun); publish(); if (tick % 300 === 0) report();
    measure('total',performance.now()-begun);
    if (game.battle.finished && !ended) { ended = true; report(); parentPort.postMessage({ type: 'finished', tick }); }
  };
  parentPort.postMessage({ type: 'ready', generation: game.playerUnit.generation, content, networkContract });
  // A notice from the server itself, to everyone in the battle (but `except`): a player joining or leaving, the operator's announcement.
  const notice=(text,except=null)=>{for(const [client,state] of clients)if(state.started&&client!==except)parentPort.postMessage({type:'chat',client,text,sender:'Server',playerId:0,team:false,admin:true});};
  parentPort.on('message', async m => {
    try {
      if(m.type==='resetMetrics'){if(profiler){await profileCall('Profiler.stop');await profileCall('Profiler.start');}threadCpuStart=process.threadCpuUsage();for(const key of Object.keys(measurements))Array.isArray(measurements[key])?measurements[key].length=0:measurements[key]=0;parentPort.postMessage({type:'metricsReset',tick:game.worldClock.tick});}
      else if (m.type === 'metrics') { if(profiler){const {profile}=await profileCall('Profiler.stop');await writeFile(workerData.profile,JSON.stringify(profile));profiler.disconnect();profiler=null;} parentPort.postMessage({type:'metrics',threadCpu:process.threadCpuUsage(threadCpuStart),...measurements,tick:game.worldClock.tick,humans:world.players.size,units:game.units.all().length,liveUnits:game.units.all().filter(u=>u.alive&&!u.removed&&u.character).length,actorRoster:liveActorRoster(game),armies:world.roster?.counts()}); }
      else if (m.type === 'join') {
        if (clients.has(m.client)) throw Error('already joined');
        const entityRegistryMessage={type:'entityRegistry',client:m.client,registry:ensureEntityHost().descriptor()};
        // Players join at the spawn screen and choose their side unless the
        // operator configured autoSpawn / autoAssignTeams (the original's SET GAME OPTIONS).
        await world.addPlayer(m.client, { spectator: m.spectator, name: typeof m.name === 'string' && m.name ? m.name.slice(0, 32) : 'Player', reliableFire:true, separateActions:true, spawn: workerData.autoSpawn===true, teamSelect: workerData.autoAssignTeams!==true });
        clients.set(m.client, { started: false });
        entityHost.addClient(m.client);entityHost.setReady(m.client);parentPort.postMessage(entityRegistryMessage);
        parentPort.postMessage({ type:'joined', client:m.client, player:{...world.player(m.client),snapshotMode:'entity-v1'}, tick:game.worldClock.tick });
        {const name=world.players.get(m.client)?.character?.name;if(name&&!m.spectator)notice(`${name} joined the game`,m.client);}
      } else if (m.type === 'leave') { report(); {const name=clients.has(m.client)?world.players.get(m.client)?.character?.name:null;if(name)notice(`${name} left the game`,m.client);} entityHost?.removeClient(m.client);clients.delete(m.client); world.removePlayer(m.client); }
      else if(m.type==='entityInput'){
        if(clients.has(m.client))entityHost?.input(m.client,m.buffer);
      }
      else if(m.type==='playerLink'){const actor=world.players.get(m.client);if(actor&&Number.isFinite(m.rtt))actor.link={rtt:m.rtt,path:m.path&&typeof m.path==='object'?{kind:String(m.path.kind||''),family:String(m.path.family||'')}:null};}
      else if(m.type==='entityEdgeAck'){entityHost?.edgeAck(m.client,m);}
      else if(m.type==='entitySent'){if(m.epoch===1)entityHost?.admitted(m.client,m.packetId,{ownCorrection:m.ownCorrection,tick:m.tick});}
      else if(m.type==='entityDropped'){entityHost?.dropped(m.client,m.bytes);}
      else if(m.type==='entityAck'){entityHost?.ack(m.client,{epoch:m.epoch,packetId:m.packetId});}
      else if (m.type === 'announcement') notice(m.text);
      else if (m.type === 'chat') {
        const sender = world.player(m.client);
        if (!sender || !clients.get(m.client)?.started) return;
        for (const [client, state] of clients) {
          if (!state.started || (m.team && world.player(client)?.team !== sender.team)) continue;
          parentPort.postMessage({ type:'chat',client,text:m.text,team:m.team,playerId:sender.unitId,sender:world.players.get(m.client)?.character?.name || `Player ${sender.unitId}` });
        }
      } else if (m.type === 'resync') entityHost?.resync(m.client);
      else if (m.type === 'call') {
        if (!clients.has(m.client)) throw Error('not joined');
        const player = world.player(m.client);
        let result = { epoch: 1, tick: game.worldClock.tick };
        if (m.method === 'init') result = { ...result, type: 'ready', generation: player.generation, playerId: player.unitId, shared: false };
        else if (m.method === 'start') { const state=clients.get(m.client);state.started=true;entityHost?.setStarted(m.client,true);result.type='started'; }
        else if (m.method === 'suspend') { clients.get(m.client).started = false; entityHost?.setStarted(m.client,false); world.clearInput?.(m.client); result.type = 'suspended'; }
        else if (m.method === 'resync') { entityHost?.resync(m.client); }
        else if (m.method === 'worldState') result = { type: 'state', state: worldState(game) };
        else if (m.method === 'inspect') {
          const actor=world.players.get(m.client),unit=actor?.unit,weapon=unit?.combat?.arsenal?.current(0);
          result = { ...result, type: 'inspection', player:{...player,inputsStale:actor?.inputsStale,lastInputAgeMs:actor?Math.max(0,performance.now()-actor.lastInputAt):null,alive:unit?.alive,aimYaw:unit?.soldier?.aimYaw,aimPitch:unit?.soldier?.aimPitch,weapon:weapon?{clip:weapon.clip,wait:weapon.wait,reloading:weapon.reloading,held:weapon.held,bottomless:weapon.bottomless,rounds:weapon.rounds}:null,trigger:actor?.fire?.buttons?.slice(0,2),pendingFire:actor?.fire?.queue?.length,networkShot:unit?.networkShot?{id:unit.networkShot.id,tick:unit.networkShot.tick,viewTick:unit.networkShot.viewTick}:null}, entityScheduling:entityHost?.inspect(m.client),inputDiagnostics:entityHost?.inputDiagnostics(m.client),players: clients.size, running: Boolean(timer) };
        }
        parentPort.postMessage({ ...result, type: 'reply', resultType: result.type, id: m.id, client: m.client });
      } else if (m.type === 'step') {
        for (let i = 0; i < Math.min(m.count ?? 1, 600); i++) step();
        parentPort.postMessage({ type: 'state', state: worldState(game) });
      } else if (m.type === 'start' && !timer) {
        last = performance.now(); timer = setInterval(() => {
          try {
            const now = performance.now(), accumulated = debt+(now-last)/1000; measurements.droppedTicks += Math.max(0,accumulated-8/60)*60; debt = Math.min(accumulated,8/60); last = now;
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
