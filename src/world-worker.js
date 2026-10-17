import { loadNetworkContract } from './network-contract.js';
import { liveActorRoster } from './actor-roster.js';
import { Session } from 'node:inspector';
import { writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { parentPort, receiveMessageOnPort, workerData } from 'node:worker_threads';
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
  const { createSnapshotHost } = await import(pathToFileURL(resolve(root, 'src/runtime/snapshot-host.js')));
  const clients = new Map(); let timer = null, last = performance.now(), debt = 0;
  let entityHost=null;
  const ensureEntityHost=()=>{
    if(!entityHost){
      // The per-client state budget (config stateBytesPerSecond); an engine
      // without the option ignores it.
      const stateBytesPerSecond=Number(workerData.stateBytesPerSecond);
      entityHost=createSnapshotHost({game,world,...(Number.isFinite(stateBytesPerSecond)&&stateBytesPerSecond>0?{stateBytesPerSecond}:{})});
      process.once('exit',()=>entityHost?.dispose());
    }
    return entityHost;
  };
  let profiler;
  const profileCall = method => new Promise((resolve,reject)=>profiler.post(method,(error,result)=>error?reject(error):resolve(result)));
  if(workerData.profile){profiler=new Session();profiler.connect();await profileCall('Profiler.enable');await profileCall('Profiler.start');}
  let threadCpuStart=process.threadCpuUsage();
  const measurements = {sim:[],encode:[],total:[],inputHop:[],droppedTicks:0,fireAccepted:0,fireRejected:0};
  // Client traffic (inputs in; snapshots and per-client events out) runs
  // over a direct port to the network thread when one is attached, never
  // through the main thread. Control messages stay on parentPort.
  let clientPort=null;
  const toClients=(message,transfer)=>(clientPort??parentPort).postMessage(message,transfer);
  const wallNow=()=>performance.timeOrigin+performance.now();
  const measure = (name,value) => { if(workerData.metrics && measurements[name].length<120000)measurements[name].push(value); };
  const publish = () => {
    const begun=performance.now();
    if(entityHost){
      const out=entityHost.publish({begun,droppedTicks:Math.floor(measurements.droppedTicks)});
      // One worker message per kind (acks and edge results ride the
      // recipients list) and one per client for its snapshot datagrams:
      // per-datagram postMessages dominated publication at 64 clients.
      if(out.inputAcks.length)toClients({type:'inputAck',recipients:out.inputAcks});
      if(out.edgeResults.length)toClients({type:'entityEdgeResults',recipients:out.edgeResults});
      for(const diagnostic of out.diagnostics)toClients({type:'entityDiagnostics',...diagnostic});
      // Gameplay events cross once per distinct record set with their
      // recipients (every client usually shares one), and the network
      // thread serializes each set once.
      const eventSets=[];
      for(const events of out.reliableEvents??[]){
        const records=events.records??[];
        let set=eventSets.find(e=>e.tick===events.tick&&e.records.length===records.length&&e.records.every((r,i)=>r===records[i]));
        if(!set)eventSets.push(set={type:'gameplayEvents',tick:events.tick,records,clients:[]});
        set.clients.push(events.client);
      }
      for(const set of eventSets)toClients(set);
      // One publication message: the shared entity parts cross to the main
      // thread once per baseline group (not once per client) and are fanned
      // out there; only the per-client own datagrams are individual.
      const shared=out.shared??[],own=out.ownPackets??[];
      if(shared.length||own.length){
        const transfer=[];
        for(const group of shared)transfer.push(...group.parts);
        for(const packet of own)transfer.push(packet.buffer);
        toClients({type:'snapshotPublication',shared,own,publishedAt:wallNow()},transfer);
      }
      for(const fault of out.faults)toClients({type:'snapshotFault',client:fault.client,reason:'entity-prerequisite-failed:'+String(fault.error.message).slice(0,100)});
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
      toClients({type:'snapshotFault',client,reason:'entity-edge-failed:'+String(error.message).slice(0,100)});
    }
  };
  const step = (publishing=true) => {
    const begun=performance.now();
    reportEdgeFaults(entityHost?.advanceEdges()??[]);
    const tick=world.step();
    reportEdgeFaults(entityHost?.consumedEdges(tick)??[]);
    measure('sim',performance.now()-begun); if(publishing)publish(); if (tick % 300 === 0) report();
    measure('total',performance.now()-begun);
    if (game.battle.finished && !ended) { ended = true; report(); parentPort.postMessage({ type: 'finished', tick }); }
    return tick;
  };
  // Client messages that arrived while a catch-up batch ran, handled
  // between its steps so inputs land on the ticks they were meant for
  // instead of all before the batch.
  const drainClientMessages=()=>{
    const port=clientPort??parentPort;
    for(let i=0;i<4096;i++){const entry=receiveMessageOnPort(port);if(!entry)return;handle(entry.message);}
  };
  // A catch-up batch publishes once, after its last step: the network
  // thread keeps only each client's newest snapshot anyway, and every
  // extra publication cost a full capture and encode. An engine whose
  // snapshot host only captures on even ticks (no staleClients, the same
  // change set) also publishes after the batch's last even tick.
  const publishesAnyTick=()=>typeof entityHost?.staleClients==='function';
  const runBatch=steps=>{
    const anyTick=publishesAnyTick();
    for(let i=0;i<steps;i++){
      if(i>0)drainClientMessages();
      const last=i===steps-1,nextTick=game.worldClock.tick+1;
      step(last||(!anyTick&&i===steps-2&&nextTick%2===0));
      debt-=1/60;
    }
  };
  // A client the snapshot host has heard nothing from (no input, no
  // acknowledgement) for this long leaves through the normal path, and its
  // session is closed: a lost leave can no longer hold a ghost recipient.
  const STALE_CLIENT_SECONDS=30;
  let staleCheckedAt=performance.now();
  const evictStale=()=>{
    const now=performance.now();
    if(now-staleCheckedAt<1000||typeof entityHost?.staleClients!=='function')return;
    staleCheckedAt=now;
    for(const client of entityHost.staleClients(STALE_CLIENT_SECONDS)??[]){
      leave(client);
      toClients({type:'clientEvicted',client,reason:'No messages from the client.'});
    }
  };
  // Joins awaiting world.addPlayer, by client: a leave meanwhile cancels it.
  const joining=new Map();
  const leave=client=>{
    const pending=joining.get(client);if(pending)pending.cancelled=true;
    report();
    {const name=clients.has(client)?world.players.get(client)?.character?.name:null;if(name)notice(`${name} left the game`,client);}
    entityHost?.removeClient(client);clients.delete(client);
    // Idempotent: a second leave (an eviction, then the session's close)
    // must not move an already disconnected player's departure tick.
    const actor=world.players.get(client);if(actor&&!actor.disconnectedAt)world.removePlayer(client);
  };
  let tickReportedAt=-Infinity;
  parentPort.postMessage({ type: 'ready', generation: game.playerUnit.generation, content, networkContract });
  // A notice from the server itself, to everyone in the battle (but `except`): a player joining or leaving, the operator's announcement.
  const notice=(text,except=null)=>{for(const [client,state] of clients)if(state.started&&client!==except)toClients({type:'chat',client,text,sender:'Server',playerId:0,team:false,admin:true});};
  const handle = async m => {
    try {
      if(m.type==='attachNetwork'){clientPort?.close?.();clientPort=m.port;clientPort.on('message',handle);return;}
      if(m.type==='resetMetrics'){if(profiler){await profileCall('Profiler.stop');await profileCall('Profiler.start');}threadCpuStart=process.threadCpuUsage();for(const key of Object.keys(measurements))Array.isArray(measurements[key])?measurements[key].length=0:measurements[key]=0;parentPort.postMessage({type:'metricsReset',tick:game.worldClock.tick});}
      else if (m.type === 'metrics') { if(profiler){const {profile}=await profileCall('Profiler.stop');await writeFile(workerData.profile,JSON.stringify(profile));profiler.disconnect();profiler=null;} parentPort.postMessage({type:'metrics',threadCpu:process.threadCpuUsage(threadCpuStart),...measurements,tick:game.worldClock.tick,humans:world.players.size,units:game.units.all().length,liveUnits:game.units.all().filter(u=>u.alive&&!u.removed&&u.character).length,actorRoster:liveActorRoster(game),armies:world.roster?.counts()}); }
      else if (m.type === 'join') {
        // A rejoin (a replaced session) waits for the join before it.
        const previous=joining.get(m.client),pending={cancelled:false};
        pending.done=new Promise(resolve=>{pending.finish=resolve;});
        joining.set(m.client,pending);
        try {
          if(previous)await previous.done;
          if(pending.cancelled)return;
          if (clients.has(m.client)) throw Error('already joined');
          const entityRegistryMessage={type:'entityRegistry',client:m.client,registry:ensureEntityHost().descriptor()};
          // Players join at the spawn screen and choose their side unless the
          // operator configured autoSpawn / autoAssignTeams (the original's SET GAME OPTIONS).
          await world.addPlayer(m.client, { spectator: m.spectator, name: typeof m.name === 'string' && m.name ? m.name.slice(0, 32) : 'Player', reliableFire:true, separateActions:true, spawn: workerData.autoSpawn===true, teamSelect: workerData.autoAssignTeams!==true });
          // The session left while the world prepared the player: undo the
          // add instead of registering a client no session will ever leave.
          if(pending.cancelled){const actor=world.players.get(m.client);if(actor&&!actor.disconnectedAt)world.removePlayer(m.client);return;}
          clients.set(m.client, { started: false });
          entityHost.addClient(m.client);entityHost.setReady(m.client);toClients(entityRegistryMessage);
          toClients({ type:'joined', client:m.client, player:{...world.player(m.client),snapshotMode:'snapshot-delta-v1'}, tick:game.worldClock.tick });
          {const name=world.players.get(m.client)?.character?.name;if(name&&!m.spectator)notice(`${name} joined the game`,m.client);}
        } catch(e) { if(pending.cancelled)return; throw e; }
        finally { pending.finish(); if(joining.get(m.client)===pending)joining.delete(m.client); }
      } else if (m.type === 'leave') leave(m.client);
      // The network thread is gone (it restarts): every client leaves.
      else if (m.type === 'dropClients') { for(const client of new Set([...clients.keys(),...joining.keys()]))leave(client); }
      else if(m.type==='entityInputs'){
        // A batch from one network-thread pass, each input an exact-size
        // transferred buffer.
        const now=wallNow();
        for(const input of m.inputs){
          // Network receipt to simulation, in ms (the network thread stamps it).
          if(Number.isFinite(input.receivedAt))measure('inputHop',now-input.receivedAt);
          if(clients.has(input.client))entityHost?.input(input.client,input.buffer);
        }
      }
      else if(m.type==='entityInput'){
        if(Number.isFinite(m.receivedAt))measure('inputHop',wallNow()-m.receivedAt);
        if(clients.has(m.client))entityHost?.input(m.client,m.buffer);
      }
      else if(m.type==='playerLink'){const actor=world.players.get(m.client);if(actor&&Number.isFinite(m.rtt))actor.link={rtt:m.rtt,path:m.path&&typeof m.path==='object'?{kind:String(m.path.kind||''),family:String(m.path.family||'')}:null};}
      else if(m.type==='entityEdgeAck'){entityHost?.edgeAck(m.client,m);}
      else if (m.type === 'announcement') notice(m.text);
      else if (m.type === 'chat') {
        const sender = world.player(m.client);
        if (!sender || !clients.get(m.client)?.started) return;
        for (const [client, state] of clients) {
          if (!state.started || (m.team && world.player(client)?.team !== sender.team)) continue;
          toClients({ type:'chat',client,text:m.text,team:m.team,playerId:sender.unitId,sender:world.players.get(m.client)?.character?.name || `Player ${sender.unitId}` });
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
          result = { ...result, type: 'inspection', player:{...player,inputsStale:actor?.inputsStale,lastInputAgeMs:actor?Math.max(0,performance.now()-actor.lastInputAt):null,alive:unit?.alive,aimYaw:unit?.soldier?.aimYaw,aimPitch:unit?.soldier?.aimPitch,weapon:weapon?{clip:weapon.clip,wait:weapon.wait,reloading:weapon.reloading,held:weapon.held,bottomless:weapon.bottomless,rounds:weapon.rounds}:null,trigger:actor?.fire?.buttons?.slice(0,2),pendingFire:actor?.fire?.queue?.length,networkShot:unit?.networkShot?{id:unit.networkShot.id,tick:unit.networkShot.tick,viewTick:unit.networkShot.viewTick}:null}, snapshotHost:entityHost?.inspect(m.client),inputDiagnostics:entityHost?.inputDiagnostics(m.client),players: clients.size, running: Boolean(timer) };
        }
        toClients({ ...result, type: 'reply', resultType: result.type, id: m.id, client: m.client });
      } else if (m.type === 'step') {
        for (let i = 0; i < Math.min(m.count ?? 1, 600); i++) step();
        parentPort.postMessage({ type: 'state', state: worldState(game) });
      } else if (m.type === 'start' && !timer) {
        last = performance.now(); timer = setInterval(() => {
          try {
            const now = performance.now(), accumulated = debt+(now-last)/1000; measurements.droppedTicks += Math.max(0,accumulated-8/60)*60; debt = Math.min(accumulated,8/60); last = now;
            let count = 0;
            for (let pending = debt; pending >= 1/60 && count < 4; pending -= 1/60) count++;
            runBatch(count);
            evictStale();
            // The main thread only shows the tick (status, health): once a
            // second is plenty, not once per timer callback.
            if (count && now-tickReportedAt>=1000) { tickReportedAt=now; parentPort.postMessage({ type: 'tick', tick: game.worldClock.tick }); }
          } catch(e) { clearInterval(timer); parentPort.postMessage({ type: 'error', message: e.message, stack: e.stack }); }
        }, 5);
      } else if (m.type === 'stop') { clearInterval(timer); timer = null; }
    } catch(e) {
      if(m.client)toClients({ type: 'reply', client: m.client, id: m.id, error: e.message });
      else parentPort.postMessage({ type: 'error', message: e.message, stack: e.stack });
    }
  };
  parentPort.on('message', handle);
} catch(e) { parentPort.postMessage({ type: 'error', message: e.message, stack: e.stack }); }
