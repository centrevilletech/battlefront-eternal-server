import { updateRoomCapabilities } from './room-capabilities.js';
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
  const { createGame } = await import(pathToFileURL(resolve(root, 'src/game.js')));
  const { createMultiplayerWorld } = await import(pathToFileURL(resolve(root, 'src/runtime/multiplayer-world.js')));
  const { worldState } = await import(pathToFileURL(resolve(root, 'src/runtime/world-state.js')));
  const { compactMetadata } = await import(pathToFileURL(resolve(root, 'src/runtime/shared-packet.js')));
  const { contentHash } = await import(pathToFileURL(resolve(root, 'src/runtime/content-hash.js')));
  const content = await contentHash(workerData.query ?? '?map=nab2&side=rep&ai=classic&seed=1');
  const game = await createGame({ headless: true, query: workerData.query ?? '?map=nab2&side=rep&ai=classic&seed=1' });
  const world = createMultiplayerWorld(game, { maxPlayers: workerData.maxPlayers ?? 64, armySize: workerData.armySize ?? 32 });
  let Journal;
  try { ({SnapshotJournal:Journal}=await import(pathToFileURL(resolve(root,'src/runtime/snapshot-journal.js')))); } catch {}
  if(typeof Journal?.prototype.pin!=='function'||typeof Journal?.prototype.unpin!=='function')Journal=null;
  let journal,lastJournalId,eventSerial=0;
  const clients = new Map(); let timer = null, last = performance.now(), debt = 0;
  let profiler;
  const profileCall = method => new Promise((resolve,reject)=>profiler.post(method,(error,result)=>error?reject(error):resolve(result)));
  if(workerData.profile){profiler=new Session();profiler.connect();await profileCall('Profiler.enable');await profileCall('Profiler.start');}
  const updateLinear=()=>updateRoomCapabilities(world,clients);
  let threadCpuStart=process.threadCpuUsage();
  const measurements = {sim:[],encode:[],total:[],droppedTicks:0,fireAccepted:0,fireRejected:0,journalBootstraps:0,journalCheckpointPackets:0};
  const measure = (name,value) => { if(workerData.metrics && measurements[name].length<120000)measurements[name].push(value); };
  const publish = () => {
    const begun=performance.now();
    const groups=new Map();
    for (const [client, state] of clients) if (state.started) {
      const keyframe=Boolean(state.keyframe), packet=world.snapshot(client,{keyframe,sharedPacket:true});
      state.keyframe=false;if(!packet)continue;
      if(packet instanceof ArrayBuffer){parentPort.postMessage({type:'packet',client,buffer:packet,keyframe},[packet]);continue;}
      let group=groups.get(packet.sharedId);
      if(!group)groups.set(packet.sharedId,group={type:'packet',sharedId:packet.sharedId,buffer:packet.buffer.slice(0),recipients:[]});
      group.recipients.push({client,keyframe,metadata:state.compactMetadata && typeof compactMetadata==='function' ? compactMetadata(packet.metadata) : packet.metadata});
    }
    for(const group of groups.values()) {
      const journalRecipients=group.recipients.filter(r=>clients.get(r.client).journal);
      if(journalRecipients.length) {
        journal??=new Journal();
        if(lastJournalId!==group.sharedId) {
          const events=journal.push(group.buffer);lastJournalId=group.sharedId;
          for(let offset=0;offset<events.calls.length;offset+=256){const batch={...events,id:++eventSerial,calls:events.calls.slice(offset,offset+256)};parentPort.postMessage({type:'journalEvents',sharedId:`events:${events.epoch}:${batch.id}`,buffer:new TextEncoder().encode(JSON.stringify({type:'snapshot-events',...batch})).buffer,recipients:journalRecipients.filter(r=>clients.get(r.client).snapshotAck?.epoch===journal.epoch)});}
        }
        const encoded=new Map();
        for(const recipient of journalRecipients) {
          const state=clients.get(recipient.client);
          if(state.journalEpoch!==journal.epoch){journal.unpin(recipient.client+':ack');journal.unpin(recipient.client+':pending');state.snapshotAck=null;state.pendingCheckpoint=null;state.checkpoints.clear();state.journalEpoch=journal.epoch;}
          const ack=state.snapshotAck;
          let baseTick=ack?.epoch===journal.epoch && journal.snapshots.has(ack.tick)?ack.tick:null;
          if(baseTick===journal.current.tick)continue;
          const reliable=baseTick===null,now=performance.now();
          if(reliable&&state.pendingCheckpoint?.epoch===journal.epoch)continue;
          const checkpoint=reliable||!state.pendingCheckpoint&&journal.current.tick%15===0;
          const key=`journal:${journal.epoch}:${journal.current.tick}:${baseTick}:${checkpoint}:${state.relativeSnapshots}`;
          let batch=encoded.get(key);
          if(!batch)encoded.set(key,batch={type:'journalPacket',sharedId:key,buffer:journal.encode({baseTick,checkpoint,relativeVectors:state.relativeSnapshots}),reliable:checkpoint,recipients:[]});
          batch.recipients.push(recipient);
          if(checkpoint){measurements.journalCheckpointPackets++;journal.pin(recipient.client+':pending',journal.current.tick);state.pendingCheckpoint={epoch:journal.epoch,tick:journal.current.tick};state.checkpoints.set(journal.current.tick,journal.epoch);while(state.checkpoints.size>16)state.checkpoints.delete(state.checkpoints.keys().next().value);}
          if(reliable){measurements.journalBootstraps++;state.bootstrapAt=now;state.bootstrapEpoch=journal.epoch;}
        }
        for(const batch of encoded.values())parentPort.postMessage(batch,[batch.buffer]);
      }
      group.recipients=group.recipients.filter(r=>!clients.get(r.client).journal);
      if(group.recipients.length)parentPort.postMessage(group,[group.buffer]);
    }
    measure('encode',performance.now()-begun);
  };
  // Run the simulation at 60 Hz and send accumulated changes at 20 Hz.
  const matchId = randomUUID();
  const report = () => parentPort.postMessage({type:'scores',match:matchId,players:[...(world.players?.values() ?? [])].filter(p=>!p.spectator).map(p=>({id:p.id,name:p.character.name,kills:p.character.kills-2*p.character.teamKills,deaths:p.character.deaths,points:p.character.captures}))});
  let ended = false;
  const step = () => {
    const begun=performance.now(), tick = world.step(); measure('sim',performance.now()-begun); if (tick % 3 === 0) publish(); if (tick % 300 === 0) report();
    measure('total',performance.now()-begun);
    if (game.battle.finished && !ended) { ended = true; report(); parentPort.postMessage({ type: 'finished', tick }); }
  };
  parentPort.postMessage({ type: 'ready', generation: game.playerUnit.generation, content });
  parentPort.on('message', async m => {
    try {
      if(m.type==='resetMetrics'){threadCpuStart=process.threadCpuUsage();for(const key of Object.keys(measurements))Array.isArray(measurements[key])?measurements[key].length=0:measurements[key]=0;}
      else if (m.type === 'metrics') { if(profiler){const {profile}=await profileCall('Profiler.stop');await writeFile(workerData.profile,JSON.stringify(profile));profiler.disconnect();profiler=null;} parentPort.postMessage({type:'metrics',threadCpu:process.threadCpuUsage(threadCpuStart),...measurements,tick:game.worldClock.tick,humans:world.players.size,linearProjectiles:world.linearProjectiles===true,units:game.units.all().length,liveUnits:game.units.all().filter(u=>u.alive&&!u.removed&&u.character).length,armies:world.roster?.counts()}); }
      else if (m.type === 'join') {
        if (clients.has(m.client)) throw Error('already joined');
        await world.addPlayer(m.client, { spectator: m.spectator, reliableFire:true, separateActions:m.separateActions===true });
        clients.set(m.client, { started: false, compactMetadata:m.compactMetadata===true, linearProjectiles:m.linearProjectiles===true,journal:m.journal===true&&!!Journal,relativeSnapshots:m.relativeSnapshots===true,checkpoints:new Map(),inputDiagnostics:{accepted:0,rejected:0,reasons:{},last:{}} });
        parentPort.postMessage({ type: 'joined', client: m.client, player: {...world.player(m.client),...(clients.get(m.client).journal?{snapshotMode:'journal-v1'}:{}),...(m.separateActions===true&&typeof world.actions==='function'?{inputMode:'split-v1'}:{})}, tick: game.worldClock.tick });
      } else if (m.type === 'leave') { report(); journal?.unpin(m.client+':ack');journal?.unpin(m.client+':pending');clients.delete(m.client); world.removePlayer(m.client); updateLinear(); }
      else if(m.type==='snapshotAck') {
        const state=clients.get(m.client),pending=state?.pendingCheckpoint;
        if(state?.journal&&pending?.epoch===m.epoch&&pending?.tick===m.tick&&journal?.epoch===m.epoch&&journal.snapshots.has(m.tick)){
          if(!journal.pin(m.client+':ack',m.tick))return;journal.unpin(m.client+':pending');state.pendingCheckpoint=null;state.snapshotAck={epoch:m.epoch,tick:m.tick};
        }
      }
      else if(m.type==='snapshotResync') {const state=clients.get(m.client);if(state){journal?.unpin(m.client+':ack');journal?.unpin(m.client+':pending');state.snapshotAck=null;state.pendingCheckpoint=null;state.bootstrapAt=0;}}
      else if(m.type==='actions'){if(clients.get(m.client)?.started)world.actions?.(m.client,m.envelope);}
      else if(m.type==='fire'){const accepted=Boolean(clients.get(m.client)?.started && world.fire(m.client,m.edge));measurements[accepted?'fireAccepted':'fireRejected']++;parentPort.postMessage({type:'fireAck',client:m.client,id:m.edge.id,accepted});}
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
        for (const command of m.commands.slice(0,32)) {
          const stats=clients.get(m.client).inputDiagnostics,accepted=world.input(m.client,command);
          stats[accepted?'accepted':'rejected']++;
          stats.last.sequence=command?.sequence;stats.last.tick=command?.tick;stats.last.generation=command?.generation;stats.last.epoch=command?.epoch;stats.last.serverTick=game.worldClock.tick;stats.last.accepted=accepted;
          if(!accepted){const reason=!command?'missing-command':Math.abs(command.tick-game.worldClock.tick)>120?'tick-window':command.epoch!==1?'epoch':'sequence-or-inactive';stats.reasons[reason]=(stats.reasons[reason]??0)+1;}
        }
      } else if (m.type === 'resync') clients.get(m.client) && (clients.get(m.client).keyframe = true);
      else if (m.type === 'call') {
        if (!clients.has(m.client)) throw Error('not joined');
        const player = world.player(m.client);
        let result = { epoch: 1, tick: game.worldClock.tick };
        if (m.method === 'init') result = { ...result, type: 'ready', generation: player.generation, playerId: player.unitId, shared: false };
        else if (m.method === 'start') { const state=clients.get(m.client);if(!state.started)state.keyframe=true;state.started=true;updateLinear();result.type='started'; }
        else if (m.method === 'suspend') { clients.get(m.client).started = false; world.clearInput?.(m.client); updateLinear(); result.type = 'suspended'; }
        else if (m.method === 'resync') { clients.get(m.client) && (clients.get(m.client).keyframe = true); }
        else if (m.method === 'worldState') result = { type: 'state', state: worldState(game) };
        else if (m.method === 'inspect') result = { ...result, type: 'inspection', player, inputDiagnostics:clients.get(m.client).inputDiagnostics, players: clients.size, running: Boolean(timer) };
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
