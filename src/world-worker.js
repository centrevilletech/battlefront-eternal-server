import { liveActorRoster } from './actor-roster.js';
import { EntityEventStore } from './entity-event-store.js';
import { entityImportance } from './entity-importance.js';
import { EntityBudget } from './entity-budget.js';
import { EntityEdges } from './entity-edges.js';
import { FreshStateWindow } from './fresh-state.js';
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
  let Journal,SnapshotMemoryLimit,partitionSnapshotEvents,entityRuntime;
  try {
    const capture=await import(pathToFileURL(resolve(root,'src/runtime/network-entity-capture.js')));
    const registry=await import(pathToFileURL(resolve(root,'src/runtime/network-entity-registry.js')));
    const codec=await import(pathToFileURL(resolve(root,'src/runtime/network-entity-codec.js')));
    const stream=await import(pathToFileURL(resolve(root,'src/runtime/network-entity-stream.js')));
    const input=await import(pathToFileURL(resolve(root,'src/runtime/network-entity-input.js')));
    const edges=await import(pathToFileURL(resolve(root,'src/runtime/network-entity-edges.js')));
    const shots=await import(pathToFileURL(resolve(root,'src/runtime/network-entity-shots.js')));
    const combat=await import(pathToFileURL(resolve(root,'src/runtime/network-entity-combat.js')));
    const summary=await import(pathToFileURL(resolve(root,'src/runtime/network-entity-summary.js')));
    const player=await import(pathToFileURL(resolve(root,'src/runtime/network-entity-player.js')));
    if(player.OWN_PLAYER_SCHEMA!==8)throw Error('bfe-sim-4 requires own-player correction schema 8');
    entityRuntime={...capture,...registry,...codec,...stream,...input,...edges,...shots,...summary,...combat};
  } catch(error) { throw new Error('Mandatory multiplayer engine runtime unavailable: '+error.message,{cause:error}); }
  let entityRegistry,entityCapture,entityShots,combatCapture,combatDispose;
  const combatStore=new EntityEventStore();
  try { ({partitionSnapshotEvents}=await import(pathToFileURL(resolve(root,'src/runtime/snapshot-transients.js')))); } catch {}
  try { ({SnapshotJournal:Journal,SnapshotMemoryLimit}=await import(pathToFileURL(resolve(root,'src/runtime/snapshot-journal.js')))); } catch {}
  if(typeof Journal?.prototype.pin!=='function'||typeof Journal?.prototype.unpin!=='function')Journal=null;
  let journal,lastJournalId,eventSerial=0;
  const clients = new Map(); let timer = null, last = performance.now(), debt = 0;
  let profiler;
  const profileCall = method => new Promise((resolve,reject)=>profiler.post(method,(error,result)=>error?reject(error):resolve(result)));
  if(workerData.profile){profiler=new Session();profiler.connect();await profileCall('Profiler.enable');await profileCall('Profiler.start');}
  const updateLinear=()=>world.setLinearProjectiles(true);
  let threadCpuStart=process.threadCpuUsage();
  const measurements = {sim:[],encode:[],total:[],droppedTicks:0,fireAccepted:0,fireRejected:0,journalBootstraps:0,journalCheckpointPackets:0};
  const measure = (name,value) => { if(workerData.metrics && measurements[name].length<120000)measurements[name].push(value); };
  const publish = () => {
    const begun=performance.now(),capturedAtUnixMs=Date.now();
    // Promote the entire publication before its first encode. A joining/resyncing
    // recipient must not create a second shared keyframe at the same tick and
    // invalidate another recipient's already-issued initial journal baseline.
    const publicationKeyframe=[...clients].some(([id,state])=>state.started&&(state.keyframe||world.players.get(id)?.keyframe));
    const groups=new Map();
    let semanticRecords,commonSummaries;
    let combatChanges=[];const combatFaults=combatCapture?.drainFaults()??[];
    if(combatCapture)try{combatChanges=combatStore.update(combatCapture.drain(),game.worldClock.tick,entityRuntime.prepareEntityRecord,entityRuntime.decodeCombat);}catch(error){combatFaults.push(String(error.message).slice(0,160));}
    const eligibilityCache=new Map();
    for (const [client, state] of clients) if (state.started) {
      if(state.fresh){const actor=world.players.get(client);if(actor)parentPort.postMessage({type:'inputAck',client,epoch:1,serverTick:game.worldClock.tick,generation:actor.unit?.generation??0,ack:actor.mailbox.ack,ackTick:actor.mailbox.ackTick,discardedInput:actor.mailbox.discarded??0,discardedActions:actor.actions?.discarded??0,ackActions:state.entity?0:actor.actions?.ack??0});}
      if(state.entity){
        const feedback=state.entityEdges.feedback();if(feedback.received.length||feedback.results.length)parentPort.postMessage({type:'entityEdgeResults',client,...feedback});
        if(state.entityReady){
          try {
            semanticRecords??=entityCapture.capture(game.worldClock.tick).map(entityRuntime.prepareEntityRecord);
            commonSummaries??=entityRuntime.captureWorldSummaries(game,world,entityRegistry,{tick:game.worldClock.tick,epoch:1}).map(entityRuntime.prepareEntityRecord);
            if(game.worldClock.tick%60===0)parentPort.postMessage({type:'entityDiagnostics',client,actors:entityCapture.unsupported.slice(0,256).map(e=>({id:e.entityId,message:String(e.message).slice(0,160)}))});
            const actor=world.players.get(client),vehicle=actor.unit?.seat?.vehicle,soldier=actor.unit?.soldier;
            const position=(vehicle?.position||actor.unit?.position)?.toArray(),velocity=(vehicle?.velocity||soldier?.velocity)?.toArray();
            let forward;
            if(vehicle?.quaternion){const {x,y,z,w}=vehicle.quaternion;forward=[2*(x*z+w*y),2*(y*z-w*x),1-2*(x*x+y*y)];}
            else if(soldier){const yaw=soldier.aimYaw,pitch=soldier.aimPitch,c=Math.cos(pitch);forward=[Math.sin(yaw)*c,Math.sin(pitch),Math.cos(yaw)*c];}
            const interest={position,velocity,forward,flying:!!vehicle?.flying};
            for(const prepared of semanticRecords){
              const record=prepared.record;if(!actor.spectator&&record.kind==='soldier'&&record.id===actor.unit?.id)continue;
              state.entity.update(prepared,entityImportance(record,interest));
            }
            for(const prepared of commonSummaries)state.entity.update(prepared,4);
            for(const record of entityRuntime.captureWorldSummaries(game,world,entityRegistry,{tick:game.worldClock.tick,epoch:1,playerId:client,includeCommon:false,eligibilityCache}))state.entity.update(record,4);
            if(combatFaults.length)parentPort.postMessage({type:'entityDiagnostics',client,actors:combatFaults.map(message=>({id:0,message:String(message).slice(0,160)}))});
            for(const event of state.combatReady?combatChanges:combatStore.records.values()){
              const priority=[event.view.ownerId,event.view.targetId,event.view.attackerId].includes(actor.unit?.id)?24:entityImportance(event.view,interest);
              state.entity.updateEvent({...event,record:event.prepared},priority);
            }
            state.combatReady=true;
            const own=actor.spectator?undefined:entityRuntime.capturePlayerCorrection(game,world,client,{registry:entityRegistry,bindingProfileId:0,epoch:1});
            const fields={tick:game.worldClock.tick,ackInput:actor.mailbox.ack,ackActions:0,ackFire:0};
            const offer=(packet,ownCorrection)=>{
              if(!packet)return false;
              if(!state.entityBudget.take(packet.buffer.byteLength,begun)){state.entityMetrics.budgetDropped++;return false;}
              state.entityMetrics[ownCorrection?'ownOffered':'remoteOffered']++;
              const buffer=packet.buffer.buffer.slice(packet.buffer.byteOffset,packet.buffer.byteOffset+packet.buffer.byteLength);
              parentPort.postMessage({type:'entityPacket',client,epoch:1,packetId:packet.packetId,ownCorrection,capturedAtUnixMs,buffer},[buffer]);return true;
            };
            const packet=state.entity.packet({...fields,own});
            if(offer(packet,!!own)){const maxBytes=Math.min(1100,state.entityBudget.available(begun));if(maxBytes>30)offer(state.entity.packet({...fields,maxBytes,excludePacketIds:[packet.packetId]}),false);}
          } catch(error){parentPort.postMessage({type:'snapshotFault',client,reason:'entity-prerequisite-failed:'+String(error.message).slice(0,100)});}
          continue;
        }
        if(state.fresh.joinIssued)continue;
      }
      const keyframe=Boolean(publicationKeyframe||state.keyframe), packet=world.snapshot(client,{keyframe,sharedPacket:true});
      state.keyframe=false;if(!packet)continue;
      if(packet instanceof ArrayBuffer){parentPort.postMessage({type:'packet',client,buffer:packet,keyframe},[packet]);continue;}
      let group=groups.get(packet.sharedId);
      if(!group)groups.set(packet.sharedId,group={type:'packet',sharedId:packet.sharedId,buffer:packet.buffer.slice(0),recipients:[]});
      group.recipients.push({client,keyframe,metadata:state.compactMetadata && typeof compactMetadata==='function' ? compactMetadata(packet.metadata) : packet.metadata});
    }
    for(const group of groups.values()) {
      const journalRecipients=group.recipients.filter(r=>clients.get(r.client).journal);
      if(journalRecipients.length) {
        try {
        journal??=new Journal({maxBytes:64*1024*1024});
        if(lastJournalId!==group.sharedId) {
          const events=journal.push(group.buffer);lastJournalId=group.sharedId;
          const readyRecipients=journalRecipients.filter(r=>!clients.get(r.client).entity&&clients.get(r.client).snapshotAck?.epoch===journal.epoch);
          for(let offset=0;offset<events.calls.length;offset+=256){
            const batch={...events,id:++eventSerial,calls:events.calls.slice(offset,offset+256)};
            const sendEvents=(type,value,recipients,kind)=>{if(!value.calls.length||!recipients.length)return;parentPort.postMessage({type,replaceKey:`${events.epoch}:${events.tick}`,sharedId:`${kind}:${events.epoch}:${batch.id}`,buffer:new TextEncoder().encode(JSON.stringify({type:'snapshot-events',...value})).buffer,recipients});};
            const split=readyRecipients.filter(r=>clients.get(r.client).transientSnapshots);
            sendEvents('journalEvents',batch,readyRecipients.filter(r=>!clients.get(r.client).transientSnapshots),'events');
            if(split.length){const parts=partitionSnapshotEvents(batch);sendEvents('journalEvents',parts.reliable,split,'reliable-events');sendEvents('journalTransients',parts.transient,split,'transient-events');}
          }
        }
        const encoded=new Map();
        for(const recipient of journalRecipients) {
          const state=clients.get(recipient.client);
          if(state.journalEpoch!==journal.epoch){journal.unpin(recipient.client+':ack');journal.unpin(recipient.client+':pending');journal.unpin(recipient.client+':join');state.snapshotAck=null;state.pendingCheckpoint=null;state.checkpoints.clear();state.journalEpoch=journal.epoch;}
          if(state.fresh){
            const previousAck=state.fresh.ack,plan=state.fresh.plan(journal.epoch,journal.current.tick,tick=>journal.snapshots.has(tick));
            if(previousAck&&!state.fresh.ack)journal.unpin(recipient.client+':ack');
            state.snapshotAck=state.fresh.ack;
            if(!plan)continue;
            if(plan.initialBootstrap&&!journal.pin(recipient.client+':join',journal.current.tick)){parentPort.postMessage({type:'snapshotFault',reason:'snapshot-memory-limit',client:recipient.client});continue;}
            const key=`fresh:${journal.epoch}:${journal.current.tick}:${plan.baseTick}:${!!plan.initialBootstrap}:${state.relativeSnapshots}`;
            let batch=encoded.get(key);
            if(!batch)encoded.set(key,batch={type:'journalPacket',sharedId:key,buffer:journal.encode({baseTick:plan.baseTick,checkpoint:true,relativeVectors:state.relativeSnapshots}),reliable:!!plan.initialBootstrap,initialBootstrap:!!plan.initialBootstrap,snapshotEpoch:journal.epoch,snapshotTick:journal.current.tick,capturedAtUnixMs,recipients:[]});
            batch.recipients.push(recipient);if(plan.baseTick===null)measurements.journalBootstraps++;
            continue;
          }
          if(state.pendingCheckpoint&&!journal.snapshots.has(state.pendingCheckpoint.tick)){journal.unpin(recipient.client+':pending');state.pendingCheckpoint=null;}
          const ack=state.snapshotAck;
          let baseTick=ack?.epoch===journal.epoch && journal.snapshots.has(ack.tick)?ack.tick:null;
          if(baseTick===journal.current.tick)continue;
          const reliable=baseTick===null,now=performance.now();
          if(reliable&&state.pendingCheckpoint?.epoch===journal.epoch)continue;
          const checkpoint=reliable||!state.pendingCheckpoint&&journal.current.tick%15===0;
          const key=`journal:${journal.epoch}:${journal.current.tick}:${baseTick}:${checkpoint}:${state.relativeSnapshots}`;
          let batch=encoded.get(key);
          if(!batch)encoded.set(key,batch={type:'journalPacket',sharedId:key,buffer:journal.encode({baseTick,checkpoint,relativeVectors:state.relativeSnapshots}),reliable:checkpoint,capturedAtUnixMs,recipients:[]});
          batch.recipients.push(recipient);
          if(checkpoint){measurements.journalCheckpointPackets++;journal.pin(recipient.client+':pending',journal.current.tick);state.pendingCheckpoint={epoch:journal.epoch,tick:journal.current.tick};state.checkpoints.set(journal.current.tick,journal.epoch);while(state.checkpoints.size>16)state.checkpoints.delete(state.checkpoints.keys().next().value);}
          if(reliable){measurements.journalBootstraps++;state.bootstrapAt=now;state.bootstrapEpoch=journal.epoch;}
        }
        for(const batch of encoded.values())parentPort.postMessage(batch,[batch.buffer]);
        } catch(error) {
          if(typeof SnapshotMemoryLimit!=='function'||!(error instanceof SnapshotMemoryLimit))throw error;
          parentPort.postMessage({type:'snapshotFault',reason:'snapshot-memory-limit',recipients:journalRecipients});journal=null;lastJournalId=null;
          for(const recipient of journalRecipients){const state=clients.get(recipient.client);state.snapshotAck=null;state.fresh?.reset(null);}
        }
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
    const begun=performance.now();
    for(const [client,state]of clients)if(state.entityEdges&&state.started){
      try{const actor=world.players.get(client);if((actor.unit?.generation??0)!==state.entityEdges.generation)state.entityEdges.reset(1,actor.unit?.generation??0);
      state.entityEdges.advance(game.worldClock.tick+1);}catch(error){state.started=false;parentPort.postMessage({type:'snapshotFault',client,reason:'entity-edge-failed:'+String(error.message).slice(0,100)});}
    }
    const tick=world.step();
    for(const[client,state]of clients)if(state.entityEdges)try{state.entityEdges.consumed(tick);}catch(error){state.started=false;parentPort.postMessage({type:'snapshotFault',client,reason:'entity-edge-failed:'+String(error.message).slice(0,100)});} measure('sim',performance.now()-begun); if (tick % 3 === 0) publish(); if (tick % 300 === 0) report();
    measure('total',performance.now()-begun);
    if (game.battle.finished && !ended) { ended = true; report(); parentPort.postMessage({ type: 'finished', tick }); }
  };
  parentPort.postMessage({ type: 'ready', generation: game.playerUnit.generation, content });
  parentPort.on('message', async m => {
    try {
      if(m.type==='resetMetrics'){if(profiler){await profileCall('Profiler.stop');await profileCall('Profiler.start');}threadCpuStart=process.threadCpuUsage();for(const key of Object.keys(measurements))Array.isArray(measurements[key])?measurements[key].length=0:measurements[key]=0;parentPort.postMessage({type:'metricsReset',tick:game.worldClock.tick});}
      else if (m.type === 'metrics') { if(profiler){const {profile}=await profileCall('Profiler.stop');await writeFile(workerData.profile,JSON.stringify(profile));profiler.disconnect();profiler=null;} parentPort.postMessage({type:'metrics',threadCpu:process.threadCpuUsage(threadCpuStart),...measurements,tick:game.worldClock.tick,humans:world.players.size,linearProjectiles:world.linearProjectiles===true,units:game.units.all().length,liveUnits:game.units.all().filter(u=>u.alive&&!u.removed&&u.character).length,actorRoster:liveActorRoster(game),armies:world.roster?.counts()}); }
      else if (m.type === 'join') {
        Object.assign(m,{entity:true,journal:true,fresh:true,compactMetadata:true,linearProjectiles:true,separateActions:true,relativeSnapshots:false,transientSnapshots:false});
        if (clients.has(m.client)) throw Error('already joined');
        if(m.entity&&!entityRuntime)throw Error('semantic entity runtime unavailable');
        if(m.fresh && (!Journal || typeof SnapshotMemoryLimit!=='function'))throw Error('fresh state requires the bounded journal runtime');
        if(m.entity&&!entityRegistry){
          entityRegistry=entityRuntime.createEntityRegistry(game.data,{contentEpoch:1,inputCodes:entityRuntime.INPUT_CODES,bindingProfiles:entityRuntime.DEFAULT_BINDING_PROFILES,posts:game.posts.posts});
          entityShots=new entityRuntime.EntityShotCounters(game.simEvents);
          combatCapture=new entityRuntime.CombatCapture({tick:()=>game.worldClock.tick,definitionId:entityRegistry.combat.definitionId,generation:1});
          combatDispose=entityRuntime.attachCombatCapture(game,combatCapture);
          process.once('exit',()=>combatDispose?.());
          entityCapture=new entityRuntime.EntityWorldCapture(game,entityRegistry,{fireCounter:unit=>entityShots.counter(unit)});
        }
        await world.addPlayer(m.client, { spectator: m.spectator, reliableFire:true, separateActions:m.separateActions===true });
        clients.set(m.client, { started: false, entity:m.entity?new entityRuntime.EntityScheduler({epoch:1}):null,entityReady:false,entityBudget:m.entity?new EntityBudget():null,entityMetrics:{ownOffered:0,remoteOffered:0,admitted:0,budgetDropped:0,lastOwnTick:null},compactMetadata:m.compactMetadata===true, linearProjectiles:m.linearProjectiles===true,journal:m.journal===true&&!!Journal,fresh:m.fresh===true&&!!Journal?new FreshStateWindow():null,relativeSnapshots:m.relativeSnapshots===true,transientSnapshots:m.transientSnapshots===true&&typeof partitionSnapshotEvents==='function',checkpoints:new Map(),fireDiagnostics:{accepted:0,rejected:0,last:null},inputDiagnostics:{accepted:0,rejected:0,reasons:{},last:{}} });
        if(m.entity)clients.get(m.client).entityEdges=new EntityEdges({Receiver:entityRuntime.EntityEdgeReceiver,actor:world.players.get(m.client),world,client:m.client,registry:entityRegistry});
        if(m.entity)parentPort.postMessage({type:'entityRegistry',client:m.client,registry:entityRuntime.entityRegistryDescriptor(entityRegistry)});
        parentPort.postMessage({ type: 'joined', client: m.client, player: {...world.player(m.client),...(clients.get(m.client).journal?{snapshotMode:m.entity?'entity-v1':clients.get(m.client).fresh?'fresh-v1':'journal-v1',snapshotTransients:clients.get(m.client).transientSnapshots}:{}),...(m.separateActions===true&&typeof world.actions==='function'?{inputMode:'split-v1'}:{})}, tick: game.worldClock.tick });
      } else if (m.type === 'leave') { report(); journal?.unpin(m.client+':ack');journal?.unpin(m.client+':pending');journal?.unpin(m.client+':join');clients.delete(m.client); world.removePlayer(m.client); updateLinear(); }
      else if(m.type==='entityInput'){
        const state=clients.get(m.client);if(!state?.entity||!state.started)return;
        const packet=entityRuntime.decodeEntityInput(new Uint8Array(m.buffer),entityRegistry);
        if(packet.epoch!==1||packet.generation!==world.players.get(m.client)?.unit?.generation)return;
        state.entityEdges.receive(packet,game.worldClock.tick);
        for(const command of packet.commands){
          const accepted=world.input(m.client,command),stats=state.inputDiagnostics;
          stats[accepted?'accepted':'rejected']++;stats.last={sequence:command.sequence,tick:command.tick,generation:command.generation,epoch:command.epoch,serverTick:game.worldClock.tick,accepted,lookTotal:command.lookTotal,down:command.down.slice(0,32)};
        }
        for(const packetId of packet.receipts)state.entity.ack({epoch:packet.epoch,packetId});
      }
      else if(m.type==='entityEdgeAck'){clients.get(m.client)?.entityEdges?.acknowledge(m);}
      else if(m.type==='entitySent'){const state=clients.get(m.client);if(m.epoch===1&&state?.entity?.admitted(m.packetId)){state.entityMetrics.admitted++;if(m.ownCorrection)state.entityMetrics.lastOwnTick=m.tick;}}
      else if(m.type==='entityAck'){clients.get(m.client)?.entity?.ack({epoch:m.epoch,packetId:m.packetId});}
      else if(m.type==='snapshotSent'){const state=clients.get(m.client);if(state?.fresh&&journal?.epoch===m.epoch)state.fresh.admitted(m.epoch,m.tick,m.initialBootstrap===true);}
      else if(m.type==='snapshotAck') {
        const state=clients.get(m.client),pending=state?.pendingCheckpoint;
        if(state?.fresh){if(state.fresh.acknowledge(m.epoch,m.tick,tick=>journal?.epoch===m.epoch&&journal.pin(m.client+':ack',tick))){state.snapshotAck=state.fresh.ack;journal.unpin(m.client+':join');if(state.entity){state.entityReady=true;journal.unpin(m.client+':ack');}}return;}
        if(state?.journal&&pending?.epoch===m.epoch&&pending?.tick===m.tick&&journal?.epoch===m.epoch&&journal.snapshots.has(m.tick)){
          if(!journal.pin(m.client+':ack',m.tick))return;journal.unpin(m.client+':pending');state.pendingCheckpoint=null;state.snapshotAck={epoch:m.epoch,tick:m.tick};
        }
      }
      else if(m.type==='snapshotResync') {const state=clients.get(m.client);if(state){journal?.unpin(m.client+':ack');journal?.unpin(m.client+':pending');journal?.unpin(m.client+':join');state.snapshotAck=null;state.pendingCheckpoint=null;state.bootstrapAt=0;state.fresh?.reset(journal?.epoch??null);}}
      else if(m.type==='actions'){if(clients.get(m.client)?.started)world.actions?.(m.client,m.envelope);}
      else if(m.type==='fire'){const state=clients.get(m.client),accepted=Boolean(state?.started && world.fire(m.client,m.edge));measurements[accepted?'fireAccepted':'fireRejected']++;if(state){state.fireDiagnostics[accepted?'accepted':'rejected']++;state.fireDiagnostics.last={id:m.edge.id,tick:m.edge.tick,generation:m.edge.generation,button:m.edge.button,pressed:m.edge.pressed,viewTick:m.edge.viewTick,serverTick:game.worldClock.tick,accepted};}parentPort.postMessage({type:'fireAck',client:m.client,id:m.edge.id,accepted});}
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
          stats.last.sequence=command?.sequence;stats.last.tick=command?.tick;stats.last.generation=command?.generation;stats.last.epoch=command?.epoch;stats.last.serverTick=game.worldClock.tick;stats.last.accepted=accepted;stats.last.lookTotal=Array.isArray(command?.lookTotal)?command.lookTotal.slice(0,2):null;stats.last.lookCount=Array.isArray(command?.look)?command.look.length:0;stats.last.down=Array.isArray(command?.down)?command.down.slice(0,32):[];
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
        else if (m.method === 'inspect') {
          const actor=world.players.get(m.client),unit=actor?.unit,weapon=unit?.combat?.arsenal?.current(0);
          result = { ...result, type: 'inspection', player:{...player,inputsStale:actor?.inputsStale,lastInputAgeMs:actor?Math.max(0,performance.now()-actor.lastInputAt):null,alive:unit?.alive,aimYaw:unit?.soldier?.aimYaw,aimPitch:unit?.soldier?.aimPitch,weapon:weapon?{clip:weapon.clip,wait:weapon.wait,reloading:weapon.reloading,held:weapon.held,bottomless:weapon.bottomless,rounds:weapon.rounds}:null,trigger:actor?.fire?.buttons?.slice(0,2),pendingFire:actor?.fire?.queue?.length,networkShot:unit?.networkShot?{id:unit.networkShot.id,tick:unit.networkShot.tick,viewTick:unit.networkShot.viewTick}:null}, entityScheduling:clients.get(m.client).entity?{profile:'entity-v1',payloadBytesPerSecond:26000,burstBytes:2200,ownNominalHz:20,...clients.get(m.client).entityMetrics,events:{...clients.get(m.client).entity.eventMetrics,retained:clients.get(m.client).entity.events.size,bytes:clients.get(m.client).entity.eventBytes,globalDropped:combatStore.dropped},offeredBytes:clients.get(m.client).entityBudget.offeredBytes,admissionAgeTicks:[...clients.get(m.client).entity.records.values()].map(entry=>({id:entry.record.id,kind:entry.record.kind,age:Number.isFinite(entry.last)?game.worldClock.tick-entry.last:null}))}:undefined,inputDiagnostics:clients.get(m.client).inputDiagnostics,fireDiagnostics:clients.get(m.client).fireDiagnostics,players: clients.size, running: Boolean(timer) };
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
