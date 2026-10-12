// Local-only actual RTC benchmark. Client processes isolate server event-loop cost.
// Application loss/retransmit delays are synthetic, not kernel/SCTP loss emulation.
import {Worker} from 'node:worker_threads';
import {fork,execFileSync} from 'node:child_process';
import {mkdtemp,rm,readdir,readFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {monitorEventLoopDelay} from 'node:perf_hooks';
import rtc from 'node-datachannel';
import {Peer} from '../src/peer.js';
import {GameSession,compressionStats} from '../src/game-session.js';
import {loadIdentity} from '../src/identity.js';
const arg=(name,fallback)=>Number(process.argv.find(v=>v.startsWith('--'+name+'='))?.split('=')[1]??fallback);
const count=arg('players',64),seconds=arg('seconds',30),loss=arg('loss',.05),jitter=arg('jitter',50),latency=arg('latency',50),portableEvery=arg('portable-every',8),retryMs=arg('retry-ms',200);
if(![1,2,8,32,64].includes(count)||seconds<1||seconds>600||loss<0||loss>.5)throw Error('Invalid scenario');
if(arg('sack-ms',-1)>=0)rtc.setSctpSettings({delayedSackTime:arg('sack-ms',20)});
const directory=await mkdtemp(join(tmpdir(),'bfe-load-')),root=resolve(process.env.BFE_ENGINE_DIR??'../battlefront-eternal');
const provenance=directory=>{try{return{sha:execFileSync('git',['-C',directory,'rev-parse','HEAD'],{encoding:'utf8'}).trim(),dirty:!!execFileSync('git',['-C',directory,'status','--porcelain'],{encoding:'utf8'}).trim()};}catch{return null;}};
const sourceRevisions={server:provenance(process.cwd()),engine:provenance(root)};
const world=new Worker(new URL('../src/world-worker.js',import.meta.url),{name:'bfe-world',workerData:{engine:root,data:resolve(process.env.BFE_DATA_DIR??'../battlefront-eternal/public'),maxPlayers:64,armySize:32,metrics:true,profile:process.argv.find(value=>value.startsWith('--profile='))?.slice(10)}});
const hosts=[],sessions=[],children=[],samples=[],ages=[],arrivalAges=[],backlogs=[],timers=new Set();const wireKinds={state:0,reliableSnapshots:0,events:0,legacy:0};const recordWire=(label,message,size)=>{wire+=size;const magic=Buffer.isBuffer(message)&&message.length>=4?message.readUInt32LE(0):0;wireKinds[label==='state'?'state':magic===0x32424642?'reliableSnapshots':magic===0x33424642?'events':'legacy']+=size;};let failed,wire=0,raw=0,seed=42,sampler,totalRss=0,serverTick=0;
const random=()=>((seed=(Math.imul(seed,1664525)+1013904223)>>>0)/4294967296);
const later=(fn,ms)=>{const t=setTimeout(()=>{timers.delete(t);fn();},ms);timers.add(t);};
const receive=(source,type)=>new Promise((resolve,reject)=>{const fn=m=>{if(m.type==='error'){source.off('message',fn);reject(Error(m.message));}else if(m.type===type){source.off('message',fn);resolve(m);}};source.on('message',fn);});
const wait=async(p,label)=>{for(let i=0;i<900;i++){if(failed)throw failed;if(p())return;await new Promise(r=>setTimeout(r,100));}throw Error('Timeout '+label);};
const summary=a=>{a=[...a].sort((x,y)=>x-y);const q=p=>a[Math.min(a.length-1,Math.floor(a.length*p))]??0;return{count:a.length,p50:q(.5),p95:q(.95),p99:q(.99),max:q(1)};};
world.on('message',m=>{if(m.type==='error')failed=Error(m.message);if(m.type==='packet')raw+=m.buffer.byteLength;if(m.type==='tick')serverTick=m.tick;});world.on('error',e=>failed=e);
const clockTicks=process.platform==='linux'?Number(execFileSync('getconf',['CLK_TCK'],{encoding:'utf8'})):0;
const threadSnapshot=async()=>{
 const result=new Map();if(!clockTicks)return result;
 for(const tid of await readdir('/proc/self/task'))try{const stat=await readFile(`/proc/self/task/${tid}/stat`,'utf8'),end=stat.lastIndexOf(')'),fields=stat.slice(end+2).split(' ');result.set(tid,{name:stat.slice(stat.indexOf('(')+1,end),ticks:Number(fields[11])+Number(fields[12])});}catch{}
 return result;
};
const loop=monitorEventLoopDelay({resolution:10});loop.enable();
try{
 const ready=await receive(world,'ready'),keys=loadIdentity(join(directory,'host'));
 for(let i=0;i<Math.ceil(count/8);i++){
 const child=fork(new URL('./load-client.mjs',import.meta.url),[],{execArgv:['--max-old-space-size=512'],stdio:['ignore','inherit','inherit','ipc']});children.push(child);
 child.on('message',m=>{if(m.type==='signal')hosts[m.index].signal(m.signal);else if(m.type==='peerClosed')console.error('Load client closed',m.index,m.reason);else if(m.type==='welcome')hosts[m.index].welcomed=true;else if(m.type==='error')failed=Error(m.message);else if(m.type==='sample'){samples[i]=m;arrivalAges.push(...m.clients.map(c=>c.ageMs));ages.push(...m.clients.map(c=>Math.max(0,serverTick-c.tick)*1000/60));totalRss=Math.max(totalRss,process.memoryUsage().rss+samples.reduce((n,s)=>n+s.rss,0));if(totalRss>8*1024**3)failed=Error('8GiB memory budget exceeded');}});
 child.on('exit',code=>{if(code)failed=Error('Client process exited '+code);});
 }
 for(let index=0;index<count;index++){
 const child=children[Math.floor(index/8)],host=new Peer({name:'load-server-'+index,keys,simulation:'bfe-sim-2',content:ready.content,portable:false,iceUdpMuxPort:arg('mux-port',0),stateBufferLimit:arg('state-buffer',65536)});hosts[index]=host;
 const send=host.send.bind(host),sendBinary=host.sendBinary.bind(host),sendStatePacket=host.sendStatePacket.bind(host),queues=new Map();let queuedBytes=0;
 const enqueue=(label,message)=>{if(label==='state'){if(random()<loss)return true;later(()=>{if(sendBinary(message,'state'))recordWire('state',message,message.length);},latency+random()*jitter);return true;}let lane=queues.get(label);if(!lane)queues.set(label,lane={queue:[],due:0,pumping:false});const size=Buffer.isBuffer(message)?message.length:Buffer.byteLength(JSON.stringify(message));if(queuedBytes+size>256*1024)return false;
 lane.due=Math.max(lane.due,Date.now()+latency+random()*jitter)+(label==='bulk'&&random()<loss?retryMs:0);lane.queue.push({message,size,due:lane.due});queuedBytes+=size;
 const pump=()=>{lane.pumping=false;while(lane.queue.length&&lane.queue[0].due<=Date.now()){const item=lane.queue[0];if(!(Buffer.isBuffer(item.message)?sendBinary(item.message):send(label,item.message)))break;if(label==='bulk')recordWire(label,item.message,item.size);queuedBytes-=item.size;lane.queue.shift();}if(lane.queue.length&&!host.closed){lane.pumping=true;later(pump,Math.max(5,lane.queue[0].due-Date.now()));}};
 if(!lane.pumping){lane.pumping=true;later(pump,Math.max(0,lane.due-Date.now()));}return true;};
 host.send=enqueue;host.sendBinary=(message,label='bulk')=>enqueue(label,message);
 host.sendStatePacket=frames=>{
 if(!loss&&!latency&&!jitter){if(!sendStatePacket(frames))return false;for(const frame of frames)recordWire('state',frame,frame.length);return true;}
 if(host.channels.get('state')?.bufferedAmount()>host.stateBufferLimit)return false;
 const delivered=frames.filter(()=>random()>=loss);if(delivered.length)later(()=>{if(sendStatePacket(delivered))for(const frame of delivered)recordWire('state',frame,frame.length);},latency+random()*jitter);return true;
 };
 host.backlog=()=>queuedBytes+[...host.channels.values()].reduce((n,ch)=>n+ch.bufferedAmount(),0);host.on('signal',signal=>child.send({type:'signal',index,signal}));host.once('ready',r=>sessions.push(new GameSession(host,world,r.id,{},{journalFrameBytes:arg('journal-frame',20000)})));
 child.send({type:'create',index,root,directory,content:ready.content,portable:portableEvery>0&&index%portableEvery===0,loss,jitter,latency,compact:arg('compact',1)!==0,binary:arg('binary',1)!==0,linear:arg('linear',1)!==0,backups:arg('backups',1),journal:arg('journal',0)!==0,relative:arg('relative',0)!==0,stateLifeMs:arg('state-life',0),stateBufferLimit:arg('state-buffer',65536),sackMs:arg('sack-ms',-1),inputLeadMs:arg('input-lead-ms',2*(latency+jitter))});
 await wait(()=>host.welcomed,'peer '+index);
 }
 world.postMessage({type:'start'});sampler=setInterval(()=>{children.forEach(c=>c.send({type:'sample'}));backlogs.push(...hosts.map(h=>h.backlog()));},250);
 await wait(()=>samples.reduce((n,s)=>n+s.clients.filter(c=>c.packets>0).length,0)===count,'initial frames');
 await new Promise(r=>setTimeout(r,arg('warmup',5)*1000));world.postMessage({type:'resetMetrics'});ages.length=0;arrivalAges.length=0;backlogs.length=0;compressionStats.durations.length=0;compressionStats.calls=0;loop.reset();
 const startKinds={...wireKinds};const threadStart=await threadSnapshot();
 const begun=performance.now(),startWire=wire,startRaw=raw,cpu=process.cpuUsage(),threadCpu=process.threadCpuUsage();children.forEach(c=>c.send({type:'begin'}));
 await new Promise(r=>setTimeout(r,seconds*1000));if(failed)throw failed;
 clearInterval(sampler);const replies=children.map(c=>receive(c,'result'));children.forEach(c=>c.send({type:'finish'}));const results=(await Promise.all(replies)).flatMap(r=>r.clients);
 world.postMessage({type:'stop'});const measurement=receive(world,'metrics');world.postMessage({type:'metrics'});const m=await measurement;
 const threadEnd=await threadSnapshot();
 const elapsed=(performance.now()-begun)/1000,usage=process.cpuUsage(cpu),threadUsage=process.threadCpuUsage(threadCpu);
 console.log(JSON.stringify({sourceRevisions,players:count,seconds:elapsed,impairment:{kind:'synthetic application loss and ordered bulk retry delay; not SCTP loss',loss,jitterMs:jitter,latencyMs:latency,retryMs},clientProcesses:children.length,serverBackend:'native',iceUdpMuxPort:arg('mux-port',0),transportTuning:{inputLeadMs:arg('input-lead-ms',2*(latency+jitter)),stateLifeMs:arg('state-life',0),stateBufferLimit:arg('state-buffer',65536),journalFrameBytes:arg('journal-frame',20000),delayedSackMs:arg('sack-ms',-1)},threadCpu:[...threadEnd].map(([tid,value])=>({tid,name:value.name,cores:(value.ticks-(threadStart.get(tid)?.ticks??value.ticks))/clockTicks/elapsed})).sort((a,b)=>b.cores-a.cores).slice(0,16),connected:hosts.filter(h=>!h.closed).length,acknowledgedClients:results.filter(c=>c.ack>0).length,simMs:summary(m.sim),encodeAllPlayersMs:summary(m.encode),wholeStepMs:summary(m.total),compressionMs:summary(compressionStats.durations),compressionCalls:compressionStats.calls,droppedTicks:m.droppedTicks,serverTick:m.tick,journalBootstraps:m.journalBootstraps,journalCheckpointPackets:m.journalCheckpointPackets,humans:m.humans,linearProjectiles:m.linearProjectiles,invalidatedActions:results.reduce((n,c)=>n+c.invalidatedActions,0),actionLatencyMs:summary(results.flatMap(c=>c.actionLatency)),actionAcknowledgedClients:results.filter(c=>c.ackActions>0).length,fireAccepted:m.fireAccepted,fireRejected:m.fireRejected,liveUnits:m.liveUnits,armies:m.armies,effectiveTicksPerSecond:summary(results.map(c=>(c.tick-c.firstTick)/elapsed)),rttMs:summary(results.flatMap(c=>c.rtt)),packetGapMs:summary(results.flatMap(c=>c.packetGaps)),stateAgeMs:summary(ages),timeSincePacketMs:summary(arrivalAges),ackLagCommands:summary(results.flatMap(c=>c.ackLag)),transportBacklogBytes:summary(backlogs),inputBytesPerSecond:results.reduce((n,c)=>n+c.inputWire,0)/elapsed,inputBytesPerPlayerSecond:results.reduce((n,c)=>n+c.inputWire,0)/elapsed/count,wireKindBytesPerPlayerSecond:Object.fromEntries(Object.entries(wireKinds).map(([kind,bytes])=>[kind,(bytes-startKinds[kind])/elapsed/count])),wireBytesPerSecond:(wire-startWire)/elapsed,wireBytesPerPlayerSecond:(wire-startWire)/elapsed/count,rawBytesPerSecond:(raw-startRaw)/elapsed,eventLoopMs:{p50:loop.percentile(50)/1e6,p95:loop.percentile(95)/1e6,p99:loop.percentile(99)/1e6,max:loop.max/1e6},serverCpuCores:(usage.user+usage.system)/1e6/elapsed,serverWorldThreadCores:(m.threadCpu.user+m.threadCpu.system)/1e6/elapsed,serverOtherThreadsCores:(usage.user+usage.system-threadUsage.user-threadUsage.system-m.threadCpu.user-m.threadCpu.system)/1e6/elapsed,serverMainThreadCores:(threadUsage.user+threadUsage.system)/1e6/elapsed,totalRssMiB:totalRss/1024**2}));
}finally{clearInterval(sampler);for(const t of timers)clearTimeout(t);sessions.forEach(s=>s.close());hosts.forEach(p=>p.close());children.forEach(c=>{if(c.connected)c.send({type:'close'});});loop.disable();await world.terminate();rtc.cleanup();await rm(directory,{recursive:true,force:true});}
