// Local-only actual RTC benchmark. Client processes isolate server event-loop cost.
// Application loss/retransmit delays are synthetic, not kernel/SCTP loss emulation.
import {Worker} from 'node:worker_threads';
import {fork} from 'node:child_process';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {monitorEventLoopDelay} from 'node:perf_hooks';
import rtc from 'node-datachannel';
import {Peer} from '../src/peer.js';
import {GameSession,compressionStats} from '../src/game-session.js';
import {loadIdentity} from '../src/identity.js';
const arg=(name,fallback)=>Number(process.argv.find(v=>v.startsWith('--'+name+'='))?.split('=')[1]??fallback);
const count=arg('players',64),seconds=arg('seconds',30),loss=arg('loss',.05),jitter=arg('jitter',50),latency=arg('latency',50),portableEvery=arg('portable-every',8),retryMs=arg('retry-ms',200);
if(![1,8,32,64].includes(count)||seconds<1||seconds>600||loss<0||loss>.5)throw Error('Invalid scenario');
const directory=await mkdtemp(join(tmpdir(),'bfe-load-')),root=resolve(process.env.BFE_ENGINE_DIR??'../battlefront-eternal');
const world=new Worker(new URL('../src/world-worker.js',import.meta.url),{workerData:{engine:root,data:resolve(process.env.BFE_DATA_DIR??'../battlefront-eternal/public'),maxPlayers:64,armySize:32,metrics:true}});
const hosts=[],sessions=[],children=[],samples=[],ages=[],backlogs=[],timers=new Set();let failed,wire=0,raw=0,seed=42,sampler,totalRss=0;
const random=()=>((seed=(Math.imul(seed,1664525)+1013904223)>>>0)/4294967296);
const later=(fn,ms)=>{const t=setTimeout(()=>{timers.delete(t);fn();},ms);timers.add(t);};
const receive=(source,type)=>new Promise((resolve,reject)=>{const fn=m=>{if(m.type==='error'){source.off('message',fn);reject(Error(m.message));}else if(m.type===type){source.off('message',fn);resolve(m);}};source.on('message',fn);});
const wait=async(p,label)=>{for(let i=0;i<900;i++){if(failed)throw failed;if(p())return;await new Promise(r=>setTimeout(r,100));}throw Error('Timeout '+label);};
const summary=a=>{a=[...a].sort((x,y)=>x-y);const q=p=>a[Math.min(a.length-1,Math.floor(a.length*p))]??0;return{count:a.length,p50:q(.5),p95:q(.95),p99:q(.99),max:q(1)};};
world.on('message',m=>{if(m.type==='error')failed=Error(m.message);if(m.type==='packet')raw+=m.buffer.byteLength;});world.on('error',e=>failed=e);
const loop=monitorEventLoopDelay({resolution:10});loop.enable();
try{
 const ready=await receive(world,'ready'),keys=loadIdentity(join(directory,'host'));
 for(let i=0;i<Math.ceil(count/8);i++){
 const child=fork(new URL('./load-client.mjs',import.meta.url),[],{execArgv:['--max-old-space-size=512'],stdio:['ignore','inherit','inherit','ipc']});children.push(child);
 child.on('message',m=>{if(m.type==='signal')hosts[m.index].signal(m.signal);else if(m.type==='welcome')hosts[m.index].welcomed=true;else if(m.type==='error')failed=Error(m.message);else if(m.type==='sample'){samples[i]=m;ages.push(...m.clients.map(c=>c.ageMs));totalRss=Math.max(totalRss,process.memoryUsage().rss+samples.reduce((n,s)=>n+s.rss,0));if(totalRss>8*1024**3)failed=Error('8GiB memory budget exceeded');}});
 child.on('exit',code=>{if(code)failed=Error('Client process exited '+code);});
 }
 for(let index=0;index<count;index++){
 const child=children[Math.floor(index/8)],host=new Peer({name:'load-server-'+index,keys,simulation:'bfe-sim-2',content:ready.content,portable:false});hosts[index]=host;
 const send=host.send.bind(host),queue=[];let due=0,pumping=false,queuedBytes=0;
 const pump=()=>{pumping=false;while(queue.length&&queue[0].due<=Date.now()){const item=queue[0];if(!send(item.label,item.message))break;if(item.label==='bulk')wire+=item.size;queuedBytes-=item.size;queue.shift();}if(queue.length&&!host.closed){pumping=true;later(pump,Math.max(5,queue[0].due-Date.now()));}};
 host.send=(label,message)=>{const size=Buffer.byteLength(JSON.stringify(message));if(queuedBytes+size>256*1024)return false;due=Math.max(due,Date.now()+latency+random()*jitter)+(label==='bulk'&&random()<loss?retryMs:0);queue.push({label,message,size,due});queuedBytes+=size;if(!pumping){pumping=true;later(pump,Math.max(0,due-Date.now()));}return true;};
 host.backlog=()=>queuedBytes;host.on('signal',signal=>child.send({type:'signal',index,signal}));host.once('ready',r=>sessions.push(new GameSession(host,world,r.id,{})));
 child.send({type:'create',index,root,directory,content:ready.content,portable:portableEvery>0&&index%portableEvery===0,loss,jitter,latency});
 await wait(()=>host.welcomed,'peer '+index);
 }
 world.postMessage({type:'start'});sampler=setInterval(()=>{children.forEach(c=>c.send({type:'sample'}));backlogs.push(...hosts.map(h=>h.backlog()));},250);
 await wait(()=>samples.reduce((n,s)=>n+s.clients.filter(c=>c.packets>0).length,0)===count,'initial frames');
 const begun=performance.now(),startWire=wire,startRaw=raw,cpu=process.cpuUsage(),threadCpu=process.threadCpuUsage();children.forEach(c=>c.send({type:'begin'}));
 await new Promise(r=>setTimeout(r,seconds*1000));if(failed)throw failed;
 clearInterval(sampler);const replies=children.map(c=>receive(c,'result'));children.forEach(c=>c.send({type:'finish'}));const results=(await Promise.all(replies)).flatMap(r=>r.clients);
 world.postMessage({type:'stop'});const measurement=receive(world,'metrics');world.postMessage({type:'metrics'});const m=await measurement;
 const elapsed=(performance.now()-begun)/1000,usage=process.cpuUsage(cpu),threadUsage=process.threadCpuUsage(threadCpu);
 console.log(JSON.stringify({players:count,seconds:elapsed,impairment:{kind:'synthetic application loss and ordered bulk retry delay; not SCTP loss',loss,jitterMs:jitter,latencyMs:latency,retryMs},clientProcesses:children.length,serverBackend:'native',connected:hosts.filter(h=>!h.closed).length,acknowledgedClients:results.filter(c=>c.ack>0).length,simMs:summary(m.sim),encodeAllPlayersMs:summary(m.encode),wholeStepMs:summary(m.total),compressionMs:summary(compressionStats.durations),compressionCalls:compressionStats.calls,droppedTicks:m.droppedTicks,serverTick:m.tick,humans:m.humans,liveUnits:m.liveUnits,armies:m.armies,effectiveTicksPerSecond:summary(results.map(c=>(c.tick-c.firstTick)/elapsed)),rttMs:summary(results.flatMap(c=>c.rtt)),packetGapMs:summary(results.flatMap(c=>c.packetGaps)),stateAgeMs:summary(ages),ackLagCommands:summary(results.flatMap(c=>c.ackLag)),transportBacklogBytes:summary(backlogs),wireBytesPerSecond:(wire-startWire)/elapsed,wireBytesPerPlayerSecond:(wire-startWire)/elapsed/count,rawBytesPerSecond:(raw-startRaw)/elapsed,eventLoopMs:{p50:loop.percentile(50)/1e6,p95:loop.percentile(95)/1e6,p99:loop.percentile(99)/1e6,max:loop.max/1e6},serverCpuCores:(usage.user+usage.system)/1e6/elapsed,serverMainThreadCores:(threadUsage.user+threadUsage.system)/1e6/elapsed,totalRssMiB:totalRss/1024**2}));
}finally{clearInterval(sampler);for(const t of timers)clearTimeout(t);sessions.forEach(s=>s.close());hosts.forEach(p=>p.close());children.forEach(c=>{if(c.connected)c.send({type:'close'});});loop.disable();await world.terminate();rtc.cleanup();await rm(directory,{recursive:true,force:true});}
