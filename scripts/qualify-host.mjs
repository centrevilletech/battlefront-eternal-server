#!/usr/bin/env node
// A controlled service made from the packaged production worker and transport.
// JSON commands belong to this harness process, not the public game protocol.
import {Worker} from 'node:worker_threads';
import {createInterface} from 'node:readline';
import {loadHostRuntime} from '../src/host-runtime.js';
import {validateNetworkContract} from '../src/network-contract.js';
import {startNetworkThread} from '../src/udp-server.js';
import {startLocalServer} from '../src/local-server.js';
import {loadIdentity,fingerprint,keyId} from '../src/identity.js';
import {resolve,join} from 'node:path';
import {mkdir} from 'node:fs/promises';
import {pathToFileURL} from 'node:url';
const arg=(name,fallback)=>process.argv.find(a=>a.startsWith(`--${name}=`))?.slice(name.length+3)??fallback;
const engine=resolve(arg('engine','.')),data=resolve(arg('data','.')),directory=resolve(arg('state','.'));
const backend=arg('backend','wasm'),workerModule=arg('world-worker',null);
if(!['wasm','native'].includes(backend))throw Error('Invalid qualification backend');
if(backend==='native'&&!workerModule)throw Error('Native qualification requires a harness world-worker module');
await mkdir(directory,{recursive:true});
const runtime=await loadHostRuntime(engine);
if(!runtime.manifest)throw Error('Service qualification requires a packaged engine/server pair');
if(backend==='wasm'&&runtime.manifest.worldBackend!=='wasm')throw Error('Bundle backend mismatch');
if(backend==='native'){
  // A future harness worker can use the shared native World before it is a
  // packaged default. It must explicitly declare its capability and contract.
  const module=await import(pathToFileURL(resolve(workerModule)));
  if(module.qualificationBackend!=='native')throw Error('Invalid native qualification worker');
}
const send=value=>process.stdout.write(JSON.stringify(value)+'\n');
const errors=[],worlds=new Map();let nextId=0,shuttingDown=false;
function boundedError(error){const text=String(error?.stack??error).slice(0,2000);if(errors.length<32)errors.push(text);return text;}
async function makeWorld(index,{map='nab2',side='rep',clients=64,army=32}={},existingTransport=null){
  if(!Number.isInteger(clients)||clients<1||clients>64)throw Error('Invalid advertised clients');
  const keys=loadIdentity(join(directory,`identity-${index}.json`));
  const transport=existingTransport??await startNetworkThread({engine,keys,port:0,host:'127.0.0.1',maxPerAddress:128,maxSessions:clients+32});
  let worker,local,closed=false,closing=null,ready,waiters=new Map();
  const request=(type,body={},expected=type)=>new Promise((resolve,reject)=>{
    const id=++nextId,timeout=setTimeout(()=>{waiters.delete(id);reject(Error(`World ${type} timeout`));},60000);
    waiters.set(id,{expected,resolve:value=>{clearTimeout(timeout);resolve(value);},reject:error=>{clearTimeout(timeout);reject(error);}});
    worker.postMessage({type,id,...body});
  });
  const close=async(keepTransport=false)=>{
    if(closing)return closing;
    closing=(async()=>{
      closed=true;await local?.close();if(!keepTransport)await transport.close();
      if(worker){try{await request('close',{},'closed');}finally{await worker.terminate();}}
      return {clients:0,workerExited:true};
    })();return closing;
  };
  try{
    const query=`?map=${map}&side=${side}&ai=classic&seed=1`;
    ready=new Promise((resolveReady,rejectReady)=>{
      worker=new Worker(workerModule?pathToFileURL(resolve(workerModule)):new URL('../src/world-worker.js',import.meta.url),
        {workerData:{engine,data,query,maxPlayers:clients,armySize:army,autoSpawn:true,autoAssignTeams:true,stateBytesPerSecond:100000}});
      worker.on('error',error=>{boundedError(error);rejectReady(error);for(const p of waiters.values())p.reject(error);waiters.clear();});
      worker.on('exit',code=>{if(!closed){const e=Error(`World exited ${code}`);boundedError(e);rejectReady(e);}for(const p of waiters.values())p.reject(Error('World exited'));waiters.clear();});
      worker.on('message',m=>{
        if(m.type==='ready'){worker.networkContract=validateNetworkContract(m.networkContract);if(m.hostBuild!==runtime.manifest.engineBuild)return rejectReady(Error('Worker build mismatch'));resolveReady(m);}
        else if(m.type==='error'){const error=Error(m.message);boundedError(error);rejectReady(error);for(const p of waiters.values())p.reject(error);waiters.clear();}
        else if(m.id!==undefined){const p=waiters.get(m.id);if(p&&m.type===p.expected){waiters.delete(m.id);p.resolve(m);}}
      });
    });
    let loadTimer;
    const boot=await Promise.race([ready,new Promise((_,reject)=>{loadTimer=setTimeout(()=>reject(Error('World load timeout')),120000);})]).finally(()=>clearTimeout(loadTimer));
    local=await startLocalServer({config:{name:'Qualification service',bind:'127.0.0.1',discovery:false,content:boot.content,maxPlayers:clients,rotation:[`${map}:${side}`]},identity:{keys,id:keyId(keys.publicKey),fingerprint:fingerprint(keys)},world:worker,query,udp:transport});
    worker.postMessage({type:'start'});
    return {index,worker,transport,local,config:{map,side,clients,army},close,info:()=>local.info(),
      async sample(){const sample=await request('hostDiagnostics',{},'hostDiagnostics');return {index,...sample.sample,network:await transport.metrics(),sessions:await transport.diagnostics()};}};
  }catch(error){await close().catch(()=>{});throw error;}
}
async function handle(m){
  if(m.op==='start'){
    if(worlds.has(m.index))throw Error('World already exists');const world=await makeWorld(m.index,m);worlds.set(m.index,world);return world.info();
  }
  if(m.op==='sample')return {pid:process.pid,js:process.memoryUsage(),worlds:await Promise.all([...worlds.values()].map(w=>w.sample())),errors:[...errors]};
  if(m.op==='rotate'){
    const old=worlds.get(m.index);if(!old)throw Error('Missing world');await old.close(true);await old.close(true);worlds.delete(m.index);
    // The real game port outlives a battle. Reusing it exposes stale-session
    // and direct-port retention that a fresh endpoint would conceal.
    const world=await makeWorld(m.index,{...old.config,...m},old.transport);worlds.set(m.index,world);return world.info();
  }
  if(m.op==='close'){
    const world=worlds.get(m.index);if(!world)return {alreadyClosed:true};const result=await world.close();await world.close();worlds.delete(m.index);return result;
  }
  if(m.op==='shutdown'){await shutdown();return {closed:true};}
  throw Error('Unknown qualification command');
}
async function shutdown(){if(shuttingDown)return;shuttingDown=true;await Promise.all([...worlds.values()].map(w=>w.close()));worlds.clear();}
const input=createInterface({input:process.stdin});let chain=Promise.resolve();
input.on('line',line=>{chain=chain.then(async()=>{
  let m;try{m=JSON.parse(line);send({id:m.id,result:await handle(m)});if(m.op==='shutdown'){input.close();process.stdin.destroy();}}
  catch(error){send({id:m?.id,error:boundedError(error)});}
});});
input.on('close',()=>{chain.finally(()=>shutdown()).catch(error=>{boundedError(error);process.exitCode=1;});});
for(const signal of ['SIGINT','SIGTERM'])process.once(signal,()=>{shutdown().finally(()=>process.exit(0));});
send({type:'ready',pid:process.pid,backend,bundle:{engine:runtime.manifest.engineRevision,server:runtime.manifest.serverRevision,build:runtime.manifest.engineBuild}});
