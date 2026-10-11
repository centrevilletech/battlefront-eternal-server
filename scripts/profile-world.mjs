// Private local simulation profiler; no RTC or public network traffic.
import {Worker} from 'node:worker_threads';
import {resolve} from 'node:path';
const worker=new Worker(new URL('../src/world-worker.js',import.meta.url),{workerData:{engine:resolve(process.env.BFE_ENGINE_DIR??'../battlefront-eternal'),data:resolve(process.env.BFE_DATA_DIR??'../battlefront-eternal/public'),maxPlayers:64,armySize:32,metrics:true,profile:'/tmp/bfe-world64.cpuprofile'}});
const players=new Map();let tick=0,sequence=0,timer;
worker.on('message',m=>{
 if(m.type==='error')throw Error(m.message);
 if(m.type==='ready')for(let i=0;i<64;i++)worker.postMessage({type:'join',client:'profile-'+i});
 if(m.type==='joined'){players.set(m.client,{generation:m.player.generation,className:m.player.className});worker.postMessage({type:'call',client:m.client,id:1,method:'start'});if(players.size===64){worker.postMessage({type:'start'});timer=setInterval(()=>{sequence++;for(const [client,p]of players){worker.postMessage({type:'inputs',client,commands:[{epoch:1,sequence,tick:tick+3,generation:p.generation,down:['KeyW'],actions:[],look:[],...(p.phase==='awaiting'&&sequence%30===0?{human:[{kind:'spawn',className:p.className}]}:{})}]});if(sequence%60===1)worker.postMessage({type:'fire',client,edge:{epoch:1,id:sequence,tick:tick+3,generation:p.generation,button:0,pressed:Math.floor(sequence/60)%2===0}});}},1000/60);setTimeout(()=>{clearInterval(timer);worker.postMessage({type:'stop'});worker.postMessage({type:'metrics'});},30000);}}
 if(m.type==='tick')tick=m.tick;
 if(m.type==='packet')for(const r of m.recipients??[]){const p=players.get(r.client);if(p){p.generation=r.metadata.generation??p.generation;p.phase=r.metadata.personal?.spawn?.phase;p.className=r.metadata.playerClass;}}
 if(m.type==='metrics'){console.log(JSON.stringify({tick:m.tick,fireAccepted:m.fireAccepted,liveUnits:m.liveUnits,profile:'/tmp/bfe-world64.cpuprofile'}));worker.terminate();}
});
