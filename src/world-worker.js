import {parentPort,workerData} from 'node:worker_threads';import {pathToFileURL} from 'node:url';import {resolve} from 'node:path';import {dataReader} from './content.js';
// This override is private to a world worker. It cannot make network requests.
globalThis.fetch=await dataReader(workerData.data);
try{
 const root=resolve(workerData.engine);
 const {createGame}=await import(pathToFileURL(resolve(root,'src/game.js')));
 const {worldState}=await import(pathToFileURL(resolve(root,'src/runtime/world-state.js')));
 const {InputMailbox}=await import(pathToFileURL(resolve(root,'src/runtime/transport.js')));
 const game=await createGame({headless:true,query:workerData.query??'?map=nab2&side=rep&ai=classic&seed=1'});
 const inputs=new InputMailbox();let timer=null,last=performance.now(),debt=0;
 parentPort.postMessage({type:'ready',generation:game.playerUnit.generation});
 parentPort.on('message',m=>{try{
  if(m.type==='input'){if(m.command.tick>game.worldClock.tick+120||m.command.tick<game.worldClock.tick-120)throw Error('input outside tick window');inputs.push(m.command);}
  else if(m.type==='step'){for(let i=0;i<Math.min(m.count??1,600);i++)game.step(inputs.take(game.worldClock.tick+1,game.playerUnit.generation));parentPort.postMessage({type:'state',state:worldState(game)});}
  else if(m.type==='start'&&!timer){last=performance.now();timer=setInterval(()=>{try{const now=performance.now();debt=Math.min(debt+(now-last)/1000,8/60);last=now;let count=0;while(debt>=1/60&&count++<4){game.step(inputs.take(game.worldClock.tick+1,game.playerUnit.generation));debt-=1/60;}if(count)parentPort.postMessage({type:'tick',tick:game.worldClock.tick});}catch(e){clearInterval(timer);parentPort.postMessage({type:'error',message:e.message});}},5);}
  else if(m.type==='stop'){clearInterval(timer);timer=null;inputs.clear();}
 }catch(e){parentPort.postMessage({type:'error',message:e.message});}});
}catch(e){parentPort.postMessage({type:'error',message:e.message,stack:e.stack});}
