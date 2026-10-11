import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, readFile, stat } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import http from 'node:http';
import { OperatorStore, operatorHandler, operatorRequest } from '../src/operator.js';
test('identity statistics survive restart and repeated checkpoints do not double count',async()=>{
  const directory=await mkdtemp(join(tmpdir(),'bfe-stats-')),id='a'.repeat(64);let store;
  try{store=new OperatorStore(directory);const players=[{id,name:'Player',kills:4,deaths:2,points:1}];store.scores('match1',players);store.scores('match1',players);store.scores('match2',[{...players[0],kills:2}]);store.ban(id,0,'host rule');store.close();store=new OperatorStore(directory);assert(store.banned(id));const [stats]=store.stats(id);assert.equal(stats.matches,2);assert.equal(stats.kills,6);assert.equal(stats.deaths,4);store.unban(id);assert(!store.banned(id));assert.throws(()=>store.ban('../path',0,''));}finally{store?.close();await rm(directory,{recursive:true,force:true});}
});
test('loopback operator controls require the local token and bounded JSON requests',async()=>{
  const directory=await mkdtemp(join(tmpdir(),'bfe-control-'));let invoked=0;
  const handler=operatorHandler(directory,command=>{invoked++;return {action:command.action};});
  const server=http.createServer(async(req,res)=>{if(!await handler(req,res))res.writeHead(404).end();});
  await new Promise(r=>server.listen(0,'127.0.0.1',r));const port=server.address().port;
  try{
    assert.equal((await fetch(`http://127.0.0.1:${port}/admin`,{method:'POST'})).status,401);assert.equal(invoked,0);
    assert.deepEqual(await operatorRequest(directory,{action:'status'},port),{action:'status'});assert.equal(invoked,1);
    const token=await readFile(join(directory,'operator-token'),'utf8');
    assert.equal((await fetch(`http://127.0.0.1:${port}/admin`,{method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},body:JSON.stringify({text:'x'.repeat(5000)})})).status,400);assert.equal(invoked,1);
    if(process.platform!=='win32')assert.equal((await stat(join(directory,'operator-token'))).mode&0o777,0o600);
  }finally{await new Promise(r=>server.close(r));await rm(directory,{recursive:true,force:true});}
});
