import test from 'node:test';
import assert from 'node:assert/strict';
import { OnlineServer } from '../src/online-server.js';

test('readiness responds for the exact launched match without creating a peer',async()=>{
  const token='a'.repeat(32),nonce='b'.repeat(32),from='c'.repeat(64),sent=[];
  const server=Object.create(OnlineServer.prototype);
  Object.assign(server,{query:`?map=nab2&side=rep&match=${token}`,isBanned:()=>false,config:{},master:{signal:(to,payload)=>sent.push({to,payload})}});
  Object.defineProperty(server,'peers',{get(){assert.fail('readiness must not allocate or access game peers');}});
  const probe=payload=>server.signal({from,payload:JSON.stringify(payload)});
  await probe({type:'candidate',matchProbe:token,nonce});
  assert.deepEqual(sent,[{to:from,payload:{type:'candidate',matchReady:token,nonce}}]);
  for(const payload of [
    {type:'candidate',matchProbe:'d'.repeat(32),nonce},
    {type:'candidate',matchProbe:token,nonce:'bad'},
    {type:'offer',matchProbe:token,nonce},
    {type:'candidate',matchProbe:'bad',nonce},
  ])await probe(payload);
  assert.equal(sent.length,1,'an old launch, malformed probe, or wrong envelope cannot claim readiness');
  server.isBanned=()=>true;
  await probe({type:'candidate',matchProbe:token,nonce});
  assert.equal(sent.length,1);
});
