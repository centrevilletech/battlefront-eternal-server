import test from 'node:test';
import assert from 'node:assert/strict';
import { OnlineServer } from '../src/online-server.js';

test('readiness responds for the exact launched match without creating a peer',async()=>{
  const token='a'.repeat(32),nonce='b'.repeat(32),from='c'.repeat(64),sent=[];
  const server=Object.create(OnlineServer.prototype);
  Object.assign(server,{query:`?map=nab2&side=rep&match=${token}`,isBanned:()=>false,config:{},master:{signal:(to,payload)=>sent.push({to,payload})}});
  Object.defineProperty(server,'peers',{get(){assert.fail('readiness must not allocate or access game peers');}});
  const probe=payload=>server.signal({from,type:'matchProbe',payload:JSON.stringify(payload)});
  await probe({type:'matchProbe',matchProbe:token,nonce});
  assert.deepEqual(sent,[{to:from,payload:{type:'matchReady',matchReady:token,nonce}}]);
  for(const payload of [
    {type:'matchProbe',matchProbe:'d'.repeat(32),nonce},
    {type:'matchProbe',matchProbe:token,nonce:'bad'},
    {type:'offer',matchProbe:token,nonce},
    {type:'matchProbe',matchProbe:'bad',nonce},
    {type:'matchProbe',matchProbe:[token],nonce},
    {type:'matchProbe',matchProbe:token,nonce:[nonce]},
    {type:'matchProbe',matchProbe:token,nonce,extra:true},
    {type:'matchProbe',matchProbe:token},
    {type:'matchProbe',nonce},
    [token],
    null,
  ])await probe(payload);
  await server.signal({from,type:'candidate',payload:JSON.stringify({type:'matchProbe',matchProbe:token,nonce})});
  assert.equal(sent.length,1,'an old launch, malformed probe, or wrong envelope cannot claim readiness');
  server.isBanned=()=>true;
  await probe({type:'matchProbe',matchProbe:token,nonce});
  assert.equal(sent.length,1);
});
