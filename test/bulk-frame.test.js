import {test} from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import rtc from 'node-datachannel';
import {encodeBulkFrame,decodeBulkFrame} from '../src/bulk-frame.js';
import {Peer} from '../src/peer.js';
import {loadIdentity} from '../src/identity.js';
test('binary bulk rejects malformed lengths, duplicate metadata positions, and excessive fragments',()=>{
 const data=randomBytes(20000),metadata={playerId:82,text:'unicode✓'};
 const frame=encodeBulkFrame({id:9,index:0,total:2,data,metadata}),decoded=decodeBulkFrame(frame);
 assert.deepEqual(decoded.data,data);assert.deepEqual(decoded.metadata,metadata);assert.equal(decoded.id,9);
 const bad=Buffer.from(frame);bad.writeUInt32LE(999999,12);assert.throws(()=>decodeBulkFrame(bad));
 assert.throws(()=>encodeBulkFrame({id:1,index:1,total:2,data,metadata}));
 assert.throws(()=>encodeBulkFrame({id:1,index:0,total:129,data}));assert.throws(()=>decodeBulkFrame(Buffer.alloc(15)));
});
test('authenticated native and portable peers exchange raw binary bulk in both directions',{timeout:15000},async()=>{
 const dir=await mkdtemp(join(tmpdir(),'bfe-binary-'));let a,b;
 try{
  const options={simulation:'test',content:'test',capabilities:['binary-bulk-v1','snapshot-journal-v1']};
  a=new Peer({...options,name:'portable',portable:true,initiator:true,keys:loadIdentity(join(dir,'a'))});
  b=new Peer({...options,name:'native',portable:false,keys:loadIdentity(join(dir,'b'))});
  a.on('signal',value=>b.signal(value));b.on('signal',value=>a.signal(value));
  await Promise.all([new Promise(r=>a.once('ready',r)),new Promise(r=>b.once('ready',r))]);
  const frame=encodeBulkFrame({id:42,index:0,total:1,data:randomBytes(20000),metadata:{compact:1}});
  for(const [sender,receiver,lane]of[[a,b,'bulk'],[b,a,'bulk']]){const got=new Promise(r=>receiver.once('binary',r));assert(sender.sendBinary(frame,lane));assert.deepEqual(await got,frame);}
 }finally{a?.close();b?.close();rtc.cleanup();await rm(dir,{recursive:true,force:true});}
});
