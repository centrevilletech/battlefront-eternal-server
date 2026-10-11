import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { inflateSync } from 'node:zlib';
import assert from 'node:assert/strict';
import rtc from 'node-datachannel';
import { MasterClient } from '../src/master-client.js';
import { decodeBulkFrame } from '../src/bulk-frame.js';
import { Peer } from '../src/peer.js';
import { loadIdentity } from '../src/identity.js';
import { dataReader } from '../src/content.js';
const serverId=process.argv[2];
if(!/^[a-f0-9]{64}$/.test(serverId??''))throw Error('Pass the directory server identity as the first argument');
const root=resolve(process.env.BFE_ENGINE_DIR??'../battlefront-eternal');
const {contentHash}=await import(pathToFileURL(join(root,'src/runtime/content-hash.js')));
const {expandMetadata}=await import(pathToFileURL(join(root,'src/runtime/shared-packet.js')));
const {BinaryReader}=await import(pathToFileURL(join(root,'src/runtime/packet.js')));
const masterUrl=process.env.BFE_MASTER_URL??'https://online.battlefronteternal.com';
const directory=await mkdtemp(join(tmpdir(),'bfe-live-'));
const clients=[];
const wait=async(predicate,label)=>{for(let n=0;n<300;n++){if(predicate())return;await new Promise(r=>setTimeout(r,100));}throw Error('Timed out: '+label)};
try {
  for(let i=0;i<2;i++) {
    const master=new MasterClient({url:masterUrl,keys:loadIdentity(join(directory,String(i))),noticeAccepted:true});
    const listing=await master.request('/v1/servers/'+serverId);
    const [map,side]=listing.rotation[0].replace(/^swbf1\//,'').split(':');
    const content=await contentHash(`?map=${map}&side=${side}`,await dataReader(resolve(process.env.BFE_DATA_DIR??'../battlefront-eternal/public')));
    assert.equal(content,listing.content,'Local gameplay content matches deployed host');
    await master.connect();
    const peer=new Peer({name:'live-'+i,keys:master.keys,initiator:true,expectedKey:serverId,simulation:listing.simulation,content,capabilities:['compact-personal-v1','binary-bulk-v1'],iceServers:await master.credentials(serverId),relay:true,portable:true});
    const c={master,peer,reader:new BinaryReader(),fragments:new Map(),packets:0,sequence:0,ack:0,compactFrames:0,binaryFrames:0};clients.push(c);
    peer.on('signal',s=>master.signal(serverId,s));master.on('signal',m=>peer.signal(JSON.parse(m.payload)));
    peer.on('fault',message=>console.error(message));
    const packet=(data,metadata)=>{if(metadata?.compact===1)c.compactFrames++;const bytes=inflateSync(Buffer.from(data,'base64'));const p=c.reader.read(bytes.buffer.slice(bytes.byteOffset,bytes.byteOffset+bytes.byteLength));Object.assign(p,expandMetadata(metadata));c.tick=p.tick;c.ack=p.ack;c.packets++;};
    peer.on('binary',raw=>{c.binaryFrames++;const m=decodeBulkFrame(raw);let parts=c.fragments.get(m.id);if(!parts)c.fragments.set(m.id,parts=[]);parts[m.index]=m.data;if(m.index===0)parts.metadata=m.metadata;if(parts.filter(Boolean).length===m.total){packet(Buffer.concat(parts),parts.metadata);c.fragments.delete(m.id);}});
    peer.on('message',(_,m)=>{
      if(m.type==='welcome')c.welcome=m;
      if(m.type==='ticks')for(const buffer of m.buffers)packet(buffer,m.metadata);
      if(m.type==='packet'){let parts=c.fragments.get(m.id);if(!parts)c.fragments.set(m.id,parts=[]);parts[m.index]=m.data;if(m.index===0)parts.metadata=m.metadata;if(parts.filter(Boolean).length===m.total){packet(parts.join(''),parts.metadata);c.fragments.delete(m.id);}}
    });
    await wait(()=>c.welcome,'authoritative welcome');
    assert(!peer.spectator,'Matched content joins as a player');
    peer.send('events',{type:'call',id:1,method:'start'});
    await wait(()=>c.packets>0,'initial replica');c.firstTick=c.tick;
  }
  assert.notEqual(clients[0].welcome.playerId,clients[1].welcome.playerId);
  for(let step=0;step<120;step++) {
    for(const [i,c]of clients.entries())c.peer.send('state',{type:'input',command:{epoch:1,sequence:++c.sequence,tick:c.tick+2,generation:c.welcome.generation,down:[i?'KeyS':'KeyW'],actions:[],look:[]}});
    await new Promise(r=>setTimeout(r,17));
  }
  await wait(()=>clients.every(c=>c.ack>30),'input acknowledgment');
  for(const c of clients){assert(c.tick>c.firstTick+30);assert(c.packets>10);assert.equal(c.peer.path().kind,'relay');assert(c.compactFrames>0,'Host negotiated compact personal metadata');assert(c.binaryFrames>0,'Host negotiated binary bulk');}
  console.log(JSON.stringify({ok:true,serverId,clients:clients.map(c=>({playerId:c.welcome.playerId,tick:c.tick,ack:c.ack,packets:c.packets,compactFrames:c.compactFrames,binaryFrames:c.binaryFrames,path:c.peer.path()}))}));
} finally {for(const c of clients){c.peer.close();c.master.close();}rtc.cleanup();await rm(directory,{recursive:true,force:true});}
