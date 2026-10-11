import{mkdtemp,rm}from'node:fs/promises';import{tmpdir}from'node:os';import{join}from'node:path';import rtc from'node-datachannel';import{loadIdentity}from'../src/identity.js';import{MasterClient}from'../src/master-client.js';import{Peer}from'../src/peer.js';
const dir=await mkdtemp(join(tmpdir(),'bfe-relay-test-'));const keys=loadIdentity(join(dir,'id'));const master=new MasterClient({url:'https://online.battlefronteternal.com',keys,noticeAccepted:true});let registered=false;
try{
 await master.request('/v1/servers','POST',{name:'Deployment connectivity test',protocol:1,simulation:'connectivity-test',content:'none',rotation:['swbf1/nab2:rep:conquest'],players:0,maxPlayers:2,relayPolicy:'always',endpoints:[],fingerprint:'sha-256 '+Array(32).fill('00').join(':')});registered=true;
 const servers=await master.credentials(master.id);
 for(const ice of servers){
  const a=new Peer({name:'probe-a',keys,initiator:true,iceServers:[ice],relay:true,simulation:'test',content:'test'}),b=new Peer({name:'probe-b',keys:loadIdentity(join(dir,'peer')),iceServers:[ice],relay:true,simulation:'test',content:'test'});
  a.on('signal',s=>b.signal(s));b.on('signal',s=>a.signal(s));
  let deadline;
  try{await Promise.race([Promise.all([new Promise(r=>a.once('ready',r)),new Promise(r=>b.once('ready',r))]),new Promise((_,reject)=>{deadline=setTimeout(()=>reject(Error('relay timeout: '+ice.relayType)),20000);})] );const got=new Promise(r=>b.once('message',(_,m)=>r(m)));a.send('events',{type:'probe',value:'encrypted roundtrip'});const result=await got;if(result.value!=='encrypted roundtrip')throw Error('bad relay data');console.log(ice.relayType,JSON.stringify(a.path()),'authenticated data delivered');}finally{clearTimeout(deadline);a.close();b.close();}
 }
}finally{if(registered)await master.request('/v1/servers/'+master.id,'DELETE').catch(()=>{});master.close();rtc.cleanup();await rm(dir,{recursive:true,force:true});}
