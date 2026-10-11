// Child process for the local RTC load harness; never contacts public services.
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {inflateSync} from 'node:zlib';
import {Peer} from '../src/peer.js';
import {loadIdentity} from '../src/identity.js';
const peers=new Map(),timers=new Set();let inputTimer,pingTimer,settings,randomSeed=41;
const random=()=>((randomSeed=(Math.imul(randomSeed,1664525)+1013904223)>>>0)/4294967296);
const later=(fn,ms)=>{const t=setTimeout(()=>{timers.delete(t);fn();},ms);timers.add(t);};
process.on('message',async m=>{
 try {
 if(m.type==='create'){
 settings=m;const {BinaryReader}=await import(pathToFileURL(join(m.root,'src/runtime/packet.js')));
 const peer=new Peer({name:'load-client-'+m.index,keys:loadIdentity(join(m.directory,'client-'+m.index)),initiator:true,simulation:'bfe-sim-2',content:m.content,portable:m.portable});
 const c={peer,index:m.index,reader:new BinaryReader(),parts:new Map(),packets:0,tick:0,ack:0,sequence:0,fireId:0,held:false,rtt:[],ackLag:[],packetGaps:[],lastPacket:0};peers.set(m.index,c);
 peer.on('signal',signal=>process.send({type:'signal',index:m.index,signal}));
 peer.on('ping',value=>c.rtt.push(value));
 peer.on('fault',error=>process.send({type:'error',message:String(error)}));
 const packet=(data,metadata)=>{const bytes=inflateSync(Buffer.from(data,'base64'));const p=c.reader.read(bytes.buffer.slice(bytes.byteOffset,bytes.byteOffset+bytes.byteLength));Object.assign(p,metadata);const now=performance.now();if(c.lastPacket)c.packetGaps.push(now-c.lastPacket);c.lastPacket=now;c.tick=p.tick;c.ack=p.ack;c.generation=p.generation??c.generation;c.packets++;if(c.sequence)c.ackLag.push(Math.max(0,c.sequence-c.ack));};
 peer.on('message',(_,v)=>{if(v.type==='welcome'){c.welcome=v;c.generation=v.generation;peer.send('events',{type:'call',id:1,method:'start'});process.send({type:'welcome',index:m.index});}else if(v.type==='reset')c.parts.clear();else if(v.type==='ticks')v.buffers.forEach(data=>packet(data,v.metadata));else if(v.type==='packet'){let p=c.parts.get(v.id);if(!p)c.parts.set(v.id,p=[]);p[v.index]=v.data;if(v.index===0)p.metadata=v.metadata;if(p.filter(Boolean).length===v.total){packet(p.join(''),p.metadata);c.parts.delete(v.id);}}});
 } else if(m.type==='signal')await peers.get(m.index).peer.signal(m.signal);
 else if(m.type==='begin'){
 for(const c of peers.values())c.firstTick=c.tick;
 inputTimer=setInterval(()=>{for(const c of peers.values()){
 if(!c.tick)continue;const command={epoch:1,sequence:++c.sequence,tick:c.tick+Math.ceil(2*(settings.latency+settings.jitter)/1000*60)+3,generation:c.generation,down:[c.index%2?'KeyS':'KeyW'],actions:[],look:[]};
 if(random()>=settings.loss)later(()=>c.peer.send('state',{type:'input',command}),settings.latency+random()*settings.jitter);
 const held=c.sequence%120<60;if(held!==c.held){c.held=held;const edge={type:'fire',epoch:1,id:++c.fireId,tick:command.tick,generation:c.generation,button:0,pressed:held,viewTick:c.tick};later(()=>c.peer.send('events',edge),settings.latency+settings.jitter);}
 }},1000/60);
 pingTimer=setInterval(()=>{for(const c of peers.values()){const time=Date.now();later(()=>c.peer.send('events',{type:'ping',time}),settings.latency+random()*settings.jitter);}},1000);
 }else if(m.type==='sample')process.send({type:'sample',clients:[...peers.values()].map(c=>({index:c.index,tick:c.tick,ack:c.ack,packets:c.packets,closed:c.peer.closed,ageMs:c.lastPacket?performance.now()-c.lastPacket:0})),rss:process.memoryUsage().rss});
 else if(m.type==='finish'){clearInterval(inputTimer);clearInterval(pingTimer);process.send({type:'result',clients:[...peers.values()].map(({index,tick,ack,packets,firstTick,rtt,ackLag,packetGaps})=>({index,tick,ack,packets,firstTick,rtt,ackLag,packetGaps})),rss:process.memoryUsage().rss});}
 else if(m.type==='close'){for(const t of timers)clearTimeout(t);for(const c of peers.values())c.peer.close();process.exit(0);}
 }catch(e){process.send({type:'error',message:e.stack});}
});
