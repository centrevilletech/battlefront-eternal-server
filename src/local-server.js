import http from 'node:http';
import { WebSocketServer, WebSocket } from 'ws';
import { Peer } from './peer.js';
import { GameSession } from './game-session.js';
import { startLanDiscovery } from './lan.js';
export async function startLocalServer({ config, identity, certificate, world, query, isBanned = () => false }) {
  const peers = new Set(), sockets = new Set();
  const info = () => ({ id: identity.id, name: config.name, protocol: 2, profile: 'entity-state-v1',
    simulation: 'bfe-sim-5', content: config.content ?? 'bfe-content-1',
    rotation: config.rotation, players: [...peers].filter(p=>p.authenticated).length,
    maxPlayers: config.maxPlayers ?? 64, relayPolicy: 'never', fingerprint: certificate.fingerprint,
    query, port: config.gamePort ?? 18451 });
  const server = http.createServer((req,res) => {
    res.setHeader('access-control-allow-origin','*');
    if (req.url !== '/info') { res.writeHead(404).end(); return; }
    res.setHeader('content-type','application/json'); res.end(JSON.stringify(info()));
  });
  const websocket = new WebSocketServer({ server, path: '/signal', maxPayload: 32768 });
  websocket.on('connection', socket => {
    if(sockets.size >= (config.maxPlayers ?? 64)*2) {socket.close();return;}
    sockets.add(socket);
    const peer=new Peer({name:'lan-client',keys:identity.keys,simulation:info().simulation,content:info().content,portable:process.env.BFE_PORTABLE_RTC==='1'||config.portableRTC===true,...certificate});
    peer.once('closed',reason=>{if(reason && reason!=='closed')console.warn('LAN peer disconnected:',reason);});
    peers.add(peer); let count=0; const timeout=setTimeout(()=>peer.close(),30000);
    peer.on('signal',s=>{if(socket.readyState===WebSocket.OPEN&&socket.bufferedAmount<65536)socket.send(JSON.stringify(s));});
    socket.on('message',raw=>{try{if(++count>180)throw Error('signaling limit');peer.signal(JSON.parse(raw));}catch{peer.close();}});
    peer.on('fault',()=>peer.close());
    peer.once('ready',r=>{clearTimeout(timeout);peer.identity=r.id;if(isBanned(r.id)){peer.close();return;}new GameSession(peer,world,r.id,{...info()});});
    peer.once('closed',()=>{clearTimeout(timeout);peers.delete(peer);socket.close();});
    socket.on('error',()=>peer.close());socket.once('close',()=>{sockets.delete(socket);if(!peer.authenticated)peer.close();});
  });
  const listen = address => new Promise((resolve,reject)=>{const failed=error=>reject(error);server.once('error',failed);server.listen(config.gamePort ?? 18451,address,()=>{server.off('error',failed);resolve();});});
  try { await listen(config.bind ?? '::'); }
  catch(error) { if(config.bind || !['EAFNOSUPPORT','EADDRNOTAVAIL'].includes(error.code))throw error; await listen('0.0.0.0'); }
  let discovery;
  try { if(config.discovery !== false) discovery=await startLanDiscovery({port:config.discoveryPort ?? 18452,getInfo:info}); }
  catch(error) { console.error('LAN discovery unavailable:',error.message); }
  return { info, players:()=>[...peers].filter(p=>p.authenticated).map(p=>p.identity), kick(id){for(const peer of peers)if(peer.identity===id)peer.close();}, match(value){for(const peer of peers)peer.send('events',{type:'match',...value});}, async close(){await discovery?.close();for(const p of peers)p.close();for(const s of sockets)s.terminate();websocket.close();await new Promise(r=>server.close(r));} };
}
