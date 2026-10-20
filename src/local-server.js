import { validateNetworkContract, wireCompatibility } from './network-contract.js';
import http from 'node:http';
import { startLanDiscovery } from './lan.js';
// LAN and JOIN IP: `/info` over HTTP on the game port and LAN discovery on
// UDP 18452. Gameplay itself is the shared native UDP endpoint on the same
// port number, which this battle registers with.
export async function startLocalServer({ config, identity, world, query, udp, browser = () => ({}), isBanned = () => false }) {
  const contract=validateNetworkContract(world.networkContract),wire=wireCompatibility(contract);
  const content = config.content ?? 'bfe-content-1';
  const info = () => ({ id: identity.id, name: config.name, ...wire, content,
    rotation: config.rotation, players: udp?.players().length ?? 0,
    maxPlayers: config.maxPlayers ?? 64, relayPolicy: 'never', fingerprint: identity.fingerprint,
    query, port: udp?.port ?? config.gamePort ?? 18451, transport: 'udp', ...browser() });
  const server = http.createServer((req,res) => {
    res.setHeader('access-control-allow-origin','*');
    if (req.url !== '/info') { res.writeHead(404).end(); return; }
    res.setHeader('content-type','application/json'); res.end(JSON.stringify(info()));
  });
  const listen = address => new Promise((resolve,reject)=>{const failed=error=>reject(error);server.once('error',failed);server.listen(udp?.port ?? config.gamePort ?? 18451,address,()=>{server.off('error',failed);resolve();});});
  try { await listen(config.bind ?? '::'); }
  catch(error) { if(config.bind || !['EAFNOSUPPORT','EADDRNOTAVAIL'].includes(error.code))throw error; await listen('0.0.0.0'); }
  udp?.setBattle({ world, content, name: () => config.name, welcome: () => ({ ...info(), serverId: identity.id }), isBanned, maxPlayers: config.maxPlayers ?? 64 });
  let discovery;
  // A discovery reply is one datagram: the description and sponsor stay in /info.
  try { if(config.discovery !== false) discovery=await startLanDiscovery({port:config.discoveryPort ?? 18452,getInfo:()=>{const {description,sponsor,...rest}=info();return rest;}}); }
  catch(error) { console.error('LAN discovery unavailable:',error.message); }
  return { info, players:()=>udp?.players()??[], kick(id){udp?.kick(id);}, match(value){udp?.match(value);},
    async close(){ udp?.clearBattle(); await discovery?.close(); await new Promise(r=>server.close(r)); } };
}
