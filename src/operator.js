import { DatabaseSync } from 'node:sqlite';
import { randomBytes, timingSafeEqual } from 'node:crypto';
import { existsSync, readFileSync, writeFileSync, chmodSync } from 'node:fs';
import { join } from 'node:path';
const identity = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);
export class OperatorStore {
  constructor(directory) {
    this.db=new DatabaseSync(join(directory,'operator.sqlite'));
    chmodSync(join(directory,'operator.sqlite'),0o600);
    this.db.exec(`PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS bans(id TEXT PRIMARY KEY, expires INTEGER NOT NULL, reason TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS scores(match TEXT NOT NULL,id TEXT NOT NULL,name TEXT NOT NULL,kills INTEGER NOT NULL,deaths INTEGER NOT NULL,points INTEGER NOT NULL,updated INTEGER NOT NULL,PRIMARY KEY(match,id));
      CREATE TABLE IF NOT EXISTS audit(time INTEGER NOT NULL,action TEXT NOT NULL,id TEXT,reason TEXT);`);
  }
  bans() { return this.db.prepare('SELECT id,expires,reason FROM bans WHERE expires=0 OR expires>? ORDER BY id LIMIT 1000').all(Date.now()); }
  banned(id) {const row=this.db.prepare('SELECT expires FROM bans WHERE id=?').get(id);return Boolean(row && (!row.expires || row.expires>Date.now()));}
  ban(id,hours,reason='') {if(!identity(id)||!Number.isFinite(hours)||hours<0||hours>8760)throw Error('Invalid identity or ban duration');this.db.prepare('INSERT OR REPLACE INTO bans VALUES(?,?,?)').run(id,hours?Date.now()+hours*3600000:0,String(reason).slice(0,240));this.audit('ban',id,reason);}
  unban(id) {if(!identity(id))throw Error('Invalid identity');this.db.prepare('DELETE FROM bans WHERE id=?').run(id);this.audit('unban',id,'');}
  audit(action,id,reason) {this.db.prepare('INSERT INTO audit VALUES(?,?,?,?)').run(Date.now(),action,id,String(reason??'').slice(0,240));}
  scores(match,players) {
    if(typeof match!=='string'||match.length>64||!Array.isArray(players)||players.length>128)throw Error('Invalid scores');
    const insert=this.db.prepare('INSERT OR REPLACE INTO scores VALUES(?,?,?,?,?,?,?)');
    this.db.exec('BEGIN');try{for(const p of players){if(!identity(p.id))continue;const values=[p.kills,p.deaths,p.points];if(!values.every(n=>Number.isSafeInteger(n)&&Math.abs(n)<1000000))continue;insert.run(match,p.id,String(p.name??'Player').slice(0,32),...values,Date.now());}this.db.exec('COMMIT');}catch(e){this.db.exec('ROLLBACK');throw e;}
  }
  stats(id) {if(id!==undefined&&!identity(id))throw Error('Invalid identity');return this.db.prepare(`SELECT id,MAX(name) name,COUNT(*) matches,SUM(kills) kills,SUM(deaths) deaths,SUM(points) points,MAX(updated) lastSeen FROM scores ${id?'WHERE id=?':''} GROUP BY id ORDER BY lastSeen DESC LIMIT 100`).all(...(id?[id]:[]));}
  close(){this.db.close();}
}
export function operatorHandler(directory, invoke) {
  const path=join(directory,'operator-token');
  if(!existsSync(path))writeFileSync(path,randomBytes(32).toString('hex'),{mode:0o600,flag:'wx'});
  chmodSync(path,0o600);
  const token=Buffer.from('Bearer '+readFileSync(path,'utf8').trim());
  return async(req,res)=>{
    if(req.url!=='/admin')return false;
    const auth=Buffer.from(req.headers.authorization??'');
    const reply=(status,value)=>{res.writeHead(status,{'content-type':'application/json','cache-control':'no-store'});res.end(JSON.stringify(value));};
    if(auth.length!==token.length||!timingSafeEqual(auth,token)){reply(401,{error:'Unauthorized'});return true;}
    if(req.method!=='POST'||req.headers['content-type']!=='application/json'){reply(400,{error:'JSON POST required'});return true;}
    try {let raw='';for await(const chunk of req){raw+=chunk;if(Buffer.byteLength(raw)>4096)throw Error('Request too large');}const value=JSON.parse(raw);if(!value||typeof value!=='object'||Array.isArray(value))throw Error('Invalid request');reply(200,await invoke(value));}
    catch(error){reply(400,{error:error.message});}
    return true;
  };
}
export async function operatorRequest(directory, value, port=Number(process.env.PORT??18450)) {
  const response=await fetch(`http://127.0.0.1:${port}/admin`,{method:'POST',headers:{'content-type':'application/json',authorization:'Bearer '+readFileSync(join(directory,'operator-token'),'utf8').trim()},body:JSON.stringify(value),signal:AbortSignal.timeout(10000)});
  const result=await response.json();if(!response.ok)throw Error(result.error??'Operator request failed');return result;
}
