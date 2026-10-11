import { verify, createPublicKey } from 'node:crypto';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
export class BanCache {
  constructor(master, file) { this.master=master;this.file=file;this.bans=[];this.expires=0;this.key=existsSync(file)?readFileSync(file,'utf8').trim():null; }
  async refresh() {
    const document=await this.master.request('/v1/bans');
    if(!this.key){const info=await this.master.request('/v1/info');this.key=info.publicKey;}
    if(document.publicKey!==this.key)throw Error('Master ban signing key changed');
    const key=createPublicKey({key:Buffer.from(this.key,'base64'),format:'der',type:'spki'});
    if(!verify(null,Buffer.from(JSON.stringify(document.payload)),key,Buffer.from(document.signature,'base64')))throw Error('Invalid ban list signature');
    const p=document.payload,now=Date.now();
    if(!Array.isArray(p.bans)||p.bans.length>10000||p.issued>now+60000||p.expires<now||p.expires>now+600000)throw Error('Invalid ban list lifetime');
    if(!existsSync(this.file))writeFileSync(this.file,this.key+'\n',{mode:0o600,flag:'wx'});
    this.bans=p.bans;this.expires=p.expires;
  }
  contains(id){return this.expires>Date.now()&&this.bans.some(b=>b.player===id&&b.expires>Date.now());}
}
