import { CONTRACT } from './contract-fixture.js';
import test from 'node:test';
import { mkdtemp, mkdir, copyFile, writeFile, symlink, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { loadIdentity } from '../src/identity.js';
test('portable server payload loads without native node-datachannel installed',async()=>{
  const root=await mkdtemp(join(tmpdir(),'bfe-portable-package-'));
  try {
    await mkdir(join(root,'src'));await mkdir(join(root,'node_modules'));
    await writeFile(join(root,'package.json'),'{"type":"module"}');
    for(const file of ['peer.js','portable-rtc.js','identity.js','transport-tuning.js','parent-diagnostics.js','network-contract.js'])await copyFile(new URL('../src/'+file,import.meta.url),join(root,'src',file));
    for(const name of ['werift','ws'])await symlink(resolve('node_modules',name),join(root,'node_modules',name),'junction');
    const {Peer}=await import(pathToFileURL(join(root,'src/peer.js')));
    const peer=new Peer({name:'packaged',keys:loadIdentity(join(root,'identity')),portable:true,contract:CONTRACT,content:'test'});
    peer.close();
  }finally{await rm(root,{recursive:true,force:true});}
});
