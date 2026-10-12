import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {execFileSync,spawnSync} from 'node:child_process';
const linux={skip:process.platform!=='linux'};
test('deployment runtime comparison detects added, removed and changed inputs but preserves documentation-only matches',linux,()=>{
 const directory=mkdtempSync(join(tmpdir(),'bfe-deploy-'));
 const script=resolve('.github/scripts/runtime-changed.sh');
 const git=(...args)=>execFileSync('git',['-c','user.name=Fixture','-c','user.email=fixture@example.invalid','-c','commit.gpgSign=false',...args],{cwd:directory,stdio:['ignore','pipe','pipe']}).toString().trim();
 const commit=()=>{git('add','-A');git('commit','-qm','fixture');return git('rev-parse','HEAD');};
 const changed=revision=>spawnSync('bash',[script,revision],{cwd:directory}).status;
 try{
  git('init','-q');mkdirSync(join(directory,'src'));writeFileSync(join(directory,'src/main.js'),'original');const initial=commit();
  writeFileSync(join(directory,'README.md'),'docs');commit();assert.equal(changed(initial),1);
  assert.equal(changed('0'.repeat(40)),0);assert.equal(changed('not-a-commit'),0);
  writeFileSync(join(directory,'src/main.js'),'changed');const edited=commit();assert.equal(changed(initial),0);
  writeFileSync(join(directory,'src/extra.js'),'added');const added=commit();assert.equal(changed(edited),0);
  rmSync(join(directory,'src/extra.js'));commit();assert.equal(changed(added),0);
  const previous=git('rev-parse','HEAD');writeFileSync(join(directory,'.dockerignore'),'new exclusion');commit();assert.equal(changed(previous),0);
 }finally{rmSync(directory,{recursive:true,force:true});}
});
test('reuse requires healthy resource limits and the exact private mount identities',linux,()=>{
 const gate=resolve('.github/scripts/reusable-runtime.jq');
 const container={State:{Health:{Status:'healthy'}},HostConfig:{NanoCpus:2000000000,Memory:1073741824},Config:{Labels:{'com.docker.compose.project':'battlefront-eternal-server-production','com.docker.compose.project.config_files':'/data/compose/39/docker-compose.yml'}},Mounts:[{Destination:'/data',Source:'/opt/battlefront-eternal/server/state',RW:true},{Destination:'/game-data',Source:'/opt/battlefront-eternal/server/game-data',RW:false},{Destination:'/engine',Source:'/opt/battlefront-eternal/server/releases/'+'a'.repeat(40),RW:false}]};
 const reusable=value=>spawnSync('jq',['-e','--arg','stackId','39','-f',gate],{input:JSON.stringify(value)}).status===0;
 assert(reusable(container));
 for(const mutate of [c=>c.State.Health.Status='starting',c=>c.HostConfig.NanoCpus=1500000000,c=>c.HostConfig.Memory=536870912,c=>c.Config.Labels['com.docker.compose.project']='unrelated',c=>c.Config.Labels['com.docker.compose.project.config_files']='/data/compose/40/docker-compose.yml',c=>c.Mounts[2].RW=true,c=>c.Mounts[2].Source='/tmp/unknown',c=>c.Mounts[0].Source='/tmp/empty-state']){
  const changed=structuredClone(container);mutate(changed);assert.equal(reusable(changed),false);
 }
});
