import {readFile} from 'node:fs/promises';
const profile=JSON.parse(await readFile(process.argv[2], 'utf8'));
const nodes=new Map(profile.nodes.map(node=>[node.id,node])),parents=new Map();
for(const node of profile.nodes)for(const child of node.children??[])parents.set(child,node.id);
const own=new Map(),total=new Map();
for(let index=0;index<profile.samples.length;index++){
 let id=profile.samples[index];const duration=profile.timeDeltas[index];own.set(id,(own.get(id)??0)+duration);
 while(id){total.set(id,(total.get(id)??0)+duration);id=parents.get(id);}
}
const rows=counter=>[...counter].sort((a,b)=>b[1]-a[1]).slice(0,30).map(([id,duration])=>{const frame=nodes.get(id).callFrame;return{ms:Math.round(duration/100)/10,fn:frame.functionName,file:frame.url,line:frame.lineNumber+1};});
console.log(JSON.stringify({self:rows(own),inclusive:rows(total)},null,2));
