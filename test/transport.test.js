import {test} from 'node:test';import assert from 'node:assert/strict';import rtc from 'node-datachannel';
test('encrypted offline connection: unreliable state and reliable events', {timeout:15000}, async()=>{
 const a=new rtc.PeerConnection('a',{iceServers:[],mtu:1200}),b=new rtc.PeerConnection('b',{iceServers:[],mtu:1200});
 try{await new Promise((resolve,reject)=>{
  let received=0;
  a.onLocalDescription((s,t)=>b.setRemoteDescription(s,t));b.onLocalDescription((s,t)=>a.setRemoteDescription(s,t));
  a.onLocalCandidate((c,m)=>b.addRemoteCandidate(c,m));b.onLocalCandidate((c,m)=>a.addRemoteCandidate(c,m));
  b.onDataChannel(ch=>{ch.onMessage(msg=>{assert.ok(['state','events','bulk'].includes(String(msg)));if(++received===3)resolve();});ch.onError(reject);});
  for(const label of ['state','events','bulk']){const ch=a.createDataChannel(label,label==='state'?{unordered:true,maxRetransmits:0}:{});ch.onOpen(()=>ch.sendMessage(label));ch.onError(reject);}
 });assert.equal(a.state(),'connected');assert.ok(a.getSelectedCandidatePair());
 }finally{a.close();b.close();rtc.cleanup();}
});
