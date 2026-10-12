import {test} from 'node:test';
import assert from 'node:assert/strict';
import {entityImportance} from '../src/entity-importance.js';
const view={position:[0,0,0],velocity:[0,0,0],forward:[0,0,1]};
const soldier=(position,velocity=[0,0,0])=>({kind:'soldier',health:100,position,velocity});
test('continuous distance and authoritative view distinguish otherwise equal actors',()=>{
 assert(entityImportance(soldier([0,0,30]),view)>entityImportance(soldier([0,0,300]),view));
 assert(entityImportance(soldier([0,0,100]),view)>entityImportance(soldier([0,0,-100]),view));
 const a=entityImportance(soldier([0,0,39.99]),view),b=entityImportance(soldier([0,0,40.01]),view);assert(Math.abs(a-b)<.01);
});
test('closing aircraft retain importance at flying engagement distances',()=>{
 const aircraft={kind:'vehicle',health:100,position:[0,0,500],velocity:[0,0,-125]};
 const approaching=entityImportance(aircraft,{...view,flying:true});
 assert(approaching>entityImportance({...aircraft,velocity:[0,0,125]},{...view,flying:true}));
 assert(approaching>entityImportance(soldier([0,0,500]),view));
 assert.equal(entityImportance({...aircraft,health:0},view),1);
 assert(approaching<=48);
});
test('lifecycle and viewpoint-free spectators retain bounded priorities',()=>{
 assert.equal(entityImportance({kind:'tombstone'}),16);assert.equal(entityImportance({kind:'seat'}),16);
 assert.equal(entityImportance({kind:'vehicle'}),6);assert.equal(entityImportance({kind:'soldier'}),1);
});
