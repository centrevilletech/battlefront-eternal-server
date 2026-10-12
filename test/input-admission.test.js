import test from 'node:test';
import assert from 'node:assert/strict';
import {InputAdmission} from '../src/input-admission.js';
test('immediate packet flood keeps 120 burst bound and sustained excess exhausts refill',()=>{
 let now=0;const a=new InputAdmission({clock:()=>now});for(let i=0;i<120;i++)assert(a.take());assert.equal(a.take(),false);
 now=1000/120;assert(a.take());assert.equal(a.take(),false);
 const b=new InputAdmission({clock:()=>now});let accepted=0;for(let i=0;i<500;i++){now+=1000/240;if(b.take())accepted++;}
 assert(accepted<=370,'only initial burst plus 120 packets/sec may pass');
});
test('clock rollback creates no refill credit',()=>{let now=100;const a=new InputAdmission({clock:()=>now});for(let i=0;i<120;i++)a.take();now=0;assert.equal(a.take(),false);now=100;assert.equal(a.take(),false);now=110;assert(a.take());});
