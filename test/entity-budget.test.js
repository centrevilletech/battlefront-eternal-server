import {test} from 'node:test';
import assert from 'node:assert/strict';
import {EntityBudget} from '../src/entity-budget.js';
test('bounded offered payload preserves20Hz own priority without unbounded recovery bursts',()=>{
 const budget=new EntityBudget({now:0});let own=0,remote=0;
 for(let now=0;now<10000;now+=50){assert(budget.take(1100,now));own++;if(budget.take(1100,now))remote++;}
 assert.equal(own,200);assert(remote>30);assert(budget.offeredBytes<=26000*10+2200);
 assert.equal(budget.available(60000),2200);assert(budget.take(1100,60000));assert(budget.take(1100,60000));assert(!budget.take(1,60000));
 assert.equal(budget.available(59000),0,'clock reversal cannot refill twice');assert.equal(budget.available(60000),0);
 assert.throws(()=>budget.take(1101,60000));
});
