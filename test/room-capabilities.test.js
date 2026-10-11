import {test} from 'node:test';
import assert from 'node:assert/strict';
import {updateRoomCapabilities} from '../src/room-capabilities.js';
test('shared projectile optimization falls back before legacy clients receive snapshots',()=>{
 const changes=[],world={linearProjectiles:false,setLinearProjectiles(value){this.linearProjectiles=value;changes.push(value);}};
 const modern={started:true,linearProjectiles:true},legacy={started:false,linearProjectiles:false},clients=new Map([['modern',modern],['legacy',legacy]]);
 updateRoomCapabilities(world,clients);assert.equal(world.linearProjectiles,true);assert(modern.keyframe);
 modern.keyframe=false;legacy.started=true;updateRoomCapabilities(world,clients);assert.equal(world.linearProjectiles,false);assert(modern.keyframe&&legacy.keyframe);
 clients.delete('legacy');updateRoomCapabilities(world,clients);assert.equal(world.linearProjectiles,true);
 modern.started=false;updateRoomCapabilities(world,clients);assert.equal(world.linearProjectiles,false);assert.deepEqual(changes,[true,false,true,false]);
});
