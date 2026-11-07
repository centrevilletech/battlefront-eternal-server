import test from 'node:test';
import assert from 'node:assert/strict';
import {hostDiagnostics} from '../src/host-diagnostics.js';
import {worldQueueDiagnostics} from '../src/game-session.js';
test('host samples distinguish linear memory from unavailable native capacity',()=>{
  const game={worldClock:{tick:19},simBridge:{x:{memory:{buffer:new ArrayBuffer(65536)}}}};
  const sample=hostDiagnostics(game,{clients:new Map([['p',{}]]),joining:new Map(),entityHost:{inspect:()=>({encodes:3})},linearHighWater:131072});
  assert.equal(sample.tick,19);assert.equal(sample.wasm.linearBytes,65536);assert.equal(sample.wasm.highWater,131072);
  assert.equal(sample.native.available,false);assert.equal(sample.queues.clients,1);assert.equal(sample.publication[0].encodes,3);
  assert.ok(sample.js.rss>0);assert.ok(sample.js.heapUsed>0);assert.equal(game.worldClock.tick,19);
  assert.deepEqual(worldQueueDiagnostics({}),{inbox:0,snapshots:0,acks:0,edges:0,inputs:0,sessions:0});
});
