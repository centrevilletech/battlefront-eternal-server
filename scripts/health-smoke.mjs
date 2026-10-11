import assert from 'node:assert/strict';
const url = process.argv[2] ?? 'http://127.0.0.1:18450/healthz';
let previous;
for (let sample = 0; sample < 3; sample++) {
  const response = await fetch(url, { signal: AbortSignal.timeout(5000) });
  assert(response.ok, 'health HTTP response');
  const value = await response.json();
  assert.equal(value.status, 'running', value.error ?? 'world not running');
  assert.equal(value.multiplayerReady, true, 'multiplayer listener ready');
  assert(Number.isSafeInteger(value.tick), 'integer simulation tick');
  if (previous !== undefined) assert(value.tick > previous, 'simulation tick must advance');
  previous = value.tick;
  if (sample < 2) await new Promise(resolve => setTimeout(resolve, 1000));
}
console.log(JSON.stringify({ ok: true, tick: previous, advancing: true }));
