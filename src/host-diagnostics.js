// Read on explicit operator/qualification requests, never in the tick loop.
// A WASM address space has a measurable high-water mark, but it is not the
// unavailable native world's reserved/used capacity or a Rust Drop count.
export function hostDiagnostics(game,{clients,joining,entityHost,linearHighWater=0}={}) {
  const linearBytes=game.simBridge?.x?.memory?.buffer.byteLength??null;
  return {tick:game.worldClock.tick,js:process.memoryUsage(),
    wasm:{linearBytes,highWater:linearBytes===null?null:Math.max(linearHighWater,linearBytes)},
    native:{available:false,reason:'Native world diagnostics are not exposed by this host'},
    queues:{clients:clients?.size??0,joining:joining?.size??0},
    publication:entityHost?[...clients.keys()].map(id=>({client:id,...entityHost.inspect(id)})):[],
  };
}
