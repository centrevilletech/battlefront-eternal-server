// bfeserver's network thread (started by udp-server.js startNetworkThread):
// the game port's sockets, encryption, acknowledgements and relays, and
// every player's UdpPeer and GameSession. Inputs and snapshots cross to the
// world worker over a port of their own, so the per-packet path never waits
// on the main thread; the main thread answers ban checks and sends control
// (battle start and end, kicks, rendezvous) here.
import { parentPort, workerData } from "node:worker_threads";
import { EventEmitter } from "node:events";
import { Session } from "node:inspector";
import { writeFile } from "node:fs/promises";
import { startUdpServer } from "./udp-server.js";

const waiters = new Map();
let nextId = 0;
const ask = (op, value) => new Promise((resolve, reject) => { const id = ++nextId; waiters.set(id, { resolve, reject }); parentPort.postMessage({ op, id, value }); });
const reply = (id, value, error) => parentPort.postMessage(error ? { op: "reply", id, error: String(error?.message ?? error) } : { op: "reply", id, value });

// `profile`: a CPU profile of this thread, written when metrics are read
// (the probe's measurement window, restarted by resetMetrics).
const { profile, ...options } = workerData;
let profiler = null;
const profileCall = method => new Promise((resolve, reject) => profiler.post(method, (error, result) => error ? reject(error) : resolve(result)));
if (profile) { profiler = new Session(); profiler.connect(); await profileCall("Profiler.enable"); await profileCall("Profiler.start"); }
let udp;
try { udp = await startUdpServer({ ...options, onPlayers: ids => parentPort.postMessage({ op: "players", value: ids }) }); }
catch (error) { parentPort.postMessage({ op: "failed", value: String(error?.message ?? error) }); }

// The world's client port, with the EventEmitter face GameSession expects.
function worldPort(port, networkContract) {
  const world = new EventEmitter();
  world.networkContract = networkContract;
  world.postMessage = (message, transfer) => port.postMessage(message, transfer);
  port.on("message", message => world.emit("message", message));
  world.close = () => { try { port.close(); } catch {} };
  return world;
}

if (udp) {
  let world = null, relayId = 0;
  const relays = new Map();
  const report = () => parentPort.postMessage({ op: "stats", value: udp.stats() });
  const timer = setInterval(report, 1000);
  report();
  parentPort.postMessage({ op: "ready", value: { port: udp.port, families: udp.families } });
  parentPort.on("message", async m => {
    try {
      if (m.op === "reply") { const waiter = waiters.get(m.id); if (waiter) { waiters.delete(m.id); m.error ? waiter.reject(Error(m.error)) : waiter.resolve(m.value); } }
      else if (m.op === "setBattle") {
        world?.close();
        world = worldPort(m.value.port, m.value.networkContract);
        const { name, welcome, content, maxPlayers } = m.value;
        udp.setBattle({ world, content, name: () => name, welcome: () => ({ ...welcome, players: udp.players().length }), isBanned: id => ask("banned", id), maxPlayers });
      }
      else if (m.op === "clearBattle") { udp.clearBattle(); world?.close(); world = null; }
      else if (m.op === "kick") udp.kick(m.value.id, m.value.message);
      else if (m.op === "match") udp.match(m.value);
      else if (m.op === "punch") udp.endpoint.punch(m.value);
      else if (m.op === "reflexive") reply(m.id, await udp.endpoint.reflexive(m.value));
      else if (m.op === "allocateRelay") { const relay = await udp.endpoint.allocateRelay(m.value), id = ++relayId; relays.set(id, relay); reply(m.id, { relayId: id, relayed: relay.relayed }); }
      else if (m.op === "relayPermit") { await relays.get(m.value.relayId)?.permit(m.value.addresses); reply(m.id, true); }
      else if (m.op === "relayClose") { relays.get(m.value.relayId)?.close(); relays.delete(m.value.relayId); }
      else if (m.op === "metrics") {
        if (profiler) { const { profile: result } = await profileCall("Profiler.stop"); await writeFile(profile, JSON.stringify(result)); profiler.disconnect(); profiler = null; }
        reply(m.id, await udp.metrics());
      }
      else if (m.op === "resetMetrics") { if (profiler) { await profileCall("Profiler.stop"); await profileCall("Profiler.start"); } reply(m.id, await udp.resetMetrics()); }
      else if (m.op === "diagnostics") reply(m.id, await udp.diagnostics());
      else if (m.op === "close") {
        clearInterval(timer);
        for (const relay of relays.values()) relay.close();
        relays.clear();
        await udp.close(); world?.close(); world = null;
        reply(m.id, true);
      }
    } catch (error) { if (m.id !== undefined && m.op !== "reply") reply(m.id, null, error); }
  });
}
