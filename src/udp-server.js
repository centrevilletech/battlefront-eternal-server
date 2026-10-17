import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { networkInterfaces } from "node:os";
import { MessageChannel, Worker } from "node:worker_threads";
import { validateNetworkContract, wireCompatibility } from "./network-contract.js";
import { GameSession } from "./game-session.js";
import { UdpPeer } from "./udp-peer.js";
import { netMetrics, resetNetMetrics } from "./net-metrics.js";

// The engine owns the wire protocol (src/net, BFE-UDP/1). One endpoint on
// the game port serves LAN, JOIN IP, online and desktop HOST players alike;
// it outlives battles, so a map rotation keeps the same socket and port.
export async function loadTransport(engine) {
  const module = name => import(pathToFileURL(resolve(engine, "src/net", name)));
  const [endpoint, crypto, connection] = await Promise.all(["endpoint.js", "crypto.js", "connection.js"].map(module));
  return { UdpEndpoint: endpoint.UdpEndpoint, identityKeys: crypto.identityKeys, CLOSE_CODE: connection.CLOSE_CODE };
}

// Addresses on this host other players might reach directly: every
// non-internal interface address, as host candidates for signaling.
export function hostCandidates(port, families = ["IPv4", "IPv6"]) {
  const out = [];
  for (const [name, entries] of Object.entries(networkInterfaces())) for (const entry of entries ?? []) {
    if (entry.internal || !families.includes(entry.family)) continue;
    // Link-local IPv6 needs a scope and only works on that link.
    if (entry.family === "IPv6" && /^fe80:/i.test(entry.address)) continue;
    out.push({ address: entry.address, port, kind: "host", family: entry.family, interface: name });
  }
  return out.slice(0, 6);
}

// maxPerAddress caps sessions from one network (an IPv4 address or an IPv6
// /64; loopback exempt): identities cost nothing to make, so without it one
// host could hold every seat. A household or LAN party behind one NAT fits.
// isBanned may answer asynchronously (the network thread asks the main
// thread); onPlayers hears every roster change.
export async function startUdpServer({ engine, keys, port = 18451, host = null, maxSessions = 160, maxPerAddress = 8, transport, onPlayers = () => {} }) {
  const { UdpEndpoint, identityKeys, CLOSE_CODE } = transport ?? await loadTransport(engine);
  let battle = null;
  const peers = new Map(), sessions = new Map(); // identity -> UdpPeer, GameSession
  const roster = () => onPlayers([...peers.keys()]);
  const endpoint = new UdpEndpoint({ identity: identityKeys(keys), maxSessions, maxSessionsPerPrefix: maxPerAddress, server: {
    // The hello must fit the handshake's single RESPONSE datagram.
    hello: () => battle ? { ...battle.wire, content: battle.content, name: String(battle.name?.() ?? "").slice(0, 100) } : { reason: "loading" },
    admit: ({ client }) => {
      if (!battle) return { code: CLOSE_CODE.REJECTED, reason: "The next battle is loading. Try again in a moment." };
      const hello = client.hello ?? {};
      if (Object.entries(battle.wire).some(([key, value]) => hello[key] !== value))
        return { code: CLOSE_CODE.VERSION, reason: `Update required: protocol ${battle.wire.protocol} / ${battle.wire.simulation} / ${battle.wire.profile}` };
      const decide = banned => banned ? { code: CLOSE_CODE.REJECTED, reason: "You are banned from this server." }
        : !peers.has(client.id) && peers.size >= battle.maxPeers ? { code: CLOSE_CODE.REJECTED, reason: "The server is full." }
        : { accept: true };
      const banned = battle.isBanned(client.id);
      return typeof banned?.then === "function" ? banned.then(decide) : decide(banned);
    },
  } });
  const bound = await endpoint.listen({ port, host });
  endpoint.on("session", session => {
    if (!battle) { session.close(CLOSE_CODE.REJECTED, "The next battle is loading."); return; }
    // A reconnect replaces a stale session of the same identity at once.
    peers.get(session.id)?.close("replaced");
    const peer = new UdpPeer(session, { contract: battle.contract, content: battle.content, closeCodes: CLOSE_CODE });
    peers.set(session.id, peer); roster();
    peer.once("closed", reason => {
      if (peers.get(session.id) === peer) { peers.delete(session.id); sessions.delete(session.id); roster(); }
      if (reason && !["closed", "replaced", "server-shutdown", "kicked"].includes(reason)) console.warn("Player disconnected:", reason);
    });
    peer.on("fault", () => peer.close("invalid-message"));
    try { sessions.set(session.id, new GameSession(peer, battle.world, session.id, battle.welcome())); }
    catch (error) { peer.close("update-required", error.message); }
  });
  return {
    endpoint, port: bound.port, families: bound.families,
    // One battle at a time: its world, wire contract, content and welcome.
    setBattle({ world, content, name = () => "", welcome = () => ({}), isBanned = () => false, maxPlayers = 64 }) {
      const contract = validateNetworkContract(world.networkContract);
      battle = { world, contract, wire: wireCompatibility(contract), content, name, welcome, isBanned, maxPeers: maxPlayers + 16 };
    },
    clearBattle() { battle = null; for (const peer of peers.values()) peer.close("server-shutdown", "The battle has ended."); },
    players: () => [...peers.keys()],
    kick(id, message = "Kicked by the server operator.") { peers.get(id)?.close("kicked", message); },
    match(value) { for (const peer of peers.values()) peer.send("events", { type: "match", ...value }); },
    candidates: () => hostCandidates(bound.port, bound.families),
    stats: () => endpoint.stats(),
    // This thread's timing (event loop, snapshot hop) and each session's
    // transport counters, for the probe and the operator.
    metrics: async () => netMetrics(),
    resetMetrics: async () => resetNetMetrics(),
    diagnostics: async () => [...sessions.values()].map(session => session.transportDiagnostics()),
    async close() { battle = null; endpoint.close(); peers.clear(); sessions.clear(); },
  };
}

// The same game port on a thread of its own (network-worker.js): sockets,
// encryption, acknowledgements, relays and every player's session, with
// inputs and snapshots crossing straight to the world worker over a port of
// their own. Nothing in the per-packet path waits on this (main) thread,
// which keeps admission policy, rendezvous, /info and the operator. The
// returned object has the shape startUdpServer's does.
export async function startNetworkThread({ engine, keys, port = 18451, host = null, maxSessions = 160, maxPerAddress = 8, profile = null }) {
  const worker = new Worker(new URL("./network-worker.js", import.meta.url), { workerData: { engine, keys, port, host, maxSessions, maxPerAddress, ...(profile ? { profile } : {}) } });
  const waiters = new Map();
  let nextId = 0, players = [], stats = null, isBanned = () => false, closing = false;
  const call = (op, value, transfer) => new Promise((resolve, reject) => { const id = ++nextId; waiters.set(id, { resolve, reject }); worker.postMessage({ op, id, value }, transfer); });
  const post = (op, value, transfer) => worker.postMessage({ op, value }, transfer);
  const fail = error => { for (const waiter of waiters.values()) waiter.reject(error); waiters.clear(); };
  const ready = new Promise((resolve, reject) => {
    worker.on("message", async m => {
      if (m.op === "reply") { const waiter = waiters.get(m.id); if (!waiter) return; waiters.delete(m.id); m.error ? waiter.reject(Error(m.error)) : waiter.resolve(m.value); }
      else if (m.op === "banned") { let value = false; try { value = Boolean(await isBanned(m.value)); } catch {} worker.postMessage({ op: "reply", id: m.id, value }); }
      else if (m.op === "players") players = m.value;
      else if (m.op === "stats") stats = m.value;
      else if (m.op === "ready") resolve(m.value);
      else if (m.op === "failed") reject(Error(m.value));
    });
    worker.on("error", error => { reject(error); fail(error); console.error("Network thread failed:", error.message); });
    worker.on("exit", code => { reject(Error(`Network thread exited (${code})`)); fail(Error("Network thread exited")); if (!closing) console.error("Network thread exited:", code); });
  });
  let bound;
  try { bound = await ready; }
  catch (error) { await worker.terminate(); throw error; }
  const relayHandle = ({ relayId, relayed }) => ({ relayed, permit: addresses => call("relayPermit", { relayId, addresses }), close: () => post("relayClose", { relayId }) });
  return {
    thread: worker, port: bound.port, families: bound.families,
    // What rendezvous (online-server.js) uses of the endpoint.
    endpoint: {
      punch: candidates => post("punch", candidates),
      reflexive: server => call("reflexive", server),
      allocateRelay: async options => relayHandle(await call("allocateRelay", options)),
      stats: () => stats,
    },
    // The world sends client traffic to the network thread over a port of
    // its own from now on; the welcome is fixed at the battle's start.
    setBattle({ world, content, name = () => "", welcome = () => ({}), isBanned: banned = () => false, maxPlayers = 64 }) {
      validateNetworkContract(world.networkContract);
      isBanned = banned;
      const { port1, port2 } = new MessageChannel();
      world.postMessage({ type: "attachNetwork", port: port1 }, [port1]);
      post("setBattle", { port: port2, networkContract: world.networkContract, content, name: String(name() ?? ""), welcome: welcome(), maxPlayers }, [port2]);
    },
    clearBattle: () => post("clearBattle"),
    players: () => players,
    kick: (id, message) => post("kick", { id, message }),
    match: value => post("match", value),
    candidates: () => hostCandidates(bound.port, bound.families),
    stats: () => stats,
    metrics: () => call("metrics"),
    resetMetrics: () => call("resetMetrics"),
    diagnostics: () => call("diagnostics"),
    async close() { closing = true; try { await call("close"); } catch {} await worker.terminate(); },
  };
}
