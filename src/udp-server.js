import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { networkInterfaces } from "node:os";
import { validateNetworkContract, wireCompatibility } from "./network-contract.js";
import { GameSession } from "./game-session.js";
import { UdpPeer } from "./udp-peer.js";

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
export async function startUdpServer({ engine, keys, port = 18451, host = null, maxSessions = 160, maxPerAddress = 8, transport }) {
  const { UdpEndpoint, identityKeys, CLOSE_CODE } = transport ?? await loadTransport(engine);
  let battle = null;
  const peers = new Map(); // identity -> UdpPeer
  const endpoint = new UdpEndpoint({ identity: identityKeys(keys), maxSessions, maxSessionsPerPrefix: maxPerAddress, server: {
    // The hello must fit the handshake's single RESPONSE datagram.
    hello: () => battle ? { ...battle.wire, content: battle.content, name: String(battle.name?.() ?? "").slice(0, 100) } : { reason: "loading" },
    admit: ({ client }) => {
      if (!battle) return { code: CLOSE_CODE.REJECTED, reason: "The next battle is loading. Try again in a moment." };
      const hello = client.hello ?? {};
      if (Object.entries(battle.wire).some(([key, value]) => hello[key] !== value))
        return { code: CLOSE_CODE.VERSION, reason: `Update required: protocol ${battle.wire.protocol} / ${battle.wire.simulation} / ${battle.wire.profile}` };
      if (battle.isBanned(client.id)) return { code: CLOSE_CODE.REJECTED, reason: "You are banned from this server." };
      if (!peers.has(client.id) && peers.size >= battle.maxPeers) return { code: CLOSE_CODE.REJECTED, reason: "The server is full." };
      return { accept: true };
    },
  } });
  const bound = await endpoint.listen({ port, host });
  endpoint.on("session", session => {
    if (!battle) { session.close(CLOSE_CODE.REJECTED, "The next battle is loading."); return; }
    // A reconnect replaces a stale session of the same identity at once.
    peers.get(session.id)?.close("replaced");
    const peer = new UdpPeer(session, { contract: battle.contract, content: battle.content, closeCodes: CLOSE_CODE });
    peers.set(session.id, peer);
    peer.once("closed", reason => {
      if (peers.get(session.id) === peer) peers.delete(session.id);
      if (reason && !["closed", "replaced", "server-shutdown", "kicked"].includes(reason)) console.warn("Player disconnected:", reason);
    });
    peer.on("fault", () => peer.close("invalid-message"));
    try { new GameSession(peer, battle.world, session.id, battle.welcome()); }
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
    async close() { battle = null; endpoint.close(); peers.clear(); },
  };
}
