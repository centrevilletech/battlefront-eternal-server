import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import rtc from "node-datachannel";
import { loadIdentity } from "../src/identity.js";
import { MasterClient } from "../src/master-client.js";
import { Peer } from "../src/peer.js";
const dir = await mkdtemp(join(tmpdir(), "bfe-signal-test-"));
const options = {
  url: "https://online.battlefronteternal.com",
  noticeAccepted: true,
};
const server = new MasterClient({
    ...options,
    keys: loadIdentity(join(dir, "server")),
  }),
  client = new MasterClient({
    ...options,
    keys: loadIdentity(join(dir, "client")),
  });
let a,
  b,
  timer,
  registered = false;
try {
  await server.request("/v1/servers", "POST", {
    name: "Signaling deployment test",
    protocol: 1,
    simulation: "test",
    content: "test",
    rotation: ["swbf1/nab2:rep:conquest"],
    players: 0,
    maxPlayers: 2,
    relayPolicy: "auto",
    endpoints: [],
    fingerprint: "sha-256 " + Array(32).fill("00").join(":"),
  });
  registered = true;
  await Promise.all([server.connect(), client.connect()]);
  b = new Peer({
    name: "server",
    keys: server.keys,
    simulation: "test",
    content: "test",
    expectedKey: client.id,
  });
  a = new Peer({
    name: "client",
    keys: client.keys,
    initiator: true,
    simulation: "test",
    content: "test",
    expectedKey: server.id,
  });
  a.on("signal", (s) => client.signal(server.id, s));
  b.on("signal", (s) => server.signal(client.id, s));
  server.on("signal", (m) => b.signal(JSON.parse(m.payload)));
  client.on("signal", (m) => a.signal(JSON.parse(m.payload)));
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(Error("signaling test timed out")), 15000);
  });
  await Promise.race([
    Promise.all([
      new Promise((r) => a.once("ready", r)),
      new Promise((r) => b.once("ready", r)),
    ]),
    timeout,
  ]);
  const received = new Promise((r) => b.once("message", (_, m) => r(m)));
  a.send("events", { type: "probe", value: "master WSS verified" });
  const value = await Promise.race([received, timeout]);
  if (value.value !== "master WSS verified") throw Error("bad payload");
  console.log("Public WSS signaling and session identity proofs passed.");
} finally {
  clearTimeout(timer);
  a?.close();
  b?.close();
  server.close();
  client.close();
  if (registered)
    await server.request("/v1/servers/" + server.id, "DELETE").catch(() => {});
  rtc.cleanup();
  await rm(dir, { recursive: true, force: true });
}
