import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import rtc from "node-datachannel";
import { loadIdentity } from "../src/identity.js";
import { MasterClient } from "../src/master-client.js";
import { Peer } from "../src/peer.js";
const dir = await mkdtemp(join(tmpdir(), "bfe-relay-test-")),
  keys = loadIdentity(join(dir, "id"));
const master = new MasterClient({
  url: "https://online.battlefronteternal.com",
  keys,
  noticeAccepted: true,
});
let registered = false;
const entry = {
  name: "Deployment connectivity test",
  protocol: 1,
  simulation: "connectivity-test",
  content: "none",
  rotation: ["swbf1/nab2:rep:conquest"],
  players: 0,
  maxPlayers: 2,
  relayPolicy: "always",
  endpoints: [],
  fingerprint: "sha-256 " + Array(32).fill("00").join(":"),
};
try {
  await master.request("/v1/servers", "POST", entry);
  registered = true;
  const cases = [
    ["TurnUdp", 0, 0],
    ["TurnTcp", 0, 0],
    ["TurnTls", 0, 0],
    ["TurnUdp", 6, 6],
    ["TurnUdp", 4, 6],
    ["TurnUdp", 6, 4],
  ];
  for (const [index, [type, aFamily, bFamily]] of cases.entries()) {
    // Each case uses a new client identity to avoid retaining allocations from a
    // just-closed prior case under the same four-allocation username quota.
    await master.request("/v1/servers/" + master.id, "PUT", entry);
    const credentials = new MasterClient({
      url: master.url.href,
      keys: loadIdentity(join(dir, `credential-${index}`)),
      noticeAccepted: true,
    });
    const ice = (await credentials.credentials(master.id)).find(
      (i) => i.relayType === type,
    );
    const a = new Peer({
      name: "probe-a",
      keys,
      initiator: true,
      iceServers: [ice],
      relay: true,
      simulation: "test",
      content: "test",
      ipFamily: aFamily,
      portable: aFamily !== 0 || type !== "TurnUdp",
    });
    const b = new Peer({
      name: "probe-b",
      keys: loadIdentity(join(dir, "peer")),
      iceServers: [ice],
      relay: true,
      simulation: "test",
      content: "test",
      ipFamily: bFamily,
      portable: bFamily !== 0 || type !== "TurnUdp",
    });
    a.on("signal", (s) => { if (/typ (host|srflx|prflx)(?: |\r|\n|$)/.test(s.sdp ?? s.candidate ?? "")) throw Error("relay-only signal exposed a direct candidate"); b.signal(s); });
    b.on("signal", (s) => { if (/typ (host|srflx|prflx)(?: |\r|\n|$)/.test(s.sdp ?? s.candidate ?? "")) throw Error("relay-only signal exposed a direct candidate"); a.signal(s); });
    let deadline;
    const timeout = new Promise((_, reject) => {
      deadline = setTimeout(
        () => reject(Error(`${type} ${aFamily}->${bFamily} timeout`)),
        20000,
      );
    });
    try {
      await Promise.race([
        Promise.all([
          new Promise((r) => a.once("ready", r)),
          new Promise((r) => b.once("ready", r)),
        ]),
        timeout,
      ]);
      const got = new Promise((r) => b.once("message", (_, m) => r(m)));
      const start = performance.now();
      a.send("events", { type: "probe", value: "encrypted roundtrip" });
      const result = await Promise.race([got, timeout]);
      if (result.value !== "encrypted roundtrip")
        throw Error("bad relay payload");
      const reply = new Promise((r) => a.once("message", (_, m) => r(m)));
      b.send("events", { type: "reply", value: result.value });
      if ((await Promise.race([reply, timeout])).value !== result.value)
        throw Error("bad relay reply");
      console.log(
        `${type} ${aFamily || "auto"}->${bFamily || "auto"}: ${JSON.stringify(a.path())}, authenticated roundtrip ${Math.round(performance.now() - start)} ms`,
      );
    } finally {
      clearTimeout(deadline);
      a.close();
      b.close();
    }
  }
} finally {
  if (registered)
    await master.request("/v1/servers/" + master.id, "DELETE").catch(() => {});
  master.close();
  rtc.cleanup();
  await rm(dir, { recursive: true, force: true });
}
