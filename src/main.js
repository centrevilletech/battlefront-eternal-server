#!/usr/bin/env node
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { resolve, join } from "node:path";
import { homedir } from "node:os";
import { createInterface } from "node:readline/promises";
import { stdin, stdout } from "node:process";
import { Worker } from "node:worker_threads";
import http from "node:http";
import { startLocalServer } from "./local-server.js";
import { loadIdentity, keyId } from "./identity.js";
import { loadCertificate } from "./certificate.js";
import { OnlineServer } from "./online-server.js";
const directory = resolve(
  process.env.BFE_STATE_DIR ?? join(homedir(), ".local/share/bfeserver"),
);
mkdirSync(directory, { recursive: true, mode: 0o700 });
const file = join(directory, "config.json");
const defaults = {
  name: "Battlefront Eternal server",
  engine: process.env.BFE_ENGINE_DIR ?? "",
  data: process.env.BFE_DATA_DIR ?? "",
  rotation: ["swbf1/nab2:rep:conquest"],
  master: "https://online.battlefronteternal.com",
  onlineNoticeAccepted: false,
  relayPolicy: "auto",
  honorGlobalBans: true,
};
const config = existsSync(file)
  ? { ...defaults, ...JSON.parse(readFileSync(file, "utf8")) }
  : defaults;
const save = () =>
  writeFileSync(file, JSON.stringify(config, null, 2) + "\n", { mode: 0o600 });
async function menu() {
  const io = createInterface({ input: stdin, output: stdout });
  try {
    for (;;) {
      console.log(
        "\nBattlefront Eternal server\n1  Server name\n2  Installed engine and imported game data\n3  Playlist\n4  Online notice and master\n5  Relay policy\n6  Global ban preference\n7  Show configuration\n0  Save and exit",
      );
      const choice = await io.question("Choice: ");
      if (choice === "0") {
        save();
        return;
      }
      if (choice === "1")
        config.name = (await io.question("Name: ")).slice(0, 80);
      else if (choice === "2") {
        config.engine = resolve(
          await io.question("Engine checkout directory: "),
        );
        config.data = resolve(
          await io.question("Imported game data directory: "),
        );
      } else if (choice === "3") {
        const maps = (
          await io.question("Maps, comma separated (swbf1/nab2:rep:conquest): ")
        )
          .split(",")
          .map((x) => x.trim());
        if (
          maps.length &&
          maps.every((x) =>
            /^swbf[12]\/[a-z0-9_-]+:[a-z0-9_-]+:[a-z0-9_-]+$/.test(x),
          )
        )
          config.rotation = maps;
        else console.log("Invalid playlist.");
      } else if (choice === "4") {
        console.log(
          "Online play processes your IP for connectivity and abuse prevention. Independent hosts and relays process connection metadata. Location sharing remains off.",
        );
        config.onlineNoticeAccepted =
          (await io.question("Accept online processing? Type yes: ")) === "yes";
        if (config.onlineNoticeAccepted) {
          const u = new URL(await io.question("Master URL: "));
          if (u.protocol !== "https:") throw Error("HTTPS required");
          config.master = u.href;
        }
      } else if (choice === "5") {
        const p = await io.question("auto, never, or always: ");
        if (["auto", "never", "always"].includes(p)) config.relayPolicy = p;
      } else if (choice === "6")
        config.honorGlobalBans =
          (await io.question("Honor global bans? yes/no: ")) === "yes";
      else if (choice === "7") console.log(JSON.stringify(config, null, 2));
      save();
    }
  } finally {
    io.close();
  }
}
async function serve() {
  let online = null, local = null;
  let world = null,
    status = "setup",
    tick = 0,
    error = null;
  // The service stays in setup mode until its engine and owned content are present.
  const server = http.createServer((req, res) => {
    if (req.url !== "/healthz") {
      res.writeHead(404);
      res.end();
      return;
    }
    res.setHeader("content-type", "application/json");
    res.end(
      JSON.stringify({
        ok: status !== "fault",
        service: "bfeserver",
        status,
        tick,
        error,
        multiplayerReady: status === "running" && Boolean(local),
      }),
    );
  });
  server.listen(Number(process.env.PORT ?? 18450), "127.0.0.1");
  if (config.engine && config.data) {
    const [map, side] = config.rotation[0].replace(/^swbf1\//, "").split(":");
    if (config.rotation[0].startsWith("swbf2/"))
      throw Error("BF2 simulation is not implemented");
    const query = `?map=${map}&side=${side}&ai=classic&seed=1`;
    world = new Worker(new URL("./world-worker.js", import.meta.url), {
      workerData: {
        engine: config.engine,
        data: config.data,
        query,
        maxPlayers: config.maxPlayers ?? 32,
      },
    });
    status = "loading";
    world.on("message", async (m) => {
      if (m.type === "ready") {
        status = "running";
        config.content = m.content;
        world.postMessage({ type: "start" });
        const keys = loadIdentity(join(directory, 'identity.json'));
        try { local = await startLocalServer({ config, directory, world, query, identity: { keys, id:keyId(keys.publicKey) }, certificate:loadCertificate(directory) }); }
        catch(e) { error=e.message; console.error('LAN listener:',e.message); }
        if (config.onlineNoticeAccepted) {
          try {
            online = new OnlineServer({ config, directory, world, query });
            online.on('fault', message => console.error('Online:', message));
            await online.start(); console.log('Online server identity:', online.id);
          } catch (e) { error = e.message; console.error('Online registration:', e.message); }
        }
      } else if (m.type === "tick") tick = m.tick;
      else if (m.type === "error") {
        status = "fault";
        error = m.message;
        console.error(m.message);
      }
    });
    world.on("error", (e) => {
      status = "fault";
      error = e.message;
    });
  }
  console.log("bfeserver service started; control menu: bfeserver");
  for (const s of ["SIGINT", "SIGTERM"])
    process.on(s, async () => {
      await online?.close();
      await local?.close();
      world?.terminate();
      server.close(() => process.exit(0));
    });
}
if (process.argv.includes("--serve")) await serve();
else if (process.argv.includes("--version"))
  console.log("bfeserver 0.1.0 (pre-release)");
else await menu();
