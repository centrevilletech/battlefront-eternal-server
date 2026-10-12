#!/usr/bin/env node
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { resolve, join } from "node:path";
import { homedir } from "node:os";
import { pathToFileURL } from "node:url";
import { createInterface } from "node:readline/promises";
import { stdin, stdout } from "node:process";
import { Worker } from "node:worker_threads";
import http from "node:http";
import { startLocalServer } from "./local-server.js";
import { loadIdentity, keyId } from "./identity.js";
import { loadCertificate } from "./certificate.js";
import { OperatorStore, operatorHandler, operatorRequest } from "./operator.js";
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
        "\nBattlefront Eternal server\n1  Server name\n2  Installed engine and imported game data\n3  Playlist\n4  Online notice and master\n5  Relay policy\n6  Global ban preference\n7  Show configuration\n8  Import owned gameplay bundle\n9  Live server administration\n0  Save and exit",
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
      else if (choice === "8") {
        if (!config.engine) { console.log('Set the installed engine directory first.'); continue; }
        const bundle = await io.question('Owned gameplay bundle directory: ');
        const destination = await io.question('New imported data directory: ');
        if (!bundle.trim() || !destination.trim()) { console.log('Both paths are required.'); continue; }
        try {
          const { importBundle } = await import(pathToFileURL(join(config.engine, 'src/runtime/gameplay-bundle-node.js')));
          const result = await importBundle({ bundle:resolve(bundle), destination:resolve(destination) });
          config.data = result.destination; console.log('Gameplay bundle verified and imported.');
        } catch(e) { console.error('Import failed:', e.message); }
      }
      if (choice === "9") await adminMenu(io);
      save();
    }
  } finally {
    io.close();
  }
}
async function adminMenu(io) {
  console.log('1  Status and players\n2  Kick player\n3  Ban identity\n4  Unban identity\n5  Player statistics\n6  Server message');
  const choice=await io.question('Admin choice: ');
  let request;
  if(choice==='1')request={action:'status'};
  else if(choice==='2')request={action:'kick',id:await io.question('Identity: ')};
  else if(choice==='3')request={action:'ban',id:await io.question('Identity: '),hours:Number(await io.question('Hours (0 permanent): ')),reason:await io.question('Reason: ')};
  else if(choice==='4')request={action:'unban',id:await io.question('Identity: ')};
  else if(choice==='5')request={action:'stats',id:(await io.question('Identity (blank for recent players): '))||undefined};
  else if(choice==='6')request={action:'message',text:await io.question('Message: ')};
  if(request)try{console.log(JSON.stringify(await operatorRequest(directory,request),null,2));}catch(e){console.error('Administration:',e.message);}
}
async function serve() {
  let online = null, local = null, stopping = false, rotationTimer = null;
  const startedAt = Date.now();
  let world = null,
    status = "setup",
    tick = 0,
    error = null;
  const operator = new OperatorStore(directory);
  const metricsEnabled=process.env.BFE_WORLD_METRICS==='1';let metricsBusy=false;
  const admin = operatorHandler(directory, async command => {
    if(command.action==='status')return {status,tick,pid:process.pid,startedAt,bans:operator.bans(),players:[...new Set([...(local?.players()??[]),...(online?.players()??[])])]};
    if(command.action==='stop'){setTimeout(()=>shutdown(),50);return {ok:true};}
    if(command.action==='stats')return {players:operator.stats(command.id)};
    if(command.action==='metrics'||command.action==='reset-metrics'){
      if(!metricsEnabled||!world||status!=='running')throw Error('World metrics are not enabled');
      if(metricsBusy)throw Error('World metrics request already pending');
      metricsBusy=true;const target=world,type=command.action==='metrics'?'metrics':'metricsReset';
      return new Promise((resolve,reject)=>{
        const cleanup=()=>{clearTimeout(timeout);target.off('message',receive);target.off('exit',exited);metricsBusy=false;};
        const receive=m=>{if(m.type===type){cleanup();resolve(m);}};
        const exited=()=>{cleanup();reject(Error('World changed during metrics request'));};
        const timeout=setTimeout(()=>{cleanup();reject(Error('World metrics timed out'));},8000);
        target.on('message',receive);target.once('exit',exited);target.postMessage({type:command.action==='metrics'?'metrics':'resetMetrics'});
      });
    }
    if(command.action==='unban'){operator.unban(command.id);return {ok:true};}
    if(command.action==='ban')operator.ban(command.id,command.hours,command.reason);
    else if(command.action==='kick'){if(!/^[a-f0-9]{64}$/.test(command.id??''))throw Error('Invalid identity');operator.audit('kick',command.id,command.reason);}
    else if(command.action==='message'){if(typeof command.text!=='string'||!command.text.trim()||command.text.length>240)throw Error('Invalid message');world?.postMessage({type:'announcement',text:command.text});return {ok:true};}
    else throw Error('Unknown operator command');
    local?.kick(command.id);online?.kick(command.id);return {ok:true};
  });
  // The service stays in setup mode until its engine and owned content are present.
  const server = http.createServer(async (req, res) => {
    if(await admin(req,res))return;
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
  let rotationIndex = 0, rotating = false;
  async function launch() {
    error = null; tick = 0;
    const mission = config.rotation[rotationIndex];
    const [map, side] = mission.replace(/^swbf1\//, "").split(":");
    if (mission.startsWith("swbf2/"))
      throw Error("BF2 simulation is not implemented");
    const query = `?map=${map}&side=${side}&ai=classic&seed=1`;
    world = new Worker(new URL("./world-worker.js", import.meta.url), {
      workerData: {
        engine: config.engine,
        data: config.data,
        query,
        maxPlayers: config.maxPlayers ?? 64,
        armySize:32,
        metrics:metricsEnabled,
        publicationMaxClients:(config.maxPlayers??64)*3,
        publicationWorker:process.env.BFE_PUBLICATION_WORKER==='1',
        ...(metricsEnabled&&process.env.BFE_WORLD_PROFILE?{profile:resolve(process.env.BFE_WORLD_PROFILE)}:{}),
      },
    });
    status = "loading";
    world.on("message", async (m) => {
      if(m.type==='inputAck'&&m.inputAckBatchId){queueMicrotask(()=>world.postMessage({type:'inputAckBatchProcessed',inputAckBatchId:m.inputAckBatchId}));}
      else if(m.type==='publicationBatchDone'){world.postMessage({...m,type:'publicationBatchProcessed'});}
      else if(m.type==='publicationFatal'){status='fault';error=m.message;await online?.close();await local?.close();await world.terminate();}
      else if (m.type === "ready") {
        status = "running";
        config.content = m.content;
        const activeConfig = { ...config, rotation:[...config.rotation.slice(rotationIndex), ...config.rotation.slice(0,rotationIndex)] };
        world.postMessage({ type: "start" });
        const keys = loadIdentity(join(directory, 'identity.json'));
        try { local = await startLocalServer({ config:activeConfig, directory, world, query, isBanned:id=>operator.banned(id), identity: { keys, id:keyId(keys.publicKey) }, certificate:loadCertificate(directory) }); }
        catch(e) { error=e.message; console.error('LAN listener:',e.message); }
        if (config.onlineNoticeAccepted) {
          try {
            online = new OnlineServer({ config:activeConfig, directory, world, query, isBanned:id=>operator.banned(id) });
            online.on('fault', message => console.error('Online:', message));
            await online.start(); console.log('Online server identity:', online.id);
          } catch (e) { error = e.message; console.error('Online registration:', e.message); }
        }
      } else if (m.type === "finished" && !rotating) {
        rotating = true;
        rotationIndex = (rotationIndex + 1) % config.rotation.length;
        const [nextMap,nextSide] = config.rotation[rotationIndex].replace(/^swbf1\//,'').split(':');
        const match = { query:`?map=${nextMap}&side=${nextSide}&ai=classic&seed=1`, delayMs:8000 };
        online?.match(match); local?.match(match);
        rotationTimer = setTimeout(async () => {
          try { await online?.close(); await local?.close(); await world.terminate(); online=null; local=null; await launch(); }
          catch(e) { status='fault'; error=e.message; console.error(e); }
          finally { rotating=false; }
        }, 500);
      } else if (m.type === "scores") operator.scores(m.match,m.players);
      else if (m.type === "tick") tick = m.tick;
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
  if (config.engine && config.data) await launch();
  console.log("bfeserver service started; control menu: bfeserver");
  async function shutdown() {
    if(stopping)return; stopping=true;clearTimeout(rotationTimer);
    await online?.close(); await local?.close(); await world?.terminate();
    server.close(() => {operator.close();process.exit(0);});
  }
  for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal,shutdown);
}
if (process.argv.includes("--admin")) {const offset=process.argv.indexOf("--admin");console.log(JSON.stringify(await operatorRequest(directory,JSON.parse(process.argv[offset+1]??'{"action":"status"}')),null,2));}
else if (process.argv.includes("--serve")) await serve();
else if (process.argv.includes("--version"))
  console.log("bfeserver 0.1.0 (pre-release)");
else await menu();
