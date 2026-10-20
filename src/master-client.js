import { headers, keyId } from "./identity.js";
import http from "node:http";
import https from "node:https";
import { WebSocket } from "ws";
import { EventEmitter } from "node:events";
export class MasterClient extends EventEmitter {
  constructor({ url, keys, noticeAccepted = false }) {
    super();
    this.url = new URL(url);
    if (this.url.protocol !== "https:" && this.url.hostname !== "127.0.0.1")
      throw Error("master requires HTTPS");
    if (!noticeAccepted)
      throw Error("online notice must be accepted before connecting");
    this.keys = keys;
    this.id = keyId(keys.publicKey);
  }
  // family: 4 or 6 sends it over that address family (a direct server's
  // heartbeats show the master both of its addresses); an HTTP failure
  // carries its status.
  async request(path, method = "GET", value, { family = 0 } = {}) {
    const body = value === undefined ? "" : JSON.stringify(value);
    const head = { "content-type": "application/json", ...headers(this.keys, method, path, body) };
    if (family) return this.requestOver(family, path, method, body, head);
    const r = await fetch(new URL(path, this.url), { method, headers: head, body: body || undefined, signal: AbortSignal.timeout(10000) });
    const result = await r.json();
    if (!r.ok) throw Object.assign(Error(result.error ?? `master HTTP ${r.status}`), { status: r.status });
    return result;
  }
  requestOver(family, path, method, body, head) {
    const url = new URL(path, this.url);
    return new Promise((resolve, reject) => {
      const request = (url.protocol === "https:" ? https : http).request(url, { method, family, timeout: 10000, headers: body ? { ...head, "content-length": Buffer.byteLength(body) } : head }, (response) => {
        const chunks = [];
        let size = 0;
        response.on("data", (chunk) => { size += chunk.length; if (size > 1 << 20) request.destroy(Error("master response too large")); else chunks.push(chunk); });
        response.on("error", reject);
        response.on("end", () => {
          let result = {};
          try { result = JSON.parse(Buffer.concat(chunks).toString() || "{}"); } catch (error) { reject(error); return; }
          if (response.statusCode >= 400) reject(Object.assign(Error(result.error ?? `master HTTP ${response.statusCode}`), { status: response.statusCode }));
          else resolve(result);
        });
      });
      request.on("timeout", () => request.destroy(Error("master request timed out")));
      request.on("error", reject);
      request.end(body || undefined);
    });
  }
  async connect() {
    const { ticket } = await this.request("/v1/signal-ticket", "POST", {});
    const url = new URL("/v1/signal", this.url);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    url.searchParams.set("ticket", ticket);
    this.ws = new WebSocket(url, { maxPayload: 32768 });
    await new Promise((resolve, reject) => {
      this.ws.once("open", resolve);
      this.ws.once("error", reject);
    });
    this.ws.on("error", () => {});
    this.ws.on("message", (data) => {
      try {
        this.emit("signal", JSON.parse(data));
      } catch {
        this.ws.close();
      }
    });
    this.ws.on("close", () => this.emit("disconnected"));
  }
  signal(to, message) {
    if (
      this.ws?.readyState !== WebSocket.OPEN ||
      this.ws.bufferedAmount > 65536
    )
      return false;
    this.ws.send(
      JSON.stringify({
        to,
        type: message.type ?? "candidate",
        payload: JSON.stringify(message),
      }),
    );
    return true;
  }
  async credentials(serverId, peerId) {
    const r = await this.request("/v1/relay-credentials", "POST", { serverId, ...(peerId ? { peerId } : {}) });
    return r.urls.map((uri) => {
      const match = uri.match(/^(turns?):([^:]+):(\d+)\?transport=(udp|tcp)$/);
      if (!match) throw Error("unsupported TURN URI");
      return {
        hostname: match[2],
        port: Number(match[3]),
        username: r.username,
        password: r.credential,
        relayType:
          match[1] === "turns"
            ? "TurnTls"
            : match[4] === "tcp"
              ? "TurnTcp"
              : "TurnUdp",
      };
    });
  }
  close() {
    this.ws?.close();
  }
}
