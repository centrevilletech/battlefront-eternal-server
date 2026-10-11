import { headers, keyId } from "./identity.js";
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
  async request(path, method = "GET", value) {
    const body = value === undefined ? "" : JSON.stringify(value);
    const r = await fetch(new URL(path, this.url), {
      method,
      headers: {
        "content-type": "application/json",
        ...headers(this.keys, method, path, body),
      },
      body: body || undefined,
      signal: AbortSignal.timeout(10000),
    });
    const result = await r.json();
    if (!r.ok) throw Error(result.error ?? `master HTTP ${r.status}`);
    return result;
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
  async credentials(serverId) {
    const r = await this.request("/v1/relay-credentials", "POST", { serverId });
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
