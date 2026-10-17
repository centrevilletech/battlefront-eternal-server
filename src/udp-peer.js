import { EventEmitter } from "node:events";
import { wireCompatibility } from "./network-contract.js";
import { countNet } from "./net-metrics.js";

// A game peer over the engine's native UDP transport (BFE-UDP/1), with the
// interface GameSession uses. The endpoint has already authenticated both
// identities and the hello before this exists, so a peer is born ready.
const STATE_MAGICS = new Set([0x31494e42, 0x31414e42]); // BNI1 input, BNA1 ack keepalive
// Close reasons as the transport's close codes (the engine's CLOSE_CODE).
const REASON_CODES = Object.freeze({ closed: "NORMAL", replaced: "NORMAL", kicked: "KICKED", banned: "REJECTED", full: "REJECTED", "update-required": "VERSION", "server-shutdown": "SHUTDOWN", idle: "IDLE" });
const closeCode = (codes, reason) => codes[REASON_CODES[reason] ?? (/invalid|flood|requires|failed/.test(reason) ? "PROTOCOL" : "APPLICATION")] ?? 7;

// Hysteresis on queueing delay: past this much round trip over the path's
// minimum, every other snapshot is skipped until it drains (the next one is
// still encoded against the client's acknowledged baseline).
const CONGESTED_MS = 120, CLEAR_MS = 50;

export class UdpPeer extends EventEmitter {
  constructor(session, { contract, content, closeCodes }) {
    super();
    this.session = session; this.closeCodes = closeCodes;
    this.compatibility = { ...wireCompatibility(contract), content };
    this.remote = { ...session.hello, publicKey: session.publicKey };
    this.identity = session.id;
    this.spectator = session.hello?.content !== content;
    this.authenticated = true; this.closed = false; this.lastSequence = 0; this.sequence = 0;
    this.congested = false; this.skipNext = false;
    session.on("message", (lane, payload) => this.receive(lane, payload));
    session.on("fault", error => this.emit("fault", String(error?.message ?? error)));
    // A player leaving normally is "closed", an orderly local close keeps its
    // reason key (shutdown, kick), and anything else keeps its reason text.
    const codes = closeCodes ?? {}, orderly = new Map([[codes.NORMAL ?? 0, "closed"], [codes.KICKED, "kicked"], [codes.SHUTDOWN, "server-shutdown"]]);
    session.once("close", info => this.finish(info.remote ? (info.code === (codes.NORMAL ?? 0) ? "closed" : `remote-${info.reason || info.code}`)
      : orderly.get(info.code) ?? (info.reason || "closed")));
    // The round trip the roster shows as PING, from the transport itself.
    this.timer = setInterval(() => this.emit("ping", Math.round(session.rtt)), 4000);
    this.timer.unref?.();
  }
  receive(lane, payload) {
    if (this.closed) return;
    try {
      if (typeof payload !== "string") {
        if (lane === "state") {
          // A Uint8Array view (newer engines: of the decrypted packet) or a Buffer.
          const magic = payload.length >= 4 ? new DataView(payload.buffer, payload.byteOffset, 4).getUint32(0, true) : 0;
          if (payload.length > 1100 || !STATE_MAGICS.has(magic) || (magic === 0x31494e42 && payload.length < 16) || (magic === 0x31414e42 && payload.length !== 10)) throw Error("invalid entity message");
        } else if (payload.length > 32768) throw Error("invalid binary message");
        this.emit("binary", payload, lane);
        return;
      }
      if (Buffer.byteLength(payload) > 32768) throw Error("invalid message");
      const message = JSON.parse(payload);
      if (!message || typeof message !== "object") throw Error("invalid message");
      // Unordered state messages are superseded by newer ones.
      if (lane === "state") { if (!Number.isSafeInteger(message.sequence) || message.sequence <= this.lastSequence) return; this.lastSequence = message.sequence; }
      if (message.type === "pong" || message.type === "ping") return;
      this.emit("message", lane, message);
    } catch (error) {
      this.emit("fault", error.message);
      this.close("invalid-message");
    }
  }
  send(label, value) {
    if (this.closed) return false;
    return this.sendEncoded(label, JSON.stringify(label === "state" ? { ...value, sequence: ++this.sequence } : value), value?.type);
  }
  // An already serialized JSON message (one encode shared by every
  // recipient of a broadcast). Never for the state lane, whose messages
  // each carry this peer's own sequence. A message over the transport's
  // bound is dropped with a log, never thrown into the caller's loop.
  sendEncoded(label, data, type = "") {
    if (this.closed) return false;
    const bytes = Buffer.byteLength(data);
    if (bytes > 32768) {
      countNet("oversizedMessages");
      console.warn("Dropped oversized message:", JSON.stringify({ lane: label, type: String(type ?? "").slice(0, 40), bytes }));
      return false;
    }
    return this.session.send(label, data);
  }
  sendBinary(value, label = "bulk") {
    if (!["bulk", "state"].includes(label)) throw Error("Invalid binary channel");
    if (value.byteLength > 32768) throw Error("binary message too large");
    return !this.closed && this.session.send(label, Buffer.from(value.buffer ?? value, value.byteOffset ?? 0, value.byteLength));
  }
  // Datagrams of one snapshot, coalesced into as few packets as they fit.
  sendStatePacket(frames) {
    if (!Array.isArray(frames) || !frames.length || frames.length > 128 || frames.some(frame => frame.byteLength > 32768)) throw Error("Invalid state packet group");
    if (this.closed) return false;
    for (const frame of frames) this.session.send("state", Buffer.from(frame.buffer ?? frame, frame.byteOffset ?? 0, frame.byteLength));
    return true;
  }
  // Packs and seals what this peer has queued now, instead of in the
  // transport's end-of-task flush: the world-message drain (game-session.js)
  // charges each session's encryption to its time slice, and a welcome
  // leaves before a snapshot queued after it.
  flush() { if (!this.closed) this.session.flush?.(); }
  // GameSession's skip-whole-snapshot hint: the local socket queue, plus a
  // large value on alternate snapshots while the path shows queueing delay.
  stateBacklog() {
    const stats = this.session.connection, queue = this.queuedBytes();
    if (Number.isFinite(stats.minRtt)) {
      const excess = stats.smoothedRtt - stats.minRtt;
      if (!this.congested && excess > CONGESTED_MS) this.congested = true;
      else if (this.congested && excess < CLEAR_MS) this.congested = false;
    }
    if (!this.congested) return queue;
    this.skipNext = !this.skipNext;
    return this.skipNext ? queue + (1 << 30) : queue;
  }
  queuedBytes() { return this.session.connection.path?.socket?.getSendQueueSize?.() ?? 0; }
  transportStats() { return { ...this.session.stats(), congested: this.congested }; }
  path() {
    const path = this.session.path;
    return path ? { kind: path.kind, family: path.family === "IPv6" ? "ipv6" : "ipv4" } : { kind: "checking" };
  }
  close(reason = "closed", message = "") {
    if (this.closed) return;
    this.finish(reason);
    this.session.close(closeCode(this.closeCodes ?? {}, reason), message || reason);
  }
  finish(reason) {
    if (this.closed) return;
    this.closed = true; this.authenticated = false;
    clearInterval(this.timer);
    this.emit("closed", reason);
  }
}
