import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
import { PortablePeerConnection } from "./portable-rtc.js";
import { randomBytes } from "node:crypto";
import { EventEmitter } from "node:events";
import { proof, verifyProof, keyId } from "./identity.js";
import { transportTuning, configureNativeTransport } from './transport-tuning.js';
export class Peer extends EventEmitter {
  constructor({
    name,
    keys,
    initiator = false,
    iceServers = [],
    relay = false,
    certificate,
    privateKey,
    ipFamily = 0,
    expectedKey,
    content,
    iceUdpMuxPort = 0,
    stateBufferLimit = transportTuning.stateBufferLimit,
    portable = process.env.BFE_PORTABLE_RTC === "1" || iceServers.some(
      (s) => s.relayType === "TurnTcp" || s.relayType === "TurnTls",
    ),
  }) {
    super();
    this.keys = keys;this.stateBufferLimit=stateBufferLimit;

    this.initiator = initiator;
    this.expectedKey = expectedKey;
    this.compatibility = { protocol:2, simulation:"bfe-sim-5", profile:"entity-state-v1", content };
    this.channels = new Map();
    this.pendingCandidates = [];
    this.hasRemoteDescription = false;
    this.authenticated = false;
    this.closed = false;
    this.sequence = 0;
    this.lastSequence = 0;
    this.nonce = randomBytes(32).toString("hex");
    this.lastTraffic = Date.now();
    const native = portable ? null : require("node-datachannel");
    if (native) configureNativeTransport(native);
    const Connection = portable ? PortablePeerConnection : native.PeerConnection;
    this.pc = new Connection(name, {
      iceServers,
      ...(iceUdpMuxPort && !portable ? {enableIceUdpMux:true,portRangeBegin:iceUdpMuxPort,portRangeEnd:iceUdpMuxPort}:{}),
      ipFamily,
      iceTransportPolicy: relay ? "relay" : "all",
      mtu: 1200,
      maxMessageSize: 65536,
      ...(certificate
        ? { certificatePemFile: certificate, keyPemFile: privateKey }
        : {}),
    });
    this.pc.onLocalDescription((sdp, type) =>
      this.emit("signal", { type, sdp }),
    );
    this.pc.onLocalCandidate((candidate, mid) =>
      this.emit("signal", { candidate, mid }),
    );
    this.pc.onDataChannel((ch) => this.channel(ch));
    this.pc.onStateChange((state) => {
      this.emit("state", state);
      if (["failed", "closed"].includes(state)) this.close(`rtc-${state}`);
    });
    this.timer = setInterval(() => {
      if (Date.now() - this.lastTraffic > 30000) this.close("idle-timeout");
      else if (this.authenticated)
        this.send("events", { type: "ping", time: Date.now() });
    }, 10000);
    this.timer.unref();
    this.authTimer = setTimeout(() => {
      if (!this.authenticated) this.close("authentication-timeout");
    }, 30000);
    this.authTimer.unref();
    // Defer channel creation so callers attach signaling callbacks first.
    if (initiator)
      queueMicrotask(() => {
        for (const label of ["state", "events", "bulk"])
          this.channel(
            this.pc.createDataChannel(
              label,
              label === "state" ? { unordered: true, maxPacketLifeTime:50 } : {},
            ),
          );
      });
  }
  channel(ch) {
    const label = ch.getLabel();
    if (
      !["state", "events", "bulk"].includes(label) ||
      this.channels.has(label)
    ) {
      ch.close();
      return;
    }
    this.channels.set(label, ch);
    ch.onError(() => this.close(`channel-${label}-error`));
    ch.onClosed(() => this.close(`channel-${label}-closed`));
    ch.onOpen(() => {
      if (label === "events") this.hello();
    });
    ch.onMessage((raw) => {
      try {
        if(typeof raw!=='string'&&label==='state'&&this.authenticated){const bytes=Buffer.from(raw),magic=this.initiator?0x31454e42:0x31494e42;if(bytes.length>1100||bytes.length<(this.initiator?30:16)||bytes.readUInt32LE(0)!==magic)throw Error('invalid entity message');this.lastTraffic=Date.now();this.emit('binary',bytes,label);return;}
        if(typeof raw!=='string' && label==='bulk' && this.authenticated){const bytes=Buffer.from(raw);if(bytes.length>32768)throw Error('invalid binary message');this.lastTraffic=Date.now();this.emit('binary',bytes,label);return;}
        if (typeof raw !== "string" || Buffer.byteLength(raw) > 32768)
          throw Error("invalid message");
        const m = JSON.parse(raw);
        this.lastTraffic = Date.now();
        if (label === "events" && !this.authenticated) {
          this.handshake(m);
          return;
        }
        if (!this.authenticated) throw Error("unauthenticated");
        if (label === "state") {
          if (
            !Number.isSafeInteger(m.sequence) ||
            m.sequence <= this.lastSequence
          )
            return;
          this.lastSequence = m.sequence;
        }
        if (m.type === "ping") {
          this.send("events", { type: "pong", time: m.time });
          return;
        }
        if (m.type === "pong") {
          this.emit("ping", Date.now() - m.time);
          return;
        }
        this.emit("message", label, m);
      } catch (error) {
        this.emit("fault", error.message);
        this.close("invalid-message");
      }
    });
    if (ch.isOpen() && label === "events") this.hello();
  }
  hello() {
    if (this.helloSent) return;
    this.helloSent = true;
    this.channels.get("events").sendMessage(
      JSON.stringify({
        type: "hello",
        nonce: this.nonce,
        ...this.compatibility,
        publicKey: this.keys.publicKey,
      }),
    );
  }
  transcript() {
    const first = this.initiator ? this.nonce : this.remote.nonce,
      second = this.initiator ? this.remote.nonce : this.nonce;
    const local = this.pc
        .localDescription()
        .sdp.match(/a=fingerprint:([^\r\n]+)/)?.[1],
      remote = this.pc
        .remoteDescription()
        .sdp.match(/a=fingerprint:([^\r\n]+)/)?.[1];
    return JSON.stringify([
      "BFE/2",
      first,
      second,
      this.initiator ? local : remote,
      this.initiator ? remote : local,
      this.compatibility.protocol,
      this.compatibility.simulation,
    ]);
  }
  handshake(m) {
    // Some backends deliver a remote message before their local open callback.
    // Send our hello first so the ordered channel cannot carry proof before hello.
    this.hello();
    if (m.type === "hello" && !this.remote) {
      if (
        !/^[a-f0-9]{64}$/.test(m.nonce) ||
        typeof m.publicKey !== "string" ||
        m.publicKey.length !== 60 ||
        m.protocol !== this.compatibility.protocol ||
        m.profile !== this.compatibility.profile ||
        m.simulation !== this.compatibility.simulation
      )
        {this.channels.get("events").sendMessage(JSON.stringify({type:"version-error",message:"Update required: protocol 2 / bfe-sim-5 / entity-state-v1"}));throw Error("Update required: incompatible protocol version");}
      if (this.expectedKey && keyId(m.publicKey) !== this.expectedKey)
        throw Error("unexpected identity");
      this.remote = m;
      this.spectator = m.content !== this.compatibility.content;
      this.channels.get("events").sendMessage(
        JSON.stringify({
          type: "proof",
          ...proof(this.keys, this.transcript()),
        }),
      );
    } else if (m.type === "proof" && this.remote) {
      if (
        m.publicKey !== this.remote.publicKey ||
        !verifyProof(m, this.transcript())
      )
        throw Error("invalid identity proof");
      this.authenticated = true;
      clearTimeout(this.authTimer);
      this.emit("ready", {
        id: keyId(m.publicKey),
        spectator: this.spectator,
        path: this.path(),
      });
    } else throw Error("invalid handshake");
  }
  signal(m) {
    if(this.closed)return;
    if (
      typeof m.sdp === "string" &&
      m.sdp.length <= 24000 &&
      ["offer", "answer"].includes(m.type)
    )
      {
        this.pc.setRemoteDescription(m.sdp, m.type);
        this.hasRemoteDescription = true;
        for (const c of this.pendingCandidates.splice(0)) this.pc.addRemoteCandidate(c.candidate, c.mid);
      }
    else if (
      typeof m.candidate === "string" &&
      m.candidate.length < 2048 &&
      typeof m.mid === "string" &&
      m.mid.length < 32
    )
      {
        if (this.hasRemoteDescription) this.pc.addRemoteCandidate(m.candidate, m.mid);
        else if (this.pendingCandidates.length < 32) this.pendingCandidates.push(m);
        else throw Error("too many queued candidates");
      }
    else throw Error("invalid signaling");
  }
  send(label, value) {
    const ch = this.channels.get(label);
    if (!this.authenticated || !ch?.isOpen()) return false;
    const data = JSON.stringify(
      label === "state" ? { ...value, sequence: ++this.sequence } : value,
    );
    if (Buffer.byteLength(data) > 32768) throw Error("message too large");
    if (ch.bufferedAmount() > (label==='state'?this.stateBufferLimit:65536)) {
      return false;
    }
    // Native libdatachannel false means accepted into its send queue.
    ch.sendMessage(data);return true;
  }
  sendBinary(value,label='bulk') {
    if(!['bulk','state'].includes(label))throw Error('Invalid binary channel');
    const ch=this.channels.get(label);if(!this.authenticated||!ch?.isOpen()||ch.bufferedAmount()>(label==='state'?this.stateBufferLimit:65536))return false;
    if(value.byteLength>32768)throw Error('binary message too large');ch.sendMessageBinary(Buffer.from(value));return true;
  }
  sendStatePacket(frames, {maxBuffered=this.stateBufferLimit}={}) {
    const ch=this.channels.get('state');
    if(!this.authenticated||!ch?.isOpen()||ch.bufferedAmount()>maxBuffered)return false;
    if(!Array.isArray(frames)||!frames.length||frames.length>128||frames.some(frame=>frame.byteLength>32768)||frames.reduce((n,frame)=>n+frame.byteLength,0)>2*1024*1024)throw Error('Invalid state packet group');
    // Admit a complete bounded snapshot, then apply backpressure to later ones.
    for(const frame of frames)ch.sendMessageBinary(Buffer.from(frame));
    return true;
  }
  path() {
    const pair = this.pc.getSelectedCandidatePair();
    return pair
      ? {
          kind: [pair.local.type, pair.remote.type].includes("relay")
            ? "relay"
            : "direct",
          family: pair.remote.address.includes(":") && !/^::ffff:(?:\d{1,3}\.){3}\d{1,3}$/i.test(pair.remote.address) ? "ipv6" : "ipv4",
        }
      : { kind: "checking" };
  }
  close(reason = "closed") {
    if (this.closed) return;
    this.closed = true;
    clearInterval(this.timer);
    clearTimeout(this.authTimer);
    for (const ch of this.channels.values()) ch.close();
    this.pc.close();
    this.emit("closed",reason);
  }
}
