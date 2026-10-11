// Standard WebRTC fallback for TURN/TCP and TURN/TLS, which the distributed
// libjuice-backed node-datachannel binary does not implement.
import { readFileSync } from "node:fs";
import { createPrivateKey } from "node:crypto";
import { RTCPeerConnection } from "werift";
class Channel {
  constructor(channel) {
    this.channel = channel;
  }
  getLabel() {
    return this.channel.label;
  }
  isOpen() {
    return this.channel.readyState === "open";
  }
  bufferedAmount() {
    return this.channel.bufferedAmount;
  }
  sendMessage(value) {
    this.channel.send(value);
    return true;
  }
  sendMessageBinary(value) {this.channel.send(Buffer.from(value));return true;}
  close() {
    this.channel.close();
  }
  onOpen(fn) {
    this.channel.onopen = fn;
  }
  onClosed(fn) {
    this.channel.onclose = fn;
  }
  onError(fn) {
    this.channel.onerror = fn;
  }
  onMessage(fn) {
    this.channel.onmessage = (e) => fn(e.data);
  }
}
export class PortablePeerConnection {
  constructor(name, config) {
    let dtls;
    if (config.certificatePemFile) {
      const keyPem = readFileSync(config.keyPemFile, "utf8"),
        certPem = readFileSync(config.certificatePemFile, "utf8");
      const type = createPrivateKey(keyPem).asymmetricKeyType;
      if (!["ec", "rsa"].includes(type)) throw Error("unsupported DTLS key");
      dtls = {
        keys: {
          certPem,
          keyPem,
          signatureHash: { hash: 4, signature: type === "ec" ? 3 : 1 },
        },
      };
    }
    const iceServers = config.iceServers.map((s) =>
      typeof s === "string"
        ? { urls: s }
        : {
            urls: `${s.relayType === "TurnTls" ? "turns" : "turn"}:${s.hostname}:${s.port}?transport=${s.relayType === "TurnUdp" ? "udp" : "tcp"}`,
            username: s.username,
            credential: s.password,
          },
    );
    this.pc = new RTCPeerConnection({
      iceServers,
      ...(dtls ? { dtls } : {}),
      iceTransportPolicy: config.iceTransportPolicy,
      iceUseIpv4: config.ipFamily !== 6,
      iceUseIpv6: config.ipFamily !== 4,
      turnUdpFamily: config.ipFamily || undefined,
    });
    this.pc.ondatachannel = (e) => this.dataChannel?.(new Channel(e.channel));
    this.pc.connectionStateChange.subscribe((state) =>
      this.stateChange?.(state),
    );
    this.chain = Promise.resolve();
    this.scheduled = false;
  }
  task(fn) {
    this.chain = this.chain.then(fn).catch((error) => {
      this.stateChange?.("failed");
      this.close();
    });
  }
  onLocalDescription(fn) {
    this.description = fn;
  }
  onLocalCandidate(fn) {
    /* Complete candidate sets travel in SDP. No third-party signaling. */
  }
  onDataChannel(fn) {
    this.dataChannel = fn;
  }
  onStateChange(fn) {
    this.stateChange = fn;
  }
  createDataChannel(label, config = {}) {
    const ch = this.pc.createDataChannel(label, {
      ordered: !config.unordered,
      ...(config.maxPacketLifeTime!==undefined?{maxPacketLifeTime:config.maxPacketLifeTime}:{}),
      ...(config.maxRetransmits !== undefined
        ? { maxRetransmits: config.maxRetransmits }
        : {}),
    });
    if (!this.scheduled) {
      this.scheduled = true;
      queueMicrotask(() =>
        this.task(async () => {
          const offer = await this.pc.createOffer();
          await this.pc.setLocalDescription(offer);
          this.description?.(this.pc.localDescription.sdp, "offer");
        }),
      );
    }
    return new Channel(ch);
  }
  setRemoteDescription(sdp, type) {
    this.task(async () => {
      await this.pc.setRemoteDescription({ sdp, type });
      if (type === "offer") {
        const answer = await this.pc.createAnswer();
        await this.pc.setLocalDescription(answer);
        this.description?.(this.pc.localDescription.sdp, "answer");
      }
    });
  }
  addRemoteCandidate(candidate, mid) {
    this.task(() =>
      this.pc.addIceCandidate({ candidate, sdpMid: mid, sdpMLineIndex: 0 }),
    );
  }
  localDescription() {
    return this.pc.localDescription;
  }
  remoteDescription() {
    return this.pc.remoteDescription;
  }
  state() {
    return this.pc.connectionState;
  }
  getSelectedCandidatePair() {
    const pair = this.pc.iceTransports[0]?.getSelectedCandidatePair();
    if (!pair) return null;
    const map = (c) => {
      const fields = c.candidate.trim().split(/\s+/);
      return { address: fields[4], port: Number(fields[5]), type: fields[7] };
    };
    return { local: map(pair.local), remote: map(pair.remote) };
  }
  close() {
    if (!this.closed) {
      this.closed = true;
      this.pc.close().catch(() => {});
    }
  }
}
