# Battlefront Eternal server

GPL-3.0-only headless authoritative multiplayer server. It runs the shared game
simulation with independent human controllers, bots and per-player replicas over
authenticated encrypted WebRTC channels. This is a development build.

Run `npm ci && npm test`, then `node src/main.js` for the local setup menu and
`npm start` for the service. Configure an engine checkout and operator-owned
imported game data with the menu or `BFE_ENGINE_DIR` and `BFE_DATA_DIR`.
Set `BFE_STATE_DIR` to keep configuration, identity and the persistent DTLS
certificate in your chosen directory. Node 22.13 or later and OpenSSL are required.
The engine must include `src/runtime/multiplayer-world.js`.
Retail data is never downloaded or included in this repository or its image.

LAN hosting works without accepting the online notice or contacting a master.
UDP discovery uses port 18452, HTTP `/info` and WebSocket `/signal` use port 18451,
and the diagnostic `/healthz` listener stays on loopback port 18450. The local
signaling listener binds `::` with IPv4 dual-stack support; set `bind` and `gamePort` in config.json to
change it. Clients verify Ed25519 identities bound to both DTLS fingerprints.

Accepting the online notice enables directory registration, 20-second
heartbeats and master WebSocket rendezvous. `relayPolicy` accepts `auto`, `never`
or `always`; always excludes direct ICE candidates. TURN credentials are short
lived. Server identity and DTLS certificate persist across restarts. With
`honorGlobalBans` enabled the server verifies signed expiring ban lists, pins the
master signing key on first use and refreshes the list every two minutes.

Input commands use an unordered channel. Replica deltas currently use reliable
ordered bulk messages so a dropped update cannot corrupt the chain. The simulation runs at 60 Hz; compressed replicas are sent at 20 Hz, preserving
transient calls between snapshots. Initial snapshots are chunked below the channel message limit and stalled clients have
a bounded backlog. This is correctness-first transport; full-match bandwidth
and performance qualification remain open.

`scripts/world-peers-smoke.mjs` runs two authenticated clients against the real
Theed simulation and checks separate actor identities and streamed snapshots.
It requires locally imported data. Add `--relay` to verify the same two-player
gameplay through the public master and Aldine relay. `scripts/relay-smoke.mjs` checks the deployed
relay over UDP, TCP, TLS and IPv4/IPv6 combinations. `scripts/signaling-smoke.mjs`
checks public master rendezvous. Their temporary directory listings are removed.

A Dockerfile, Compose example and systemd unit are included. Mount the engine
and imported data read-only and persist `/data`. A Linux Docker host should use
host networking for LAN discovery and ICE. No GPU, Electron, DOM or WebGL is
required by the server.

The Compose example and Aldine production deployment allocate two CPU cores and
1 GiB of memory to the game server. CI enforces that production budget while
preserving the operator's engine and game-data mounts.

Finished matches advance the configured playlist and reload connected clients
into a new authoritative world. The same persistent server identity is retained.

The setup menu's live administration option shows authenticated player identities,
kicks players, sets local expiring or permanent bans, queries persisted match
statistics and sends server announcements. These controls use a loopback-only
HTTP endpoint and a local `operator-token` file readable only by its owner.
`bfeserver --admin '{"action":"status"}'` also works for local automation.
SQLite stores cumulative match checkpoints under authenticated identities; repeated
checkpoints do not duplicate scores. No player IP addresses are stored there.

Remaining release gates include Steam acquisition, signed installers/updates, vehicle and late-join
qualification, and full-match testing under latency, packet loss and cellular
networks. Do not treat workstation relay tests as Starlink or load qualification.

Transport dependencies retain their licenses: node-datachannel/libdatachannel
(MPL-2.0), ws (MIT), and werift (MIT). The portable WebRTC backend is used for
TURN/TCP and TLS because the distributed native libjuice build did not complete
TURN/TCP in deployment testing.

Local capacity testing uses `node --max-old-space-size=6144 scripts/load-peers.mjs --players=64 --seconds=60`. Supported sizes are 1, 8, 32, and 64. It starts an actual private headless world and native server RTC connections, with client traffic in separate processes (one per eight clients). Every eighth client uses the portable backend. No public server or relay is used. The harness caps combined RSS at 8 GiB and reports tick/encoding/compression latency, RTT, receive age, input acknowledgment lag, transport backlog, bandwidth, and server CPU. Use `--loss=0 --latency=0 --jitter=0` for a clean baseline. Defaults apply 5% input loss and 50–100 ms one-way delay; reliable bulk messages receive a synthetic 200 ms retry delay on 5% of sends. These application delays are **not** a measurement of actual SCTP retransmission under kernel packet loss. Run without browsers or other CPU-heavy tasks for capacity qualification.

Dedicated hosts default to native RTC and use UDP when allocating their own TURN relay. Remote clients can still reach that allocation through their own TCP or TLS relay connection. `BFE_PORTABLE_RTC=1` or configuration `portableRTC: true` selects the portable backend for desktop packaging or hosts requiring TCP/TLS-only egress. `node scripts/relay-smoke.mjs --mixed` checks native-host interoperability with portable TCP/TLS clients using the configured public connectivity infrastructure.

Authenticated hello capabilities keep older clients compatible:

- `compact-personal-v1` enables the private engine's compact personal metadata codec; other clients receive ordinary metadata.
- `binary-bulk-v1` enables raw compressed replica frames and is confirmed by `welcome.binaryBulk`. A frame has a 16-byte little-endian header: magic `0x31424642`, packet ID (u32), fragment index (u16), fragment count (u16), and metadata byte length (u32). UTF-8 JSON metadata appears only in fragment zero, followed by at most 20,000 compressed bytes. Metadata is capped at 8,192 bytes; frames at 32,768 bytes; a packet at 128 fragments. Reset messages remain ordered JSON on the same bulk channel. Clients without the capability retain base64 JSON packets.
- `input-state-v1` enables unreliable movement snapshots plus independent reliable action envelopes, confirmed by `welcome.inputMode: "split-v1"`. Reliable `actions` carry `epoch`, `id`, `tick`, `generation`, `actions`, and `human`; the worker binds the authenticated identity and acknowledges consumed actions with `ackActions`. Movement redundancy cannot discard a delayed reliable action.
- `linear-projectiles-v1` enables shared linear projectile events only while every started client advertises support. A legacy client starting or a capable client leaving recomputes support and forces a fresh keyframe when the shared mode changes.

The load harness enables these capabilities by default. Use `--binary=0`, `--compact=0`, or `--linear=0` for comparisons. It excludes a five-second warmup from reported measurements (`--warmup=0` disables that). `--profile=/tmp/world.cpuprofile` captures the worker's V8 CPU profile; `node scripts/profile-summary.mjs /tmp/world.cpuprofile` prints the hottest call sites. Profile paths are local operator arguments and are never accepted from a remote peer.

The experimental `snapshot-journal-v1` capability is opt-in. It sends acknowledged
checkpoint deltas on the unordered state channel, with bootstrap snapshots and
compressed event batches on reliable bulk. Peers without this capability keep
the existing ordered snapshot stream. Checkpoint ACKs are accepted only for
snapshots published to that authenticated peer; expired baselines bootstrap
again. This path is under qualification and is not enabled by default. Local
qualification can select it with `node scripts/load-peers.mjs --players=2
--seconds=10 --journal=1`; these measurements exclude Internet TURN capacity.

The additional opt-in `snapshot-transients-v1` capability moves stateless gunfire,
impact sounds and brief animation flashes to deflated BFB4 frames on the unordered
state channel. Effect handles and persistent animation transitions remain reliable.
Expendable batches yield to reliable backlog, and each fragment group is admitted
atomically. Clients without this capability keep the complete reliable event stream.
Use `--journal=1 --transients=1` for local qualification; production clients do not
enable this experimental mode by default.
