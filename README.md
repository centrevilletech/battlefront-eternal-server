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
Each connected player receives up to `stateBytesPerSecond` of entity state
(default 100000, about 800 kbit/s; a full 32 versus 32 field refreshed at 20 Hz
uses about 85 KB/s of it); lower it in config.json for a host with little
upstream bandwidth, at the cost of less frequent updates for distant actors.
Players join at the spawn screen and may pick their side; `autoAssignTeams: true`
keeps the roster's assignment and `autoSpawn: true` puts a joining player on the
field at once. A finished battle shows its end table for ten seconds
(`rotationDelayMs`) before the next map loads. The operator menu (and the desktop HOST) can restart the match.

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
CI still builds and tests documentation and harness changes, but deployment keeps
a healthy active match when runtime inputs match its pinned image commit. It also
checks the stack identity, private mounts and resource budget before skipping.
A manual workflow run forces deployment, including a base-image refresh.

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

All online, LAN and desktop HOST sessions require the one contract exported by
`src/runtime/network-contract.js` in the mounted engine. The current staged
engine uses protocol 2, `bfe-sim-5`, profile `entity-state-v1`, and correction schema
8. Before gameplay readiness, the server validates manifest version 1 and adapter
ABI 1 and checks the runtime correction export against the manifest. Missing or
incompatible manifests fail startup. Discovery, signaling, identity proofs and
sessions all use that validated contract; the server has no fallback simulation
string. Older clients receive an update-required version error. There is no
capability negotiation or legacy gameplay fallback.

Adapter ABI 1 fixes the server-owned BNI1/BNE1 envelope magic, 1100-byte limit,
30-byte state header and tick offset 12, epoch/input/edge semantics, and worker API
shapes. Engine-owned record bodies may evolve with a new mandatory simulation
revision while these interfaces remain unchanged. Changing the adapter ABI needs
a matched server or atomic pair deployment. Stage this manifest on the existing
simulation-five engine before deploying a server that requires it.
The identity proof uses the `BFE/2` transcript and the same signed peer identities.

One reliable registry/initial-world bootstrap establishes the scene. Ongoing
state consists of independent absolute entity messages of at most 1100 bytes on
an unordered channel with a fixed 50 ms lifetime. Each native admission receipt
authorizes only that packet's membership ACK. Missing actors are unchanged;
lifecycle removal requires a tombstone. The current own correction includes consumed
and discarded input watermarks, authoritative held controls and view context.

Compact BNI1 input carries held state, cumulative look and independent numbered
edges. Durable receipt stops retransmission; explicit consumed/expired/cancelled
results retire prediction only when an applied correction reaches their outcome
tick. Consumption means the simulation received the control, not that a weapon
fired or hit anything. Independent input ACKs never imply discarded input executed.

The fixed scheduler reserves nominal 20 Hz own correction and bounds offered
state payload to 26,000 bytes/second with a 2,200-byte burst. Remaining credit
admits remote-only messages sized to the remaining credit. Authoritative distance,
closing speed, aim direction and a vehicle priority floor determine importance.
Common actor and battle/post records are prepared once per publication and shared
across recipient schedulers. Native admission age is
separate from actual delivery age; control traffic and network overhead are
outside this state budget.

Ground-driver correction exists, but full mounted gameplay, stationary turrets,
walkers, special units, complete effects/gameplay parity and
64-player latency, coverage and bandwidth qualification remain outstanding.
Historical load scripts and transport reports describe retired protocols; use
the private engine's current Chromium/netem harness for matched protocol-2 tests.

Local operator profiling is disabled by default. Start with
`BFE_WORLD_METRICS=1` and optionally `BFE_WORLD_PROFILE=/tmp/world.cpuprofile`.
Authenticated loopback admin actions `reset-metrics` and `metrics` delimit the
measurement window and return bounded simulation/publication samples and worker
CPU usage. Reading metrics writes the configured V8 profile. Remote gameplay
peers cannot enable profiling or choose output paths.
