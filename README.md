# Battlefront Eternal server

GPL-3.0-only headless authoritative multiplayer server. It runs the shared game
simulation with independent human controllers, bots and per-player entity state over
the engine's authenticated, encrypted native UDP transport (protocol 3, BFE-UDP/1).
This is a development build.

Run `npm ci && npm test`, then `node src/main.js` for the local setup menu and
`npm start` for the service. Configure an engine checkout and operator-owned
imported game data with the menu or `BFE_ENGINE_DIR` and `BFE_DATA_DIR`.
Set `BFE_STATE_DIR` to keep configuration and identity in your chosen directory.
Node 22.13 or later is required; no native module or OpenSSL binary is. The engine
must include `src/runtime/multiplayer-world.js` and the transport in `src/net/`:
the server imports the wire protocol from the mounted engine, as it does the
simulation, so both always speak the same revision.
Retail data is never downloaded or included in this repository or its image.

LAN hosting works without accepting the online notice or contacting a master.
Gameplay uses UDP 18451 (the game port, IPv4 and IPv6 sockets), HTTP `/info`
uses TCP 18451, LAN discovery UDP 18452, and the diagnostic `/healthz` listener
stays on loopback port 18450. Open UDP and TCP 18451 for players outside the LAN.
Set `bind` and `gamePort` in config.json to change them. The game port is bound
once for the life of the service; map rotations keep it. It runs on a network
thread of its own, where every player's packets are encrypted, acknowledged and
exchanged with the world simulation without passing through the main thread
(`networkThread: false` keeps it on the main thread). The transport handshake
proves both Ed25519 identities and refuses an incompatible client with an
update-required reason before it joins. One network (an IPv4 address or an IPv6
/64; loopback exempt) may hold `maxPlayersPerAddress` sessions (default 8), since
player identities cost nothing to create; raise it for a large LAN party behind
one NAT joining an online server.
Each connected player receives up to `stateBytesPerSecond` of entity state
(default 100000, about 800 kbit/s of server-to-client state payload). Lower
it in config.json for a host with little upstream bandwidth, at the cost of
less frequent updates for distant actors. The unified presentation records need
fresh measurements; the earlier 85 KB/s full-field figure predates that path.
Players join at the spawn screen and may pick their side; `autoAssignTeams: true`
keeps the roster's assignment and `autoSpawn: true` puts a joining player on the
field at once. A finished battle shows its end table for ten seconds
(`rotationDelayMs`) before the next map loads. The operator menu (and the desktop HOST) can restart the match.

Accepting the online notice enables directory registration, 20-second
heartbeats and master WebSocket rendezvous. Rendezvous exchanges UDP candidates:
the server answers with its public and reflexive (STUN) game-port addresses
(private ones only to a player behind the server's own NAT), punches toward the
player's public addresses, and the player connects to whichever answers first.
`relayPolicy` accepts `auto`, `never` or `always`. With `auto` a server behind NAT,
and with `always` every online player, gets a per-player TURN UDP relay on the
master's relay; `always` advertises no direct address. At most `maxPlayers` + 8
relays exist at once, and a relay whose player has not joined within 30 seconds is
released. TURN credentials are short lived. Server identity persists across restarts. With
`honorGlobalBans` enabled the server verifies signed expiring ban lists, pins the
master signing key on first use and refreshes the list every two minutes.

Input commands and snapshot datagrams use the unreliable `state` lane; registry,
welcome, gameplay events and chat use the ordered reliable `events` lane. The
simulation runs at 60 Hz. The engine's shared entity host schedules remote
updates at nominal 20 Hz, with additional own-player corrections. A reliable
registry identifies classes, weapons and posts; entity records establish both
the initial scene and ongoing gameplay. There is no replica snapshot bootstrap.
The same engine host implements local play, desktop HOST and dedicated hosting.
The server owns authentication, transport, administration and the worker loop.

A Dockerfile, Compose example and systemd unit are included. Mount the engine
and imported data read-only and persist `/data`. A Linux Docker host should use
host networking for LAN discovery and to keep players' source addresses. No GPU,
Electron, DOM or WebGL is required by the server.

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

The only runtime dependency is ws (MIT), for master rendezvous. The WebRTC stack
(node-datachannel, werift and its patch), the DTLS certificate and the RTC load
and smoke scripts are retired with protocol 2. Load and impairment testing uses
the engine's `tools/net-probe.mjs` (real world worker, `UdpPeer`, `GameSession` and
production clients over the native transport, with optional kernel shaping).
`BFE_ENGINE_DIR=<engine> npm test` also runs the real-transport integration test,
which is skipped without an engine checkout.

All online, LAN and desktop HOST sessions require the one contract exported by
`src/runtime/network-contract.js` in the mounted engine. The current engine uses
protocol 3 (native UDP), the engine's current simulation revision, profile
`snapshot-delta-v1` and server adapter ABI 4. Missing or incompatible manifests fail startup before
advertising gameplay readiness. Older clients receive an update-required error;
there is no capability negotiation or legacy gameplay fallback.

ABI 2 removed the replica bootstrap and delegates joins and ongoing publication
to the engine-owned entity host. Deploy this adapter with the matching engine.
ABI 3 identifies dropped offers by epoch and packet ID rather than reporting
only a byte count. The host releases pending membership and refunds only the
charged bytes once; forced uncharged corrections cannot create refund credit.
This requires a paired engine/adapter deployment, with unchanged client wire.
The BNI1/BNE1 envelopes retain their 1100-byte limit and 30-byte state header.
State uses independent absolute entity messages on an unordered channel. Each
native admission receipt authorizes only that packet's membership ACK. Missing
actors are unchanged; lifecycle removal requires a tombstone. The own correction
includes consumed and discarded input watermarks, held controls and view context.

Compact BNI1 input carries held state, cumulative look and independent numbered
edges. Durable receipt stops retransmission; explicit consumed/expired/cancelled
results retire prediction only when an applied correction reaches their outcome
tick. Consumption means the simulation received the control, not that a weapon
fired or hit anything. Independent input ACKs never imply discarded input executed.

The engine scheduler defaults to 100,000 bytes/second of state payload per
connected client. Control traffic and network overhead are outside this budget,
and own-player corrections have a starvation fallback. The client-upload target
is 200 kbit/s, with up to 300 kbit/s available if measurements require it; this
is independent of server state traffic, which consumes client download. Client
download may use about 2 Mbit/s. Aldine currently allows 100,000 bytes/s of state
payload per recipient; controls and transport overhead are additional. These are
bandwidth planning targets, not enforced wire-rate caps. Armies remain capped
at 32 per team (64 players total, replacing bots); 64 versus 64 is unsupported.
Distance, closing speed, aim direction and a vehicle priority floor determine
importance. Captured common records are shared across recipient schedulers.

Historical load scripts and transport reports describe retired protocols. Use
the engine's entity-path probes for current checks. Full 64-player capacity,
physical M1 rendering and impaired-network qualification remain separate gates.

Local operator profiling is disabled by default. Start with
`BFE_WORLD_METRICS=1` and optionally `BFE_WORLD_PROFILE=/tmp/world.cpuprofile`.
Authenticated loopback admin actions `reset-metrics` and `metrics` delimit the
measurement window and return bounded simulation/publication samples and worker
CPU usage. Reading metrics writes the configured V8 profile. Remote gameplay
peers cannot enable profiling or choose output paths.
