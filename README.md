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

Finished matches advance the configured playlist and reload connected clients
into a new authoritative world. The same persistent server identity is retained.

Remaining release gates include complete operator kick/ban/stats
controls, Steam acquisition, signed installers/updates, vehicle and late-join
qualification, and full-match testing under latency, packet loss and cellular
networks. Do not treat workstation relay tests as Starlink or load qualification.

Transport dependencies retain their licenses: node-datachannel/libdatachannel
(MPL-2.0), ws (MIT), and werift (MIT). The portable WebRTC backend is used for
TURN/TCP and TLS because the distributed native libjuice build did not complete
TURN/TCP in deployment testing.
