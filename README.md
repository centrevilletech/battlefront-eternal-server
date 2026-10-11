# Battlefront Eternal server

GPL-3.0-only headless Node runtime and encrypted transport implementation.
Pre-release, not yet a playable multiplayer server. Do not advertise it as one.

`npm ci && npm test` tests offline encrypted state/events/bulk channels,
Ed25519 session proofs bound to DTLS fingerprints, content mismatch spectator
admission, and restricted local file loading. `scripts/relay-smoke.mjs` exercises
the deployed relay over UDP, TCP and TLS using short-lived credentials and
removes its temporary directory listing afterward.

`npm start` runs the service. `node src/main.js` opens the numbered local menu.
The service remains in setup mode until an engine checkout and operator-owned
imported data are configured. It never downloads or redistributes retail data.
Set `BFE_STATE_DIR`, `BFE_ENGINE_DIR` and `BFE_DATA_DIR`, or use the menu.
A Dockerfile, Compose definition and systemd unit are included. The systemd
unit expects a packaged Node runtime at `/opt/bfeserver/node/bin/node`.
No GPU, Electron, DOM or WebGL is needed for the world worker. The engine must
include the lazy texture-library import fix from 2026-10-11.

The worker uses the existing `createGame()` simulation and a confined local
file data port. A Theed world passed a 120-tick smoke test under Node 22.
The CLI is local; the health listener binds only loopback. It exposes
`multiplayerReady:false` until the remaining gameplay integration is complete.

Remaining release gates: multiple human controller ownership and spawn/seat
rules, per-player replicas and input validation, Electron IPC/network menus,
server list integration, LAN discovery and direct join, map rotation/content
manifests, operator kick/ban/stats controls, Steam acquisition, packaged installers,
signed updates and cross-platform qualification. The transport modules are
implemented, but the service does not yet connect them to a playable session.

Third-party transport: node-datachannel 0.33.4 / libdatachannel, MPL-2.0;
ws, MIT. Dependencies retain their licenses. Native GameNetworkingSockets was
reviewed as a candidate; it has no equally direct maintained Node binding and
turnkey deployment path here. WebRTC passed the local encrypted-transport test;
its complete cross-platform and adverse-network qualification remains open.
