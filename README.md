# Battlefront Eternal server

This repository held the Node.js dedicated server of Battlefront Eternal up to
preview 331. That server is retired: the dedicated server is now `bfeserver`,
built from the main repository
([centrevilletech/battlefront-eternal](https://github.com/centrevilletech/battlefront-eternal),
`rust/bfe-server`, docs/native-server.md) and published with every release.
Clients of the current release cannot join a Node.js server, and Electron
clients of preview 331 cannot join `bfeserver`: the two simulations differ, and
each side tells the player which one is out of date.

`bfeserver` reads the same `config.json`, `identity.json`, `operator-token`,
`operator.sqlite` (bans and scores) and `master-ban-key` as the Node.js server
did, in the same state folder, with the same ports (game UDP and `/info` TCP
18451, LAN discovery UDP 18452, loopback `/healthz` and `/admin` 18450). A
server keeps its name, identity, listing and bans when it moves. Game data
already in the release's format is kept; older data is prepared again from the
owned game.

## Moving a server to bfeserver

**A server that updates itself** (set up as docs/server-updates.md of the main
repository describes: `/opt/bfeserver`, `bfeserver-update.timer`): nothing to
do. Its updater installs the new release, prepares its game data, and moves
each server to `bfeserver` once nobody plays on it. `journalctl -u
bfeserver-update` shows each step.

**Any other Linux host with systemd** (this repository's
`deploy/bfeserver.service`, a git checkout started by a unit of your own):
one command, as root:

```sh
curl -fsSL https://raw.githubusercontent.com/centrevilletech/battlefront-eternal-server/main/get-bfeserver.sh | sudo sh -s -- --game /path/to/game
```

`get-bfeserver.sh` (in this repository: read it first if you prefer, then run
it) downloads the newest server archive from
`https://get.battlefronteternal.com/downloads/latest/`, checks it against the
release's `SHA256SUMS`, unpacks it and runs its `install-server.sh`, which:

- finds every `bfeserver*.service` unit that runs the dedicated server and
  writes a drop-in that starts `/opt/bfeserver/rust/current/bfeserver` instead,
  keeping the unit's name, user, state folder and limits;
- keeps the server's game data when the release can use it, otherwise prepares
  new data from `--game` (the folder holding `GameData` of your own Steam or
  GOG copy) and points the unit at it;
- restarts each server that nobody plays on; a busy one moves at its next
  restart;
- without any server unit, sets up `bfeserver.service` from scratch.

Run it again with a newer release to update. To go back to the Node.js
server, remove `/etc/systemd/system/<unit>.service.d/50-bfeserver.conf` and
restart the unit.

**Docker or Compose**: the `Dockerfile` here now builds an image of
`bfeserver` from a release's server archive. Keep the state volume (`/data`)
and the game data mount; the engine mount is no longer used:

```sh
docker compose build --pull && docker compose up -d
```

## Running bfeserver

`bfeserver` alone opens the setup and administration menu; `bfeserver
--serve` runs the service; `bfeserver --admin '{"action":"status"}'` sends an
operator command to the running server; `bfeserver --help` lists every option
and environment variable. docs/native-server.md and docs/server-updates.md in
the main repository describe the configuration, the online listing, global
bans and automatic updates.

The Node.js source stays in this repository's history (the last revision is
10ffbb5).

## Independent project and trademarks

Battlefront Eternal is an independent fan project. It is not affiliated with
or endorsed by Lucasfilm, Disney, Electronic Arts, Aspyr, Valve, GOG or the
developers and publishers of the original games. Star Wars, Battlefront, Steam
and GOG are trademarks of their respective owners.

No original game files are included or distributed with this server, its
image or its releases. Each server prepares its game data from the operator's
own Steam or GOG copy of Star Wars: Battlefront (Classic, 2004), and that
prepared data stays on the operator's machine.
