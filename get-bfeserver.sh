#!/bin/sh
# get-bfeserver.sh: download the newest Battlefront Eternal dedicated server (bfeserver) archive,
# check it against the release's SHA256SUMS, and run its install-server.sh with your arguments.
#
#   sudo sh get-bfeserver.sh [--game DIR] [--now] [...]   (install-server.sh --help lists them)
#
# It installs a new server, or moves every server that runs the earlier Node.js server to
# bfeserver, keeping each one's configuration, identity, bans, ports and service name.
set -eu
BASE=${BFE_DOWNLOADS:-https://get.battlefronteternal.com/downloads/latest}
case $(uname -m) in
  x86_64|amd64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) echo "bfeserver is built for x86_64 and aarch64 Linux, not $(uname -m)" >&2; exit 1 ;;
esac
[ "$(id -u)" = 0 ] || { echo "run as root (sudo)" >&2; exit 1; }
for tool in curl tar xz sha256sum; do command -v $tool >/dev/null || { echo "needs $tool" >&2; exit 1; }; done
WORK=$(mktemp -d /tmp/bfeserver.XXXXXX)
trap 'rm -rf "$WORK"' EXIT
curl -fsSL "$BASE/SHA256SUMS" -o "$WORK/SHA256SUMS"
LINE=$(grep -E "  battlefront-eternal-server_[^ ]+_linux-$ARCH\.tar\.xz\$" "$WORK/SHA256SUMS" | head -1)
[ -n "$LINE" ] || { echo "no server archive for linux-$ARCH in $BASE/SHA256SUMS" >&2; exit 1; }
NAME=${LINE##* }
echo "downloading $NAME"
curl -fSL "$BASE/$NAME" -o "$WORK/$NAME"
(cd "$WORK" && echo "$LINE" | sha256sum -c -)
mkdir "$WORK/server"
tar -xJf "$WORK/$NAME" -C "$WORK/server"
[ -x "$WORK/server/install-server.sh" ] || { echo "$NAME has no install-server.sh (a release before bfeserver?)" >&2; exit 1; }
# The archive is installed from a copy, so the temporary folder can go afterwards.
"$WORK/server/install-server.sh" "$@"
