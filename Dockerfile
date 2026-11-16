# bfeserver, the Battlefront Eternal dedicated server, from a release's server archive (checked
# against the release's SHA256SUMS). The state volume (/data) of the earlier Node.js image works
# as it is: same files, same owner (uid 1000).
FROM debian:trixie-slim
ARG BFE_DOWNLOADS=https://get.battlefronteternal.com/downloads/latest
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl xz-utils \
 && rm -rf /var/lib/apt/lists/* \
 && arch=$(dpkg --print-architecture) \
 && curl -fsSL "$BFE_DOWNLOADS/SHA256SUMS" -o /tmp/SHA256SUMS \
 && line=$(grep -E "  battlefront-eternal-server_[^ ]+_linux-$arch\.tar\.xz\$" /tmp/SHA256SUMS | head -1) \
 && name=${line##* } \
 && curl -fsSL "$BFE_DOWNLOADS/$name" -o "/tmp/$name" \
 && (cd /tmp && echo "$line" | sha256sum -c -) \
 && mkdir -p /opt/bfeserver \
 && tar -xJf "/tmp/$name" -C /opt/bfeserver \
 && rm -f /tmp/SHA256SUMS "/tmp/$name" \
 && useradd --uid 1000 --create-home --shell /usr/sbin/nologin bfeserver \
 && mkdir /data && chown bfeserver:bfeserver /data
USER bfeserver
ENV BFE_STATE_DIR=/data
EXPOSE 18451/udp 18451/tcp 18452/udp
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s CMD ["curl", "-fsS", "-o", "/dev/null", "http://127.0.0.1:18450/healthz"]
CMD ["/opt/bfeserver/bfeserver", "--serve"]
