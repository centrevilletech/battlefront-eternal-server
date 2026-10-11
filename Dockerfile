FROM node:24-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends openssl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev && mkdir /data && chown node:node /data
COPY src ./src
USER node
ENV BFE_STATE_DIR=/data
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s CMD ["node", "src/health-check.js"]
CMD ["node", "src/main.js", "--serve"]
