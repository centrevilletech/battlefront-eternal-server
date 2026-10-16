FROM node:24-bookworm-slim
WORKDIR /app
COPY package*.json ./
COPY scripts ./scripts
RUN npm ci --omit=dev --ignore-scripts && mkdir /data && chown node:node /data
COPY src ./src
USER node
ENV BFE_STATE_DIR=/data
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s CMD ["node", "src/health-check.js"]
CMD ["node", "src/main.js", "--serve"]
