FROM node:24-bookworm-slim
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev && mkdir /data && chown node:node /data
COPY src ./src
USER node
ENV BFE_STATE_DIR=/data
HEALTHCHECK --interval=30s --timeout=5s CMD node -e "fetch('http://127.0.0.1:18450/healthz').then(r=>r.json()).then(x=>process.exit(x.ok?0:1)).catch(()=>process.exit(1))"
CMD ["node", "src/main.js", "--serve"]
