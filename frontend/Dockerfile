# dagron frontend — Next.js 15 standalone build → slim node runtime.
#
# Build context is this frontend/ directory:
#   podman build -t dagron-frontend -f frontend/Dockerfile frontend
# The /api proxy target is set at runtime via DAGRON_API_URL (next.config.js).
FROM node:22-slim AS builder
WORKDIR /app
ENV NEXT_TELEMETRY_DISABLED=1
# next.config.js bakes the /api rewrite destination at BUILD time into the routes
# manifest, so the API host must be present now (runtime env can't change it).
ARG DAGRON_API_URL=http://localhost:8080
ENV DAGRON_API_URL=$DAGRON_API_URL
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

# ── Runtime: only the standalone server + static assets ─────────────────────────
FROM node:22-slim AS runner
WORKDIR /app
ENV NODE_ENV=production \
    NEXT_TELEMETRY_DISABLED=1 \
    PORT=3000 \
    HOSTNAME=0.0.0.0
# Next "standalone" output bundles a minimal node_modules + server.js.
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
EXPOSE 3000
# node:22-slim ships a non-root `node` user (uid 1000) — drop root before serving.
USER node
CMD ["node", "server.js"]
