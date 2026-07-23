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
# App version shown in the sidebar brand — pass the release/image tag here. Baked
# into the client bundle at build (NEXT_PUBLIC_APP_VERSION); defaults to
# package.json version when unset.
ARG APP_VERSION
ENV APP_VERSION=$APP_VERSION
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

# ── Runtime: distroless node — only the standalone server + static assets ───────
# Distroless (gcr.io/distroless/nodejs22-debian12) ships the node runtime and
# nothing else: no shell, no package manager, no bundled global npm — which strips
# the OS/npm CVEs a full node:*-slim image carries (perl, util-linux-extra, and
# npm's own tar/sigstore/brace-expansion deps). The `:nonroot` tag runs as an
# unprivileged user (uid 65532). ENTRYPOINT is already `node`, so CMD is the script.
FROM gcr.io/distroless/nodejs22-debian12:nonroot AS runner
WORKDIR /app
ENV NODE_ENV=production \
    NEXT_TELEMETRY_DISABLED=1 \
    PORT=3000 \
    HOSTNAME=0.0.0.0
# Next "standalone" output bundles a minimal node_modules + server.js.
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
# public/ is NOT included in standalone output; copy it so the self-hosted
# Monaco runtime (public/monaco/vs, staged at build) ships and serves offline.
COPY --from=builder /app/public ./public
EXPOSE 3000
CMD ["server.js"]
