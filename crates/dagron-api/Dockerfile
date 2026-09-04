# dagron-api — authenticated UI gateway. Depends on the sibling `dagron-logging`
# crate (path dep), so the build context is the module_54 root:
#   podman build -t dagron-api -f crates/dagron-api/Dockerfile .
# ── Console: the web UI, built as a fully static site and served by dagron-api ──
#
# The console used to ship as its own image (mancube/dagron-frontend, 281 MB of
# Node runtime) whose only jobs were serving files and proxying /api. It never
# needed a server: 47 of its 65 sources are "use client", it uses no server-only
# Next API, and every request goes through `BASE = "/api"` — a relative path. So
# it is exported (`output: "export"`) and handed to the Rust binary instead.
#
# Removing the proxy is not just a size win. Next's rewrites() buffers responses
# and never flushes headers for an infinite text/event-stream, so /api/events/stream
# hung and the console reported "Offline" against a healthy API. Same-origin here,
# the stream opens immediately.
FROM node:22-slim AS console
WORKDIR /web
ARG APP_VERSION=dev
ENV APP_VERSION=$APP_VERSION
ENV NEXT_TELEMETRY_DISABLED=1
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM rust:1.94-slim-bookworm AS builder
WORKDIR /app
# dagron-api builds standalone — its own crate, not via the module workspace. It
# only needs sibling path deps (dagron-logging, dagron-identity, dagron-crypto,
# dagron-artifact), so we deliberately do NOT copy the workspace root Cargo.toml
# (which would pull in the unrelated engine libs). The in-image layout mirrors
# crates/ so the path deps resolve.
#
# This list is load-bearing and silent when wrong: adding a path dep to
# dagron-api's Cargo.toml without adding it here fails the image build with
# "failed to read /app/crates/<dep>/Cargo.toml", and only at image-build time —
# `cargo build` on a dev box resolves it through the workspace and stays green.
# (dagron-crypto was the same miss in #671; dagron-artifact arrived with the
# artifact routes in #709/#714.)
COPY crates/dagron-logging/ ./crates/dagron-logging/
COPY crates/dagron-identity/ ./crates/dagron-identity/
COPY crates/dagron-crypto/ ./crates/dagron-crypto/
COPY crates/dagron-artifact/ ./crates/dagron-artifact/
COPY crates/dagron-core/ ./crates/dagron-core/
COPY crates/dagron-api/Cargo.toml ./crates/dagron-api/Cargo.toml
WORKDIR /app/crates/dagron-api
# Cargo features for the api binary. Empty = OSS. Pass `enterprise` for the
# enterprise build (unlocks the audit trail + flips the reported edition to
# "enterprise").
ARG CARGO_FEATURES=
# Compile dependencies against a stub main first so this layer is cached and a
# source-only change doesn't force a full dependency rebuild.
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release --bin dagron-api ${CARGO_FEATURES:+--features $CARGO_FEATURES}
COPY crates/dagron-api/src ./src
# Bust the stub's mtime so cargo rebuilds the real binary (deps stay cached).
RUN touch src/main.rs && cargo build --release --bin dagron-api ${CARGO_FEATURES:+--features $CARGO_FEATURES}

# ── Runtime: distroless/cc (glibc, no shell) ────────────────────────────────────
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /app/crates/dagron-api/target/release/dagron-api /usr/local/bin/dagron-api
# Served at / by the binary; override the location with DAGRON_CONSOLE_DIR.
# Absent = API-only, which is what a `cargo run` on a dev box gets.
COPY --from=console /web/out /usr/share/dagron/console
ENV PORT=8080
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/dagron-api"]
