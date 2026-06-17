# Pinned to -bookworm so the builder's glibc matches the distroless debian12
# runtime below (a trixie builder would link a newer glibc the runtime lacks).
FROM --platform=$BUILDPLATFORM rust:1.94-slim-bookworm AS builder
ARG TARGETARCH

# Cargo feature set for the dagron binary. Defaults to the sqlite + ops build
# (zero-infra single node). For the Kubernetes / operator path the engine must
# talk to Postgres and create task pods, so build with:
#   --build-arg FEATURES=postgres,ops,kubernetes
# sqlite and postgres are mutually exclusive (the db_target cfg blocks collide),
# hence --no-default-features below + an explicit set here.
ARG FEATURES=sqlite,ops

WORKDIR /app

# module_54 is a Cargo workspace; build `dagron` directly in it. The other members
# are copied so the workspace resolves — only `dagron` is compiled below (-p).
COPY Cargo.toml .
COPY Cargo.lock .
COPY src ./src/
# Workspace members live under crates/ — the dagron-engine lib (which `-p dagron`
# compiles) carries the reconcile loop, the ops modules, and openapi.yaml; core
# ships the migrations. dagron-api / -logging are copied so the workspace resolves.
COPY crates ./crates/

# C toolchain for the `kafka` feature: rdkafka vendors librdkafka from source and
# needs a C/C++ compiler + make + cmake + python3. No-op for non-kafka builds
# (builder stage only — the runtime image is unaffected).
RUN apt-get update -q && apt-get install -y -q --no-install-recommends \
      build-essential cmake python3 pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Cross-compilation toolchain for ARM64; no-op on amd64
RUN if [ "$TARGETARCH" = "arm64" ]; then \
      apt-get update -q && apt-get install -y -q gcc-aarch64-linux-gnu && \
      rustup target add aarch64-unknown-linux-gnu; \
    fi

RUN if [ "$TARGETARCH" = "arm64" ]; then \
      CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
      CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
        cargo build --release --target aarch64-unknown-linux-gnu -p dagron --no-default-features --features "$FEATURES" && \
        cp /app/target/aarch64-unknown-linux-gnu/release/dagron /dagron; \
    else \
      cargo build --release -p dagron --no-default-features --features "$FEATURES" && \
        cp /app/target/release/dagron /dagron; \
    fi

# An empty, non-root-owned /workflows to COPY into the shell-less runtime (we
# can't `mkdir`/`chown` there). The binary seeds it from the examples on start.
RUN mkdir -p /seed/workflows && chown -R 65532:65532 /seed

# ── Local-dev runtime: debian-slim. Has coreutils + /bin/sh, so example tasks
# whose command is `echo`/`sh -c ...` actually resolve (the distroless prod image
# has no such binaries). NOT for production — select with compose `target: localdev`.
FROM debian:bookworm-slim AS localdev
RUN apt-get update -q && apt-get install -y -q --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /dagron /usr/local/bin/dagron
COPY examples/ /etc/dagron/examples/
RUN mkdir -p /workflows && chown -R 65532:65532 /workflows
VOLUME /workflows
# Run as the same nonroot uid as the distroless prod image — good habit even in dev.
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/dagron"]

# ── Runtime: distroless/cc (glibc + libgcc + ca-certificates, no shell/pkg mgr) ─
# ~20 MB base vs ~80 MB for debian:slim. No shell, so the old docker-entrypoint.sh
# GitOps seeding now lives in the binary (seed_workflow_dir in main.rs). The
# `nonroot` tag runs as uid 65532 by default. This stays the LAST stage so the
# default build target (prod / CI release) remains the distroless image.
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

COPY --from=builder /dagron /usr/local/bin/dagron

# Bundle example workflows — the binary seeds /workflows from here on first start.
COPY examples/ /etc/dagron/examples/
# Pre-created, nonroot-owned GitOps volume mount point (distroless can't mkdir).
COPY --from=builder --chown=65532:65532 /seed/workflows /workflows

# /workflows is the GitOps-managed volume mount point. Override the path with the
# WORKFLOW_DIR env var (the binary seeds it from /etc/dagron/examples when empty).
VOLUME /workflows

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/dagron"]
