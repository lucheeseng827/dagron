# dagron-gitops — the GitOps worker. Built from the module_54 root (it uses
# sibling path deps):
#   podman build -t dagron-gitops -f crates/dagron-gitops/Dockerfile .
#
# This is the one dagron image that intentionally carries a `git` binary. It is
# also the one you only deploy if you use GitOps — which is the point of the
# split: dagron-api stays distroless and subprocess-free for the deployments
# (most of them) that never connect a repository.
FROM rust:1.94-slim-bookworm AS builder
WORKDIR /app
# Sibling path deps, mirroring crates/ so they resolve. Keep this list in sync
# with dagron-gitops/Cargo.toml — a missing entry fails only here, never on a dev
# box where the workspace resolves it (the trap that broke the api image).
COPY crates/dagron-logging/ ./crates/dagron-logging/
COPY crates/dagron-core/ ./crates/dagron-core/
COPY crates/dagron-gitops/Cargo.toml ./crates/dagron-gitops/Cargo.toml
WORKDIR /app/crates/dagron-gitops
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release --bin dagron-gitops
COPY crates/dagron-gitops/src ./src
RUN touch src/main.rs && cargo build --release --bin dagron-gitops

# ── Runtime: slim + git (no shell needed by us, but git wants one) ─────────────
# Not distroless, deliberately: this process execs `git`. Keeping that blast
# radius in an optional, non-internet-facing container is the whole design.
FROM debian:bookworm-slim
RUN apt-get update -q && apt-get install -y -q --no-install-recommends \
      git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# Runs as a non-root user: it clones untrusted repository content into a scratch
# dir, so it should never have been root.
RUN useradd --system --create-home --uid 10001 gitops
USER gitops
COPY --from=builder /app/crates/dagron-gitops/target/release/dagron-gitops /usr/local/bin/dagron-gitops
ENTRYPOINT ["/usr/local/bin/dagron-gitops"]
