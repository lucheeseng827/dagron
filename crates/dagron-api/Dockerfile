# dagron-api — authenticated UI gateway. Depends on the sibling `dagron-logging`
# crate (path dep), so the build context is the module_54 root:
#   podman build -t dagron-api -f crates/dagron-api/Dockerfile .
FROM rust:1.94-slim-bookworm AS builder
WORKDIR /app
# dagron-api builds standalone — its own crate, not via the module workspace. It
# only needs the sibling dagron-logging path dep, so we deliberately do NOT copy
# the workspace root Cargo.toml (which would pull in the unrelated engine libs).
# Both crates live under crates/; the in-image layout mirrors that so the
# `../dagron-logging` path dep resolves.
COPY crates/dagron-logging/ ./crates/dagron-logging/
COPY crates/dagron-api/Cargo.toml ./crates/dagron-api/Cargo.toml
WORKDIR /app/crates/dagron-api
# Compile dependencies against a stub main first so this layer is cached and a
# source-only change doesn't force a full dependency rebuild.
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release --bin dagron-api
COPY crates/dagron-api/src ./src
# Bust the stub's mtime so cargo rebuilds the real binary (deps stay cached).
RUN touch src/main.rs && cargo build --release --bin dagron-api

# ── Runtime: distroless/cc (glibc, no shell) ────────────────────────────────────
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /app/crates/dagron-api/target/release/dagron-api /usr/local/bin/dagron-api
ENV PORT=8080
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/dagron-api"]
