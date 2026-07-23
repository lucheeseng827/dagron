# dagron-mcp — the OSS Model Context Protocol server. A stdio JSON-RPC binary an
# MCP client launches as a per-agent subprocess; it holds a bearer token and
# drives the dagron management API (submit/list/inspect/cancel/logs + metrics /
# dead-letters / run-events). No sibling path deps, so it builds standalone —
# context is the module root (matches the other OSS images):
#   podman build -t dagron-mcp -f crates/dagron-mcp/Dockerfile .
#
# This is the open server. The separate enterprise server adds a closed NL->DAG
# tool and ships from its own private pipeline; it is not built here.
FROM rust:1.94-slim-bookworm AS builder
WORKDIR /app/crates/dagron-mcp
COPY crates/dagron-mcp/Cargo.toml ./Cargo.toml
# Compile dependencies against stub sources first so this layer caches and a
# source-only change doesn't force a full dependency rebuild. The crate has a
# lib + a bin, so both stubs must exist.
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && : > src/lib.rs \
    && cargo build --release --bin dagron-mcp
COPY crates/dagron-mcp/src ./src
# Bust the stub mtime so cargo rebuilds the real binary (deps stay cached).
RUN touch src/main.rs src/lib.rs && cargo build --release --bin dagron-mcp

# ── Runtime: distroless/cc (glibc, no shell / package manager) ──────────────────
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /app/crates/dagron-mcp/target/release/dagron-mcp /usr/local/bin/dagron-mcp
USER 65532:65532
# stdio JSON-RPC: MCP protocol on stdout, logs on stderr.
ENTRYPOINT ["/usr/local/bin/dagron-mcp"]
