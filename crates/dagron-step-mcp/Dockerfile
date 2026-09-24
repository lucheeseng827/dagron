# dagron-step-mcp — the OSS MCP-tool step. A *task* binary: it runs as one DAG
# task, spawns an MCP server over stdio, calls one tool, and writes the result.
# Context is the module root (matches the other OSS images):
#   podman build -t dagron-step-mcp -f crates/dagron-step-mcp/Dockerfile .
#
# WHAT THIS IMAGE IS FOR. Unlike the engine, the API and dagron-mcp, this is not
# a service you deploy — a step runs inside whatever image its task already
# uses. So the normal way to consume it is to copy the binary into that image:
#
#   COPY --from=mancube/dagron-step-mcp:0.10 \
#        /usr/local/bin/dagron-step-mcp /usr/local/bin/dagron-step-mcp
#
# Running the image directly works too, for a task that needs nothing else:
#   command: ["dagron-step-mcp"]   # docs/MCP.md §"a DAG that drives an agent"
#
# It exists because docs/MCP.md tells readers to write exactly that `command:`,
# and until this file the binary shipped in no image and no release asset — the
# one thing the docs promise that nothing delivered.
FROM rust:1.94-slim-bookworm AS builder
WORKDIR /app/crates/dagron-step-mcp
# Its one path dep. `../dagron-logging` resolves from this WORKDIR, so both
# crate directories have to be present before any cargo invocation.
COPY crates/dagron-logging /app/crates/dagron-logging
COPY crates/dagron-step-mcp/Cargo.toml ./Cargo.toml
# Compile dependencies against stub sources first so this layer caches and a
# source-only change doesn't force a full dependency rebuild. The crate has a
# lib + a bin, so both stubs must exist — same shape as crates/dagron-mcp.
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && : > src/lib.rs \
    && cargo build --release --bin dagron-step-mcp
COPY crates/dagron-step-mcp/src ./src
# Bust the stub mtime so cargo rebuilds the real binary (deps stay cached).
RUN touch src/main.rs src/lib.rs && cargo build --release --bin dagron-step-mcp

# ── Runtime: distroless/cc (glibc, no shell / package manager) ──────────────────
#
# The step spawns an MCP server as a child process, so an image used *directly*
# must also carry that server. Most tasks will copy the binary into an image
# that already has one; this runtime is the minimum that runs the step itself.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /app/crates/dagron-step-mcp/target/release/dagron-step-mcp \
     /usr/local/bin/dagron-step-mcp
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/dagron-step-mcp"]
