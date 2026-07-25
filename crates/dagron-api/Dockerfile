# dagron-api — authenticated UI gateway. Depends on the sibling `dagron-logging`
# crate (path dep), so the build context is the module_54 root:
#   podman build -t dagron-api -f crates/dagron-api/Dockerfile .
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
ENV PORT=8080
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/dagron-api"]
