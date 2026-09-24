# dagron-step-spark — the Spark submit step. A *task* binary: it starts a Spark
# job, prints the job's id, and exits, so the engine can park the task on that
# job instead of holding a worker for its duration.
# Context is the module root (matches the other OSS images):
#   podman build -t dagron-step-spark -f crates/dagron-step-spark/Dockerfile .
#
# Built WITH `--features k8s`, unlike the crate's default. The crate defaults to
# the lean HTTP/CLI backends because a source build vendoring this binary should
# not be made to carry the Kubernetes client; an *image* is the opposite case —
# one image that can submit to any backend beats three a user has to choose
# between.
#
# Normal use is to copy the binary into whatever image the task already runs:
#   COPY --from=mancube/dagron-step-spark:0.10 \
#        /usr/local/bin/dagron-step-spark /usr/local/bin/dagron-step-spark
FROM rust:1.94-slim-bookworm AS builder
WORKDIR /app/crates/dagron-step-spark
COPY crates/dagron-logging /app/crates/dagron-logging
COPY crates/dagron-step-spark/Cargo.toml ./Cargo.toml
RUN apt-get update -q && apt-get install -y -q --no-install-recommends \
      pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# Dependency layer against stubs, so a source-only change does not rebuild the
# Kubernetes client. lib + bin, so both stubs must exist.
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && : > src/lib.rs \
    && cargo build --release --features k8s --bin dagron-step-spark
COPY crates/dagron-step-spark/src ./src
RUN touch src/main.rs src/lib.rs \
    && cargo build --release --features k8s --bin dagron-step-spark

# ── Runtime: distroless/cc (glibc, no shell / package manager) ──────────────────
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /app/crates/dagron-step-spark/target/release/dagron-step-spark \
     /usr/local/bin/dagron-step-spark
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/dagron-step-spark"]
