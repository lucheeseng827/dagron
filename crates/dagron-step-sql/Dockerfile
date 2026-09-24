# dagron-step-sql — run one SQL statement as a task, with a bounded result
# contract (nothing to stdout in exec, one bounded value in scalar, NDJSON into
# $DAGRON_ARTIFACTS in rows).
# Context is the module root (matches the other OSS images):
#   podman build -t dagron-step-sql -f crates/dagron-step-sql/Dockerfile .
#
# Built with BOTH wire features, unlike the crate's default. The crate defaults
# to HTTP only (ClickHouse and Trino need no driver, and a wire driver is a large
# dependency for a task image that may never speak one); an image is the
# opposite trade — one image covering all three wires beats three images a user
# has to pick between.
#
# Normal use is to copy the binary into whatever image the task already runs:
#   COPY --from=mancube/dagron-step-sql:0.10 \
#        /usr/local/bin/dagron-step-sql /usr/local/bin/dagron-step-sql
FROM rust:1.94-slim-bookworm AS builder
WORKDIR /app/crates/dagron-step-sql
COPY crates/dagron-logging /app/crates/dagron-logging
COPY crates/dagron-step-sql/Cargo.toml ./Cargo.toml
RUN apt-get update -q && apt-get install -y -q --no-install-recommends \
      pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && : > src/lib.rs \
    && cargo build --release --features mysql,postgres --bin dagron-step-sql
COPY crates/dagron-step-sql/src ./src
RUN touch src/main.rs src/lib.rs \
    && cargo build --release --features mysql,postgres --bin dagron-step-sql

# ── Runtime: distroless/cc (glibc, no shell / package manager) ──────────────────
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /app/crates/dagron-step-sql/target/release/dagron-step-sql \
     /usr/local/bin/dagron-step-sql
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/dagron-step-sql"]
