# dagron-gitops — the GitOps worker. Built from the module_54 root (it uses
# sibling path deps):
#   podman build -t dagron-gitops -f crates/dagron-gitops/Dockerfile .
#
# This is the one dagron image that intentionally carries a `git` binary. It is
# also the one you only deploy if you use GitOps — which is the point of the
# split: dagron-api stays distroless and subprocess-free for the deployments
# (most of them) that never connect a repository.
#
# Alpine, not debian-slim, and the reason is `git`'s dependency list rather than
# image size. On Debian `git` hard-Depends on `perl` and `liberror-perl`, and
# debian:bookworm-slim ships `perl-base` before you install anything at all.
# That dragged four perl packages into this image carrying CVE-2026-57433,
# CVE-2026-8376, CVE-2026-13221 and CVE-2026-42496 — all CRITICAL, and all with
# an empty "fixed in" column, so no base bump or `apt-get upgrade` could clear
# them. Alpine's git depends only on musl, libcurl, libexpat, libpcre2 and libz:
# there is no perl in the image to be vulnerable. Its zlib is 1.3.2, also past
# the minizip CVE-2023-45853 that Debian's 1.2.13 reports.
#
# Keep this runtime perl-free. A package that pulls perl back in silently
# restores every one of those criticals.
FROM rust:1.94-alpine AS builder
# The C toolchain rustc links through. Absent on rust:alpine, and its absence
# fails at link time, not at `cargo build` start.
RUN apk add --no-cache musl-dev
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

# ── Runtime: alpine + git (no shell needed by us, but git wants one) ──────────
# Not distroless, deliberately: this process execs `git`. Keeping that blast
# radius in an optional, non-internet-facing container is the whole design.
FROM alpine:3.21
RUN apk add --no-cache git ca-certificates
# Runs as a non-root user: it clones untrusted repository content into a scratch
# dir, so it should never have been root.
RUN adduser -S -h /home/gitops -u 10001 -D gitops
USER gitops
COPY --from=builder /app/crates/dagron-gitops/target/release/dagron-gitops /usr/local/bin/dagron-gitops
ENTRYPOINT ["/usr/local/bin/dagron-gitops"]
