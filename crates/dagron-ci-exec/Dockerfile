# dagron-ci-exec — the step runner of a dagron-ci task, as an image to install
# it from. Context is the module root (matches the other OSS images):
#   docker build -t dagron-ci-exec -f crates/dagron-ci-exec/Dockerfile .
#
# WHAT THIS IMAGE IS FOR. The engine installs the binary into each task that
# has a workspace from the image named by DAGRON_CI_EXEC_IMAGE: an init
# container runs this image's entrypoint as
#
#   dagron-ci-exec install /dagron/bin
#
# into a volume of its own, which the task then mounts read-only at
# /dagron/bin, where a compiled task looks for it first. The steps of a task
# in any image run under it that way. So the image is the binary and nothing
# else: static (musl), on scratch.
#
# And, beside it, the Node.js a GitHub workflow's JavaScript action runs on,
# installed with it into a task that asks for it (`install --with node`): a
# job that uses an action. It is Alpine's Node.js with its musl loader and
# its two C++ libraries beside it, its interpreter set to that loader where
# it is installed (/dagron/bin/node/lib) and its library path to its own
# lib, so that it runs in a job image of any libc, glibc, musl, or none.
# About 130 MB; the local test stack makes the same.
#
# To put the binary in an image of your own instead:
#   COPY --from=mancube/dagron-ci-exec:0.11 \
#        /usr/local/bin/dagron-ci-exec /usr/local/bin/dagron-ci-exec
FROM rust:1.94-slim-bookworm AS builder
WORKDIR /app
# The workspace, so that it resolves; only dagron-ci-exec is compiled (-p).
COPY Cargo.toml Cargo.lock ./
COPY src ./src/
COPY crates ./crates/
# The release this is built for, which the binary answers `--version` with. A
# release's build passes it, and any other build says `dev`. Declared here,
# just before the build, so that the layers above are reused across versions.
ARG APP_VERSION=dev
ENV APP_VERSION=$APP_VERSION
# musl, so the binary runs in a task image whatever its libc. Its two
# dependencies are pure Rust, so it links only the musl the target itself
# ships (`rustup target add`), with the image's gcc as the linker driver: no
# musl-tools, no apt. Each platform builds natively on its own runner
# (docker.yml), so the target is this one.
# musl-gcc builds dagron-ci-cache's C (zstd, and ring for TLS) for the same
# static target.
RUN apt-get update -q && apt-get install -y -q --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/* \
    && target="$(uname -m)-unknown-linux-musl" \
    && rustup target add "$target" \
    && cargo build --release --locked -p dagron-ci-exec -p dagron-ci-cache --target "$target" \
    && cp "target/$target/release/dagron-ci-exec" /dagron-ci-exec \
    && cp "target/$target/release/dagron-ci-cache" /dagron-ci-cache \
    && mkdir -p /seed/dagron/bin /seed/root/workspace \
    && chmod 0777 /seed/root/workspace

# The Node.js a JavaScript action runs on, made to run where it is
# installed (/dagron/bin/node) in an image of any libc.
FROM node:24.22.0-alpine AS node
RUN apk add --no-cache patchelf \
    && arch="$(uname -m)" && mkdir -p /out/bin /out/lib \
    && cp /usr/local/bin/node /out/bin/ \
    && cp /usr/lib/libstdc++.so.6 /usr/lib/libgcc_s.so.1 "/lib/ld-musl-$arch.so.1" /out/lib/ \
    && ln -s "ld-musl-$arch.so.1" "/out/lib/libc.musl-$arch.so.1" \
    && patchelf --set-interpreter "/dagron/bin/node/lib/ld-musl-$arch.so.1" \
        --set-rpath '$ORIGIN/../lib' /out/bin/node

FROM scratch
COPY --from=builder /dagron-ci-exec /usr/local/bin/dagron-ci-exec
# `install --with node` copies it into the task beside the step runner.
COPY --from=node /out/ /usr/local/bin/node/
# Beside it, so `install` copies it into the task too: restores and saves the
# task's path caches (`workspace.cache`).
COPY --from=builder /dagron-ci-cache /usr/local/bin/dagron-ci-cache
# Owned by the user the image runs as. A Docker volume first mounted over a
# directory the image has takes that directory's owner, and the init
# container's volume is mounted here: so `install`, as 65532, can write into
# it. (A Kubernetes emptyDir is writable by anyone already.)
COPY --from=builder --chown=65532:65532 /seed/dagron /dagron
# Writable by anyone: under Docker, the task's workspace is a volume this
# image's init container makes, so it starts as a copy of this directory,
# and the task's own image may run as any user. (On Kubernetes, an emptyDir
# is writable by anyone already, and a claim is given to the pod's fsGroup.)
# Copied as an entry of its parent: a directory copied itself lands 0755.
COPY --from=builder /seed/root/ /
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/dagron-ci-exec"]
