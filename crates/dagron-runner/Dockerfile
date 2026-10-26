# dagron-runner — dagron's remote runner, as an image. Context is the module
# root (matches the other OSS images):
#   docker build -t dagron-runner -f crates/dagron-runner/Dockerfile .
#
# WHAT THIS IMAGE IS FOR. The runner as a pod or a container:
#   - on Kubernetes, the in-cluster controller (DAGRON_RUNNER_EXECUTOR=k8s):
#     it starts each task as a pod of its own in K8S_NAMESPACE, through its
#     ServiceAccount. Built with `kubernetes`, so `dagron-runner scale`, the
#     autoscaler, is in it too;
#   - beside a Docker daemon (DAGRON_RUNNER_EXECUTOR=docker), with the daemon's
#     socket mounted at /var/run/docker.sock.
# A runner whose tasks are processes on its host (`local`) is the release
# binary on that host, not this image: a task would find none of its tools
# here.
FROM rust:1.94-slim-bookworm AS builder
WORKDIR /app
# The workspace, so that it resolves; only dagron-runner is compiled (-p).
COPY Cargo.toml Cargo.lock ./
COPY src ./src/
COPY crates ./crates/
ARG APP_VERSION=dev
ENV APP_VERSION=$APP_VERSION
# musl, so the binary is static: the image below has no libc to find. Each
# platform builds natively on its own runner, so the target is this one; musl
# for the C the build compiles (the bundled SQLite dagron-core links, and ring
# under rustls).
RUN apt-get update -q && apt-get install -y -q --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/* \
    && target="$(uname -m)-unknown-linux-musl" \
    && rustup target add "$target" \
    && cargo build --release --locked -p dagron-runner --features kubernetes --target "$target" \
    && cp "target/$target/release/dagron-runner" /dagron-runner \
    && mkdir -p /seed/state && chmod g=u /seed/state

# The CA bundle (rustls reads the system's roots) and nothing else.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /dagron-runner /usr/local/bin/dagron-runner
# Where it keeps the credential it registers for, unless one is given
# (DAGRON_RUNNER_TOKEN_FILE, a Secret, say). Group 0 with the owner's rights:
# OpenShift runs the image as an arbitrary uid in group 0.
# (--chmod: a directory COPY makes is 0755 whatever its source's mode.)
COPY --from=builder --chown=65532:0 --chmod=775 /seed/state /var/lib/dagron-runner
ENV DAGRON_RUNNER_STATE_DIR=/var/lib/dagron-runner
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/dagron-runner"]
CMD ["run"]
