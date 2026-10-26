# dagron-ci — the CI service, as an image: it takes a forge's events (a GitHub
# webhook, a poll sweep, a dispatch through its run API), turns each into runs
# in dagron-api, and reports them back as commit statuses. Context is the
# module root (matches the other OSS images):
#   docker build -t dagron-ci -f crates/dagron-ci/Dockerfile .
#
# WHAT RUNS. The entrypoint is the binary, and with no argument it serves
# (`dagron-ci serve`): HTTP on CI_LISTEN (0.0.0.0:8090), /healthz, /readyz and
# /metrics beside the receivers. It needs CI_DATABASE_URL and nothing else to
# turn ready; crates/dagron-ci/README.md has every setting. Its other commands
# run from the same image, against the same database:
#   docker run --rm -e CI_DATABASE_URL=… mancube/dagron-ci repo add github.com/acme/widgets
#   kubectl exec deploy/dagron-ci -- dagron-ci repo list
#
# WHAT IS NOT IN IT. No git: dagron-ci speaks git's protocol itself, to read
# refs and to fetch a change's two commits for a paths filter. No shell and
# no package manager: the runtime is distroless. And no `dagron` engine, so
# `dagron-ci run`, which runs a checkout's pipelines on an engine of its own,
# is for the binary on a developer's machine, not for this image.
FROM rust:1.94-slim-bookworm AS builder
WORKDIR /app
# The workspace, so that it resolves; only dagron-ci is compiled (-p). Its
# two native dependencies (SQLite, bundled, and ring) build with the image's
# own gcc: no apt.
COPY Cargo.toml Cargo.lock ./
COPY src ./src/
COPY crates ./crates/
# The release this is built for, which the binary answers `--version` with. A
# release's build passes it, and any other build says `dev`. Declared here,
# just before the build, so that the layers above are reused across versions.
ARG APP_VERSION=dev
ENV APP_VERSION=$APP_VERSION
# The features the image is built with, as .github/workflows/docker.yml
# gives them (scripts/published-features.sh reads them from there): otel, as
# the engine's and dagron-api's images are.
ARG CI_FEATURES=
RUN cargo build --release --locked -p dagron-ci ${CI_FEATURES:+--features} ${CI_FEATURES} \
    && cp target/release/dagron-ci /dagron-ci

# ── Runtime: distroless/cc (glibc, CA roots, no shell / package manager) ──────
# The CA roots are what its HTTPS to the forge and to dagron-api is checked
# against (rustls, with the system's roots).
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /dagron-ci /usr/local/bin/dagron-ci
USER 65532:65532
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/dagron-ci"]
