#!/usr/bin/env bash
# published-features.sh: the cargo features each published image is built
# with, read from the workflow that builds the images.
#
#   published-features.sh engine   print the engine images' features
#   published-features.sh api      print dagron-api's
#   published-features.sh ci       print dagron-ci's
#   published-features.sh check    compile the binaries that way, and run the
#                                  tests those features gate
#
# The images are built by .github/workflows/docker.yml, on a release tag and
# nowhere else. A default build compiles none of what these features gate, so
# without `check` in CI a feature set that does not compile, or whose gated
# tests fail, is first found by the release that needs it.
#
# The workflow is the one place the sets are written. This reads them from
# there, so that what CI compiles is what the release will build.
set -euo pipefail
cd "$(dirname "$0")/.."
workflow=.github/workflows/docker.yml

# one NAME: the one value NAME is given among the images' build arguments.
# The engine is built twice (distroless, and with a shell for local tasks) and
# both name the set: two different values would be two engines under one tag.
one() {
  local found
  found="$(sed -n "s/^ *build_args: \"$1=\(.*\)\" *\$/\1/p" "$workflow" | tr -d '\r' | sort -u)"
  if [ -z "$found" ]; then
    echo "published-features: $workflow gives no image a $1" >&2
    exit 1
  fi
  if [ "$(printf '%s\n' "$found" | wc -l)" -ne 1 ]; then
    echo "published-features: $workflow gives $1 more than one value:" >&2
    printf '  %s\n' $found >&2
    exit 1
  fi
  printf '%s\n' "$found"
}

case "${1:-}" in
  engine) one FEATURES ;;
  api) one CARGO_FEATURES ;;
  ci) one CI_FEATURES ;;
  check)
    engine="$(one FEATURES)"
    api="$(one CARGO_FEATURES)"
    ci="$(one CI_FEATURES)"
    echo "== the engine images: $engine"
    echo "== the dagron-api image: $api"
    echo "== the dagron-ci image: $ci"
    # As the images build them.
    cargo check --locked -p dagron --no-default-features --features "$engine"
    cargo check --locked -p dagron-api --features "$api"
    cargo check --locked -p dagron-ci --features "$ci"
    # The tests those features gate. The engine library's run on its default
    # datastore: none of them needs the Postgres the images are built for, and
    # the workspace's own tests cover the datastores.
    gated="$(printf '%s\n' "$engine" | tr ',' '\n' | { grep -v -x -e postgres -e sqlite -e ops || true; } | paste -sd, -)"
    cargo test --locked -p dagron-engine ${gated:+--features "$gated"}
    cargo test --locked -p dagron-logging --features otel
    cargo test --locked -p dagron-artifact --features s3,gcs,azure
    cargo test --locked -p dagron-api --features "$api"
    ;;
  *)
    echo "usage: $0 engine | api | check" >&2
    exit 2
    ;;
esac
