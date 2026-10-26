#!/usr/bin/env bash
# run.sh [SECONDS] [TARGET...]: runs each fuzz target for SECONDS (60 unless
# said), every one unless some are named, and stops at the first that finds
# an input its parser does not survive. README.md says what is needed.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
secs=${1:-60}
shift || true
# A limit of 0 is no limit at all to libFuzzer: the run would never end.
case "$secs" in
  '' | *[!0-9]* | 0*)
    echo "run.sh: SECONDS is a whole number of at least 1, not '$secs'" >&2
    exit 2
    ;;
esac
cd "$here/.."
targets=("$@")
if [ ${#targets[@]} -eq 0 ]; then
  # Asked for in a statement of its own: a command that fails inside <( )
  # fails nothing, and a toolchain that is not there would list no target.
  listed=$(cargo +nightly fuzz list)
  mapfile -t targets <<<"$listed"
fi
if [ ${#targets[@]} -eq 0 ] || [ -z "${targets[0]}" ]; then
  echo "run.sh: there is no fuzz target to run" >&2
  exit 1
fi
bash "$here/seed.sh"
for target in "${targets[@]}"; do
  echo "== $target, for ${secs}s"
  cargo +nightly fuzz run "$target" -- -max_total_time="$secs" -max_len=65536
done
echo "== no target found an input its parser does not survive: ${targets[*]}"
