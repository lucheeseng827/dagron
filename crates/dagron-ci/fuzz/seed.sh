#!/usr/bin/env bash
# seed.sh: gives each fuzz target somewhere to start from. A pipeline and a
# workflow file start from the golden corpus's sources (tests/corpus/cases),
# and the rest from the few inputs in seeds/. What a run finds beyond these
# stays in corpus/, which is not kept in git.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cases=$here/../tests/corpus/cases
mkdir -p "$here/corpus/pipeline" "$here/corpus/workflow"
for case in "$cases"/*.yaml; do
  name=$(basename "$case" .yaml)
  to=pipeline
  grep -q '^front: github$' "$case" && to=workflow
  # The source is a block: the lines under `source: |`, two spaces in.
  awk '/^source: \|/ { on = 1; next } on && /^[^ ]/ { on = 0 } on { sub(/^  /, ""); print }' "$case" \
    > "$here/corpus/$to/$name"
done
for seeds in "$here"/seeds/*/; do
  target=$(basename "$seeds")
  mkdir -p "$here/corpus/$target"
  cp "$seeds"* "$here/corpus/$target/"
done
