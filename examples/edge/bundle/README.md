# Signed workflow bundle — sign two specs, apply them, watch a tamper get refused

The smallest end-to-end walkthrough of [`docs/BUNDLES.md`](../../../docs/BUNDLES.md):
two ordinary workflow specs, a key, a signature, and the two ways a verified
bundle lands in the workflows table. Everything runs on a laptop with the
workspace checked out; the only binaries involved are the ones this repo builds.

```text
examples/edge/bundle/
├── README.md                 # this walkthrough
└── specs/
    ├── line_health.yaml      # a gateway self-check
    └── nightly_report.yaml   # a scheduled report
```

`manifest.json` and `manifest.sig` are **generated, not committed** — step 2
writes them. A committed signature would need a committed private seed to be
reproducible, and a seed anyone can read is a key nobody should trust.

## 1. Make a key

```bash
cargo run -p dagron-crypto --example bundle_sign -- --keygen
```

```text
# signing seed — keep this secret; it is the whole private key
9f1c…e2a0
# public key — safe to publish
4a7b…c913
# on every verifier (dagron-gitops, dagron-api, each unit):
export DAGRON_BUNDLE_PUBKEYS=4a7b…c913
```

Keep the seed out of shell history: paste it into a file and pass it as
`@seed.hex` below. Run the `export` line in every shell that will verify.

## 2. Sign the specs

```bash
cargo run -p dagron-crypto --example bundle_sign -- \
    examples/edge/bundle/specs @seed.hex --name plant-3/line-a --version 2026.09.01-1
```

```text
signed 2 spec(s) in examples/edge/bundle/specs
  line_health.yaml
  nightly_report.yaml
provenance: bundle:plant-3/line-a@2026.09.01-1#3d0c5a9e71b4
digest:     3d0c5a9e71b4…
```

Look at what was written: `specs/manifest.json` lists each spec by path and
SHA-256, and `specs/manifest.sig` is the base64 ed25519 signature over the
exact manifest bytes. The tool verified the directory back with the public key
before reporting success — it never leaves behind a bundle it would refuse.

The specs are still plain specs; lint them the way you would any other:

```bash
cargo run -p dagron -- validate examples/edge/bundle/specs/*.yaml
```

## 3. Apply it — through Git

Commit `specs/` (manifest and signature included) to a repository connected on
the console's GitOps page, with the repo's *path* pointing at that directory.
The `dagron-gitops` worker notices the `manifest.json` and switches from
file-by-file sync to all-or-nothing: it verifies against
`DAGRON_BUNDLE_PUBKEYS`, validates every spec through the engine's parser, and
writes both workflows in one transaction. The repo row's last message reads

```text
applied signed bundle bundle:plant-3/line-a@2026.09.01-1#3d0c5a9e71b4: 2 workflow(s) at 8f2c1e07
```

`DAGRON_BUNDLE_REQUIRE=1` on the worker makes it refuse any connected repo
that *lacks* a manifest — for deployments where "Git said so" is not enough.

## 4. Apply it — through the API

Same bundle, pushed instead of pulled. Build the body from the signed directory
(`jq` and `base64` are the only tools needed) and post it:

```bash
cd examples/edge/bundle/specs
jq -n \
  --arg manifest "$(base64 -w0 manifest.json)" \
  --arg signature "$(tr -d '\n' < manifest.sig)" \
  --arg a "$(base64 -w0 line_health.yaml)" \
  --arg b "$(base64 -w0 nightly_report.yaml)" \
  '{manifest_b64: $manifest, signature_b64: $signature,
    files: [{path: "line_health.yaml", content_b64: $a},
            {path: "nightly_report.yaml", content_b64: $b}]}' > /tmp/bundle.json

curl -sS -X POST http://localhost:8080/api/workflows/bundle \
     -H "Authorization: Bearer $DAGRON_TOKEN" -H 'Content-Type: application/json' \
     --data-binary @/tmp/bundle.json
```

```json
{
  "bundle": "plant-3/line-a",
  "version": "2026.09.01-1",
  "digest": "3d0c5a9e71b4…",
  "provenance": "bundle:plant-3/line-a@2026.09.01-1#3d0c5a9e71b4",
  "applied": [
    { "id": "…", "name": "line_health", "version": 1 },
    { "id": "…", "name": "nightly_report", "version": 1 }
  ]
}
```

`501` means dagron-api has no `DAGRON_BUNDLE_PUBKEYS`; `400` carries the
reason a bundle was refused. Nothing is written on either.

## 5. See where it came from

Either path stamps every workflow's new version with the provenance string:

```bash
curl -sS -H "Authorization: Bearer $DAGRON_TOKEN" \
     http://localhost:8080/api/workflows/<id>/versions | jq '.[0].created_by'
# "bundle:plant-3/line-a@2026.09.01-1#3d0c5a9e71b4"
```

One string, greppable, on every row a bundle touched — through Git or the API.

## 6. Break it on purpose

Edit `specs/line_health.yaml` after signing (add a task, change a command) and
apply again. Both paths refuse the **whole** bundle — `nightly_report` is not
re-applied either, although it is untouched:

```text
signed bundle rejected: sha256 mismatch for "line_health.yaml": manifest says …, file hashes to … — the spec was changed after signing
```

Drop an unlisted `extra.yaml` beside the manifest: refused (`not listed in
manifest.json`). Sign with a different key than the one exported: refused
(`matches none of the 1 trusted key(s)`). Re-sign (step 2) and it applies
again with a new digest, and each workflow gets version 2.

## Staging across a fleet

Sending `"cohort": "canary"` in the API body asks for a staged rollout —
canary first, then the rest, rolled back automatically if the canary's failure
rate crosses a bound. That is not in this build; this build answers
with a signpost and applies nothing. The signed bundle is the same file either
way: what is proven here on one deployment is what a fleet plane stages.
