# Signed workflow bundles — over-the-air updates for the workflow layer

A **bundle** is a directory of ordinary workflow specs plus a signed manifest
that names each spec by path and SHA-256. Anything that applies a bundle — the
`dagron-gitops` worker pulling from a repository, `POST /api/workflows/bundle`
being pushed to, a unit receiving work from a fleet plane — verifies the
signature against the keys it was told to trust and then applies **all of the
specs in one transaction, or none of them**. The result is a workflow layer
that can be updated remotely with the same confidence as a signed firmware
image: what runs is what was signed, and a definition that was altered in
transit, in a repository, or on disk is refused rather than run.

Signature **verification ships in every build**. Paywalling it would make the
open build's default "execute unsigned remote definitions", which is not a
security default worth shipping. Staged rollout of a bundle across a fleet —
cohorts, canaries, automatic rollback — is not in this build; the seam is here and
consumes this same primitive; see [Staged rollout](#staged-rollout-cohorts).

The runnable walkthrough is [`examples/edge/bundle/`](../examples/edge/bundle/).

## Layout

```text
<bundle dir>/
├── manifest.json        # the signed statement: format, name, version, specs[]
├── manifest.sig         # base64 of a 64-byte ed25519 signature over manifest.json's exact bytes
├── line_health.yaml     # listed in the manifest with its sha256
└── nightly_report.yaml  # listed in the manifest with its sha256
```

`manifest.json`:

```json
{
  "format": "dagron.bundle.v1",
  "name": "plant-3/line-a",
  "version": "2026.09.01-1",
  "created_at": "2026-09-01T10:15:00Z",
  "key_id": "ops-2026",
  "specs": [
    { "path": "line_health.yaml",    "sha256": "5b0e…f3a1" },
    { "path": "nightly_report.yaml", "sha256": "c19d…07be" }
  ]
}
```

| Field | Meaning |
| --- | --- |
| `format` | Must be exactly `dagron.bundle.v1`. Anything else is refused — a future format is a new verifier, never a guess. |
| `name` | The bundle's name, typically a workflow namespace (`plant-3/line-a`). Non-empty. |
| `version` | Free-form, non-empty. Recorded as provenance; a sortable timestamp or a release tag both work. |
| `created_at` | RFC 3339. Informational only — a unit's clock may be wrong, so nothing is decided by it. |
| `key_id` | Optional. Names the signing key for operators who rotate keys; the verifier does not use it. |
| `specs[]` | Every spec, by manifest-relative path and lowercase hex SHA-256 of its bytes. Non-empty; no duplicates. |

The signature is over the **exact bytes** of `manifest.json` — there is no
canonical-JSON step to get wrong on either side. The specs are bound to the
manifest by content hash, so the one signature covers them all.

## What verification checks (and refuses)

Every check below is fatal, and a fatal check applies nothing. In order:

1. **Trust set.** No keys configured is an error, never "trust all". A
   signature is checked against the raw manifest bytes before the manifest is
   interpreted at all; it must verify under at least one configured key.
2. **Manifest.** `format` is `dagron.bundle.v1`; `name` and `version` are
   non-empty; `specs` is non-empty with no duplicate path.
3. **Paths.** Each spec path is relative, made only of plain components (no
   `.`/`..`, no root, no drive prefix, no backslash) and ends in `.yaml` or
   `.yml` — the same filter the plain GitOps sync applies, so a bundle cannot
   smuggle in a file that path would never have read.
4. **File set.** The files present are *exactly* the files listed: a listed
   file that is missing, an unlisted `*.yaml`/`*.yml` beside the manifest, and
   any file whose SHA-256 differs from the manifest are each refused. Non-spec
   files (a README, a `.gitignore`) and hidden directories are ignored.
5. **Filesystem.** When read from a directory, symlinks are refused at every
   level — the bundle directory itself, any directory on the way to a spec,
   and the spec — so a checkout cannot point a listed name outside itself.
6. **Specs.** Each spec must pass the same validation the console applies on
   create and update (the engine's parser for the GitOps worker,
   `parse_and_validate` for the API). Two files defining one workflow name are
   refused: applying both would silently overwrite one with the other.

A refused bundle leaves the datastore exactly as it was, and the message names
the file and the check.

## Keys

| Variable | Where | Value |
| --- | --- | --- |
| `DAGRON_BUNDLE_PUBKEYS` | `dagron-gitops`, `dagron-api`, and the engine on a unit that receives bundles | Comma-separated ed25519 public keys, each 32 bytes as 64 hex characters or standard base64. Unset = every bundle is refused (the API answers `501`; the worker records the error on the repo row). |
| `DAGRON_BUNDLE_REQUIRE` | `dagron-gitops` | `1`/`true`: refuse connected repositories that carry no `manifest.json` under their path. Default off — plain repositories keep syncing file by file. |

**Rotation.** List the old and the new key together, re-sign bundles with the
new key, then drop the old one. `key_id` in the manifest tells an operator
which key a given bundle was signed with; the verifier tries every listed key.

**Trust is per verifier.** The worker, the API and each unit each read their
own environment. A bundle accepted by the API can still be refused by a unit
that was given a different key list — by design: a unit's trust set is the
last word on what runs on it.

## Signing

`bundle_sign` (an example binary in `dagron-crypto`) does keygen and signing;
it is small enough to read and copy into a release pipeline.

```bash
# 1. a key: prints the seed (secret), the public key, and the export line
cargo run -p dagron-crypto --example bundle_sign -- --keygen

# 2. sign a directory in place: writes manifest.json + manifest.sig, then
#    verifies the directory back with the derived public key
cargo run -p dagron-crypto --example bundle_sign -- \
    path/to/specs @seed.hex --name plant-3/line-a --version 2026.09.01-1 [--key-id ops-2026]
```

The seed is the whole private key: 32 random bytes from the OS CSPRNG, given
as 64 hex characters, or as `@path` to read from a file rather than the
command line. Without `--version` the version is a UTC timestamp
(`20260901T101500Z`); without `--name` the directory's name is used.

Signing elsewhere (a CI job, an HSM-backed signer) needs only: ed25519 over the
exact manifest bytes, the signature written as standard base64 in
`manifest.sig`, hashes as lowercase hex SHA-256.

## Applying a bundle

### Through Git (`dagron-gitops`)

Connect the repository as usual (console → GitOps, or `POST /api/git-repos`)
with its *path* pointing at the bundle directory. The presence of
`manifest.json` under that path is the switch:

| Path contains | Behaviour |
| --- | --- |
| no `manifest.json` | **Plain sync**, unchanged: every `*.yaml`/`*.yml` is validated and upserted on its own; one bad file is a warning beside the ones that synced. Refused outright when `DAGRON_BUNDLE_REQUIRE=1`. |
| `manifest.json` | **Bundle sync**: verified against `DAGRON_BUNDLE_PUBKEYS`, every spec validated, then all applied in one transaction — a version row per workflow, `created_by` set to the bundle's provenance. Any failure: nothing applied, the reason on the repo row (`Error`). |

The repo row's last message names what is live:

```text
applied signed bundle bundle:plant-3/line-a@2026.09.01-1#3d0c5a9e71b4: 2 workflow(s) at 8f2c1e07
```

Presence, not validity, is what switches modes. A directory that *claims* to
be signed and gets it wrong is refused, never quietly synced file by file with
the manifest treated as one more YAML.

### Through the API (`POST /api/workflows/bundle`)

```json
{
  "manifest_b64":  "<base64 of manifest.json>",
  "signature_b64": "<contents of manifest.sig>",
  "files": [
    { "path": "line_health.yaml",    "content_b64": "<base64>" },
    { "path": "nightly_report.yaml", "content_b64": "<base64>" }
  ],
  "cohort": "canary"
}
```

`cohort` is optional — see [Staged rollout](#staged-rollout-cohorts). Response
`200`:

```json
{
  "bundle": "plant-3/line-a",
  "version": "2026.09.01-1",
  "digest": "3d0c5a9e71b4…",
  "provenance": "bundle:plant-3/line-a@2026.09.01-1#3d0c5a9e71b4",
  "applied": [
    { "id": "…", "name": "line_health",    "version": 1 },
    { "id": "…", "name": "nightly_report", "version": 1 }
  ]
}
```

| Status | Meaning |
| --- | --- |
| `200` | Applied. Every workflow in `applied` moved to the bundle's definition in one transaction, each with a new `workflow_versions` row. |
| `400` | Refused: bad base64, signature, format, hash, file set, path, a spec the validator rejects, a duplicate workflow name — or `cohort` present in a build that cannot stage. The body says which. Nothing written. |
| `401` | Not authenticated (cookie or bearer token, as every `/api` route). |
| `413` | Body over the core **1 MiB** cap this route shares with every other core route. Base64 costs a third, so roughly 750 KiB of YAML is the most one bundle can carry — bundles are definitions, not data. |
| `501` | `DAGRON_BUNDLE_PUBKEYS` is not set on dagron-api. Nothing is accepted unsigned. |

The keys are read once at startup, like every other dagron-api knob. A
malformed key list is a startup-time error worth a log line and a `500`, not a
silent "trust nothing and carry on".

## Provenance

Both paths stamp `workflow_versions.created_by` with

```text
bundle:<name>@<version>#<first 12 hex characters of the manifest digest>
```

(`GET /api/workflows/{id}/versions` shows it, newest first.) The digest is the
SHA-256 of the exact manifest bytes, so two bundles with the same name and
version but different content — a re-sign after a fix — get different
provenance, and `#3d0c5a9e71b4` in a history row is enough to find the exact
manifest that put a definition there. The same string is what a unit reports
back when a fleet plane hands it a bundle.

## Staged rollout (cohorts)

`"cohort": "<name>"` in the API body asks that the bundle be staged across a
fleet — canary first, then the rest, rolled back automatically if the canary's
failure rate crosses a bound — instead of applied here. Staged rollout is not
in this build.
This build answers `400` with a signpost saying so and applies **nothing**:
it applies a bundle to this deployment immediately when `cohort` is absent,
and a caller who asked to stage must not find it silently applied to
everything instead.

The bundle format, the keys and the verifier are identical on both sides of
that line. A bundle proven on one deployment through this document is the
file a fleet plane stages.

## Troubleshooting

| Message | Cause |
| --- | --- |
| `DAGRON_BUNDLE_PUBKEYS is not set …` / API `501` | The verifier has no trust set. Export the public key(s) on that process. |
| `manifest signature matches none of the N trusted key(s)` | Signed with a key that is not listed, or `manifest.json` changed after signing (even whitespace). |
| `sha256 mismatch for "x.yaml"` | The spec was edited after signing. Re-sign. |
| `"x.yaml" is not listed in manifest.json` | A spec was added beside the manifest without re-signing. Re-sign or remove it. |
| `manifest lists "x.yaml" but the file is missing` | A listed spec was deleted or renamed. |
| `… verified but N spec(s) failed validation, nothing applied: x.yaml: …` | The signature is fine; a spec is something the engine cannot run. Every failing file is listed. |
| `… workflow name 'n' is already defined by a.yaml` | Two files in the bundle carry the same `name:`. |
| `'path' carries no manifest.json and DAGRON_BUNDLE_REQUIRE is set` | The worker only accepts signed bundles and this repository is plain. |
| `… is a symlink — refusing` | A symlink on the way to the manifest or a spec. Bundles are plain files only. |

## Reference

- `DAGRON_BUNDLE_PUBKEYS`, `DAGRON_BUNDLE_REQUIRE` — [`CONFIG.md`](CONFIG.md).
- `POST /api/workflows/bundle`, `GET /api/workflows/{id}/versions` — [`API.md`](API.md).
- Deploying the `dagron-gitops` worker — [`OPERATIONS.md`](OPERATIONS.md).
