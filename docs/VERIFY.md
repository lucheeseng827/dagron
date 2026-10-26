# Verifying a release

What a release publishes is signed by the workflow that built it: the
images, the Helm chart and the binaries. This page says how to check each,
and what a check does and does not tell you.

A release published before signing was added has none of this. Its
checksums file is all it has.

## How it is signed

With [cosign](https://docs.sigstore.dev/), and with no key. A release
workflow on GitHub is given a short-lived token that says which repository,
workflow and ref it is running for. Sigstore issues a certificate for that
identity, good for minutes, and the signature is made with it and recorded in
a public log. So there is no signing key for anyone to lose or to steal, and
what you check a signature against is an identity:

- the repository `lucheeseng827/dagron`, the public mirror every release is
  cut from;
- the workflow: `docker.yml` for the images and the chart, `binaries.yml`
  for the binaries;
- the ref: the release's tag, `refs/tags/vX.Y.Z`.

A release that was published again by hand, from a branch, is signed for
that branch's ref and not for the tag. The commands below then say so by
failing, and that is worth knowing.

Set these once. The version is the release's, with no `v`:

```sh
VERSION=X.Y.Z
ISSUER=https://token.actions.githubusercontent.com
REPO=https://github.com/lucheeseng827/dagron
```

## An image

```sh
cosign verify docker.io/mancube/dagron-engine:$VERSION \
  --certificate-identity "$REPO/.github/workflows/docker.yml@refs/tags/v$VERSION" \
  --certificate-oidc-issuer "$ISSUER"
```

It prints the digest that was signed. A signature is of a digest and never
of a tag: run what you verified by that digest
(`mancube/dagron-engine@sha256:…`), and a tag that is moved later cannot
change what you run.

Every image of a release is signed the same way: `dagron-engine`,
`dagron-engine-localdev`, `dagron-api`, `dagron-mcp`, `dagron-gitops`,
`dagron-ci`, `dagron-ci-exec` and the step images.

Each also carries an SBOM (SPDX) and SLSA provenance, made by BuildKit as
the image was built, for each platform:

```sh
docker buildx imagetools inspect mancube/dagron-engine:$VERSION \
  --format '{{ json .SBOM }}'
docker buildx imagetools inspect mancube/dagron-engine:$VERSION \
  --format '{{ json .Provenance }}'
```

They are part of what the signature covers: the signed digest is of the
index that holds each platform's image and its attestations.

## The chart

The chart is an OCI artifact beside the images, and is checked as one is:

```sh
cosign verify docker.io/mancube/dagron:$VERSION \
  --certificate-identity "$REPO/.github/workflows/docker.yml@refs/tags/v$VERSION" \
  --certificate-oidc-issuer "$ISSUER"
```

## A binary

A release's assets are the archives, `SHA256SUMS` over them, and beside
those:

| File | What it is |
|---|---|
| `SHA256SUMS.sigstore.json` | the signature of `SHA256SUMS` |
| `<file>.provenance.sigstore.json` | a signed statement of where `<file>` was built |
| `dagron-vX.Y.Z.cdx.json`, `dagron-ci-exec-vX.Y.Z.cdx.json` | an SBOM (CycloneDX) of each binary |

Check the checksums' signature, then the archive against the checksums:

```sh
cosign verify-blob SHA256SUMS --bundle SHA256SUMS.sigstore.json \
  --certificate-identity "$REPO/.github/workflows/binaries.yml@refs/tags/v$VERSION" \
  --certificate-oidc-issuer "$ISSUER"
sha256sum --check --ignore-missing SHA256SUMS
```

And, if you want it, where an archive was built:

```sh
FILE=dagron-v$VERSION-x86_64-unknown-linux-musl.tar.gz
cosign verify-blob-attestation "$FILE" \
  --bundle "$FILE.provenance.sigstore.json" --type slsaprovenance1 \
  --certificate-identity "$REPO/.github/workflows/binaries.yml@refs/tags/v$VERSION" \
  --certificate-oidc-issuer "$ISSUER"
```

## What a check tells you

- **That the artifact is the one that workflow published**, at that tag of
  that repository, and that nothing has changed it since.
- **Not that it is free of defects**, and not what went into the source at
  that tag: a signature says who built a thing, never what is in it. The
  SBOM is where to look for what it is made of.
- **An image's provenance is BuildKit's own account of its build.** A
  binary's is written by the workflow that built it. What vouches for
  either is the signing certificate, which names the repository, the
  workflow and the commit as GitHub issued them. Neither is a claim of a
  SLSA level.
- **A binary's SBOM is of its build on Linux**, from the lockfile the
  release was built with. A build for another platform links a few crates
  more or fewer.
- These can help you evidence where your software came from. They do not
  by themselves satisfy any standard.
