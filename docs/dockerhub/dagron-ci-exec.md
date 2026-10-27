# dagron-ci step runner (`mancube/dagron-ci-exec`)

**What runs a dagron-ci job's steps inside the job's own image: per-step timeouts, cancellation, cleanup and a result, in an image that has nothing but `sh`.**

- **Image:** `mancube/dagron-ci-exec`: two static (musl) binaries, `dagron-ci-exec` and `dagron-ci-cache`, and a relocatable Node.js, on **scratch**. Runs as **nonroot** (uid 65532).
- **Arch:** `linux/amd64`, `linux/arm64`
- **Runtime:** **not a service.** No ports. Its entrypoint takes `install DIR`, and it runs as an init container.
- **Website:** dagron.dev · **Source / full docs:** github.com/lucheeseng827/dagron · Apache-2.0

## This is an init image, not a service

You do not run this image yourself. Point the dagron engine at it, and every
dagron-ci job starts with an init container from it:

```bash
DAGRON_CI_EXEC_IMAGE=docker.io/mancube/dagron-ci-exec:0.11.1
```

The init container runs `dagron-ci-exec install /dagron/bin` into a volume of
its own. The job's container mounts that volume read-only at `/dagron/bin`,
whatever image the job names: `alpine`, `debian`, a distroless image with
only `sh`. On Docker the init container also makes the job's workspace volume,
which starts open to every user (`0777`), so a job image that runs as any user
can write in it.

`compose.ci.yaml` sets this variable for you. On Kubernetes, the CI quickstart
(`docs/CI_QUICKSTART.md`) sets it through the `mancube/dagron` chart's
`engine.extraEnv`.

## What it adds over a plain `sh` script

A job runs without it too: dagron-ci then runs the steps as a POSIX `sh`
script, with the same markers, output and exit code. The binary adds what a
script cannot do in every image:

- **per-step timeouts**, without `timeout(1)` in the image;
- **cancellation**: a cancelled run's signal reaches the running step, which is
  killed if it has not stopped once the grace has passed, and no further step
  starts;
- **cleanup**: what a step leaves running is killed when the job ends;
- **a result**: each step's outcome, as the last log line and in a file.

Beside it:

- **`dagron-ci-cache`** restores and saves a job's path caches (`workspace.cache`).
- **Node.js** (`install --with node`), for a GitHub workflow's JavaScript
  action. It is Alpine's, with its own loader and libraries, so it runs in a job
  image of any libc: glibc, musl, or none. It is most of the image's size.

## Tags

| Tag | Meaning |
|---|---|
| `latest` | newest stable release |
| `0.11.1` | pinned version (= current `latest`) |
| `0.11` | floating minor: newest `0.11.x` |

First published with 0.11.0. Use the same version as your engine and
`mancube/dagron-ci`.

## Putting the binary in your own image

An image that carries the binary needs no init container for it. Put it at
`/dagron/bin/dagron-ci-exec`:

```dockerfile
FROM your-ci-image:1.2.3
COPY --from=mancube/dagron-ci-exec:0.11 \
     /usr/local/bin/dagron-ci-exec /dagron/bin/dagron-ci-exec
```

The release assets on GitHub also carry it as `dagron-ci-exec-<version>-<target>.tar.gz`
for `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl`.

## Related

- [`mancube/dagron-ci`](https://hub.docker.com/r/mancube/dagron-ci): the CI service that compiles a pipeline's `steps:` for this runner.
- [`mancube/dagron-engine`](https://hub.docker.com/r/mancube/dagron-engine): the engine that starts each job, and its init container.
