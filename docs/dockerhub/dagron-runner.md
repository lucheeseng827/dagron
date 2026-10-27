# dagron remote runner (`mancube/dagron-runner`)

**Runs dagron tasks where no engine is: a GPU box, a team's own cluster, a machine behind NAT. It dials dagron-api over HTTPS, and nothing dials it.**

- **Image:** `mancube/dagron-runner`: a static Rust binary on **distroless/static**, running as **nonroot** (uid 65532), with no shell. Built with the Kubernetes executor.
- **Arch:** `linux/amd64`, `linux/arm64`
- **Runtime:** **no ports.** It needs only outbound HTTPS to dagron-api. Its credential is kept in `/var/lib/dagron-runner`, or in the file you name.
- **Holds:** no database credentials and no state of the run. A runner that dies loses only the tasks it was running, which an engine puts back to `ready`.
- **Website:** dagron.dev · **Source / full docs:** github.com/lucheeseng827/dagron · Apache-2.0

## What it does

A task's `runner_class` is its label. A runner registered for `gpu` takes the
ready tasks of class `gpu`, with the engine's own claim: the same priorities,
pools and fencing. It runs each as an engine would, with the same executors,
secret redaction, log and output caps, and timeouts, and reports how it came
out. An engine then applies that result: retries, `repeat:`, `defer:`, the
cache, dead letters.

```yaml
tasks:
  - name: train
    runner_class: gpu
    docker_image: pytorch/pytorch:2.4.0-cuda12.4-cudnn9-runtime
    command: ["python", "train.py"]
```

Give each engine the classes it serves (`RUNNER_CLASSES`). An engine with none
set serves every class, and would race your runners for theirs.

## Running it

1. **An admin mints a join token** for the labels the runner will serve. It is shown once:

   ```bash
   curl -b cookies.txt -X POST "$API/api/runners/join-tokens" \
     -H 'Content-Type: application/json' \
     -d '{"labels": ["gpu"], "max_uses": 1, "ttl_secs": 86400}'
   ```

2. **The runner starts with it**, registers, and keeps the credential it is
   given for its next start. On Kubernetes, one runner pod starts each task as
   a pod of its own (`DAGRON_RUNNER_EXECUTOR=k8s`). The source has a complete
   manifest, with its RBAC and a Secret for the credential
   (`deploy/runner/kubernetes.yaml`):

   ```yaml
   containers:
     - name: runner
       image: mancube/dagron-runner:0.11.0
       args: ["run"]
       env:
         - { name: DAGRON_RUNNER_URL, value: "https://dagron.example.com" }
         - { name: DAGRON_RUNNER_TOKEN_FILE, value: /var/run/dagron-runner/credential }
         - { name: DAGRON_RUNNER_LABELS, value: team-a }
         - { name: DAGRON_RUNNER_CAPACITY, value: "4" }
         - { name: DAGRON_RUNNER_EXECUTOR, value: k8s }
         - { name: K8S_NAMESPACE, valueFrom: { fieldRef: { fieldPath: metadata.namespace } } }
   ```

   To share one credential among many replicas, run `dagron-runner register`
   once. It writes the credential to `DAGRON_RUNNER_TOKEN_FILE` and exits, and
   each replica mounts it from a Secret.

A runner scaled away is sent SIGTERM and **drains**: it takes nothing more and
lets its tasks finish. Give it a `terminationGracePeriodSeconds` as long as
your longest task.

On a Mac, a Windows host or bare metal, run the binary instead of this image
(`DAGRON_RUNNER_EXECUTOR=local` or `docker`). The GitHub release carries it for
Linux x86_64 and arm64 (static), macOS arm64 and Windows x86_64.

## Settings

| Variable | Default | |
|---|---|---|
| `DAGRON_RUNNER_URL` | **required** | Where dagron-api is: `https://`, or `http://` to this machine |
| `DAGRON_RUNNER_JOIN_TOKEN_FILE` | — | The one-time join token, for the first start |
| `DAGRON_RUNNER_TOKEN_FILE` | `/var/lib/dagron-runner/credential` | The runner's credential: written by the first start, or given |
| `DAGRON_RUNNER_LABELS` | all it was registered for | The runner classes this process serves, comma-separated |
| `DAGRON_RUNNER_CAPACITY` | `1` | Tasks at once, 1 to 256 |
| `DAGRON_RUNNER_EXECUTOR` | `docker` | `local`, `docker` or `k8s` |
| `DAGRON_RUNNER_SECRETS` | `server` | `server`: dagron-api sends a task's secrets with it. `home`: dagron-api sends only their names, and the runner reads the values from its own environment (`DAGRON_SECRET_<NAME>`) or `DAGRON_SECRETS_DIR` |
| `DAGRON_RUNNER_NAME` | the host's name | How it is listed |

The executor is configured by the engine's own variables (`K8S_IMAGE`,
`K8S_NAMESPACE`, `DOCKER_IMAGE`, `DAGRON_CI_EXEC_IMAGE`, …). The full list is
in `crates/dagron-runner/README.md` in the source.

## Tags

| Tag | Meaning |
|---|---|
| `latest` | newest stable release |
| `0.11.0` | pinned version (= current `latest`) |
| `0.11` | floating minor: newest `0.11.x` |

First published with 0.11.0. Upgrade dagron-api before its runners: an older
dagron-api refuses the log segments that carry the end of a long task's log.
