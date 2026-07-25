# Long scripts: where the code lives

A task's `command:` is argv, not a shell script file. dagron has **no `script:`
field and no file-include directive** — a spec never pulls in another file. So
"my script is 400 lines, I don't want it in the YAML" is really the question
*where does the code live so the executor can reach it*, and there are four
answers.

## What actually reaches a task

Everything else is context for the scheduler; only these cross into the process:

| | reaches the task? |
| --- | --- |
| `command:` | yes — argv |
| `env:` | yes — every backend (subprocess env, container env, pod env) |
| `docker_image:` | yes — Docker/Kubernetes backends |
| `resources:` / `service_account:` | Kubernetes only |
| `input:` | **no** — it is stored on the task row, not passed to the process |

And two constraints that decide most of this:

- **No executor mounts host paths.** The Docker backend creates containers with
  a default `HostConfig` — no binds; the Kubernetes backend declares no volumes.
  A file on the engine host is invisible to a `docker_image:` task.
- **`DAGRON_ARTIFACTS` is a host directory.** With `DAGRON_ARTIFACT_DIR` set, the
  engine gives every task in a run the same per-run directory, which is how host
  tasks pass files to each other. Containers don't get it mounted, so it can't
  carry a script into one.

## The four patterns

| | file | when | runs as-is? |
| --- | --- | --- | --- |
| 01 | [`01_image_baked.yaml`](01_image_baked.yaml) | docker/k8s — **the default answer** | needs your image |
| 02 | [`02_host_path.yaml`](02_host_path.yaml) | `EXECUTOR=local`, script already on the box | needs the file |
| 03 | [`03_fetch_at_runtime.yaml`](03_fetch_at_runtime.yaml) | script changes faster than you rebuild images | needs a real URL |
| 04 | [`04_env_script.yaml`](04_env_script.yaml) | tens of lines, no build pipeline | **yes** |

### 01 — bake it into the image

The script ships inside the container; `command:` is an entrypoint plus
arguments. The script's version *is* the image tag, so re-running an old run
against a pinned tag runs the code that ran then. Declare the image once with
`task_defaults.docker_image` and override per task where it differs.

### 02 — a path on the engine host

Local executor only. Use **absolute** paths: the executor sets no working
directory, so a relative path resolves against the engine process's cwd, not the
workflow's location. And every engine replica needs the file — any replica may
claim any task, so a script present on one box fails whenever another wins the
claim. If you're building an image to solve that, you have arrived at 01.

### 03 — fetch when the run starts

On the host executor ([`03_fetch_at_runtime.yaml`](03_fetch_at_runtime.yaml)) one
task downloads into the shared per-run `DAGRON_ARTIFACTS` directory and later
tasks execute it. Containers don't get that directory mounted, so the container
executor ([`03_fetch_in_container.yaml`](03_fetch_in_container.yaml)) makes each
task fetch for itself inside its own container — one runnable shape per executor,
since no single DAG runs coherently across both. Either way, pin an immutable
object (commit sha, versioned key): a moving URL is a run you cannot reproduce.

### 04 — the body in an env var

`command: ["sh", "-c", 'eval "$SCRIPT"']` with the script as a YAML block scalar.
No build, no host file, no network — and no shell-quoting hazard, since the body
travels as data rather than argv. Still inlined, though: the spec is re-parsed
every run, diffed on every GitOps sync, and submitted through an API that caps
bodies at 1 MiB. Good for tens of lines; past that use 01-03.

## Things that look like answers but aren't

- **YAML anchors/aliases** dedupe *within* one document. They don't pull in
  another file, and the console's visual editor expands them on round-trip.
- **`templates:`** ([`../templates/`](../templates/README.md)) is DAG reuse, not
  code reuse. It dedupes *steps* and parameterizes them with `arguments:`; it
  will not shorten a long `command:`.
- **GitOps sync** ingests files that have a `tasks:` key and stores them as
  workflows. It does not ship anything else in the repo to your executors.
- **`input:`** never reaches the process (see the table above).

If you want the script to live in its own file *in your repo*, assemble the spec
in CI or an SDK step ([`../sdk/`](../sdk/)) and submit the result — the spec is
just text, and dagron validates whatever it is handed.

## Validate before you ship

Every file here parses, expands and graph-validates through the same pipeline
each submit path uses:

```console
dagron validate examples/scripts        # --json for CI
```

Pattern 04 also runs end to end with no setup:

```console
dagron examples/scripts/04_env_script.yaml
```
