# dagron Spark step (`mancube/dagron-step-spark`)

**A dagron task that submits a Spark job and hands its id back — so a six-hour run costs one database row, not a worker slot.**

- **Image:** `mancube/dagron-step-spark` — a Rust binary on **distroless/cc** (no shell, no package manager), runs as **nonroot** (uid 65532).
- **Arch:** `linux/amd64`, `linux/arm64`
- **Runtime:** submits and exits · **no ports** · built with the `k8s` feature, so all three backends are present
- **Website:** dagron.dev · **Source / full docs:** github.com/lucheeseng827/dagron · Apache-2.0

## This is a step, not a service

The task's `command` is the **submit**, and nothing else. This binary starts a Spark job, prints `dagron::handle=<id>`, and exits 0. The engine parks the task row on that id, and a reconcile sweep polls it — so the job runs for as long as it runs while holding no worker, and if every scheduler dies mid-job the row is untouched and whichever replica comes back resumes the poll.

You usually do **not** deploy this image. You copy the binary into whatever image your task already uses:

```dockerfile
FROM your-task-image:1.2.3
COPY --from=mancube/dagron-step-spark:0.10 \
     /usr/local/bin/dagron-step-spark /usr/local/bin/dagron-step-spark
```

Running the image directly works for a task that needs nothing else.

## The name is the idempotency

The dangerous window is: the cluster accepts the job, and the engine dies before the park commits. A naive retry starts a second 200-node cluster.

So the job is named `dagron-<DAGRON_TASK_ID>-<DAGRON_EXTERNAL_EPOCH>`, both injected by the engine at dispatch. The task id is stable across lease recovery, so a retried submit reuses the name, the cluster answers AlreadyExists, and this step **adopts** the running job rather than starting another — and only if that job is still non-terminal, since adopting a failed one would park the task on a corpse.

**This is a real guarantee on `k8s` only.** There `AlreadyExists` is the apiserver's own enforcement on the object name. On `rest` it is only as strong as the body you write — `{{ name }}` must land in a field your vendor treats as an idempotency key, and the step cannot check that without knowing the vendor's schema. On `spark-submit` there is none at all: `--name` is just `spark.app.name`. Use `k8s` where a crash during submit must not double-spend a cluster.

## Backends

| `SPARK_BACKEND` | What it does |
|---|---|
| `k8s` (default — and this image is built with it, so it works here) | creates a `SparkApplication` CR — vendor-free, no account needed, and AlreadyExists is the API's own semantics |
| `rest` | POSTs a body you supply to a URL you supply, and reads the id out of the response by path — one adapter for Databricks, EMR Serverless, Dataproc, Livy, Kyuubi |
| `spark-submit` | spawns the CLI; needs a version-matched Spark distribution in your task image |

Vendor names (`livy`, `databricks`, `emr`, `dataproc`, `kyuubi`) are **refused with a pointer at `rest`** rather than silently accepted — they all submit over HTTP like everything else.

## Configure it

Entirely by environment, like every other dagron step.

| Variable | Meaning |
|---|---|
| `SPARK_APP` | application URI to run (or `--app`, which wins) |
| `SPARK_BACKEND` | `k8s` (default) · `rest` · `spark-submit` |
| `SPARK_WAIT` | `defer` (default — print the handle and exit) · `inline` (block until the job finishes; **`spark-submit` only** — refused on `k8s` and `rest`, which submit and return) |
| `SPARK_NAMESPACE` | Kubernetes namespace (default `default`) |
| `SPARK_IMAGE` | Spark image for the CR (default `spark:3.5.3`) |
| `SPARK_DRIVER_SERVICE_ACCOUNT` | ServiceAccount the driver pod runs as (`k8s`; default: the operator's, i.e. the namespace's `default`, which usually cannot create executor pods). **Use this, not `SPARK_CONF_*`** — that mapping lowercases every key and Spark's are case-sensitive, so `spark.kubernetes.authenticate.driver.serviceAccountName` cannot be written as an env var |
| `SPARK_CONF_*` | extra `sparkConf` / `--conf` — `SPARK_CONF_SPARK_EXECUTOR_INSTANCES=4` → `spark.executor.instances=4` |
| `SPARK_MASTER`, `SPARK_SUBMIT_BIN` | for `spark-submit` |
| `SPARK_REST_URL`, `SPARK_REST_BODY`(`_FILE`), `SPARK_REST_HANDLE_PATH`, `SPARK_REST_HEADER_*` | for `rest` |

## Use it in a DAG

```yaml
tasks:
  - name: rollup
    docker_image: your-task-image:1.2.3   # with the binary COPY'd in
    command: ["dagron-step-spark", "submit", "--app", "s3://jobs/daily_rollup.py"]
    timeout_secs: 300                      # bounds the SUBMIT
    pool: spark                            # for a deferred task, caps concurrent REMOTE JOBS
    env:
      - { name: SPARK_BACKEND,   value: k8s }
      - { name: SPARK_NAMESPACE, value: data }
      - { name: SPARK_CONF_SPARK_EXECUTOR_INSTANCES, value: "8" }
    defer:
      kind: spark-k8s
      poll_secs: 30
      max_wait_secs: 21600                 # bounds the JOB
    produces: ["clickhouse://analytics/marts/daily_rollup/{{ ds }}"]
```

`timeout_secs` bounds the submit; `defer.max_wait_secs` bounds the job. Conflating the two is the mistake the split exists to prevent — the executor's default `timeout_secs` is **25 seconds**.

`SPARK_WAIT=inline` only means anything on `spark-submit`, where the CLI genuinely blocks. It is **refused** on `k8s` and `rest`: those submit and return, so "inline" there would succeed the task the moment the job was accepted. Where it is legal, it still needs a generous explicit `timeout_secs:` or the task is killed long before any real job ends.

`produces:` on a deferred task is recorded when the **remote** job finishes, in the reconcile sweep — not when the submit returns. Downstream consumers unblock at the right moment rather than six hours early.

## Tags

| Tag | Notes |
|---|---|
| `latest` | newest release |
| `0.10` | floating minor — newest `0.10.x` |

Pin in production — pick the newest published tag rather than copying a version from this page, which ages.

> First published in **0.10.0**, with `defer:` itself.

## See also

- **`docs/EXTERNAL_JOBS.md`** — `defer:`, the park shape, and the poll sweep
- **`mancube/dagron-step-sql`** — the other half of the warehouse pair
