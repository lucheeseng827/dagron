# dagron SQL step (`mancube/dagron-step-sql`)

**A dagron task that runs one statement against an analytical store — with a result budget that refuses before anything is written, not after the rows are already in your task's output.**

- **Image:** `mancube/dagron-step-sql` — a Rust binary on **distroless/cc** (no shell, no package manager), runs as **nonroot** (uid 65532).
- **Arch:** `linux/amd64`, `linux/arm64`
- **Runtime:** runs one statement and exits · **no ports** · built with the `mysql` and `postgres` features, so every transport is present
- **Website:** dagron.dev · **Source / full docs:** github.com/lucheeseng827/dagron · Apache-2.0

## This is a step, not a service

You usually do **not** deploy this image. You copy the binary into whatever image your task already uses:

```dockerfile
FROM your-task-image:1.2.3
COPY --from=mancube/dagron-step-sql:0.11 \
     /usr/local/bin/dagron-step-sql /usr/local/bin/dagron-step-sql
```

Running the image directly works for a task that needs nothing else.

## Why not `sh -c "clickhouse-client …"`

That already works, and dagron's docs say so. Two things it cannot do:

1. **A bounded result contract**, instead of `SELECT *` into an uncapped column.
2. One image, instead of N vendor CLIs baked into N task images.

Two more things a binary here *could* do — a cancel hook (`KILL QUERY`) and a defer handle — are **not built**. On Snowflake, BigQuery and Databricks the step holds the statement's handle and cancels it at its own deadline (`SQL_TIMEOUT_SECS`), but not when dagron cancels the run. This step never prints `dagron::handle=`, so a long query holds its worker for its duration; `mancube/dagron-step-spark` is the deferred one.

## The output contract is the point

dagron streams live logs unconditionally, appending each stdout line to the task's `output` column with **no cap anywhere on that path**. So a `SELECT *` printing rows to stdout is an unbounded write into the datastore, and a check applied *after* the rows are printed is a check applied after the damage.

Rows therefore never go to stdout, and the budget is checked **before each row is written**, so an over-budget statement writes nothing.

It bounds the read too. The ClickHouse path parses NDJSON off the response as it arrives, and the wire path fetches rows one at a time, so a `SELECT *` over a huge table is refused with `SQL_MAX_ROWS` or `SQL_MAX_BYTES` named, rather than OOM-killing the step before the refusal can print. Resident memory is a row at a time, whatever the budget is set to. The statement APIs answer a page at a time, so there it is one page, refused while still arriving once it passes twice `SQL_MAX_BYTES` plus 1 MiB. A `LIMIT` in the statement is still the better way to ask for less data; it is no longer what stands between a careless `SELECT *` and an unexplained exit code.

| `SQL_MODE` | What it writes |
|---|---|
| `exec` | **nothing to stdout** — the outcome is logged, and what the log carries differs by transport, so it is not something to gate on. For INSERT / MERGE / DDL |
| `scalar` | exactly one row, one column, to stdout — so a downstream `when:` can gate on it, and both are **enforced**: a multi-row result is refused rather than silently reduced to its first row. Bounded by a 1 KiB scalar limit, **not** by `SQL_MAX_BYTES`: this is the one mode that writes to the uncapped column on purpose, so it gets the stricter one |
| `rows` | NDJSON into `$DAGRON_ARTIFACTS`. Never stdout |
| `script` | several statements in order, nothing to stdout. Not on ClickHouse, whose HTTP interface takes one statement per request |

## Stores

Named after what people search for; implemented against what actually varies. Maintenance is bounded by **protocol count, not vendor count** — the reason Airflow deprecated its per-store operators in favour of one generic one plus a connection.

| Transport | `SQL_ENGINE` |
|---|---|
| HTTP | `clickhouse` |
| MySQL wire | `starrocks`, `doris`, `mysql` |
| Postgres wire | `postgres`, `redshift` — and anything else speaking it (Materialize, CockroachDB, Greenplum) |
| Statement API (HTTPS: submit, poll, page, cancel at the deadline) | `snowflake`, `bigquery`, `databricks` — three vendor protocols with one shape, counted as three |
| Child process | `duckdb` — the DuckDB CLI, `exec` and `script` only |

`duckdb` is the fifth transport: the DuckDB CLI as a child process, the engine a copy
uses where a store cannot read or write Parquet (Postgres on-prem). It runs `exec` and
`script` only. **This image does not carry the DuckDB CLI**: a copy task's image needs
both binaries.

`dagron-step-sql parity` compares one relation's rows on two stores: row count and an
order-independent checksum over each value's canonical text, per side, computed where
the data is. Exit `0` equal, `2` different, `1` not compared. Each side is configured
as `PARITY_SOURCE_*` / `PARITY_TARGET_*`, with the same variables as `SQL_*`.

For the statement APIs, `SQL_DSN` is the API's host with the session as query parameters (`https://<account>.snowflakecomputing.com/?warehouse=…&database=…`, `https://bigquery.googleapis.com/?project=…&dataset=…`, `https://<workspace>.cloud.databricks.com/?warehouse_id=…`), and `SQL_PASSWORD` is the bearer token. BigQuery with no `SQL_PASSWORD` takes its token from the GCE metadata server. Values arrive as the APIs' text. The crate README has the details.

## Configure it

| Variable | Meaning |
|---|---|
| `SQL_ENGINE` | which store (table above) — **required** |
| `SQL_DSN` | how to reach it — **required** |
| `SQL_STATEMENT` / `SQL_STATEMENT_FILE` | the one statement to run |
| `SQL_MODE` | `exec` · `scalar` · `rows` · `script` |
| `SQL_USER` | username, when the DSN does not carry one |
| `SQL_PASSWORD` | the credential (the bearer token, for the statement APIs) — feed it from `value_from: { secret: … }` |
| `SQL_TOKEN_TYPE` | Snowflake only: `programmatic_access_token` (default), `oauth`, `keypair_jwt` |
| `SQL_MAX_ROWS` | row budget, refused before anything is written (default `10000`) |
| `SQL_MAX_BYTES` | byte budget, same (default `8 MiB`) |
| `SQL_TIMEOUT_SECS` | statement deadline; on the statement APIs, a statement still running at it is cancelled |
| `SQL_OUTPUT_NAME` | artifact filename in `rows` mode (default `result.ndjson`) |

**A password inline in `SQL_DSN` is refused, not warned about.** dagron's redactor masks task env vars by *name* — `SQL_DSN` matches none of its patterns, so an inline credential would appear unmasked in logs and in any error this step prints. `SQL_PASSWORD` is a name it does match.

## Use it in a DAG

```yaml
tasks:
  - name: verify
    docker_image: your-task-image:1.2.3   # with the binary COPY'd in
    command: ["dagron-step-sql"]
    max_attempts: 1
    env:
      - { name: SQL_ENGINE,    value: clickhouse }
      - { name: SQL_DSN,       value: "http://reader@clickhouse:8123/" }
      - { name: SQL_PASSWORD,  value_from: { secret: CLICKHOUSE_PASSWORD } }
      - { name: SQL_MODE,      value: scalar }
      - { name: SQL_STATEMENT, value: "SELECT count() > 0 FROM marts WHERE ds = '{{ ds }}'" }

  - name: publish
    depends_on: [verify]
    when: "{{ tasks.verify.output }} == 1"
    command: ["sh", "-c", "echo notifying downstream"]
```

A scalar to stdout is the one thing `when:` can read, which is what makes this the sanity check in front of a publish.

## Tags

| Tag | Notes |
|---|---|
| `latest` | newest release |
| `0.11` | floating minor — newest `0.11.x` |

Pin in production — pick the newest published tag rather than copying a version from this page, which ages.

> Tags exist from **0.9.3**; the repository has been public since 0.10.0.

## See also

- **`mancube/dagron-step-spark`** — the other half of the warehouse pair
- **`docs/DATASETS.md`** — `produces:` and the dataset triggers this step's output usually feeds
