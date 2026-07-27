# dagron-logging — shared tracing/observability bootstrap

`dagron-logging` is the **single source of truth** for how every internal dagron
process turns `tracing` events into output. The workflow controller / reconcile
loop and its worker pool (`dagron`), the management API (`dagron-api`), and the
operator all call [`init`], so verbosity and format are configured in one place
and logs stay consistent across services — important when shipping them to a
central aggregator (Loki, CloudWatch, Datadog, …) that needs them
machine-parseable and per-service attributable.

Configuration is entirely environment-driven, so a deployment can be tuned
without a rebuild.

## What it does

- `init(service)` — initializes the global tracing subscriber for a named
  `service` (e.g. `"controller"`, `"api"`, `"operator"`). Call exactly once, as
  early in `main` as possible. The `service` name is emitted on the startup
  "logging initialized" line so each process is identifiable in a shared stream.
  Uses `try_init`, so a double call (e.g. in tests) is a no-op rather than a panic.
- Reads the `RUST_LOG` / `LOG_LEVEL` verbosity knobs (via `EnvFilter`) and the
  `LOG_FORMAT` output style — `full` / `compact` / `pretty` / `json` — plus a set
  of per-line detail toggles. A malformed `RUST_LOG` / `LOG_LEVEL` is reported to
  stderr and ignored rather than silently swallowing all logs.

## Quickstart

```rust
fn main() {
    dagron_logging::init("controller");
    tracing::info!("started");
}
```

```sh
LOG_FORMAT=json LOG_LEVEL=debug ./dagron        # machine-parseable, verbose
RUST_LOG=info,dagron::worker=debug,sqlx=warn ./dagron   # per-target control
```

## Config

Verbosity precedence: `RUST_LOG` (if set and parseable) → `LOG_LEVEL` → `info`.

| Env | Purpose |
|-----|---------|
| `RUST_LOG` | Full `EnvFilter` directive for per-target verbosity; wins over `LOG_LEVEL` |
| `LOG_LEVEL` | Simple global level `trace`/`debug`/`info`/`warn`/`error` (default `info`) |
| `LOG_FORMAT` | `full`/`compact`/`pretty`/`json` (default `full`) |
| `LOG_TARGET` | Include the emitting module path (`1`/`0`, default `1`) |
| `LOG_THREAD_IDS` | Include the OS thread id (`1`/`0`, default `0`) |
| `LOG_THREAD_NAMES` | Include the thread name (`1`/`0`, default `0`) |
| `LOG_LINE` | Include source file + line number (`1`/`0`, default `0`) |
| `LOG_SPAN_EVENTS` | Span lifecycle events `none`/`new`/`enter`/`exit`/`close`/`active`/`full` (default `none`) |
| `LOG_ANSI` | Force ANSI colors on/off (`1`/`0`, default auto; always off for `json`) |

## Reading logs back (`logfilter`, feature-gated)

Everything above is the **emit** side. The `logfilter` module — behind the
`logfilter` cargo feature — is the **read** side: the filter grammar applied to a
run's *stored task output* when a human opens a workflow's logs.

The two are deliberately separate concerns. `init` decides what a process emits;
`logfilter` decides what someone sees when they ask "show me only the errors from
these two tasks, minus the healthcheck noise". Task output is stored verbatim, so
that question is a query over text — not something the emitting side can be
reconfigured to answer after the fact.

It lives here, rather than in `dagron-api`, because more than one process needs
the *same* grammar — and a filter that means two different things is worse than
no filter at all. `dagron-api` and the engine's `ops` API both apply it, and
anything that wants to *store* a filter (a config file, a runbook, a saved view
in a downstream tool) can parse it here first, so a filter can never be recorded
in a form the engine will later reject.

```rust
use dagron_logging::logfilter::{Level, LogFilter};

let filter = LogFilter::builder()
    .levels([Level::Error, Level::Warn])
    .exclude("healthz")
    .context(1)
    .build()?;
let view = filter.apply(&task_output);
println!("{} of {} lines match", view.matched, view.total);
```

| Query param | Predicate |
|-------------|-----------|
| `q` | line contains **every** term (repeatable) |
| `exclude` | line contains **none** of these terms (repeatable) |
| `regex` | line matches this regular expression (max 512 bytes) |
| `level` | line's *inferred* level is one of these (repeatable/CSV) |
| `case` | `1` for case-sensitive matching (default: insensitive) |
| `context` | also keep N unmatched lines either side of a match |
| `limit` | cap returned lines (default 2000, hard cap 50000) |
| `tail` | when capping, keep the **last** N lines |

Two properties worth knowing before trusting a filtered view:

- **Levels are inferred**, from the head of each line only. Scanning the whole
  line would classify `echo "no errors found"` as an error — the single most
  annoying false positive a log filter can have. A line that never names a level
  is `plain`, which is most ordinary output.
- **`FilterResult` reports `total` and `matched` before the line cap**, so a
  caller can always say what was hidden rather than implying "that's all there
  was".

The feature is off by default: a reconcile-only engine build only ever *writes*
logs, so it shouldn't link a regex engine. `dagron-api` and `dagron-engine`'s
`ops` feature turn it on.
