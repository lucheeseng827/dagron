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
