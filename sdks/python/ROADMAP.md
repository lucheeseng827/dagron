# dagron Python SDK — coverage & roadmap

> Status of this document: **living plan.** It tracks what the SDK covers today
> (`0.9.0`) and the planned path to `1.0`. Update the coverage matrix in the same
> PR as any endpoint change so the SDK never silently drifts from `dagron-api`.

## Goal

Let a Python program do **anything the dagron web UI can do** — author workflows,
trigger and observe runs, manage schedules and environments, redrive dead letters,
wire up GitOps — without hand-rolling REST calls, signing JWTs by hand, or
memorising request shapes. The SDK is the typed, documented seam between user code
and the dagron control plane.

## Versioning

The SDK's version tracks **the `dagron-api` version it covers**, not its own
feature history — the convention set when the SDKs were versioned to the `0.3.0`
API. `0.9.x` means "speaks to the 0.9 gateway". Within that, changes follow SemVer
against the SDK's own public surface.

## Design principles

1. **Standard-library only.** `json` + `urllib` — `pip install dagron-sdk` pulls
   nothing else. Keeps the SDK trivially vendorable into locked-down task images.
2. **Target the gateway, not the database.** The SDK speaks to **`dagron-api`**
   (`/api/...`, authenticated) — the same surface the UI uses — rather than the
   engine's in-process ops API. One auth model, one base URL.
3. **Fail fast, locally.** `Dag` runs the server's structural checks
   (`validate_graph`: unique names, known deps, one kind per task, trigger rules,
   runner classes, template calls, acyclicity) client-side, so a malformed DAG
   raises a clear `ValueError` instead of a 400 round-trip. It stops exactly where
   the server does: a dependency that only resolves after expansion is left to the
   engine, so a spec the server would accept is never rejected here.
4. **Thin and honest.** Methods map one endpoint to one call and return the
   server's JSON (as `dict`/`list`) unmodified. No hidden caching or retries in
   `0.x`; what you call is what hits the wire. Typed result objects come later
   (see M3) as an additive layer, not a rewrite.
5. **Errors are first-class.** Every non-2xx raises `DagronError(status, message)`
   with the server's message unwrapped from `{"error": ...}` or plain text.

## The two dagron HTTP surfaces

dagron exposes **two** HTTP APIs. Knowing which is which explains the SDK's choices.

| Surface | Crate | Base | Auth | Submit body | SDK target |
|---|---|---|---|---|---|
| **Gateway** | `dagron-api` | `/api/...` | session JWT or access token (Bearer/cookie) | `{"yaml": "<spec>"}` | **Yes — primary** |
| **Engine ops** | `dagron-engine` (`src/api.rs`, `openapi.yaml`) | `/...` | none (trusted network) | raw spec body | Planned (M4) |

> **Contract note:** the gateway's `POST /api/runs` expects
> `{"yaml": "<spec string>"}` (`control.rs::SubmitBody`), optionally with
> `parameters` and an `Idempotency-Key` header. `submit_run` wraps all three.

## Coverage matrix (`dagron-api` gateway)

Legend: ✅ shipped · 🔜 planned (milestone) · ➖ intentionally out of scope.

### Auth & identity
| Endpoint | SDK |
|---|---|
| `POST /api/login` | `Client.login` ✅ |
| `POST /api/logout` | `Client.logout` ✅ |
| `GET  /api/me` | `Client.me` ✅ |
| `GET  /api/users` | `Client.list_users` ✅ |
| `POST /api/users` | `Client.create_user` ✅ |
| `GET  /api/tokens` | `Client.list_tokens` ✅ |
| `POST /api/tokens` | `Client.create_token` ✅ |
| `DELETE /api/tokens/{id}` | `Client.revoke_token` ✅ |

### Runs (trigger / inspect / control)
| Endpoint | SDK |
|---|---|
| `POST /api/runs` | `Client.submit_run` ✅ |
| `GET  /api/runs` | `Client.list_runs` / `Client.iter_runs` ✅ |
| `GET  /api/runs/{id}` | `Client.get_run` ✅ |
| `GET  /api/runs/{id}/spec` | `Client.get_run_spec` ✅ |
| `GET  /api/runs/{id}/graph` | `Client.get_run_graph` ✅ |
| `GET  /api/runs/{id}/wait` | `Client.wait_run` ✅ |
| `GET  /api/runs/{id}/logs` | `Client.get_run_logs` ✅ |
| `GET  /api/runs/{id}/tasks/{tid}/logs` | `Client.get_task_logs` ✅ |
| `GET  /api/runs/{id}/stream` (SSE) | `Client.stream_run` ✅ |
| `GET  /api/events/stream` (SSE) | `Client.stream_events` ✅ |
| `POST /api/runs/{id}/cancel` | `Client.cancel_run` ✅ |
| `POST /api/runs/{id}/rerun` | `Client.rerun_run` ✅ |
| `POST /api/runs/{id}/resubmit` | `Client.resubmit_run` ✅ |
| `POST /api/runs/{id}/tasks/{tid}/retry` | `Client.retry_task` ✅ |
| `POST /api/runs/{id}/tasks/{tid}/clear` | `Client.clear_task` ✅ |
| `POST /api/runs/{id}/tasks/{tid}/approve` | `Client.approve_task` ✅ |
| `POST /api/runs/{id}/tasks/{tid}/reject` | `Client.reject_task` ✅ |
| `GET  /api/approvals` | `Client.list_approvals` ✅ |
| `POST /api/runs/{id}/triage` | `Client.set_triage` ✅ |
| `DELETE /api/runs/{id}/triage` | `Client.clear_triage` ✅ |
| `POST /api/runs/{id}/archive` | `Client.archive_run` ✅ |
| `GET  /api/archive/runs` | `Client.list_archived_runs` ✅ |
| `GET  /api/archive/runs/{id}` | `Client.get_archived_run` ✅ |
| *(poll helper, no endpoint)* | `Client.wait_for_run` ✅ |

### Workflows (saved, reusable definitions)
| Endpoint | SDK |
|---|---|
| `GET    /api/workflows` | `Client.list_workflows` (incl. `tag=`) ✅ |
| `GET    /api/workflows/{id}` | `Client.get_workflow` ✅ |
| `POST   /api/workflows` | `Client.create_workflow` ✅ |
| `PUT    /api/workflows/{id}` | `Client.update_workflow` ✅ |
| `DELETE /api/workflows/{id}` | `Client.delete_workflow` ✅ |
| `POST   /api/workflows/{id}/run` | `Client.run_workflow` (incl. `parameters=`) ✅ |
| `GET    /api/workflows/{id}/runs` | `Client.list_workflow_runs` ✅ |
| `GET    /api/workflows/{id}/versions` | `Client.list_workflow_versions` ✅ |
| `POST   /api/workflows/{id}/state` | `Client.set_workflow_state` ✅ |
| `POST   /api/workflows/bundle` | `Client.apply_bundle` ✅ |
| `GET    /api/badges/{name}` | `Client.workflow_badge` ✅ |
| `POST   /api/workflows/{id}/sync-to-git` | `Client.sync_workflow_to_git` ✅ |

### Schedules & backfills
| Endpoint | SDK |
|---|---|
| `GET    /api/schedules` | `Client.list_schedules` ✅ |
| `POST   /api/schedules` | `Client.create_schedule` ✅ |
| `PUT    /api/schedules/{id}` | `Client.update_schedule` ✅ |
| `DELETE /api/schedules/{id}` | `Client.delete_schedule` ✅ |
| `POST   /api/schedules/{id}/backfill` | `Client.backfill_schedule` ✅ |
| `POST   /api/backfills` | `Client.create_backfill` ✅ |
| `GET    /api/backfills` | `Client.list_backfills` ✅ |
| `GET    /api/backfills/{id}` | `Client.get_backfill` ✅ |
| `POST   /api/backfills/{id}/cancel` | `Client.cancel_backfill` ✅ |

### Environments & instance settings
| Endpoint | SDK |
|---|---|
| `GET    /api/environments` | `Client.list_environments` ✅ |
| `POST   /api/environments` | `Client.create_environment` ✅ |
| `PUT    /api/environments/{id}` | `Client.update_environment` ✅ |
| `DELETE /api/environments/{id}` | `Client.delete_environment` ✅ |
| `PUT    /api/environments/{id}/secrets/{name}` | `Client.set_environment_secret` ✅ |
| `DELETE /api/environments/{id}/secrets/{name}` | `Client.delete_environment_secret` ✅ |
| `GET/PUT /api/settings/notifications` | `Client.get/set_notification_settings` ✅ |
| `POST   /api/settings/notifications/test` | `Client.test_notifications` ✅ |
| `GET/PUT /api/settings/dead-letters` | `Client.get/set_dead_letter_settings` ✅ |

### Datasets & artifacts
| Endpoint | SDK |
|---|---|
| `GET /api/datasets` | `Client.list_datasets` ✅ |
| `GET /api/datasets/events` | `Client.list_dataset_events` ✅ |
| `PUT /api/runs/{id}/artifacts/{task}/{name}` | `Client.put_artifact` ✅ |
| `GET /api/runs/{id}/artifacts/{task}/{name}` | `Client.get_artifact` ✅ |
| `GET /api/runs/{id}/artifacts/{task}/{name}/exists` | `Client.artifact_exists` ✅ |
| `POST /api/artifacts/sync` | `Client.sync_artifacts` ✅ |

### Dead letters, GitOps & observability
| Endpoint | SDK |
|---|---|
| `GET    /api/dead-letters` | `Client.list_dead_letters` ✅ |
| `POST   /api/dead-letters/{id}/redrive` | `Client.redrive_dead_letter` ✅ |
| `DELETE /api/dead-letters/{id}` | `Client.discard_dead_letter` ✅ |
| `GET    /api/git-repos` | `Client.list_git_repos` ✅ |
| `POST   /api/git-repos` | `Client.connect_git_repo` ✅ |
| `PUT    /api/git-repos/{id}/auth` | `Client.set_git_repo_auth` ✅ |
| `DELETE /api/git-repos/{id}/auth` | `Client.clear_git_repo_auth` ✅ |
| `POST   /api/git-repos/{id}/sync` | `Client.sync_git_repo` ✅ |
| `DELETE /api/git-repos/{id}` | `Client.disconnect_git_repo` ✅ |
| `GET    /api/metrics` | `Client.metrics` ✅ |
| `GET    /api/metrics/timeseries` | `Client.metrics_timeseries` ✅ |
| `GET    /api/search` | `Client.search` ✅ |
| `GET    /api/health` | `Client.health` ✅ |
| `GET    /healthz` | `Client.healthz` ✅ |
| `GET    /readyz` | `Client.readyz` ✅ |

### Not wrapped
| Endpoint | Why |
|---|---|
| `GET /api/audit`, `GET /api/fleet`, `GET/POST /api/link*`, `POST /api/artifacts/rotate` | ➖ Build-gated: absent, or answering a signpost, in this build. The SDK wraps what this gateway actually serves. |

## Spec coverage (`Dag` builder)

`Dag`/`Template` emit the engine's whole authoring surface, not a subset — the
drift that made the console silently drop `with_items`, `wait`, `cache`, `pool`,
`priority` and `produces` is the exact failure this matrix exists to prevent.

| Level | Fields |
|---|---|
| Spec | `name`, `parameters`, `tags`, `environment`, `runner_class`, `task_defaults`, `run_timeout_secs`, `max_active_runs`, `result_from`, `budget`, `deadline`, `notify`, `on_datasets`, `datasets_mode`, `templates`, `tasks` |
| Task kind | leaf (`command`), call (`template` + `arguments`), chain (`workflow_ref`), `type: approval` / `workflow` / `wait` |
| Task | `docker_image`, `depends_on`, `input`, `env` (incl. `value_from`), `resources`, `service_account`, `when`, `trigger_rule`, `hook`, `allow_failure`, `with_items`, `with_param`, `instance_key`, `max_attempts`, `retry_delay_secs`, `retry_max_delay_secs`, `retry_on_timeout`, `retry_budgets`, `timeout_secs`, `runner_class`, `pool`, `priority`, `cache`, `repeat`, `produces`, `gang`, `isolation`, `approval_timeout_secs`, `approval_on_timeout`, `workflow`, `wait` |

Shorthands: `Dag.template()`, `.approval()`, `.sensor()`, `.trigger()`.

## Milestones

### M1 — Core client *(shipped)*
Full gateway coverage, the `Dag` builder with client-side validation, `DagronError`,
SSE streaming, and a `wait_for_run` poll helper. Real-socket test suite.

### M2 — Robustness & ergonomics *(partly shipped in 0.9.0)*
- ✅ **`from_env()` constructor** reading `DAGRON_API_URL` / `DAGRON_TOKEN`, paired
  with `create_token` so automation never stores a password.
- ✅ **Pagination iterator** — `iter_runs()` walks `limit`/`offset` transparently.
- ✅ **Context-manager** support (`with Client(...) as c:`) and explicit `close()`.
- 🔜 **Retries with backoff** for idempotent reads and `429 Too Many Requests`
  (the submit admission valve returns `Retry-After`); opt-in, off by default.
- 🔜 **Connection reuse / session object** to avoid a fresh TCP+TLS handshake per
  call on hot loops (still stdlib — a small keep-alive `http.client` pool).

### M3 — Typed models
- Lightweight `@dataclass` views (`Run`, `TaskRun`, `Workflow`, `Schedule`,
  `DeadLetter`, `GitRepo`) checked against the gateway's shapes and returned by an
  opt-in typed layer. Raw-`dict` methods stay for forward compatibility.
- Enums for `RunStatus` / `TaskStatus` mirroring the engine state machines.
- `Dag` importers: build a `Dag` from an existing spec `dict`/YAML string
  (round-trip with `to_spec`), easing migration from the YAML/`importers/airflow`
  path. `get_run_spec` now gives that importer its input.

### M4 — Second backend & deployment surfaces
- **Engine ops API client** (`EngineClient`) for the no-auth `dagron-engine`
  surface (`/runs` raw-body submit, `/metrics` Prometheus text, `/dead-letters`) —
  useful for in-cluster sidecar automation that bypasses the gateway.
- **Async client** (`AsyncClient`) — an `asyncio`/`aiohttp`-optional twin for
  high-fan-out submit/poll workloads. Kept as an extra so the core stays zero-dep.
- **CLI** (`python -m dagron ...`) wrapping the most common verbs (submit, runs,
  logs, cancel) for shell use.

### M5 — Deeper dagron internals *(gated on server exposure)*
These reach beyond the current HTTP surface; each needs a server-side endpoint or a
documented protocol before the SDK can wrap it.

| Internal | Where it lives today | SDK plan |
|---|---|---|
| **MCP tools** (model-driven control) | `crates/dagron-mcp` | thin `mcp` helper once the tool schema stabilises |
| **Lineage / data catalog** | `crates/dagron-lineage` | deeper `lineage` reads once endpoints exist (the dataset registry + ledger are covered today) |
| **Event sources / queues** | queue sources (kafka/sqs/nats/redis) | publish-a-trigger helper |
| **Operator CRDs** (`Workflow`, schedules) | Helm `crds/` | optional Kubernetes-native authoring export |
| **State plans** | `crates/dagron-state` | compile a backfill plan into runs from Python |
| **SSO / OIDC login** | the identity seam | pluggable auth provider on `Client` |

### M6 — Stabilise to `1.0`
SemVer guarantees on the public surface, a published changelog, generated API
reference, and a conformance test that runs the SDK against a live `dagron-api`
(beyond the unit-level fake gateway) in CI.

## Out of scope (deliberately)

- ➖ **Re-implementing scheduling/execution.** The SDK is a *client*; the engine
  owns reconciliation, leasing, and retries.
- ➖ **Embedding a YAML parser.** JSON is a YAML subset and the gateway accepts it,
  so `Dag.to_json()` covers authoring without a third-party dependency. (Reading
  arbitrary hand-written YAML specs into a `Dag` is an M3 *import* nicety, gated on
  whether it can stay stdlib-only.)
- ➖ **Storing credentials.** The caller owns the token lifecycle; the SDK only
  holds it in memory for the session, and `close()`/`__exit__` drop it.

## Open questions

1. **Keep-alive without leaving the stdlib?** `urllib` opens a fresh connection per
   call. `http.client` can pool, but the abstraction is lower-level — measure
   whether M2's pooling is worth the complexity for typical (low-QPS) automation.
2. **Typed models: hand-written vs generated?** The gateway has no OpenAPI doc yet
   (only the engine does). Generating types needs the gateway to publish a spec
   first — itself a worthwhile platform task.
3. **One client for both backends, or two?** Current lean: two (`Client`,
   `EngineClient`) since auth and the submit body differ; revisit if the surfaces
   converge.
4. **Keeping the TypeScript SDK in step.** `@dagron/sdk` 0.9.0 mirrors this
   surface method for method (camelCased), so the matrix above is the coverage
   matrix for both. They have always been released together; the open question is
   whether to keep hand-mirroring or generate one from the other once the gateway
   publishes an OpenAPI document (question 2).
