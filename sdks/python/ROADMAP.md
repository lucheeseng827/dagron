# dagron Python SDK — coverage & roadmap

> Status of this document: **living plan.** It tracks what the SDK covers today
> (`v0.2`) and the planned path to `v1.0`. Update the coverage matrix in the same
> PR as any endpoint change so the SDK never silently drifts from `dagron-api`.

## Goal

Let a Python program do **anything the dagron web UI can do** — author workflows,
trigger and observe runs, manage schedules, redrive dead letters, wire up GitOps —
without hand-rolling REST calls, signing JWTs by hand, or memorising request
shapes. The SDK is the typed, documented seam between user code and the dagron
control plane.

## Design principles

1. **Standard-library only.** `json` + `urllib` — `pip install dagron-sdk` pulls
   nothing else. Keeps the SDK trivially vendorable into locked-down task images.
2. **Target the gateway, not the database.** The SDK speaks to **`dagron-api`**
   (`/api/...`, JWT-authenticated) — the same authenticated surface the UI uses —
   rather than the engine's in-process ops API. One auth model, one base URL.
3. **Fail fast, locally.** `Dag` runs the server's structural checks
   (unique names, known deps, leaf-xor-chain, acyclic) client-side, so a malformed
   DAG raises a clear `ValueError` instead of a 400 round-trip.
4. **Thin and honest.** Methods map one endpoint to one call and return the
   server's JSON (as `dict`/`list`) unmodified. No hidden caching or retries in
   `v0.x`; what you call is what hits the wire. Typed result objects come later
   (see M3) as an additive layer, not a rewrite.
5. **Errors are first-class.** Every non-2xx raises `DagronError(status, message)`
   with the server's message unwrapped from `{"error": ...}` or plain text.

## The two dagron HTTP surfaces

dagron exposes **two** HTTP APIs. Knowing which is which explains the SDK's choices.

| Surface | Crate | Base | Auth | Submit body | SDK target |
|---|---|---|---|---|---|
| **Gateway** | `dagron-api` | `/api/...` | session JWT (Bearer/cookie) | `{"yaml": "<spec>"}` | **Yes — primary** |
| **Engine ops** | `dagron-engine` (`src/api.rs`, `openapi.yaml`) | `/...` | none (trusted network) | raw spec body | Planned (M4) |

> **Contract note / fixed bug:** the gateway's `POST /api/runs` expects
> `{"yaml": "<spec string>"}` (`control.rs::SubmitBody`), confirmed by the UI client
> (`frontend/src/lib/dagron-api.ts`). The `v0.1` SDK posted the *raw* spec — which
> the gateway rejects — so `submit()` was corrected in `v0.2` to wrap the spec.

## Coverage matrix (`dagron-api` gateway)

Legend: ✅ shipped · 🔜 planned (milestone) · ➖ intentionally out of scope.

### Auth & identity
| Endpoint | SDK | Status |
|---|---|---|
| `POST /api/login` | `Client.login` | ✅ v0.2 |
| `POST /api/logout` | `Client.logout` | ✅ v0.2 |
| `GET  /api/me` | `Client.me` | ✅ v0.2 |
| `POST /api/users` | `Client.create_user` | ✅ v0.2 |

### Runs (trigger / inspect / control)
| Endpoint | SDK | Status |
|---|---|---|
| `POST /api/runs` | `Client.submit_run` | ✅ v0.2 |
| `GET  /api/runs` | `Client.list_runs` | ✅ v0.2 |
| `GET  /api/runs/{id}` | `Client.get_run` | ✅ v0.2 |
| `GET  /api/runs/{id}/graph` | `Client.get_run_graph` | ✅ v0.2 |
| `GET  /api/runs/{id}/tasks/{tid}/logs` | `Client.get_task_logs` | ✅ v0.2 |
| `GET  /api/runs/{id}/stream` (SSE) | `Client.stream_run` | ✅ v0.2 |
| `POST /api/runs/{id}/cancel` | `Client.cancel_run` | ✅ v0.2 |
| `POST /api/runs/{id}/rerun` | `Client.rerun_run` | ✅ v0.2 |
| `POST /api/runs/{id}/resubmit` | `Client.resubmit_run` | ✅ v0.2 |
| `POST /api/runs/{id}/tasks/{tid}/retry` | `Client.retry_task` | ✅ v0.2 |
| *(poll helper, no endpoint)* | `Client.wait_for_run` | ✅ v0.2 |

### Workflows (saved, reusable definitions)
| Endpoint | SDK | Status |
|---|---|---|
| `GET    /api/workflows` | `Client.list_workflows` | ✅ v0.2 |
| `GET    /api/workflows/{id}` | `Client.get_workflow` | ✅ v0.2 |
| `POST   /api/workflows` | `Client.create_workflow` | ✅ v0.2 |
| `PUT    /api/workflows/{id}` | `Client.update_workflow` | ✅ v0.2 |
| `DELETE /api/workflows/{id}` | `Client.delete_workflow` | ✅ v0.2 |
| `POST   /api/workflows/{id}/run` | `Client.run_workflow` | ✅ v0.2 |
| `POST   /api/workflows/{id}/sync-to-git` | `Client.sync_workflow_to_git` | ✅ v0.2 |

### Schedules
| Endpoint | SDK | Status |
|---|---|---|
| `GET    /api/schedules` | `Client.list_schedules` | ✅ v0.2 |
| `POST   /api/schedules` | `Client.create_schedule` | ✅ v0.2 |
| `PUT    /api/schedules/{id}` | `Client.update_schedule` | ✅ v0.2 |
| `DELETE /api/schedules/{id}` | `Client.delete_schedule` | ✅ v0.2 |
| `POST   /api/schedules/{id}/backfill` | `Client.backfill_schedule` | ✅ v0.2 |

### Dead letters & observability
| Endpoint | SDK | Status |
|---|---|---|
| `GET    /api/metrics` | `Client.metrics` | ✅ v0.2 |
| `GET    /api/dead-letters` | `Client.list_dead_letters` | ✅ v0.2 |
| `POST   /api/dead-letters/{id}/redrive` | `Client.redrive_dead_letter` | ✅ v0.2 |
| `DELETE /api/dead-letters/{id}` | `Client.discard_dead_letter` | ✅ v0.2 |
| `GET    /healthz` | `Client.healthz` | ✅ v0.2 |

### GitOps repository registry
| Endpoint | SDK | Status |
|---|---|---|
| `GET    /api/git-repos` | `Client.list_git_repos` | ✅ v0.2 |
| `POST   /api/git-repos` | `Client.connect_git_repo` | ✅ v0.2 |
| `POST   /api/git-repos/{id}/sync` | `Client.sync_git_repo` | ✅ v0.2 |
| `DELETE /api/git-repos/{id}` | `Client.disconnect_git_repo` | ✅ v0.2 |

**`v0.2` reaches 100% of the current `dagron-api` HTTP surface.** From here the
roadmap is about hardening, ergonomics, and reaching dagron internals that are not
yet (or only partially) exposed over HTTP.

## Milestones

### M1 — Core client *(shipped, v0.2)*
Full gateway coverage, the `Dag` builder with client-side validation, `DagronError`,
SSE streaming, and a `wait_for_run` poll helper. Real-socket test suite.

### M2 — Robustness & ergonomics *(next, v0.3)*
- **Retries with backoff** for idempotent reads and `429 Too Many Requests`
  (the submit admission valve returns `Retry-After`); opt-in, off by default.
- **Connection reuse / session object** to avoid a fresh TCP+TLS handshake per call
  on hot loops (still stdlib — a small keep-alive `http.client` pool).
- **`from_env()` constructor** reading `DAGRON_API_URL` / `DAGRON_TOKEN`.
- **Pagination iterator** — `iter_runs()` that walks `limit`/`offset` transparently.
- **Context-manager** support (`with Client(...) as c:`) and explicit `close()`.

### M3 — Typed models *(v0.4)*
- Lightweight `@dataclass` views (`Run`, `TaskRun`, `Workflow`, `Schedule`,
  `DeadLetter`, `GitRepo`) generated from / checked against `openapi.yaml`, returned
  by an opt-in typed layer. Raw-`dict` methods stay for forward compatibility.
- Enums for `RunStatus` / `TaskStatus` mirroring the engine state machines.
- `Dag` importers: build a `Dag` from an existing spec `dict`/YAML string
  (round-trip with `to_spec`), easing migration from the YAML/`importers/airflow` path.

### M4 — Second backend & deployment surfaces *(v0.5)*
- **Engine ops API client** (`EngineClient`) for the no-auth `dagron-engine`
  surface (`/runs` raw-body submit, `/metrics` Prometheus text, `/dead-letters`) —
  useful for in-cluster sidecar automation that bypasses the gateway.
- **Async client** (`AsyncClient`) — an `asyncio`/`aiohttp`-optional twin for
  high-fan-out submit/poll workloads. Kept as an extra so the core stays zero-dep.
- **CLI** (`python -m dagron ...`) wrapping the most common verbs (submit, runs,
  logs, cancel) for shell use, mirroring `ee/dagron-cli-ee`.

### M5 — Deeper dagron internals *(v0.6+, gated on server exposure)*
These reach beyond the current HTTP surface; each needs a server-side endpoint or a
documented protocol before the SDK can wrap it. Tracked here so the SDK roadmap and
the platform roadmap stay aligned (`docs/ROADMAP.md`, `ee/PRODUCT_ROADMAP.md`).

| Internal | Where it lives today | SDK plan |
|---|---|---|
| **MCP tools** (model-driven control) | `crates/dagron-mcp`, `ee/dagron-mcp-ee` | thin `mcp` helper once the tool schema stabilises |
| **Lineage / data catalog** | `crates/dagron-lineage` | `lineage` reads once an HTTP endpoint exists |
| **Artifacts** (task inputs/outputs) | `crates/dagron-artifact` | `artifacts.get/put` against object storage |
| **Event sources / queues** | `ee/dagron-source-queues` (kafka/sqs/nats/redis) | publish-a-trigger helper |
| **Operator CRDs** (`Workflow`, schedules) | `ee/dagron-operator`, Helm `crds/` | optional Kubernetes-native authoring export |
| **Backfill (durable)** | `schedules/{id}/backfill` (MVP, in-process) | track the queue-backed beta follow-up |
| **SSO / OIDC login** | `ee/dagron-sso` | pluggable auth provider on `Client` |

### M6 — Stabilise to `v1.0`
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
  holds it in memory for the session.

## Open questions

1. **Keep-alive without leaving the stdlib?** `urllib` opens a fresh connection per
   call. `http.client` can pool, but the abstraction is lower-level — measure
   whether M2's pooling is worth the complexity for typical (low-QPS) automation.
2. **Typed models: hand-written vs generated from `openapi.yaml`?** The gateway has
   no OpenAPI doc yet (only the engine does). Generating types needs the gateway to
   publish a spec first — itself a worthwhile platform task.
3. **One client for both backends, or two?** Current lean: two (`Client`,
   `EngineClient`) since auth and the submit body differ; revisit if the surfaces
   converge.
