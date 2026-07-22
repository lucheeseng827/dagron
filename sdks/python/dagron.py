"""dagron Python SDK — author DAGs in code and drive the full dagron control plane.

Two layers, both standard-library only (``json`` + ``urllib``):

* :class:`Dag` — a fluent builder for a dagron workflow spec. ``to_json()`` emits
  valid dagron input (dagron parses YAML, and JSON is a YAML subset) and the
  builder validates the graph (unique names, known deps, leaf-xor-chain, acyclic)
  client-side, mirroring the server's ``validate_graph``.

* :class:`Client` — a thin, typed wrapper over the **dagron-api** gateway
  (``/api/...``, JWT-authenticated). It covers the same surface the web UI uses —
  login, runs, workflows, schedules, dead-letters, GitOps repos, metrics — so an
  automation can do anything the UI can without hand-rolling REST calls.

    from dagron import Dag, Client

    dag = Dag("etl")
    extract = dag.task("extract", image="alpine", command=["echo", "hi"])
    dag.task("load", image="alpine", command=["true"], depends_on=[extract])

    api = Client("http://localhost:8080")
    api.login("admin@example.com", "hunter2222")   # stores the session token
    run_id = api.submit_run(dag)                    # trigger an ad-hoc run
    run = api.wait_for_run(run_id)                  # poll to a terminal state
    print(run["status"])

The gateway expects a DAG submitted as ``{"yaml": "<spec>"}``; the SDK wraps that
for you, so callers pass a :class:`Dag`, a spec ``dict``, or a YAML/JSON string.

Targeting the no-auth engine ops API instead (``/runs``, raw-body submit) is on the
roadmap (see ``ROADMAP.md``); today the client speaks the gateway dialect.
"""

from __future__ import annotations

import copy
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Union

__all__ = ["Dag", "Client", "DagronError", "SpecLike", "__version__"]
__version__ = "0.3.0"

#: Run statuses the engine treats as terminal (no further transitions).
TERMINAL_RUN_STATUSES = frozenset({"succeeded", "failed", "cancelled"})

#: Anything accepted where a DAG spec is expected: a builder, a spec mapping, or
#: an already-serialised YAML/JSON string.
SpecLike = Union["Dag", Mapping[str, Any], str]


# ── Builder ───────────────────────────────────────────────────────────────────


class Dag:
    """Build a dagron workflow spec in code.

    Add tasks with :meth:`task`; ``to_spec()`` / ``to_json()`` validate the graph
    and emit the spec. Pass the builder straight to :meth:`Client.submit_run` or
    :meth:`Client.create_workflow`.
    """

    def __init__(self, name: str, *, runner_class: Optional[str] = None) -> None:
        """Create an empty DAG named ``name`` (raises if the name is empty).

        ``runner_class`` is the workflow-level default runner class — every task
        that doesn't set its own ``runner_class`` routes to that pool of engine
        replicas (e.g. ``"etl"``, ``"pulse"``, ``"ml_training"``). Leave unset
        for the shared default pool.
        """
        if not name:
            raise ValueError("Dag requires a name")
        self.name = name
        self.runner_class = runner_class
        self._tasks: List[Dict[str, Any]] = []
        self._names: set[str] = set()

    def task(
        self,
        name: str,
        *,
        image: Optional[str] = None,
        command: Optional[Sequence[str]] = None,
        depends_on: Optional[Sequence[str]] = None,
        workflow_ref: Optional[str] = None,
        input: Optional[Any] = None,
        max_attempts: Optional[int] = None,
        retry_delay_secs: Optional[int] = None,
        timeout_secs: Optional[int] = None,
        env: Optional[Union[Mapping[str, str], Sequence[Mapping[str, str]]]] = None,
        resources: Optional[Mapping[str, Any]] = None,
        service_account: Optional[str] = None,
        runner_class: Optional[str] = None,
    ) -> str:
        """Add a task; returns its name (pass it to a later task's ``depends_on``).

        A task is either a *leaf* (runs ``command``) or a *chain* (``workflow_ref``
        inlines another saved workflow), never both — enforced at build time by
        :meth:`to_spec`, mirroring the server. The remaining keyword arguments map
        one-to-one onto the engine's ``TaskSpec`` and are omitted from the emitted
        spec when left unset so the JSON stays minimal.
        """
        if not name:
            raise ValueError("task requires a name")
        # str/bytes are sequences, so list("echo") would silently split into
        # characters — reject the scalar instead of building a broken spec.
        if isinstance(command, (str, bytes)):
            raise TypeError("command must be a sequence of strings, not a single string")
        if isinstance(depends_on, (str, bytes)):
            raise TypeError("depends_on must be a sequence of task names, not a single string")
        if name in self._names:
            raise ValueError(f"duplicate task '{name}'")
        self._names.add(name)

        t: Dict[str, Any] = {"name": name}
        if image:
            t["docker_image"] = image
        if command:
            t["command"] = list(command)
        if depends_on:
            t["depends_on"] = list(depends_on)
        if workflow_ref:
            t["workflow_ref"] = workflow_ref
        if input is not None:
            t["input"] = input
        if max_attempts is not None:
            if max_attempts < 1:
                raise ValueError("max_attempts must be >= 1")
            t["max_attempts"] = max_attempts
        if retry_delay_secs is not None:
            t["retry_delay_secs"] = retry_delay_secs
        if timeout_secs is not None:
            t["timeout_secs"] = timeout_secs
        if env is not None:
            t["env"] = _normalize_env(env)
        if resources is not None:
            t["resources"] = dict(resources)
        if service_account:
            t["service_account"] = service_account
        if runner_class:
            t["runner_class"] = runner_class

        self._tasks.append(t)
        return name

    def to_spec(self) -> Dict[str, Any]:
        """Build the validated dagron spec dict.

        Runs the same structural checks the gateway runs server-side, so a bad DAG
        fails locally with a clear message instead of a 400 round-trip:

        * every task is exactly one of leaf (``command``) or chain (``workflow_ref``);
        * every ``depends_on`` names a real task;
        * the dependency graph is acyclic.
        """
        for t in self._tasks:
            is_leaf = bool(t.get("command"))
            is_chain = bool(t.get("workflow_ref"))
            if is_leaf and is_chain:
                raise ValueError(
                    f"task '{t['name']}' sets both command and workflow_ref — use exactly one"
                )
            if not is_leaf and not is_chain:
                raise ValueError(
                    f"task '{t['name']}' needs a command (leaf) or a workflow_ref (chain)"
                )
            for d in t.get("depends_on", []):
                if d not in self._names:
                    raise ValueError(f"task '{t['name']}' depends on unknown task '{d}'")
        self._assert_acyclic()
        # Deep-copy so callers can't mutate our internal task state via the
        # returned spec (to_json/submit both go through here).
        spec: Dict[str, Any] = {"name": self.name, "tasks": self._tasks}
        if self.runner_class:
            spec["runner_class"] = self.runner_class
        return copy.deepcopy(spec)

    def _assert_acyclic(self) -> None:
        """DFS colouring; raise on the first back-edge (a dependency cycle)."""
        adjacency: Dict[str, List[str]] = {t["name"]: list(t.get("depends_on", [])) for t in self._tasks}
        WHITE, GREY, BLACK = 0, 1, 2
        color: Dict[str, int] = {name: WHITE for name in adjacency}

        def visit(node: str) -> None:
            """Depth-first visit; a GREY neighbour is a back-edge, i.e. a cycle."""
            color[node] = GREY
            for dep in adjacency[node]:
                if color[dep] == GREY:
                    raise ValueError(f"DAG '{self.name}' contains a cycle (through '{dep}')")
                if color[dep] == WHITE:
                    visit(dep)
            color[node] = BLACK

        for name in adjacency:
            if color[name] == WHITE:
                visit(name)

    def to_json(self) -> str:
        """dagron spec as JSON (valid dagron input — YAML is a JSON superset)."""
        return json.dumps(self.to_spec())

    def submit(self, api_url: str, token: Optional[str] = None, *, timeout: float = 30) -> str:
        """Submit the DAG as an ad-hoc run; returns the new ``run_id``.

        Convenience one-liner equivalent to ``Client(api_url, token).submit_run(self)``.
        """
        return Client(api_url, token=token, timeout=timeout).submit_run(self)


# ── Client ────────────────────────────────────────────────────────────────────


class DagronError(Exception):
    """A non-2xx response (or transport failure) from dagron-api.

    ``status`` is the HTTP code (``0`` for a transport-level failure), ``message``
    the server's error text (unwrapped from ``{"error": ...}`` when present), and
    ``body`` the raw response body for inspection.
    """

    def __init__(self, status: int, message: str, *, body: Optional[str] = None) -> None:
        """Store the HTTP ``status`` (``0`` for transport failures), ``message``, and raw ``body``."""
        self.status = status
        self.message = message
        self.body = body
        super().__init__(f"dagron-api {status}: {message}" if status else message)

    @classmethod
    def _from_body(cls, status: int, raw: bytes) -> "DagronError":
        """Build an error from a response body, unwrapping ``{"error": ...}`` when present."""
        text = raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else (raw or "")
        message = text
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict) and isinstance(parsed.get("error"), str):
                message = parsed["error"]
        except (ValueError, TypeError):
            pass
        message = (message or "").strip() or f"HTTP {status}"
        return cls(status, message, body=text)


class Client:
    """Typed client for the dagron-api gateway (``/api/...``).

    Construct with the gateway base URL (``http://host:port``) and either a session
    JWT (``token=``) or call :meth:`login` to obtain one. Every authed call sends
    ``Authorization: Bearer <token>``; the token can be rotated via :attr:`token`.
    """

    def __init__(self, base_url: str, token: Optional[str] = None, *, timeout: float = 30) -> None:
        """Bind the client to a gateway ``base_url`` (http/https) with an optional session token."""
        # urlopen also speaks file://, ftp://, … — restrict to HTTP(S) so a bad
        # base_url can't leak the bearer token or reach local files (SSRF).
        scheme = urllib.parse.urlparse(base_url).scheme
        if scheme not in ("http", "https"):
            raise ValueError("base_url must use http or https")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    # ── auth ──────────────────────────────────────────────────────────────────

    def login(self, email: str, password: str) -> str:
        """Exchange credentials for a session token, store it, and return it.

        After this call the client is authenticated for every other method.
        """
        body = self._request("POST", "/api/login", body={"email": email, "password": password}, auth=False)
        token = body.get("token") if isinstance(body, dict) else None
        if not token:
            raise DagronError(0, "login succeeded but no token was returned")
        self.token = token
        return token

    def logout(self) -> None:
        """Clear the session cookie server-side and drop the local token.

        Sends the bearer token so the call is authenticated and consistent with
        the rest of the client. dagron's session JWT is stateless, so logout is a
        cookie clear today (no server-side denylist to revoke against); dropping
        the local token is what ends the bearer session for this client.
        """
        self._request("POST", "/api/logout", parse_json=False)
        self.token = None

    def me(self) -> Dict[str, Any]:
        """Return the authenticated session's claims (``sub``/``email``/``groups``/…)."""
        return self._request("GET", "/api/me")

    def create_user(
        self, email: str, password: str, name: str, groups: Optional[Sequence[str]] = None
    ) -> Dict[str, Any]:
        """Create a user (caller must be in the ``admin`` group). Returns ``{"id": ...}``."""
        return self._request(
            "POST",
            "/api/users",
            body={"email": email, "password": password, "name": name, "groups": list(groups or [])},
        )

    # ── runs ──────────────────────────────────────────────────────────────────

    def submit_run(self, spec: SpecLike) -> str:
        """Submit a DAG as an ad-hoc run; returns the new ``run_id``.

        ``spec`` may be a :class:`Dag`, a spec mapping, or a YAML/JSON string.
        """
        resp = self._request("POST", "/api/runs", body={"yaml": _spec_to_str(spec)})
        return resp["run_id"]

    def list_runs(
        self, *, status: Optional[str] = None, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """List runs newest-first, optionally filtered by ``status`` and paged."""
        return self._request(
            "GET", "/api/runs", params={"status": status, "limit": limit, "offset": offset}
        )

    def get_run(self, run_id: str) -> Dict[str, Any]:
        """Fetch one run plus its task rows."""
        return self._request("GET", f"/api/runs/{_seg(run_id)}")

    def get_run_graph(self, run_id: str) -> Dict[str, Any]:
        """Fetch the run's task nodes + dependency edges (for graph rendering)."""
        return self._request("GET", f"/api/runs/{_seg(run_id)}/graph")

    def get_task_logs(self, run_id: str, task_id: str) -> Dict[str, Any]:
        """Fetch one task's captured output, scoped to its run."""
        return self._request("GET", f"/api/runs/{_seg(run_id)}/tasks/{_seg(task_id)}/logs")

    def cancel_run(self, run_id: str) -> int:
        """Cancel a run; returns the number of tasks flipped to ``cancelled``."""
        return self._request("POST", f"/api/runs/{_seg(run_id)}/cancel")["cancelled"]

    def rerun_run(self, run_id: str, *, params: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        """Cascade-rerun a failed/cancelled run from its failure frontier.

        Succeeded tasks are kept; failed/cancelled tasks (and what they blocked)
        reset and re-run. Optional ``params`` is deep-merged into each reset task's
        input for a fix-forward rerun. Returns ``{"run_id", "rerun": <tasks reset>}``.
        """
        body: Dict[str, Any] = {"params": dict(params)} if params else {}
        return self._request("POST", f"/api/runs/{_seg(run_id)}/rerun", body=body)

    def resubmit_run(self, run_id: str) -> str:
        """Start a brand-new run from this run's stored definition; returns the new ``run_id``."""
        return self._request("POST", f"/api/runs/{_seg(run_id)}/resubmit")["run_id"]

    def retry_task(self, run_id: str, task_id: str) -> bool:
        """Resurrect a single failed/cancelled task within a run."""
        return self._request("POST", f"/api/runs/{_seg(run_id)}/tasks/{_seg(task_id)}/retry")["retried"]

    def approve_task(self, run_id: str, task_id: str) -> Dict[str, Any]:
        """Approve a ``type: approval`` gate: the task succeeds and its dependents
        advance. Returns ``{"run_id", "task_id", "resolution"}``. Raises
        :class:`DagronError` with status 409 if the task is not awaiting approval.
        """
        return self._request(
            "POST", f"/api/runs/{_seg(run_id)}/tasks/{_seg(task_id)}/approve"
        )

    def reject_task(self, run_id: str, task_id: str) -> Dict[str, Any]:
        """Reject a ``type: approval`` gate: the task fails and its ``all_success``
        dependents skip. Same return/errors as :meth:`approve_task`.
        """
        return self._request(
            "POST", f"/api/runs/{_seg(run_id)}/tasks/{_seg(task_id)}/reject"
        )

    def stream_run(self, run_id: str, *, timeout: Optional[float] = None) -> Iterator[Dict[str, Any]]:
        """Yield live task-state events for a run as Server-Sent Events.

        Each item is ``{"event": <name>, "data": <parsed JSON or raw str>}``. The
        generator runs until the connection closes; pass ``timeout`` to bound an
        idle read. A ``resync`` event means the client fell behind and should
        refetch the full graph via :meth:`get_run_graph`.
        """
        url = f"{self.base_url}/api/runs/{_seg(run_id)}/stream"
        headers = {"accept": "text/event-stream"}
        if self.token:
            headers["authorization"] = f"Bearer {self.token}"
        req = urllib.request.Request(url, headers=headers, method="GET")
        # Normalise connection/HTTP failures to DagronError, same as _request, so
        # callers see one exception type across the whole client API.
        try:
            resp = urllib.request.urlopen(req, timeout=timeout)  # noqa: S310 (scheme checked in __init__)
        except urllib.error.HTTPError as e:
            raise DagronError._from_body(e.code, e.read()) from None
        except urllib.error.URLError as e:
            raise DagronError(0, f"request to {url} failed: {e.reason}") from None
        try:
            yield from _parse_sse(resp)
        finally:
            resp.close()

    def wait_for_run(
        self, run_id: str, *, poll_interval: float = 2.0, timeout: Optional[float] = 300.0
    ) -> Dict[str, Any]:
        """Poll :meth:`get_run` until the run reaches a terminal state; return it.

        Raises :class:`TimeoutError` if ``timeout`` seconds elapse first (``None``
        waits forever). A lightweight alternative to :meth:`stream_run` for scripts.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            run = self.get_run(run_id)
            if run.get("status") in TERMINAL_RUN_STATUSES:
                return run
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"run '{run_id}' did not finish within {timeout}s")
            time.sleep(poll_interval)

    # ── workflows (first-class, saved definitions) ────────────────────────────

    def list_workflows(self) -> List[Dict[str, Any]]:
        """List saved workflows enriched with schedule + recent-run digest."""
        return self._request("GET", "/api/workflows")

    def get_workflow(self, workflow_id: str) -> Dict[str, Any]:
        """Fetch one saved workflow including its spec."""
        return self._request("GET", f"/api/workflows/{_seg(workflow_id)}")

    def create_workflow(
        self, spec: SpecLike, *, name: Optional[str] = None, description: Optional[str] = None
    ) -> Dict[str, Any]:
        """Save a new workflow. ``name`` defaults to the spec's name. 409 on a dup name."""
        return self._request(
            "POST",
            "/api/workflows",
            body={"spec": _spec_to_str(spec), "name": name, "description": description},
        )

    def update_workflow(
        self,
        workflow_id: str,
        spec: SpecLike,
        *,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Replace a saved workflow's spec (and optionally rename it)."""
        return self._request(
            "PUT",
            f"/api/workflows/{_seg(workflow_id)}",
            body={"spec": _spec_to_str(spec), "name": name, "description": description},
        )

    def delete_workflow(self, workflow_id: str) -> None:
        """Delete a saved workflow."""
        self._request("DELETE", f"/api/workflows/{_seg(workflow_id)}", parse_json=False)

    def run_workflow(self, workflow_id: str) -> Dict[str, Any]:
        """Trigger a saved workflow as a run. Returns ``{"run_id", "workflow_id"}``."""
        return self._request("POST", f"/api/workflows/{_seg(workflow_id)}/run")

    def sync_workflow_to_git(self, workflow_id: str) -> Dict[str, Any]:
        """Open a PR committing the workflow's raw spec to the configured GitOps repo."""
        return self._request("POST", f"/api/workflows/{_seg(workflow_id)}/sync-to-git")

    # ── schedules ─────────────────────────────────────────────────────────────

    def list_schedules(self, *, workflow_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all schedules, or just one workflow's."""
        return self._request("GET", "/api/schedules", params={"workflow_id": workflow_id})

    def create_schedule(self, workflow_id: str, cron_expr: str, *, enabled: bool = True) -> Dict[str, Any]:
        """Attach a cron schedule to a saved workflow."""
        return self._request(
            "POST",
            "/api/schedules",
            body={"workflow_id": workflow_id, "cron_expr": cron_expr, "enabled": enabled},
        )

    def update_schedule(
        self, schedule_id: str, *, cron_expr: Optional[str] = None, enabled: Optional[bool] = None
    ) -> Dict[str, Any]:
        """Change a schedule's cron expression and/or enabled flag."""
        body: Dict[str, Any] = {}
        if cron_expr is not None:
            body["cron_expr"] = cron_expr
        if enabled is not None:
            body["enabled"] = enabled
        return self._request("PUT", f"/api/schedules/{_seg(schedule_id)}", body=body)

    def delete_schedule(self, schedule_id: str) -> None:
        """Remove a schedule."""
        self._request("DELETE", f"/api/schedules/{_seg(schedule_id)}", parse_json=False)

    def backfill_schedule(
        self, schedule_id: str, frm: str, to: str, *, max_runs: Optional[int] = None
    ) -> Dict[str, Any]:
        """Materialise a schedule's missed runs across ``[frm, to]`` (RFC3339).

        Re-issuing the same window is safe — already-materialised fire-times are
        reported as ``skipped`` rather than double-run.
        """
        body: Dict[str, Any] = {"from": frm, "to": to}
        if max_runs is not None:
            body["max_runs"] = max_runs
        return self._request("POST", f"/api/schedules/{_seg(schedule_id)}/backfill", body=body)

    # ── backfill jobs (durable, paced) ────────────────────────────────────────

    def create_backfill(
        self, schedule_id: str, frm: str, to: str, *, max_runs: Optional[int] = None
    ) -> Dict[str, Any]:
        """Create a durable, paced backfill *job* over ``[frm, to]`` (RFC3339).

        Unlike :meth:`backfill_schedule` (which materialises the whole window in one
        synchronous call, capped low), this snapshots the schedule and lets the
        engine drip a bounded number of fire-times per tick — listable, monitorable,
        and cancellable. Returns the created backfill job. Slots already materialised
        by a manual/auto backfill are deduped, never double-run.
        """
        body: Dict[str, Any] = {"schedule_id": schedule_id, "from": frm, "to": to}
        if max_runs is not None:
            body["max_runs"] = max_runs
        return self._request("POST", "/api/backfills", body=body)

    def list_backfills(
        self, *, schedule_id: Optional[str] = None, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """List backfill jobs, newest first; filter by ``schedule_id``."""
        return self._request(
            "GET", "/api/backfills", params={"schedule_id": schedule_id, "limit": limit}
        )

    def get_backfill(self, backfill_id: str) -> Dict[str, Any]:
        """Fetch one backfill job for monitoring (``fired``/``requested``/``status``)."""
        return self._request("GET", f"/api/backfills/{_seg(backfill_id)}")

    def cancel_backfill(self, backfill_id: str) -> Dict[str, Any]:
        """Stop pacing a running backfill job. Returns the updated job."""
        return self._request("POST", f"/api/backfills/{_seg(backfill_id)}/cancel")

    # ── dead letters ──────────────────────────────────────────────────────────

    def list_dead_letters(self, *, limit: int = 100) -> List[Dict[str, Any]]:
        """List parked poison submissions, newest failure first."""
        return self._request("GET", "/api/dead-letters", params={"limit": limit})

    def redrive_dead_letter(self, dead_letter_id: str) -> Dict[str, Any]:
        """Re-attempt a parked payload as a fresh run. Returns ``{"run_id", "redriven_from"}``."""
        return self._request("POST", f"/api/dead-letters/{_seg(dead_letter_id)}/redrive")

    def discard_dead_letter(self, dead_letter_id: str) -> None:
        """Discard a parked payload."""
        self._request("DELETE", f"/api/dead-letters/{_seg(dead_letter_id)}", parse_json=False)

    # ── GitOps repository registry ────────────────────────────────────────────

    def list_git_repos(self) -> List[Dict[str, Any]]:
        """List tracked GitOps repositories."""
        return self._request("GET", "/api/git-repos")

    def connect_git_repo(
        self,
        url: str,
        *,
        branch: Optional[str] = None,
        auto_sync: bool = False,
        path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Register (connect) a Git repository. ``path`` scopes discovery to a
        subdirectory of the repo (server default ``dagron`` when omitted)."""
        body: Dict[str, Any] = {"url": url, "branch": branch, "auto_sync": auto_sync}
        if path is not None:
            body["path"] = path
        return self._request("POST", "/api/git-repos", body=body)

    def sync_git_repo(self, repo_id: str) -> Dict[str, Any]:
        """Mark a tracked repo synced now."""
        return self._request("POST", f"/api/git-repos/{_seg(repo_id)}/sync")

    def disconnect_git_repo(self, repo_id: str) -> None:
        """Stop tracking (disconnect) a repo."""
        self._request("DELETE", f"/api/git-repos/{_seg(repo_id)}", parse_json=False)

    # ── observability ─────────────────────────────────────────────────────────

    def metrics(self) -> Dict[str, Any]:
        """Live run/task counts by status plus the dead-letter total (JSON gauges)."""
        return self._request("GET", "/api/metrics")

    def healthz(self) -> str:
        """Liveness probe (``"ok"`` while the gateway is serving). Unauthenticated."""
        return self._request("GET", "/healthz", parse_json=False, auth=False)

    # ── transport ─────────────────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: Optional[Any] = None,
        params: Optional[Mapping[str, Any]] = None,
        parse_json: bool = True,
        auth: bool = True,
    ) -> Any:
        """Issue one request; return parsed JSON (or text), or raise :class:`DagronError`."""
        url = self.base_url + path
        if params:
            query = {k: v for k, v in params.items() if v is not None}
            if query:
                url += "?" + urllib.parse.urlencode(query)

        data = None
        headers: Dict[str, str] = {"accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["content-type"] = "application/json"
        if auth and self.token:
            headers["authorization"] = f"Bearer {self.token}"

        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:  # noqa: S310 (scheme checked)
                raw = resp.read()
        except urllib.error.HTTPError as e:
            raise DagronError._from_body(e.code, e.read()) from None
        except urllib.error.URLError as e:
            raise DagronError(0, f"request to {url} failed: {e.reason}") from None

        if not parse_json:
            return raw.decode("utf-8")
        if not raw:
            return None
        return json.loads(raw)


# ── helpers ───────────────────────────────────────────────────────────────────


def _spec_to_str(spec: SpecLike) -> str:
    """Coerce a Dag / mapping / string into the YAML-or-JSON spec string the API wants."""
    if isinstance(spec, Dag):
        return spec.to_json()
    if isinstance(spec, str):
        return spec
    if isinstance(spec, Mapping):
        return json.dumps(spec)
    raise TypeError("spec must be a Dag, a mapping, or a YAML/JSON string")


def _normalize_env(
    env: Union[Mapping[str, str], Sequence[Mapping[str, str]]],
) -> List[Dict[str, str]]:
    """Accept env as a ``{name: value}`` map or a list of ``{"name", "value"}`` and
    normalise to the engine's ``[{"name", "value"}]`` shape."""
    if isinstance(env, Mapping):
        return [{"name": str(k), "value": str(v)} for k, v in env.items()]
    out: List[Dict[str, str]] = []
    for item in env:
        if not isinstance(item, Mapping) or "name" not in item or "value" not in item:
            raise TypeError("env list items must be {'name': ..., 'value': ...} mappings")
        out.append({"name": str(item["name"]), "value": str(item["value"])})
    return out


def _seg(value: str) -> str:
    """Percent-encode a single path segment (ids are UUIDs, but never trust input)."""
    return urllib.parse.quote(str(value), safe="")


def _parse_sse(lines: Iterable[bytes]) -> Iterator[Dict[str, Any]]:
    """Minimal Server-Sent-Events parser: group ``event:``/``data:`` lines into
    one dict per blank-line-delimited event, JSON-decoding the data when possible."""
    event: Optional[str] = None
    data_lines: List[str] = []
    for raw_line in lines:
        line = raw_line.decode("utf-8", "replace").rstrip("\r\n")
        if line == "":  # dispatch on the blank line that terminates an event
            if data_lines:
                payload = "\n".join(data_lines)
                yield {"event": event or "message", "data": _maybe_json(payload)}
            event, data_lines = None, []
            continue
        if line.startswith(":"):  # comment / keep-alive ping
            continue
        field, _, rest = line.partition(":")
        value = rest[1:] if rest.startswith(" ") else rest
        if field == "event":
            event = value
        elif field == "data":
            data_lines.append(value)
    # Flush a trailing event with no terminating blank line.
    if data_lines:
        yield {"event": event or "message", "data": _maybe_json("\n".join(data_lines))}


def _maybe_json(text: str) -> Any:
    """Parse ``text`` as JSON, returning the raw string when it isn't valid JSON."""
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return text
