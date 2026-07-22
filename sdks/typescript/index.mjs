// dagron TypeScript/JavaScript SDK (v0) — define a DAG in code, emit dagron spec,
// submit to dagron-api. Zero runtime dependencies (uses global fetch, Node 18+).
//
// The emitted JSON is valid dagron input: dagron parses YAML, and JSON is a YAML
// subset, so `toJSON()` can be POSTed to /api/runs directly.

/**
 * @typedef {Object} TaskOptions
 * @property {string} [image]        Container image (docker_image).
 * @property {string[]} [command]    argv to run.
 * @property {string[]} [dependsOn]  Names of upstream tasks.
 * @property {string} [runnerClass]  Runner class (pool) that may claim this task.
 */

export class Dag {
  /**
   * @param {string} name
   * @param {{ runnerClass?: string }} [opts]  `runnerClass` is the workflow-level
   *   default runner pool for tasks that don't set their own (e.g. "etl",
   *   "pulse", "ml_training").
   */
  constructor(name, opts = {}) {
    if (!name) throw new Error("Dag requires a name");
    /** @type {string} */
    this.name = name;
    /** @type {string | undefined} */
    this.runnerClass = opts.runnerClass;
    /** @type {Array<Record<string, unknown>>} */
    this._tasks = [];
    /** @type {Set<string>} */
    this._names = new Set();
  }

  /**
   * Add a task. Returns its name so it can be passed to a later task's dependsOn.
   * @param {string} name
   * @param {TaskOptions} [opts]
   * @returns {string}
   */
  task(name, opts = {}) {
    if (!name) throw new Error("task requires a name");
    if (this._names.has(name)) throw new Error(`duplicate task '${name}'`);
    this._names.add(name);
    /** @type {Record<string, unknown>} */
    const t = { name };
    if (opts.image) t.docker_image = opts.image;
    if (opts.command && opts.command.length) t.command = opts.command;
    if (opts.dependsOn && opts.dependsOn.length) t.depends_on = opts.dependsOn;
    if (opts.runnerClass) t.runner_class = opts.runnerClass;
    this._tasks.push(t);
    return name;
  }

  /** Build the dagron spec object (validates dependency references). */
  toSpec() {
    for (const t of this._tasks) {
      for (const d of /** @type {string[]} */ (t.depends_on ?? [])) {
        if (!this._names.has(d)) {
          throw new Error(`task '${t.name}' depends on unknown task '${d}'`);
        }
      }
    }
    const spec = { name: this.name, tasks: this._tasks };
    if (this.runnerClass) spec.runner_class = this.runnerClass;
    return spec;
  }

  /** dagron spec as JSON (valid dagron input — YAML is a JSON superset). */
  toJSON() {
    return JSON.stringify(this.toSpec());
  }

  /**
   * Submit the DAG as a run to dagron-api (POST /api/runs).
   *
   * The gateway expects the spec wrapped as `{"yaml": "<spec>"}` (a spec string
   * under the `yaml` key — JSON is accepted since it is a YAML subset), so we
   * wrap `toJSON()` rather than posting it raw. The response is `{"run_id": ...}`;
   * this returns the `run_id`.
   * @param {string} apiUrl  e.g. "http://localhost:8080"
   * @param {{ token?: string }} [opts]
   * @returns {Promise<string>} the new run id
   */
  async submit(apiUrl, opts = {}) {
    const headers = { "content-type": "application/json" };
    if (opts.token) headers["authorization"] = `Bearer ${opts.token}`;
    const res = await fetch(`${apiUrl.replace(/\/$/, "")}/api/runs`, {
      method: "POST",
      headers,
      body: JSON.stringify({ yaml: this.toJSON() }),
    });
    const body = await res.text();
    if (!res.ok) throw new Error(`dagron-api ${res.status}: ${body}`);
    try {
      const parsed = JSON.parse(body);
      if (typeof parsed === "string") return parsed;
      if (parsed && typeof parsed === "object" && typeof parsed.run_id === "string") {
        return parsed.run_id;
      }
      throw new Error(`dagron-api success response missing string run_id: ${body}`);
    } catch (err) {
      if (err instanceof SyntaxError) {
        // Be forgiving if a future gateway returns the id as a bare string.
        return body;
      }
      throw err;
    }
  }
}

const TERMINAL_RUN_STATUSES = new Set(["succeeded", "failed", "cancelled"]);

/** Percent-encode a single path segment (ids are UUIDs, but never trust input). */
function seg(value) {
  return encodeURIComponent(String(value));
}

/** Coerce a Dag / object / string into the spec string the API wants. */
function specToStr(spec) {
  if (spec instanceof Dag) return spec.toJSON();
  if (typeof spec === "string") return spec;
  if (spec && typeof spec === "object") return JSON.stringify(spec);
  throw new TypeError("spec must be a Dag, an object, or a YAML/JSON string");
}

/** Parse text as JSON, returning the raw string when it isn't valid JSON. */
function maybeJson(text) {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Error raised by {@link Client}. `status` is the HTTP code (`0` for a
 * transport-level failure), `message` the server's error text (unwrapped from
 * `{"error": ...}` when present), and `body` the raw response body.
 */
export class DagronError extends Error {
  constructor(status, message, body) {
    super(status ? `dagron-api ${status}: ${message}` : message);
    this.name = "DagronError";
    this.status = status;
    this.body = body;
  }

  /** Build an error from a Response, unwrapping `{"error": ...}` when present. */
  static async _fromResponse(res) {
    const text = await res.text();
    let message = text;
    try {
      const parsed = JSON.parse(text);
      if (parsed && typeof parsed === "object" && typeof parsed.error === "string") {
        message = parsed.error;
      }
    } catch {
      // not JSON — keep the raw text
    }
    message = (message || "").trim() || `HTTP ${res.status}`;
    return new DagronError(res.status, message, text);
  }
}

/**
 * Typed client for the dagron-api gateway (`/api/...`). Zero runtime deps (global
 * `fetch`, Node 18+). Construct with the gateway base URL and either a session JWT
 * (`token`) or call {@link Client#login}. Every authed call sends
 * `Authorization: Bearer <token>`; rotate via the `token` property.
 */
export class Client {
  /** @param {string} baseUrl @param {{ token?: string, timeout?: number }} [opts] */
  constructor(baseUrl, opts = {}) {
    let scheme;
    try {
      scheme = new URL(baseUrl).protocol;
    } catch {
      throw new Error("base_url must be a valid http(s) URL");
    }
    // Restrict to HTTP(S) so a bad base_url can't leak the bearer token or reach
    // file://-style targets (SSRF), mirroring the Python client.
    if (scheme !== "http:" && scheme !== "https:") {
      throw new Error("base_url must use http or https");
    }
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.token = opts.token ?? null;
    this.timeout = opts.timeout ?? 30000; // ms
  }

  // ── auth ────────────────────────────────────────────────────────────────────

  async login(email, password) {
    const body = await this._request("POST", "/api/login", {
      body: { email, password },
      auth: false,
    });
    const token = body && typeof body === "object" ? body.token : null;
    if (!token) throw new DagronError(0, "login succeeded but no token was returned");
    this.token = token;
    return token;
  }

  async logout() {
    await this._request("POST", "/api/logout", { parseJson: false });
    this.token = null;
  }

  async me() {
    return this._request("GET", "/api/me");
  }

  async createUser(email, password, name, groups) {
    return this._request("POST", "/api/users", {
      body: { email, password, name, groups: groups ?? [] },
    });
  }

  // ── runs ────────────────────────────────────────────────────────────────────

  async submitRun(spec) {
    return (await this._request("POST", "/api/runs", { body: { yaml: specToStr(spec) } })).run_id;
  }

  async listRuns({ status, limit, offset } = {}) {
    return this._request("GET", "/api/runs", { params: { status, limit, offset } });
  }

  async getRun(runId) {
    return this._request("GET", `/api/runs/${seg(runId)}`);
  }

  async getRunGraph(runId) {
    return this._request("GET", `/api/runs/${seg(runId)}/graph`);
  }

  async getTaskLogs(runId, taskId) {
    return this._request("GET", `/api/runs/${seg(runId)}/tasks/${seg(taskId)}/logs`);
  }

  async cancelRun(runId) {
    return (await this._request("POST", `/api/runs/${seg(runId)}/cancel`)).cancelled;
  }

  async rerunRun(runId, { params } = {}) {
    const body = params ? { params } : {};
    return this._request("POST", `/api/runs/${seg(runId)}/rerun`, { body });
  }

  async resubmitRun(runId) {
    return (await this._request("POST", `/api/runs/${seg(runId)}/resubmit`)).run_id;
  }

  async retryTask(runId, taskId) {
    return (await this._request("POST", `/api/runs/${seg(runId)}/tasks/${seg(taskId)}/retry`)).retried;
  }

  /** Approve a `type: approval` gate: the task succeeds and its dependents advance. */
  async approveTask(runId, taskId) {
    return this._request("POST", `/api/runs/${seg(runId)}/tasks/${seg(taskId)}/approve`);
  }

  /** Reject a `type: approval` gate: the task fails and its `all_success` dependents skip. */
  async rejectTask(runId, taskId) {
    return this._request("POST", `/api/runs/${seg(runId)}/tasks/${seg(taskId)}/reject`);
  }

  /**
   * Yield live task-state events for a run as Server-Sent Events. Each item is
   * `{ event, data }` (data is JSON-parsed when possible). Runs until the stream
   * closes; a `resync` event means the client fell behind — refetch via
   * {@link Client#getRunGraph}.
   */
  async *streamRun(runId, { timeout } = {}) {
    const url = `${this.baseUrl}/api/runs/${seg(runId)}/stream`;
    const headers = { accept: "text/event-stream" };
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    const ctrl = new AbortController();
    const timer = timeout ? setTimeout(() => ctrl.abort(), timeout) : null;
    let res;
    try {
      res = await fetch(url, { method: "GET", headers, signal: ctrl.signal });
    } catch (err) {
      if (timer) clearTimeout(timer);
      throw new DagronError(0, `request to ${url} failed: ${err?.message ?? err}`);
    }
    if (!res.ok) {
      if (timer) clearTimeout(timer);
      throw await DagronError._fromResponse(res);
    }
    const decoder = new TextDecoder();
    let buf = "";
    let event = null;
    let dataLines = [];
    // Collect complete SSE frames from `res.body`, yielding outside the reader's
    // try so a network error mid-stream becomes a DagronError (consistent with the
    // rest of the client) while a consumer `break` still runs `finally` and
    // releases the underlying HTTP connection.
    const drain = async function* () {
      try {
        for await (const chunk of res.body) {
          buf += decoder.decode(chunk, { stream: true });
          let nl;
          while ((nl = buf.indexOf("\n")) >= 0) {
            let line = buf.slice(0, nl);
            buf = buf.slice(nl + 1);
            if (line.endsWith("\r")) line = line.slice(0, -1);
            if (line === "") {
              if (dataLines.length) {
                yield { event: event ?? "message", data: maybeJson(dataLines.join("\n")) };
              }
              event = null;
              dataLines = [];
              continue;
            }
            if (line.startsWith(":")) continue; // comment / keep-alive
            const c = line.indexOf(":");
            const field = c === -1 ? line : line.slice(0, c);
            let value = c === -1 ? "" : line.slice(c + 1);
            if (value.startsWith(" ")) value = value.slice(1);
            if (field === "event") event = value;
            else if (field === "data") dataLines.push(value);
          }
        }
        if (dataLines.length) {
          yield { event: event ?? "message", data: maybeJson(dataLines.join("\n")) };
        }
      } catch (err) {
        throw new DagronError(0, `stream for ${url} failed: ${err?.message ?? err}`);
      }
    };
    try {
      yield* drain();
    } finally {
      if (timer) clearTimeout(timer);
      // Release the SSE connection if the consumer bailed out early (break/throw)
      // before the stream ended on its own.
      try {
        await res.body?.cancel();
      } catch {
        // already closed / consumed — nothing to release
      }
    }
  }

  /**
   * Poll {@link Client#getRun} until the run reaches a terminal state; resolves to
   * it. Rejects if `timeout` ms elapse first (`null` waits forever).
   */
  async waitForRun(runId, { pollInterval = 2000, timeout = 300000 } = {}) {
    const deadline = timeout == null ? null : Date.now() + timeout;
    for (;;) {
      const run = await this.getRun(runId);
      if (TERMINAL_RUN_STATUSES.has(run.status)) return run;
      if (deadline != null && Date.now() >= deadline) {
        throw new Error(`run '${runId}' did not finish within ${timeout}ms`);
      }
      await sleep(pollInterval);
    }
  }

  // ── workflows ───────────────────────────────────────────────────────────────

  async listWorkflows() {
    return this._request("GET", "/api/workflows");
  }

  async getWorkflow(workflowId) {
    return this._request("GET", `/api/workflows/${seg(workflowId)}`);
  }

  async createWorkflow(spec, { name, description } = {}) {
    return this._request("POST", "/api/workflows", {
      body: { spec: specToStr(spec), name, description },
    });
  }

  async updateWorkflow(workflowId, spec, { name, description } = {}) {
    return this._request("PUT", `/api/workflows/${seg(workflowId)}`, {
      body: { spec: specToStr(spec), name, description },
    });
  }

  async deleteWorkflow(workflowId) {
    await this._request("DELETE", `/api/workflows/${seg(workflowId)}`, { parseJson: false });
  }

  async runWorkflow(workflowId) {
    return this._request("POST", `/api/workflows/${seg(workflowId)}/run`);
  }

  async syncWorkflowToGit(workflowId) {
    return this._request("POST", `/api/workflows/${seg(workflowId)}/sync-to-git`);
  }

  // ── schedules ───────────────────────────────────────────────────────────────

  async listSchedules({ workflowId } = {}) {
    return this._request("GET", "/api/schedules", { params: { workflow_id: workflowId } });
  }

  async createSchedule(workflowId, cronExpr, { enabled = true } = {}) {
    return this._request("POST", "/api/schedules", {
      body: { workflow_id: workflowId, cron_expr: cronExpr, enabled },
    });
  }

  async updateSchedule(scheduleId, { cronExpr, enabled } = {}) {
    const body = {};
    if (cronExpr !== undefined) body.cron_expr = cronExpr;
    if (enabled !== undefined) body.enabled = enabled;
    return this._request("PUT", `/api/schedules/${seg(scheduleId)}`, { body });
  }

  async deleteSchedule(scheduleId) {
    await this._request("DELETE", `/api/schedules/${seg(scheduleId)}`, { parseJson: false });
  }

  /** Synchronous materialise of a schedule's missed runs over `[from, to]` (RFC3339). */
  async backfillSchedule(scheduleId, from, to, { maxRuns } = {}) {
    const body = { from, to };
    if (maxRuns != null) body.max_runs = maxRuns;
    return this._request("POST", `/api/schedules/${seg(scheduleId)}/backfill`, { body });
  }

  // ── backfill jobs (durable, paced) ──────────────────────────────────────────

  /** Create a durable, paced backfill job over `[from, to]` (RFC3339). */
  async createBackfill(scheduleId, from, to, { maxRuns } = {}) {
    const body = { schedule_id: scheduleId, from, to };
    if (maxRuns != null) body.max_runs = maxRuns;
    return this._request("POST", "/api/backfills", { body });
  }

  /** List backfill jobs, newest first; filter by `scheduleId`. */
  async listBackfills({ scheduleId, limit } = {}) {
    return this._request("GET", "/api/backfills", { params: { schedule_id: scheduleId, limit } });
  }

  async getBackfill(backfillId) {
    return this._request("GET", `/api/backfills/${seg(backfillId)}`);
  }

  async cancelBackfill(backfillId) {
    return this._request("POST", `/api/backfills/${seg(backfillId)}/cancel`);
  }

  // ── dead letters ────────────────────────────────────────────────────────────

  async listDeadLetters({ limit = 100 } = {}) {
    return this._request("GET", "/api/dead-letters", { params: { limit } });
  }

  async redriveDeadLetter(deadLetterId) {
    return this._request("POST", `/api/dead-letters/${seg(deadLetterId)}/redrive`);
  }

  async discardDeadLetter(deadLetterId) {
    await this._request("DELETE", `/api/dead-letters/${seg(deadLetterId)}`, { parseJson: false });
  }

  // ── GitOps repository registry ──────────────────────────────────────────────

  async listGitRepos() {
    return this._request("GET", "/api/git-repos");
  }

  /** Register a Git repository. `path` scopes discovery to a subdir (server default `dagron`). */
  async connectGitRepo(url, { branch, autoSync = false, path } = {}) {
    const body = { url, branch, auto_sync: autoSync };
    if (path != null) body.path = path;
    return this._request("POST", "/api/git-repos", { body });
  }

  async syncGitRepo(repoId) {
    return this._request("POST", `/api/git-repos/${seg(repoId)}/sync`);
  }

  async disconnectGitRepo(repoId) {
    await this._request("DELETE", `/api/git-repos/${seg(repoId)}`, { parseJson: false });
  }

  // ── observability ───────────────────────────────────────────────────────────

  async metrics() {
    return this._request("GET", "/api/metrics");
  }

  async healthz() {
    return this._request("GET", "/healthz", { parseJson: false, auth: false });
  }

  // ── transport ───────────────────────────────────────────────────────────────

  /** Issue one request; resolve to parsed JSON (or text/null), or throw {@link DagronError}. */
  async _request(method, path, { body, params, parseJson = true, auth = true } = {}) {
    let url = this.baseUrl + path;
    if (params) {
      const usp = new URLSearchParams();
      for (const [k, v] of Object.entries(params)) {
        if (v !== undefined && v !== null) usp.append(k, String(v));
      }
      const qs = usp.toString();
      if (qs) url += `?${qs}`;
    }

    const headers = { accept: "application/json" };
    let data;
    if (body !== undefined && body !== null) {
      data = JSON.stringify(body);
      headers["content-type"] = "application/json";
    }
    if (auth && this.token) headers.authorization = `Bearer ${this.token}`;

    const ctrl = new AbortController();
    const timer = this.timeout ? setTimeout(() => ctrl.abort(), this.timeout) : null;
    let res;
    try {
      res = await fetch(url, { method, headers, body: data, signal: ctrl.signal });
    } catch (err) {
      throw new DagronError(0, `request to ${url} failed: ${err?.message ?? err}`);
    } finally {
      if (timer) clearTimeout(timer);
    }

    if (!res.ok) throw await DagronError._fromResponse(res);
    if (!parseJson) return res.text();
    const text = await res.text();
    if (!text) return null;
    return JSON.parse(text);
  }
}
