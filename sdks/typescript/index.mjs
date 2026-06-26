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
 */

export class Dag {
  /** @param {string} name */
  constructor(name) {
    if (!name) throw new Error("Dag requires a name");
    /** @type {string} */
    this.name = name;
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
    return { name: this.name, tasks: this._tasks };
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
