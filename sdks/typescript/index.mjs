// dagron TypeScript/JavaScript SDK — define a DAG in code, emit dagron spec, and
// drive the dagron-api control plane. Zero runtime dependencies (global fetch,
// Node 18+).
//
// The emitted JSON is valid dagron input: dagron parses YAML, and JSON is a YAML
// subset, so `toJSON()` can be POSTed to /api/runs directly.
//
// The version tracks the dagron-api version this SDK covers, not the SDK's own
// feature history — `0.9.x` means "speaks to the 0.9 gateway".

// The one import: a Recipe's tag is the SHA-256 of its canonical form, and it
// must agree byte for byte with the Rust builder and the Python SDK.
import { createHash } from "node:crypto";

/**
 * Seconds of transport headroom added on top of a server-side wait budget, so
 * the client's own timeout can never abort a long poll a moment before the
 * gateway answers it.
 */
const WAIT_TRANSPORT_MARGIN_SECS = 5;

/**
 * The server's bounds on `GET /api/runs/{id}/wait?timeout_secs=` — it clamps to
 * this range and defaults to the low end, so the client can size its transport
 * timeout against the budget the server will actually honour.
 */
const WAIT_BUDGET_DEFAULT_SECS = 30;
const WAIT_BUDGET_MIN_SECS = 1;
const WAIT_BUDGET_MAX_SECS = 600;

/**
 * The keys a `repeat:` block may carry, as the engine spells them. Checked at
 * build time because the alternative is a spec that validates locally and is
 * then rejected on the wire — a camelCased `maxIterations`, say, would leave
 * the required field unset with nothing to say so.
 */
const REPEAT_KEYS = Object.freeze(["until", "max_iterations", "delay_secs"]);

/** Run statuses the engine treats as terminal (no further transitions). */
const TERMINAL_RUN_STATUSES = new Set(["succeeded", "failed", "cancelled"]);

/**
 * When a task runs relative to its dependencies' outcomes (engine
 * `trigger_rule`). Unset means `all_success`.
 */
export const TRIGGER_RULES = Object.freeze([
  "all_success",
  "all_done",
  "one_failed",
  "all_failed",
  "none_failed",
]);

/**
 * Task kinds that run no command: they park instead (a human gate, a child-run
 * trigger, a deferrable sensor). Each is refused a `command` / `template` /
 * `workflowRef`, mirroring the server.
 */
export const COMMANDLESS_TASK_TYPES = Object.freeze(["approval", "workflow", "wait"]);

/**
 * Every option `task()` accepts. An unknown key throws rather than being
 * dropped: an options bag that silently swallows `retryDelay` for
 * `retryDelaySecs` produces a task with no retry delay and no complaint, which
 * is the failure this SDK already refuses for log filters.
 */
const TASK_OPTION_KEYS = Object.freeze([
  "image", "command", "dependsOn", "workflowRef", "template", "arguments",
  "taskType", "workflow", "wait", "approvalTimeoutSecs", "approvalOnTimeout",
  "input", "when", "triggerRule", "hook", "allowFailure", "withItems",
  "withParam", "instanceKey", "maxAttempts", "retryDelaySecs",
  "retryMaxDelaySecs", "retryOnTimeout", "retryBudgets", "timeoutSecs", "env",
  "resources", "serviceAccount", "runnerClass", "pool", "priority", "cache",
  "repeat", "produces", "gang", "isolation",
]);

/** Every option the `Dag` constructor accepts. Same reasoning as above. */
const DAG_OPTION_KEYS = Object.freeze([
  "runnerClass", "parameters", "tags", "environment", "taskDefaults",
  "runTimeoutSecs", "maxActiveRuns", "resultFrom", "budget", "deadline",
  "notify", "onDatasets", "datasetsMode",
  // The three that matter only when a task's `image` is a Recipe.
  "imageRepository", "buildRunnerClass", "buildTimeoutSecs",
]);

/** Throw on any key outside `allowed`, naming what was expected. */
function rejectUnknownOptions(opts, allowed, what) {
  for (const key of Object.keys(opts)) {
    if (!allowed.includes(key)) {
      throw new TypeError(
        `unknown ${what} option "${key}"; expected one of ${allowed.join(", ")}`,
      );
    }
  }
}

/**
 * Normalise env to the engine's `[{name, value|value_from}]` shape.
 *
 * Accepts a `{NAME: value}` object for the common literal case, or an array of
 * entries — each either `{name, value}` or `{name, valueFrom: {secret}}` (or the
 * secret name directly), which resolves a secret at dispatch so the credential
 * never lands in the spec or the datastore.
 */
function normalizeEnv(env) {
  if (!Array.isArray(env)) {
    return Object.entries(env).map(([name, value]) => ({ name, value: String(value) }));
  }
  return env.map((item) => {
    if (!item || typeof item !== "object" || !item.name) {
      throw new TypeError(
        "env array items must be {name, value} or {name, valueFrom: {secret}} objects",
      );
    }
    const from = item.valueFrom ?? item.value_from;
    if (from !== undefined) {
      const secret = typeof from === "string" ? from : from?.secret;
      if (!secret) throw new TypeError("env valueFrom must be {secret} (or the name itself)");
      const entry = { name: String(item.name), value_from: { secret: String(secret) } };
      if (item.value !== undefined) entry.value = String(item.value);
      return entry;
    }
    if (item.value === undefined) {
      throw new TypeError("env array items must set `value` or `valueFrom`");
    }
    return { name: String(item.name), value: String(item.value) };
  });
}

/**
 * Drop unset keys from a `wait:` sensor spec, keeping only what was chosen, and
 * map the JS-friendly `for`/`duration` spelling onto the wire's `for`.
 *
 * `sensor()` passes all four forms with `undefined` for the ones the caller left
 * out; emitting those would make the server see four keys set to nothing.
 */
function normalizeWait(wait) {
  const out = {};
  for (const [key, value] of Object.entries(wait)) {
    if (value === undefined || value === null) continue;
    const wire = key === "duration" ? "for" : key;
    if (!["for", "until", "url", "dataset"].includes(wire)) {
      throw new TypeError(
        `unknown wait key "${key}"; expected one of duration (for), until, url, dataset`,
      );
    }
    out[wire] = value;
  }
  return out;
}

/**
 * Mirror the server's runner-class rule: lowercase `[a-z0-9_-]`, 1-64 chars.
 * `other` is refused because it is the metrics tail bucket — a task routed there
 * would vanish into the bucket that counts everything else.
 */
function validateRunnerClass(name, where) {
  if (!name || name.length > 64) {
    throw new Error(
      `invalid runner_class for ${where}: must be 1-64 characters, got ${name.length} ('${name}')`,
    );
  }
  if (!/^[a-z0-9_-]+$/.test(name)) {
    throw new Error(`invalid runner_class for ${where}: '${name}' may only contain [a-z0-9_-]`);
  }
  if (name === "other") {
    throw new Error(
      `invalid runner_class for ${where}: 'other' is reserved (the metrics tail bucket)`,
    );
  }
}

/**
 * Task names a `when:` reads as `{{ tasks.<name>.output }}`, in order. Mirrors
 * the server's `when_output_refs`: only that exact shape counts, so a
 * `{{ param }}` substituted at submit is not mistaken for a dependency.
 */
function whenOutputRefs(condition) {
  const refs = [];
  for (const match of String(condition).matchAll(/\{\{([\s\S]*?)\}\}/g)) {
    const key = match[1].trim();
    if (key.startsWith("tasks.") && key.endsWith(".output")) {
      const name = key.slice("tasks.".length, -".output".length);
      if (name && !refs.includes(name)) refs.push(name);
    }
  }
  return refs;
}

/**
 * The task-list half shared by a {@link Dag} and each {@link Template}.
 *
 * Both hold an ordered list of tasks under a name, add them through the same
 * `task()` signature, and run the same per-task structural checks — a template's
 * sub-graph is validated exactly like the top-level graph server-side, so
 * sharing the code here is what keeps the two from drifting.
 */

/**
 * Identifies the recipe format *and* the Dockerfile the builder synthesises from
 * it. It is hashed with the recipe, so bumping it re-tags every image on
 * purpose: two images with the same tag must have been built the same way.
 * Must equal `GENERATOR_VERSION` in `ee/dagron-build/src/recipe.rs` and
 * `BUILD_GENERATOR_VERSION` in the Python SDK.
 */
export const BUILD_GENERATOR_VERSION = "dagron-build/v2";

/**
 * Env keys the builder accepts. Enforced here for a reason that is not obvious:
 * the canonical form sorts env by key, and JavaScript sorts differently from
 * Rust in two ways that would silently change the tag.
 *
 * 1. A key that looks like an integer jumps to the front of a plain object in
 *    numeric order, whatever order it was inserted in — `{"10":…,"9":…}`
 *    stringifies with `9` first in Rust and `10` first here.
 * 2. `Array.prototype.sort` compares UTF-16 code units, while Rust's `BTreeMap`
 *    compares UTF-8 bytes. They disagree above the BMP: an astral character
 *    starts with a surrogate (0xD800) and sorts *below* U+E000–U+FFFF here,
 *    and *above* it there.
 *
 * This pattern makes both unreachable — no leading digit, ASCII only — which is
 * why it is not optional politeness. The builder applies the same rule.
 */
const ENV_KEY_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;
const RECIPE_NAME_RE = /^[a-z0-9]([a-z0-9._-]*[a-z0-9])?$/;
/**
 * A UTF-16 surrogate with no partner. JavaScript strings can hold one; UTF-8
 * cannot represent it, so `JSON.stringify` emits `\ud800` and the builder's
 * parser stops at "unexpected end of hex escape" — the build task then fails on
 * its own recipe, minutes after the spec looked fine. There is no tag to
 * disagree about here; there is simply no recipe.
 */
const LONE_SURROGATE_RE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;
/** The builder's ceiling on a recipe name — it becomes a repository path component. */
const MAX_RECIPE_NAME_LEN = 128;

/** A text file the recipe places in the image. */
export class RecipeFile {
  /**
   * @param {string} path      absolute destination inside the image
   * @param {string} content   the file's text
   * @param {{ executable?: boolean }} [opts]  executable → mode 0755, else 0644
   */
  constructor(path, content, opts = {}) {
    /** @type {string} */
    this.path = path;
    /** @type {string} */
    this.content = content;
    /** @type {boolean} */
    this.executable = Boolean(opts.executable);
  }

  /**
   * The canonical form. `executable` is omitted when false because the builder
   * omits it when false, and this object is hashed.
   * @returns {Record<string, unknown>}
   */
  toObject() {
    const o = { path: this.path, content: this.content };
    if (this.executable) o.executable = true;
    return o;
  }
}

/**
 * What a task's image should contain, instead of a Dockerfile.
 *
 * Pass one to {@link Dag#task} as `image` and the build task is added for you:
 *
 * ```js
 * const recipe = new Recipe("etl", "python:3.12-slim", { pip: ["duckdb==1.1.3"] });
 * const dag = new Dag("nightly");
 * dag.task("report", { image: recipe, command: ["python", "/app/report.py"] });
 * ```
 *
 * **The tag is a function of the recipe.** `tag()` is `r-<16 hex of sha256>`
 * over the canonical form, so the image reference is known before the image
 * exists: downstream tasks can name it at author time, and re-submitting an
 * unchanged recipe is a registry lookup rather than a build.
 *
 * That makes this class a *contract*, not a convenience. The program that
 * actually produces the image is written in another language
 * (`ee/dagron-build`), and if the two disagree about the canonical form by one
 * byte, an author pins a task to an image no build will ever push and nothing
 * fails until the run does. `sdks/recipe-vectors.json` holds golden vectors
 * generated from the builder; `recipe.test.mjs` asserts against them.
 */
export class Recipe {
  /**
   * @param {string} name  the repository's last component (`etl`); the registry
   *   and workspace prefix are supplied where the build runs, not here
   * @param {string} base  any image reference
   * @param {Object} [opts]
   * @param {string[]} [opts.apt]        packages installed with apt-get
   * @param {string[]} [opts.pip]        requirement specifiers installed with pip
   * @param {Record<string,string>} [opts.env]  ENV lines — **baked into the
   *   image** and visible to anyone who can pull it
   * @param {string} [opts.workdir]      WORKDIR, absolute
   * @param {Array<RecipeFile | {path: string, content: string, executable?: boolean}>} [opts.files]
   * @param {string[]} [opts.run]        extra RUN lines, in order
   * @param {string} [opts.user]         final USER
   * @param {boolean} [opts.keepEntrypoint]  keep the base image's ENTRYPOINT
   * @param {string} [opts.platform]     e.g. "linux/arm64"
   * @param {string} [opts.dockerfile]   verbatim Dockerfile; every field except
   *   name, files and platform is then ignored, though all of them still hash
   */
  constructor(name, base, opts = {}) {
    /** @type {string} */
    this.name = name;
    /** @type {string} */
    this.base = base;
    /** @type {string[]} */
    this.apt = [...(opts.apt ?? [])];
    /** @type {string[]} */
    this.pip = [...(opts.pip ?? [])];
    /** @type {Record<string,string>} */
    this.env = { ...(opts.env ?? {}) };
    /** @type {string | undefined} */
    this.workdir = opts.workdir;
    /** @type {RecipeFile[]} */
    this.files = (opts.files ?? []).map(asRecipeFile);
    /** @type {string[]} */
    this.run = [...(opts.run ?? [])];
    /** @type {string | undefined} */
    this.user = opts.user;
    /** @type {boolean} */
    this.keepEntrypoint = Boolean(opts.keepEntrypoint);
    /** @type {string | undefined} */
    this.platform = opts.platform;
    /** @type {string | undefined} */
    this.dockerfile = opts.dockerfile;
  }

  /**
   * The recipe as the builder serialises it.
   *
   * Keys are inserted in the builder's **declaration order** — not alphabetical
   * — because `JSON.stringify` emits a plain object's keys in insertion order
   * and a Rust struct serialises in declaration order. Absent, empty and false
   * fields are omitted rather than emitted as `null` / `[]` / `false`, and env
   * is sorted by key, because the builder does both and this object is hashed.
   * @returns {Record<string, unknown>}
   */
  toObject() {
    /** @type {Record<string, unknown>} */
    const o = { name: this.name, base: this.base };
    if (this.apt.length) o.apt = [...this.apt];
    if (this.pip.length) o.pip = [...this.pip];
    const envKeys = Object.keys(this.env);
    if (envKeys.length) {
      /** @type {Record<string,string>} */
      const env = {};
      // Safe against JS's integer-key reordering only because ENV_KEY_RE
      // forbids a leading digit; `validate()` is what enforces that.
      for (const k of envKeys.sort()) env[k] = this.env[k];
      o.env = env;
    }
    // `!= null`, not `!== undefined`: the builder's fields are `Option`, and
    // serde reads a JSON `null` as `None` and then omits it. A recipe parsed
    // from a file — the documented way to get one — routinely carries an
    // explicit `dockerfile: null` (a bare `dockerfile:` in YAML is null), and
    // emitting it here made this SDK hash different bytes than the builder,
    // pinning a tag no build would ever push. Python got this right with
    // `is not None`; this line is why the two SDKs once disagreed.
    if (this.workdir != null) o.workdir = this.workdir;
    if (this.files.length) o.files = this.files.map((f) => f.toObject());
    if (this.run.length) o.run = [...this.run];
    if (this.user != null) o.user = this.user;
    if (this.keepEntrypoint) o.keep_entrypoint = true;
    if (this.platform != null) o.platform = this.platform;
    if (this.dockerfile != null) o.dockerfile = this.dockerfile;
    return o;
  }

  /**
   * The exact bytes that are hashed: `JSON.stringify` with no replacer and no
   * indent, over {@link Recipe#toObject}. JavaScript happens to make this the
   * easy case — it never escapes non-ASCII and never escapes `/`, which are the
   * two places other languages' defaults diverge from the builder.
   * @returns {string}
   */
  canonicalJson() {
    return JSON.stringify(this.toObject());
  }

  /**
   * Lowercase hex SHA-256 over the generator version and the canonical form.
   * The version is hashed too, so a change to how the Dockerfile is synthesised
   * re-tags every image rather than silently reusing one built by the old rules.
   * @returns {string}
   */
  hash() {
    return createHash("sha256")
      .update(BUILD_GENERATOR_VERSION, "utf8")
      .update("\n", "utf8")
      .update(this.canonicalJson(), "utf8")
      .digest("hex");
  }

  /** The content-addressed tag, `r-<16 hex>`. @returns {string} */
  tag() {
    return "r-" + this.hash().slice(0, 16);
  }

  /**
   * `<prefix>/<name>:<tag>`, or `<name>:<tag>` with no prefix. No prefix means a
   * daemon-local image: built and used on the same socket, never pushed.
   * @param {string} [repositoryPrefix]
   * @returns {string}
   */
  imageRef(repositoryPrefix = "") {
    // Strips every trailing slash, not just one, matching the builder.
    const prefix = repositoryPrefix.replace(/\/+$/, "");
    return prefix ? `${prefix}/${this.name}:${this.tag()}` : `${this.name}:${this.tag()}`;
  }

  /** Every author-supplied string, with something to call it. @returns {Array<[string,string]>} */
  _strings() {
    /** @type {Array<[string,string]>} */
    const out = [["name", this.name ?? ""], ["base", this.base ?? ""]];
    if (this.workdir != null) out.push(["workdir", this.workdir]);
    if (this.user != null) out.push(["user", this.user]);
    if (this.platform != null) out.push(["platform", this.platform]);
    if (this.dockerfile != null) out.push(["dockerfile", this.dockerfile]);
    for (const p of this.apt) out.push([`apt entry '${p}'`, p]);
    for (const p of this.pip) out.push([`pip entry '${p}'`, p]);
    for (const r of this.run) out.push([`run line '${r}'`, r]);
    for (const [k, v] of Object.entries(this.env)) {
      out.push([`env key '${k}'`, k], [`env '${k}'`, v]);
    }
    for (const f of this.files) {
      out.push([`file path '${f.path}'`, f.path], [`the contents of '${f.path}'`, f.content]);
    }
    return out;
  }

  /**
   * Refuse a recipe that could not produce a well-formed image, with the
   * builder's rules and roughly its messages, so a mistake surfaces where it was
   * made rather than in a build log twenty minutes later.
   *
   * Note this is *not* called by {@link Recipe#hash} — the builder does not
   * validate before hashing either — but {@link Dag#task} calls it before
   * emitting a spec, which is the path that matters.
   */
  validate() {
    for (const [what, value] of this._strings()) {
      if (LONE_SURROGATE_RE.test(value)) {
        throw new Error(
          `recipe '${this.name}': ${what} contains an unpaired UTF-16 surrogate, which cannot be ` +
            "encoded as UTF-8 — the builder cannot parse a recipe containing one"
        );
      }
    }
    if (!this.name || this.name.length > MAX_RECIPE_NAME_LEN || !RECIPE_NAME_RE.test(this.name)) {
      throw new Error(
        `recipe name '${this.name}' must be 1-${MAX_RECIPE_NAME_LEN} characters of ` +
          "[a-z0-9._-], starting and ending alphanumeric, with no '/' — the registry " +
          "and workspace prefix are added where the build runs"
      );
    }
    if (!this.base || isRustBlank(this.base) || hasRustWhitespace(this.base)) {
      throw new Error(`recipe '${this.name}': base must be a single image reference`);
    }
    noContinuation(this.name, "base", this.base);
    for (const [k, v] of Object.entries(this.env)) {
      if (!ENV_KEY_RE.test(k)) {
        throw new Error(
          `recipe '${this.name}': env key '${k}' must match [A-Za-z_][A-Za-z0-9_]*`
        );
      }
      noLineBreaks(this.name, "env", v);
      noContinuation(this.name, "env", v);
      noUserinfo(this.name, "env", v);
    }
    if (this.workdir != null) {
      absolutePath(this.name, "workdir", this.workdir);
      noContinuation(this.name, "workdir", this.workdir);
    }
    const seen = new Set();
    for (const f of this.files) {
      absolutePath(this.name, "file", f.path);
      noContinuation(this.name, "file", f.path);
      // Two entries for one path is a recipe that cannot say which content it
      // means; the builder refuses rather than pick.
      if (seen.has(f.path)) {
        throw new Error(`recipe '${this.name}': file '${f.path}' is listed twice`);
      }
      seen.add(f.path);
    }
    for (const [field, entries] of [["apt", this.apt], ["pip", this.pip], ["run", this.run]]) {
      for (const line of entries) {
        if (isRustBlank(line)) throw new Error(`recipe '${this.name}': ${field} has an empty entry`);
        noLineBreaks(this.name, field, line);
        noContinuation(this.name, field, line);
        noUserinfo(this.name, field, line);
        // A backslash in a package specifier is a shell habit that does not
        // mean anything here — `run` is the field for shell.
        if (field !== "run" && line.includes("\\")) {
          throw new Error(
            `recipe '${this.name}': ${field} entry '${line}' contains a backslash; ` +
              "entries are package specifiers"
          );
        }
        // An entry beginning with `-` is not a package, it is an OPTION to the
        // command that installs them, and both installers have options that run
        // code or move where packages come from: `apt-get -o
        // DPkg::Pre-Invoke::=<shell>` executes that shell as root during the
        // build, and `pip --index-url <url>` fetches from somewhere else.
        // Quoting does not help — quoting is what makes `-o` a clean argument.
        if (field !== "run" && line.startsWith("-")) {
          throw new Error(
            `recipe '${this.name}': ${field} entry '${line}' starts with '-', which is an ` +
              "option to the installer rather than a package; use `run` if that is what you meant"
          );
        }
      }
    }
    if (this.user != null) {
      if (!this.user || hasRustWhitespace(this.user)) {
        throw new Error(`recipe '${this.name}': user must be a single uid or name`);
      }
      noContinuation(this.name, "user", this.user);
    }
    if (this.platform != null && (!this.platform || hasRustWhitespace(this.platform))) {
      throw new Error(`recipe '${this.name}': platform must look like linux/arm64`);
    }
  }
}

/** Accept a RecipeFile or the plain object a YAML recipe uses. */
function asRecipeFile(f) {
  if (f instanceof RecipeFile) return f;
  if (f && typeof f === "object" && "path" in f && "content" in f) {
    return new RecipeFile(String(f.path), String(f.content), { executable: Boolean(f.executable) });
  }
  throw new TypeError("files must be RecipeFile objects or {path, content} objects");
}

function noLineBreaks(recipe, field, value) {
  // NUL too: the builder refuses it because it would truncate the value at a C
  // boundary downstream, so the image would not contain what the recipe says.
  if (value.includes("\n") || value.includes("\r") || value.includes("\0")) {
    throw new Error(`recipe '${recipe}': ${field} must not contain a line break or a NUL`);
  }
}

/**
 * Rust's `char::is_whitespace`, which is what the builder tests with.
 *
 * JavaScript's `\s` and `trim()` are close but not equal, in both directions:
 * they treat U+FEFF as whitespace and Rust does not, and Rust treats U+0085
 * (NEL) as whitespace and `\s` does not. Either difference changes whether a
 * recipe is accepted, so the set is spelled out rather than borrowed — and it
 * was measured against the builder, one code point at a time, rather than
 * assumed. (U+001C-U+001F are *not* in it: Python's `str.isspace()` counts them
 * and Rust does not, which is the mistake this list exists to avoid.)
 */
const RUST_WHITESPACE = new Set([
  "\t", "\n", "\v", "\f", "\r", " ",
  "\u0085", "\u00a0", "\u1680",
  "\u2000", "\u2001", "\u2002", "\u2003", "\u2004", "\u2005",
  "\u2006", "\u2007", "\u2008", "\u2009", "\u200a",
  "\u2028", "\u2029", "\u202f", "\u205f", "\u3000",
]);

function hasRustWhitespace(value) {
  for (const ch of value) if (RUST_WHITESPACE.has(ch)) return true;
  return false;
}

function rustTrimEnd(value) {
  let end = value.length;
  while (end > 0 && RUST_WHITESPACE.has(value[end - 1])) end -= 1;
  return value.slice(0, end);
}

function isRustBlank(value) {
  for (const ch of value) if (!RUST_WHITESPACE.has(ch)) return false;
  return true;
}

/** A trailing backslash continues the generated Dockerfile line onto the next,
 *  splicing two directives into one. */
function noContinuation(recipe, field, value) {
  if (rustTrimEnd(value).endsWith("\\")) {
    throw new Error(`recipe '${recipe}': ${field} must not end with a backslash`);
  }
}

/** `https://user:password@host` in an ENV bakes a credential into the image. */
function noUserinfo(recipe, field, value) {
  if (/[a-zA-Z][a-zA-Z0-9+.-]*:\/\/[^/\s@]*:[^/\s@]*@/.test(value)) {
    throw new Error(
      `recipe '${recipe}': ${field} carries a 'user:password@' URL — an ENV is baked ` +
        "into the image and visible to anyone who can pull it"
    );
  }
}

function absolutePath(recipe, field, value) {
  const segments = value.split("/").slice(1);
  if (
    !value.startsWith("/") ||
    hasRustWhitespace(value) ||
    value.includes("\0") ||
    segments.some((s) => s === "" || s === "." || s === "..")
  ) {
    throw new Error(
      `recipe '${recipe}': ${field} path '${value}' must be absolute, without ` +
        "whitespace, `.`/`..` or empty components"
    );
  }
}


class TaskSet {
  /** @param {string} name @param {string} what  "DAG" or "template", for messages. */
  constructor(name, what) {
    if (!name) throw new Error(`${what} requires a name`);
    /** @type {string} */
    this.name = name;
    /** @type {string} */
    this._what = what;
    /** @type {Array<Record<string, unknown>>} */
    this._tasks = [];
    /** @type {Set<string>} */
    this._names = new Set();
    // Where a Recipe image is pushed and pulled from, and the builds already
    // injected for this task set, keyed by image tag. Here rather than on Dag
    // because `task()` is what triggers a build and `task()` is shared.
    /** @type {string} */
    this.imageRepository = "";
    /** @type {string} */
    this.buildRunnerClass = "build";
    /** @type {number} */
    this.buildTimeoutSecs = 900;
    /** @type {Map<string, string>} */
    this._builds = new Map();
  }

  /**
   * Add a task; returns its name so it can be passed to a later task's
   * `dependsOn`.
   *
   * A task is exactly one **kind**: a *leaf* (runs `command`), a *call*
   * (`template` inlines a sub-DAG declared on this spec), a *chain*
   * (`workflowRef` inlines another saved workflow), or one of the command-less
   * kinds selected by `taskType` — `approval` (a human gate), `workflow`
   * (trigger a registered workflow as a child run) or `wait` (a deferrable
   * sensor). `toSpec()` enforces that at build time, mirroring the server.
   *
   * Every other option maps one-to-one onto the engine's `TaskSpec` and is
   * omitted from the emitted spec when left unset, so the JSON stays minimal.
   * An unknown option throws.
   *
   * @param {string} name
   * @param {import("./index.d.ts").TaskOptions} [opts]
   * @returns {string}
   */
  task(name, opts = {}) {
    if (!name) throw new Error("task requires a name");
    rejectUnknownOptions(opts, TASK_OPTION_KEYS, "task");
    let image = opts.image;
    let dependsOn = opts.dependsOn ? [...opts.dependsOn] : [];
    // Validate a Recipe BEFORE the name is reserved, so a bad recipe leaves the
    // task set untouched rather than half-mutated.
    if (image instanceof Recipe) image.validate();
    if (this._names.has(name)) throw new Error(`duplicate task '${name}'`);
    // Reserve the author's name BEFORE injecting the build. The other way
    // round, `task("build-etl", {image: new Recipe("etl", …)})` has the
    // injected task claim `build-etl` and the author's own call then fails as a
    // duplicate -- which is what this SDK did while Python did not, so the same
    // program worked in one language and threw in the other.
    this._names.add(name);
    if (image instanceof Recipe) {
      const buildName = this._ensureBuild(image);
      if (!dependsOn.includes(buildName)) dependsOn.push(buildName);
      image = image.imageRef(this.imageRepository);
    }

    /** @type {Record<string, unknown>} */
    const t = { name };
    if (image) t.docker_image = image;
    if (opts.command?.length) t.command = [...opts.command];
    if (dependsOn.length) t.depends_on = dependsOn;
    if (opts.workflowRef) t.workflow_ref = opts.workflowRef;
    if (opts.template) t.template = opts.template;
    if (opts.arguments && Object.keys(opts.arguments).length) {
      t.arguments = { ...opts.arguments };
    }
    // The engine's field is `type`; `taskType` is only the JS spelling, since
    // `type` reads as a reserved-ish word in an options bag.
    if (opts.taskType) t.type = opts.taskType;
    if (opts.workflow) t.workflow = opts.workflow;
    if (opts.wait !== undefined) t.wait = normalizeWait(opts.wait);
    if (opts.approvalTimeoutSecs != null) t.approval_timeout_secs = opts.approvalTimeoutSecs;
    if (opts.approvalOnTimeout != null) {
      if (!["approve", "reject"].includes(opts.approvalOnTimeout)) {
        throw new Error("approvalOnTimeout must be 'approve' or 'reject'");
      }
      t.approval_on_timeout = opts.approvalOnTimeout;
    }
    if (opts.input !== undefined) t.input = opts.input;
    if (opts.when !== undefined) t.when = opts.when;
    if (opts.triggerRule !== undefined) t.trigger_rule = opts.triggerRule;
    if (opts.hook !== undefined) {
      if (!["on_exit", "on_failure"].includes(opts.hook)) {
        throw new Error("hook must be 'on_exit' or 'on_failure'");
      }
      t.hook = opts.hook;
    }
    if (opts.allowFailure) t.allow_failure = true;
    if (opts.withItems !== undefined) t.with_items = [...opts.withItems];
    if (opts.withParam !== undefined) t.with_param = opts.withParam;
    if (opts.instanceKey !== undefined) t.instance_key = opts.instanceKey;
    if (opts.maxAttempts != null) {
      if (opts.maxAttempts < 1) throw new Error("maxAttempts must be >= 1");
      t.max_attempts = opts.maxAttempts;
    }
    if (opts.retryDelaySecs != null) t.retry_delay_secs = opts.retryDelaySecs;
    if (opts.retryMaxDelaySecs != null) t.retry_max_delay_secs = opts.retryMaxDelaySecs;
    if (opts.retryOnTimeout != null) t.retry_on_timeout = Boolean(opts.retryOnTimeout);
    if (opts.retryBudgets && Object.keys(opts.retryBudgets).length) {
      t.retry_budgets = { ...opts.retryBudgets };
    }
    if (opts.timeoutSecs != null) t.timeout_secs = opts.timeoutSecs;
    if (opts.env !== undefined) t.env = normalizeEnv(opts.env);
    if (opts.resources !== undefined) t.resources = { ...opts.resources };
    if (opts.serviceAccount) t.service_account = opts.serviceAccount;
    if (opts.runnerClass) t.runner_class = opts.runnerClass;
    if (opts.pool) t.pool = opts.pool;
    // 0 is the engine's default and means "fall back to task_defaults";
    // emitting it would pin the task to 0 and defeat that.
    if (opts.priority) t.priority = opts.priority;
    if (opts.cache !== undefined) t.cache = { ...opts.cache };
    if (opts.repeat !== undefined) {
      rejectUnknownOptions(opts.repeat, REPEAT_KEYS, "repeat");
      t.repeat = { ...opts.repeat };
    }
    if (opts.produces?.length) t.produces = [...opts.produces];
    if (opts.gang !== undefined) {
      t.gang = typeof opts.gang === "number" ? { size: opts.gang } : { ...opts.gang };
    }
    if (opts.isolation !== undefined) t.isolation = { ...opts.isolation };

    this._tasks.push(t);
    return name;
  }

  /**
   * Add a human approval gate (`type: approval`).
   *
   * The task parks in `awaiting_approval` when its dependencies are satisfied
   * and waits for {@link Client#approveTask} / {@link Client#rejectTask} — or,
   * if `timeoutSecs` is set, for the deadline to resolve it as `onTimeout`
   * (`"reject"` by default: absent a human decision, a gate fails safe).
   */
  approval(name, { timeoutSecs, onTimeout, ...rest } = {}) {
    return this.task(name, {
      ...rest,
      taskType: "approval",
      approvalTimeoutSecs: timeoutSecs,
      approvalOnTimeout: onTimeout,
    });
  }

  /**
   * Add a deferrable sensor (`type: wait`) — it holds no worker slot.
   *
   * Exactly one of `duration` (a relative span like `"5m"`, anchored when the
   * task is reached), `until` (an absolute RFC3339 instant), `url` (poll until
   * it answers 2xx) or `dataset` (wait for a *fresh* update to that dataset).
   */
  sensor(name, { duration, until, url, dataset, ...rest } = {}) {
    return this.task(name, { ...rest, taskType: "wait", wait: { duration, until, url, dataset } });
  }

  /**
   * Add a sub-workflow trigger (`type: workflow`).
   *
   * The engine submits the named **registered** workflow as a child run and
   * parks this task until that run is terminal, succeeding or failing with it.
   * `arguments` become the child run's parameters, so a repeating trigger can
   * hand each child different inputs.
   */
  trigger(name, workflow, opts = {}) {
    return this.task(name, { ...opts, taskType: "workflow", workflow });
  }

  /**
   * Run the server's per-task and graph checks over this task set.
   *
   * Mirrors `routes::control::validate_graph`, and deliberately stops where it
   * does: a dependency that only resolves after expansion (a name inside a
   * chained sub-workflow) is left for the engine, so a spec the server would
   * accept is never rejected here.
   */
  _validate(templateNames) {
    for (const t of this._tasks) {
      const name = t.name;
      if (t.trigger_rule !== undefined && !TRIGGER_RULES.includes(t.trigger_rule)) {
        throw new Error(
          `task '${name}' has invalid trigger_rule '${t.trigger_rule}' ` +
            `(expected one of ${TRIGGER_RULES.join(", ")})`,
        );
      }
      if (t.repeat !== undefined) {
        if (!String(t.repeat.until ?? "").trim()) {
          throw new Error(`task '${name}' repeat.until is empty`);
        }
        if (!(Number(t.repeat.max_iterations ?? 0) >= 1)) {
          throw new Error(`task '${name}' repeat.max_iterations must be >= 1`);
        }
        if (t.type !== undefined && t.type !== "task" && t.type !== "workflow") {
          throw new Error(
            `task '${name}' cannot combine \`repeat\` with \`type: ${t.type}\` — ` +
              "`repeat` applies to command tasks and sub-workflow triggers",
          );
        }
      }
      // A templated class (`{{ param }}`) is only a real name after the server
      // substitutes it, so checking its charset here would reject a spec the
      // engine accepts.
      if (t.runner_class && !t.runner_class.includes("{{")) {
        validateRunnerClass(t.runner_class, `task '${name}'`);
      }
      for (const referenced of whenOutputRefs(t.when ?? "")) {
        if (!(t.depends_on ?? []).includes(referenced)) {
          throw new Error(
            `task '${name}' when references '{{ tasks.${referenced}.output }}' but does not ` +
              `depend on '${referenced}' — add it to dependsOn`,
          );
        }
      }
      const kinds = [Boolean(t.command?.length), "template" in t, "workflow_ref" in t];
      const kindCount = kinds.filter(Boolean).length;
      if (COMMANDLESS_TASK_TYPES.includes(t.type)) {
        if (kindCount > 0) {
          throw new Error(
            `task '${name}' is a command-less kind (approval / workflow / wait) and cannot ` +
              "set `command`, `template` or `workflowRef`",
          );
        }
      } else if (kindCount === 0) {
        throw new Error(
          `task '${name}' needs a \`command\` (leaf), a \`template\` (sub-DAG call) or a ` +
            "`workflowRef` (chain)",
        );
      } else if (kindCount > 1) {
        throw new Error(
          `task '${name}' sets more than one of \`command\` / \`template\` / \`workflowRef\` — ` +
            "use exactly one",
        );
      }
      if (t.type === "workflow") {
        if (!String(t.workflow ?? "").trim()) {
          throw new Error(`task '${name}' is type: workflow but names no \`workflow:\` to trigger`);
        }
      } else if ("workflow" in t) {
        throw new Error(`task '${name}' sets \`workflow:\` but is not \`type: workflow\``);
      }
      if (t.type === "wait") {
        if (Object.keys(t.wait ?? {}).length !== 1) {
          throw new Error(
            `task '${name}' is type: wait and needs exactly one of ` +
              "`duration` / `until` / `url` / `dataset`",
          );
        }
        if ("hook" in t) throw new Error(`task '${name}' cannot be both a wait sensor and a hook`);
      } else if ("wait" in t) {
        throw new Error(`task '${name}' sets \`wait:\` but is not \`type: wait\``);
      }
      if (t.template !== undefined) {
        if (!templateNames.includes(t.template)) {
          throw new Error(
            `task '${name}' calls unknown template '${t.template}' in ${this._what} ` +
              `'${this.name}' — declare it with Dag#template()`,
          );
        }
      } else if (t.arguments && Object.keys(t.arguments).length && t.type !== "workflow") {
        throw new Error(
          `task '${name}' sets \`arguments\` with no \`template\` or \`type: workflow\` to pass ` +
            "them to",
        );
      }
    }

    // Names a dependency may legitimately forward-reference: a task inside a
    // chained sub-workflow, namespaced only once the chain is inlined.
    const chains = this._tasks.filter((t) => "workflow_ref" in t).map((t) => t.name);
    for (const t of this._tasks) {
      for (const dep of t.depends_on ?? []) {
        if (this._names.has(dep)) continue;
        if (chains.some((c) => dep === c || dep.startsWith(`${c}.`))) continue;
        throw new Error(`task '${t.name}' depends on unknown task '${dep}'`);
      }
    }
    this._assertAcyclic();
  }

  /** DFS colouring; throw on the first back-edge (a dependency cycle). */
  _assertAcyclic() {
    const adjacency = new Map(
      this._tasks.map((t) => [t.name, (t.depends_on ?? []).filter((d) => this._names.has(d))]),
    );
    const WHITE = 0;
    const GREY = 1;
    const BLACK = 2;
    const color = new Map([...adjacency.keys()].map((n) => [n, WHITE]));
    const visit = (node) => {
      color.set(node, GREY);
      for (const dep of adjacency.get(node)) {
        if (color.get(dep) === GREY) {
          throw new Error(`${this._what} '${this.name}' contains a cycle (through '${dep}')`);
        }
        if (color.get(dep) === WHITE) visit(dep);
      }
      color.set(node, BLACK);
    };
    for (const node of adjacency.keys()) {
      if (color.get(node) === WHITE) visit(node);
    }
  }

  _ensureBuild(recipe) {
    recipe.validate();
    const tag = recipe.tag();
    const existing = this._builds.get(tag);
    if (existing !== undefined) return existing;

    const base = `build-${recipe.name}`;
    // A second recipe named the same thing, or an author's own task called
    // `build-x`, must not collide. The tag disambiguates and stays stable.
    const buildName = this._names.has(base) ? `${base}-${tag.slice(2, 10)}` : base;
    if (this._names.has(buildName)) {
      throw new Error(
        `cannot add a build task for recipe '${recipe.name}': both '${base}' and ` +
          `'${buildName}' are taken`
      );
    }

    // The recipe travels as canonical JSON, not as the YAML someone typed. JSON
    // is a YAML subset so the builder parses it either way, and the canonical
    // form has no whitespace left to disagree about — which is how the tag
    // computed here and the tag the builder computes stay the same string. An
    // earlier version of this feature embedded the YAML verbatim and a stripped
    // trailing newline moved the hash.
    const env = [{ name: "DAGRON_BUILD_RECIPE", value: recipe.canonicalJson() }];
    if (this.imageRepository) {
      // Pin both halves in the spec. The pool has its own defaults for these; a
      // task's env wins over them, so the image this build pushes is the image
      // the tasks reference even against a pool configured for a different
      // registry.
      env.push({ name: "DAGRON_IMAGE_REPOSITORY", value: this.imageRepository });
      env.push({ name: "DAGRON_BUILD_PUSH", value: "1" });
    }

    const imageRef = recipe.imageRef(this.imageRepository);
    this._names.add(buildName);
    this._builds.set(tag, buildName);
    this._tasks.push({
      name: buildName,
      command: ["dagron-build"],
      runner_class: this.buildRunnerClass,
      env,
      timeout_secs: this.buildTimeoutSecs,
      // Declarative lineage: the thing this task makes. Deliberately no cache —
      // the engine's memo is keyed on the cache key alone and never looks at a
      // registry, so a memo outliving a pruned image would skip the build and
      // leave every task pulling a tag that is no longer there. The builder's
      // own registry lookup is the reuse check that cannot go stale.
      produces: [`oci://${imageRef}`],
    });
    return buildName;
  }
}

/**
 * A named, reusable sub-DAG declared on a {@link Dag} and called by a task.
 *
 * Create one with {@link Dag#template}, fill it with `task()`, and call it from
 * a task with `{ template: "<name>", arguments: {…} }`. Its tasks live in their
 * own namespace — `dependsOn` inside a template names the template's own tasks,
 * and the expander prefixes each produced task with the calling task's name
 * (`run-etl.build`).
 */
export class Template extends TaskSet {
  /** @param {string} name @param {{ parameters?: Record<string, string> }} [opts] */
  constructor(name, opts = {}) {
    super(name, "template");
    /** @type {Record<string, string>} */
    this.parameters = { ...(opts.parameters ?? {}) };
  }

  /**
   * The template as it appears under a spec's `templates:` list.
   *
   * Deep-copied like {@link Dag#toSpec}: called through the DAG the outer copy
   * would cover it, but a caller who builds a template and reads it directly
   * would otherwise be handed the builder's own task list to mutate.
   */
  toSpec() {
    const spec = { name: this.name };
    if (Object.keys(this.parameters).length) spec.parameters = { ...this.parameters };
    spec.tasks = structuredClone(this._tasks);
    return spec;
  }
}

/**
 * Build a dagron workflow spec in code.
 *
 * Add tasks with `task()` (or the `approval()` / `sensor()` / `trigger()`
 * shorthands), reusable sub-DAGs with {@link Dag#template}; `toSpec()` /
 * `toJSON()` validate the graph and emit the spec. Pass the builder straight to
 * {@link Client#submitRun} or {@link Client#createWorkflow}.
 */
export class Dag extends TaskSet {
  /**
   * @param {string} name
   * @param {import("./index.d.ts").DagOptions} [opts]  Spec-level properties,
   *   all optional: `runnerClass` (the default runner pool for tasks that don't
   *   set their own), `parameters` (declared parameters and their defaults,
   *   substituted at submit), `tags`, `environment` (the named variable set +
   *   secrets this spec runs under), `taskDefaults` (the DRY block merged into
   *   every task), `runTimeoutSecs`, `maxActiveRuns`, `resultFrom` (the task
   *   whose output becomes the run's result), `budget` (`{tasks: N}` — a ceiling
   *   on what one run may expand to), `deadline` (`{within: "2h"}` — the *soft*
   *   deadline that notifies rather than kills), `notify`, and
   *   `onDatasets`/`datasetsMode` for data-aware scheduling. An unknown option
   *   throws.
   */
  constructor(name, opts = {}) {
    super(name, "DAG");
    rejectUnknownOptions(opts, DAG_OPTION_KEYS, "Dag");
    /** @type {string | undefined} */
    this.runnerClass = opts.runnerClass;
    // The three that matter only when a task's `image` is a Recipe: where the
    // built image is pushed and pulled from, and the envelope of the build task
    // that gets injected for it.
    this.imageRepository = opts.imageRepository ?? "";
    this.buildRunnerClass = opts.buildRunnerClass ?? "build";
    this.buildTimeoutSecs = opts.buildTimeoutSecs ?? 900;
    this.parameters = { ...(opts.parameters ?? {}) };
    this.tags = [...(opts.tags ?? [])];
    this.environment = opts.environment;
    this.taskDefaults = opts.taskDefaults;
    this.runTimeoutSecs = opts.runTimeoutSecs;
    this.maxActiveRuns = opts.maxActiveRuns;
    this.resultFrom = opts.resultFrom;
    this.budget = opts.budget;
    this.deadline = opts.deadline;
    this.notify = opts.notify;
    this.onDatasets = [...(opts.onDatasets ?? [])];
    this.datasetsMode = opts.datasetsMode;
    /** @type {Template[]} */
    this._templates = [];
  }

  /**
   * Declare a reusable sub-DAG and return it for filling with tasks.
   *
   * Call it from a task with `{ template: name, arguments: {…} }`; the engine
   * inlines the template's tasks in place of the calling task at run creation,
   * wiring that task's upstreams to the sub-DAG's roots and its downstreams to
   * the sub-DAG's exits.
   */
  template(name, opts = {}) {
    if (this._templates.some((t) => t.name === name)) {
      throw new Error(`duplicate template '${name}'`);
    }
    const tpl = new Template(name, opts);
    // The DAG's build settings, not the TaskSet defaults. A Recipe used inside a
    // template must resolve to the same reference, and push to the same
    // registry, as one used directly on the DAG -- otherwise one spec carries
    // two references for one recipe, and the template's is a daemon-local
    // `etl:r-<tag>` that was never pushed and a remote runner cannot pull.
    tpl.imageRepository = this.imageRepository;
    tpl.buildRunnerClass = this.buildRunnerClass;
    tpl.buildTimeoutSecs = this.buildTimeoutSecs;
    this._templates.push(tpl);
    return tpl;
  }

  /**
   * Build the validated dagron spec object.
   *
   * Runs the same structural checks the gateway runs server-side, so a bad DAG
   * fails locally with a clear message instead of a 400 round-trip: every task
   * is exactly one kind, trigger rules and runner classes are well-formed, every
   * `template` call and `dependsOn` resolves, `resultFrom` names a real task,
   * and the dependency graph is acyclic.
   */
  toSpec() {
    if (this.runnerClass && !this.runnerClass.includes("{{")) {
      validateRunnerClass(this.runnerClass, `DAG '${this.name}'`);
    }
    if (this.runTimeoutSecs != null && this.runTimeoutSecs < 1) {
      throw new Error(
        `invalid runTimeoutSecs=${this.runTimeoutSecs} in DAG '${this.name}'; expected >= 1 (or omit)`,
      );
    }
    const templateNames = this._templates.map((t) => t.name);
    this._validate(templateNames);
    for (const tpl of this._templates) tpl._validate(templateNames);
    if (this.resultFrom != null && !this._names.has(this.resultFrom)) {
      throw new Error(`resultFrom '${this.resultFrom}' in DAG '${this.name}' names no task`);
    }

    const spec = { name: this.name };
    if (Object.keys(this.parameters).length) spec.parameters = { ...this.parameters };
    if (this.tags.length) spec.tags = [...this.tags];
    if (this.environment) spec.environment = this.environment;
    if (this.runnerClass) spec.runner_class = this.runnerClass;
    if (this.taskDefaults) spec.task_defaults = { ...this.taskDefaults };
    if (this.runTimeoutSecs != null) spec.run_timeout_secs = this.runTimeoutSecs;
    if (this.maxActiveRuns != null) spec.max_active_runs = this.maxActiveRuns;
    if (this.resultFrom) spec.result_from = this.resultFrom;
    if (this.budget) spec.budget = { ...this.budget };
    if (this.deadline) spec.deadline = { ...this.deadline };
    if (this.notify) spec.notify = { ...this.notify };
    if (this.onDatasets.length) spec.on_datasets = [...this.onDatasets];
    if (this.datasetsMode) spec.datasets_mode = this.datasetsMode;
    if (this._templates.length) spec.templates = this._templates.map((t) => t.toSpec());
    spec.tasks = this._tasks;
    // Deep-copy so callers can't mutate our internal task state via the returned
    // spec (toJSON/submit both go through here).
    return structuredClone(spec);
  }

  /** dagron spec as JSON (valid dagron input — YAML is a JSON superset). */
  toJSON() {
    return JSON.stringify(this.toSpec());
  }

  /**
   * Submit the DAG as an ad-hoc run; resolves to the new `run_id`.
   *
   * Convenience one-liner equivalent to
   * `new Client(apiUrl, { token }).submitRun(this)`.
   * @param {string} apiUrl  e.g. "http://localhost:8080"
   * @param {{ token?: string, timeout?: number }} [opts]
   * @returns {Promise<string>} the new run id
   */
  async submit(apiUrl, opts = {}) {
    return new Client(apiUrl, opts).submitRun(this);
  }
}

/** Percent-encode a single path segment (ids are UUIDs, but never trust input). */
function seg(value) {
  return encodeURIComponent(String(value));
}

/** The artifact route for one `(run, task, name)` key. */
function artifactPath(runId, task, name) {
  return `/api/runs/${seg(runId)}/artifacts/${seg(task)}/${seg(name)}`;
}

/**
 * Copy the fields that were actually given into `body`.
 *
 * A partial-update body must carry only what the caller chose: sending
 * `{"enabled": null}` for an option they never passed asks the server to change
 * a field they never mentioned.
 */
function putIfSet(body, fields) {
  for (const [key, value] of Object.entries(fields)) {
    if (value !== undefined && value !== null) body[key] = value;
  }
  return body;
}

/** Standard base64 of bytes (or of a string's UTF-8), as the wire wants. */
function b64(data) {
  const bytes = typeof data === "string" ? new TextEncoder().encode(data) : new Uint8Array(data);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

/**
 * The log filter grammar accepted by both log endpoints. Mirrors
 * `dagron_logging::logfilter` — the server owns the semantics; this is only the
 * list of names, so a typo throws here instead of becoming a silently-ignored
 * parameter that makes an unfiltered response look filtered.
 */
export const LOG_FILTER_PARAMS = Object.freeze([
  "q",
  "exclude",
  "regex",
  "level",
  "case",
  "context",
  "limit",
  "tail",
]);

/**
 * Normalise log filter options into query parameters. Arrays are joined with
 * commas (`level: ["error", "warn"]`); `true` becomes `1` and `false` is
 * dropped, because an explicit `case=0` would still count as "the caller
 * filtered" and change server behaviour. An unknown key throws.
 */
export function logFilterParams(filter = {}) {
  const params = {};
  for (const [key, value] of Object.entries(filter)) {
    if (!LOG_FILTER_PARAMS.includes(key)) {
      throw new TypeError(
        `unknown log filter parameter "${key}"; expected one of ${LOG_FILTER_PARAMS.join(", ")}`,
      );
    }
    if (value === undefined || value === null || value === "" || value === false) continue;
    if (value === true) params[key] = "1";
    else if (Array.isArray(value)) {
      const joined = value.join(",");
      if (joined) params[key] = joined;
    } else params[key] = value;
  }
  return params;
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

  /**
   * Build a client from `DAGRON_API_URL` and (optionally) `DAGRON_TOKEN`.
   *
   * The shape automation wants: mint a personal access token once with
   * {@link Client#createToken}, put it in the environment, and no job ever has
   * to store the password that would mint another. Throws when
   * `DAGRON_API_URL` is unset — an unset URL is a misconfigured job, not a
   * reason to guess at localhost.
   */
  static fromEnv({ timeout } = {}) {
    const env = globalThis.process?.env ?? {};
    if (!env.DAGRON_API_URL) throw new Error("DAGRON_API_URL is not set");
    return new Client(env.DAGRON_API_URL, { token: env.DAGRON_TOKEN || undefined, timeout });
  }

  /**
   * Drop the in-memory token. `fetch` holds no connection to release, so this
   * is only about not letting the credential outlive its use.
   *
   * Not `[Symbol.dispose]`: this package supports Node 18, where that symbol is
   * `undefined` — a computed `[Symbol.dispose]()` there defines a method named
   * "undefined" rather than a disposer, which is worse than not having one.
   */
  close() {
    this.token = null;
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

  /** List users (admin only). Password hashes are never returned. */
  async listUsers() {
    return this._request("GET", "/api/users");
  }

  // ── personal access tokens ──────────────────────────────────────────────────

  /**
   * The calling user's access tokens, revoked ones included. Each row carries
   * the cleartext `prefix` (never the secret), plus `last_used_at` — the field
   * that answers "is anything still using this".
   */
  async listTokens() {
    return this._request("GET", "/api/tokens");
  }

  /**
   * Mint a named access token; resolves to it including the plaintext `token`.
   *
   * **This is the only response that ever carries the secret** — only its hash
   * is stored, so there is no endpoint that can show it again. Minting requires
   * a password session ({@link Client#login}): a token cannot mint another,
   * which is what keeps a leaked one from outrunning revocation.
   */
  async createToken(name, { expiresInDays } = {}) {
    const body = { name };
    if (expiresInDays != null) body.expires_in_days = expiresInDays;
    return this._request("POST", "/api/tokens", { body });
  }

  /** Revoke one access token. Revoking twice is not an error. */
  async revokeToken(tokenId) {
    await this._request("DELETE", `/api/tokens/${seg(tokenId)}`, { parseJson: false });
  }

  // ── runs ────────────────────────────────────────────────────────────────────

  async submitRun(spec, { parameters, idempotencyKey } = {}) {
    const body = { yaml: specToStr(spec) };
    if (parameters && Object.keys(parameters).length) body.parameters = { ...parameters };
    // With an idempotency key, repeating this call returns the same run_id
    // instead of creating a second run. Reusing a key for a different spec or
    // different parameters throws a 409 rather than returning the first run.
    //
    // Distinguish an omitted key from an explicit empty string: `""` is a 400
    // server-side, so forwarding it surfaces the error, whereas dropping it (a
    // truthiness check would) hands back a non-idempotent submit the caller
    // believes is retry-safe. Only `undefined`/`null` means "no key".
    const headers =
      idempotencyKey !== undefined && idempotencyKey !== null
        ? { "idempotency-key": idempotencyKey }
        : undefined;
    const res = await this._request("POST", "/api/runs", { body, headers });
    // A submit that resolves to `undefined` is worse than one that throws: the
    // caller stores it as a run id and only finds out when the next call 404s.
    if (typeof res?.run_id !== "string") {
      throw new DagronError(0, `submit succeeded but returned no run_id: ${JSON.stringify(res)}`);
    }
    return res.run_id;
  }

  /**
   * Runs newest-first, optionally filtered and paged. `name` is the
   * workflow/DAG name (exact match) and `trigger` is what started the run —
   * `manual`, `schedule` or `backfill`.
   */
  async listRuns({ status, name, trigger, limit, offset } = {}) {
    return this._request("GET", "/api/runs", {
      params: { status, name, trigger, limit, offset },
    });
  }

  /**
   * Yield runs across pages, walking `limit`/`offset` transparently.
   *
   * Takes the same filters as {@link Client#listRuns}, minus the two it drives
   * itself: a caller's `limit` or `offset` would be silently overwritten by the
   * paging arguments, so they are refused up front with the name of the option
   * to use instead. Stops on the first short page, so a caller can `break` out
   * early without fetching the rest.
   */
  async *iterRuns({ pageSize = 100, ...filters } = {}) {
    for (const reserved of ["limit", "offset"]) {
      if (reserved in filters) {
        throw new TypeError(
          `iterRuns drives "${reserved}" itself — use pageSize to size the pages`,
        );
      }
    }
    for (let offset = 0; ; offset += pageSize) {
      const page = await this.listRuns({ ...filters, limit: pageSize, offset });
      yield* page;
      if (page.length < pageSize) return;
    }
  }

  async getRun(runId) {
    return this._request("GET", `/api/runs/${seg(runId)}`);
  }

  async getRunGraph(runId) {
    return this._request("GET", `/api/runs/${seg(runId)}/graph`);
  }

  /**
   * The DAG spec this run was created from (`{yaml, name}`) — the stored,
   * un-expanded spec, so a "re-run with changes" flow can start from the real
   * definition rather than reconstructing one.
   */
  async getRunSpec(runId) {
    return this._request("GET", `/api/runs/${seg(runId)}/spec`);
  }

  /**
   * One task's captured output, scoped to its run.
   *
   * `offset` (a prior response's `next_offset`) tails: only output past that
   * character offset comes back, until `eof`. The log filter is applied
   * server-side *within* that slice, so a filtered tail appends only matching
   * new lines while `next_offset` keeps advancing over the raw text.
   */
  async getTaskLogs(runId, taskId, { offset, ...filter } = {}) {
    const params = logFilterParams(filter);
    if (offset !== undefined && offset !== null) params.offset = offset;
    return this._request("GET", `/api/runs/${seg(runId)}/tasks/${seg(taskId)}/logs`, { params });
  }

  /**
   * The whole run's output as one attributed, filtered stream — the call for
   * "something in this run failed and I don't know which task", instead of one
   * request per task.
   *
   * `tasks`/`statuses` choose which task output is read at all; the filter then
   * chooses which of their lines survive. `total`/`matched` in the response are
   * counted before the line cap, so a truncated view always says so.
   *
   *   await api.getRunLogs(runId, { level: "error", context: 2 });
   *   await api.getRunLogs(runId, { tasks: ["extract"], regex: "rows=\\d+" });
   */
  async getRunLogs(runId, { tasks, statuses, ...filter } = {}) {
    const params = logFilterParams(filter);
    if (tasks?.length) params.task = tasks.join(",");
    if (statuses?.length) params.status = statuses.join(",");
    return this._request("GET", `/api/runs/${seg(runId)}/logs`, { params });
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

  /**
   * Clear a task **and everything downstream of it**, then re-arm the run.
   *
   * Unlike {@link Client#retryTask} (which resurrects one task), this resets the
   * task and its dependents to `pending` and recomputes their dependency counts,
   * so a fix applied mid-run re-runs the whole affected subtree.
   */
  async clearTask(runId, taskId) {
    return this._request("POST", `/api/runs/${seg(runId)}/tasks/${seg(taskId)}/clear`);
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
    yield* this._stream(`/api/runs/${seg(runId)}/stream`, { timeout });
  }

  /**
   * Yield task-state events across **all** runs as Server-Sent Events.
   *
   * The account-wide feed behind the console's live mode: each event carries the
   * run it belongs to, so one connection replaces polling every list. Same item
   * shape as {@link Client#streamRun}.
   */
  async *streamEvents({ timeout } = {}) {
    yield* this._stream("/api/events/stream", { timeout });
  }

  /** Open one SSE connection and yield its parsed events until it closes. */
  async *_stream(path, { timeout } = {}) {
    const url = this.baseUrl + path;
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

  /**
   * Long-poll the run server-side until it is terminal; resolve to the result.
   *
   * This is synchronous invocation: one request that blocks on the engine's own
   * event feed rather than a poll loop, so there is no polling interval to tune
   * and no wasted round trips. Resolves to `{run_id, status, finished, result,
   * failure}` — `result` is the `result_from` task's output on success, and
   * `failure` explains the failure without a second call. A wait that times out
   * resolves with `finished: false` and the live status, so the caller simply
   * calls again.
   *
   * `timeoutSecs` is the *server's* budget (clamped to 1-600, default 30). The
   * transport timeout for this one call is widened to cover it — without that,
   * the default 30 s client timeout races the default 30 s server wait and
   * aborts the request just as the answer arrives. Only this call is widened:
   * `this.timeout` is shared with every concurrent request and is never mutated.
   */
  async waitRun(runId, { timeoutSecs } = {}) {
    const budget = Math.min(
      Math.max(timeoutSecs ?? WAIT_BUDGET_DEFAULT_SECS, WAIT_BUDGET_MIN_SECS),
      WAIT_BUDGET_MAX_SECS,
    );
    return this._request("GET", `/api/runs/${seg(runId)}/wait`, {
      params: { timeout_secs: timeoutSecs },
      timeoutMs: Math.max(this.timeout, (budget + WAIT_TRANSPORT_MARGIN_SECS) * 1000),
    });
  }

  // ── triage (what a human decided about a failure) ───────────────────────────

  /**
   * Record what a person concluded about a run: `acknowledged`,
   * `investigating` or `resolved`, with an optional `note`.
   *
   * `status` is what the engine did; this is what was done about it — the
   * distinction a single "mark as read" flag would lose. Re-triaging overwrites,
   * because acknowledged-then-resolved is the normal path.
   */
  async setTriage(runId, state, { note } = {}) {
    const body = { state };
    if (note !== undefined) body.note = note;
    return this._request("POST", `/api/runs/${seg(runId)}/triage`, { body });
  }

  /** Undo a triage decision, putting the run back in the attention queue. */
  async clearTriage(runId) {
    return this._request("DELETE", `/api/runs/${seg(runId)}/triage`);
  }

  // ── archive (cold storage for terminal runs) ────────────────────────────────

  /** Page the archive index, newest-finished-first. Pure index read. */
  async listArchivedRuns({ name, limit, offset } = {}) {
    return this._request("GET", "/api/archive/runs", { params: { name, limit, offset } });
  }

  /**
   * An archived run's full document (run + definition + tasks + events).
   * Throws 404 when the run was never archived, or 410 once it has been
   * compacted to Parquet — the body then carries the `parquet_path` to read.
   */
  async getArchivedRun(runId) {
    return this._request("GET", `/api/archive/runs/${seg(runId)}`);
  }

  /**
   * Archive one terminal run **now** instead of waiting for retention.
   *
   * Destructive and admin-only: the document is written to the configured sink,
   * indexed, and the run is then purged from the hot store — it leaves
   * {@link Client#listRuns} and reappears under
   * {@link Client#listArchivedRuns}. Throws 409 if the run is not terminal and
   * 501 when no archive sink is configured.
   */
  async archiveRun(runId) {
    return this._request("POST", `/api/runs/${seg(runId)}/archive`);
  }

  // ── workflows ───────────────────────────────────────────────────────────────

  /**
   * Saved workflows enriched with schedule + recent-run digest. `tag` narrows
   * the list to workflows declaring that tag in their spec.
   */
  async listWorkflows({ tag } = {}) {
    return this._request("GET", "/api/workflows", { params: { tag } });
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

  /**
   * Trigger a saved workflow as a run. `parameters` supply arguments for the
   * spec's declared `parameters:` — this is what makes a stored workflow
   * callable as a function, instead of fetching its spec, splicing values in
   * client-side and submitting the result as new YAML. A declared
   * `environment:` still wins over any key it also sets.
   */
  async runWorkflow(workflowId, { parameters } = {}) {
    const body = parameters ? { parameters: { ...parameters } } : undefined;
    return this._request("POST", `/api/workflows/${seg(workflowId)}/run`, { body });
  }

  /** One workflow's runs, newest first. */
  async listWorkflowRuns(workflowId, { limit, offset } = {}) {
    return this._request("GET", `/api/workflows/${seg(workflowId)}/runs`, {
      params: { limit, offset },
    });
  }

  /** A workflow's version history (every saved definition), newest first. */
  async listWorkflowVersions(workflowId) {
    return this._request("GET", `/api/workflows/${seg(workflowId)}/versions`);
  }

  /**
   * Set a workflow's lifecycle state: `active`, `paused` or `retired`.
   *
   * Note what this is *not*: deleting. A paused workflow keeps its schedules and
   * resumes on exactly the cron it had, and `retired` records "we are done with
   * this" rather than "off for now" — the distinction a single disabled flag
   * would lose.
   */
  async setWorkflowState(workflowId, state) {
    return this._request("POST", `/api/workflows/${seg(workflowId)}/state`, { body: { state } });
  }

  /**
   * Apply a signed workflow bundle to this deployment, in one transaction.
   *
   * `manifest` and `signature` are the bundle's raw bytes (or strings);
   * `files` maps each manifest-relative spec path to its content. The SDK
   * base64-encodes them for the wire. Verification is fail-closed: an unsigned
   * or untrusted-key bundle is refused, and every spec in it is validated before
   * anything is written. Throws 501 when no trust set is configured.
   */
  async applyBundle(manifest, signature, files) {
    return this._request("POST", "/api/workflows/bundle", {
      body: {
        manifest_b64: b64(manifest),
        signature_b64: b64(signature),
        files: Object.entries(files).map(([path, content]) => ({
          path,
          content_b64: b64(content),
        })),
      },
    });
  }

  /**
   * A workflow's latest-run status badge as SVG (unauthenticated) — the same
   * image a README embeds, returned as text so a caller can write it to a file
   * or serve it.
   */
  async workflowBadge(name) {
    return this._request("GET", `/api/badges/${seg(name)}`, { parseJson: false, auth: false });
  }

  async syncWorkflowToGit(workflowId) {
    return this._request("POST", `/api/workflows/${seg(workflowId)}/sync-to-git`);
  }

  // ── schedules ───────────────────────────────────────────────────────────────

  async listSchedules({ workflowId } = {}) {
    return this._request("GET", "/api/schedules", { params: { workflow_id: workflowId } });
  }

  /**
   * Attach a cron schedule to a saved workflow.
   *
   * `timezone` is the IANA zone the cron is read in (so a 02:00 job stays at
   * 02:00 across a DST shift). `whenExpr` gates a fire — the schedule only runs
   * when it evaluates true — and `stopExpr` retires the schedule once it does,
   * recording why. The `catchup*` options decide what happens after downtime:
   * whether missed fire-times run at all, how far back to look, and how many to
   * materialise at once.
   */
  async createSchedule(
    workflowId,
    cronExpr,
    {
      enabled = true,
      timezone,
      whenExpr,
      stopExpr,
      catchup,
      catchupWindowSecs,
      catchupMaxRuns,
    } = {},
  ) {
    const body = { workflow_id: workflowId, cron_expr: cronExpr, enabled };
    putIfSet(body, {
      timezone,
      when_expr: whenExpr,
      stop_expr: stopExpr,
      catchup,
      catchup_window_secs: catchupWindowSecs,
      catchup_max_runs: catchupMaxRuns,
    });
    return this._request("POST", "/api/schedules", { body });
  }

  /**
   * Change a schedule; only the fields you pass are sent (and changed). Same
   * knobs as {@link Client#createSchedule}. The next fire time is recomputed
   * server-side from whatever the update leaves in place.
   */
  async updateSchedule(
    scheduleId,
    { cronExpr, enabled, timezone, whenExpr, stopExpr, catchup, catchupWindowSecs, catchupMaxRuns } = {},
  ) {
    const body = {};
    putIfSet(body, {
      cron_expr: cronExpr,
      enabled,
      timezone,
      when_expr: whenExpr,
      stop_expr: stopExpr,
      catchup,
      catchup_window_secs: catchupWindowSecs,
      catchup_max_runs: catchupMaxRuns,
    });
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

  // ── environments (variable sets + write-only secrets) ───────────────────────

  /**
   * Environments: their variables, and the **names** of their secrets. Secret
   * values are never returned — the store is write-only by design, so this says
   * what exists, not what it holds.
   */
  async listEnvironments() {
    return this._request("GET", "/api/environments");
  }

  /**
   * Create an environment. 409 on a duplicate name.
   *
   * A spec names it with `environment:`; its variables then join the
   * substitution scope as `{{ env.NAME }}` and its secrets are resolved at
   * dispatch. Variable names become env-var names, so keep them
   * identifier-shaped.
   */
  async createEnvironment(name, { variables, description } = {}) {
    const body = { name };
    putIfSet(body, { description, variables });
    return this._request("POST", "/api/environments", { body });
  }

  /**
   * Update an environment's description and/or variables. A present
   * `variables` **replaces the whole map** — pass the full set, not a delta.
   * The name is immutable: workflow specs reference it.
   */
  async updateEnvironment(environmentId, { variables, description } = {}) {
    const body = {};
    putIfSet(body, { description, variables });
    return this._request("PUT", `/api/environments/${seg(environmentId)}`, { body });
  }

  /**
   * Delete an environment and its secrets. Runs already created keep working —
   * their parameters were resolved at creation. Future runs of specs naming it
   * fail loudly at submit.
   */
  async deleteEnvironment(environmentId) {
    await this._request("DELETE", `/api/environments/${seg(environmentId)}`, { parseJson: false });
  }

  /**
   * Set (or rotate) one secret. Write-only: it is encrypted and never read back.
   * Throws 503 when the deployment has no secret key configured — storing
   * plaintext is not an acceptable fallback.
   */
  async setEnvironmentSecret(environmentId, name, value) {
    await this._request(
      "PUT",
      `/api/environments/${seg(environmentId)}/secrets/${seg(name)}`,
      { body: { value }, parseJson: false },
    );
  }

  /** Remove one secret from an environment. */
  async deleteEnvironmentSecret(environmentId, name) {
    await this._request(
      "DELETE",
      `/api/environments/${seg(environmentId)}/secrets/${seg(name)}`,
      { parseJson: false },
    );
  }

  // ── datasets (data-aware scheduling + its lineage ledger) ───────────────────

  /**
   * The dataset registry, most recently updated first: each row is a dataset URI
   * with when it last changed, what produced that change, and which workflows
   * consume it — what `produces:` tasks write and dataset sensors read.
   */
  async listDatasets({ limit } = {}) {
    return this._request("GET", "/api/datasets", { params: { limit } });
  }

  /**
   * The lineage ledger newest-first, optionally scoped to one `uri`.
   * Append-only: who updated a dataset, from which run and task, and when.
   */
  async listDatasetEvents({ uri, limit } = {}) {
    return this._request("GET", "/api/datasets/events", { params: { uri, limit } });
  }

  // ── instance settings ───────────────────────────────────────────────────────

  /**
   * The instance-wide notification defaults (admin only — the stored webhook
   * URLs are effectively secrets).
   */
  async getNotificationSettings() {
    return this._request("GET", "/api/settings/notifications");
  }

  /**
   * Replace the notification defaults (admin only). Takes the full document —
   * `slack_enabled`, `slack_webhook_url`, `slack_on`, `webhook_enabled`,
   * `webhook_url`, `webhook_on`. Empty `*_on` lists mean each target's
   * built-in default: Slack notifies on incidents only, the webhook on every
   * event.
   */
  async setNotificationSettings(settings) {
    return this._request("PUT", "/api/settings/notifications", { body: settings });
  }

  /**
   * Send a test message to each **enabled** target in `settings` — what is on
   * screen, saved or not — reporting per-target outcomes rather than failing the
   * whole call, so one broken target does not hide the other's success.
   */
  async testNotifications(settings) {
    return this._request("POST", "/api/settings/notifications/test", { body: settings });
  }

  /** The dead-letter retry policy (`max_attempts`; absent = unset). */
  async getDeadLetterSettings() {
    return this._request("GET", "/api/settings/dead-letters");
  }

  /**
   * Set how many times ingestion retries a submission before parking it. Takes
   * effect on the next ingestion failure — no restart, and no window where the
   * console disagrees with what is running.
   */
  async setDeadLetterSettings(maxAttempts) {
    return this._request("PUT", "/api/settings/dead-letters", {
      body: { max_attempts: maxAttempts },
    });
  }

  // ── GitOps repository registry ──────────────────────────────────────────────

  async listGitRepos() {
    return this._request("GET", "/api/git-repos");
  }

  /**
   * Register a Git repository. `path` scopes discovery to a subdir (server
   * default `dagron`); `auth` sets the credential in the same call — the same
   * fields {@link Client#setGitRepoAuth} takes.
   */
  async connectGitRepo(url, { branch, autoSync = false, path, auth } = {}) {
    const body = { url, branch, auto_sync: autoSync };
    if (path != null) body.path = path;
    if (auth != null) body.auth = { ...auth };
    return this._request("POST", "/api/git-repos", { body });
  }

  /**
   * Set or rotate a repository's credential — an HTTPS token or an SSH key.
   *
   * Write-only: the secret is encrypted on arrival and is never readable back,
   * so this call is the only way to change it. `knownHosts` pins the host key
   * for SSH remotes.
   */
  async setGitRepoAuth(repoId, { kind, username, token, sshPrivateKey, knownHosts } = {}) {
    const body = {};
    putIfSet(body, {
      kind,
      username,
      token,
      ssh_private_key: sshPrivateKey,
      known_hosts: knownHosts,
    });
    return this._request("PUT", `/api/git-repos/${seg(repoId)}/auth`, { body });
  }

  /** Remove a repository's stored credential; the repo stays registered. */
  async clearGitRepoAuth(repoId) {
    await this._request("DELETE", `/api/git-repos/${seg(repoId)}/auth`, { parseJson: false });
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

  /**
   * Per-day run counts by outcome plus duration stats, newest bucket last.
   * `days` is the window; `name` narrows it to one workflow's trend.
   */
  async metricsTimeseries({ days, name } = {}) {
    return this._request("GET", "/api/metrics/timeseries", { params: { days, name } });
  }

  /**
   * Every task parked in `awaiting_approval`, oldest first: the
   * human-in-the-loop worklist, across all runs, without walking them.
   */
  async listApprovals() {
    return this._request("GET", "/api/approvals");
  }

  /**
   * Search workflows, runs and schedules at once (capped, server-side).
   * Resolves to `{query, workflows, runs, schedules}`.
   */
  async search(query, { limit } = {}) {
    return this._request("GET", "/api/search", { params: { q: query, limit } });
  }

  /**
   * Rich health: database, scheduler leadership, and the attention counters.
   * Never throws for an unhealthy *deployment* — a database failure comes back
   * as `db: "error"`, because an outage is a finding to render, not an error to
   * swallow.
   */
  async health() {
    return this._request("GET", "/api/health");
  }

  async healthz() {
    return this._request("GET", "/healthz", { parseJson: false, auth: false });
  }

  /**
   * Readiness probe: 200 only when the datastore answers within its budget
   * **and** the event listener is subscribed. Unauthenticated.
   *
   * This is the one to point a readiness probe at; {@link Client#healthz} is the
   * bare liveness check and stays 200 through a database outage.
   */
  async readyz() {
    return this._request("GET", "/readyz", { parseJson: false, auth: false });
  }

  // ── artifacts (task inputs/outputs in the object store) ─────────────────────

  /**
   * Store an artifact under `(run, task, name)`; resolves to its location.
   *
   * Encrypted at rest where the deployment configures a key. The body limit is
   * separate from (and much larger than) the one on spec submits, but bodies are
   * buffered server-side — this is for checkpoints and outputs, not for
   * streaming a dataset.
   */
  async putArtifact(runId, task, name, data) {
    return this._request("PUT", artifactPath(runId, task, name), {
      rawBody: typeof data === "string" ? new TextEncoder().encode(data) : data,
      parseJson: false,
    });
  }

  /** An artifact's (decrypted) bytes as a Uint8Array. 404 when it does not exist. */
  async getArtifact(runId, task, name) {
    return this._request("GET", artifactPath(runId, task, name), { rawResponse: true });
  }

  /** Whether an artifact exists, without transferring it. */
  async artifactExists(runId, task, name) {
    return Boolean((await this._request("GET", `${artifactPath(runId, task, name)}/exists`)).exists);
  }

  /**
   * Drain the tiered artifact store to its remote tier now (admin only).
   *
   * The periodic loop is the default; this is the on-demand path for an instance
   * that just regained its uplink. Resolves to `{moved: N}` — and `0` on a
   * store that is not tiered. Throws 409 while another store-wide sweep is
   * running: overlapping sweeps would upload objects mid-rekey.
   */
  async syncArtifacts() {
    return this._request("POST", "/api/artifacts/sync");
  }

  // ── transport ───────────────────────────────────────────────────────────────

  /** Issue one request; resolve to parsed JSON (or text/null), or throw {@link DagronError}. */
  async _request(
    method,
    path,
    {
      body,
      rawBody,
      params,
      headers: extraHeaders,
      parseJson = true,
      rawResponse = false,
      auth = true,
      // Overrides `this.timeout` for this call only — a long poll needs more
      // than the client's default, and mutating the shared field would change
      // every request in flight.
      timeoutMs,
    } = {},
  ) {
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
    if (rawBody !== undefined && rawBody !== null) {
      // Artifact uploads send bytes as-is rather than JSON-encoding them.
      data = rawBody;
      headers["content-type"] = "application/octet-stream";
    } else if (body !== undefined && body !== null) {
      data = JSON.stringify(body);
      headers["content-type"] = "application/json";
    }
    if (auth && this.token) headers.authorization = `Bearer ${this.token}`;
    // Caller headers last, but they cannot displace auth or content-type: a
    // per-call header is for things like Idempotency-Key, not for quietly
    // re-pointing the request's identity or encoding.
    for (const [k, v] of Object.entries(extraHeaders ?? {})) {
      if (!["authorization", "content-type"].includes(k.toLowerCase())) headers[k] = v;
    }

    const ctrl = new AbortController();
    const budget = timeoutMs ?? this.timeout;
    const timer = budget ? setTimeout(() => ctrl.abort(), budget) : null;
    let res;
    try {
      res = await fetch(url, { method, headers, body: data, signal: ctrl.signal });
    } catch (err) {
      throw new DagronError(0, `request to ${url} failed: ${err?.message ?? err}`);
    } finally {
      if (timer) clearTimeout(timer);
    }

    if (!res.ok) throw await DagronError._fromResponse(res);
    // An artifact download is not text at all, so it comes back undecoded.
    if (rawResponse) return new Uint8Array(await res.arrayBuffer());
    if (!parseJson) return res.text();
    const text = await res.text();
    if (!text) return null;
    return JSON.parse(text);
  }
}
