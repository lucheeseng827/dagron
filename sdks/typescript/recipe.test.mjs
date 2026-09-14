// The cross-language contract for the content-addressed image tag.
//
// Three programs compute this tag: `ee/dagron-build` (Rust, the one that
// actually builds and pushes the image), the Python SDK, and this one. An
// author writes a recipe here, this SDK derives the tag and pins every task's
// docker_image to it, and the builder produces the image those tasks will pull.
// A one-byte disagreement means the tasks reference an image no build will ever
// push, and nothing fails until the run does.
//
// `../recipe-vectors.json` is generated FROM the builder and asserted against by
// all three. Run with `node --test`.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import { BUILD_GENERATOR_VERSION, Dag, Recipe, RecipeFile } from "./index.mjs";

const VECTORS = JSON.parse(
  readFileSync(fileURLToPath(new URL("../recipe-vectors.json", import.meta.url)), "utf8")
);

/** A vector's `recipe` object uses the builder's wire names; map to constructor opts. */
function fromVector(r) {
  return new Recipe(r.name, r.base, {
    apt: r.apt,
    pip: r.pip,
    env: r.env,
    workdir: r.workdir,
    files: r.files,
    run: r.run,
    user: r.user,
    keepEntrypoint: r.keep_entrypoint,
    platform: r.platform,
    dockerfile: r.dockerfile,
  });
}

test("the generator version matches the vectors", () => {
  assert.equal(BUILD_GENERATOR_VERSION, VECTORS.generator_version);
});

test("every golden vector tags identically", () => {
  assert.ok(VECTORS.vectors.length > 0, "vector file is empty");
  for (const v of VECTORS.vectors) {
    const r = fromVector(v.recipe);
    assert.equal(r.tag(), v.tag, `${v.key}: ${v.why}`);
    assert.equal(r.imageRef(), v.image_ref, `${v.key}: unprefixed ref`);
    assert.equal(
      r.imageRef(VECTORS.prefix_for_image_ref_with_prefix),
      v.image_ref_with_prefix,
      `${v.key}: prefixed ref`
    );
  }
});

test("a trailing slash on the prefix is not a different image", () => {
  const r = fromVector(VECTORS.vectors[0].recipe);
  const p = VECTORS.prefix_for_image_ref_with_prefix;
  assert.equal(r.imageRef(p + "/"), r.imageRef(p));
  assert.equal(r.imageRef(p + "///"), r.imageRef(p));
});

// ── the canonical form: the three things a port gets wrong ───────────────────

test("fields are in declaration order, not alphabetical", () => {
  const r = new Recipe("x", "alpine", { apt: ["a"], workdir: "/w" });
  assert.equal(r.canonicalJson(), '{"name":"x","base":"alpine","apt":["a"],"workdir":"/w"}');
});

test("env is sorted by key", () => {
  const r = new Recipe("x", "alpine", { env: { Z: "1", A: "2", M: "3" } });
  assert.equal(r.canonicalJson(), '{"name":"x","base":"alpine","env":{"A":"2","M":"3","Z":"1"}}');
});

test("empty and false fields are omitted entirely", () => {
  const r = new Recipe("x", "alpine", {
    apt: [], pip: [], env: {}, files: [], run: [], keepEntrypoint: false,
  });
  assert.equal(r.canonicalJson(), '{"name":"x","base":"alpine"}');
});

test("an explicit null is the same recipe as an absent field", () => {
  // The builder's fields are `Option`, and serde reads a JSON null as None and
  // then omits it. Gating on `!== undefined` rather than `!= null` emitted the
  // null, hashed different bytes than the builder, and pinned a tag no build
  // would ever push. A recipe parsed from a file routinely carries one — a bare
  // `dockerfile:` in YAML is null.
  const absent = new Recipe("etl", "alpine", { pip: ["x==1"] });
  const explicit = new Recipe("etl", "alpine", {
    pip: ["x==1"], workdir: null, user: null, platform: null, dockerfile: null,
  });
  assert.equal(explicit.canonicalJson(), absent.canonicalJson());
  assert.equal(explicit.tag(), absent.tag());
  // ...and validate() must not trip over one either.
  explicit.validate();
});

test("an empty string is not an absent field", () => {
  assert.notEqual(new Recipe("x", "alpine", { user: "" }).tag(), new Recipe("x", "alpine").tag());
});

test("list order is part of the identity", () => {
  assert.notEqual(
    new Recipe("x", "alpine", { apt: ["a", "b"] }).tag(),
    new Recipe("x", "alpine", { apt: ["b", "a"] }).tag()
  );
});

test("executable false is omitted but true is not", () => {
  const plain = new Recipe("x", "alpine", { files: [new RecipeFile("/a", "c")] });
  const explicit = new Recipe("x", "alpine", { files: [new RecipeFile("/a", "c", { executable: false })] });
  const exe = new Recipe("x", "alpine", { files: [new RecipeFile("/a", "c", { executable: true })] });
  assert.equal(plain.tag(), explicit.tag());
  assert.notEqual(plain.tag(), exe.tag());
});

test("files accept plain objects", () => {
  assert.equal(
    new Recipe("x", "alpine", { files: [new RecipeFile("/a", "c", { executable: true })] }).tag(),
    new Recipe("x", "alpine", { files: [{ path: "/a", content: "c", executable: true }] }).tag()
  );
});

// ── the JavaScript-only ordering traps ───────────────────────────────────────
//
// These two would silently change the tag in this language and no other, and no
// golden vector can catch them because the builder refuses the inputs outright.
// The env-key rule is what makes them unreachable, so it is tested as a rule
// rather than as politeness.

test("an integer-like env key is refused, because objects would reorder it", () => {
  // `{"10":…,"9":…}` stringifies with 10 first here and 9 first in the builder.
  assert.throws(() => new Recipe("x", "alpine", { env: { 10: "a", 9: "b" } }).validate(), /env key/);
});

test("a non-ASCII env key is refused, because sort() is UTF-16 here and UTF-8 there", () => {
  // U+10000 starts with surrogate 0xD800, so it sorts below U+E000 in JS and
  // above it in Rust.
  assert.throws(
    () => new Recipe("x", "alpine", { env: { "\u{10000}": "a", "": "b" } }).validate(),
    /env key/
  );
});

test("the tag is sixteen hex after the prefix", () => {
  assert.match(new Recipe("x", "alpine").tag(), /^r-[0-9a-f]{16}$/);
});

// ── validation ───────────────────────────────────────────────────────────────

test("the name must be a single lowercase path component", () => {
  for (const bad of ["", "UPPER", "with/slash", "-leading", "trailing-", "sp ace"]) {
    assert.throws(() => new Recipe(bad, "alpine").validate(), /recipe name/, `accepted ${bad}`);
  }
  new Recipe("a.b_c-d9", "alpine").validate();
});

test("a trailing backslash would splice two Dockerfile lines", () => {
  assert.throws(() => new Recipe("x", "alpine", { run: ["echo hi \\"] }).validate(), /backslash/);
});

test("a credential in an env URL is refused", () => {
  assert.throws(
    () => new Recipe("x", "alpine", { env: { PIP_INDEX_URL: "https://u:p@mirror/simple" } }).validate(),
    /user:password/
  );
  new Recipe("x", "alpine", { env: { PIP_INDEX_URL: "https://mirror/simple" } }).validate();
});

// The rules below were found by differentially fuzzing this validate() against
// the builder's: each is a recipe the SDK used to accept and the builder
// refuses, which meant the error arrived in a build log minutes later instead of
// where the recipe was written.

test("a name longer than the builder allows is refused", () => {
  new Recipe("a".repeat(128), "alpine").validate();
  assert.throws(() => new Recipe("a".repeat(129), "alpine").validate(), /recipe name/);
});

test("a lone surrogate is refused by name", () => {
  // JavaScript strings can hold one; UTF-8 cannot represent it, so
  // JSON.stringify emits \ud800 and the builder stops at "unexpected end of hex
  // escape" — the build task fails on its own recipe, minutes later.
  assert.throws(
    () => new Recipe("x", "alpine", { files: [{ path: "/a", content: "lone\ud800" }] }).validate(),
    /unpaired UTF-16 surrogate/
  );
  assert.throws(
    () => new Recipe("x\udfff", "alpine").validate(),
    /unpaired UTF-16 surrogate/
  );
  // A real astral character is a pair and is fine.
  new Recipe("x", "alpine", { files: [{ path: "/a", content: "\u{1f409}" }] }).validate();
});

test("an empty entry in a list is refused", () => {
  for (const field of ["apt", "pip", "run"]) {
    for (const entry of ["", "   "]) {
      assert.throws(
        () => new Recipe("x", "alpine", { [field]: [entry] }).validate(),
        /empty entry/,
        `${field} accepted ${JSON.stringify(entry)}`
      );
    }
  }
});

test("a backslash in a package specifier is refused but allowed in run", () => {
  for (const field of ["apt", "pip"]) {
    assert.throws(
      () => new Recipe("x", "alpine", { [field]: ["pkg\\x"] }).validate(),
      /backslash/,
      field
    );
  }
  new Recipe("x", "alpine", { run: ["echo a\\x"] }).validate();
});

test("an installer option is not a package", () => {
  // Found by adversarially reviewing the MCP tool, which offers no shell field
  // and could still reach one: `apt` entries are argv words to `apt-get
  // install`, and `apt-get -o DPkg::Pre-Invoke::=<shell>` runs that shell as
  // root during the build. `pip --index-url` is the same shape. Quoting is not
  // a defence; quoting is what makes `-o` a clean argument.
  for (const field of ["apt", "pip"]) {
    for (const entry of ["-o", "--index-url", "-e", "--config-settings=x"]) {
      assert.throws(
        () => new Recipe("x", "alpine", { [field]: [entry] }).validate(),
        /option to the installer/,
        `${field} accepted ${entry}`
      );
    }
  }
  new Recipe("x", "alpine", { run: ["apt-get -o Foo=bar install curl"] }).validate();
  new Recipe("x", "alpine", { apt: ["ca-certificates"], pip: ["duckdb==1.1.3"] }).validate();
});

test("a credential URL is refused in a package list too", () => {
  for (const field of ["apt", "pip", "run"]) {
    assert.throws(
      () => new Recipe("x", "alpine", { [field]: ["https://u:p@mirror/x"] }).validate(),
      /user:password/,
      field
    );
  }
});

test("an empty user or platform is refused", () => {
  assert.throws(() => new Recipe("x", "alpine", { user: "" }).validate(), /user/);
  assert.throws(() => new Recipe("x", "alpine", { user: "two words" }).validate(), /user/);
  assert.throws(() => new Recipe("x", "alpine", { platform: "" }).validate(), /platform/);
  assert.throws(() => new Recipe("x", "alpine", { platform: "two words" }).validate(), /platform/);
});

test("the same file path twice is refused", () => {
  assert.throws(
    () =>
      new Recipe("x", "alpine", {
        files: [new RecipeFile("/a", "one"), new RecipeFile("/a", "two")],
      }).validate(),
    /listed twice/
  );
  new Recipe("x", "alpine", {
    files: [new RecipeFile("/a", "one"), new RecipeFile("/b", "two")],
  }).validate();
});

test("a trailing backslash is refused wherever it would splice a line", () => {
  assert.throws(() => new Recipe("x", "alpine\\").validate(), /backslash/);
  assert.throws(() => new Recipe("x", "alpine", { workdir: "/app\\" }).validate(), /backslash/);
  assert.throws(() => new Recipe("x", "alpine", { user: "app\\" }).validate(), /backslash/);
  assert.throws(
    () => new Recipe("x", "alpine", { files: [new RecipeFile("/a\\", "x")] }).validate(),
    /backslash/
  );
});

test("paths must be absolute and clean", () => {
  for (const bad of ["relative", "/a/../b", "/a//b", "/with space", "/a/."]) {
    assert.throws(
      () => new Recipe("x", "alpine", { files: [new RecipeFile(bad, "c")] }).validate(),
      /must be absolute/,
      `accepted ${bad}`
    );
  }
  new Recipe("x", "alpine", { files: [new RecipeFile("/a/b.py", "c")] }).validate();
});

// ── the build task the SDK injects ───────────────────────────────────────────

function tasksOf(dag) {
  return Object.fromEntries(dag.toSpec().tasks.map((t) => [t.name, t]));
}

test("a recipe image adds one build and a dependency", () => {
  const r = new Recipe("etl", "python:3.12-slim", { pip: ["duckdb==1.1.3"] });
  const dag = new Dag("nightly");
  dag.task("query", { image: r, command: ["python", "/app/q.py"] });
  const t = tasksOf(dag);
  assert.deepEqual(Object.keys(t).sort(), ["build-etl", "query"]);
  assert.deepEqual(t.query.depends_on, ["build-etl"]);
  assert.equal(t.query.docker_image, r.imageRef());
  assert.deepEqual(t["build-etl"].command, ["dagron-build"]);
  assert.equal(t["build-etl"].runner_class, "build");
  assert.deepEqual(t["build-etl"].produces, [`oci://${r.imageRef()}`]);
});

test("the embedded recipe is the canonical form and round-trips to the same tag", () => {
  const r = new Recipe("etl", "alpine", { files: [new RecipeFile("/a", "x\n")] });
  const dag = new Dag("w");
  dag.task("t", { image: r, command: ["true"] });
  const env = Object.fromEntries(tasksOf(dag)["build-etl"].env.map((e) => [e.name, e.value]));
  assert.equal(env.DAGRON_BUILD_RECIPE, r.canonicalJson());
  const parsed = JSON.parse(env.DAGRON_BUILD_RECIPE);
  assert.equal(
    new Recipe(parsed.name, parsed.base, { files: parsed.files }).tag(),
    r.tag()
  );
});

test("one recipe shared by several tasks is built once", () => {
  const r = new Recipe("etl", "alpine");
  const dag = new Dag("w");
  const a = dag.task("a", { image: r, command: ["true"] });
  dag.task("b", { image: r, command: ["true"], dependsOn: [a] });
  dag.task("c", { image: r, command: ["true"] });
  const t = tasksOf(dag);
  assert.equal(Object.keys(t).filter((n) => n.startsWith("build-")).length, 1);
  assert.deepEqual(t.b.depends_on, ["a", "build-etl"]);
});

test("two different recipes named the same get two builds", () => {
  const dag = new Dag("w");
  dag.task("a", { image: new Recipe("etl", "alpine:3.20"), command: ["true"] });
  dag.task("b", { image: new Recipe("etl", "alpine:3.19"), command: ["true"] });
  const t = tasksOf(dag);
  assert.equal(Object.keys(t).filter((n) => n.startsWith("build-")).length, 2);
  assert.notEqual(t.a.docker_image, t.b.docker_image);
});

test("a repository pins both halves in the spec", () => {
  const r = new Recipe("etl", "alpine");
  const dag = new Dag("w", { imageRepository: "registry.example/ws-8f3a" });
  dag.task("t", { image: r, command: ["true"] });
  const t = tasksOf(dag);
  const env = Object.fromEntries(t["build-etl"].env.map((e) => [e.name, e.value]));
  assert.equal(env.DAGRON_IMAGE_REPOSITORY, "registry.example/ws-8f3a");
  assert.equal(env.DAGRON_BUILD_PUSH, "1");
  assert.equal(t.t.docker_image, `registry.example/ws-8f3a/etl:${r.tag()}`);
});

// A template is a task set like any other, so its build settings are the DAG's.
// Without the propagation the template kept the TaskSet defaults, and the same
// recipe produced `etl:r-<tag>` inside a template and the prefixed reference
// outside it -- one spec, two references, the template's never pushed.
test("a recipe inside a template uses the DAG's repository", () => {
  const r = new Recipe("etl", "alpine");
  const dag = new Dag("w", { imageRepository: "registry.example/ws-8f3a" });
  const tpl = dag.template("sub");
  tpl.task("t", { image: r, command: ["true"] });
  const spec = dag.toSpec();
  const t = Object.fromEntries(spec.templates[0].tasks.map((x) => [x.name, x]));
  const env = Object.fromEntries(t["build-etl"].env.map((e) => [e.name, e.value]));
  assert.equal(env.DAGRON_IMAGE_REPOSITORY, "registry.example/ws-8f3a");
  assert.equal(env.DAGRON_BUILD_PUSH, "1");
  assert.equal(t.t.docker_image, `registry.example/ws-8f3a/etl:${r.tag()}`);

  // And it is the same reference the DAG itself would produce.
  dag.task("direct", { image: r, command: ["true"] });
  const top = Object.fromEntries(dag.toSpec().tasks.map((x) => [x.name, x]));
  assert.equal(top.direct.docker_image, t.t.docker_image);
});

test("no repository means a daemon-local image and no push", () => {
  const r = new Recipe("etl", "alpine");
  const dag = new Dag("w");
  dag.task("t", { image: r, command: ["true"] });
  const env = Object.fromEntries(tasksOf(dag)["build-etl"].env.map((e) => [e.name, e.value]));
  assert.equal(env.DAGRON_BUILD_PUSH, undefined);
  assert.equal(env.DAGRON_IMAGE_REPOSITORY, undefined);
  assert.equal(tasksOf(dag).t.docker_image, `etl:${r.tag()}`);
});

test("a build task never shadows an author's own task", () => {
  const dag = new Dag("w");
  dag.task("build-etl", { command: ["true"] });
  dag.task("t", { image: new Recipe("etl", "alpine"), command: ["true"] });
  const t = tasksOf(dag);
  assert.deepEqual(t["build-etl"].command, ["true"]);
  const injected = Object.keys(t).filter((n) => n.startsWith("build-etl-"));
  assert.equal(injected.length, 1);
  assert.deepEqual(t.t.depends_on, injected);
});

test("the author's name wins over the injected one", () => {
  // This SDK reserved the injected name first and threw "duplicate task" on the
  // author's own call, so the same program worked in Python and failed here.
  const dag = new Dag("w");
  dag.task("build-etl", { image: new Recipe("etl", "alpine"), command: ["true"] });
  const t = tasksOf(dag);
  assert.equal(Object.keys(t).length, 2, JSON.stringify(Object.keys(t)));
  assert.deepEqual(t["build-etl"].command, ["true"], "the author's task was replaced");
});

test("a rejected recipe does not reserve the task name", () => {
  const dag = new Dag("w");
  assert.throws(() => dag.task("t", { image: new Recipe("BAD NAME", "alpine"), command: ["true"] }));
  dag.task("t", { image: new Recipe("etl", "alpine"), command: ["true"] });
  assert.deepEqual(Object.keys(tasksOf(dag)).sort(), ["build-etl", "t"]);
});

test("toSpec hands out a copy, not the builder's own tasks", () => {
  const dag = new Dag("w");
  dag.task("a", { command: ["true"] });
  const spec = dag.toSpec();
  spec.tasks[0].name = "hijacked";
  spec.tasks.push({ name: "injected" });
  assert.deepEqual(Object.keys(tasksOf(dag)), ["a"]);
});

test("the build runner class and timeout are configurable", () => {
  const dag = new Dag("w", { buildRunnerClass: "images", buildTimeoutSecs: 120 });
  dag.task("t", { image: new Recipe("etl", "alpine"), command: ["true"] });
  const build = tasksOf(dag)["build-etl"];
  assert.equal(build.runner_class, "images");
  assert.equal(build.timeout_secs, 120);
});

test("an invalid recipe fails where it was written", () => {
  const dag = new Dag("w");
  assert.throws(() => dag.task("t", { image: new Recipe("BAD NAME", "alpine"), command: ["true"] }));
});

test("a plain string image still works", () => {
  const dag = new Dag("w");
  dag.task("t", { image: "alpine:3.20", command: ["true"] });
  assert.deepEqual(Object.keys(tasksOf(dag)), ["t"]);
  assert.equal(tasksOf(dag).t.docker_image, "alpine:3.20");
});
