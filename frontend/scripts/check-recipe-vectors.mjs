#!/usr/bin/env node
// Hold the console's recipe implementation to the same contract as the builder
// and the two SDKs.
//
// `sdks/recipe-vectors.json` is generated from `ee/dagron-build` — the program
// that actually produces the image — and every implementation of the
// content-addressed tag is checked against it. This one exists separately from
// the SDKs because the console runs in a browser and cannot use `node:crypto`;
// a fourth implementation is a fourth thing that can drift, and a drifted tag is
// not an error, it is a task pinned to an image no build will ever push.
//
//     npm run check:recipe
//
// No test framework and no dependencies on purpose: this repo's frontend has
// neither, and a check that requires standing up a toolchain is a check nobody
// runs. Node strips the TypeScript types itself.

import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

// The file under test is enterprise-only and is stripped from the public
// mirror, where this script still ships (it is an ordinary npm script in a
// mirrored package.json). Say what is going on and succeed, rather than failing
// a public build with a module-not-found for a path that is *supposed* to be
// missing there.
const RECIPE_TS = new URL("../src/ee/recipe.ts", import.meta.url);
if (!existsSync(fileURLToPath(RECIPE_TS))) {
  console.log(
    "src/ee/recipe.ts is not in this build — nothing to check.\n" +
      "This check covers the enterprise 'build from a description' control; an\n" +
      "open build does not ship it.",
  );
  process.exit(0);
}

const { canonicalJson, recipeTag, recipeImageRef, validateRecipe, BUILD_GENERATOR_VERSION } =
  await import(RECIPE_TS.href);

const VECTORS = new URL("../../sdks/recipe-vectors.json", import.meta.url);
if (!existsSync(fileURLToPath(VECTORS))) {
  console.error(`the shared recipe vectors are missing at ${fileURLToPath(VECTORS)}`);
  process.exit(1);
}
const doc = JSON.parse(readFileSync(VECTORS, "utf8"));

let checked = 0;
const failures = [];
function check(label, actual, expected) {
  checked += 1;
  if (actual !== expected) failures.push(`${label}\n    expected ${expected}\n    got      ${actual}`);
}

check("generator version", BUILD_GENERATOR_VERSION, doc.generator_version);

for (const v of doc.vectors) {
  const r = v.recipe;
  // `run` and `dockerfile` are not offered by the console, so a vector using
  // either cannot be represented here — skip it rather than pretend, and say so.
  if (r.run || r.dockerfile) {
    console.log(`  skip  ${v.key.padEnd(24)} (uses a field the console does not offer)`);
    continue;
  }
  const recipe = {
    name: r.name,
    base: r.base,
    apt: r.apt,
    pip: r.pip,
    env: r.env,
    workdir: r.workdir,
    files: r.files,
    user: r.user,
    keepEntrypoint: r.keep_entrypoint,
    platform: r.platform,
  };
  const tag = await recipeTag(recipe);
  check(`${v.key}: tag — ${v.why}`, tag, v.tag);
  check(`${v.key}: image ref`, await recipeImageRef(recipe), v.image_ref);
  check(
    `${v.key}: prefixed image ref`,
    await recipeImageRef(recipe, doc.prefix_for_image_ref_with_prefix),
    v.image_ref_with_prefix,
  );
  console.log(`  ok    ${v.key.padEnd(24)} ${tag}`);
}

// The JavaScript-only ordering traps, which no golden vector can catch because
// the builder refuses these inputs outright. The env-key rule is what makes them
// unreachable, so it is checked as a rule.
const base = { name: "x", base: "alpine" };
for (const [label, env] of [
  ["an integer-like env key", { 10: "a", 9: "b" }],
  ["a non-ASCII env key", { "\u{10000}": "a", "": "b" }],
]) {
  checked += 1;
  const problems = validateRecipe({ ...base, env });
  if (!problems.some((p) => p.includes("Environment name"))) {
    failures.push(`${label} was accepted — object key order would silently change the tag`);
  }
}

// Validation parity. These rules were found by differentially fuzzing this
// file's validateRecipe against the builder's: each is a description the console
// used to accept and the builder refuses, which would have meant a green form
// and a build that failed minutes later.
const refuses = [
  ["a name longer than 128", { name: "a".repeat(129), base: "alpine" }],
  ["an empty package entry", { ...base, pip: [""] }],
  ["a backslash in a package", { ...base, apt: ["pkg\\x"] }],
  ["a credential URL in a package", { ...base, pip: ["https://u:p@mirror/x"] }],
  // apt-get -o DPkg::Pre-Invoke::=<shell> runs that shell as root during the
  // build; this form deliberately offers no way to run shell at build time, and
  // without this rule it offered one by accident.
  ["an apt installer option", { ...base, apt: ["-o", "DPkg::Pre-Invoke::=sh -c evil"] }],
  ["a pip installer option", { ...base, pip: ["--index-url", "https://attacker/simple"] }],
  ["an empty user", { ...base, user: "" }],
  ["a user with a space", { ...base, user: "two words" }],
  ["an empty platform", { ...base, platform: "" }],
  ["the same file path twice", { ...base, files: [{ path: "/a", content: "1" }, { path: "/a", content: "2" }] }],
  ["a trailing backslash on the base", { name: "x", base: "alpine\\" }],
  ["a trailing backslash on the workdir", { ...base, workdir: "/app\\" }],
  // An empty workdir is emitted by canonicalObject, so skipping validation for
  // it accepted a description the builder refuses with the form showing nothing.
  ["an empty working directory", { ...base, workdir: "" }],
  // A lone surrogate cannot be encoded as UTF-8: JSON.stringify emits \ud800 and
  // the builder cannot parse the recipe at all.
  ["a lone surrogate", { ...base, files: [{ path: "/a", content: "x\ud800" }] }],
];
for (const [label, recipe] of refuses) {
  checked += 1;
  if (validateRecipe(recipe).length === 0) {
    failures.push(`${label} was accepted — the builder refuses it, so the form would look fine and the build would fail`);
  }
}
// ...and the same shapes, valid, are accepted.
const accepts = [
  ["a 128-character name", { name: "a".repeat(128), base: "alpine" }],
  ["two different file paths", { ...base, files: [{ path: "/a", content: "1" }, { path: "/b", content: "2" }] }],
  ["an ordinary package list", { ...base, apt: ["curl"], pip: ["duckdb==1.1.3"] }],
  ["a real astral character", { ...base, files: [{ path: "/a", content: "\u{1f409}" }] }],
];
for (const [label, recipe] of accepts) {
  checked += 1;
  const problems = validateRecipe(recipe);
  if (problems.length) failures.push(`${label} was refused: ${problems.join(" ")}`);
}

// A canonical-form spot check that does not depend on the vector file.
check(
  "declaration order, not alphabetical",
  canonicalJson({ name: "x", base: "alpine", apt: ["a"], workdir: "/w" }),
  '{"name":"x","base":"alpine","apt":["a"],"workdir":"/w"}',
);

if (failures.length) {
  console.error(`\n${failures.length} of ${checked} checks FAILED:\n`);
  for (const f of failures) console.error("  " + f);
  console.error(
    "\nThe console now derives a different image reference than the builder. A task pinned\n" +
      "through the console would reference an image no build will ever push.\n",
  );
  process.exit(1);
}
console.log(`\n${checked} checks passed — the console agrees with the builder.`);
