#!/usr/bin/env node
// Hold the console's reading of a CI job's log, and of a run's jobs, to what
// the step runner writes and the engine names.
//
//     npm run check:ci
//
// The run page shows a job's log by step, with its report beside it, by
// reading the lines dagron-ci-exec writes among the job's own
// (`::dagron-ci::…`, crates/dagron-ci-exec/README.md). dagron-ci reads the
// same lines for a check run (crates/dagron-ci/src/said.rs), and holds them
// to bounds whatever wrote them, since a step can print any of them. The
// console must agree with it: what one shows and the other posts is one
// report. A run's jobs are read from its tasks' names, the one thing that
// says which task is an instance of which job.
//
// No test framework, matching the other check scripts.

import assert from "node:assert/strict";
import { register } from "node:module";

register("./alias-hook.mjs", import.meta.url);

const { parseCiLog, hasCiSteps, hasCiReport, stepReads, countsLine, SKIPPED_LINE } = await import("@/lib/ci-log");
const { jobRows, hasInstances, jobStatus } = await import("@/lib/ci-jobs");

// ── A job's log, by step ─────────────────────────────────────────────────────

const output = [
  "Cloning into '.'...",
  "::dagron-ci::step::1::start::install",
  "added 12 packages",
  "::dagron-ci::warning::no timeout(1) in this image",
  "::dagron-ci::step::1::end::0",
  "::dagron-ci::step::2::start::flaky",
  "this step fails on purpose",
  "::dagron-ci::step::2::end::7",
  "::dagron-ci::step::2::continued",
  "::dagron-ci::step::3::start::test",
  "echo ::dagron-ci::step::9::end::0",
  "1 failed\r",
  "::dagron-ci::step::3::end::1",
  '::dagron-ci::report::{"tests":12,"failures":1,"errors":0,"skipped":2,"findings":1,"more_annotations":0,"more_failed":0,"problems":["lint.sarif could not be read: no such file"]}',
  '::dagron-ci::annotation::{"path":"src/lib.rs","start_line":7,"end_line":9,"level":"failure","title":"t.bad","message":"left != right"}',
  '::dagron-ci::failed::{"name":"t.bad","message":"left != right\\nat line 7"}',
  '::dagron-ci::summary::"## Built\\n\\n"',
  '::dagron-ci::summary::"3 crates."',
  '::dagron-ci::result::{"protocol":1,"exit_code":1}',
  '::dagron-ci::job::{"outputs":{},"result":"failure"}',
  "::dagron-ci::newer::something a later runner says",
  "",
].join("\n");

assert.equal(hasCiSteps(output), true);
assert.equal(hasCiSteps("plain output\nof a task\n"), false);
assert.equal(hasCiSteps("echo ::dagron-ci::step::1::start::x"), false, "a marker is a line's beginning");

const log = parseCiLog(output);
assert.deepEqual(
  log.segments.map((s) => (s.kind === "step" ? `step ${s.step.n} ${s.step.name}` : `lines ${s.lines.length}`)),
  ["lines 1", "step 1 install", "step 2 flaky", "step 3 test"],
  "the log in its order: what came before the steps, then each step",
);
const [before, install, flaky, test] = log.segments;
assert.deepEqual(before.lines, ["Cloning into '.'..."]);
assert.deepEqual(
  install.step.lines,
  ["added 12 packages", "warning: no timeout(1) in this image"],
  "what the runner says to a reader stands where it was said",
);
assert.deepEqual([install.step.code, install.step.continued], [0, false]);
assert.deepEqual([flaky.step.code, flaky.step.continued], [7, true], "a step that failed and let the job go on");
assert.deepEqual(
  test.step.lines,
  ["echo ::dagron-ci::step::9::end::0", "1 failed"],
  "a line that only holds a marker is the step's own, and a carriage return is not part of a line",
);
assert.equal(test.step.code, 1);
assert.equal(
  log.segments.flatMap((s) => (s.kind === "step" ? s.step.lines : s.lines)).some((l) => l.includes("::dagron-ci::result")),
  false,
  "what is for a machine, and what this does not know, is left out",
);

assert.deepEqual(stepReads(install.step, false), { label: "ok", tone: "ok" });
assert.deepEqual(stepReads(flaky.step, false), { label: "failed, exit 7 (the job went on)", tone: "failed" });
assert.deepEqual(stepReads(test.step, false), { label: "failed, exit 1", tone: "failed" });
assert.deepEqual(stepReads({ ...test.step, code: 124 }, false), { label: "timed out", tone: "failed" });

// A step whose end is not in the log yet: running while its task is, and
// said not to have ended once the task is over.
const tailing = parseCiLog("::dagron-ci::step::1::start::build\ncompiling");
assert.deepEqual(tailing.segments[0].step.lines, ["compiling"]);
assert.deepEqual(stepReads(tailing.segments[0].step, true), { label: "running", tone: "running" });
assert.deepEqual(stepReads(tailing.segments[0].step, false), { label: "did not end", tone: "unknown" });

// A job that ran no step (its condition did not hold) has a log of the
// runner's lines alone, each for a machine. A reader is told that it ran
// none, in place of a log with nothing in it.
const skipped = [
  "::dagron-ci::job::skipped",
  '::dagron-ci::result::{"exit_code":0,"protocol":3,"result":"skipped"}',
  '::dagron-ci::job::{"outputs":{},"result":"skipped"}',
  "",
].join("\n");
assert.equal(hasCiSteps(skipped), true);
assert.deepEqual(parseCiLog(skipped).segments, [{ kind: "lines", lines: [SKIPPED_LINE] }]);

// ── What the job said of itself ──────────────────────────────────────────────

assert.equal(hasCiReport(log), true);
assert.equal(hasCiReport(tailing), false);
assert.deepEqual(log.counts, {
  tests: 12,
  failures: 1,
  errors: 0,
  skipped: 2,
  findings: 1,
  more_annotations: 0,
  more_failed: 0,
  problems: ["lint.sarif could not be read: no such file"],
});
assert.equal(countsLine(log.counts), "12 tests: 1 failed, 0 errors, 2 skipped; 1 finding");
assert.deepEqual(log.annotations, [
  { path: "src/lib.rs", start_line: 7, end_line: 9, level: "failure", title: "t.bad", message: "left != right" },
]);
assert.deepEqual(log.failed, [{ name: "t.bad", message: "left != right\nat line 7" }]);
assert.equal(log.summary, "## Built\n\n3 crates.");

// The lines are a job's own word, on a stream its steps write to: what they
// may say is held to bounds here, as dagron-ci holds it for a check run.
const at = (path, line) =>
  `::dagron-ci::annotation::${JSON.stringify({ path, start_line: line, level: "shout", title: "t", message: "" })}`;
const outside = [
  at("/etc/passwd", 1),
  at("../secrets", 1),
  at("a/../../b", 1),
  at("a\\b", 1),
  at("", 1),
  at("src/lib.rs", 0),
  at("src/lib.rs", -4),
  at("src/lib.rs", 1.5),
  "::dagron-ci::annotation::not json",
  "::dagron-ci::annotation::[1,2]",
].join("\n");
assert.deepEqual(parseCiLog(outside).annotations, [], "a place outside the repository, and no place, is no annotation");
const kept = parseCiLog(at("src/lib.rs", 3)).annotations[0];
assert.deepEqual([kept.level, kept.message, kept.end_line], ["notice", "(no message)", 3]);
const many = parseCiLog(Array.from({ length: 59 }, (_, i) => at("src/lib.rs", i + 1)).join("\n"));
assert.equal(many.annotations.length, 50, "no more than a check run takes");
assert.equal(many.counts.more_annotations, 9, "and the rest is counted");
const long = parseCiLog(`::dagron-ci::failed::${JSON.stringify({ name: "n".repeat(5000), message: "m".repeat(5000) })}`);
assert.ok([...long.failed[0].name].length <= 160 && [...long.failed[0].message].length <= 400);
const big = parseCiLog(Array.from({ length: 20 }, () => `::dagron-ci::summary::${JSON.stringify("é".repeat(3000))}`).join("\n"));
assert.equal(big.summary.length, 32 * 1024, "a summary is kept to its size");

// ── A run's jobs ─────────────────────────────────────────────────────────────

const t = (name, status = "succeeded") => ({ id: `t-${name}`, name, status });
const rows = jobRows([
  t("lint"),
  t("test.linux-18", "failed"),
  t("test.linux-20"),
  t("test.macos-20", "running"),
  t("build.strategy.matrix"),
  t("build"),
  t("build.a"),
  t("build.b"),
  t("docs.site"),
]);
assert.deepEqual(
  rows.map((r) => [r.job, r.head?.name ?? null, r.cells.map((c) => c.label)]),
  [
    ["lint", null, [""]],
    ["test", null, ["linux-18", "linux-20", "macos-20"]],
    ["build.strategy.matrix", null, [""]],
    ["build", "build", ["a", "b"]],
    ["docs.site", null, [""]],
  ],
  "a job a row, in the order of its first task; a name alone with a dot in it is a job of that name",
);
assert.equal(hasInstances(rows), true);
assert.equal(hasInstances(jobRows([t("lint"), t("test"), t("docs.site")])), false);
assert.equal(jobStatus(rows[1]), "failed", "a job reads as the worst of its instances");
assert.equal(jobStatus(rows[3]), "succeeded");
assert.equal(jobStatus(jobRows([t("only", "running")])[0]), "running");
assert.equal(rows[1].cells[0].id, "t-test.linux-18", "a cell is its task");

console.log("ok — a job's log is read by step, its report held to bounds, and a run's jobs told from its tasks' names");
