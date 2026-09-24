#!/usr/bin/env node
// Hold the spec differ to the properties the diff views depend on.
//
//     npm run check:diff
//
// The console shows workflow history as a diff rather than as one YAML dump per
// entry, so the differ is now load-bearing: a wrong diff doesn't look broken, it
// looks like a change nobody made. Four properties are checked, and the
// randomized pass at the end is there because the first three only check the
// cases someone thought of.
//
//   * **Reconstruction.** Dropping the additions rebuilds the base exactly;
//     dropping the removals rebuilds the compared side. This is the one that
//     catches an off-by-one in the prefix/suffix trim, and it holds for every
//     input, so the fuzz pass can assert it thousands of times.
//   * **Minimality.** A one-line edit reports one line added and one removed,
//     not a wholesale replacement.
//   * **Line numbers.** Both columns count up in step with the real files.
//   * **Hunks.** Unchanged runs collapse, changes never do.
//
// No test framework, matching the other check scripts.

import assert from "node:assert/strict";
import { register } from "node:module";

register("./alias-hook.mjs", import.meta.url);

const { diffLines, splitLines, toHunks, diffStat } = await import("@/lib/diff");

let checks = 0;

/// Every diff must be able to rebuild both inputs it came from.
function assertRebuilds(a, b, label) {
  const d = diffLines(a, b);
  const base = d.lines.filter((l) => l.kind !== "add").map((l) => l.text);
  const head = d.lines.filter((l) => l.kind !== "del").map((l) => l.text);
  assert.deepEqual(base, splitLines(a), `${label}: dropping additions rebuilds the base`);
  assert.deepEqual(head, splitLines(b), `${label}: dropping removals rebuilds the new side`);
  // Line numbers must be present exactly where the kind implies, and ascending.
  let lastA = 0;
  let lastB = 0;
  for (const l of d.lines) {
    if (l.kind !== "add") {
      assert.equal(l.a, lastA + 1, `${label}: base line numbers are contiguous`);
      lastA = l.a;
    } else {
      assert.equal(l.a, undefined, `${label}: an added line has no base number`);
    }
    if (l.kind !== "del") {
      assert.equal(l.b, lastB + 1, `${label}: new-side line numbers are contiguous`);
      lastB = l.b;
    } else {
      assert.equal(l.b, undefined, `${label}: a removed line has no new-side number`);
    }
  }
  return d;
}

const SPEC_A = `name: my-workflow
tasks:
  - name: prepare
    command: ["echo", "prepare"]
  - name: process
    command: ["echo", "process"]
    depends_on: [prepare]
`;

// ── the cases that actually happen ──────────────────────────────────────────

{
  const d = assertRebuilds(SPEC_A, SPEC_A, "identical");
  assert.equal(d.added, 0);
  assert.equal(d.removed, 0);
  assert.equal(diffStat(d), null, "an unchanged spec has no stat line");
  assert.ok(d.lines.every((l) => l.kind === "same"));
  checks++;
}

{
  // The edit this feature exists for: one line changed deep in a spec.
  const b = SPEC_A.replace('command: ["echo", "process"]', 'command: ["echo", "PROCESS"]');
  const d = assertRebuilds(SPEC_A, b, "one-line edit");
  assert.equal(d.added, 1, "exactly one line added");
  assert.equal(d.removed, 1, "exactly one line removed");
  assert.equal(diffStat(d), "+1 −1");
  // …and it reads as `- old` then `+ new`, not the reverse.
  const i = d.lines.findIndex((l) => l.kind !== "same");
  assert.equal(d.lines[i].kind, "del", "the removal comes first");
  assert.equal(d.lines[i + 1].kind, "add", "then the addition");
  checks++;
}

{
  // Appending a task — the other common edit.
  const b = SPEC_A + `  - name: publish\n    command: ["echo", "publish"]\n`;
  const d = assertRebuilds(SPEC_A, b, "task appended");
  assert.equal(d.removed, 0, "appending removes nothing");
  assert.equal(d.added, 2);
  checks++;
}

{
  const d = assertRebuilds(SPEC_A, "", "everything deleted");
  assert.equal(d.added, 0);
  assert.equal(d.removed, splitLines(SPEC_A).length);
  checks++;
}

{
  const d = assertRebuilds("", SPEC_A, "created from nothing");
  assert.equal(d.removed, 0);
  assert.equal(d.added, splitLines(SPEC_A).length);
  checks++;
}

{
  // A trailing newline is not a change. Both of these are the same document,
  // and reporting a phantom edit on every save would make the view useless.
  const d = diffLines("a\nb\n", "a\nb");
  assert.equal(d.added, 0, "a missing trailing newline is not an addition");
  assert.equal(d.removed, 0, "…nor a removal");
  checks++;
}

{
  // CRLF likewise — a spec round-tripped through a Windows editor is not a
  // rewrite of every line.
  const d = diffLines("a\r\nb\r\n", "a\nb\n");
  assert.equal(d.added + d.removed, 0, "line endings are normalized, not diffed");
  checks++;
}

{
  // Repeated lines are where a naive differ drifts: YAML is full of identical
  // `command:` and `depends_on:` lines, so the result must still rebuild.
  const a = "x\nx\nx\ny\n";
  const b = "x\ny\nx\nx\n";
  assertRebuilds(a, b, "repeated lines");
  checks++;
}

// ── hunks ───────────────────────────────────────────────────────────────────

{
  // 100 unchanged lines with one edit in the middle collapses to one hunk of
  // (1 change + 2×3 context), not 100 rows.
  const a = Array.from({ length: 101 }, (_, i) => `line ${i}`).join("\n");
  const b = a.replace("line 50", "line fifty");
  const d = diffLines(a, b);
  const hunks = toHunks(d.lines, 3);
  assert.equal(hunks.length, 1, "one edit is one hunk");
  assert.equal(hunks[0].lines.length, 8, "1 removed + 1 added + 6 context");
  assert.equal(hunks[0].skipped, 47, "the unchanged run before it is counted, not rendered");
  assert.equal(hunks[0].a, 48, "the hunk names where it starts in the base");
  checks++;
}

{
  // Two distant edits stay two hunks; two adjacent ones merge into one.
  const a = Array.from({ length: 60 }, (_, i) => `l${i}`).join("\n");
  const far = a.replace("l5", "L5").replace("l50", "L50");
  assert.equal(toHunks(diffLines(a, far).lines, 3).length, 2, "distant edits stay apart");
  const near = a.replace("l5", "L5").replace("l7", "L7");
  assert.equal(toHunks(diffLines(a, near).lines, 3).length, 1, "nearby edits merge");
  checks++;
}

{
  // An unchanged file has no hunks at all — the view says "identical" instead
  // of rendering an empty frame.
  assert.deepEqual(toHunks(diffLines(SPEC_A, SPEC_A).lines, 3), [], "no changes, no hunks");
  checks++;
}

// ── randomized: the cases nobody thought of ─────────────────────────────────

{
  // A cheap deterministic PRNG so a failure is reproducible from the seed.
  let seed = 0x2f6e2b1;
  const rnd = (n) => {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    return seed % n;
  };
  // A small alphabet on purpose: lots of repeated lines is the hard case, and
  // it is also what YAML looks like.
  const alphabet = ["a", "b", "c", "  d: 1", "  d: 2", ""];
  for (let t = 0; t < 400; t++) {
    const mk = () =>
      Array.from({ length: rnd(14) }, () => alphabet[rnd(alphabet.length)]).join("\n");
    const a = mk();
    const b = mk();
    const d = assertRebuilds(a, b, `fuzz #${t}`);
    // Minimality: the diff never reports more churn than replacing wholesale.
    assert.ok(
      d.added <= splitLines(b).length && d.removed <= splitLines(a).length,
      `fuzz #${t}: never worse than a full replacement`,
    );
    // Hunks must cover every change, whatever the shape.
    const inHunks = toHunks(d.lines, 2).flatMap((h) => h.lines).filter((l) => l.kind !== "same");
    assert.equal(
      inHunks.length,
      d.added + d.removed,
      `fuzz #${t}: every change appears in some hunk`,
    );
  }
  checks++;
}

// ── grouping runs into the definitions they ran ─────────────────────────────

// Every run snapshots its own definition row, so `definition_id` is unique per
// run and useless for grouping — the eras have to come from the spec text. That
// is easy to get subtly wrong in the direction that looks right (a workflow
// nobody edited rendering as 25 separate "changes"), so it is checked directly.
{
  const { groupEras, eraIndexByRun } = await import("@/lib/spec-eras");
  // Newest first, the order the API returns and the table shows.
  const run = (id, iso) => ({ id, created_at: iso, definition_id: `def-${id}` });
  const runs = [
    run("r5", "2026-09-05T00:00:00Z"),
    run("r4", "2026-09-04T00:00:00Z"),
    run("r3", "2026-09-03T00:00:00Z"),
    run("r2", "2026-09-02T00:00:00Z"),
    run("r1", "2026-09-01T00:00:00Z"),
  ];

  {
    // Untouched workflow: distinct definition ids, one era.
    const specs = new Map(runs.map((r) => [r.id, SPEC_A]));
    const { eras, unresolved } = groupEras(runs, specs);
    assert.equal(eras.length, 1, "identical specs are one era, whatever the definition ids");
    assert.equal(unresolved, 0);
    assert.equal(eras[0].runs.length, 5);
    assert.equal(eras[0].firstRun.id, "r1", "the era starts at its oldest run");
    assert.equal(eras[0].lastRun.id, "r5", "…and ends at its newest");
    assert.deepEqual(eras[0].runs.map((r) => r.id), ["r5", "r4", "r3", "r2", "r1"], "runs stay newest-first");
    checks++;
  }

  {
    // Edited after r2: two eras, oldest first.
    const b = SPEC_A.replace("prepare", "PREPARE");
    const specs = new Map([
      ["r1", SPEC_A],
      ["r2", SPEC_A],
      ["r3", b],
      ["r4", b],
      ["r5", b],
    ]);
    const { eras } = groupEras(runs, specs);
    assert.equal(eras.length, 2);
    assert.equal(eras[0].firstRun.id, "r1", "era 0 is the oldest definition");
    assert.equal(eras[1].firstRun.id, "r3", "era 1 starts at the first run that used the new spec");
    assert.equal(eras[0].runs.length, 2);
    assert.equal(eras[1].runs.length, 3);
    const byRun = eraIndexByRun(eras);
    assert.equal(byRun.get("r2"), 0);
    assert.equal(byRun.get("r3"), 1);
    checks++;
  }

  {
    // Edited and reverted: three eras, not two. The workflow really did run
    // something else in between, and merging era 0 with era 2 would hide it.
    const b = SPEC_A.replace("prepare", "PREPARE");
    const specs = new Map([
      ["r1", SPEC_A],
      ["r2", b],
      ["r3", b],
      ["r4", SPEC_A],
      ["r5", SPEC_A],
    ]);
    const { eras } = groupEras(runs, specs);
    assert.equal(eras.length, 3, "a revert is a third era, not a re-entry into the first");
    assert.equal(eras[0].spec, eras[2].spec, "…even though its text matches the first");
    assert.equal(eras[2].firstRun.id, "r4");
    checks++;
  }

  {
    // Unreadable specs are left out and counted, never guessed at.
    const specs = new Map([
      ["r1", SPEC_A],
      ["r5", SPEC_A],
    ]);
    const { eras, unresolved } = groupEras(runs, specs);
    assert.equal(unresolved, 3, "the three runs with no spec are reported");
    assert.equal(eras.length, 1, "the two that resolved are adjacent once the gaps drop out");
    assert.equal(eraIndexByRun(eras).has("r3"), false, "an unresolved run belongs to no era");
    checks++;
  }

  {
    assert.deepEqual(groupEras([], new Map()), { eras: [], unresolved: 0 }, "no runs, no eras");
    checks++;
  }
}

console.log(`spec diff: ${checks} checks passed`);
