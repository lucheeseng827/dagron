// Line diff for workflow specs — no dependency, because the console has none
// and a diff view is not worth its first one.
//
// The shape of the problem is narrow and that is what makes a small
// implementation honest: two YAML specs, usually a few dozen to a few hundred
// lines, almost always differing in a handful of them. So:
//
//   1. Trim the common prefix and suffix. A one-line edit to a 300-line spec
//      collapses to a 1×1 problem before any real work starts, which is the
//      case that actually happens.
//   2. Run a longest-common-subsequence table over what is left. Minimal, and
//      simple enough to check by reading — Myers would be asymptotically better
//      on inputs this never sees.
//   3. Refuse rather than hang. The table is O(n×m); past `MAX_CELLS` the diff
//      degrades to "this block was replaced" and says so, instead of allocating
//      gigabytes to be precise about two files that share nothing.
//
// Line numbers are 1-based and carried on every line, because the renderer
// shows both columns and a hunk header has to name where it starts.

export type DiffKind = "same" | "add" | "del";

export interface DiffLine {
  kind: DiffKind;
  text: string;
  /// 1-based line number in the base (absent on an added line).
  a?: number;
  /// 1-based line number in the compared side (absent on a removed line).
  b?: number;
}

export interface DiffResult {
  lines: DiffLine[];
  added: number;
  removed: number;
  /// True when the inputs were too large to diff exactly and the changed region
  /// was reported as one wholesale replacement. Surfaced in the UI — a diff that
  /// quietly stopped being a diff is worse than one that says so.
  truncated: boolean;
}

/// Cells in the LCS table before the exact diff gives up. 4M is ~16 MB as a
/// Uint32Array, and is far beyond any spec a person edits by hand.
const MAX_CELLS = 4_000_000;

/// Split into lines for diffing. A trailing newline is not a final empty line —
/// every well-formed YAML file ends with one, and counting it would report a
/// phantom change whenever only one side had it.
export function splitLines(text: string): string[] {
  const s = text.replace(/\r\n?/g, "\n");
  const lines = s.split("\n");
  if (lines.length && lines[lines.length - 1] === "") lines.pop();
  return lines;
}

/// Diff two texts by line. Total: identical inputs give all-`same` lines and
/// zero counts, and either side may be empty.
export function diffLines(aText: string, bText: string): DiffResult {
  const a = splitLines(aText);
  const b = splitLines(bText);

  // Common prefix.
  let head = 0;
  while (head < a.length && head < b.length && a[head] === b[head]) head++;
  // Common suffix, never overlapping the prefix.
  let tail = 0;
  while (
    tail < a.length - head &&
    tail < b.length - head &&
    a[a.length - 1 - tail] === b[b.length - 1 - tail]
  ) {
    tail++;
  }

  const out: DiffLine[] = [];
  for (let i = 0; i < head; i++) out.push({ kind: "same", text: a[i], a: i + 1, b: i + 1 });

  const aMid = a.slice(head, a.length - tail);
  const bMid = b.slice(head, b.length - tail);
  const mid = diffMiddle(aMid, bMid, head);
  out.push(...mid.lines);

  for (let i = 0; i < tail; i++) {
    const ai = a.length - tail + i;
    const bi = b.length - tail + i;
    out.push({ kind: "same", text: a[ai], a: ai + 1, b: bi + 1 });
  }

  let added = 0;
  let removed = 0;
  for (const l of out) {
    if (l.kind === "add") added++;
    else if (l.kind === "del") removed++;
  }
  return { lines: out, added, removed, truncated: mid.truncated };
}

/// Diff the region left after trimming. `offset` is how many lines were trimmed
/// off the front, so the emitted line numbers are absolute.
function diffMiddle(
  a: string[],
  b: string[],
  offset: number,
): { lines: DiffLine[]; truncated: boolean } {
  // One side empty: pure insert or pure delete, no table needed.
  if (!a.length || !b.length) {
    return { lines: [...del(a, offset), ...add(b, offset)], truncated: false };
  }
  if ((a.length + 1) * (b.length + 1) > MAX_CELLS) {
    // Too big to be exact. Report the whole changed region as replaced — true,
    // just coarse — and let the caller say so.
    return { lines: [...del(a, offset), ...add(b, offset)], truncated: true };
  }

  // lcs[i][j] = length of the LCS of a[i..] and b[j..], flattened row-major
  // with a (b.length + 1)-wide stride.
  const w = b.length + 1;
  const lcs = new Uint32Array((a.length + 1) * w);
  for (let i = a.length - 1; i >= 0; i--) {
    for (let j = b.length - 1; j >= 0; j--) {
      lcs[i * w + j] =
        a[i] === b[j]
          ? lcs[(i + 1) * w + j + 1] + 1
          : Math.max(lcs[(i + 1) * w + j], lcs[i * w + j + 1]);
    }
  }

  const lines: DiffLine[] = [];
  let i = 0;
  let j = 0;
  while (i < a.length && j < b.length) {
    if (a[i] === b[j]) {
      lines.push({ kind: "same", text: a[i], a: offset + i + 1, b: offset + j + 1 });
      i++;
      j++;
    } else if (lcs[(i + 1) * w + j] >= lcs[i * w + j + 1]) {
      // Deletions before insertions at the same position, so a changed line
      // reads as `- old` then `+ new` rather than the other way round.
      lines.push({ kind: "del", text: a[i], a: offset + i + 1 });
      i++;
    } else {
      lines.push({ kind: "add", text: b[j], b: offset + j + 1 });
      j++;
    }
  }
  lines.push(...del(a.slice(i), offset + i));
  lines.push(...add(b.slice(j), offset + j));
  return { lines, truncated: false };
}

const del = (lines: string[], offset: number): DiffLine[] =>
  lines.map((text, k) => ({ kind: "del" as const, text, a: offset + k + 1 }));

const add = (lines: string[], offset: number): DiffLine[] =>
  lines.map((text, k) => ({ kind: "add" as const, text, b: offset + k + 1 }));

export interface DiffHunk {
  /// 1-based first line of this hunk on each side, for the `@@` header.
  a: number;
  b: number;
  lines: DiffLine[];
  /// Unchanged lines skipped between the previous hunk and this one. 0 for the
  /// first hunk when the file starts with a change.
  skipped: number;
}

/// Group a diff into hunks, dropping runs of unchanged lines longer than
/// `2 × context`. A spec is mostly unchanged on any given save; showing all of
/// it buries the three lines that moved.
export function toHunks(lines: DiffLine[], context = 3): DiffHunk[] {
  const changed = lines.map((l) => l.kind !== "same");
  // Which lines to keep: every change, plus `context` either side.
  const keep = new Array<boolean>(lines.length).fill(false);
  for (let i = 0; i < lines.length; i++) {
    if (!changed[i]) continue;
    for (let k = Math.max(0, i - context); k <= Math.min(lines.length - 1, i + context); k++) {
      keep[k] = true;
    }
  }

  const hunks: DiffHunk[] = [];
  let i = 0;
  let skipped = 0;
  while (i < lines.length) {
    if (!keep[i]) {
      skipped++;
      i++;
      continue;
    }
    const start = i;
    while (i < lines.length && keep[i]) i++;
    const slice = lines.slice(start, i);
    hunks.push({
      a: slice.find((l) => l.a != null)?.a ?? 0,
      b: slice.find((l) => l.b != null)?.b ?? 0,
      lines: slice,
      skipped,
    });
    skipped = 0;
  }
  return hunks;
}

/// `+12 −3`, or null when nothing changed. The caller decides how to say
/// "identical" — a run list and a version table word it differently.
export function diffStat(d: DiffResult): string | null {
  if (!d.added && !d.removed) return null;
  const parts: string[] = [];
  if (d.added) parts.push(`+${d.added}`);
  if (d.removed) parts.push(`−${d.removed}`);
  return parts.join(" ");
}
