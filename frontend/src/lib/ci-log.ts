/// A CI job's log, read as its step runner wrote it.
///
/// dagron-ci-exec writes lines of its own among a job's output, each
/// beginning `::dagron-ci::`: where a step starts and how it ended, what the
/// job's reports held, what its steps wrote to their summaries, and its
/// result for a machine. The console reads them here to show a job's log by
/// step, and its report beside it.
///
/// **These lines are the job's own word.** Its steps write to the same
/// stream, so a step can print any of them. Nothing here says how the job
/// ended: that is the task's status, which the engine keeps. What a job says
/// is held to bounds whatever wrote it, as dagron-ci holds it
/// (`crates/dagron-ci/src/said.rs`): so many annotations, so much text, and a
/// place only inside the repository.

export const CI_PREFIX = "::dagron-ci::";

const ANNOTATIONS_MAX = 50;
const FAILED_MAX = 50;
const SUMMARY_MAX = 32 * 1024;
const PATH_MAX = 240;
const TITLE_MAX = 160;
const MESSAGE_MAX = 400;

export interface CiStep {
  n: number;
  name: string;
  /// The step's own lines, with what the runner said to a reader inside it.
  lines: string[];
  /// Its exit code once it has ended. `null` while it runs, and where the
  /// log ends before the step does.
  code: number | null;
  /// It failed, and `continue_on_error` let the job go on.
  continued: boolean;
}

/// A stretch of the log, in its order: a step, or lines outside any step.
export type CiSegment = { kind: "step"; step: CiStep } | { kind: "lines"; lines: string[] };

export type CiLevel = "failure" | "warning" | "notice";

export interface CiAnnotation {
  path: string;
  start_line: number;
  end_line: number;
  level: CiLevel;
  title: string;
  message: string;
}

export interface CiCounts {
  tests: number;
  failures: number;
  errors: number;
  skipped: number;
  findings: number;
  more_annotations: number;
  more_failed: number;
  /// A report the job could not read, and why.
  problems: string[];
}

export interface CiLog {
  segments: CiSegment[];
  counts: CiCounts | null;
  annotations: CiAnnotation[];
  /// Each test that failed, by name, with what it said.
  failed: { name: string; message: string }[];
  /// What the job's steps wrote to their summaries: Markdown, as text.
  summary: string;
}

/// What the runner says of a job that ran no step.
export const SKIPPED_LINE =
  "skipped: no step ran. The job's condition did not hold, or a job it needs did not succeed.";

/// Whether `output` holds a step's marker, or the runner's word that the
/// job ran none: whether it reads better by step than as it was written.
export function hasCiSteps(output: string): boolean {
  return [`${CI_PREFIX}step::`, `${CI_PREFIX}job::skipped`].some(
    (marker) => output.startsWith(marker) || output.includes(`\n${marker}`),
  );
}

/// Whether `output` holds anything a job said of itself: a report or a
/// summary.
export function hasCiReport(log: CiLog): boolean {
  return log.counts != null || log.annotations.length > 0 || log.failed.length > 0 || log.summary.trim() !== "";
}

function clamp(text: unknown, max: number): string {
  const s = typeof text === "string" ? text.trim() : "";
  return [...s].length <= max ? s : `${[...s].slice(0, max - 1).join("")}…`;
}

function count(v: unknown): number {
  return typeof v === "number" && Number.isFinite(v) && v >= 0 ? Math.floor(v) : 0;
}

/// Whether `path` is a file's path inside a repository: relative, with no
/// way out.
function inRepository(path: unknown): path is string {
  return (
    typeof path === "string" &&
    path !== "" &&
    [...path].length <= PATH_MAX &&
    !path.startsWith("/") &&
    !/[\\\n\r\0]/.test(path) &&
    path.split("/").every((part) => part !== "" && part !== "." && part !== "..")
  );
}

function json(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return undefined;
  }
}

function annotation(v: unknown): CiAnnotation | null {
  if (v == null || typeof v !== "object" || Array.isArray(v)) return null;
  const a = v as Record<string, unknown>;
  if (!inRepository(a.path)) return null;
  const start = a.start_line;
  if (typeof start !== "number" || !Number.isInteger(start) || start < 1) return null;
  const end = typeof a.end_line === "number" && Number.isInteger(a.end_line) && a.end_line >= start ? a.end_line : start;
  const level: CiLevel = a.level === "failure" || a.level === "warning" ? a.level : "notice";
  return {
    path: a.path,
    start_line: start,
    end_line: end,
    level,
    title: clamp(a.title, TITLE_MAX),
    message: clamp(a.message, MESSAGE_MAX) || "(no message)",
  };
}

/// Read a job's log. A line that is not the runner's is the job's own, in
/// the step it was printed in. One of the runner's that is for a machine
/// (the result, a later job's reading) or that this does not know is left
/// out, as a newer runner's would be.
export function parseCiLog(output: string): CiLog {
  const log: CiLog = { segments: [], counts: null, annotations: [], failed: [], summary: "" };
  let open: CiStep | null = null;
  let moreAnnotations = 0;
  let moreFailed = 0;
  // Where a line of the job's own goes: the open step, or the stretch of
  // lines outside any step that is last.
  const say = (line: string) => {
    if (open) {
      open.lines.push(line);
      return;
    }
    const last = log.segments[log.segments.length - 1];
    if (last?.kind === "lines") last.lines.push(line);
    else log.segments.push({ kind: "lines", lines: [line] });
  };
  const lines = output.split("\n");
  // A log that ends in a newline has no line after it.
  if (lines[lines.length - 1] === "") lines.pop();
  for (const raw of lines) {
    const line = raw.endsWith("\r") ? raw.slice(0, -1) : raw;
    if (!line.startsWith(CI_PREFIX)) {
      say(line);
      continue;
    }
    const rest = line.slice(CI_PREFIX.length);
    const cut = rest.indexOf("::");
    if (cut < 0) continue;
    const kind = rest.slice(0, cut);
    const body = rest.slice(cut + 2);
    switch (kind) {
      case "step": {
        const at = body.indexOf("::");
        const n = Number(at < 0 ? body : body.slice(0, at));
        if (!Number.isInteger(n) || at < 0) break;
        const what = body.slice(at + 2);
        if (what.startsWith("start::")) {
          open = { n, name: what.slice("start::".length), lines: [], code: null, continued: false };
          log.segments.push({ kind: "step", step: open });
        } else if (what.startsWith("end::")) {
          const code = Number(what.slice("end::".length));
          const step: CiStep | null = open?.n === n ? open : null;
          if (step && Number.isInteger(code)) step.code = code;
          if (open?.n === n) open = null;
        } else if (what === "continued") {
          // Said after the step's end: of the step that last had this number.
          for (let i = log.segments.length - 1; i >= 0; i--) {
            const s = log.segments[i];
            if (s.kind === "step" && s.step.n === n) {
              s.step.continued = true;
              break;
            }
          }
        }
        break;
      }
      case "warning":
        say(`warning: ${body}`);
        break;
      case "error":
        say(`error: ${body}`);
        break;
      case "cancelled":
        say(`cancelled by signal ${body}`);
        break;
      case "job":
        // What a later job reads of this one is for a machine. That it ran
        // no step is for a reader, whose log would hold nothing else.
        if (body === "skipped") say(SKIPPED_LINE);
        break;
      case "report": {
        const v = json(body);
        if (v == null || typeof v !== "object" || Array.isArray(v)) break;
        const r = v as Record<string, unknown>;
        // The last one said stands: a job says one.
        log.counts = {
          tests: count(r.tests),
          failures: count(r.failures),
          errors: count(r.errors),
          skipped: count(r.skipped),
          findings: count(r.findings),
          more_annotations: count(r.more_annotations),
          more_failed: count(r.more_failed),
          problems: (Array.isArray(r.problems) ? r.problems : [])
            .filter((p): p is string => typeof p === "string")
            .slice(0, 8)
            .map((p) => clamp(p, MESSAGE_MAX)),
        };
        break;
      }
      case "annotation": {
        const a = annotation(json(body));
        if (!a) break;
        if (log.annotations.length < ANNOTATIONS_MAX) log.annotations.push(a);
        else moreAnnotations += 1;
        break;
      }
      case "failed": {
        const v = json(body);
        if (v == null || typeof v !== "object" || Array.isArray(v)) break;
        const f = v as Record<string, unknown>;
        if (typeof f.name !== "string" || f.name.trim() === "") break;
        if (log.failed.length < FAILED_MAX) {
          log.failed.push({ name: clamp(f.name, TITLE_MAX), message: clamp(f.message, MESSAGE_MAX) });
        } else moreFailed += 1;
        break;
      }
      case "summary": {
        const part = json(body);
        if (typeof part !== "string") break;
        const room = SUMMARY_MAX - log.summary.length;
        if (room > 0) log.summary += part.slice(0, room);
        break;
      }
      default:
        break;
    }
  }
  // What was said beyond what is kept is counted with what the job itself
  // left out.
  if (moreAnnotations > 0 || moreFailed > 0) {
    log.counts ??= {
      tests: 0,
      failures: 0,
      errors: 0,
      skipped: 0,
      findings: 0,
      more_annotations: 0,
      more_failed: 0,
      problems: [],
    };
    log.counts.more_annotations += moreAnnotations;
    log.counts.more_failed += moreFailed;
  }
  return log;
}

/// How a step reads: what it is doing, or how it ended.
export function stepReads(step: CiStep, taskRunning: boolean): { label: string; tone: "ok" | "failed" | "running" | "unknown" } {
  if (step.code == null) {
    return taskRunning ? { label: "running", tone: "running" } : { label: "did not end", tone: "unknown" };
  }
  if (step.code === 0) return { label: "ok", tone: "ok" };
  if (step.continued) return { label: `failed, exit ${step.code} (the job went on)`, tone: "failed" };
  if (step.code === 124) return { label: "timed out", tone: "failed" };
  return { label: `failed, exit ${step.code}`, tone: "failed" };
}

/// The counts as one line, as a check run's text has them.
export function countsLine(c: CiCounts): string {
  const parts: string[] = [];
  if (c.tests > 0 || c.failures > 0 || c.errors > 0) {
    parts.push(
      `${c.tests} test${c.tests === 1 ? "" : "s"}: ${c.failures} failed, ${c.errors} error${c.errors === 1 ? "" : "s"}, ${c.skipped} skipped`,
    );
  }
  if (c.findings > 0) parts.push(`${c.findings} finding${c.findings === 1 ? "" : "s"}`);
  return parts.join("; ");
}
