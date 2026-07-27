// The client half of the log filter grammar implemented in
// `crates/dagron-logging/src/logfilter.rs`.
//
// Nothing here filters anything: the server does that, because a run's output
// can be hundreds of megabytes and "download it all, then grep in the browser"
// is not a filter. This module only *describes* a filter — one state object, one
// query-string encoding — so the task panel, the workflow log view and the
// shareable URL all speak the same language. A filter typed in one place has to
// mean the same thing everywhere it's read back.

import type { LogLevel } from "@/types/dagron";

/// The server's line cap when `limit` is left at 0 — mirrors
/// `dagron_logging::logfilter::DEFAULT_LIMIT`. Kept here so a truncation notice
/// can name the number without a round-trip to ask what it was.
export const DEFAULT_LOG_LIMIT = 2000;

/// Levels offered as toggles, in severity order (highest first — that's the
/// order someone triaging a failure scans them in).
export const LOG_LEVELS: LogLevel[] = ["error", "warn", "info", "debug", "trace", "plain"];

/// A filter as the UI holds it. Every field is optional in effect: the default
/// state below selects nothing out.
export interface LogFilterState {
  /// Substring every line must contain.
  q: string;
  /// Substring no line may contain.
  exclude: string;
  /// Regular expression every line must match (server-compiled; an invalid one
  /// comes back as a 400 with the reason).
  regex: string;
  /// Match case-sensitively. Off by default — nobody remembers whether the
  /// stack trace said "Timeout" or "timeout".
  caseSensitive: boolean;
  /// Keep only these inferred levels. Empty = all levels.
  levels: LogLevel[];
  /// Also keep N unmatched lines either side of a match (grep -C). A matching
  /// line on its own often isn't enough to know what happened around it.
  context: number;
  /// Cap on returned lines. 0 = the server default.
  limit: number;
  /// When capping, keep the last lines rather than the first — the end of a
  /// failing run is usually the interesting end.
  tail: boolean;
}

export const EMPTY_FILTER: LogFilterState = {
  q: "",
  exclude: "",
  regex: "",
  caseSensitive: false,
  levels: [],
  context: 0,
  limit: 0,
  tail: false,
};

/// Is this filter a no-op — i.e. does it select every line? Used to keep the
/// "showing N of M" summary honest and to skip decorating an unfiltered view.
export function isEmptyFilter(f: LogFilterState): boolean {
  return !f.q && !f.exclude && !f.regex && f.levels.length === 0;
}

/// Encode a filter as URL query params, omitting everything at its default so
/// an unfiltered request stays byte-identical to what it was before filters
/// existed (the server skips its filter path entirely when no filter key is
/// present, and a stray `q=` would defeat that).
export function toParams(f: LogFilterState): URLSearchParams {
  const p = new URLSearchParams();
  if (f.q) p.set("q", f.q);
  if (f.exclude) p.set("exclude", f.exclude);
  if (f.regex) p.set("regex", f.regex);
  if (f.caseSensitive) p.set("case", "1");
  if (f.levels.length) p.set("level", f.levels.join(","));
  if (f.context > 0) p.set("context", String(f.context));
  if (f.limit > 0) p.set("limit", String(f.limit));
  if (f.tail) p.set("tail", "1");
  return p;
}

/// The query-string form (no leading `?`, empty when the filter is a no-op with
/// default caps) — what goes in a shared link, or anywhere a filter is stored
/// to be replayed later.
export function toQuery(f: LogFilterState): string {
  return toParams(f).toString();
}

/// Parse a filter back out of query params. Unknown keys are ignored, so this
/// can be handed a full page URL's params without stripping `task=`/`view=`
/// first. Unrecognized level names are dropped rather than throwing — a filter
/// from a stale bookmark should degrade, not blank the page.
export function fromParams(p: URLSearchParams): LogFilterState {
  const num = (key: string) => {
    const n = Number(p.get(key));
    return Number.isFinite(n) && n > 0 ? Math.floor(n) : 0;
  };
  const levels = (p.get("level") ?? "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter((s): s is LogLevel => (LOG_LEVELS as string[]).includes(s));
  return {
    q: p.get("q") ?? "",
    exclude: p.get("exclude") ?? "",
    regex: p.get("regex") ?? "",
    caseSensitive: truthy(p.get("case")),
    levels,
    context: num("context"),
    limit: num("limit"),
    tail: truthy(p.get("tail")),
  };
}

export function fromQuery(query: string): LogFilterState {
  return fromParams(new URLSearchParams(query));
}

function truthy(v: string | null): boolean {
  if (v == null) return false;
  return ["1", "true", "yes", "on", ""].includes(v.toLowerCase());
}

/// Toggle one level in a filter (immutably), keeping severity order so the
/// serialized form of a given selection is stable — otherwise the same filter
/// would produce different saved-view strings depending on click order.
export function toggleLevel(f: LogFilterState, level: LogLevel): LogFilterState {
  const has = f.levels.includes(level);
  const next = has ? f.levels.filter((l) => l !== level) : [...f.levels, level];
  return { ...f, levels: LOG_LEVELS.filter((l) => next.includes(l)) };
}

const STORAGE_KEY = "dagron.logFilter";

/// Remember the last filter across navigations. Triage is repetitive — the
/// person who filtered to errors on one run almost always wants errors on the
/// next one too, and retyping it every time is the whole complaint.
///
/// Storage is best-effort: private-mode browsers throw on access, and a broken
/// log view is a worse outcome than a forgotten filter.
export function loadFilter(): LogFilterState {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return EMPTY_FILTER;
    return fromQuery(raw);
  } catch {
    return EMPTY_FILTER;
  }
}

export function saveFilter(f: LogFilterState): void {
  try {
    window.localStorage.setItem(STORAGE_KEY, toQuery(f));
  } catch {
    /* storage unavailable — the filter simply doesn't persist */
  }
}

/// Colour for a level, from the shared status palette so a log view reads the
/// same as the rest of the console.
export function levelColor(level: LogLevel): string {
  switch (level) {
    case "error":
      return "var(--red)";
    case "warn":
      return "var(--amber)";
    case "info":
      return "var(--blue)";
    case "debug":
    case "trace":
      return "var(--dim)";
    default:
      return "var(--muted)";
  }
}

/// How long a task ran, phrased for a tooltip ("281ms", "1m 04s"). Returns
/// undefined when the task hasn't finished, because an open-ended window is
/// exactly the case where an estimated line time is least trustworthy and
/// pretending otherwise would be the whole problem.
export function taskDuration(startedAt?: string, finishedAt?: string): string | undefined {
  if (!startedAt || !finishedAt) return undefined;
  const a = new Date(startedAt).getTime();
  const b = new Date(finishedAt).getTime();
  if (Number.isNaN(a) || Number.isNaN(b) || b < a) return undefined;
  const ms = b - a;
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  const m = Math.floor(ms / 60_000);
  const s = Math.round((ms % 60_000) / 1000);
  return `${m}m ${String(s).padStart(2, "0")}s`;
}

/// The time to show against a log line.
///
/// A line that printed its own RFC3339 stamp gets that, exactly. Everything
/// else — which is most output — gets the start of the task that printed it,
/// flagged as an estimate. The estimate is only ever as good as the task was
/// short, so the task's duration rides along and the UI shows it: a 200ms task
/// makes this millisecond-accurate, a four-hour task makes it nearly useless,
/// and the reader is entitled to know which one they are looking at.
///
/// Deliberately NOT interpolated across the task's window by line position.
/// That would spread lines evenly over a duration they were never evenly spread
/// across, inventing precision that reads as recorded fact — the worst outcome
/// for a view whose job is correlating events in time.
export function lineTime(
  ts: string | undefined,
  task: { started?: string; within?: string } | undefined,
): { iso: string; exact: boolean; within?: string } | undefined {
  if (ts) return { iso: ts, exact: true };
  if (task?.started) return { iso: task.started, exact: false, within: task.within };
  return undefined;
}
