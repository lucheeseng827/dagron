/// Compact relative time ("just now", "5m ago", "3h ago", "2d ago").
/// Falls back to the raw string when unparseable.
export function timeAgo(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return iso;
  const s = Math.floor((Date.now() - date.getTime()) / 1000);
  // Future timestamps (clock skew, pre-scheduled runs) would otherwise render as
  // negative "−5m ago"; treat anything not in the past as "just now".
  if (s < 60) return "just now";
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

/// Future-relative time ("in 5h 12m", "in 12m", "due"). Falls back to raw input.
export function fromNow(iso: string): string {
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return iso;
  const s = Math.floor((t - Date.now()) / 1000);
  if (s <= 0) return "due";
  if (s < 60) return `in ${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `in ${m}m`;
  const h = Math.floor(m / 60);
  const rm = m % 60;
  if (h < 24) return rm ? `in ${h}h ${rm}m` : `in ${h}h`;
  return `in ${Math.floor(h / 24)}d`;
}

/// Absolute wall-clock form for tooltips/titles: the viewer's local time plus
/// the raw UTC instant, so relative times are always one hover from precision.
export function absTime(iso: string | null | undefined): string {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const local = d.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
  return `${local} (local) · ${d.toISOString().replace(".000Z", "Z")} UTC`;
}

/// Seconds → compact human duration ("45s", "3m 20s", "2h 5m"). Rounds the
/// total first so boundary values (119.5) normalize to "2m", never "1m 60s".
export function durationSecs(secs: number | null | undefined): string {
  if (secs == null || !Number.isFinite(secs) || secs < 0) return "—";
  const total = Math.round(secs);
  if (total < 60) return `${total}s`;
  const m = Math.floor(total / 60);
  const rs = total % 60;
  if (m < 60) return rs ? `${m}m ${rs}s` : `${m}m`;
  const h = Math.floor(m / 60);
  const rm = m % 60;
  return rm ? `${h}h ${rm}m` : `${h}h`;
}

/// Human duration between two ISO timestamps ("1m 23s", "2h 5m", "800ms").
/// When `end` is null the run is still going → returns "running". Falls back to
/// "—" on unparseable input.
export function duration(start: string, end: string | null): string {
  if (!end) return "running";
  const s = new Date(start).getTime();
  const e = new Date(end).getTime();
  if (Number.isNaN(s) || Number.isNaN(e) || e < s) return "—";
  const ms = e - s;
  if (ms < 1000) return `${ms}ms`;
  const sec = Math.floor(ms / 1000);
  if (sec < 60) return `${sec}s`;
  const m = Math.floor(sec / 60);
  const rs = sec % 60;
  if (m < 60) return rs ? `${m}m ${rs}s` : `${m}m`;
  const h = Math.floor(m / 60);
  const rm = m % 60;
  return rm ? `${h}h ${rm}m` : `${h}h`;
}
