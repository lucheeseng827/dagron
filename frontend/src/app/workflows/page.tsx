"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import LiveToggle from "@/components/LiveToggle";
import { deleteWorkflow, listWorkflows, runWorkflow, updateSchedule } from "@/lib/dagron-api";
import { statusColor } from "@/lib/adapter";
import { errMsg } from "@/lib/err";
import { useLiveRefresh, useLiveUpdates } from "@/lib/live";
import { timeAgo, fromNow } from "@/lib/time";
import type { TaskStatus, WorkflowRow } from "@/types/dagron";

type Filter = "all" | "active" | "paused" | "gitops";
type ViewMode = "table" | "board";
const GRID = "2.4fr 1.5fr 1.2fr 1.3fr 0.7fr 92px";

export default function WorkflowsPage() {
  const router = useRouter();
  const [rows, setRows] = useState<WorkflowRow[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [q, setQ] = useState("");
  const [filter, setFilter] = useState<Filter>("all");
  const [tagFilter, setTagFilter] = useState<string | null>(null);
  const [view, setView] = useState<ViewMode>("table");

  const [live] = useLiveUpdates();

  const load = useCallback(() => {
    listWorkflows()
      .then((data) => {
        setRows(data);
        setError(null);
      })
      .catch((e) => setError(errMsg(e)));
  }, []);
  useEffect(() => load(), [load]);
  // Live mode: run activity changes last-run/history/success columns — refetch
  // on events from the account-wide stream instead of polling.
  const conn = useLiveRefresh(live, load);

  const counts = useMemo(
    () => ({
      defs: rows.length,
      schedules: rows.filter((r) => r.has_schedule && !r.paused).length,
      gitops: rows.filter((r) => r.source === "git").length,
    }),
    [rows],
  );

  const shown = useMemo(() => {
    const needle = q.trim().toLowerCase();
    return rows.filter((r) => {
      if (filter === "active" && !(r.has_schedule && !r.paused)) return false;
      if (filter === "paused" && !r.paused) return false;
      if (filter === "gitops" && r.source !== "git") return false;
      if (tagFilter && !(r.tags ?? []).includes(tagFilter)) return false;
      if (needle && !(`${r.name} ${r.description ?? ""} ${(r.tags ?? []).join(" ")}`.toLowerCase().includes(needle))) return false;
      return true;
    });
  }, [rows, q, filter, tagFilter]);

  const onRun = async (id: string) => {
    setBusy(id);
    try {
      const { run_id } = await runWorkflow(id);
      router.push(`/runs/detail/?id=${run_id}`);
    } catch (e) {
      setError(errMsg(e));
      setBusy(null);
    }
  };
  const onTogglePause = async (r: WorkflowRow) => {
    if (!r.schedule_id) return;
    setBusy(r.id);
    try {
      await updateSchedule(r.schedule_id, { enabled: r.paused });
      load();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(null);
    }
  };
  const onDelete = async (id: string) => {
    if (!confirm("Delete this workflow?")) return;
    setBusy(id);
    try {
      await deleteWorkflow(id);
      load();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="dy-page" style={{ maxWidth: 1320 }}>
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            Workflows
          </h1>
          <p className="dy-subtitle">
            {counts.defs} definitions · {counts.schedules} active schedules · {counts.gitops} from GitOps
          </p>
        </div>
        <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
          <LiveToggle status={conn} onRefresh={load} />
          <div style={{ display: "flex", gap: 3, background: "var(--panel)", border: "1px solid var(--border)", borderRadius: 8, padding: 3 }}>
            {(["table", "board"] as const).map((v) => (
              <button key={v} onClick={() => setView(v)} className={`dy-pill ${view === v ? "dy-pill-active" : ""}`} style={{ cursor: "pointer", textTransform: "capitalize" }}>
                {v}
              </button>
            ))}
          </div>
          <Link href="/workflows/new" className="dy-btn dy-btn-primary">
            + New workflow
          </Link>
        </div>
      </div>

      {/* search + filter tabs */}
      <div style={{ display: "flex", gap: 14, alignItems: "center", marginBottom: 16, flexWrap: "wrap" }}>
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Search workflows…"
          style={{ flex: 1, minWidth: 240, maxWidth: 420, background: "var(--panel)", color: "var(--fg)", border: "1px solid var(--border)", borderRadius: 8, padding: "8px 12px" }}
        />
        <div style={{ display: "flex", gap: 4 }}>
          {(["all", "active", "paused", "gitops"] as const).map((f) => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`dy-pill ${filter === f ? "dy-pill-active" : ""}`}
              style={{ cursor: "pointer", textTransform: f === "gitops" ? "none" : "capitalize" }}
            >
              {f === "gitops" ? "GitOps" : f}
            </button>
          ))}
        </div>
        {tagFilter && (
          <button
            type="button"
            onClick={() => setTagFilter(null)}
            title="Clear tag filter"
            className="dy-pill dy-pill-active"
            style={{ cursor: "pointer", display: "inline-flex", alignItems: "center", gap: 6 }}
          >
            #{tagFilter} <span aria-hidden style={{ opacity: 0.7 }}>✕</span>
          </button>
        )}
      </div>

      {error && <p style={{ color: "var(--red)" }}>{error}</p>}

      {view === "table" ? (
        <div className="dy-card" style={{ padding: 0, overflow: "hidden" }}>
          <div style={{ display: "grid", gridTemplateColumns: GRID, gap: 12, padding: "11px 18px", borderBottom: "1px solid var(--border)", fontSize: 11, fontWeight: 600, color: "var(--dim)", textTransform: "uppercase", letterSpacing: "0.05em" }}>
            <div>Workflow</div>
            <div>Schedule</div>
            <div>Last run</div>
            <div>14-run history</div>
            <div>Success</div>
            <div />
          </div>
          {shown.map((r) => (
            <div key={r.id} style={{ display: "grid", gridTemplateColumns: GRID, gap: 12, padding: "14px 18px", borderBottom: "1px solid var(--border)", alignItems: "center", opacity: r.paused ? 0.6 : 1 }}>
              {/* workflow */}
              <div style={{ minWidth: 0 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <Link href={`/workflows/history/?id=${r.id}`} style={{ fontWeight: 600, color: "var(--fg)" }} title="Run history & task health">
                    {r.name}
                  </Link>
                  <SourceBadge source={r.source} />
                  {r.paused && <Tag text="PAUSED" color="var(--muted)" bg="rgba(139,148,158,0.13)" />}
                </div>
                {r.description && (
                  <div style={{ fontSize: 12.5, color: "var(--muted)", marginTop: 3, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                    {r.description}
                  </div>
                )}
                {(r.tags ?? []).length > 0 && (
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 4, marginTop: 5 }}>
                    {(r.tags ?? []).map((t) => (
                      <TagChip key={t} tag={t} active={t === tagFilter} onClick={() => setTagFilter(t === tagFilter ? null : t)} />
                    ))}
                  </div>
                )}
              </div>
              {/* schedule */}
              <div style={{ fontSize: 13 }}>
                <span className="mono">{r.cron_expr ? `⏱ ${r.cron_expr}` : "— manual"}</span>
                <div style={{ fontSize: 12, color: "var(--dim)", marginTop: 2 }}>
                  {r.paused ? "next paused" : r.next_fire_at ? `next ${fromNow(r.next_fire_at)}` : "—"}
                </div>
              </div>
              {/* last run */}
              <LastRun status={r.last_status} at={r.last_at} />
              {/* sparkline */}
              <Sparkline history={r.history} />
              {/* success */}
              <div style={{ fontWeight: 600, fontSize: 13.5, color: rateColor(r.success_rate) }}>
                {r.success_rate == null ? "—" : `${r.success_rate}%`}
              </div>
              {/* actions */}
              <div style={{ display: "flex", gap: 6, justifyContent: "flex-end" }}>
                {r.has_schedule ? (
                  <IconBtn
                    title={r.paused ? "Resume" : "Pause"}
                    ariaLabel={r.paused ? `Resume ${r.name}` : `Pause ${r.name}`}
                    disabled={busy === r.id}
                    onClick={() => onTogglePause(r)}
                  >
                    {r.paused ? "▶" : "⏸"}
                  </IconBtn>
                ) : (
                  <IconBtn title="Run now" ariaLabel={`Run ${r.name} now`} disabled={busy === r.id} onClick={() => onRun(r.id)}>
                    ▶
                  </IconBtn>
                )}
                <Link href={`/workflows/detail/?id=${r.id}`} className="dy-iconbtn" title="Edit" aria-label={`Edit ${r.name}`}>
                  ✎
                </Link>
                <IconBtn title="Delete" ariaLabel={`Delete ${r.name}`} disabled={busy === r.id} onClick={() => onDelete(r.id)}>
                  ✕
                </IconBtn>
              </div>
            </div>
          ))}
          {shown.length === 0 && <p className="dy-empty" style={{ padding: 16 }}>No workflows match.</p>}
        </div>
      ) : (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(320px,1fr))", gap: 14 }}>
          {shown.map((r) => (
            // The card is a plain container, not a Link: the tag chips are
            // real buttons, and nesting a button inside an anchor is invalid
            // HTML — it also swallowed the click, so filtering by tag navigated
            // to history instead. The link now covers the card body only, with
            // the chips as siblings that filter exactly as they do in the table.
            <div key={r.id} className="dy-card" style={{ position: "relative", opacity: r.paused ? 0.65 : 1 }}>
              <div style={{ position: "absolute", top: 0, left: 0, width: 3, height: "100%", background: statusColor((r.last_status ?? "pending") as TaskStatus) }} />
              <Link href={`/workflows/history/?id=${r.id}`} style={{ display: "block", color: "var(--fg)" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
                  <strong>{r.name}</strong>
                  <SourceBadge source={r.source} />
                  {r.paused && <Tag text="PAUSED" color="var(--muted)" bg="rgba(139,148,158,0.13)" />}
                </div>
                {r.description && <div style={{ fontSize: 12.5, color: "var(--muted)", marginBottom: 10 }}>{r.description}</div>}
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
                  <LastRun status={r.last_status} at={r.last_at} compact />
                  <span style={{ marginLeft: "auto", fontWeight: 600, color: rateColor(r.success_rate) }}>
                    {r.success_rate == null ? "—" : `${r.success_rate}%`}
                  </span>
                </div>
                <Sparkline history={r.history} />
                <div className="mono" style={{ fontSize: 12, color: "var(--dim)", marginTop: 10 }}>
                  {r.cron_expr ? `⏱ ${r.cron_expr}` : "manual"}
                </div>
              </Link>
              {(r.tags ?? []).length > 0 && (
                <div style={{ display: "flex", flexWrap: "wrap", gap: 4, marginTop: 10 }}>
                  {(r.tags ?? []).map((t) => (
                    <TagChip
                      key={t}
                      tag={t}
                      active={t === tagFilter}
                      onClick={() => setTagFilter(t === tagFilter ? null : t)}
                    />
                  ))}
                </div>
              )}
            </div>
          ))}
          {shown.length === 0 && <p className="dy-empty">No workflows match.</p>}
        </div>
      )}
    </div>
  );
}

function SourceBadge({ source }: { source: "git" | "manual" }) {
  return source === "git" ? (
    <Tag text="GitOps" color="var(--accent)" bg="rgba(232,131,58,0.13)" />
  ) : (
    <Tag text="Manual" color="var(--blue)" bg="rgba(47,129,247,0.13)" />
  );
}
function Tag({ text, color, bg }: { text: string; color: string; bg: string }) {
  return (
    <span style={{ fontSize: 9.5, fontWeight: 600, padding: "2px 7px", borderRadius: 999, color, background: bg, flexShrink: 0 }}>
      {text}
    </span>
  );
}

// Deterministic color per tag (Airflow #16432 colored tags): same tag → same
// chip color across the board, so a tag is visually recognizable.
const TAG_PALETTE: { color: string; bg: string }[] = [
  { color: "var(--blue)", bg: "rgba(47,129,247,0.13)" },
  { color: "var(--accent)", bg: "rgba(232,131,58,0.13)" },
  { color: "#3fb950", bg: "rgba(63,185,80,0.14)" },
  { color: "#a371f7", bg: "rgba(163,113,247,0.14)" },
  { color: "#e3b341", bg: "rgba(227,179,65,0.14)" },
  { color: "#f778ba", bg: "rgba(247,120,186,0.14)" },
];
function tagColor(tag: string): { color: string; bg: string } {
  let h = 0;
  for (let i = 0; i < tag.length; i++) h = (h * 31 + tag.charCodeAt(i)) >>> 0;
  return TAG_PALETTE[h % TAG_PALETTE.length];
}

// A colored tag chip; clickable to filter the list by that tag (#26).
function TagChip({ tag, active, onClick }: { tag: string; active?: boolean; onClick?: () => void }) {
  const c = tagColor(tag);
  return (
    <button
      type="button"
      onClick={onClick}
      title={onClick ? `Filter by tag "${tag}"` : tag}
      style={{
        fontSize: 9.5,
        fontWeight: 600,
        padding: "2px 7px",
        borderRadius: 999,
        color: c.color,
        background: c.bg,
        border: `1px solid ${active ? c.color : "transparent"}`,
        cursor: onClick ? "pointer" : "default",
        flexShrink: 0,
        lineHeight: 1.4,
      }}
    >
      #{tag}
    </button>
  );
}

function LastRun({ status, at, compact }: { status: string | null; at: string | null; compact?: boolean }) {
  if (!status) return <span style={{ color: "var(--dim)", fontSize: 13 }}>never run</span>;
  const color = statusColor(status as TaskStatus);
  const label = status.charAt(0).toUpperCase() + status.slice(1);
  return (
    <div style={{ fontSize: 13 }}>
      <span style={{ display: "inline-flex", alignItems: "center", gap: 7, color }}>
        <span className="dy-dot" style={{ background: color }} />
        {label}
      </span>
      {!compact && (
        <div style={{ fontSize: 12, color: "var(--dim)", marginTop: 2 }}>
          {status === "running" ? "running now" : at ? timeAgo(at) : ""}
        </div>
      )}
    </div>
  );
}

function Sparkline({ history }: { history: TaskStatus[] }) {
  if (history.length === 0) return <span style={{ color: "var(--dim)", fontSize: 12 }}>no runs</span>;
  return (
    <div style={{ display: "flex", gap: 2, alignItems: "flex-end", height: 30 }}>
      {history.map((s, i) => (
        <div key={i} title={s} style={{ flex: 1, minWidth: 4, height: "100%", borderRadius: 1.5, background: statusColor(s) }} />
      ))}
    </div>
  );
}

function rateColor(rate: number | null): string {
  if (rate == null) return "var(--muted)";
  if (rate >= 90) return "var(--green)";
  if (rate >= 75) return "var(--amber)";
  return "var(--red)";
}

function IconBtn({
  children,
  title,
  ariaLabel,
  onClick,
  disabled,
}: {
  children: React.ReactNode;
  title: string;
  ariaLabel?: string;
  onClick: () => void;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      className="dy-iconbtn"
      title={title}
      aria-label={ariaLabel ?? title}
      onClick={onClick}
      disabled={disabled}
    >
      {children}
    </button>
  );
}
