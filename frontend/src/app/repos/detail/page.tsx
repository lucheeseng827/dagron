"use client";

import { Suspense, useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { listRepoRefs } from "@/lib/dagron-api";
import { runColor, runReads } from "@/lib/adapter";
import { useLiveUpdates } from "@/lib/live";
import { absTime, timeAgo } from "@/lib/time";
import type { RefRuns } from "@/types/dagron";

const TERMINAL = new Set(["succeeded", "failed", "cancelled"]);
const GRID = "minmax(160px, 1.1fr) 110px minmax(90px, 0.6fr) 110px minmax(220px, 2fr)";
/// How often a repository with a run in flight is read again.
const POLL_MS = 5000;

export default function RepoPage() {
  // useSearchParams() suspends during prerender; the static export build
  // fails without this boundary.
  return (
    <Suspense fallback={null}>
      <RepoPageInner />
    </Suspense>
  );
}

type Kind = "pulls" | "branches" | "tags" | "other";

const TITLES: Record<Kind, string> = {
  pulls: "Pull requests",
  branches: "Branches",
  tags: "Tags",
  other: "Other refs",
};

function kindOf(r: RefRuns): Kind {
  if (r.pr != null) return "pulls";
  if (r.ref.startsWith("refs/heads/")) return "branches";
  if (r.ref.startsWith("refs/tags/")) return "tags";
  return "other";
}

/// A ref as a person names it: a branch or a tag by its name, a pull request
/// by its number.
function shortRef(r: RefRuns): string {
  if (r.pr != null) return `#${r.pr}`;
  return r.ref.replace(/^refs\/(heads|tags)\//, "");
}

/// A repository's branches and pull requests, each with the runs for its
/// latest commit: the latest run of each pipeline for that commit, whether
/// a push started it or someone asked for it by hand.
///
/// It is read from the runs' own records, so a branch is here once CI has
/// run for it, and reads as its last run did. What is named (a branch, an
/// author, a commit) is the submitting service's word about another system:
/// text, and no link to it.
function RepoPageInner() {
  const repo = useSearchParams().get("repo") ?? "";
  const [refs, setRefs] = useState<RefRuns[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [live] = useLiveUpdates();
  // The repository on screen. An answer for another one is dropped: going
  // from one repository to the next, the first's answer may come last, and
  // would put its refs under the second's name.
  const shown = useRef(repo);
  shown.current = repo;

  const load = useCallback(() => {
    if (!repo) return;
    const asked = repo;
    listRepoRefs(asked, 200)
      .then((rows) => {
        if (shown.current !== asked) return;
        setRefs(rows);
        setError(null);
      })
      .catch((e) => {
        if (shown.current === asked) setError(String(e));
      });
  }, [repo]);

  useEffect(() => {
    setRefs(null);
    load();
  }, [load]);

  // Read again while a run is in flight, and not otherwise: a repository at
  // rest changes only when something is pushed, and Refresh is one click.
  const active = useMemo(
    () => (refs ?? []).some((r) => r.runs.some((run) => !TERMINAL.has(run.status))),
    [refs],
  );
  useEffect(() => {
    if (!live || !active) return;
    const timer = setInterval(load, POLL_MS);
    return () => clearInterval(timer);
  }, [live, active, load]);

  const groups = useMemo(() => {
    const by: Record<Kind, RefRuns[]> = { pulls: [], branches: [], tags: [], other: [] };
    for (const r of refs ?? []) by[kindOf(r)].push(r);
    return by;
  }, [refs]);

  if (!repo) {
    return (
      <div className="dy-page dy-page-wide">
        <p className="dy-empty">
          No repository named. <Link href="/repos">Repositories</Link>
        </p>
      </div>
    );
  }

  return (
    <div className="dy-page dy-page-wide">
      <div className="dy-pagehead">
        <div style={{ minWidth: 0 }}>
          <h1 className="dy-h1 mono" style={{ marginBottom: 0, overflow: "hidden", textOverflow: "ellipsis" }}>
            {repo}
          </h1>
          <p className="dy-subtitle">
            <Link href="/repos">Repositories</Link> · each branch and pull request, with the runs for its latest commit.
          </p>
        </div>
        <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
          <Link href={`/runs/?trigger=ci&repo=${encodeURIComponent(repo)}`} className="dy-btn" title="Every run for this repository, newest first">
            All runs
          </Link>
          <button onClick={load} className="dy-btn">
            Refresh
          </button>
        </div>
      </div>

      {error && <p style={{ color: "var(--red)" }}>{error}</p>}

      {refs != null && refs.length === 0 && !error && (
        <p className="dy-empty">No run names this repository. A branch is listed once CI has run for it.</p>
      )}

      {(Object.keys(TITLES) as Kind[]).map((kind) =>
        groups[kind].length === 0 ? null : (
          <section key={kind} style={{ marginBottom: 22 }}>
            <h2 style={{ fontSize: 13, fontWeight: 600, color: "var(--muted)", margin: "0 0 8px" }}>
              {TITLES[kind]} <span style={{ color: "var(--dim)", fontWeight: 400 }}>{groups[kind].length}</span>
            </h2>
            <div className="dy-card" style={{ padding: 0, overflow: "hidden" }}>
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: GRID,
                  gap: 12,
                  padding: "11px 18px",
                  borderBottom: "1px solid var(--border)",
                  fontSize: 11,
                  fontWeight: 600,
                  color: "var(--dim)",
                  textTransform: "uppercase",
                  letterSpacing: "0.05em",
                }}
              >
                <div>{kind === "pulls" ? "Pull request" : "Ref"}</div>
                <div>Commit</div>
                <div>By</div>
                <div>Latest</div>
                <div>Runs</div>
              </div>
              {groups[kind].map((r) => (
                <RefRow key={r.ref} r={r} />
              ))}
            </div>
          </section>
        ),
      )}
    </div>
  );
}

function RefRow({ r }: { r: RefRuns }) {
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: GRID,
        gap: 12,
        alignItems: "center",
        padding: "10px 18px",
        borderBottom: "1px solid var(--border)",
        fontSize: 13.5,
      }}
    >
      <span className="mono" style={{ fontWeight: 600, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={r.ref}>
        {shortRef(r)}
      </span>
      <span className="mono" style={{ color: "var(--muted)" }} title={r.sha ?? undefined}>
        {r.sha ? r.sha.slice(0, 10) : "—"}
      </span>
      <span style={{ color: "var(--muted)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
        {r.actor ?? "—"}
      </span>
      <span style={{ color: "var(--muted)" }} title={absTime(r.at)}>
        {timeAgo(r.at)}
      </span>
      <span style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
        {r.runs.map((run) => (
          <Link
            key={run.id}
            href={`/runs/detail/?id=${run.id}`}
            className="dy-pill"
            title={`${run.name ?? "run"}: ${runReads(run)}`}
            style={{ display: "inline-flex", alignItems: "center", gap: 6 }}
          >
            <span className="dy-dot dy-dot-sm" style={{ background: runColor(run) }} />
            {run.name ?? run.id.slice(0, 8)}
          </Link>
        ))}
      </span>
    </div>
  );
}
