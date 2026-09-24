"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import SpecDiff from "@/components/SpecDiff";
import { getRunSpecs } from "@/lib/dagron-api";
import { diffLines, diffStat } from "@/lib/diff";
import { errMsg } from "@/lib/err";
import { eraIndexByRun, groupEras, type SpecEra } from "@/lib/spec-eras";
import { absTime, timeAgo } from "@/lib/time";
import type { RunSummary } from "@/types/dagron";

/// "What was actually running?" for a page of run history.
///
/// The run table answers when each run happened and how it went, and says
/// nothing about the definition behind it — so a Tuesday of failures that
/// started with a Monday edit reads as a mystery. This card puts the edit in the
/// history: the runs are grouped into the specs they ran, oldest first, and each
/// later spec is shown as a diff from a base rather than as another YAML dump.
///
/// **Collapsed by default.** The page's specs come from a single
/// `GET /runs/specs?ids=…`, which is cheap — but it is still a request and a
/// spec-sized response for a card most visits don't open, so nothing is fetched
/// until it is.
export default function RunSpecHistory({
  runs,
  onGrouping,
}: {
  runs: RunSummary[];
  /// Reports the run → era index map back to the run table, so each row can say
  /// which definition it ran. Lives here rather than in the page because the
  /// specs are fetched here; the page renders the badge and owns nothing else.
  /// Called with an empty map while the card is closed.
  onGrouping?: (byRun: Map<string, number>) => void;
}) {
  const [open, setOpen] = useState(false);
  const [specs, setSpecs] = useState<Map<string, string>>(new Map());
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  /// Which era's diff is expanded, by index into `eras`. Null = none.
  const [openEra, setOpenEra] = useState<number | null>(null);
  /// The era to measure from. Null means "the oldest on this page" — see the
  /// note on the selector: that is a baseline, not necessarily the workflow's
  /// first definition ever.
  const [baseEra, setBaseEra] = useState<number | null>(null);
  /// Bumped by Retry. The fetch lives in the effect so the effect's cleanup
  /// owns every request: calling `load` straight from the button would discard
  /// the cleanup it returns, and that orphaned request can still resolve after
  /// the page has turned — writing the previous page's specs over the new
  /// page's, which `groupEras` then reads as "no run resolved".
  const [reload, setReload] = useState(0);

  // Re-fetch when the page of runs changes; a spec map keyed by run id would
  // otherwise serve the previous page's entries for ids that happen to remain.
  const runIds = useMemo(() => runs.map((r) => r.id).join(","), [runs]);

  const load = useCallback(() => {
    if (!runs.length) return;
    setLoading(true);
    setError(null);
    let alive = true;
    // One request for the page. The server returns distinct specs with the runs
    // that used each, so this is also far less to send than a spec per run: a
    // workflow nobody edited comes back as a single document rather than
    // twenty-five copies of it.
    getRunSpecs(runs.map((r) => r.id))
      .then((groups) => {
        if (!alive) return;
        const byRun = new Map<string, string>();
        for (const g of groups) for (const id of g.run_ids) byRun.set(id, g.yaml);
        setSpecs(byRun);
        // Nothing resolving at all is a broken endpoint, not a history with
        // nothing in it — say so rather than rendering an empty card.
        if (byRun.size === 0) setError("Could not read the spec for any run on this page.");
      })
      .catch((e) => alive && setError(errMsg(e)))
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runIds]);

  useEffect(() => {
    // Reset when the page turns, and re-fetch only if the card is open.
    setSpecs(new Map());
    setOpenEra(null);
    setBaseEra(null);
    if (open) return load();
  }, [open, load, reload]);

  const { eras, unresolved } = useMemo(() => groupEras(runs, specs), [runs, specs]);
  const base = baseEra == null ? 0 : Math.min(baseEra, Math.max(0, eras.length - 1));

  // Hand the grouping up so the run table can badge each row. Keyed on the
  // derived map rather than on `eras`, and only while open: a closed card has
  // fetched nothing and must not leave stale badges on the table.
  const byRun = useMemo(() => (open ? eraIndexByRun(eras) : new Map<string, number>()), [open, eras]);
  useEffect(() => {
    onGrouping?.(byRun);
    // `onGrouping` is the parent's setState; including it would re-run this on
    // every parent render, and the map is what actually changed.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [byRun]);

  /// What each era changed against the one before it — the per-save stat, same
  /// as the version table's "Changed" column.
  const stepStats = useMemo(
    () =>
      eras.map((e, i) => (i === 0 ? null : diffStat(diffLines(eras[i - 1].spec, e.spec)))),
    [eras],
  );

  if (!runs.length) return null;

  return (
    <div className="dy-card" style={{ marginBottom: 18 }}>
      <div className="dy-cardhead">
        <strong>Definition changes</strong>
        <button
          className="dy-btn"
          style={{ fontSize: 12, padding: "4px 9px" }}
          onClick={() => setOpen((v) => !v)}
          aria-expanded={open}
          title="Group these runs by the spec they ran, and diff each change."
        >
          {open ? "Hide" : "Show what changed"}
        </button>
      </div>

      {!open ? (
        <p style={{ color: "var(--dim)", fontSize: 12.5, margin: 0 }}>
          Which definition each run actually used, and the diff between them.
          One request for the page, so it loads when you ask.
        </p>
      ) : loading ? (
        <p className="dy-empty" style={{ margin: 0 }}>
          Loading specs…
        </p>
      ) : error ? (
        <p className="dy-empty" style={{ margin: 0, color: "var(--red)" }} role="alert">
          {error}{" "}
          <button
            className="dy-btn"
            style={{ fontSize: 12, padding: "2px 8px" }}
            onClick={() => setReload((n) => n + 1)}
          >
            Retry
          </button>
        </p>
      ) : eras.length === 0 ? (
        <p className="dy-empty" style={{ margin: 0 }}>
          No specs could be read for these runs.
        </p>
      ) : (
        <>
          <p style={{ color: "var(--muted)", fontSize: 12.5, margin: "0 0 10px" }}>
            {eras.length === 1 ? (
              <>
                All {plural(runs.length - unresolved, "run")} on this page ran the{" "}
                <strong>same definition</strong> — nothing changed here.
              </>
            ) : (
              <>
                {plural(eras.length, "definition")} across{" "}
                {plural(runs.length - unresolved, "run")}, oldest first. Each row is measured
                against the base.
              </>
            )}
            {unresolved > 0 && (
              <span style={{ color: "var(--amber)" }}>
                {" "}
                {plural(unresolved, "run")} left out — their spec could not be read.
              </span>
            )}
          </p>

          {eras.length > 1 && (
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10, flexWrap: "wrap" }}>
              <label htmlFor="era-base" style={{ color: "var(--muted)", fontSize: 12 }}>
                Compare from
              </label>
              <select
                id="era-base"
                value={base}
                onChange={(e) => setBaseEra(Number(e.target.value))}
                style={selectStyle}
                // Deliberately not called "origin": this is the oldest
                // definition among the runs *on this page*, and paging back
                // may well find an older one. Naming it origin would be a
                // claim the data doesn't support.
                title="The definition this page's diffs are measured against — the oldest on this page unless you change it."
              >
                {eras.map((e, i) => (
                  <option key={i} value={i}>
                    #{i + 1} · from {e.firstRun.id.slice(0, 8)} ({timeAgo(e.firstRun.created_at)})
                    {i === 0 ? " · oldest here" : ""}
                  </option>
                ))}
              </select>
            </div>
          )}

          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            {eras.map((era, i) => (
              <EraRow
                key={`${i}:${era.firstRun.id}`}
                era={era}
                index={i}
                isBase={i === base}
                stepStat={stepStats[i]}
                expanded={openEra === i}
                onToggle={() => setOpenEra(openEra === i ? null : i)}
                baseEra={eras[base]}
                baseIndex={base}
              />
            ))}
          </div>
        </>
      )}
    </div>
  );
}

function EraRow({
  era,
  index,
  isBase,
  stepStat,
  expanded,
  onToggle,
  baseEra,
  baseIndex,
}: {
  era: SpecEra;
  index: number;
  isBase: boolean;
  stepStat: string | null;
  expanded: boolean;
  onToggle: () => void;
  baseEra: SpecEra;
  baseIndex: number;
}) {
  return (
    <div style={{ border: "1px solid var(--border)", borderRadius: 6, overflow: "hidden" }}>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 10,
          flexWrap: "wrap",
          padding: "7px 10px",
          background: isBase ? "rgba(88,166,255,0.07)" : "transparent",
        }}
      >
        <span className="mono" style={{ fontSize: 12, color: "var(--muted)" }}>
          #{index + 1}
        </span>
        <span style={{ fontSize: 12.5 }}>
          {plural(era.runs.length, "run")}{" "}
          <span style={{ color: "var(--dim)" }} title={absTime(era.firstRun.created_at)}>
            from {timeAgo(era.firstRun.created_at)}
          </span>
        </span>
        <Link
          href={`/runs/detail/?id=${era.firstRun.id}`}
          className="mono"
          style={{ fontSize: 11.5, color: "var(--blue)" }}
          title="The first run that used this definition"
        >
          {era.firstRun.id.slice(0, 8)}
        </Link>
        {/* What this change did relative to the one before it — the same
            "per-save" reading the version table gives, independent of which
            base is selected. */}
        {stepStat && (
          <span className="mono" style={{ fontSize: 11.5 }} title="Change from the definition before it">
            {stepStat.split(" ").map((p, k) => (
              <span key={k} style={{ color: p.startsWith("+") ? "var(--green)" : "var(--red)", marginRight: 4 }}>
                {p}
              </span>
            ))}
          </span>
        )}
        {isBase && (
          <span className="dy-pill" style={{ fontSize: 10.5 }}>
            base
          </span>
        )}
        <span style={{ flex: 1 }} />
        {!isBase && (
          <button
            className="dy-btn"
            style={{ fontSize: 11.5, padding: "3px 8px" }}
            onClick={onToggle}
            aria-expanded={expanded}
          >
            {expanded ? "Hide diff" : "Diff"}
          </button>
        )}
      </div>
      {expanded && !isBase && (
        <div style={{ padding: "0 10px 10px" }}>
          <SpecDiff
            base={baseEra.spec}
            head={era.spec}
            baseLabel={`#${baseIndex + 1}`}
            headLabel={`#${index + 1}`}
            identical={`Definition #${index + 1} is byte-identical to the base — it is a separate era because something else ran in between.`}
            maxHeight={360}
          />
        </div>
      )}
    </div>
  );
}

const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;

const selectStyle: React.CSSProperties = {
  padding: "3px 6px",
  background: "var(--bg)",
  color: "var(--fg)",
  border: "1px solid var(--border)",
  borderRadius: 5,
  fontSize: 12,
};
