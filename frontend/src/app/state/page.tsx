"use client";

// State plans (dagron-state): paste a backfill planner's plan and see what a SQL
// change will actually rebuild — which models, why, and via which columns —
// before anything runs. The Explain call is read-only; submitting is a separate,
// deliberate second step.
//
// The planner's own `--json` output is accepted unwrapped: this page wraps it in
// the `{plan, graph?, options?}` envelope the API expects, so the paste path is
// "copy the CLI output" with nothing to hand-edit.

import { useCallback, useEffect, useMemo, useState } from "react";
import { useToast } from "@/components/Toasts";
import { explainStatePlan, submitStatePlan } from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";
import type { StateExplanation } from "@/types/dagron";

const REASON_COLOR: Record<string, string> = {
  directly_changed: "var(--amber, #b45309)",
  downstream: "var(--blue)",
};

const PLACEHOLDER = `{
  "models": [
    { "name": "stg_orders", "reason": "directly_changed", "unit": "full_model" },
    {
      "name": "mart_revenue",
      "reason": { "downstream": { "because_of": "stg_orders", "via_columns": ["amount"] } },
      "unit": "full_model"
    }
  ]
}`;

const DEFAULT_COMMAND = "dbt run --select {{ model }}";

/// "Nothing to rebuild" is the planner's *good* outcome, and the API says so with
/// a 422 rather than a 400. It must not reach the user as a red error.
function isEmptyPlan(e: unknown): boolean {
  return errMsg(e).startsWith("422");
}

export default function StatePlansPage() {
  const toast = useToast();
  const [raw, setRaw] = useState("");
  const [command, setCommand] = useState(DEFAULT_COMMAND);
  const [sequential, setSequential] = useState(false);
  const [explanation, setExplanation] = useState<StateExplanation | null>(null);
  const [empty, setEmpty] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  // Wrap the planner's bare output, or pass an envelope through untouched. Kept
  // in a memo so a parse error surfaces while typing rather than on submit.
  const envelope = useMemo(() => {
    if (!raw.trim()) return null;
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      return null;
    }
    // JSON.parse happily returns null, 5 or "text" — none of which can be probed
    // for a `plan` key. Reading `.plan` off null throws inside this memo, which
    // crashes the render rather than showing the malformed-input message.
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return null;
    const obj = parsed as Record<string, unknown>;
    const base = obj.plan !== undefined ? obj : { plan: obj };
    const priorOptions = (base.options ?? {}) as Record<string, unknown>;
    return {
      ...base,
      options: {
        ...priorOptions,
        command_template: ["sh", "-c", command],
        ordering: sequential ? "sequential" : "derived",
      },
    };
  }, [raw, command, sequential]);

  const malformed = raw.trim().length > 0 && envelope === null;

  // Any edit invalidates the explanation. Without this, a user can explain plan
  // A, edit the box to plan B, and hit Submit while the table still shows A —
  // running a plan nobody reviewed, which is exactly what the two-step flow at
  // the top of this file exists to prevent. Submit is gated on `explanation`, so
  // clearing it forces a fresh Explain on the edited input.
  useEffect(() => {
    setExplanation(null);
    setEmpty(false);
  }, [raw, command, sequential]);

  const onExplain = useCallback(() => {
    if (!envelope) return;
    setBusy(true);
    setError(null);
    setEmpty(false);
    explainStatePlan(envelope)
      .then((ex) => {
        setExplanation(ex);
        setEmpty(false);
      })
      .catch((e) => {
        setExplanation(null);
        if (isEmptyPlan(e)) setEmpty(true);
        else setError(errMsg(e));
      })
      .finally(() => setBusy(false));
  }, [envelope]);

  const onSubmit = useCallback(() => {
    if (!envelope) return;
    setBusy(true);
    submitStatePlan(envelope)
      .then((r) => toast(`Run ${r.run_id} submitted — ${r.model_count} models`))
      .catch((e) => toast(errMsg(e), "error"))
      .finally(() => setBusy(false));
  }, [envelope, toast]);

  const copy = (text: string, what: string) => {
    navigator.clipboard
      .writeText(text)
      .then(() => toast(`${what} copied`))
      .catch(() => toast("Copy failed", "error"));
  };

  return (
    <div className="dy-page">
      <div className="dy-pagehead" style={{ marginBottom: 16 }}>
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            State plans
          </h1>
          <p className="dy-subtitle">
            What a SQL change actually rebuilds — which models, why, and via which columns.
            Explaining changes nothing; submitting is a separate step.
          </p>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <button
            onClick={onExplain}
            disabled={busy || !envelope}
            className="dy-btn"
            style={{ cursor: busy ? "wait" : "pointer" }}
          >
            Explain
          </button>
          <button
            onClick={onSubmit}
            disabled={busy || !explanation}
            className="dy-btn dy-btn-primary"
            style={{ cursor: busy ? "wait" : "pointer" }}
            title={explanation ? "Compile and run this plan" : "Explain the plan first"}
          >
            Submit as run
          </button>
        </div>
      </div>

      <div className="dy-card" style={{ marginBottom: 16 }}>
        <label className="dy-subtitle" htmlFor="plan-json">
          Planner output — paste <code className="mono">freshet plan --json</code>, or a full
          <code className="mono"> {"{plan, graph, options}"} </code> envelope
        </label>
        <textarea
          id="plan-json"
          value={raw}
          onChange={(e) => setRaw(e.target.value)}
          placeholder={PLACEHOLDER}
          spellCheck={false}
          className="mono"
          rows={12}
          style={{ width: "100%", marginTop: 8, resize: "vertical" }}
        />
        {malformed && (
          <p style={{ color: "var(--red)", marginTop: 8 }}>That is not valid JSON.</p>
        )}

        <div style={{ display: "flex", gap: 16, marginTop: 12, flexWrap: "wrap", alignItems: "center" }}>
          <label style={{ flex: "1 1 320px" }}>
            <span className="dy-subtitle">Per-model command</span>
            <input
              value={command}
              onChange={(e) => setCommand(e.target.value)}
              spellCheck={false}
              className="mono"
              style={{ width: "100%", marginTop: 4 }}
              title="Run as `sh -c <command>`. {{ model }}, {{ unit }} and {{ partitions }} substitute."
            />
          </label>
          <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
            <input
              type="checkbox"
              checked={sequential}
              onChange={(e) => setSequential(e.target.checked)}
            />
            <span
              title="A plan carries a single-cause attribution, not the full edge set. Without a `graph`, derived edges can under-constrain; sequential is always correct."
            >
              Run sequentially
            </span>
          </label>
        </div>
      </div>

      {error && (
        <div className="dy-card" style={{ marginBottom: 16, color: "var(--red)" }}>
          {error}
        </div>
      )}

      {empty && (
        <div className="dy-card dy-empty" style={{ marginBottom: 16 }}>
          Nothing to rebuild — every model matches its committed state.
        </div>
      )}

      {explanation && (
        <>
          <div className="dy-card" style={{ marginBottom: 16 }}>
            <strong>{explanation.summary}</strong>
          </div>

          <div className="dy-card" style={{ marginBottom: 16 }}>
            <table className="dy-table">
              <thead>
                <tr>
                  <th>Model</th>
                  <th>Why</th>
                  <th>Unit</th>
                  <th>Waits on</th>
                </tr>
              </thead>
              <tbody>
                {explanation.rows.map((row) => (
                  <tr key={row.task}>
                    <td className="mono">{row.model}</td>
                    <td>
                      <span className="dy-pill" style={{ color: REASON_COLOR[row.reason] }}>
                        {row.reason === "directly_changed" ? "directly changed" : "downstream"}
                      </span>
                      {row.because_of && (
                        <span style={{ marginLeft: 8 }}>
                          of <code className="mono">{row.because_of}</code>
                          {row.via_columns.length > 0 && (
                            <> via <code className="mono">{row.via_columns.join(", ")}</code></>
                          )}
                        </span>
                      )}
                    </td>
                    <td>{row.unit}</td>
                    <td className="mono">{row.depends_on.join(", ") || "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="dy-card">
            <div className="dy-bar" style={{ marginBottom: 8 }}>
              <span className="dy-subtitle">For a pull request</span>
              <div style={{ display: "flex", gap: 8 }}>
                <button className="dy-btn" onClick={() => copy(explanation.markdown, "Markdown")}>
                  Copy markdown
                </button>
                <button className="dy-btn" onClick={() => copy(explanation.mermaid, "Mermaid")}>
                  Copy Mermaid
                </button>
              </div>
            </div>
            {/* Rendered as source, not as a diagram: GitHub renders Mermaid
                natively in a comment, so the useful artifact here is text to
                paste — not a second diagramming library in this bundle. */}
            <pre className="mono" style={{ overflowX: "auto", margin: 0 }}>
              {explanation.mermaid}
            </pre>
          </div>
        </>
      )}
    </div>
  );
}
