"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { getWorkflow, runWorkflow } from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";
import { changedValues, fieldError, paramFields, type ParamField } from "@/lib/run-params";

/// "Run workflow": asks for the workflow's declared parameters, with its
/// `param_schema` shaping each field (enum → dropdown, required marker,
/// description, pattern), then starts the run and opens it. A workflow that
/// declares no parameters starts straight away, as the Run button always did.
export default function RunWorkflowDialog({ workflowId, onClose }: { workflowId: string; onClose: () => void }) {
  const router = useRouter();
  const [name, setName] = useState("");
  const [fields, setFields] = useState<ParamField[] | null>(null);
  const [values, setValues] = useState<Record<string, string>>({});
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const started = useRef(false);

  const start = async (parameters?: Record<string, string>) => {
    if (started.current) return;
    started.current = true;
    setBusy(true);
    setError(null);
    try {
      const { run_id } = await runWorkflow(workflowId, parameters);
      onClose();
      router.push(`/runs/detail/?id=${run_id}`);
    } catch (e) {
      // The engine is authoritative (a schema refusal is a 400 naming the parameter).
      started.current = false;
      setError(errMsg(e));
      setBusy(false);
    }
  };

  useEffect(() => {
    let live = true;
    getWorkflow(workflowId)
      .then((wf) => {
        if (!live) return;
        const fs = paramFields(wf.parameters, wf.param_schema);
        setName(wf.name);
        setFields(fs);
        setValues(Object.fromEntries(fs.map((f) => [f.name, f.default])));
        if (fs.length === 0) void start();
      })
      .catch((e) => live && setError(errMsg(e)));
    return () => {
      live = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workflowId]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && !busy && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose, busy]);

  const problems = useMemo(
    () => Object.fromEntries((fields ?? []).map((f) => [f.name, fieldError(f, values[f.name] ?? "")])),
    [fields, values],
  );
  const invalid = Object.values(problems).some(Boolean);
  const showForm = fields != null && fields.length > 0;

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 50,
        background: "rgba(0,0,0,0.6)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 16,
      }}
      onClick={busy ? undefined : onClose}
      role="presentation"
    >
      <form
        onClick={(e) => e.stopPropagation()}
        onSubmit={(e) => {
          e.preventDefault();
          if (fields && !invalid) void start(changedValues(fields, values));
        }}
        role="dialog"
        aria-modal="true"
        aria-label="Run workflow"
        style={{
          width: "min(560px, 100%)",
          maxHeight: "88vh",
          display: "flex",
          flexDirection: "column",
          background: "var(--panel)",
          border: "1px solid var(--border)",
          borderRadius: 12,
          overflow: "hidden",
          boxShadow: "0 20px 60px rgba(0,0,0,0.5)",
        }}
      >
        <header style={{ padding: "14px 18px", borderBottom: "1px solid var(--border)" }}>
          <strong style={{ fontSize: 15 }}>Run {name || "workflow"}</strong>
          <div style={{ color: "var(--muted)", fontSize: 12, marginTop: 2 }}>
            {showForm
              ? "Set the parameters for this run. Unchanged fields keep the workflow's defaults."
              : error
                ? "Could not start the run."
                : "Starting…"}
          </div>
        </header>

        {error && <p style={{ color: "var(--red)", margin: 0, padding: "8px 18px", fontSize: 13 }}>{error}</p>}

        {showForm && (
          <div style={{ padding: "12px 18px", overflowY: "auto", display: "grid", gap: 14 }}>
            {fields.map((f) => {
              const v = values[f.name] ?? "";
              const problem = problems[f.name];
              const id = `param-${f.name}`;
              return (
                <div key={f.name}>
                  <label htmlFor={id} className="mono" style={{ fontSize: 13, fontWeight: 600 }}>
                    {f.name}
                    {f.required && (
                      <span style={{ color: "var(--red)" }} aria-label="required">
                        {" "}*
                      </span>
                    )}
                  </label>
                  {f.description && (
                    <div style={{ color: "var(--muted)", fontSize: 12, margin: "2px 0 4px" }}>{f.description}</div>
                  )}
                  {f.choices.length ? (
                    <select
                      id={id}
                      value={v}
                      onChange={(e) => setValues({ ...values, [f.name]: e.target.value })}
                      className="mono"
                      style={inputStyle}
                    >
                      {!f.choices.includes(v) && <option value={v}>{v === "" ? "— choose —" : v}</option>}
                      {f.choices.map((c) => (
                        <option key={c} value={c}>
                          {c}
                        </option>
                      ))}
                    </select>
                  ) : (
                    <input
                      id={id}
                      value={v}
                      onChange={(e) => setValues({ ...values, [f.name]: e.target.value })}
                      className="mono"
                      style={inputStyle}
                      aria-invalid={!!problem}
                      spellCheck={false}
                    />
                  )}
                  {f.pattern && !f.choices.length && (
                    <div className="mono" style={{ color: "var(--muted)", fontSize: 11, marginTop: 3 }}>
                      pattern: {f.pattern}
                    </div>
                  )}
                  {problem && <div style={{ color: "var(--red)", fontSize: 12, marginTop: 3 }}>{problem}</div>}
                </div>
              );
            })}
          </div>
        )}

        <footer
          style={{
            display: "flex",
            justifyContent: "flex-end",
            gap: 8,
            padding: "12px 18px",
            borderTop: "1px solid var(--border)",
          }}
        >
          <button type="button" onClick={onClose} disabled={busy} className="dy-btn">
            Cancel
          </button>
          {showForm && (
            <button type="submit" disabled={busy || invalid} className="dy-btn dy-btn-primary">
              {busy ? "Starting…" : "▶ Run"}
            </button>
          )}
        </footer>
      </form>
    </div>
  );
}

const inputStyle: React.CSSProperties = {
  width: "100%",
  boxSizing: "border-box",
  background: "var(--bg)",
  color: "var(--fg)",
  border: "1px solid var(--border)",
  borderRadius: 8,
  padding: "7px 10px",
  fontSize: 13,
};
