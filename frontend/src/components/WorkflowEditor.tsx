"use client";

import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import Editor from "@monaco-editor/react";
import "@/lib/monaco"; // self-host the Monaco runtime (air-gap; no CDN)
import ScheduleDrawer from "@/components/ScheduleDrawer";
import EditableDag from "@/components/dag/EditableDag";
import { modelToYaml, parseModel } from "@/lib/spec-model";
import { visualSupport } from "@/lib/spec-support";
import { STARTERS } from "@/lib/starters";
import {
  createWorkflow,
  deleteWorkflow,
  getWorkflow,
  runWorkflow,
  syncWorkflowToGit,
  updateWorkflow,
} from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";

const SAMPLE = `name: my-workflow
tasks:
  - name: prepare
    command: ["echo", "prepare"]
  - name: process
    command: ["echo", "process"]
    depends_on: [prepare]
`;

/// Create (id undefined) or edit a first-class workflow. Two synced views: a
/// YAML editor and an editable visual DAG (round-tripped through spec-model).
export default function WorkflowEditor({ id }: { id?: string }) {
  const router = useRouter();
  const isNew = !id;
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [spec, setSpec] = useState(isNew ? SAMPLE : "");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [loading, setLoading] = useState(!isNew);
  const [view, setView] = useState<"yaml" | "visual">("visual");
  const [prUrl, setPrUrl] = useState<string | null>(null);

  // Live parse for the visual editor; the model is derived from the YAML spec.
  const parsed = useMemo(() => parseModel(spec), [spec]);
  // …and a separate question: would the canvas be telling the truth? A spec
  // using features the visual model can't represent (fan-out, sensors, hooks)
  // locks Visual mode rather than letting an edit quietly corrupt it.
  const support = useMemo(() => visualSupport(spec), [spec]);
  const locked = !support.ok;

  // Loading a workflow that uses those features while sitting on the Visual tab
  // must move the user, not present a graph they can edit into nonsense.
  useEffect(() => {
    if (locked) setView("yaml");
  }, [locked]);

  useEffect(() => {
    if (!id) return;
    getWorkflow(id)
      .then((w) => {
        setName(w.name);
        setSpec(w.spec);
        setDescription(w.description ?? "");
      })
      .catch((e) => setError(errMsg(e)))
      .finally(() => setLoading(false));
  }, [id]);

  const onSave = async () => {
    setBusy(true);
    setError(null);
    try {
      if (isNew) {
        const w = await createWorkflow(spec, name || undefined, description || undefined);
        router.push(`/workflows/${w.id}`);
      } else {
        await updateWorkflow(id!, spec, name || undefined, description || undefined);
      }
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  };

  const onRun = async () => {
    if (!id) return;
    setBusy(true);
    try {
      const { run_id } = await runWorkflow(id);
      router.push(`/runs/${run_id}`);
    } catch (e) {
      setError(errMsg(e));
      setBusy(false);
    }
  };

  const onDelete = async () => {
    if (!id || !confirm("Delete this workflow?")) return;
    setBusy(true);
    try {
      await deleteWorkflow(id);
      router.push("/workflows");
    } catch (e) {
      setError(errMsg(e));
      setBusy(false);
    }
  };

  // Persist current edits, then open a PR with the raw DAG spec.
  const onSyncGit = async () => {
    if (!id) return;
    setBusy(true);
    setError(null);
    setPrUrl(null);
    try {
      await updateWorkflow(id, spec, name || undefined, description || undefined);
      const { pr_url } = await syncWorkflowToGit(id);
      setPrUrl(pr_url);
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  };

  if (loading) return <div className="dy-page">Loading…</div>;

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100vh", padding: "20px 24px" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 12 }}>
        <h1 className="dy-h1" style={{ margin: 0 }}>
          {isNew ? "New workflow" : "Edit workflow"}
        </h1>
        <input
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="name (optional — derived from spec)"
          style={{ background: "var(--bg)", color: "var(--fg)", border: "1px solid var(--border)", borderRadius: 8, padding: "7px 10px", minWidth: 220 }}
        />
        <input
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          placeholder="description (optional)"
          style={{ background: "var(--bg)", color: "var(--fg)", border: "1px solid var(--border)", borderRadius: 8, padding: "7px 10px", minWidth: 240 }}
        />
        {isNew && (
          <select
            aria-label="Start from an example"
            defaultValue=""
            onChange={(e) => {
              const s = STARTERS.find((x) => x.id === e.target.value);
              if (s) setSpec(s.spec);
              e.target.selectedIndex = 0; // reset so re-picking the same example works
            }}
            title="Load an example workflow into the editor"
            style={{ background: "var(--bg)", color: "var(--fg)", border: "1px solid var(--border)", borderRadius: 8, padding: "7px 10px" }}
          >
            <option value="" disabled>
              Start from an example…
            </option>
            {STARTERS.map((s) => (
              <option key={s.id} value={s.id} title={s.description}>
                {s.label}
              </option>
            ))}
          </select>
        )}
        <div className="dy-seg" style={{ display: "flex", gap: 4, background: "var(--panel)", border: "1px solid var(--border)", borderRadius: 8, padding: 3 }}>
          {(["visual", "yaml"] as const).map((v) => {
            const disabled = v === "visual" && locked;
            return (
              <button
                key={v}
                onClick={() => !disabled && setView(v)}
                disabled={disabled}
                className={`dy-pill ${view === v ? "dy-pill-active" : ""}`}
                style={{
                  cursor: disabled ? "not-allowed" : "pointer",
                  textTransform: "capitalize",
                  opacity: disabled ? 0.5 : 1,
                }}
                title={
                  disabled
                    ? "This workflow uses features the visual editor can't edit — use the YAML tab."
                    : undefined
                }
              >
                {v}
              </button>
            );
          })}
        </div>
        <div style={{ flex: 1 }} />
        {!isNew && (
          <>
            <button onClick={onSyncGit} disabled={busy} className="dy-btn" title="Save and open a pull request with this DAG">
              Sync to Git
            </button>
            <button onClick={onRun} disabled={busy} className="dy-btn dy-btn-primary">
              ▶ Run
            </button>
            <button onClick={onDelete} disabled={busy} className="dy-btn dy-btn-danger">
              Delete
            </button>
          </>
        )}
        <button onClick={onSave} disabled={busy} className="dy-btn dy-btn-primary">
          {busy ? "Saving…" : "Save"}
        </button>
      </div>

      {error && <p style={{ color: "var(--red)", marginBottom: 8 }}>{error}</p>}
      {locked && <VisualLockedNotice reasons={support.reasons} />}
      {prUrl && (
        <p style={{ marginBottom: 8 }}>
          Pull request opened:{" "}
          <a href={prUrl} target="_blank" rel="noreferrer">
            {prUrl}
          </a>
        </p>
      )}

      <div style={{ flex: 1, minHeight: 320, border: "1px solid var(--border)", borderRadius: 10, overflow: "hidden" }}>
        {view === "yaml" ? (
          <Editor
            height="100%"
            defaultLanguage="yaml"
            theme="vs-dark"
            value={spec}
            onChange={(v) => setSpec(v ?? "")}
            options={{ minimap: { enabled: false }, fontSize: 13, tabSize: 2, scrollBeyondLastLine: false }}
          />
        ) : parsed.model ? (
          <EditableDag model={parsed.model} onChange={(m) => setSpec(modelToYaml(m))} />
        ) : (
          <div style={{ padding: 16, color: "var(--red)" }}>
            Can&apos;t render graph: {parsed.error}. Fix it in the YAML view.
          </div>
        )}
      </div>

      {!isNew && id && <ScheduleDrawer workflowId={id} />}
    </div>
  );
}

/// Why Visual mode is off. The headline is the whole message for most people;
/// the field list is there for the one who wants to know which line to change,
/// capped so a fan-out over twenty tasks doesn't push the editor off screen.
function VisualLockedNotice({ reasons }: { reasons: string[] }) {
  const SHOWN = 4;
  return (
    <div
      style={{
        marginBottom: 8,
        padding: "9px 12px",
        borderRadius: 8,
        border: "1px solid rgba(210,153,34,0.45)",
        background: "rgba(210,153,34,0.10)",
        fontSize: 13,
      }}
    >
      <span style={{ color: "var(--amber)" }}>
        This workflow uses features the visual editor can&apos;t edit — use the YAML tab.
      </span>
      {reasons.length > 0 && (
        <ul style={{ margin: "6px 0 0", paddingLeft: 20, color: "var(--muted)", fontSize: 12 }}>
          {reasons.slice(0, SHOWN).map((r) => (
            <li key={r}>{r}</li>
          ))}
          {reasons.length > SHOWN && <li>…and {reasons.length - SHOWN} more</li>}
        </ul>
      )}
    </div>
  );
}
