"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import SpecEditorWithPreview from "@/components/dag/SpecEditorWithPreview";
import { submitRun } from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";

const SAMPLE = `name: my-pipeline
tasks:
  - name: prepare
    command: ["echo", "prepare"]
  - name: process
    command: ["echo", "process"]
    depends_on: [prepare]
`;

export default function SubmitPage() {
  const router = useRouter();
  const [yaml, setYaml] = useState(SAMPLE);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function onSubmit() {
    setBusy(true);
    setError(null);
    try {
      const { run_id } = await submitRun(yaml);
      router.push(`/runs/${run_id}`);
    } catch (e) {
      // Server is the authoritative validator (cycle/dup/unknown-dep → 400).
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div style={{ padding: "26px 32px 24px", display: "flex", flexDirection: "column", height: "100vh" }}>
      <div className="dy-pagehead" style={{ marginBottom: 16 }}>
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            Submit workflow
          </h1>
          <p className="dy-subtitle">Paste a DAG spec and run it once — the graph previews live as you type. Validated server-side.</p>
        </div>
        <button onClick={onSubmit} disabled={busy} className="dy-btn dy-btn-primary" style={{ cursor: busy ? "wait" : "pointer" }}>
          {busy ? "Submitting…" : "▶ Run"}
        </button>
      </div>
      <div style={{ flex: 1, minHeight: 0, border: "1px solid var(--border)", borderRadius: 12, overflow: "hidden" }}>
        <SpecEditorWithPreview value={yaml} onChange={setYaml} />
      </div>
      {error && <p style={{ color: "var(--red)", marginTop: 10 }}>{error}</p>}
    </div>
  );
}
