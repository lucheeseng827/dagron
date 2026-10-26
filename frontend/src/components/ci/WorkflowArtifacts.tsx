"use client";

import { useEffect, useState } from "react";
import {
  listWorkflowArtifacts,
  workflowArtifactProvenanceUrl,
  workflowArtifactUrl,
  type WorkflowArtifact,
} from "@/lib/dagron-api";

/// A size as a person reads one.
function size(bytes: number | null): string {
  if (bytes == null) return "";
  const units = ["B", "KB", "MB", "GB"];
  let n = bytes;
  let unit = 0;
  while (n >= 1024 && unit < units.length - 1) {
    n /= 1024;
    unit += 1;
  }
  return `${unit === 0 ? n : n.toFixed(1)} ${units[unit]}`;
}

/// The statuses a run ends in: no job of it uploads after.
const OVER = new Set(["succeeded", "failed", "cancelled", "skipped"]);

/// How often the list is read again while the run is not over, since each
/// job uploads as it ends.
const AGAIN_MS = 15_000;

/// What a GitHub workflow's jobs uploaded with `actions/upload-artifact`,
/// which the engine keeps: each by name, its size and when it expires, and
/// its zip to download, with its signed provenance where the engine signed
/// one. Nothing is shown for a run that has none. Read
/// again as the run's status changes, and every little while until it is
/// over. A listing that fails says so, with a way to ask again.
export default function WorkflowArtifacts({
  runId,
  status,
}: {
  runId: string;
  status?: string;
}) {
  const [artifacts, setArtifacts] = useState<WorkflowArtifact[]>([]);
  const [failed, setFailed] = useState<string | null>(null);
  const [asked, setAsked] = useState(0);
  useEffect(() => {
    let current = true;
    listWorkflowArtifacts(runId)
      .then((listed) => {
        if (!current) return;
        setArtifacts(listed);
        setFailed(null);
      })
      .catch((e: unknown) => {
        if (current) setFailed(e instanceof Error ? e.message : String(e));
      });
    return () => {
      current = false;
    };
  }, [runId, status, asked]);
  useEffect(() => {
    if (status && OVER.has(status)) return;
    const timer = setInterval(() => setAsked((n) => n + 1), AGAIN_MS);
    return () => clearInterval(timer);
  }, [status]);
  if (failed) {
    return (
      <div
        role="alert"
        style={{ padding: "8px 24px", fontSize: 13, borderBottom: "1px solid var(--border)" }}
      >
        <span style={{ color: "var(--red)" }}>
          The run&apos;s artifacts could not be listed: {failed}
        </span>{" "}
        <button type="button" onClick={() => setAsked((n) => n + 1)}>
          Try again
        </button>
      </div>
    );
  }
  if (artifacts.length === 0) return null;
  return (
    <div
      aria-label="Workflow artifacts"
      style={{
        display: "flex",
        flexWrap: "wrap",
        alignItems: "center",
        gap: 8,
        padding: "8px 24px",
        borderBottom: "1px solid var(--border)",
        fontSize: 13,
      }}
    >
      <span style={{ color: "var(--muted)" }}>Artifacts</span>
      {artifacts.map((a) => (
        <span key={a.name} style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
          <a
            href={workflowArtifactUrl(runId, a.name)}
            download={`${a.name}.zip`}
            title={`${a.name}: ${size(a.size_bytes)}, kept until ${a.expires_at}${
              a.sha256 ? `, sha256 ${a.sha256}` : ""
            }`}
            style={{
              border: "1px solid var(--border)",
              borderRadius: 6,
              padding: "2px 8px",
              textDecoration: "none",
              color: "inherit",
            }}
          >
            {a.name}
            <span style={{ color: "var(--muted)", marginLeft: 6 }}>{size(a.size_bytes)}</span>
          </a>
          {a.provenance && (
            <a
              href={workflowArtifactProvenanceUrl(runId, a.name)}
              download={`${a.name}.intoto.json`}
              title={`${a.name}'s SLSA provenance, signed by the engine: verify it against the zip with cosign verify-blob-attestation`}
              style={{ color: "var(--muted)", fontSize: 12 }}
            >
              provenance
            </a>
          )}
        </span>
      ))}
    </div>
  );
}
