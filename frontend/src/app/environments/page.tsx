"use client";

// Environments: named variable sets + write-only secrets. A workflow opts in
// with `environment: <name>`; variables become {{ env.NAME }} template refs,
// secrets resolve `value_from: {secret: NAME}` at dispatch. Secret values are
// never displayed — set/replace/delete only.

import { useCallback, useEffect, useState } from "react";
import { useToast } from "@/components/Toasts";
import {
  createEnvironment,
  deleteEnvironment,
  deleteEnvironmentSecret,
  listEnvironments,
  putEnvironmentSecret,
  updateEnvironment,
} from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";
import { timeAgo, absTime } from "@/lib/time";
import type { EnvironmentView } from "@/types/dagron";

export default function EnvironmentsPage() {
  const toast = useToast();
  const [envs, setEnvs] = useState<EnvironmentView[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [newName, setNewName] = useState("");
  const [newDesc, setNewDesc] = useState("");

  const load = useCallback(() => {
    listEnvironments()
      .then((e) => {
        setEnvs(e);
        setError(null);
      })
      .catch((e) => setError(errMsg(e)));
  }, []);
  useEffect(() => load(), [load]);

  const onCreate = async () => {
    setBusy(true);
    try {
      await createEnvironment(newName.trim(), newDesc.trim() || undefined);
      toast(`Environment "${newName.trim()}" created`);
      setNewName("");
      setNewDesc("");
      load();
    } catch (e) {
      toast(errMsg(e), "error");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="dy-page" style={{ maxWidth: 900 }}>
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            Environments
          </h1>
          <p className="dy-subtitle">
            Named variable sets for workflow templating. Reference one with{" "}
            <code className="mono">environment: name</code>, use variables as{" "}
            <code className="mono">{"{{ env.NAME }}"}</code> and secrets via{" "}
            <code className="mono">{"value_from: {secret: NAME}"}</code>.
          </p>
        </div>
      </div>
      {error && <p style={{ color: "var(--red)" }}>{error}</p>}

      {/* create */}
      <div className="dy-card" style={{ marginBottom: 18 }}>
        <strong>New environment</strong>
        <div style={{ display: "flex", gap: 10, alignItems: "flex-end", flexWrap: "wrap", marginTop: 12 }}>
          <label style={{ fontSize: 12, color: "var(--muted)" }}>
            Name (e.g. staging, prod)
            <br />
            <input value={newName} onChange={(e) => setNewName(e.target.value)} className="dy-btn mono" style={{ minWidth: 180, marginTop: 4 }} />
          </label>
          <label style={{ fontSize: 12, color: "var(--muted)" }}>
            Description
            <br />
            <input value={newDesc} onChange={(e) => setNewDesc(e.target.value)} className="dy-btn" style={{ minWidth: 260, marginTop: 4 }} />
          </label>
          <button onClick={onCreate} disabled={busy || !newName.trim()} className="dy-btn dy-btn-primary">
            Create
          </button>
        </div>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
        {envs.map((env) => (
          <EnvironmentCard key={env.id} env={env} onChanged={load} />
        ))}
        {envs.length === 0 && !error && (
          <div className="dy-card">
            <p className="dy-empty" style={{ margin: 0 }}>
              No environments yet. Create one (e.g. <code className="mono">staging</code>) and
              reference it from a workflow to template it per environment.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

function EnvironmentCard({ env, onChanged }: { env: EnvironmentView; onChanged: () => void }) {
  const toast = useToast();
  const [busy, setBusy] = useState(false);
  const [varName, setVarName] = useState("");
  const [varValue, setVarValue] = useState("");
  const [secretName, setSecretName] = useState("");
  const [secretValue, setSecretValue] = useState("");

  // Resolves true on success so callers clear their inputs only then — a
  // failed save must not wipe what the user typed (esp. a secret value).
  const act = async (fn: () => Promise<unknown>, okMsg: string): Promise<boolean> => {
    setBusy(true);
    try {
      await fn();
      toast(okMsg);
      onChanged();
      return true;
    } catch (e) {
      toast(errMsg(e), "error");
      return false;
    } finally {
      setBusy(false);
    }
  };

  const setVar = () => {
    const name = varName.trim();
    if (!name) return;
    const variables = { ...env.variables, [name]: varValue };
    void act(() => updateEnvironment(env.id, { variables }), `Variable ${name} set`).then((ok) => {
      if (ok) {
        setVarName("");
        setVarValue("");
      }
    });
  };
  const dropVar = (name: string) => {
    const variables = { ...env.variables };
    delete variables[name];
    void act(() => updateEnvironment(env.id, { variables }), `Variable ${name} removed`);
  };
  const setSecret = () => {
    const name = secretName.trim();
    if (!name || !secretValue) return;
    void act(() => putEnvironmentSecret(env.id, name, secretValue), `Secret ${name} stored (write-only)`).then((ok) => {
      if (ok) {
        setSecretName("");
        setSecretValue("");
      }
    });
  };
  const onDelete = () => {
    if (!confirm(`Delete environment "${env.name}" and its secrets? Specs naming it will fail to submit.`)) return;
    void act(() => deleteEnvironment(env.id), `Environment ${env.name} deleted`);
  };

  return (
    <div className="dy-card">
      <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
        <strong className="mono">{env.name}</strong>
        {env.description && <span style={{ color: "var(--muted)", fontSize: 13 }}>{env.description}</span>}
        <span style={{ marginLeft: "auto", fontSize: 12, color: "var(--dim)" }} title={absTime(env.updated_at)}>
          updated {timeAgo(env.updated_at)}
        </span>
        <button onClick={onDelete} disabled={busy} className="dy-btn dy-btn-danger">
          Delete
        </button>
      </div>

      {/* variables */}
      <div style={{ marginTop: 12 }}>
        <div style={{ fontSize: 11, fontWeight: 600, color: "var(--dim)", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 6 }}>
          Variables — templated as {"{{ env.NAME }}"}
        </div>
        {Object.entries(env.variables).map(([k, v]) => (
          <div key={k} style={{ display: "flex", alignItems: "center", gap: 10, padding: "4px 0", fontSize: 13 }}>
            <code className="mono" style={{ minWidth: 160 }}>{k}</code>
            <span className="mono" style={{ color: "var(--muted)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{v}</span>
            <button onClick={() => dropVar(k)} disabled={busy} className="dy-iconbtn" style={{ marginLeft: "auto" }} title={`Remove ${k}`} aria-label={`Remove variable ${k}`}>
              ✕
            </button>
          </div>
        ))}
        {Object.keys(env.variables).length === 0 && <p style={{ color: "var(--dim)", fontSize: 12.5, margin: "4px 0" }}>No variables.</p>}
        <div style={{ display: "flex", gap: 8, marginTop: 6, flexWrap: "wrap" }}>
          <input value={varName} onChange={(e) => setVarName(e.target.value)} placeholder="NAME" className="dy-btn mono" style={{ width: 160 }} />
          <input value={varValue} onChange={(e) => setVarValue(e.target.value)} placeholder="value" className="dy-btn mono" style={{ flex: 1, minWidth: 180 }} />
          <button onClick={setVar} disabled={busy || !varName.trim()} className="dy-btn">
            Set variable
          </button>
        </div>
      </div>

      {/* secrets */}
      <div style={{ marginTop: 14 }}>
        <div style={{ fontSize: 11, fontWeight: 600, color: "var(--dim)", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 6 }}>
          Secrets — write-only, resolved via {"value_from: {secret: NAME}"}
        </div>
        {!env.secrets_configured && (
          <p style={{ color: "var(--amber)", fontSize: 12.5, margin: "4px 0" }}>
            Secret storage is off: set <code className="mono">DAGRON_ENV_SECRET_KEY</code> on
            dagron-api and the engine to enable it.
          </p>
        )}
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
          {env.secret_names.map((s) => (
            <span key={s} className="dy-pill mono" title="Value is write-only; replace by setting it again">
              🔒 {s}
              <button
                onClick={() => {
                  if (confirm(`Delete secret "${s}"?`)) {
                    void act(() => deleteEnvironmentSecret(env.id, s), `Secret ${s} deleted`);
                  }
                }}
                disabled={busy}
                style={{ background: "none", border: "none", color: "var(--muted)", cursor: "pointer", padding: 0, marginLeft: 4 }}
                title={`Delete ${s}`}
                aria-label={`Delete secret ${s}`}
              >
                ✕
              </button>
            </span>
          ))}
          {env.secret_names.length === 0 && <span style={{ color: "var(--dim)", fontSize: 12.5 }}>No secrets.</span>}
        </div>
        {env.secrets_configured && (
          <div style={{ display: "flex", gap: 8, marginTop: 8, flexWrap: "wrap" }}>
            <input value={secretName} onChange={(e) => setSecretName(e.target.value)} placeholder="SECRET_NAME" className="dy-btn mono" style={{ width: 180 }} />
            <input
              value={secretValue}
              onChange={(e) => setSecretValue(e.target.value)}
              placeholder="value (stored encrypted, never shown again)"
              type="password"
              autoComplete="off"
              className="dy-btn mono"
              style={{ flex: 1, minWidth: 220 }}
            />
            <button onClick={setSecret} disabled={busy || !secretName.trim() || !secretValue} className="dy-btn dy-btn-primary">
              Store secret
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
