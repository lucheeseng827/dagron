//! Signed-bundle reconcile: verify `manifest.json` + `manifest.sig` against
//! `DAGRON_BUNDLE_PUBKEYS`, then apply every spec atomically.
//!
//! The plain sync path (`sync::reconcile`) is per-file and forgiving — one
//! malformed spec is a warning beside the ones that synced, because the files
//! in a directory are independent statements. A bundle is one statement,
//! signed as a whole, so this path is the opposite on purpose: every check is
//! all-or-nothing, and the first failure — a bad signature, an altered file, a
//! spec the engine's parser rejects, two files claiming one workflow name —
//! leaves the datastore exactly as it was. "Half a signed bundle" is not a
//! state a fleet should ever be in, and it would be indistinguishable from an
//! attacker who managed to get one file past the verifier.
//!
//! Split in two so the half without a datastore is testable: [`prepare`]
//! verifies and validates (pure, filesystem in, specs out), and
//! [`reconcile_bundle`] hands its result to `sync::apply_specs` — one
//! transaction, one version row per workflow stamped with the bundle's
//! provenance.

use std::collections::HashMap;
use std::path::Path;

use dagron_crypto::bundle::{verify_bundle_dir, VerifyingKey};
use tracing::info;

/// What a bundle reconcile applied.
pub struct Applied {
    /// `bundle:<name>@<version>#<digest>` — what every version row was stamped
    /// with, and what the console's last-message line shows.
    pub provenance: String,
    /// Workflow names, in manifest order.
    pub synced: Vec<String>,
}

/// Workflow names a spec chains with `workflow_ref`.
///
/// A `workflow_ref` task is resolved at *run creation* by inlining the
/// referenced workflow's stored spec. That row is ordinary mutable state —
/// anyone who can write a workflow can change what it holds — so a bundle that
/// chains outside itself does not decide what runs, and the signature stops
/// meaning "what runs is what was signed". A bundle may therefore only chain
/// workflows it defines; anything else is refused with the whole bundle.
fn chained_refs(yaml: &str) -> Vec<String> {
    let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(yaml) else { return Vec::new() };
    let Some(tasks) = doc.get("tasks").and_then(serde_yaml::Value::as_sequence) else {
        return Vec::new();
    };
    tasks
        .iter()
        .filter_map(|t| t.get("workflow_ref").and_then(serde_yaml::Value::as_str))
        .map(str::to_string)
        .collect()
}

/// Verify the bundle at `dir` against `keys` and validate every spec through
/// the engine's parser; returns the provenance string and `(name, yaml)` in
/// manifest order. No datastore access. Every failure is one message naming
/// the file — all of them at once for parse errors, so an author fixes the
/// bundle in one round rather than one file per sync.
pub fn prepare(dir: &Path, keys: &[VerifyingKey]) -> Result<(String, Vec<(String, String)>), String> {
    let bundle = verify_bundle_dir(dir, keys).map_err(|e| format!("signed bundle rejected: {e:#}"))?;
    let provenance = bundle.provenance();

    let mut specs = Vec::with_capacity(bundle.specs.len());
    let mut errors = Vec::new();
    let mut owners: HashMap<String, String> = HashMap::new();
    for (path, yaml) in &bundle.specs {
        match crate::sync::validate(yaml) {
            Ok(name) => {
                if let Some(first) = owners.insert(name.clone(), path.clone()) {
                    errors.push(format!("{path}: workflow name '{name}' is already defined by {first}"));
                    continue;
                }
                specs.push((name, yaml.clone()));
            }
            Err(msg) => errors.push(format!("{path}: {msg}")),
        }
    }
    // Every chained workflow must be one this bundle signs (see `chained_refs`).
    for (name, yaml) in &specs {
        for target in chained_refs(yaml) {
            if !owners.contains_key(&target) {
                errors.push(format!(
                    "{name}: workflow_ref '{target}' is not defined by this bundle — a signed \
                     bundle may only chain workflows it signs, or what runs is not what was signed"
                ));
            }
        }
    }
    if !errors.is_empty() {
        return Err(format!(
            "{provenance} verified but {} spec(s) failed validation, nothing applied: {}",
            errors.len(),
            errors.join("; ")
        ));
    }
    Ok((provenance, specs))
}

/// [`prepare`] followed by a single-transaction apply. `Err` is the message
/// stored on the repo row; on any error nothing was written.
pub async fn reconcile_bundle(
    pool: &sqlx::PgPool,
    dir: &Path,
    keys: &[VerifyingKey],
) -> Result<Applied, String> {
    let (provenance, specs) = prepare(dir, keys)?;
    let synced = crate::sync::apply_specs(pool, &specs, &provenance)
        .await
        .map_err(|e| format!("{provenance} verified but applying it failed, nothing applied: {e}"))?;
    info!(bundle = %provenance, workflows = synced.len(), "signed bundle applied");
    Ok(Applied { provenance, synced })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dagron_crypto::bundle::{
        sha256_hex, sign_manifest, signature_b64, Manifest, SigningKey, SpecEntry, BUNDLE_FORMAT,
        MANIFEST_FILE, SIGNATURE_FILE,
    };
    use std::path::PathBuf;

    const GOOD_A: &str = "name: line_health\ntasks:\n  - name: check\n    command: [\"true\"]\n";
    const GOOD_B: &str = "name: nightly_report\ntasks:\n  - name: report\n    command: [\"true\"]\n";
    /// Parses as YAML and looks like a spec, but the engine refuses it (a task
    /// with no command) — exactly the file that must not reach the datastore.
    const BAD: &str = "name: broken\ntasks:\n  - name: a\n";

    struct Scratch(PathBuf);
    impl Scratch {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("dagron-gitops-bundle-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            Scratch(dir)
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Write and sign a bundle of `(path, text)` under a fresh directory,
    /// exactly as the `bundle_sign` example lays it out.
    fn signed(specs: &[(&str, &str)]) -> (Scratch, Vec<VerifyingKey>) {
        let sk = SigningKey::from_bytes(&[5u8; 32]);
        let s = Scratch::new();
        let mut entries = Vec::new();
        for (p, t) in specs {
            let path = s.0.join(p);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, t).unwrap();
            entries.push(SpecEntry { path: p.to_string(), sha256: sha256_hex(t.as_bytes()) });
        }
        let manifest = Manifest {
            format: BUNDLE_FORMAT.into(),
            name: "plant-3/line-a".into(),
            version: "2026.09.01-3".into(),
            created_at: "2026-09-01T00:00:00Z".into(),
            key_id: None,
            specs: entries,
        }
        .to_bytes()
        .unwrap();
        let sig = signature_b64(&sign_manifest(&manifest, &sk));
        std::fs::write(s.0.join(MANIFEST_FILE), &manifest).unwrap();
        std::fs::write(s.0.join(SIGNATURE_FILE), format!("{sig}\n")).unwrap();
        (s, vec![sk.verifying_key()])
    }

    #[test]
    fn a_good_bundle_yields_named_specs_in_manifest_order() {
        let (s, keys) = signed(&[("specs/b.yaml", GOOD_B), ("specs/a.yaml", GOOD_A)]);
        let (provenance, specs) = prepare(&s.0, &keys).unwrap();
        assert!(provenance.starts_with("bundle:plant-3/line-a@2026.09.01-3#"), "{provenance}");
        let names: Vec<&str> = specs.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["nightly_report", "line_health"]);
        assert_eq!(specs[1].1, GOOD_A);
    }

    #[test]
    fn a_bundle_signed_by_an_untrusted_key_is_rejected_before_parsing() {
        let (s, _) = signed(&[("a.yaml", GOOD_A)]);
        let other = SigningKey::from_bytes(&[6u8; 32]).verifying_key();
        let err = prepare(&s.0, &[other]).unwrap_err();
        assert!(err.starts_with("signed bundle rejected:"), "{err}");
        assert!(err.contains("trusted key"), "{err}");
    }

    /// Correctly signed, but one spec is something the engine cannot run: the
    /// whole bundle is refused, and the message names the file — plus every
    /// other bad file, so the author fixes them in one round.
    #[test]
    fn a_validly_signed_bundle_with_an_invalid_spec_applies_nothing() {
        let (s, keys) = signed(&[("good.yaml", GOOD_A), ("bad.yaml", BAD), ("worse.yaml", BAD)]);
        let err = prepare(&s.0, &keys).unwrap_err();
        assert!(err.contains("nothing applied"), "{err}");
        assert!(err.contains("2 spec(s)"), "{err}");
        assert!(err.contains("bad.yaml:"), "{err}");
        assert!(err.contains("worse.yaml:"), "{err}");
        // The provenance is still reported so the operator knows *which* bundle
        // was refused.
        assert!(err.contains("bundle:plant-3/line-a@"), "{err}");
    }

    #[test]
    fn two_files_claiming_one_workflow_name_are_refused() {
        let (s, keys) = signed(&[("one.yaml", GOOD_A), ("two.yaml", GOOD_A)]);
        let err = prepare(&s.0, &keys).unwrap_err();
        assert!(err.contains("already defined by one.yaml"), "{err}");
        assert!(err.contains("two.yaml:"), "{err}");
    }

    #[test]
    fn a_spec_changed_after_signing_is_refused() {
        let (s, keys) = signed(&[("a.yaml", GOOD_A)]);
        std::fs::write(s.0.join("a.yaml"), GOOD_B).unwrap();
        let err = prepare(&s.0, &keys).unwrap_err();
        assert!(err.contains("sha256 mismatch"), "{err}");
    }
}
