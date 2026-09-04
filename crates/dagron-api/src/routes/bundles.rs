//! `POST /api/workflows/bundle` — apply a signed workflow bundle.
//!
//! The push counterpart of the GitOps worker's bundle path: the same
//! `manifest.json` + `manifest.sig` + specs, carried in a JSON body instead of
//! a checkout, verified by the same `dagron_crypto::bundle` primitive against
//! the same `DAGRON_BUNDLE_PUBKEYS`, and applied with the same shape — one
//! transaction, every workflow upserted by name, a `workflow_versions` row per
//! workflow whose `created_by` is the bundle's provenance string. A fleet
//! operator who pushes through this route and one who syncs from Git get
//! identical rows.
//!
//! **Fail closed, all or nothing.** No trust set configured is `501` — not
//! "accept unsigned" — and a body that fails any check (base64, signature,
//! hash, an unlisted or missing file, a spec the validator rejects, two files
//! claiming one name) is `400` with nothing written. There is no partial
//! apply: a bundle is one signed statement.
//!
//! **Cohorts.** The body may name a `cohort` to stage the bundle across a
//! fleet. Staged rollout is not in this build, which answers with
//! a signpost (`400`) naming the gap and what it does instead — apply to
//! this deployment, now — rather than silently applying to everything.
//!
//! **Size.** The route sits under the core 1 MiB body cap (see `main.rs`);
//! base64 costs a third, so a bundle of roughly 750 KiB of YAML is the most
//! this route accepts. Bundles are workflow definitions, not data — that is
//! generous.

use std::collections::HashMap;
use std::sync::OnceLock;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use base64::Engine as _;
use dagron_crypto::bundle::{self, VerifiedBundle, VerifyingKey, PUBKEYS_ENV};
use serde::{Deserialize, Serialize};

use crate::auth::AuthUser;
use crate::routes::control;
use crate::state::AppState;

/// What the open build says when a bundle names a cohort.
#[cfg(not(feature = "enterprise"))]
pub(crate) const COHORT_SIGNPOST: &str = "staged rollout by cohort is not in this build — \
     https://github.com/lucheeseng827/dagron#what-this-build-does-not-do. This build applies the bundle to \
     this deployment immediately (docs/BUNDLES.md): resend without `cohort` to apply it here.";

/// In an enterprise build the cohort machinery lives in the fleet plane, not
/// in this per-deployment route: staging is a rollout there, and the units
/// receive the bundle through their fleet link. This route stays what it is —
/// "apply to this deployment, now" — so a caller who meant to stage does not
/// accidentally apply.
#[cfg(feature = "enterprise")]
pub(crate) const COHORT_SIGNPOST: &str = "staged rollout by cohort is a fleet-plane rollout, not a \
     management-API call: create the rollout on the fleet plane. This route applies the bundle to \
     this deployment immediately (docs/BUNDLES.md): resend without `cohort` to apply it here.";

/// Request body. `files[].path` is the manifest-relative spec path; every
/// binary field is standard base64.
#[derive(Debug, Deserialize)]
pub struct BundleBody {
    pub manifest_b64: String,
    pub signature_b64: String,
    #[serde(default)]
    pub files: Vec<BundleFileBody>,
    /// Stage across a cohort instead of applying here — see the module doc.
    #[serde(default)]
    pub cohort: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct BundleFileBody {
    pub path: String,
    pub content_b64: String,
}

#[derive(Debug, Serialize)]
pub struct AppliedWorkflow {
    pub id: String,
    pub name: String,
    /// The `workflow_versions.version` this apply wrote.
    pub version: i64,
}

#[derive(Debug, Serialize)]
pub struct BundleResponse {
    /// The manifest's `name`.
    pub bundle: String,
    /// The manifest's `version`.
    pub version: String,
    /// Hex SHA-256 of the exact manifest bytes.
    pub digest: String,
    /// `bundle:<name>@<version>#<digest[..12]>` — what every version row's
    /// `created_by` says, so a client can grep for exactly what it applied.
    pub provenance: String,
    /// In manifest order.
    pub applied: Vec<AppliedWorkflow>,
}

/// The trust set, read once: a process's environment never changes after
/// boot (see `config.rs`), and parsing keys per request would only make the
/// effective configuration harder to reason about.
///
/// `Ok(None)` — not configured (the route answers `501`). `Err` — configured
/// but unparseable, which is an operator error worth a `500` and a log line,
/// never a fallback to "trust nothing and pretend it is fine".
fn trusted_keys() -> Result<Option<&'static [VerifyingKey]>, (StatusCode, String)> {
    static KEYS: OnceLock<Result<Option<Vec<VerifyingKey>>, String>> = OnceLock::new();
    match KEYS.get_or_init(|| keys_from(std::env::var(PUBKEYS_ENV).ok().as_deref())) {
        Ok(Some(keys)) => Ok(Some(keys.as_slice())),
        Ok(None) => Ok(None),
        Err(e) => {
            tracing::error!(error = %e, "{PUBKEYS_ENV} is set but unusable — every bundle is refused");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("{PUBKEYS_ENV} is set but could not be parsed — see the dagron-api log"),
            ))
        }
    }
}

/// The pure half of [`trusted_keys`]: unset or blank is "not configured";
/// anything else must parse as a key list.
pub(crate) fn keys_from(raw: Option<&str>) -> Result<Option<Vec<VerifyingKey>>, String> {
    match raw.map(str::trim) {
        None | Some("") => Ok(None),
        Some(raw) => bundle::parse_pubkeys(raw).map(Some).map_err(|e| format!("{e:#}")),
    }
}

/// A verified, validated bundle: what is left to do is write it.
#[derive(Debug)]
pub(crate) struct Prepared {
    pub bundle: VerifiedBundle,
    /// `(workflow name, yaml)` in manifest order.
    pub specs: Vec<(String, String)>,
}

/// Everything before the transaction: decode, verify against `keys`, and
/// validate every spec through the same `parse_and_validate` the console's
/// create/update paths use. Pure, so the whole refusal surface is testable
/// without a datastore.
pub(crate) fn prepare(body: &BundleBody, keys: &[VerifyingKey]) -> Result<Prepared, (StatusCode, String)> {
    let manifest = decode_field("manifest_b64", &body.manifest_b64)?;
    let signature = decode_field("signature_b64", &body.signature_b64)?;
    let mut files = Vec::with_capacity(body.files.len());
    for f in &body.files {
        files.push((f.path.clone(), decode_field(&format!("files[{}].content_b64", f.path), &f.content_b64)?));
    }
    let bundle = bundle::verify_bundle(&manifest, &signature, &files, keys)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bundle rejected: {e:#}")))?;

    let mut specs = Vec::with_capacity(bundle.specs.len());
    let mut owners: HashMap<String, String> = HashMap::new();
    let mut errors = Vec::new();
    for (path, yaml) in &bundle.specs {
        match control::parse_and_validate(yaml) {
            Ok(spec) => {
                if let Some(first) = owners.insert(spec.name.clone(), path.clone()) {
                    errors.push(format!("{path}: workflow name '{}' is already defined by {first}", spec.name));
                    continue;
                }
                specs.push((spec.name, yaml.clone()));
            }
            Err((_, msg)) => errors.push(format!("{path}: {msg}")),
        }
    }
    // Every chained workflow must be one this bundle signs. `workflow_ref` is
    // resolved at run creation from the `workflows` table — ordinary mutable
    // state — so a bundle that chains outside itself does not decide what runs,
    // and the signature would no longer mean "what runs is what was signed".
    for (name, yaml) in &specs {
        let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(yaml) else { continue };
        for target in crate::expand::direct_refs(&doc) {
            if !owners.contains_key(&target) {
                errors.push(format!(
                    "{name}: workflow_ref '{target}' is not defined by this bundle — a signed \
                     bundle may only chain workflows it signs"
                ));
            }
        }
    }
    if !errors.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "{} verified but {} spec(s) failed validation, nothing applied: {}",
                bundle.provenance(),
                errors.len(),
                errors.join("; ")
            ),
        ));
    }
    Ok(Prepared { bundle, specs })
}

fn decode_field(field: &str, value: &str) -> Result<Vec<u8>, (StatusCode, String)> {
    base64::engine::general_purpose::STANDARD
        .decode(value.trim())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("{field} is not standard base64: {e}")))
}

/// Map a sqlx error to an opaque 500 — logged, never returned, so a client
/// cannot read datastore internals out of the body (as `lifecycle.rs` does).
fn internal(err: sqlx::Error) -> (StatusCode, String) {
    tracing::error!(error = ?err, "bundle apply db query failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
}

/// Apply a signed bundle (manifest + signature + spec files) in one transaction.
pub async fn apply_bundle(
    auth: AuthUser,
    State(state): State<AppState>,
    Json(body): Json<BundleBody>,
) -> Result<(StatusCode, Json<BundleResponse>), (StatusCode, String)> {
    // Refuse the cohort *before* touching the trust set: the caller asked for
    // something this route does not do, and the answer is the same whether or
    // not keys are configured.
    if body.cohort.is_some() {
        return Err((StatusCode::BAD_REQUEST, COHORT_SIGNPOST.to_string()));
    }
    let keys = trusted_keys()?.ok_or_else(|| {
        (
            StatusCode::NOT_IMPLEMENTED,
            format!(
                "signed bundle apply is not configured on this deployment: set {PUBKEYS_ENV} on dagron-api to the trusted signing key(s) (docs/BUNDLES.md)"
            ),
        )
    })?;
    let Prepared { bundle, specs } = prepare(&body, keys)?;
    let provenance = bundle.provenance();

    // One transaction for the whole bundle. Each workflow row is locked
    // (`FOR UPDATE`) before its version is computed, for the same reason
    // `update_workflow` locks: a concurrent console edit of the same workflow
    // takes the same lock, so the two serialise instead of both reading one
    // `MAX(version)` and colliding on `UNIQUE (workflow_id, version)`. Unlike
    // the edit path there is no savepoint around `record_version`: a bundle
    // without its history rows is not a bundle that was applied, so any
    // failure rolls back the lot.
    let mut tx = state.write_pool.begin().await.map_err(internal)?;
    let now = chrono::Utc::now().to_rfc3339();
    let mut applied = Vec::with_capacity(specs.len());
    for (name, yaml) in &specs {
        let existing: Option<String> =
            sqlx::query_scalar("SELECT id FROM workflows WHERE name = $1 FOR UPDATE")
                .bind(name)
                .fetch_optional(&mut *tx)
                .await
                .map_err(internal)?;
        let id = match existing {
            Some(id) => {
                sqlx::query("UPDATE workflows SET spec = $1, updated_at = $2 WHERE id = $3")
                    .bind(yaml)
                    .bind(&now)
                    .bind(&id)
                    .execute(&mut *tx)
                    .await
                    .map_err(internal)?;
                id
            }
            None => {
                let id = uuid::Uuid::new_v4().to_string();
                sqlx::query(
                    "INSERT INTO workflows (id, name, spec, created_at, updated_at) VALUES ($1,$2,$3,$4,$4)",
                )
                .bind(&id)
                .bind(name)
                .bind(yaml)
                .bind(&now)
                .execute(&mut *tx)
                .await
                .map_err(internal)?;
                id
            }
        };
        let version =
            crate::routes::lifecycle::record_version(&mut tx, &id, name, yaml, Some(&provenance))
                .await
                .map_err(internal)?;
        applied.push(AppliedWorkflow { id, name: name.clone(), version });
    }
    tx.commit().await.map_err(internal)?;

    tracing::info!(
        bundle = %provenance,
        digest = %bundle.digest,
        workflows = applied.len(),
        by = %auth.0.email,
        "signed bundle applied"
    );
    Ok((
        StatusCode::OK,
        Json(BundleResponse {
            bundle: bundle.manifest.name.clone(),
            version: bundle.manifest.version.clone(),
            digest: bundle.digest.clone(),
            provenance,
            applied,
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dagron_crypto::bundle::{sha256_hex, sign_manifest, SigningKey, BUNDLE_FORMAT};

    const GOOD_A: &str = "name: line_health\ntasks:\n  - name: check\n    command: [\"true\"]\n";
    const GOOD_B: &str = "name: nightly_report\ntasks:\n  - name: report\n    command: [\"true\"]\n";
    /// Valid YAML, refused by `parse_and_validate`: a dependency on a task that
    /// does not exist.
    const BAD_DEP: &str =
        "name: dangling\ntasks:\n  - name: a\n    command: [\"true\"]\n    depends_on: [nowhere]\n";

    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    /// A signed body over `(path, text)` specs, plus the key that signed it.
    fn signed_body(specs: &[(&str, &str)]) -> (BundleBody, VerifyingKey) {
        let sk = SigningKey::from_bytes(&[11u8; 32]);
        let entries: Vec<serde_json::Value> = specs
            .iter()
            .map(|(p, t)| serde_json::json!({ "path": p, "sha256": sha256_hex(t.as_bytes()) }))
            .collect();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "format": BUNDLE_FORMAT,
            "name": "plant-3/line-a",
            "version": "2026.09.01-3",
            "created_at": "2026-09-01T00:00:00Z",
            "specs": entries,
        }))
        .unwrap();
        let sig = sign_manifest(&manifest, &sk);
        let body = BundleBody {
            manifest_b64: b64(&manifest),
            signature_b64: b64(&sig),
            files: specs
                .iter()
                .map(|(p, t)| BundleFileBody { path: p.to_string(), content_b64: b64(t.as_bytes()) })
                .collect(),
            cohort: None,
        };
        (body, sk.verifying_key())
    }

    #[test]
    fn open_build_signposts_the_gap_and_the_fallback() {
        #[cfg(not(feature = "enterprise"))]
        {
            assert!(COHORT_SIGNPOST.contains("is not in this build"));
            assert!(COHORT_SIGNPOST.contains("#what-this-build-does-not-do"));
        }
        // Both builds: what this route does instead, citing a mirrored doc.
        assert!(COHORT_SIGNPOST.contains("applies the bundle to this deployment immediately"));
        assert!(COHORT_SIGNPOST.contains("docs/BUNDLES.md"));
    }

    #[test]
    fn body_shape_cohort_optional_files_default() {
        let b: BundleBody = serde_json::from_str(r#"{"manifest_b64":"e30=","signature_b64":""}"#).unwrap();
        assert!(b.cohort.is_none());
        assert!(b.files.is_empty());
        let b: BundleBody = serde_json::from_str(
            r#"{"manifest_b64":"e30=","signature_b64":"","files":[{"path":"a.yaml","content_b64":"eA=="}],"cohort":"canary"}"#,
        )
        .unwrap();
        assert_eq!(b.cohort.as_deref(), Some("canary"));
        assert_eq!(b.files[0].path, "a.yaml");
        // `null` is absent, not a cohort called "null".
        let b: BundleBody =
            serde_json::from_str(r#"{"manifest_b64":"e30=","signature_b64":"","cohort":null}"#).unwrap();
        assert!(b.cohort.is_none());
    }

    #[test]
    fn keys_from_env_value() {
        assert!(matches!(keys_from(None), Ok(None)));
        assert!(matches!(keys_from(Some("  ")), Ok(None)));
        let vk = SigningKey::from_bytes(&[2u8; 32]).verifying_key();
        // Keys are accepted in hex or base64; this binary has base64 to hand.
        let keys = keys_from(Some(&b64(&vk.to_bytes()))).unwrap().unwrap();
        assert_eq!(keys, vec![vk]);
        // Set-but-junk is an error, not "not configured": the operator meant
        // to configure trust and must hear that it did not take.
        assert!(keys_from(Some("not-a-key")).is_err());
    }

    #[test]
    fn prepare_accepts_a_signed_bundle_and_names_its_workflows() {
        let (body, vk) = signed_body(&[("specs/b.yaml", GOOD_B), ("specs/a.yaml", GOOD_A)]);
        let p = prepare(&body, &[vk]).unwrap();
        assert!(p.bundle.provenance().starts_with("bundle:plant-3/line-a@2026.09.01-3#"));
        let names: Vec<&str> = p.specs.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["nightly_report", "line_health"]);
    }

    #[test]
    fn prepare_refuses_bad_base64_tampering_and_invalid_specs_with_400() {
        // Base64 that is not: the field is named.
        let (mut body, vk) = signed_body(&[("a.yaml", GOOD_A)]);
        body.manifest_b64 = "%%%".into();
        let (status, msg) = prepare(&body, &[vk]).unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(msg.contains("manifest_b64"), "{msg}");
        let (mut body, vk) = signed_body(&[("a.yaml", GOOD_A)]);
        body.files[0].content_b64 = "%%%".into();
        let (_, msg) = prepare(&body, &[vk]).unwrap_err();
        assert!(msg.contains("files[a.yaml].content_b64"), "{msg}");

        // A spec altered after signing.
        let (mut body, vk) = signed_body(&[("a.yaml", GOOD_A)]);
        body.files[0].content_b64 = b64(GOOD_B.as_bytes());
        let (status, msg) = prepare(&body, &[vk]).unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(msg.starts_with("bundle rejected:"), "{msg}");
        assert!(msg.contains("sha256 mismatch"), "{msg}");

        // The wrong trust set.
        let (body, _) = signed_body(&[("a.yaml", GOOD_A)]);
        let other = SigningKey::from_bytes(&[12u8; 32]).verifying_key();
        let (_, msg) = prepare(&body, &[other]).unwrap_err();
        assert!(msg.contains("trusted key"), "{msg}");

        // Correctly signed, but a spec the validator refuses: nothing applies,
        // and the message names the file and the bundle.
        let (body, vk) = signed_body(&[("good.yaml", GOOD_A), ("bad.yaml", BAD_DEP)]);
        let (status, msg) = prepare(&body, &[vk]).unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(msg.contains("nothing applied"), "{msg}");
        assert!(msg.contains("bad.yaml:"), "{msg}");
        assert!(msg.contains("nowhere"), "{msg}");

        // Two files, one workflow name.
        let (body, vk) = signed_body(&[("one.yaml", GOOD_A), ("two.yaml", GOOD_A)]);
        let (_, msg) = prepare(&body, &[vk]).unwrap_err();
        assert!(msg.contains("already defined by one.yaml"), "{msg}");
    }
}
