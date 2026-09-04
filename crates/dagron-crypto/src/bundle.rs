//! Signed workflow bundles — the workflow layer's OTA format.
//!
//! A bundle is a directory (or a JSON body) carrying a `manifest.json`, a
//! detached `manifest.sig` (base64 of a 64-byte ed25519 signature over the
//! **exact manifest bytes** — no canonicalisation step to get wrong), and the
//! workflow specs the manifest lists by relative path and SHA-256. Verification
//! is all-or-nothing and **fails closed**: an unknown format, a missing or extra
//! file, a hash mismatch, a duplicate path, an empty manifest or a signature that
//! matches none of the trusted keys is an error, and no partially-verified
//! bundle is ever returned. There is deliberately no unsigned path.
//!
//! Verification ships in every build: paywalling it would make the open
//! build's default "execute unsigned remote definitions". Staged rollout of a
//! bundle across a fleet (cohorts, canaries, automatic rollback) ships with
//! not in this build; the seam it would use is, and it consumes this
//! same primitive.
//!
//! **API contract.** The signatures below are fixed — three sibling components
//! (the GitOps reconciler, the management API, and the fleet-plane consumers)
//! are written against them concurrently; only additive helpers may be added.
//!
//! **Order of checks.** The signature is checked over the raw manifest bytes
//! *before* the manifest is interpreted, so a bundle nobody trusted never has
//! its JSON, its paths or its hashes acted on. Only then is the manifest parsed
//! and the file set compared against it. The one exception is
//! [`read_bundle_dir`], which has to read the (unverified) listing to know which
//! files to load — it uses it for nothing but file names, refuses every path
//! that could leave the directory, and hands the bytes to [`verify_bundle`],
//! which re-parses after the signature check.
//!
//! **Why ed25519 over raw bytes.** A detached signature over the exact manifest
//! bytes means the verifier needs no canonical JSON, no key ordering rules and
//! no whitespace policy — the thing signed is the thing on disk, byte for byte.
//! The specs are bound to the manifest by content hash, so the signature covers
//! them transitively without signing each file.

use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context, Result};
use base64::Engine as _;
pub use ed25519_dalek::{SigningKey, VerifyingKey};
use ed25519_dalek::{Signature, Signer as _};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The one format string a v1 manifest must carry.
pub const BUNDLE_FORMAT: &str = "dagron.bundle.v1";
/// Comma-separated trusted verifying keys (32-byte, hex or standard base64).
pub const PUBKEYS_ENV: &str = "DAGRON_BUNDLE_PUBKEYS";
/// File names inside a bundle directory.
pub const MANIFEST_FILE: &str = "manifest.json";
pub const SIGNATURE_FILE: &str = "manifest.sig";

/// Length of an ed25519 signature; anything else is refused before decoding.
const SIGNATURE_LEN: usize = 64;
/// Length of an ed25519 verifying key.
const KEY_LEN: usize = 32;
/// Characters of the manifest digest that identify a bundle in provenance
/// strings (`bundle:<name>@<version>#<digest[..12]>`). Twelve hex characters
/// is 48 bits — plenty to tell two versions of one bundle apart, short enough
/// to read in a `created_by` column or a log line.
const PROVENANCE_DIGEST_CHARS: usize = 12;

/// One listed spec: its manifest-relative path and the hex SHA-256 of its bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecEntry {
    pub path: String,
    pub sha256: String,
}

/// The signed manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Must equal [`BUNDLE_FORMAT`].
    pub format: String,
    /// Bundle name (a workflow namespace, e.g. `plant-3/line-a`).
    pub name: String,
    /// Bundle version (free-form, e.g. `2026.09.01-3`); recorded as provenance.
    pub version: String,
    /// RFC 3339 creation time — informational (a unit's clock may be wrong).
    pub created_at: String,
    /// Optional identifier of the signing key, for operators rotating keys.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    /// The specs, each content-addressed. Must be non-empty and free of
    /// duplicate paths.
    pub specs: Vec<SpecEntry>,
}

impl Manifest {
    /// The bytes a signer signs and writes as `manifest.json`: pretty-printed
    /// JSON with a trailing newline. Any serialisation would do — the verifier
    /// checks the exact bytes, whatever they are — but one canonical way to
    /// *produce* them keeps every signer's output diffable in a repository.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = serde_json::to_vec_pretty(self).context("serialising the manifest")?;
        bytes.push(b'\n');
        Ok(bytes)
    }
}

/// `manifest.sig`'s text for a raw signature: standard base64, one line. The
/// inverse of what [`read_bundle_dir`] decodes; here so a signer without a
/// base64 dependency of its own writes exactly what the verifier reads.
pub fn signature_b64(signature: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(signature)
}

/// A bundle that passed every check. `digest` is the hex SHA-256 of the exact
/// manifest bytes and doubles as the bundle id in provenance strings.
#[derive(Debug, Clone)]
pub struct VerifiedBundle {
    pub manifest: Manifest,
    pub manifest_bytes: Vec<u8>,
    /// `(manifest-relative path, spec text)` in manifest order.
    pub specs: Vec<(String, String)>,
    pub digest: String,
}

impl VerifiedBundle {
    /// The provenance string every consumer stamps on what it applies:
    /// `bundle:<name>@<version>#<digest[..12]>`. The GitOps worker and the
    /// management API write it to `workflow_versions.created_by`, and the
    /// fleet-plane consumer reports it in its acknowledgement, so one grep
    /// finds every place a bundle landed.
    pub fn provenance(&self) -> String {
        format!(
            "bundle:{}@{}#{}",
            self.manifest.name,
            self.manifest.version,
            &self.digest[..PROVENANCE_DIGEST_CHARS]
        )
    }
}

/// Hex SHA-256 of `bytes` — the content address a manifest carries per spec,
/// and the bundle digest over the manifest itself. Public so a signer computes
/// exactly what the verifier will compare.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Parse a comma-separated list of 32-byte verifying keys (hex or base64).
/// Empty input is an error: an empty trust set must not read as "trust all".
pub fn parse_pubkeys(raw: &str) -> Result<Vec<VerifyingKey>> {
    let mut keys = Vec::new();
    for (i, item) in raw.split(',').map(str::trim).enumerate() {
        if item.is_empty() {
            continue;
        }
        let bytes = decode_key_material(item)
            .with_context(|| format!("{PUBKEYS_ENV} entry {} is not a 32-byte hex or base64 key", i + 1))?;
        let key = VerifyingKey::from_bytes(&bytes)
            .with_context(|| format!("{PUBKEYS_ENV} entry {} is not a valid ed25519 key", i + 1))?;
        keys.push(key);
    }
    if keys.is_empty() {
        bail!("{PUBKEYS_ENV} names no keys — an empty trust set refuses every bundle rather than trusting all");
    }
    Ok(keys)
}

/// A 32-byte key from its hex (64 chars) or standard-base64 (44 chars, padded)
/// spelling. Both are accepted because both are what key tooling prints; the
/// length pins which one was meant, so nothing is guessed.
fn decode_key_material(item: &str) -> Result<[u8; KEY_LEN]> {
    let bytes = if item.len() == KEY_LEN * 2 && item.bytes().all(|b| b.is_ascii_hexdigit()) {
        hex::decode(item).context("hex decode")?
    } else {
        base64::engine::general_purpose::STANDARD
            .decode(item)
            .context("neither hex nor standard base64")?
    };
    <[u8; KEY_LEN]>::try_from(bytes.as_slice())
        .map_err(|_| anyhow::anyhow!("decoded to {} bytes, expected {KEY_LEN}", bytes.len()))
}

/// The trusted keys from [`PUBKEYS_ENV`]. Unset or empty is an error — there is
/// no unsigned path, and a missing trust set refuses every bundle.
pub fn pubkeys_from_env() -> Result<Vec<VerifyingKey>> {
    match std::env::var(PUBKEYS_ENV) {
        Ok(raw) if !raw.trim().is_empty() => parse_pubkeys(&raw),
        _ => bail!(
            "{PUBKEYS_ENV} is not set — signed bundles are refused until this process is told which keys to trust"
        ),
    }
}

/// Verify `signature` (raw 64 bytes — callers base64-decode `manifest.sig`
/// themselves) over the exact `manifest_bytes` against at least one of `keys`,
/// then check that `files` (`(path, bytes)`) are exactly the manifest's specs
/// with matching SHA-256s. Any failure is an error and nothing is returned.
pub fn verify_bundle(
    manifest_bytes: &[u8],
    signature: &[u8],
    files: &[(String, Vec<u8>)],
    keys: &[VerifyingKey],
) -> Result<VerifiedBundle> {
    // 1. Authenticate the manifest bytes before interpreting a single one.
    if keys.is_empty() {
        bail!("no trusted keys were supplied — refusing to verify against an empty trust set");
    }
    if signature.len() != SIGNATURE_LEN {
        bail!("signature is {} bytes, expected {SIGNATURE_LEN} (raw ed25519 — decode base64 first)", signature.len());
    }
    let signature = Signature::from_slice(signature).context("malformed ed25519 signature")?;
    // `verify_strict` rejects the small-order / non-canonical points a plain
    // `verify` tolerates; a bundle signer never produces those, so refusing
    // them costs nothing and closes the malleability corner.
    let trusted = keys.iter().any(|k| k.verify_strict(manifest_bytes, &signature).is_ok());
    if !trusted {
        bail!(
            "manifest signature matches none of the {} trusted key(s) — the bundle was signed with another key, or the manifest was altered after signing",
            keys.len()
        );
    }

    // 2. Now the manifest is trusted; interpret it.
    let manifest: Manifest = serde_json::from_slice(manifest_bytes).context("parsing the signed manifest")?;
    if manifest.format != BUNDLE_FORMAT {
        bail!("unknown bundle format {:?} (this build understands {BUNDLE_FORMAT:?})", manifest.format);
    }
    if manifest.name.trim().is_empty() || manifest.version.trim().is_empty() {
        bail!("manifest name and version must be non-empty — they are the bundle's provenance");
    }
    if manifest.specs.is_empty() {
        bail!("manifest lists no specs — an empty bundle is refused rather than applied as \"remove everything\"");
    }
    let mut listed: HashMap<&str, &SpecEntry> = HashMap::with_capacity(manifest.specs.len());
    for entry in &manifest.specs {
        check_rel_path(&entry.path).with_context(|| format!("manifest entry {:?}", entry.path))?;
        if entry.sha256.len() != 64 || !entry.sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
            bail!("manifest entry {:?} carries a malformed sha256 (expected 64 hex characters)", entry.path);
        }
        if listed.insert(entry.path.as_str(), entry).is_some() {
            bail!("manifest lists {:?} twice", entry.path);
        }
    }

    // 3. The file set must be exactly the listing — nothing missing, nothing
    //    extra, nothing altered.
    let mut supplied: HashMap<&str, &[u8]> = HashMap::with_capacity(files.len());
    for (path, bytes) in files {
        if supplied.insert(path.as_str(), bytes.as_slice()).is_some() {
            bail!("file {path:?} was supplied twice");
        }
        if !listed.contains_key(path.as_str()) {
            bail!("file {path:?} is not listed in the manifest — an unsigned spec cannot ride along with signed ones");
        }
    }
    let mut specs = Vec::with_capacity(manifest.specs.len());
    for entry in &manifest.specs {
        let Some(bytes) = supplied.get(entry.path.as_str()) else {
            bail!("manifest lists {:?} but the file is missing", entry.path);
        };
        let actual = sha256_hex(bytes);
        if !actual.eq_ignore_ascii_case(&entry.sha256) {
            bail!(
                "sha256 mismatch for {:?}: manifest says {}…, file hashes to {}… — the spec was changed after signing",
                entry.path,
                &entry.sha256[..PROVENANCE_DIGEST_CHARS],
                &actual[..PROVENANCE_DIGEST_CHARS]
            );
        }
        let text = std::str::from_utf8(bytes)
            .with_context(|| format!("spec {:?} is not UTF-8 text", entry.path))?
            .to_string();
        specs.push((entry.path.clone(), text));
    }

    Ok(VerifiedBundle {
        digest: sha256_hex(manifest_bytes),
        manifest,
        manifest_bytes: manifest_bytes.to_vec(),
        specs,
    })
}

/// A manifest-relative spec path that cannot leave the bundle directory:
/// relative, made only of normal components (no `.`, `..`, root or drive
/// prefix), forward slashes only, and named like a spec (`.yaml` / `.yml`) —
/// the same filter the plain GitOps path applies, so a bundle cannot smuggle
/// in a file the unsigned path would never have read.
fn check_rel_path(path: &str) -> Result<()> {
    if path.is_empty() {
        bail!("empty path");
    }
    if path.contains('\0') || path.contains('\\') {
        bail!("path contains a NUL or a backslash");
    }
    let p = Path::new(path);
    if !p.components().all(|c| matches!(c, Component::Normal(_))) {
        bail!("path must be relative and free of `.`/`..` components");
    }
    if !matches!(p.extension().and_then(|e| e.to_str()), Some("yaml") | Some("yml")) {
        bail!("path must name a `.yaml` or `.yml` spec");
    }
    Ok(())
}

/// Sign the exact `manifest_bytes`; returns the raw 64-byte signature (encode
/// it as standard base64 to write `manifest.sig`).
pub fn sign_manifest(manifest_bytes: &[u8], key: &SigningKey) -> Vec<u8> {
    key.sign(manifest_bytes).to_bytes().to_vec()
}

/// Read a bundle directory: the manifest bytes, the decoded signature, and the
/// bytes of every file the manifest lists (and nothing else). Refuses symlinks
/// at every level and never reads a path outside `dir`.
///
/// A `.yaml`/`.yml` under `dir` that the manifest does *not* list is an error
/// too: silently ignoring it would let an unsigned spec sit beside signed ones
/// looking applied while nothing ever read it. Non-spec files (a README, a
/// `.gitignore`) and hidden directories are ignored.
pub fn read_bundle_dir(dir: &Path) -> Result<(Vec<u8>, Vec<u8>, Vec<(String, Vec<u8>)>)> {
    let md = std::fs::symlink_metadata(dir)
        .with_context(|| format!("bundle directory {}", dir.display()))?;
    if md.file_type().is_symlink() {
        bail!("bundle directory {} is a symlink — refusing to follow it", dir.display());
    }
    if !md.is_dir() {
        bail!("{} is not a directory", dir.display());
    }
    let manifest = read_within(dir, MANIFEST_FILE)?;
    let sig_text = read_within(dir, SIGNATURE_FILE)?;
    let signature = base64::engine::general_purpose::STANDARD
        .decode(String::from_utf8_lossy(&sig_text).trim())
        .with_context(|| format!("{SIGNATURE_FILE} is not standard base64"))?;

    // The listing is UNVERIFIED here and is used for file names only;
    // `verify_bundle` re-parses it after the signature check.
    let listing: Manifest =
        serde_json::from_slice(&manifest).with_context(|| format!("parsing {MANIFEST_FILE}"))?;
    let mut files = Vec::with_capacity(listing.specs.len());
    let mut listed: HashSet<String> = HashSet::with_capacity(listing.specs.len());
    for entry in &listing.specs {
        check_rel_path(&entry.path).with_context(|| format!("manifest entry {:?}", entry.path))?;
        if !listed.insert(entry.path.clone()) {
            bail!("{MANIFEST_FILE} lists {:?} twice", entry.path);
        }
        let bytes = read_within(dir, &entry.path)?;
        files.push((entry.path.clone(), bytes));
    }

    let mut present = Vec::new();
    collect_specs(dir, dir, &mut present)
        .with_context(|| format!("scanning {} for specs", dir.display()))?;
    for rel in present {
        if !listed.contains(&rel) {
            bail!("{rel:?} is not listed in {MANIFEST_FILE} — re-sign the bundle with it, or remove it");
        }
    }
    Ok((manifest, signature, files))
}

/// Read `rel` under `dir`, refusing a symlink at any component on the way —
/// `symlink_metadata` does not follow links, so a checkout cannot point a
/// listed spec at `/etc/passwd` and have this read it. `rel` has already
/// passed [`check_rel_path`] (or is one of the two fixed file names).
fn read_within(dir: &Path, rel: &str) -> Result<Vec<u8>> {
    let mut cur = dir.to_path_buf();
    for comp in Path::new(rel).components() {
        let Component::Normal(part) = comp else {
            bail!("{rel:?} is not a plain relative path");
        };
        cur.push(part);
        let md = std::fs::symlink_metadata(&cur).with_context(|| format!("reading {rel:?}"))?;
        if md.file_type().is_symlink() {
            bail!("{rel:?} is (or passes through) a symlink — refusing to follow it");
        }
    }
    let md = std::fs::metadata(&cur).with_context(|| format!("reading {rel:?}"))?;
    if !md.is_file() {
        bail!("{rel:?} is not a regular file");
    }
    std::fs::read(&cur).with_context(|| format!("reading {rel:?}"))
}

/// Every `*.yaml` / `*.yml` under `dir` as a `/`-joined path relative to
/// `root`, skipping symlinks and hidden entries (a checkout's `.git`).
fn collect_specs(root: &Path, dir: &Path, out: &mut Vec<String>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_symlink() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') {
            continue;
        }
        let path: PathBuf = entry.path();
        if path.is_dir() {
            collect_specs(root, &path, out)?;
        } else if matches!(path.extension().and_then(|e| e.to_str()), Some("yaml") | Some("yml")) {
            let rel = path.strip_prefix(root).unwrap_or(&path);
            let joined: Vec<String> =
                rel.components().map(|c| c.as_os_str().to_string_lossy().into_owned()).collect();
            out.push(joined.join("/"));
        }
    }
    Ok(())
}

/// [`read_bundle_dir`] followed by [`verify_bundle`].
pub fn verify_bundle_dir(dir: &Path, keys: &[VerifyingKey]) -> Result<VerifiedBundle> {
    let (manifest, sig, files) = read_bundle_dir(dir)?;
    verify_bundle(&manifest, &sig, &files, keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const SPEC_A: &str = "name: line_health\ntasks:\n  - name: check\n    command: [\"true\"]\n";
    const SPEC_B: &str = "name: nightly\ntasks:\n  - name: report\n    command: [\"true\"]\n";

    fn keypair(seed: u8) -> (SigningKey, VerifyingKey) {
        let sk = SigningKey::from_bytes(&[seed; 32]);
        let vk = sk.verifying_key();
        (sk, vk)
    }

    /// A manifest over `specs` as `(path, text)`, serialised the way a signer
    /// would write it.
    fn manifest_json(specs: &[(&str, &str)]) -> Vec<u8> {
        let m = Manifest {
            format: BUNDLE_FORMAT.into(),
            name: "plant-3/line-a".into(),
            version: "2026.09.01-3".into(),
            created_at: "2026-09-01T00:00:00Z".into(),
            key_id: Some("ops-2026".into()),
            specs: specs
                .iter()
                .map(|(p, t)| SpecEntry { path: p.to_string(), sha256: sha256_hex(t.as_bytes()) })
                .collect(),
        };
        serde_json::to_vec_pretty(&m).unwrap()
    }

    fn files_of(specs: &[(&str, &str)]) -> Vec<(String, Vec<u8>)> {
        specs.iter().map(|(p, t)| (p.to_string(), t.as_bytes().to_vec())).collect()
    }

    /// Sign a two-spec bundle with key 7: `(manifest, signature, files, keys)`.
    fn signed() -> (Vec<u8>, Vec<u8>, Vec<(String, Vec<u8>)>, Vec<VerifyingKey>) {
        let (sk, vk) = keypair(7);
        let specs = [("specs/line_health.yaml", SPEC_A), ("specs/nightly.yaml", SPEC_B)];
        let manifest = manifest_json(&specs);
        let sig = sign_manifest(&manifest, &sk);
        (manifest, sig, files_of(&specs), vec![vk])
    }

    #[test]
    fn happy_path_returns_specs_in_manifest_order_with_digest_and_provenance() {
        let (manifest, sig, files, keys) = signed();
        let b = verify_bundle(&manifest, &sig, &files, &keys).unwrap();
        assert_eq!(b.manifest.name, "plant-3/line-a");
        assert_eq!(b.manifest.key_id.as_deref(), Some("ops-2026"));
        assert_eq!(b.specs.len(), 2);
        assert_eq!(b.specs[0], ("specs/line_health.yaml".to_string(), SPEC_A.to_string()));
        assert_eq!(b.specs[1].0, "specs/nightly.yaml");
        assert_eq!(b.manifest_bytes, manifest);
        assert_eq!(b.digest, sha256_hex(&manifest));
        assert_eq!(b.digest.len(), 64);
        assert_eq!(b.provenance(), format!("bundle:plant-3/line-a@2026.09.01-3#{}", &b.digest[..12]));
        // Same inputs, same digest: the digest is content-addressed, not random.
        assert_eq!(verify_bundle(&manifest, &sig, &files, &keys).unwrap().digest, b.digest);
    }

    /// File order in the request is irrelevant; manifest order is what comes
    /// back — a consumer applying in order gets a deterministic sequence.
    #[test]
    fn file_order_does_not_matter() {
        let (manifest, sig, mut files, keys) = signed();
        files.reverse();
        let b = verify_bundle(&manifest, &sig, &files, &keys).unwrap();
        assert_eq!(b.specs[0].0, "specs/line_health.yaml");
    }

    #[test]
    fn a_tampered_manifest_fails_the_signature() {
        let (mut manifest, sig, files, keys) = signed();
        // Flip one byte inside the JSON — still valid JSON if it lands in a
        // string, still refused because the signature is over the bytes.
        let idx = manifest.iter().position(|b| *b == b'3').unwrap();
        manifest[idx] = b'4';
        let err = verify_bundle(&manifest, &sig, &files, &keys).unwrap_err().to_string();
        assert!(err.contains("signature"), "{err}");
        assert!(err.contains("trusted key"), "{err}");
    }

    #[test]
    fn a_tampered_spec_fails_its_hash() {
        let (manifest, sig, mut files, keys) = signed();
        files[0].1.extend_from_slice(b"  - name: extra\n    command: [\"rm\", \"-rf\", \"/\"]\n");
        let err = verify_bundle(&manifest, &sig, &files, &keys).unwrap_err().to_string();
        assert!(err.contains("sha256 mismatch"), "{err}");
        assert!(err.contains("specs/line_health.yaml"), "{err}");
    }

    #[test]
    fn a_missing_spec_is_refused() {
        let (manifest, sig, mut files, keys) = signed();
        files.pop();
        let err = verify_bundle(&manifest, &sig, &files, &keys).unwrap_err().to_string();
        assert!(err.contains("missing"), "{err}");
        assert!(err.contains("specs/nightly.yaml"), "{err}");
    }

    #[test]
    fn an_extra_file_is_refused() {
        let (manifest, sig, mut files, keys) = signed();
        files.push(("specs/rogue.yaml".into(), SPEC_A.as_bytes().to_vec()));
        let err = verify_bundle(&manifest, &sig, &files, &keys).unwrap_err().to_string();
        assert!(err.contains("not listed"), "{err}");
        assert!(err.contains("rogue.yaml"), "{err}");
    }

    #[test]
    fn a_file_supplied_twice_is_refused() {
        let (manifest, sig, mut files, keys) = signed();
        files.push(files[0].clone());
        let err = verify_bundle(&manifest, &sig, &files, &keys).unwrap_err().to_string();
        assert!(err.contains("supplied twice"), "{err}");
    }

    #[test]
    fn a_duplicate_manifest_path_is_refused_even_when_signed() {
        let (sk, vk) = keypair(7);
        let specs = [("a.yaml", SPEC_A), ("a.yaml", SPEC_A)];
        let manifest = manifest_json(&specs);
        let sig = sign_manifest(&manifest, &sk);
        let files = vec![("a.yaml".to_string(), SPEC_A.as_bytes().to_vec())];
        let err = verify_bundle(&manifest, &sig, &files, &[vk]).unwrap_err().to_string();
        assert!(err.contains("twice"), "{err}");
    }

    #[test]
    fn an_empty_manifest_is_refused_even_when_signed() {
        let (sk, vk) = keypair(7);
        let manifest = manifest_json(&[]);
        let sig = sign_manifest(&manifest, &sk);
        let err = verify_bundle(&manifest, &sig, &[], &[vk]).unwrap_err().to_string();
        assert!(err.contains("no specs"), "{err}");
    }

    #[test]
    fn the_wrong_key_is_refused() {
        let (manifest, sig, files, _) = signed();
        let (_, other) = keypair(9);
        let err = verify_bundle(&manifest, &sig, &files, &[other]).unwrap_err().to_string();
        assert!(err.contains("none of the 1 trusted key"), "{err}");
        // …but a trust set that *contains* the signer is enough.
        let (_, right) = keypair(7);
        assert!(verify_bundle(&manifest, &sig, &files, &[other, right]).is_ok());
    }

    #[test]
    fn an_empty_key_list_is_refused_not_trusted() {
        let (manifest, sig, files, _) = signed();
        let err = verify_bundle(&manifest, &sig, &files, &[]).unwrap_err().to_string();
        assert!(err.contains("empty trust set"), "{err}");
    }

    #[test]
    fn an_unknown_format_is_refused_even_when_signed() {
        let (sk, vk) = keypair(7);
        let specs = [("a.yaml", SPEC_A)];
        let text = String::from_utf8(manifest_json(&specs)).unwrap();
        let manifest = text.replace(BUNDLE_FORMAT, "dagron.bundle.v2").into_bytes();
        let sig = sign_manifest(&manifest, &sk);
        let err = verify_bundle(&manifest, &sig, &files_of(&specs), &[vk]).unwrap_err().to_string();
        assert!(err.contains("unknown bundle format"), "{err}");
        assert!(err.contains("dagron.bundle.v2"), "{err}");
    }

    #[test]
    fn a_signature_of_the_wrong_length_is_refused_before_decoding() {
        let (manifest, sig, files, keys) = signed();
        let err = verify_bundle(&manifest, &sig[..63], &files, &keys).unwrap_err().to_string();
        assert!(err.contains("63 bytes"), "{err}");
        let mut long = sig.clone();
        long.push(0);
        assert!(verify_bundle(&manifest, &long, &files, &keys).is_err());
        // A signature that is the right length but garbage is refused too.
        assert!(verify_bundle(&manifest, &[0u8; 64], &files, &keys).is_err());
    }

    #[test]
    fn paths_that_could_leave_the_bundle_are_refused_even_when_signed() {
        let (sk, vk) = keypair(7);
        for bad in ["../up.yaml", "/etc/x.yaml", "a/../b.yaml", "./a.yaml", "a\\b.yaml", "", "notes.txt"] {
            let specs = [(bad, SPEC_A)];
            let manifest = manifest_json(&specs);
            let sig = sign_manifest(&manifest, &sk);
            let err = verify_bundle(&manifest, &sig, &files_of(&specs), &[vk]).unwrap_err().to_string();
            assert!(err.contains("manifest entry"), "{bad:?}: {err}");
        }
    }

    #[test]
    fn a_spec_that_is_not_utf8_is_refused() {
        let (sk, vk) = keypair(7);
        let bytes = [0xffu8, 0xfe, b'x'];
        let m = Manifest {
            format: BUNDLE_FORMAT.into(),
            name: "n".into(),
            version: "v".into(),
            created_at: String::new(),
            key_id: None,
            specs: vec![SpecEntry { path: "a.yaml".into(), sha256: sha256_hex(&bytes) }],
        };
        let manifest = serde_json::to_vec(&m).unwrap();
        let sig = sign_manifest(&manifest, &sk);
        let err = verify_bundle(&manifest, &sig, &[("a.yaml".into(), bytes.to_vec())], &[vk])
            .unwrap_err()
            .to_string();
        assert!(err.contains("UTF-8"), "{err}");
    }

    #[test]
    fn a_blank_name_or_version_is_refused() {
        let (sk, vk) = keypair(7);
        let specs = [("a.yaml", SPEC_A)];
        let text = String::from_utf8(manifest_json(&specs)).unwrap();
        let manifest = text.replace("\"2026.09.01-3\"", "\"  \"").into_bytes();
        let sig = sign_manifest(&manifest, &sk);
        let err = verify_bundle(&manifest, &sig, &files_of(&specs), &[vk]).unwrap_err().to_string();
        assert!(err.contains("non-empty"), "{err}");
    }

    #[test]
    fn pubkeys_parse_hex_and_base64_and_refuse_junk() {
        let (_, vk) = keypair(7);
        let hexed = hex::encode(vk.to_bytes());
        let b64 = base64::engine::general_purpose::STANDARD.encode(vk.to_bytes());
        let keys = parse_pubkeys(&format!(" {hexed} , {b64},, ")).unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], vk);
        assert_eq!(keys[1], vk);

        // Empty is an error, never "trust all".
        assert!(parse_pubkeys("").unwrap_err().to_string().contains("no keys"));
        assert!(parse_pubkeys(" , ").is_err());
        // Wrong length, wrong alphabet, and a hex string of the wrong size all fail
        // naming the entry.
        assert!(parse_pubkeys("zz").unwrap_err().to_string().contains("entry 1"));
        assert!(parse_pubkeys(&hexed[..62]).is_err());
        assert!(parse_pubkeys(&format!("{hexed},abc")).unwrap_err().to_string().contains("entry 2"));
        // 32 bytes that are not a curve point are refused by the key constructor
        // (`0x02` repeated does not decompress; most byte patterns do, so the
        // value is deliberate).
        let err = parse_pubkeys(&hex::encode([0x02u8; 32])).unwrap_err().to_string();
        assert!(err.contains("not a valid ed25519 key"), "{err}");
    }

    /// `PUBKEYS_ENV` is process-global, so this test owns it for its duration
    /// and restores whatever it found; no other test here reads it.
    #[test]
    fn pubkeys_from_env_refuses_unset_and_blank() {
        let previous = std::env::var(PUBKEYS_ENV).ok();
        std::env::remove_var(PUBKEYS_ENV);
        let err = pubkeys_from_env().unwrap_err().to_string();
        assert!(err.contains(PUBKEYS_ENV), "{err}");
        std::env::set_var(PUBKEYS_ENV, "   ");
        assert!(pubkeys_from_env().is_err());
        let (_, vk) = keypair(3);
        std::env::set_var(PUBKEYS_ENV, hex::encode(vk.to_bytes()));
        assert_eq!(pubkeys_from_env().unwrap(), vec![vk]);
        match previous {
            Some(v) => std::env::set_var(PUBKEYS_ENV, v),
            None => std::env::remove_var(PUBKEYS_ENV),
        }
    }

    #[test]
    fn manifest_serde_omits_an_absent_key_id() {
        let m = Manifest {
            format: BUNDLE_FORMAT.into(),
            name: "n".into(),
            version: "v".into(),
            created_at: "2026-09-01T00:00:00Z".into(),
            key_id: None,
            specs: vec![SpecEntry { path: "a.yaml".into(), sha256: "00".repeat(32) }],
        };
        let json = serde_json::to_string(&m).unwrap();
        assert!(!json.contains("key_id"), "{json}");
        let back: Manifest = serde_json::from_str(&json).unwrap();
        assert_eq!(back, m);
    }

    // ── directory flavour ────────────────────────────────────────────────────

    /// A scratch directory removed on drop. No `tempfile` dependency in this
    /// crate, so the name is made unique by pid + a counter.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new() -> Self {
            static N: AtomicUsize = AtomicUsize::new(0);
            let dir = std::env::temp_dir().join(format!(
                "dagron-bundle-test-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).unwrap();
            Scratch(dir)
        }
        fn write(&self, rel: &str, bytes: &[u8]) {
            let p = self.0.join(rel);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, bytes).unwrap();
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Lay a signed bundle out on disk exactly as `bundle_sign` writes it.
    fn signed_dir() -> (Scratch, Vec<VerifyingKey>) {
        let (manifest, sig, files, keys) = signed();
        let s = Scratch::new();
        s.write(MANIFEST_FILE, &manifest);
        s.write(SIGNATURE_FILE, format!("{}\n", base64::engine::general_purpose::STANDARD.encode(&sig)).as_bytes());
        for (p, b) in &files {
            s.write(p, b);
        }
        (s, keys)
    }

    #[test]
    fn a_bundle_directory_round_trips() {
        let (s, keys) = signed_dir();
        // A README and a hidden directory beside the bundle are not specs and
        // do not disturb it.
        s.write("README.md", b"# how to sign\n");
        s.write(".git/config", b"[core]\n");
        let b = verify_bundle_dir(&s.0, &keys).unwrap();
        assert_eq!(b.specs.len(), 2);
        assert_eq!(b.specs[0].0, "specs/line_health.yaml");
    }

    #[test]
    fn a_directory_missing_its_signature_or_manifest_is_refused() {
        let (s, keys) = signed_dir();
        std::fs::remove_file(s.0.join(SIGNATURE_FILE)).unwrap();
        let err = verify_bundle_dir(&s.0, &keys).unwrap_err().to_string();
        assert!(err.contains(SIGNATURE_FILE), "{err}");
        let (s, keys) = signed_dir();
        std::fs::remove_file(s.0.join(MANIFEST_FILE)).unwrap();
        assert!(verify_bundle_dir(&s.0, &keys).is_err());
        let (s, keys) = signed_dir();
        s.write(SIGNATURE_FILE, b"not base64 !!!\n");
        let err = verify_bundle_dir(&s.0, &keys).unwrap_err().to_string();
        assert!(err.contains("base64"), "{err}");
    }

    #[test]
    fn an_unlisted_spec_beside_the_manifest_is_refused() {
        let (s, keys) = signed_dir();
        s.write("specs/unsigned.yaml", SPEC_A.as_bytes());
        let err = verify_bundle_dir(&s.0, &keys).unwrap_err().to_string();
        assert!(err.contains("not listed"), "{err}");
        assert!(err.contains("specs/unsigned.yaml"), "{err}");
    }

    #[test]
    fn a_listed_spec_that_is_changed_on_disk_is_refused() {
        let (s, keys) = signed_dir();
        s.write("specs/nightly.yaml", b"name: nightly\ntasks:\n  - name: evil\n    command: [\"true\"]\n");
        let err = verify_bundle_dir(&s.0, &keys).unwrap_err().to_string();
        assert!(err.contains("sha256 mismatch"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_are_refused_at_every_level() {
        // A listed spec that is a symlink — even to a file with the right
        // content — is refused: the bytes may match today and point elsewhere
        // tomorrow.
        let (s, keys) = signed_dir();
        let target = s.0.join("elsewhere.txt");
        std::fs::write(&target, SPEC_B.as_bytes()).unwrap();
        std::fs::remove_file(s.0.join("specs/nightly.yaml")).unwrap();
        std::os::unix::fs::symlink(&target, s.0.join("specs/nightly.yaml")).unwrap();
        let err = verify_bundle_dir(&s.0, &keys).unwrap_err().to_string();
        assert!(err.contains("symlink"), "{err}");

        // A symlinked directory on the way to a spec.
        let (s, keys) = signed_dir();
        let real = s.0.join("real-specs");
        std::fs::rename(s.0.join("specs"), &real).unwrap();
        std::os::unix::fs::symlink(&real, s.0.join("specs")).unwrap();
        let err = verify_bundle_dir(&s.0, &keys).unwrap_err().to_string();
        assert!(err.contains("symlink"), "{err}");

        // The bundle directory itself.
        let (s, keys) = signed_dir();
        let link = Scratch::new();
        let link_path = link.0.join("bundle");
        std::os::unix::fs::symlink(&s.0, &link_path).unwrap();
        let err = verify_bundle_dir(&link_path, &keys).unwrap_err().to_string();
        assert!(err.contains("symlink"), "{err}");
    }
}
