//! **Run attestation** — a signed, content-addressed record of what a run
//! actually executed, and the comparison that turns two such records into a
//! statement about determinism.
//!
//! The gap this closes is stated plainly in `docs/FAMILIES.md` (family 5): a
//! finished dagron run leaves rows in a database that anyone with write access
//! can edit, and no artefact that survives the datastore. For settlement,
//! regulatory reporting, compliance evidence and audit-grade change management,
//! "the run succeeded" is worth nothing unless you can also say *what ran* —
//! which spec, which argv, which image, under which trust envelope — and prove
//! the answer was not written after the fact.
//!
//! An [`Attestation`] is that answer: one canonical JSON statement per run,
//! digested with SHA-256 and signed with the same ed25519 machinery the
//! [`crate::bundle`] verifier already uses, so an operator has one trust set
//! and one key-rotation story rather than two.
//!
//! ## What it proves, and what it does not
//!
//! This is deliberately narrow, because the wide claim would be false:
//!
//! * It **does** prove that a specific spec digest, argv, image, environment
//!   shape and isolation envelope are what the engine dispatched, that the
//!   recorded outputs hash to what is stated, and that nobody edited the record
//!   afterwards without the signing key.
//! * It **does not** make user code deterministic, and no orchestrator can. A
//!   task that reads the wall clock or a live API will differ between runs no
//!   matter what is signed.
//!
//! So "deterministic replay" is expressed as a *measurement* rather than a
//! guarantee: [`replay_diff`] compares two attestations of the same spec and
//! classifies every divergence as either **input drift** (the two runs were not
//! given the same thing to do — the comparison is void) or **output drift**
//! (identical inputs, different results — the task is nondeterministic, and now
//! you know which one). A compliance story built on the first being empty and
//! the second being explainable is one that survives an auditor; a bare claim
//! of reproducibility is not.
//!
//! ## The chain
//!
//! Each attestation carries `prev`: the digest of the previous attestation from
//! the same engine. That makes the sequence tamper-evident as a whole and not
//! merely per-record — deleting a run from the middle breaks every link after
//! it, which is exactly the property an evidence collector for an air-gapped
//! estate needs. [`verify_chain`] checks it.
//!
//! ## Values are never recorded
//!
//! Environment variables are digested, never stored: an attestation is designed
//! to be shipped off the host to an auditor, and a record that leaks a database
//! DSN or an API token is one nobody may forward. The digest still detects that
//! a value changed between two runs, which is all the comparison needs.

use anyhow::{bail, Context, Result};
use base64::Engine as _;
use ed25519_dalek::{Signature, Signer as _, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};

use crate::bundle::sha256_hex;

/// The one format string a v1 attestation must carry.
pub const ATTESTATION_FORMAT: &str = "dagron.attestation.v1";
/// Comma-separated trusted verifying keys for attestations (32-byte, hex or
/// standard base64). A separate variable from `DAGRON_BUNDLE_PUBKEYS` on
/// purpose: the key that *authors* workflows and the key that *witnesses* runs
/// are different roles, and an estate that separates them must be able to.
pub const PUBKEYS_ENV: &str = "DAGRON_ATTEST_PUBKEYS";

/// Length of an ed25519 signature; anything else is refused before decoding.
const SIGNATURE_LEN: usize = 64;

/// What one task actually did.
///
/// Every field is either a digest or a small, non-secret scalar, so the whole
/// record is safe to forward to a party who may not see the workload's data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRecord {
    /// Task name as it appears in the run — fan-out instances included
    /// (`shard.3`), because the attestation records what ran, not what was
    /// written.
    pub name: String,
    /// Terminal status: `succeeded`, `failed`, `skipped`, …
    pub status: String,
    /// Attempts consumed, including the one that succeeded.
    pub attempts: u32,
    /// Exit code of the final attempt, when the executor reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// SHA-256 over the argv (see [`command_digest`]).
    pub command_digest: String,
    /// Container image as dispatched. Prefer a digest-pinned reference
    /// (`repo@sha256:…`); a mutable tag is recorded faithfully but attests to
    /// much less, and [`replay_diff`] cannot see behind it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// SHA-256 over the environment's names and values (see [`env_digest`]).
    /// Values never appear here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_digest: Option<String>,
    /// SHA-256 over the effective trust envelope this task ran under — the
    /// family-3 seam. Two runs whose isolation digests differ did not run under
    /// the same privileges, however identical their output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub isolation_digest: Option<String>,
    /// SHA-256 over the task's recorded output. `None` when nothing was
    /// captured — which is itself worth attesting, since an absent output
    /// cannot later be claimed to have matched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_digest: Option<String>,
    /// RFC 3339 start/finish of the final attempt. Informational: these are the
    /// engine's clock, and [`replay_diff`] never compares them — two runs of the
    /// same work necessarily differ here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
}

/// The signed statement about one run.
///
/// Field order is the canonical serialisation order — see [`Attestation::to_bytes`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attestation {
    /// Must equal [`ATTESTATION_FORMAT`].
    pub format: String,
    /// The run this attests to.
    pub run_id: String,
    /// Workflow name at dispatch time.
    pub workflow: String,
    /// SHA-256 of the exact spec bytes the run was created from. This is the
    /// join to [`crate::bundle`]: a spec that arrived in a signed bundle has
    /// the same digest in that bundle's manifest, so "who authorised this
    /// workflow" and "what did this run execute" answer with one hash.
    pub spec_digest: String,
    /// Terminal run status.
    pub status: String,
    /// RFC 3339 run start/finish (engine clock).
    pub started_at: String,
    pub finished_at: String,
    /// Version of the engine that ran it.
    pub engine_version: String,
    /// Executor the tasks were dispatched through (`local`, `docker`, `k8s`).
    pub executor: String,
    /// Every task, in a stable order (see [`Attestation::new`]).
    pub tasks: Vec<TaskRecord>,
    /// Digest of the previous attestation from this engine, or `None` for the
    /// first link. See [`verify_chain`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev: Option<String>,
    /// Identifier of the signing key, for operators rotating keys.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
}

impl Attestation {
    /// Build a v1 attestation, sorting `tasks` by name.
    ///
    /// The sort is what makes the digest a function of the run rather than of
    /// the order rows happened to come back from a query — two engines
    /// attesting the same run must produce the same bytes, and a `LIMIT`
    /// without an `ORDER BY` somewhere upstream must not be able to change a
    /// signature.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_id: impl Into<String>,
        workflow: impl Into<String>,
        spec_digest: impl Into<String>,
        status: impl Into<String>,
        started_at: impl Into<String>,
        finished_at: impl Into<String>,
        engine_version: impl Into<String>,
        executor: impl Into<String>,
        mut tasks: Vec<TaskRecord>,
    ) -> Self {
        tasks.sort_by(|a, b| a.name.cmp(&b.name));
        Self {
            format: ATTESTATION_FORMAT.to_string(),
            run_id: run_id.into(),
            workflow: workflow.into(),
            spec_digest: spec_digest.into(),
            status: status.into(),
            started_at: started_at.into(),
            finished_at: finished_at.into(),
            engine_version: engine_version.into(),
            executor: executor.into(),
            tasks,
            prev: None,
            key_id: None,
        }
    }

    /// Link this attestation onto `prev_digest`.
    pub fn chained_to(mut self, prev_digest: impl Into<String>) -> Self {
        self.prev = Some(prev_digest.into());
        self
    }

    /// Record which key signed it.
    pub fn with_key_id(mut self, key_id: impl Into<String>) -> Self {
        self.key_id = Some(key_id.into());
        self
    }

    /// The exact bytes that are digested and signed: compact JSON in struct
    /// field order, with a trailing newline.
    ///
    /// Compact rather than pretty, unlike a bundle manifest, because an
    /// attestation is machine-to-machine evidence appended to a log — not a
    /// file a human reviews in a pull request. Determinism comes from the
    /// struct itself: every field is a scalar or a sequence, there is no map
    /// whose iteration order could vary, and `serde` emits fields in
    /// declaration order.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = serde_json::to_vec(self).context("serialising the attestation")?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    /// Hex SHA-256 of [`Attestation::to_bytes`] — this record's identity, and
    /// what the *next* record carries as `prev`.
    pub fn digest(&self) -> Result<String> {
        Ok(sha256_hex(&self.to_bytes()?))
    }

    /// Sign the canonical bytes, returning `(bytes, raw 64-byte signature)`.
    ///
    /// The bytes come back with the signature because a verifier checks the
    /// signature over *exactly* those bytes; re-serialising on the far side and
    /// hoping the two agree is the classic way a canonical-form scheme quietly
    /// stops being one.
    pub fn sign(&self, key: &SigningKey) -> Result<(Vec<u8>, [u8; SIGNATURE_LEN])> {
        let bytes = self.to_bytes()?;
        let sig = key.sign(&bytes);
        Ok((bytes, sig.to_bytes()))
    }

    /// Reject anything structurally unusable before any field is trusted: a
    /// format this verifier does not understand, or a duplicate task name.
    ///
    /// Duplicates matter because every comparison keys tasks by name. Two
    /// records both called `extract` would collapse into one, and
    /// [`replay_diff`] could then report no divergence between runs whose task
    /// sets genuinely differ — a blind spot an evidence primitive must not
    /// have. The module docs say an attestation may arrive from another
    /// producer, so this cannot be left to the producer's good manners.
    fn check_format(&self) -> Result<()> {
        if self.format != ATTESTATION_FORMAT {
            bail!(
                "unsupported attestation format '{}' (expected '{}')",
                self.format,
                ATTESTATION_FORMAT
            );
        }
        if let Some(name) = first_duplicate(&self.tasks) {
            bail!(
                "attestation lists task '{name}' more than once — task names key every \
                 comparison, so a duplicate would silently collapse two records into one"
            );
        }
        Ok(())
    }
}

/// The first task name that appears more than once, if any.
///
/// `tasks` is sorted by name in [`Attestation::new`], but a record deserialised
/// from another producer's bytes may not be, so this does not assume order.
fn first_duplicate(tasks: &[TaskRecord]) -> Option<&str> {
    let mut seen = std::collections::BTreeSet::new();
    tasks
        .iter()
        .find(|t| !seen.insert(t.name.as_str()))
        .map(|t| t.name.as_str())
}

/// Base64 for an attestation signature, matching [`crate::bundle::signature_b64`]
/// so a store holding both kinds of evidence holds them the same way.
pub fn signature_b64(signature: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(signature)
}

/// Append `field` to `buf` as an 8-byte little-endian length followed by its
/// bytes.
///
/// Every digest below is taken over a sequence of caller-supplied strings, and
/// **no separator byte is safe**: a delimiter that can occur inside a field
/// lets two different inputs encode identically. Space fails on `["a b"]` vs
/// `["a", "b"]`; `=` fails on `("A", "B=2")` vs `("A=B", "2")`; even NUL fails,
/// because a Rust `String` may legally contain one and a task's environment is
/// not validated against that. Length-prefixing has no such case — the decoder
/// never has to guess where a field ends — which is what a digest that anchors
/// an audit record has to be able to say.
fn push_len_prefixed(buf: &mut Vec<u8>, field: &str) {
    buf.extend_from_slice(&(field.len() as u64).to_le_bytes());
    buf.extend_from_slice(field.as_bytes());
}

/// SHA-256 over an argv, length-prefixed per argument (see [`push_len_prefixed`]),
/// so `["a b"]` and `["a", "b"]` — different commands — cannot collide.
pub fn command_digest(argv: &[String]) -> String {
    let mut buf: Vec<u8> = Vec::new();
    for arg in argv {
        push_len_prefixed(&mut buf, arg);
    }
    sha256_hex(&buf)
}

/// SHA-256 over an environment: every name and value, length-prefixed, sorted
/// by name.
///
/// Both halves are covered, so the digest changes when a *value* changes even
/// though no value is recoverable from it. Sorted because dispatch order is not
/// a property of the environment.
pub fn env_digest(env: &[(String, String)]) -> String {
    let mut pairs: Vec<&(String, String)> = env.iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let mut buf: Vec<u8> = Vec::new();
    for (name, value) in pairs {
        push_len_prefixed(&mut buf, name);
        push_len_prefixed(&mut buf, value);
    }
    sha256_hex(&buf)
}

/// An attestation whose signature checked out against a trusted key.
#[derive(Debug, Clone)]
pub struct VerifiedAttestation {
    pub attestation: Attestation,
    /// The exact bytes the signature covered.
    pub bytes: Vec<u8>,
    /// Hex SHA-256 of `bytes`.
    pub digest: String,
}

/// Verify `signature` (raw 64 bytes) over `bytes` against at least one of
/// `keys`, then parse. Any failure is an error and nothing is returned.
///
/// Parsing happens *after* verification, and the parsed value is never
/// re-serialised for comparison: the digest is taken over the bytes that were
/// actually signed, so a record that round-trips imperfectly cannot be laundered
/// into a different digest.
pub fn verify(bytes: &[u8], signature: &[u8], keys: &[VerifyingKey]) -> Result<VerifiedAttestation> {
    if keys.is_empty() {
        bail!("no trusted attestation keys — refusing to verify against an empty trust set");
    }
    if signature.len() != SIGNATURE_LEN {
        bail!(
            "attestation signature is {} bytes, expected {}",
            signature.len(),
            SIGNATURE_LEN
        );
    }
    let mut raw = [0u8; SIGNATURE_LEN];
    raw.copy_from_slice(signature);
    let sig = Signature::from_bytes(&raw);

    if !keys.iter().any(|k| k.verify_strict(bytes, &sig).is_ok()) {
        bail!("attestation signature does not verify against any trusted key");
    }

    let attestation: Attestation =
        serde_json::from_slice(bytes).context("parsing the attestation")?;
    attestation.check_format()?;

    Ok(VerifiedAttestation {
        attestation,
        digest: sha256_hex(bytes),
        bytes: bytes.to_vec(),
    })
}

/// The trusted keys from [`PUBKEYS_ENV`]. Unset or empty is an error: a missing
/// trust set must refuse every attestation, never accept all of them.
pub fn pubkeys_from_env() -> Result<Vec<VerifyingKey>> {
    let raw = std::env::var(PUBKEYS_ENV)
        .with_context(|| format!("{PUBKEYS_ENV} is not set — no trusted attestation keys"))?;
    crate::bundle::parse_pubkeys(&raw)
        .with_context(|| format!("parsing {PUBKEYS_ENV}"))
}

/// Check that `chain` is a contiguous hash chain, oldest first.
///
/// Each record's `prev` must be the digest of the one before it; the first may
/// carry `None` (a genesis link) or a digest naming a record outside the slice
/// (a window of a longer chain), and either is accepted — what is refused is a
/// *break* between two records that claim to be adjacent.
pub fn verify_chain(chain: &[VerifiedAttestation]) -> Result<()> {
    for pair in chain.windows(2) {
        let (prev, next) = (&pair[0], &pair[1]);
        match &next.attestation.prev {
            Some(p) if *p == prev.digest => {}
            Some(p) => bail!(
                "attestation chain broken at run '{}': prev is {} but the preceding record \
                 (run '{}') digests to {}",
                next.attestation.run_id,
                p,
                prev.attestation.run_id,
                prev.digest
            ),
            None => bail!(
                "attestation chain broken at run '{}': it claims to be a genesis record but \
                 follows run '{}'",
                next.attestation.run_id,
                prev.attestation.run_id
            ),
        }
    }
    Ok(())
}

/// One way two attestations of the same workflow disagreed.
///
/// Split into input and output drift because they mean opposite things, and a
/// report that mixes them is unreadable: input drift voids the comparison,
/// output drift *is* the finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Divergence {
    /// The two runs did not execute the same spec — nothing below is comparable.
    SpecDigest { a: String, b: String },
    /// A task present in one run and not the other.
    TaskOnlyIn { run: WhichRun, task: String },
    /// Same task, different argv / image / environment / trust envelope: the
    /// two runs were not asked to do the same thing.
    InputDrift {
        task: String,
        field: &'static str,
        a: String,
        b: String,
    },
    /// Same inputs, different result. This is nondeterminism in the workload,
    /// and it is the only class of finding this comparison exists to surface.
    OutputDrift {
        task: String,
        field: &'static str,
        a: String,
        b: String,
    },
    /// A record lists one task name twice, so it cannot be compared at all.
    ///
    /// [`verify`] refuses such a record outright; this exists because
    /// [`replay_diff`] takes an `Attestation` directly and may be handed one
    /// that never passed through verification.
    DuplicateTask { run: WhichRun, task: String },
}

/// Which of the two compared runs a divergence points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WhichRun {
    A,
    B,
}

impl Divergence {
    /// True when this divergence means the two runs were given different work,
    /// so any output comparison between them is meaningless.
    pub fn is_input_drift(&self) -> bool {
        matches!(
            self,
            Divergence::SpecDigest { .. }
                | Divergence::TaskOnlyIn { .. }
                | Divergence::InputDrift { .. }
                // A record that cannot be compared voids the comparison for the
                // same reason the others do.
                | Divergence::DuplicateTask { .. }
        )
    }
}

/// Compare two attestations of (nominally) the same work.
///
/// Timestamps, run ids, attempt counts and chain links are deliberately not
/// compared: they differ between any two runs by construction, and reporting
/// them would bury the one difference that matters. Attempts are excluded for
/// the same reason a retry is not a correctness signal — a task that succeeded
/// on attempt 2 ran the same command as one that succeeded on attempt 1.
///
/// An empty result means: same spec, same task set, same commands, images,
/// environments and trust envelopes, and the same output digests. That is the
/// strongest reproducibility statement an orchestrator is in a position to make.
pub fn replay_diff(a: &Attestation, b: &Attestation) -> Vec<Divergence> {
    let mut out = Vec::new();

    if a.spec_digest != b.spec_digest {
        out.push(Divergence::SpecDigest {
            a: a.spec_digest.clone(),
            b: b.spec_digest.clone(),
        });
    }

    // A duplicate name would silently collapse under the index below, so say so
    // and stop: two records claiming to be the same task cannot be compared, and
    // reporting "no divergence" for them would be worse than reporting nothing.
    for (which, att) in [(WhichRun::A, a), (WhichRun::B, b)] {
        if let Some(dup) = first_duplicate(&att.tasks) {
            out.push(Divergence::DuplicateTask {
                run: which,
                task: dup.to_string(),
            });
        }
    }
    if out.iter().any(|d| matches!(d, Divergence::DuplicateTask { .. })) {
        return out;
    }

    // `tasks` is sorted by name in `new`, but an attestation may also arrive
    // deserialised from bytes some other producer wrote, so index rather than
    // assume.
    let index = |t: &[TaskRecord]| -> std::collections::BTreeMap<String, TaskRecord> {
        t.iter().map(|r| (r.name.clone(), r.clone())).collect()
    };
    let (ia, ib) = (index(&a.tasks), index(&b.tasks));

    for name in ia.keys() {
        if !ib.contains_key(name) {
            out.push(Divergence::TaskOnlyIn {
                run: WhichRun::A,
                task: name.clone(),
            });
        }
    }
    for name in ib.keys() {
        if !ia.contains_key(name) {
            out.push(Divergence::TaskOnlyIn {
                run: WhichRun::B,
                task: name.clone(),
            });
        }
    }

    for (name, ta) in &ia {
        let Some(tb) = ib.get(name) else { continue };

        let mut input = |field: &'static str, x: &str, y: &str| {
            if x != y {
                out.push(Divergence::InputDrift {
                    task: name.clone(),
                    field,
                    a: x.to_string(),
                    b: y.to_string(),
                });
            }
        };
        input("command_digest", &ta.command_digest, &tb.command_digest);
        input(
            "image",
            ta.image.as_deref().unwrap_or(""),
            tb.image.as_deref().unwrap_or(""),
        );
        input(
            "env_digest",
            ta.env_digest.as_deref().unwrap_or(""),
            tb.env_digest.as_deref().unwrap_or(""),
        );
        input(
            "isolation_digest",
            ta.isolation_digest.as_deref().unwrap_or(""),
            tb.isolation_digest.as_deref().unwrap_or(""),
        );

        let mut output = |field: &'static str, x: &str, y: &str| {
            if x != y {
                out.push(Divergence::OutputDrift {
                    task: name.clone(),
                    field,
                    a: x.to_string(),
                    b: y.to_string(),
                });
            }
        };
        output("status", &ta.status, &tb.status);
        output(
            "output_digest",
            ta.output_digest.as_deref().unwrap_or(""),
            tb.output_digest.as_deref().unwrap_or(""),
        );
        output(
            "exit_code",
            &ta.exit_code.map(|c| c.to_string()).unwrap_or_default(),
            &tb.exit_code.map(|c| c.to_string()).unwrap_or_default(),
        );
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fully-populated succeeded task — the baseline every drift test
    /// mutates exactly one field of.
    fn task(name: &str) -> TaskRecord {
        TaskRecord {
            name: name.to_string(),
            status: "succeeded".into(),
            attempts: 1,
            exit_code: Some(0),
            command_digest: command_digest(&["python".into(), "etl.py".into()]),
            image: Some("registry/etl@sha256:aaaa".into()),
            env_digest: Some(env_digest(&[("STAGE".into(), "extract".into())])),
            isolation_digest: Some("sha-iso".into()),
            output_digest: Some("sha-out".into()),
            started_at: Some("2026-09-05T10:00:00Z".into()),
            finished_at: Some("2026-09-05T10:00:10Z".into()),
        }
    }

    /// A succeeded run of `etl-diamond` over `tasks`, with fixed timestamps so
    /// only what a test changes can move the digest.
    fn attestation(run: &str, tasks: Vec<TaskRecord>) -> Attestation {
        Attestation::new(
            run,
            "etl-diamond",
            "sha-spec",
            "succeeded",
            "2026-09-05T10:00:00Z",
            "2026-09-05T10:01:00Z",
            "0.9.1",
            "k8s",
            tasks,
        )
    }

    /// A fixed-seed signing key: these tests assert on stable bytes, so the
    /// key must not vary between runs.
    fn key() -> SigningKey {
        SigningKey::from_bytes(&[7u8; 32])
    }

    #[test]
    fn digest_is_stable_across_task_order() {
        let a = attestation("r1", vec![task("extract"), task("load")]);
        let b = attestation("r1", vec![task("load"), task("extract")]);
        assert_eq!(
            a.digest().unwrap(),
            b.digest().unwrap(),
            "task order must not change a run's identity"
        );
    }

    #[test]
    fn digest_changes_when_anything_attested_changes() {
        let base = attestation("r1", vec![task("extract")]);
        let baseline = base.digest().unwrap();

        let mut drifted = base.clone();
        drifted.tasks[0].image = Some("registry/etl@sha256:bbbb".into());
        assert_ne!(
            baseline,
            drifted.digest().unwrap(),
            "a different image must not digest the same"
        );

        let mut chained = base.clone();
        chained.prev = Some("sha-prev".into());
        assert_ne!(
            baseline,
            chained.digest().unwrap(),
            "the chain link is covered by the digest, or it could be rewritten"
        );
    }

    #[test]
    fn sign_then_verify_round_trips() {
        let k = key();
        let att = attestation("r1", vec![task("extract")]);
        let (bytes, sig) = att.sign(&k).unwrap();

        let verified = verify(&bytes, &sig, &[k.verifying_key()]).unwrap();
        assert_eq!(verified.attestation.run_id, "r1");
        assert_eq!(verified.digest, att.digest().unwrap());
    }

    #[test]
    fn a_tampered_record_does_not_verify() {
        let k = key();
        let att = attestation("r1", vec![task("extract")]);
        let (bytes, sig) = att.sign(&k).unwrap();

        // Flip the recorded status from succeeded to failed — the single edit
        // an operator covering up a bad run would make.
        let text = String::from_utf8(bytes).unwrap();
        let tampered = text.replacen("\"status\":\"succeeded\"", "\"status\":\"failed\"", 1);
        assert_ne!(text, tampered, "the test's own edit must have landed");

        let err = verify(tampered.as_bytes(), &sig, &[k.verifying_key()]).unwrap_err();
        assert!(
            err.to_string().contains("does not verify"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn an_untrusted_key_is_refused_and_so_is_an_empty_trust_set() {
        let k = key();
        let other = SigningKey::from_bytes(&[9u8; 32]);
        let att = attestation("r1", vec![task("extract")]);
        let (bytes, sig) = att.sign(&k).unwrap();

        assert!(verify(&bytes, &sig, &[other.verifying_key()]).is_err());

        let err = verify(&bytes, &sig, &[]).unwrap_err();
        assert!(
            err.to_string().contains("empty trust set"),
            "an empty trust set must refuse, not accept: {err}"
        );
    }

    #[test]
    fn a_short_signature_is_refused_before_decoding() {
        let k = key();
        let att = attestation("r1", vec![task("extract")]);
        let (bytes, _) = att.sign(&k).unwrap();
        let err = verify(&bytes, &[0u8; 8], &[k.verifying_key()]).unwrap_err();
        assert!(err.to_string().contains("expected 64"), "{err}");
    }

    #[test]
    fn a_foreign_format_is_refused_after_a_valid_signature() {
        let k = key();
        let mut att = attestation("r1", vec![task("extract")]);
        att.format = "dagron.attestation.v2".into();
        let (bytes, sig) = att.sign(&k).unwrap();

        // Correctly signed, and still refused: a future format must not be
        // interpreted by a v1 verifier that would misread its fields.
        let err = verify(&bytes, &sig, &[k.verifying_key()]).unwrap_err();
        assert!(err.to_string().contains("unsupported attestation format"), "{err}");
    }

    #[test]
    fn a_duplicate_task_name_is_refused_by_verify_and_reported_by_diff() {
        let k = key();
        let mut att = attestation("r1", vec![task("extract")]);
        // Two records claiming to be the same task. `new` sorts but does not
        // deduplicate, and a record from another producer need not even be
        // sorted, so this shape is reachable.
        att.tasks.push(task("extract"));
        let (bytes, sig) = att.sign(&k).unwrap();

        // Correctly signed, and still refused: the signature says nobody edited
        // it, not that it means anything.
        let err = verify(&bytes, &sig, &[k.verifying_key()]).unwrap_err();
        assert!(err.to_string().contains("more than once"), "{err}");

        // And a diff on an unverified record reports it rather than collapsing
        // the two entries and answering "no divergence".
        let clean = attestation("r2", vec![task("extract")]);
        let diff = replay_diff(&att, &clean);
        assert!(
            diff.iter().any(|d| matches!(
                d,
                Divergence::DuplicateTask { run: WhichRun::A, task } if task == "extract"
            )),
            "{diff:?}"
        );
        assert!(diff.iter().all(|d| d.is_input_drift()), "{diff:?}");
    }

    #[test]
    fn a_chain_verifies_and_a_deletion_breaks_it() {
        let k = key();
        let signed = |a: &Attestation| {
            let (bytes, sig) = a.sign(&k).unwrap();
            verify(&bytes, &sig, &[k.verifying_key()]).unwrap()
        };

        let first = attestation("r1", vec![task("extract")]);
        let v1 = signed(&first);
        let second = attestation("r2", vec![task("extract")]).chained_to(v1.digest.clone());
        let v2 = signed(&second);
        let third = attestation("r3", vec![task("extract")]).chained_to(v2.digest.clone());
        let v3 = signed(&third);

        verify_chain(&[v1.clone(), v2.clone(), v3.clone()]).expect("intact chain");

        // Excise the middle run — every record still verifies individually,
        // which is exactly why the chain has to be what catches it.
        let err = verify_chain(&[v1, v3]).unwrap_err();
        assert!(err.to_string().contains("chain broken"), "{err}");
    }

    #[test]
    fn a_chain_window_may_start_mid_chain_but_not_restart() {
        let k = key();
        let signed = |a: &Attestation| {
            let (bytes, sig) = a.sign(&k).unwrap();
            verify(&bytes, &sig, &[k.verifying_key()]).unwrap()
        };

        // A window whose first record points at something outside the slice is
        // legitimate — that is what reading the tail of a long ledger looks like.
        let a = attestation("r2", vec![task("extract")]).chained_to("sha-earlier");
        let va = signed(&a);
        let b = attestation("r3", vec![task("extract")]).chained_to(va.digest.clone());
        verify_chain(&[va.clone(), signed(&b)]).expect("a mid-chain window is fine");

        // A genesis record appearing after another one is not.
        let orphan = attestation("r4", vec![task("extract")]);
        let err = verify_chain(&[va, signed(&orphan)]).unwrap_err();
        assert!(err.to_string().contains("genesis"), "{err}");
    }

    #[test]
    fn identical_runs_diff_clean_despite_different_ids_and_clocks() {
        let a = attestation("r1", vec![task("extract"), task("load")]);
        let mut b = attestation("r2", vec![task("extract"), task("load")]);
        b.started_at = "2026-09-06T22:15:00Z".into();
        b.finished_at = "2026-09-06T22:16:00Z".into();
        b.tasks[0].started_at = Some("2026-09-06T22:15:01Z".into());
        b.tasks[0].attempts = 3;

        assert!(
            replay_diff(&a, &b).is_empty(),
            "run id, wall clock and attempt count are not reproducibility signals"
        );
    }

    #[test]
    fn output_drift_is_reported_separately_from_input_drift() {
        let a = attestation("r1", vec![task("extract")]);
        let mut b = attestation("r2", vec![task("extract")]);
        b.tasks[0].output_digest = Some("sha-different".into());

        let diff = replay_diff(&a, &b);
        assert_eq!(diff.len(), 1, "{diff:?}");
        assert!(
            !diff[0].is_input_drift(),
            "same inputs, different output is the nondeterminism finding: {diff:?}"
        );
        assert!(matches!(
            &diff[0],
            Divergence::OutputDrift { field: "output_digest", .. }
        ));
    }

    #[test]
    fn a_changed_environment_voids_the_comparison_as_input_drift() {
        let a = attestation("r1", vec![task("extract")]);
        let mut b = attestation("r2", vec![task("extract")]);
        b.tasks[0].env_digest = Some(env_digest(&[("STAGE".into(), "load".into())]));
        b.tasks[0].output_digest = Some("sha-different".into());

        let diff = replay_diff(&a, &b);
        assert!(
            diff.iter().any(|d| d.is_input_drift()),
            "a different environment must be reported as input drift: {diff:?}"
        );
    }

    #[test]
    fn a_changed_trust_envelope_is_input_drift_even_with_identical_output() {
        let a = attestation("r1", vec![task("extract")]);
        let mut b = attestation("r2", vec![task("extract")]);
        b.tasks[0].isolation_digest = Some("sha-iso-weakened".into());

        let diff = replay_diff(&a, &b);
        assert_eq!(diff.len(), 1, "{diff:?}");
        assert!(
            diff[0].is_input_drift(),
            "running the same code under different privileges is not the same run: {diff:?}"
        );
    }

    #[test]
    fn a_spec_change_and_a_missing_task_are_both_reported() {
        let a = attestation("r1", vec![task("extract"), task("load")]);
        let mut b = attestation("r2", vec![task("extract")]);
        b.spec_digest = "sha-spec-v2".into();

        let diff = replay_diff(&a, &b);
        assert!(diff.iter().any(|d| matches!(d, Divergence::SpecDigest { .. })));
        assert!(diff.iter().any(|d| matches!(
            d,
            Divergence::TaskOnlyIn { run: WhichRun::A, task } if task == "load"
        )));
        assert!(diff.iter().all(|d| d.is_input_drift()));
    }

    #[test]
    fn command_digest_does_not_collide_across_argv_boundaries() {
        assert_ne!(
            command_digest(&["a b".into()]),
            command_digest(&["a".into(), "b".into()]),
            "a separator that can appear inside an argument would let these collide"
        );
        // A NUL inside an argument is legal in a Rust String, so a NUL-joined
        // encoding would collide here too.
        assert_ne!(
            command_digest(&["a\0b".into()]),
            command_digest(&["a".into(), "b".into()]),
        );
    }

    #[test]
    fn env_digest_is_order_independent_but_value_sensitive() {
        let one = env_digest(&[("A".into(), "1".into()), ("B".into(), "2".into())]);
        let two = env_digest(&[("B".into(), "2".into()), ("A".into(), "1".into())]);
        assert_eq!(one, two, "dispatch order is not a property of the environment");

        let changed = env_digest(&[("A".into(), "1".into()), ("B".into(), "3".into())]);
        assert_ne!(one, changed, "a changed value must change the digest");

        // Neither name nor value is recoverable, but both are covered: moving
        // the `=` must not produce the same digest.
        assert_ne!(
            env_digest(&[("A".into(), "B=2".into())]),
            env_digest(&[("A=B".into(), "2".into())]),
        );
    }
}
