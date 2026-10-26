//! **Signed build provenance**: what a build output is,
//! what made it, and who says so.
//!
//! An [SLSA Provenance v1] predicate, in an [in-toto Statement] v1, in a
//! [DSSE] envelope signed with ECDSA P-256 over SHA-256: the shape
//! `cosign attest-blob` writes. So a build output is checked with the tool
//! anyone checking one already has:
//!
//! ```text
//! cosign verify-blob-attestation --key provenance.pub --type slsaprovenance1 \
//!   --signature build.zip.intoto.json --insecure-ignore-tlog build.zip
//! ```
//!
//! `--insecure-ignore-tlog`, because nothing is written to Sigstore's public
//! log: an installation that cannot reach it (air-gapped) signs all the same,
//! with a key its operator holds ([`KeySigner`], `DAGRON_PROVENANCE_KEY`). The
//! enterprise build signs with a KMS key instead, through a command
//! ([`CommandSigner`], `DAGRON_PROVENANCE_SIGN_CMD`), so the private key never
//! leaves the KMS.
//!
//! What it proves, and what it does not, as the run attestation ([`crate::attest`])
//! says of itself: it proves that whoever holds the key asserts these bytes
//! (by their SHA-256) came out of this job of this run, built from this
//! commit of this repository, for this trigger, and that nobody edited the
//! statement afterwards. It does not prove the build reproducible, nor that
//! the job's own steps were what its repository says: the builder vouches
//! for what it ran, and the key vouches for the builder.
//!
//! [SLSA Provenance v1]: https://slsa.dev/spec/v1.0/provenance
//! [in-toto Statement]: https://github.com/in-toto/attestation/blob/main/spec/v1/statement.md
//! [DSSE]: https://github.com/secure-systems-lab/dsse/blob/master/protocol.md

use anyhow::{bail, Context, Result};
use base64::Engine as _;
use ring::rand::SystemRandom;
use ring::signature::{
    EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_ASN1, ECDSA_P256_SHA256_ASN1_SIGNING,
};
use serde_json::{json, Value};
use sha2::Digest as _;

/// A file holding the signing key: an ECDSA P-256 private key, PEM, PKCS#8
/// (`openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256`).
pub const ENV_KEY: &str = "DAGRON_PROVENANCE_KEY";
/// Enterprise: a command that signs with a KMS key, ECDSA P-256 over SHA-256
/// ([`crate::sign_digest_by_command`]): it is given the SHA-256 of what is
/// signed, base64, on stdin, and writes the ASN.1 DER signature, base64, on
/// stdout. `aws kms sign --message-type DIGEST --signing-algorithm
/// ECDSA_SHA_256`, say.
pub const ENV_SIGN_CMD: &str = "DAGRON_PROVENANCE_SIGN_CMD";
/// With [`ENV_SIGN_CMD`]: a file holding the KMS key's public half, PEM, which
/// is published for verifiers and checked against each signature.
pub const ENV_PUBLIC_KEY: &str = "DAGRON_PROVENANCE_PUBLIC_KEY";

pub const PAYLOAD_TYPE: &str = "application/vnd.in-toto+json";
pub const STATEMENT_TYPE: &str = "https://in-toto.io/Statement/v1";
pub const PREDICATE_TYPE: &str = "https://slsa.dev/provenance/v1";

/// The DER of a P-256 public key's SubjectPublicKeyInfo, up to its point:
/// `SEQUENCE { SEQUENCE { id-ecPublicKey, prime256v1 }, BIT STRING { 0x04 … } }`.
const P256_SPKI_PREFIX: [u8; 26] = [
    0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08, 0x2a,
    0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x03, 0x42, 0x00,
];

fn b64() -> base64::engine::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

/// One thing the provenance is of: its name, and its SHA-256, hex.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subject {
    pub name: String,
    pub sha256: String,
}

/// What signs a statement: ECDSA P-256 over SHA-256, ASN.1 DER.
pub trait Signer: Send + Sync {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>>;
    /// The public key that verifies what it signs, PEM (SubjectPublicKeyInfo):
    /// what an operator publishes for verifiers.
    fn public_key_pem(&self) -> &str;
}

/// A key the operator holds: an ECDSA P-256 private key, PEM, PKCS#8.
pub struct KeySigner {
    pair: EcdsaKeyPair,
    rng: SystemRandom,
    public_pem: String,
}

impl KeySigner {
    pub fn from_pem(pem: &str) -> Result<KeySigner> {
        let der = pem_body(pem, "PRIVATE KEY").context(
            "the provenance key is not a PKCS#8 PEM private key (`-----BEGIN PRIVATE KEY-----`); \
             make one with `openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256`",
        )?;
        let rng = SystemRandom::new();
        let pair = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &der, &rng)
            .map_err(|e| anyhow::anyhow!("the provenance key is not an ECDSA P-256 key: {e}"))?;
        let public_pem = public_pem_of(pair.public_key().as_ref());
        Ok(KeySigner {
            pair,
            rng,
            public_pem,
        })
    }
}

impl Signer for KeySigner {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>> {
        let sig = self
            .pair
            .sign(&self.rng, message)
            .map_err(|_| anyhow::anyhow!("signing the provenance failed"))?;
        Ok(sig.as_ref().to_vec())
    }

    fn public_key_pem(&self) -> &str {
        &self.public_pem
    }
}

/// A KMS key, signing through a command (enterprise): the SHA-256 of the
/// message goes in, base64, on stdin; the signature comes out, base64, on
/// stdout ([`crate::sign_digest_by_command`]). Each signature is checked
/// against the published public key before it is used, so a command that
/// signs with the wrong key fails here, not at a verifier.
pub struct CommandSigner {
    cmd: Vec<String>,
    public_pem: String,
    point: Vec<u8>,
}

impl CommandSigner {
    pub fn new(cmd: Vec<String>, public_pem: &str) -> Result<CommandSigner> {
        if cmd.is_empty() {
            bail!("{ENV_SIGN_CMD} is empty");
        }
        let point = public_point(public_pem)?;
        Ok(CommandSigner {
            cmd,
            public_pem: public_pem.trim().to_string() + "\n",
            point,
        })
    }
}

impl Signer for CommandSigner {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>> {
        let digest: [u8; 32] = sha2::Sha256::digest(message).into();
        let sig = crate::sign_digest_by_command(&self.cmd, &digest)
            .context("the provenance signing command")?;
        ring::signature::UnparsedPublicKey::new(&ECDSA_P256_SHA256_ASN1, &self.point)
            .verify(message, &sig)
            .map_err(|_| {
                anyhow::anyhow!(
                    "the signing command's signature does not verify with {ENV_PUBLIC_KEY}: \
                     is it the same key?"
                )
            })?;
        Ok(sig)
    }

    fn public_key_pem(&self) -> &str {
        &self.public_pem
    }
}

/// A new ECDSA P-256 private key, PEM, PKCS#8: what [`KeySigner::from_pem`]
/// reads, as `openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256`
/// writes one.
pub fn generate_key_pem() -> Result<String> {
    let doc = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &SystemRandom::new())
        .map_err(|_| anyhow::anyhow!("generating a P-256 key failed"))?;
    Ok(pem_of("PRIVATE KEY", doc.as_ref()))
}

/// The signer the environment names, or `None` where it names none: a key
/// file (`DAGRON_PROVENANCE_KEY`), or, in the enterprise build, a KMS command
/// (`DAGRON_PROVENANCE_SIGN_CMD` with `DAGRON_PROVENANCE_PUBLIC_KEY`). One
/// that cannot be read is an error: provenance asked for and not signed is
/// what an operator must hear of as the process starts.
pub fn signer_from_env() -> Result<Option<Box<dyn Signer>>> {
    signer_from(&|name| std::env::var(name).ok().filter(|v| !v.trim().is_empty()))
}

fn signer_from(get: &dyn Fn(&str) -> Option<String>) -> Result<Option<Box<dyn Signer>>> {
    let read = |name: &str, path: &str| {
        std::fs::read_to_string(path.trim()).with_context(|| format!("reading {name} at {path}"))
    };
    match (get(ENV_SIGN_CMD), get(ENV_KEY)) {
        (Some(_), Some(_)) => bail!("set {ENV_KEY} or {ENV_SIGN_CMD}, not both"),
        (Some(cmd), None) => {
            if !cfg!(feature = "enterprise") {
                bail!(
                    "{ENV_SIGN_CMD} signs provenance with a KMS key, which is not in this build: \
                     sign with a key of your own ({ENV_KEY})"
                );
            }
            let public = get(ENV_PUBLIC_KEY).with_context(|| {
                format!("{ENV_SIGN_CMD} needs {ENV_PUBLIC_KEY}: the KMS key's public half, PEM")
            })?;
            let public = read(ENV_PUBLIC_KEY, &public)?;
            Ok(Some(Box::new(CommandSigner::new(
                crate::command_line(&cmd),
                &public,
            )?)))
        }
        (None, Some(path)) => Ok(Some(Box::new(KeySigner::from_pem(&read(ENV_KEY, &path)?)?))),
        (None, None) => Ok(None),
    }
}

/// An in-toto Statement v1 of `subjects`, carrying an SLSA Provenance v1
/// `predicate`.
pub fn statement(subjects: &[Subject], predicate: Value) -> Value {
    json!({
        "_type": STATEMENT_TYPE,
        "subject": subjects
            .iter()
            .map(|s| json!({ "name": s.name, "digest": { "sha256": s.sha256 } }))
            .collect::<Vec<_>>(),
        "predicateType": PREDICATE_TYPE,
        "predicate": predicate,
    })
}

/// DSSE's pre-authentication encoding: what is signed.
pub fn pae(payload_type: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = format!(
        "DSSEv1 {} {} {} ",
        payload_type.len(),
        payload_type,
        payload.len()
    )
    .into_bytes();
    out.extend_from_slice(payload);
    out
}

/// `statement`, signed by `signer`, as a DSSE envelope: the JSON
/// `cosign verify-blob-attestation` reads.
pub fn envelope(signer: &dyn Signer, statement: &Value) -> Result<Value> {
    let payload = serde_json::to_vec(statement)?;
    let sig = signer.sign(&pae(PAYLOAD_TYPE, &payload))?;
    Ok(json!({
        "payloadType": PAYLOAD_TYPE,
        "payload": b64().encode(&payload),
        "signatures": [{ "keyid": "", "sig": b64().encode(sig) }],
    }))
}

/// The statement in `envelope`, where a signature on it verifies with
/// `public_key_pem`. An error says why it is not to be believed.
pub fn verify(envelope: &Value, public_key_pem: &str) -> Result<Value> {
    if envelope["payloadType"] != PAYLOAD_TYPE {
        bail!("the envelope is not of an in-toto statement");
    }
    let payload = b64()
        .decode(
            envelope["payload"]
                .as_str()
                .context("the envelope has no payload")?,
        )
        .context("the envelope's payload is not base64")?;
    let point = public_point(public_key_pem)?;
    let key = ring::signature::UnparsedPublicKey::new(&ECDSA_P256_SHA256_ASN1, &point);
    let message = pae(PAYLOAD_TYPE, &payload);
    let signed = envelope["signatures"]
        .as_array()
        .context("the envelope has no signatures")?
        .iter()
        .filter_map(|s| s["sig"].as_str())
        .filter_map(|s| b64().decode(s).ok())
        .any(|sig| key.verify(&message, &sig).is_ok());
    if !signed {
        bail!("no signature on the provenance verifies with this key");
    }
    let statement: Value = serde_json::from_slice(&payload).context("the payload is not JSON")?;
    if statement["_type"] != STATEMENT_TYPE || statement["predicateType"] != PREDICATE_TYPE {
        bail!("the payload is not an SLSA v1 provenance statement");
    }
    Ok(statement)
}

/// Whether `statement` names a subject with this SHA-256.
pub fn covers(statement: &Value, sha256: &str) -> bool {
    statement["subject"]
        .as_array()
        .is_some_and(|subjects| subjects.iter().any(|s| s["digest"]["sha256"] == sha256))
}

fn pem_body(pem: &str, label: &str) -> Result<Vec<u8>> {
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let start = pem.find(&begin).with_context(|| format!("no {begin}"))? + begin.len();
    let stop = pem[start..]
        .find(&end)
        .with_context(|| format!("no {end}"))?
        + start;
    let body: String = pem[start..stop]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    b64().decode(body).context("the PEM body is not base64")
}

/// A P-256 public key's point, from its PEM (SubjectPublicKeyInfo).
fn public_point(pem: &str) -> Result<Vec<u8>> {
    let der = pem_body(pem, "PUBLIC KEY").context("the public key is not a PEM public key")?;
    match der.strip_prefix(P256_SPKI_PREFIX.as_slice()) {
        Some(point) if point.len() == 65 && point[0] == 0x04 => Ok(point.to_vec()),
        _ => bail!("the public key is not an uncompressed ECDSA P-256 key"),
    }
}

fn public_pem_of(point: &[u8]) -> String {
    let mut der = P256_SPKI_PREFIX.to_vec();
    der.extend_from_slice(point);
    pem_of("PUBLIC KEY", &der)
}

/// `der` as PEM, in lines of 64 as openssl writes them.
fn pem_of(label: &str, der: &[u8]) -> String {
    let body = b64().encode(der);
    let lines: Vec<&str> = body
        .as_bytes()
        .chunks(64)
        .map(|c| std::str::from_utf8(c).unwrap_or_default())
        .collect();
    format!(
        "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
        lines.join("\n")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh P-256 key, PEM, PKCS#8: no key is kept in the source.
    fn key() -> String {
        generate_key_pem().unwrap()
    }

    fn subject() -> Subject {
        Subject {
            name: "build.zip".into(),
            sha256: "6fa65ede27c1d0c4dc280732b086ca6cb1f93ef41c814cc73e8ec788e29625ca".into(),
        }
    }

    fn openssl() -> bool {
        std::process::Command::new("openssl")
            .arg("version")
            .output()
            .is_ok_and(|o| o.status.success())
    }

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("m54-prov-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn a_signed_statement_verifies_with_the_key_s_public_half_and_says_what_it_is_of() {
        let signer = KeySigner::from_pem(&key()).unwrap();
        let public = signer.public_key_pem().to_string();
        let st = statement(
            &[subject()],
            json!({ "buildDefinition": { "buildType": "x" } }),
        );
        let env = envelope(&signer, &st).unwrap();
        assert_eq!(env["payloadType"], PAYLOAD_TYPE);
        assert_eq!(env["signatures"][0]["keyid"], "");
        let read = verify(&env, &public).unwrap();
        assert_eq!(read, st);
        assert!(covers(&read, &subject().sha256));
        assert!(!covers(&read, "00"));
    }

    /// The public half is the one `openssl pkey -pubout` writes of the key,
    /// byte for byte: what an operator publishes is what openssl would.
    #[test]
    fn the_public_half_is_the_one_openssl_writes() {
        if !openssl() {
            return;
        }
        let dir = scratch("pub");
        let pem = key();
        std::fs::write(dir.join("key.pem"), &pem).unwrap();
        let out = std::process::Command::new("openssl")
            .args(["pkey", "-pubout", "-in"])
            .arg(dir.join("key.pem"))
            .output()
            .unwrap();
        assert!(out.status.success());
        assert_eq!(
            KeySigner::from_pem(&pem).unwrap().public_key_pem(),
            String::from_utf8(out.stdout).unwrap()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_statement_changed_after_it_was_signed_or_another_key_is_refused() {
        let signer = KeySigner::from_pem(&key()).unwrap();
        let public = signer.public_key_pem().to_string();
        let mut env = envelope(&signer, &statement(&[subject()], json!({}))).unwrap();
        let other = KeySigner::from_pem(&key()).unwrap();
        assert!(verify(&env, other.public_key_pem()).is_err());
        let forged = statement(
            &[Subject {
                name: "build.zip".into(),
                sha256: "00".into(),
            }],
            json!({}),
        );
        env["payload"] = json!(b64().encode(serde_json::to_vec(&forged).unwrap()));
        assert!(verify(&env, &public).is_err());
    }

    #[test]
    fn dsse_s_encoding_is_its_spec_s() {
        assert_eq!(pae("a/b", b"hello"), b"DSSEv1 3 a/b 5 hello".to_vec());
    }

    #[test]
    fn a_key_that_is_not_a_p256_pkcs8_key_is_refused_saying_how_to_make_one() {
        let e = KeySigner::from_pem(
            "-----BEGIN EC PRIVATE KEY-----\nAAAA\n-----END EC PRIVATE KEY-----\n",
        )
        .err()
        .unwrap()
        .to_string();
        assert!(e.contains("openssl genpkey"), "{e}");
        assert!(
            public_point("-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----\n").is_err()
        );
    }

    #[test]
    fn the_environment_names_a_key_or_a_kms_command_and_not_both() {
        let dir = scratch("env");
        let pem = key();
        let path = dir.join("key.pem");
        std::fs::write(&path, &pem).unwrap();
        let path = path.to_str().unwrap().to_string();
        let none = |_: &str| None;
        assert!(signer_from(&none).unwrap().is_none());
        let with_key = |name: &str| (name == ENV_KEY).then(|| path.clone());
        assert_eq!(
            signer_from(&with_key).unwrap().unwrap().public_key_pem(),
            KeySigner::from_pem(&pem).unwrap().public_key_pem()
        );
        let both = |name: &str| match name {
            ENV_KEY => Some(path.clone()),
            ENV_SIGN_CMD => Some("kms-sign".into()),
            _ => None,
        };
        assert!(signer_from(&both).is_err());
        let cmd_only = |name: &str| (name == ENV_SIGN_CMD).then(|| "kms-sign".to_string());
        let e = signer_from(&cmd_only).err().unwrap().to_string();
        if cfg!(feature = "enterprise") {
            assert!(e.contains(ENV_PUBLIC_KEY), "{e}");
        } else {
            assert!(e.contains("not in this build"), "{e}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A KMS command, stood in for by a script that signs a digest through
    /// openssl as a KMS does: what goes in, what comes out, and that a
    /// signature by another key is refused before it is used.
    #[cfg(all(unix, feature = "enterprise"))]
    #[test]
    fn a_kms_command_signs_and_its_signature_is_checked_against_the_published_key() {
        if !openssl() {
            return;
        }
        let dir = scratch("cmd");
        let pem = key();
        let public = KeySigner::from_pem(&pem)
            .unwrap()
            .public_key_pem()
            .to_string();
        let path = dir.join("key.pem");
        std::fs::write(&path, &pem).unwrap();
        let script = dir.join("sign");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\nbase64 -d | openssl pkeyutl -sign -inkey {} -pkeyopt digest:sha256 | base64 | tr -d '\\n'\n",
                path.display()
            ),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cmd = vec![script.to_str().unwrap().to_string()];
        let signer = CommandSigner::new(cmd.clone(), &public).unwrap();
        assert_eq!(signer.public_key_pem(), public);
        let env = envelope(&signer, &statement(&[subject()], json!({}))).unwrap();
        assert!(verify(&env, &public).is_ok());
        let other = KeySigner::from_pem(&key()).unwrap();
        let wrong = CommandSigner::new(cmd, other.public_key_pem()).unwrap();
        let e = wrong.sign(b"x").err().unwrap().to_string();
        assert!(e.contains("same key"), "{e}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
