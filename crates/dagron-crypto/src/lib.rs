//! Secret-value encryption for dagron environments.
//!
//! Environment secrets are stored in the database encrypted with AES-256-GCM
//! under a key derived from the `DAGRON_ENV_SECRET_KEY` environment variable —
//! dagron-api encrypts on write, dagron-engine decrypts at task dispatch, so
//! **both processes must see the same key**. The key may be either 32 bytes of
//! standard base64, or any other string (hashed to 32 bytes with SHA-256).
//!
//! Wire format of a stored ciphertext: `v1:<base64(nonce ‖ ciphertext+tag)>`.
//! The random 96-bit nonce makes every encryption unique; the version prefix
//! leaves room to rotate the scheme without guessing at old rows.
//!
//! This crate deliberately depends on neither sqlx nor any dagron crate:
//! dagron-api cannot depend on dagron-core (its sqlite/postgres feature
//! exclusivity would trip under workspace feature unification), so the shared
//! primitive lives here.

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use anyhow::{anyhow, bail, Context, Result};
use base64::Engine as _;
use sha2::{Digest, Sha256};

/// Environment variable holding the shared encryption key.
pub const KEY_ENV: &str = "DAGRON_ENV_SECRET_KEY";

const VERSION_PREFIX: &str = "v1:";
const NONCE_LEN: usize = 12;

fn b64() -> base64::engine::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

/// Derive the 32-byte AES key from `DAGRON_ENV_SECRET_KEY`. Errors when the
/// variable is unset/empty — callers surface that as "secret storage is not
/// configured" rather than silently storing plaintext.
pub fn load_key() -> Result<[u8; 32]> {
    let raw = std::env::var(KEY_ENV)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .with_context(|| format!("{KEY_ENV} is not set — required to store or read environment secrets"))?;
    // 32 bytes of standard base64 is used verbatim; anything else is treated
    // as a passphrase and hashed to key length.
    if let Ok(bytes) = b64().decode(&raw) {
        if bytes.len() == 32 {
            let mut key = [0u8; 32];
            key.copy_from_slice(&bytes);
            return Ok(key);
        }
    }
    let digest = Sha256::digest(raw.as_bytes());
    let mut key = [0u8; 32];
    key.copy_from_slice(&digest);
    Ok(key)
}

/// True when a key is configured (secret storage is available).
pub fn key_configured() -> bool {
    load_key().is_ok()
}

/// Encrypt a secret value for storage. Returns the `v1:` wire form.
pub fn encrypt(key: &[u8; 32], plaintext: &str) -> Result<String> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ct = cipher
        .encrypt(&nonce, plaintext.as_bytes())
        .map_err(|_| anyhow!("encryption failed"))?;
    let mut blob = Vec::with_capacity(NONCE_LEN + ct.len());
    blob.extend_from_slice(&nonce);
    blob.extend_from_slice(&ct);
    Ok(format!("{VERSION_PREFIX}{}", b64().encode(blob)))
}

/// Decrypt a stored `v1:` ciphertext back to the secret value.
pub fn decrypt(key: &[u8; 32], stored: &str) -> Result<String> {
    let Some(encoded) = stored.strip_prefix(VERSION_PREFIX) else {
        bail!("unknown secret ciphertext version (expected {VERSION_PREFIX}…)");
    };
    let blob = b64().decode(encoded).context("secret ciphertext is not valid base64")?;
    if blob.len() <= NONCE_LEN {
        bail!("secret ciphertext is truncated");
    }
    let (nonce, ct) = blob.split_at(NONCE_LEN);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let pt = cipher
        .decrypt(Nonce::from_slice(nonce), ct)
        .map_err(|_| anyhow!("secret decryption failed (wrong {KEY_ENV}?)"))?;
    String::from_utf8(pt).context("decrypted secret is not UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_uniqueness() {
        let key = [7u8; 32];
        let a = encrypt(&key, "hunter2").unwrap();
        let b = encrypt(&key, "hunter2").unwrap();
        assert_ne!(a, b, "random nonce must make ciphertexts unique");
        assert!(a.starts_with("v1:"));
        assert_eq!(decrypt(&key, &a).unwrap(), "hunter2");
        assert_eq!(decrypt(&key, &b).unwrap(), "hunter2");
    }

    #[test]
    fn wrong_key_fails_loudly() {
        let ct = encrypt(&[1u8; 32], "s3cret").unwrap();
        assert!(decrypt(&[2u8; 32], &ct).is_err());
    }

    #[test]
    fn garbage_is_rejected() {
        let key = [0u8; 32];
        assert!(decrypt(&key, "not-versioned").is_err());
        assert!(decrypt(&key, "v1:%%%").is_err());
        assert!(decrypt(&key, "v1:AAAA").is_err()); // shorter than a nonce
    }
}
