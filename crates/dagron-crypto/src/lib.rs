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

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
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
    key_from_env(KEY_ENV)
}

/// Derive a 32-byte AES key from an arbitrary env var (32 bytes of standard
/// base64 used verbatim; anything else treated as a passphrase and hashed to key
/// length). Errors when the variable is unset/empty. Used for both the legacy
/// secret key and the envelope KEK (`DAGRON_ENV_KEK`).
pub fn key_from_env(name: &str) -> Result<[u8; 32]> {
    let raw = std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .with_context(|| format!("{name} is not set — required to store or read encrypted values"))?;
    Ok(key_from_material(&raw))
}

/// 32-byte base64 → verbatim; otherwise SHA-256 of the passphrase.
fn key_from_material(raw: &str) -> [u8; 32] {
    if let Ok(bytes) = b64().decode(raw) {
        if bytes.len() == 32 {
            let mut key = [0u8; 32];
            key.copy_from_slice(&bytes);
            return key;
        }
    }
    let digest = Sha256::digest(raw.as_bytes());
    let mut key = [0u8; 32];
    key.copy_from_slice(&digest);
    key
}

/// True when a key is configured (secret storage is available).
pub fn key_configured() -> bool {
    load_key().is_ok()
}

/// AES-256-GCM seal: returns the raw `nonce ‖ ciphertext+tag` blob (no version
/// prefix / base64). Shared by the `v1:` secret path and the envelope payload.
fn seal(key: &[u8; 32], plaintext: &[u8]) -> Result<Vec<u8>> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ct = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|_| anyhow!("encryption failed"))?;
    let mut blob = Vec::with_capacity(NONCE_LEN + ct.len());
    blob.extend_from_slice(&nonce);
    blob.extend_from_slice(&ct);
    Ok(blob)
}

/// Inverse of [`seal`]: open a raw `nonce ‖ ciphertext+tag` blob.
fn open(key: &[u8; 32], blob: &[u8]) -> Result<Vec<u8>> {
    if blob.len() <= NONCE_LEN {
        bail!("ciphertext is truncated");
    }
    let (nonce, ct) = blob.split_at(NONCE_LEN);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    cipher
        .decrypt(Nonce::from_slice(nonce), ct)
        .map_err(|_| anyhow!("decryption failed"))
}

/// Encrypt a secret value for storage. Returns the `v1:` wire form.
pub fn encrypt(key: &[u8; 32], plaintext: &str) -> Result<String> {
    Ok(format!("{VERSION_PREFIX}{}", b64().encode(seal(key, plaintext.as_bytes())?)))
}

/// Decrypt a stored `v1:` ciphertext back to the secret value.
pub fn decrypt(key: &[u8; 32], stored: &str) -> Result<String> {
    let Some(encoded) = stored.strip_prefix(VERSION_PREFIX) else {
        bail!("unknown secret ciphertext version (expected {VERSION_PREFIX}…)");
    };
    let blob = b64().decode(encoded).context("secret ciphertext is not valid base64")?;
    let pt = open(key, &blob).map_err(|_| anyhow!("secret decryption failed (wrong {KEY_ENV}?)"))?;
    String::from_utf8(pt).context("decrypted secret is not UTF-8")
}

// ── envelope encryption (v2) — G-C1 ──────────────────────────────────────────
//
// Instead of one shared symmetric key, each value is sealed under a fresh random
// **data key (DEK)**; the DEK is then **wrapped** by a **key-encryption key (KEK)**
// held by a [`KeyProvider`]. The KEK can live in the customer's KMS / HSM / Vault
// (bring-your-own-key), so the operator never holds the key that protects the
// data. Wire form: `v2:<provider-id>:<base64(wrapped-DEK)>:<base64(nonce‖ct)>`.
// The `<provider-id>` selects the provider on decrypt and records which KEK
// wrapped the value. `v1:` values remain readable via [`decrypt`], so migration
// is per-value and lazy.

const ENVELOPE_PREFIX: &str = "v2:";

/// Env var selecting the KEK provider (`local` | `command` | `none`).
pub const PROVIDER_ENV: &str = "DAGRON_ENV_KEK_PROVIDER";
/// Env var holding the local KEK (32-byte base64 or a passphrase).
pub const KEK_ENV: &str = "DAGRON_ENV_KEK";

/// Wraps/unwraps data keys with a key-encryption key. Implementations back the
/// KEK with anything: a local key, a cloud KMS, an HSM, Vault transit. The DEK
/// bytes are the only sensitive material crossing this boundary; a remote
/// provider never sees plaintext values.
pub trait KeyProvider: Send + Sync {
    /// Stable id recorded in the ciphertext and matched on decrypt.
    fn id(&self) -> &str;
    /// Wrap a 32-byte data key with the KEK; returns opaque wrapped bytes.
    fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>>;
    /// Unwrap previously wrapped bytes back to the 32-byte data key.
    fn unwrap(&self, wrapped: &[u8]) -> Result<[u8; 32]>;
}

/// Which envelope/legacy version a stored ciphertext is (`"v2"`, `"v1"`, or
/// `None` if unrecognized). Lets a caller pick the envelope vs legacy path.
pub fn version_of(stored: &str) -> Option<&'static str> {
    if stored.starts_with(ENVELOPE_PREFIX) {
        Some("v2")
    } else if stored.starts_with(VERSION_PREFIX) {
        Some("v1")
    } else {
        None
    }
}

/// Envelope-encrypt a value: fresh DEK → seal value under the DEK → wrap the DEK
/// with the provider's KEK. Returns the `v2:` wire form.
pub fn encrypt_envelope(provider: &dyn KeyProvider, plaintext: &str) -> Result<String> {
    // The provider id is embedded between colons in the wire form and split back
    // out on decrypt (`splitn(3, ':')`); an id containing ':' would shift the
    // framing and make the value permanently undecryptable. Reject it up front.
    let id = provider.id();
    if id.contains(':') {
        bail!("KEK provider id '{id}' must not contain ':' (it delimits the v2 wire format)");
    }
    let mut dek = [0u8; 32];
    dek.copy_from_slice(&Aes256Gcm::generate_key(&mut OsRng));
    let payload = seal(&dek, plaintext.as_bytes())?;
    let wrapped = provider.wrap(&dek)?;
    Ok(format!(
        "{ENVELOPE_PREFIX}{}:{}:{}",
        provider.id(),
        b64().encode(wrapped),
        b64().encode(payload),
    ))
}

/// Decrypt a `v2:` envelope ciphertext. The provider's id must match the one the
/// value was wrapped with (else the wrong KEK would be tried).
pub fn decrypt_envelope(provider: &dyn KeyProvider, stored: &str) -> Result<String> {
    let rest = stored
        .strip_prefix(ENVELOPE_PREFIX)
        .context("not a v2 envelope ciphertext")?;
    let mut parts = rest.splitn(3, ':');
    let pid = parts.next().context("v2: missing provider id")?;
    let wrapped_b64 = parts.next().context("v2: missing wrapped key")?;
    let payload_b64 = parts.next().context("v2: missing payload")?;
    if pid != provider.id() {
        bail!(
            "ciphertext was wrapped by KEK provider '{pid}', but provider '{}' was supplied",
            provider.id()
        );
    }
    let wrapped = b64().decode(wrapped_b64).context("wrapped key is not valid base64")?;
    let payload = b64().decode(payload_b64).context("envelope payload is not valid base64")?;
    let dek = provider.unwrap(&wrapped)?;
    let pt = open(&dek, &payload).map_err(|_| anyhow!("envelope payload decryption failed"))?;
    String::from_utf8(pt).context("decrypted secret is not UTF-8")
}

// ── binary envelope (for artifacts / blobs) ──────────────────────────────────
//
// The string envelope above base64s everything, which wastes ~33% on large
// artifacts (checkpoints can be gigabytes). This is the same construction in a
// compact **binary** frame — no base64 — for `&[u8]` payloads:
//
//   0x02 | id_len:u8 | id | wrapped_len:u32-LE | wrapped | (nonce ‖ ct)
//
// `0x02` marks the binary envelope (distinct from the textual `v2:` prefix).

const ENVELOPE_BIN_TAG: u8 = 0x02;

/// Envelope-encrypt raw bytes into the compact binary frame (see module note).
pub fn encrypt_envelope_bytes(provider: &dyn KeyProvider, plaintext: &[u8]) -> Result<Vec<u8>> {
    let id = provider.id().as_bytes();
    if id.len() > u8::MAX as usize {
        bail!("KEK provider id is too long ({} bytes; max 255)", id.len());
    }
    let mut dek = [0u8; 32];
    dek.copy_from_slice(&Aes256Gcm::generate_key(&mut OsRng));
    let payload = seal(&dek, plaintext)?;
    let wrapped = provider.wrap(&dek)?;
    let mut out = Vec::with_capacity(1 + 1 + id.len() + 4 + wrapped.len() + payload.len());
    out.push(ENVELOPE_BIN_TAG);
    out.push(id.len() as u8);
    out.extend_from_slice(id);
    out.extend_from_slice(&(wrapped.len() as u32).to_le_bytes());
    out.extend_from_slice(&wrapped);
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decrypt a binary-envelope blob produced by [`encrypt_envelope_bytes`]. The
/// provider id must match the one that wrapped it.
pub fn decrypt_envelope_bytes(provider: &dyn KeyProvider, blob: &[u8]) -> Result<Vec<u8>> {
    // Bounds are checked at each step so a truncated/corrupt frame errors rather
    // than panics.
    let mut rest = blob;
    let tag = take(&mut rest, 1)?[0];
    if tag != ENVELOPE_BIN_TAG {
        bail!("not a binary envelope blob (tag {tag:#04x})");
    }
    let id_len = take(&mut rest, 1)?[0] as usize;
    let id = take(&mut rest, id_len)?;
    let pid = std::str::from_utf8(id).context("provider id is not UTF-8")?;
    if pid != provider.id() {
        bail!(
            "blob was wrapped by KEK provider '{pid}', but provider '{}' was supplied",
            provider.id()
        );
    }
    let wrapped_len = u32::from_le_bytes(
        take(&mut rest, 4)?.try_into().expect("take(4) yields 4 bytes"),
    ) as usize;
    let wrapped = take(&mut rest, wrapped_len)?;
    let dek = provider.unwrap(wrapped)?;
    open(&dek, rest).map_err(|_| anyhow!("envelope payload decryption failed"))
}

/// Split the first `n` bytes off `buf` (advancing it), or error on underrun.
fn take<'a>(buf: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if buf.len() < n {
        bail!("truncated envelope frame (need {n} bytes, have {})", buf.len());
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    Ok(head)
}

// ── key rotation & crypto-shredding (G-C3) ───────────────────────────────────
//
// Rotation **re-wraps the data key only** — it unwraps the DEK with the old KEK
// and re-wraps it with the new KEK, leaving the (much larger) sealed payload
// untouched. So rotating a 10 GiB checkpoint to a new KEK is a few-hundred-byte
// rewrite, not a re-encryption. **Crypto-shredding** falls out for free: destroy a
// KEK (delete the KMS key / drop the local KEK material) and every value whose DEK
// it wrapped becomes permanently unrecoverable — a GDPR/right-to-erasure primitive
// that needs no per-object deletion. (Rewrapping the *whole* store is an
// application-level sweep over its keys — this is the per-object primitive it
// calls.)

/// Re-wrap a `v2:` string envelope from `old`'s KEK to `new`'s, without touching
/// the payload. After this, only `new` can decrypt it; `old` cannot.
pub fn rewrap_envelope(
    old: &dyn KeyProvider,
    new: &dyn KeyProvider,
    stored: &str,
) -> Result<String> {
    let new_id = new.id();
    if new_id.contains(':') {
        bail!("KEK provider id '{new_id}' must not contain ':' (it delimits the v2 wire format)");
    }
    let rest = stored
        .strip_prefix(ENVELOPE_PREFIX)
        .context("not a v2 envelope ciphertext")?;
    let mut parts = rest.splitn(3, ':');
    let pid = parts.next().context("v2: missing provider id")?;
    let wrapped_b64 = parts.next().context("v2: missing wrapped key")?;
    let payload_b64 = parts.next().context("v2: missing payload")?;
    if pid != old.id() {
        bail!("ciphertext was wrapped by '{pid}', but the old provider is '{}'", old.id());
    }
    let wrapped = b64().decode(wrapped_b64).context("wrapped key is not valid base64")?;
    let dek = old.unwrap(&wrapped)?;
    let rewrapped = new.wrap(&dek)?;
    Ok(format!("{ENVELOPE_PREFIX}{new_id}:{}:{payload_b64}", b64().encode(rewrapped)))
}

/// Re-wrap a binary-envelope blob from `old`'s KEK to `new`'s, payload untouched.
pub fn rewrap_envelope_bytes(
    old: &dyn KeyProvider,
    new: &dyn KeyProvider,
    blob: &[u8],
) -> Result<Vec<u8>> {
    let new_id = new.id().as_bytes();
    if new_id.len() > u8::MAX as usize {
        bail!("KEK provider id is too long ({} bytes; max 255)", new_id.len());
    }
    let mut rest = blob;
    let tag = take(&mut rest, 1)?[0];
    if tag != ENVELOPE_BIN_TAG {
        bail!("not a binary envelope blob (tag {tag:#04x})");
    }
    let id_len = take(&mut rest, 1)?[0] as usize;
    let id = take(&mut rest, id_len)?;
    let pid = std::str::from_utf8(id).context("provider id is not UTF-8")?;
    if pid != old.id() {
        bail!("blob was wrapped by '{pid}', but the old provider is '{}'", old.id());
    }
    let wrapped_len =
        u32::from_le_bytes(take(&mut rest, 4)?.try_into().expect("take(4) yields 4 bytes")) as usize;
    let wrapped = take(&mut rest, wrapped_len)?;
    let dek = old.unwrap(wrapped)?;
    let rewrapped = new.wrap(&dek)?;
    let payload = rest; // untouched sealed payload
    let mut out = Vec::with_capacity(1 + 1 + new_id.len() + 4 + rewrapped.len() + payload.len());
    out.push(ENVELOPE_BIN_TAG);
    out.push(new_id.len() as u8);
    out.extend_from_slice(new_id);
    out.extend_from_slice(&(rewrapped.len() as u32).to_le_bytes());
    out.extend_from_slice(&rewrapped);
    out.extend_from_slice(payload);
    Ok(out)
}

// ── streaming envelope (constant-memory, for large artifacts) ────────────────
//
// The whole-buffer envelope holds the entire plaintext + ciphertext in memory. For
// GB-scale artifacts that's untenable, so this variant seals a stream in fixed
// **chunks**: one DEK for the whole stream (wrapped once in a header), then each
// chunk is an independent AES-256-GCM message with a random nonce. The chunk's
// **index is bound as associated data**, so reordering or splicing chunks fails
// authentication; a `final` flag on the last chunk lets the reader detect
// truncation (a stream that ends before the final chunk is rejected).
//
// This crate stays sync + IO-free: the *caller* (which owns the async runtime)
// does length-delimited framing over its transport and calls these per chunk. The
// DEK never leaves the [`StreamSealer`] / [`StreamOpener`].

const STREAM_TAG: u8 = 0x03;

/// Seals a byte stream chunk-by-chunk under one wrapped data key. Build it, write
/// its [`header`](StreamSealer::header) first, then feed chunks to
/// [`seal_chunk`](StreamSealer::seal_chunk) (marking the last `final`).
pub struct StreamSealer {
    dek: [u8; 32],
    index: u32,
}

impl StreamSealer {
    /// Returns the stream header (tag + provider id + wrapped DEK — write this
    /// once, before any chunk) and the sealer.
    pub fn new(provider: &dyn KeyProvider) -> Result<(Vec<u8>, Self)> {
        let id = provider.id().as_bytes();
        if id.len() > u8::MAX as usize {
            bail!("KEK provider id is too long ({} bytes; max 255)", id.len());
        }
        let mut dek = [0u8; 32];
        dek.copy_from_slice(&Aes256Gcm::generate_key(&mut OsRng));
        let wrapped = provider.wrap(&dek)?;
        let mut header = Vec::with_capacity(1 + 1 + id.len() + 4 + wrapped.len());
        header.push(STREAM_TAG);
        header.push(id.len() as u8);
        header.extend_from_slice(id);
        header.extend_from_slice(&(wrapped.len() as u32).to_le_bytes());
        header.extend_from_slice(&wrapped);
        Ok((header, Self { dek, index: 0 }))
    }

    /// Seal one chunk. `is_final` marks the last chunk (the reader rejects a stream
    /// that ends without it). Returns the self-describing frame `flag ‖ nonce ‖ ct`.
    pub fn seal_chunk(&mut self, plaintext: &[u8], is_final: bool) -> Result<Vec<u8>> {
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.dek));
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let aad = chunk_aad(self.index, is_final);
        let ct = cipher
            .encrypt(&nonce, Payload { msg: plaintext, aad: &aad })
            .map_err(|_| anyhow!("chunk encryption failed"))?;
        let mut frame = Vec::with_capacity(1 + NONCE_LEN + ct.len());
        frame.push(is_final as u8);
        frame.extend_from_slice(&nonce);
        frame.extend_from_slice(&ct);
        self.index += 1;
        Ok(frame)
    }
}

/// Additional authenticated data for a stream chunk: its index **and** its
/// final-flag. Binding the flag defeats a truncation attack — an attacker who
/// flips an early chunk's `is_final` byte would otherwise still authenticate (the
/// index matches), set `finished`, and yield a silently truncated plaintext.
fn chunk_aad(index: u32, is_final: bool) -> [u8; 5] {
    let mut aad = [0u8; 5];
    aad[..4].copy_from_slice(&index.to_le_bytes());
    aad[4] = is_final as u8;
    aad
}

/// Opens a chunk-sealed stream. Parse the header once, then feed each frame to
/// [`open_chunk`](StreamOpener::open_chunk); stop when it reports `is_final`.
pub struct StreamOpener {
    dek: [u8; 32],
    index: u32,
    finished: bool,
}

impl StreamOpener {
    /// Parse the stream header (from [`StreamSealer::new`]) and unwrap the DEK. The
    /// provider id in the header must match `provider`.
    pub fn new(provider: &dyn KeyProvider, header: &[u8]) -> Result<Self> {
        let mut rest = header;
        let tag = take(&mut rest, 1)?[0];
        if tag != STREAM_TAG {
            bail!("not a stream header (tag {tag:#04x})");
        }
        let id_len = take(&mut rest, 1)?[0] as usize;
        let id = take(&mut rest, id_len)?;
        let pid = std::str::from_utf8(id).context("provider id is not UTF-8")?;
        if pid != provider.id() {
            bail!("stream wrapped by '{pid}', but provider '{}' was supplied", provider.id());
        }
        let wrapped_len =
            u32::from_le_bytes(take(&mut rest, 4)?.try_into().expect("take(4)")) as usize;
        let wrapped = take(&mut rest, wrapped_len)?;
        let dek = provider.unwrap(wrapped)?;
        Ok(Self { dek, index: 0, finished: false })
    }

    /// Open one frame. Returns `(plaintext, is_final)`. Errors on tamper/reorder
    /// (the chunk index is authenticated) or a chunk after the final one.
    pub fn open_chunk(&mut self, frame: &[u8]) -> Result<(Vec<u8>, bool)> {
        if self.finished {
            bail!("chunk received after the final chunk");
        }
        let mut rest = frame;
        let is_final = take(&mut rest, 1)?[0] != 0;
        let nonce = take(&mut rest, NONCE_LEN)?;
        let aad = chunk_aad(self.index, is_final);
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.dek));
        let pt = cipher
            .decrypt(Nonce::from_slice(nonce), Payload { msg: rest, aad: &aad })
            .map_err(|_| anyhow!("chunk decryption failed (tampered, reordered, or final-flag flipped?)"))?;
        self.index += 1;
        self.finished = is_final;
        Ok((pt, is_final))
    }

    /// Whether the final chunk has been seen (a stream that ends before this is
    /// truncated — the caller must treat that as an error).
    pub fn finished(&self) -> bool {
        self.finished
    }
}

/// Re-wrap a streaming envelope **header** from `old`'s KEK to `new`'s (rotation).
/// Only the header holds the wrapped DEK; the chunk frames are keyed by that same
/// DEK and are left untouched, so rotating a streamed artifact rewrites only its
/// (few-hundred-byte) header. Returns the new header bytes; the caller keeps the
/// original chunk frames.
pub fn rewrap_stream_header(
    old: &dyn KeyProvider,
    new: &dyn KeyProvider,
    header: &[u8],
) -> Result<Vec<u8>> {
    let new_id = new.id().as_bytes();
    if new_id.len() > u8::MAX as usize {
        bail!("KEK provider id is too long ({} bytes; max 255)", new_id.len());
    }
    let mut rest = header;
    let tag = take(&mut rest, 1)?[0];
    if tag != STREAM_TAG {
        bail!("not a stream header (tag {tag:#04x})");
    }
    let id_len = take(&mut rest, 1)?[0] as usize;
    let id = take(&mut rest, id_len)?;
    let pid = std::str::from_utf8(id).context("provider id is not UTF-8")?;
    if pid != old.id() {
        bail!("stream wrapped by '{pid}', but the old provider is '{}'", old.id());
    }
    let wrapped_len =
        u32::from_le_bytes(take(&mut rest, 4)?.try_into().expect("take(4)")) as usize;
    let wrapped = take(&mut rest, wrapped_len)?;
    let dek = old.unwrap(wrapped)?;
    let rewrapped = new.wrap(&dek)?;
    let mut out = Vec::with_capacity(1 + 1 + new_id.len() + 4 + rewrapped.len());
    out.push(STREAM_TAG);
    out.push(new_id.len() as u8);
    out.extend_from_slice(new_id);
    out.extend_from_slice(&(rewrapped.len() as u32).to_le_bytes());
    out.extend_from_slice(&rewrapped);
    Ok(out)
}

/// Local KEK provider: the key-encryption key lives in `DAGRON_ENV_KEK`. Wraps a
/// DEK by AES-256-GCM-sealing it under the KEK. Zero-infra BYOK — the customer
/// holds the KEK and can rotate/revoke it without re-encrypting every value.
pub struct LocalKekProvider {
    kek: [u8; 32],
    id: String,
}

impl LocalKekProvider {
    pub fn new(kek: [u8; 32]) -> Self {
        Self { kek, id: "local".to_string() }
    }
    /// Tag the provider with a KEK id (recorded in the ciphertext) so rotation
    /// can tell which KEK wrapped an old value.
    pub fn with_id(kek: [u8; 32], id: impl Into<String>) -> Self {
        Self { kek, id: id.into() }
    }
}

impl KeyProvider for LocalKekProvider {
    fn id(&self) -> &str {
        &self.id
    }
    fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>> {
        seal(&self.kek, dek)
    }
    fn unwrap(&self, wrapped: &[u8]) -> Result<[u8; 32]> {
        let bytes = open(&self.kek, wrapped)
            .map_err(|_| anyhow!("data-key unwrap failed (wrong local KEK?)"))?;
        bytes
            .as_slice()
            .try_into()
            .context("unwrapped data key is not 32 bytes")
    }
}

/// External-KMS KEK provider via a wrap/unwrap **command** — the dependency-free
/// seam to any KMS/HSM/Vault. The command receives `base64(input)` on stdin and
/// must emit `base64(output)` on stdout: `wrap_cmd` maps DEK→wrapped-blob,
/// `unwrap_cmd` maps wrapped-blob→DEK. Point these at a thin wrapper around
/// `aws kms encrypt/decrypt`, `gcloud kms`, `az keyvault`, or `vault transit`.
///
/// Each invocation is bounded by `DAGRON_ENV_KMS_TIMEOUT_SECS` (default 30s); a
/// wedged wrapper is killed rather than hanging encrypt/decrypt forever.
pub struct CommandKmsProvider {
    pub id: String,
    pub wrap_cmd: Vec<String>,
    pub unwrap_cmd: Vec<String>,
}

impl KeyProvider for CommandKmsProvider {
    fn id(&self) -> &str {
        &self.id
    }
    fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>> {
        let out = run_pipe(&self.wrap_cmd, b64().encode(dek).as_bytes())?;
        b64_from_output(&out).context("wrap command did not emit base64")
    }
    fn unwrap(&self, wrapped: &[u8]) -> Result<[u8; 32]> {
        let out = run_pipe(&self.unwrap_cmd, b64().encode(wrapped).as_bytes())?;
        let bytes = b64_from_output(&out).context("unwrap command did not emit base64")?;
        bytes
            .as_slice()
            .try_into()
            .context("unwrapped data key is not 32 bytes")
    }
}

fn b64_from_output(out: &[u8]) -> Result<Vec<u8>> {
    let s = std::str::from_utf8(out).context("command output is not UTF-8")?;
    b64().decode(s.trim()).context("command output is not valid base64")
}

/// Per-call timeout for a KMS wrap/unwrap command (`DAGRON_ENV_KMS_TIMEOUT_SECS`,
/// default 30s). A wedged wrapper is killed rather than hanging encrypt/decrypt
/// for the lifetime of the process.
fn kms_timeout() -> std::time::Duration {
    let secs = std::env::var("DAGRON_ENV_KMS_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(30);
    std::time::Duration::from_secs(secs)
}

/// Feed `input` to a command's stdin and return its stdout. Non-zero exit is an
/// error carrying stderr. Bounded by [`kms_timeout`]: a command that doesn't exit
/// in time is killed and reaped, and the call returns an error instead of hanging.
///
/// Dependency-free: stdin is written and stdout/stderr drained on their own
/// threads (so a chatty child can't deadlock on a full pipe while we wait), and
/// completion is polled with `try_wait` until the deadline — no `wait-timeout`
/// crate / SIGCHLD handler.
fn run_pipe(cmd: &[String], input: &[u8]) -> Result<Vec<u8>> {
    use std::io::{Read, Write};
    use std::process::{Command, Stdio};
    use std::time::Instant;

    let (prog, args) = cmd.split_first().context("empty KMS command")?;
    let mut child = Command::new(prog)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawning KMS command '{prog}'"))?;

    // Write stdin on its own thread so a child that fills stdout before draining
    // stdin can't deadlock us; dropping the handle closes stdin (EOF). A broken
    // pipe (child exited/killed early) is expected and ignored.
    let mut stdin = child.stdin.take().context("KMS command has no stdin")?;
    let input_owned = input.to_vec();
    let writer = std::thread::spawn(move || {
        let _ = stdin.write_all(&input_owned);
    });
    // Drain stdout/stderr concurrently so the child never blocks on a full pipe
    // while we poll for exit.
    let mut out_pipe = child.stdout.take().context("KMS command has no stdout")?;
    let mut err_pipe = child.stderr.take().context("KMS command has no stderr")?;
    let out_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = out_pipe.read_to_end(&mut buf);
        buf
    });
    let err_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = err_pipe.read_to_end(&mut buf);
        buf
    });

    let timeout = kms_timeout();
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait().context("waiting on KMS command")? {
            Some(status) => break status,
            None => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait(); // reap the killed child
                    let _ = writer.join();
                    let _ = out_reader.join();
                    let _ = err_reader.join();
                    bail!("KMS command '{prog}' timed out after {timeout:?}");
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        }
    };

    let _ = writer.join();
    let stdout = out_reader.join().map_err(|_| anyhow!("stdout reader thread panicked"))?;
    let stderr = err_reader.join().map_err(|_| anyhow!("stderr reader thread panicked"))?;
    if !status.success() {
        bail!(
            "KMS command '{prog}' failed ({}): {}",
            status,
            String::from_utf8_lossy(&stderr).trim()
        );
    }
    Ok(stdout)
}

// ── native AWS KMS KEK provider (feature `kms-aws`) ───────────────────────────
//
// Wraps/unwraps the data key with an AWS KMS customer master key via KMS
// Encrypt/Decrypt — the KEK never leaves KMS, so this is true customer-managed-key
// BYOK without the external-command seam. The `KeyProvider` trait is synchronous
// while the KMS SDK is async, so the provider owns a dedicated current-thread
// runtime and drives each call from a fresh scoped thread — that avoids the
// "runtime within a runtime" panic when `wrap`/`unwrap` are called from dagron's
// async dispatch path.

/// AWS KMS-backed KEK provider. Requires the `kms-aws` feature.
#[cfg(feature = "kms-aws")]
pub struct AwsKmsProvider {
    id: String,
    key_id: String,
    client: aws_sdk_kms::Client,
    rt: tokio::runtime::Runtime,
}

#[cfg(feature = "kms-aws")]
impl AwsKmsProvider {
    /// Build a provider for the given KMS key (id / ARN / alias). Loads AWS config
    /// from the ambient environment (env vars / profile / IMDS / IRSA).
    pub fn from_env(key_id: impl Into<String>, id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        if id.contains(':') {
            bail!("KMS provider id '{id}' must not contain ':' (it delimits the v2 wire format)");
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("building the KMS provider runtime")?;
        let client = rt.block_on(async {
            let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            aws_sdk_kms::Client::new(&cfg)
        });
        Ok(Self { id, key_id: key_id.into(), client, rt })
    }

    /// Run a KMS future to completion on a fresh thread, so the provider's
    /// current-thread runtime never nests inside the caller's runtime.
    fn run<T: Send>(&self, fut: impl std::future::Future<Output = T> + Send) -> T {
        std::thread::scope(|s| s.spawn(|| self.rt.block_on(fut)).join().unwrap())
    }
}

#[cfg(feature = "kms-aws")]
impl KeyProvider for AwsKmsProvider {
    fn id(&self) -> &str {
        &self.id
    }
    fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>> {
        let dek = dek.to_vec();
        self.run(async {
            let out = self
                .client
                .encrypt()
                .key_id(&self.key_id)
                .plaintext(aws_sdk_kms::primitives::Blob::new(dek))
                .send()
                .await
                .map_err(|e| anyhow!("KMS encrypt (key '{}'): {e}", self.key_id))?;
            out.ciphertext_blob()
                .map(|b| b.as_ref().to_vec())
                .context("KMS encrypt returned no ciphertext")
        })
    }
    fn unwrap(&self, wrapped: &[u8]) -> Result<[u8; 32]> {
        let wrapped = wrapped.to_vec();
        self.run(async {
            let out = self
                .client
                .decrypt()
                .ciphertext_blob(aws_sdk_kms::primitives::Blob::new(wrapped))
                .send()
                .await
                .map_err(|e| anyhow!("KMS decrypt: {e}"))?;
            let pt = out.plaintext().context("KMS decrypt returned no plaintext")?;
            pt.as_ref()
                .try_into()
                .context("KMS-unwrapped data key is not 32 bytes")
        })
    }
}

// ── native GCP Cloud KMS KEK provider (feature `kms-gcp`) ─────────────────────

/// GCP Cloud KMS-backed KEK provider. Requires the `kms-gcp` feature. Wraps the
/// data key by `Encrypt`/`Decrypt` on a symmetric Cloud KMS key (name
/// `projects/…/locations/…/keyRings/…/cryptoKeys/…`); the KEK never leaves KMS.
#[cfg(feature = "kms-gcp")]
pub struct GcpKmsProvider {
    id: String,
    key_name: String,
    client: google_cloud_kms::client::Client,
    rt: tokio::runtime::Runtime,
}

#[cfg(feature = "kms-gcp")]
impl GcpKmsProvider {
    /// Build a provider for `key_name`. Auth is loaded from the ambient
    /// environment (`GOOGLE_APPLICATION_CREDENTIALS` / metadata server).
    pub fn from_env(key_name: impl Into<String>, id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        if id.contains(':') {
            bail!("KMS provider id '{id}' must not contain ':' (it delimits the v2 wire format)");
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("building the KMS provider runtime")?;
        let client = rt.block_on(async {
            let config = google_cloud_kms::client::ClientConfig::default()
                .with_auth()
                .await
                .map_err(|e| anyhow!("GCP auth: {e}"))?;
            google_cloud_kms::client::Client::new(config)
                .await
                .map_err(|e| anyhow!("GCP KMS client: {e}"))
        })?;
        Ok(Self { id, key_name: key_name.into(), client, rt })
    }

    fn run<T: Send>(&self, fut: impl std::future::Future<Output = T> + Send) -> T {
        std::thread::scope(|s| s.spawn(|| self.rt.block_on(fut)).join().unwrap())
    }
}

#[cfg(feature = "kms-gcp")]
impl KeyProvider for GcpKmsProvider {
    fn id(&self) -> &str {
        &self.id
    }
    fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>> {
        let dek = dek.to_vec();
        self.run(async {
            use google_cloud_kms::grpc::kms::v1::EncryptRequest;
            let resp = self
                .client
                .encrypt(
                    EncryptRequest { name: self.key_name.clone(), plaintext: dek, ..Default::default() },
                    None,
                )
                .await
                .map_err(|e| anyhow!("GCP KMS encrypt (key '{}'): {e}", self.key_name))?;
            Ok(resp.ciphertext)
        })
    }
    fn unwrap(&self, wrapped: &[u8]) -> Result<[u8; 32]> {
        let wrapped = wrapped.to_vec();
        self.run(async {
            use google_cloud_kms::grpc::kms::v1::DecryptRequest;
            let resp = self
                .client
                .decrypt(
                    DecryptRequest { name: self.key_name.clone(), ciphertext: wrapped, ..Default::default() },
                    None,
                )
                .await
                .map_err(|e| anyhow!("GCP KMS decrypt: {e}"))?;
            resp.plaintext
                .as_slice()
                .try_into()
                .context("GCP KMS-unwrapped data key is not 32 bytes")
        })
    }
}

// ── native Azure Key Vault KEK provider (feature `kms-azure`) ─────────────────

/// Azure Key Vault-backed KEK provider. Requires the `kms-azure` feature. Wraps
/// the data key with `wrapKey`/`unwrapKey` (RSA-OAEP-256) on a Key Vault key; the
/// KEK never leaves the vault/HSM.
#[cfg(feature = "kms-azure")]
pub struct AzureKvProvider {
    id: String,
    key_name: String,
    key_version: String,
    client: azure_security_keyvault_keys::KeyClient,
    rt: tokio::runtime::Runtime,
}

#[cfg(feature = "kms-azure")]
impl AzureKvProvider {
    /// Build a provider for `key_name` (optional `key_version`, empty = latest) in
    /// the vault at `vault_url`. Prefers an explicit service principal from
    /// `AZURE_TENANT_ID` / `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET`, falling back to
    /// `DefaultAzureCredential` (managed identity / CLI) when those are unset.
    pub fn from_env(
        vault_url: impl Into<String>,
        key_name: impl Into<String>,
        key_version: impl Into<String>,
        id: impl Into<String>,
    ) -> Result<Self> {
        let id = id.into();
        if id.contains(':') {
            bail!("KMS provider id '{id}' must not contain ':' (it delimits the v2 wire format)");
        }
        let vault_url = vault_url.into();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("building the KMS provider runtime")?;
        let client = rt.block_on(async {
            // Prefer an explicit service principal from AZURE_TENANT_ID / AZURE_CLIENT_ID
            // / AZURE_CLIENT_SECRET (headless): DefaultAzureCredential in this SDK
            // version does not include the env-var SP in its chain (it tries CLI /
            // managed identity only). Fall back to DefaultAzureCredential for managed
            // identity / dev-CLI environments.
            let cred: std::sync::Arc<dyn azure_core::credentials::TokenCredential> = match (
                std::env::var("AZURE_TENANT_ID"),
                std::env::var("AZURE_CLIENT_ID"),
                std::env::var("AZURE_CLIENT_SECRET"),
            ) {
                (Ok(t), Ok(c), Ok(s)) if !t.is_empty() && !c.is_empty() && !s.is_empty() => {
                    azure_identity::ClientSecretCredential::new(&t, c, s.into(), None)
                        .map_err(|e| anyhow!("Azure client-secret auth: {e}"))?
                }
                _ => azure_identity::DefaultAzureCredential::new()
                    .map_err(|e| anyhow!("Azure auth: {e}"))?,
            };
            azure_security_keyvault_keys::KeyClient::new(&vault_url, cred, None)
                .map_err(|e| anyhow!("Azure Key Vault client: {e}"))
        })?;
        Ok(Self { id, key_name: key_name.into(), key_version: key_version.into(), client, rt })
    }

    fn run<T: Send>(&self, fut: impl std::future::Future<Output = T> + Send) -> T {
        std::thread::scope(|s| s.spawn(|| self.rt.block_on(fut)).join().unwrap())
    }
}

#[cfg(feature = "kms-azure")]
impl KeyProvider for AzureKvProvider {
    fn id(&self) -> &str {
        &self.id
    }
    fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>> {
        use azure_security_keyvault_keys::models::{EncryptionAlgorithm, KeyOperationParameters};
        let params = KeyOperationParameters {
            algorithm: Some(EncryptionAlgorithm::RsaOAEP256),
            value: Some(dek.to_vec()),
            ..Default::default()
        };
        self.run(async {
            let resp = self
                .client
                .wrap_key(&self.key_name, &self.key_version, params.try_into()?, None)
                .await
                .map_err(|e| anyhow!("Azure Key Vault wrapKey: {e}"))?;
            resp.into_body()
                .await?
                .result
                .context("Azure wrapKey returned no result")
        })
    }
    fn unwrap(&self, wrapped: &[u8]) -> Result<[u8; 32]> {
        use azure_security_keyvault_keys::models::{EncryptionAlgorithm, KeyOperationParameters};
        let params = KeyOperationParameters {
            algorithm: Some(EncryptionAlgorithm::RsaOAEP256),
            value: Some(wrapped.to_vec()),
            ..Default::default()
        };
        self.run(async {
            let resp = self
                .client
                .unwrap_key(&self.key_name, &self.key_version, params.try_into()?, None)
                .await
                .map_err(|e| anyhow!("Azure Key Vault unwrapKey: {e}"))?;
            let bytes = resp
                .into_body()
                .await?
                .result
                .context("Azure unwrapKey returned no result")?;
            bytes
                .as_slice()
                .try_into()
                .context("Azure-unwrapped data key is not 32 bytes")
        })
    }
}

/// Build the configured KEK provider from the environment, or `None` when
/// envelope mode is off (`PROVIDER_ENV` unset/`none`) — callers then fall back to
/// the legacy `v1:` single-key path. `local` reads `KEK_ENV`; `command`/`kms`
/// read `DAGRON_ENV_KMS_WRAP_CMD` + `DAGRON_ENV_KMS_UNWRAP_CMD`; `awskms` (feature
/// `kms-aws`) reads `DAGRON_ENV_KMS_KEY_ID` (+ optional `DAGRON_ENV_KMS_ID`).
pub fn provider_from_env() -> Result<Option<Box<dyn KeyProvider>>> {
    build_provider(&|name| {
        std::env::var(name).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
    })
}

/// Build the **previous** KEK provider from `*_OLD`-suffixed env vars
/// (`DAGRON_ENV_KEK_PROVIDER_OLD`, `DAGRON_ENV_KEK_OLD`, `DAGRON_ENV_KMS_KEY_ID_OLD`,
/// …). Used by the key-rotation sweep to unwrap data keys wrapped by the retiring
/// KEK before rewrapping them under the current one.
pub fn old_provider_from_env() -> Result<Option<Box<dyn KeyProvider>>> {
    build_provider(&|name| {
        std::env::var(format!("{name}_OLD"))
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    })
}

/// Resolves an env var name to its value (trimmed, non-empty → `None`).
type Getter<'a> = &'a dyn Fn(&str) -> Option<String>;

/// Build a KEK provider from a name→value resolver, so the same logic serves both
/// the current provider and the `*_OLD` provider (rotation). Feature-gated arms
/// error clearly when the build lacks the SDK.
fn build_provider(get: Getter) -> Result<Option<Box<dyn KeyProvider>>> {
    let which = get(PROVIDER_ENV).map(|v| v.to_ascii_lowercase());

    // ── Open-core line ────────────────────────────────────────────────────────
    // Envelope encryption (BYOK/KMS-wrapped data keys) is Enterprise. The open
    // build keeps the full `v1:` path — environment secrets encrypted with
    // AES-256-GCM under `DAGRON_ENV_SECRET_KEY` — which is what compose ships and
    // what `docs/HOWTO.md` §5 documents; only the KEK layer above it is paid.
    //
    // Every consumer of a KEK reaches it through here: environment-secret
    // envelope mode (`v2:`), artifact encryption at rest, and the rotation sweep.
    // Gating this one function is therefore the whole line, and it errors rather
    // than quietly returning `None` — a silent fallback would downgrade a
    // deployment that asked for KMS to a single env-var key, which is the kind of
    // security surprise nobody should discover from a ciphertext dump.
    if !cfg!(feature = "enterprise") && !matches!(which.as_deref(), None | Some("none")) {
        bail!(
            "{PROVIDER_ENV}={} requests envelope encryption (BYOK/KMS-wrapped data keys),              which ships with dagron Enterprise —              https://github.com/lucheeseng827/dagron#dagron-enterprise. This build encrypts              environment secrets with AES-256-GCM under DAGRON_ENV_SECRET_KEY (the `v1:`              format); unset {PROVIDER_ENV} to use it.",
            which.as_deref().unwrap_or("")
        );
    }

    match which.as_deref() {
        None | Some("none") => Ok(None),
        Some("local") => Ok(Some(Box::new(LocalKekProvider::new(key_from_material(&req(get, KEK_ENV)?))))),
        Some("awskms") | Some("aws-kms") => {
            #[cfg(feature = "kms-aws")]
            {
                Ok(Some(Box::new(AwsKmsProvider::from_env(req(get, "DAGRON_ENV_KMS_KEY_ID")?, kms_id(get, "awskms")?)?)))
            }
            #[cfg(not(feature = "kms-aws"))]
            {
                let _ = get;
                bail!("{PROVIDER_ENV}=awskms but this build lacks the 'kms-aws' feature");
            }
        }
        Some("gcpkms") | Some("gcp-kms") => {
            #[cfg(feature = "kms-gcp")]
            {
                Ok(Some(Box::new(GcpKmsProvider::from_env(req(get, "DAGRON_ENV_KMS_KEY_ID")?, kms_id(get, "gcpkms")?)?)))
            }
            #[cfg(not(feature = "kms-gcp"))]
            {
                let _ = get;
                bail!("{PROVIDER_ENV}=gcpkms but this build lacks the 'kms-gcp' feature");
            }
        }
        Some("azurekv") | Some("azure-kv") => {
            #[cfg(feature = "kms-azure")]
            {
                let vault_url = req(get, "DAGRON_ENV_KMS_VAULT_URL")?;
                let key_name = req(get, "DAGRON_ENV_KMS_KEY_ID")?;
                let key_version = get("DAGRON_ENV_KMS_KEY_VERSION").unwrap_or_default();
                Ok(Some(Box::new(AzureKvProvider::from_env(vault_url, key_name, key_version, kms_id(get, "azurekv")?)?)))
            }
            #[cfg(not(feature = "kms-azure"))]
            {
                let _ = get;
                bail!("{PROVIDER_ENV}=azurekv but this build lacks the 'kms-azure' feature");
            }
        }
        Some("command") | Some("kms") => {
            let wrap_cmd = split_cmd(&req(get, "DAGRON_ENV_KMS_WRAP_CMD")?);
            let unwrap_cmd = split_cmd(&req(get, "DAGRON_ENV_KMS_UNWRAP_CMD")?);
            Ok(Some(Box::new(CommandKmsProvider { id: kms_id(get, "kms")?, wrap_cmd, unwrap_cmd })))
        }
        Some(other) => bail!(
            "unknown {PROVIDER_ENV} '{other}' (expected local|command|awskms|gcpkms|azurekv|none)"
        ),
    }
}

/// Required env value or a clear error.
fn req(get: Getter, name: &str) -> Result<String> {
    get(name).with_context(|| format!("{name} is required for the configured KEK provider"))
}

/// The provider id (`DAGRON_ENV_KMS_ID`, default `default`), rejecting a `:` that
/// would corrupt the `v2:<id>:…` framing — fails fast at startup.
fn kms_id(get: Getter, default: &str) -> Result<String> {
    let id = get("DAGRON_ENV_KMS_ID").unwrap_or_else(|| default.to_string());
    if id.contains(':') {
        bail!("DAGRON_ENV_KMS_ID '{id}' must not contain ':' (it delimits the v2 wire format)");
    }
    Ok(id)
}

/// Whitespace-split a command string. For arguments containing spaces, point the
/// env var at a wrapper script instead.
fn split_cmd(s: &str) -> Vec<String> {
    s.split_whitespace().map(|w| w.to_string()).collect()
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

    // ── envelope (v2) ────────────────────────────────────────────────────────
    #[test]
    fn envelope_roundtrip_and_dek_is_unique() {
        let p = LocalKekProvider::new([9u8; 32]);
        let a = encrypt_envelope(&p, "patient-record").unwrap();
        let b = encrypt_envelope(&p, "patient-record").unwrap();
        assert!(a.starts_with("v2:local:"));
        // Fresh DEK + nonce each time → both the wrapped key and payload differ.
        assert_ne!(a, b, "each value gets a fresh data key");
        assert_eq!(decrypt_envelope(&p, &a).unwrap(), "patient-record");
        assert_eq!(decrypt_envelope(&p, &b).unwrap(), "patient-record");
        assert_eq!(version_of(&a), Some("v2"));
    }

    #[test]
    fn envelope_wrong_kek_cannot_unwrap() {
        let sealed = encrypt_envelope(&LocalKekProvider::new([1u8; 32]), "s3cret").unwrap();
        let wrong = LocalKekProvider::new([2u8; 32]);
        assert!(decrypt_envelope(&wrong, &sealed).is_err(), "wrong KEK must fail");
    }

    #[test]
    fn envelope_provider_id_must_match() {
        let sealed = encrypt_envelope(&LocalKekProvider::with_id([3u8; 32], "kek-2026"), "x").unwrap();
        // Same KEK bytes but a different id → refuse (records which KEK wrapped it).
        let other_id = LocalKekProvider::with_id([3u8; 32], "kek-2025");
        assert!(decrypt_envelope(&other_id, &sealed).is_err());
    }

    #[test]
    fn envelope_tamper_is_detected() {
        let p = LocalKekProvider::new([5u8; 32]);
        let sealed = encrypt_envelope(&p, "do-not-touch").unwrap();
        // Flip a char in the payload segment → GCM auth fails.
        let mut parts: Vec<&str> = sealed.splitn(4, ':').collect(); // ["v2","local",wrapped,payload]
        let mut payload: Vec<char> = parts[3].chars().collect();
        payload[0] = if payload[0] == 'A' { 'B' } else { 'A' };
        let payload: String = payload.into_iter().collect();
        parts[3] = &payload;
        let tampered = parts.join(":");
        assert!(decrypt_envelope(&p, &tampered).is_err(), "tampered payload must fail");
    }

    #[test]
    fn v1_still_readable_alongside_v2() {
        // Legacy single-key values keep working (lazy per-value migration).
        let legacy = encrypt(&[4u8; 32], "old-value").unwrap();
        assert_eq!(version_of(&legacy), Some("v1"));
        assert_eq!(decrypt(&[4u8; 32], &legacy).unwrap(), "old-value");
    }

    // A stand-in KMS provider (identity wrap) to exercise the trait generically —
    // proves encrypt/decrypt_envelope are provider-agnostic.
    struct MockKms;
    impl KeyProvider for MockKms {
        fn id(&self) -> &str {
            "mock"
        }
        fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>> {
            Ok(dek.to_vec())
        }
        fn unwrap(&self, wrapped: &[u8]) -> Result<[u8; 32]> {
            Ok(wrapped.try_into()?)
        }
    }

    #[test]
    fn works_with_an_arbitrary_provider() {
        let p = MockKms;
        let sealed = encrypt_envelope(&p, "via-kms").unwrap();
        assert!(sealed.starts_with("v2:mock:"));
        assert_eq!(decrypt_envelope(&p, &sealed).unwrap(), "via-kms");
    }

    #[test]
    fn colon_in_provider_id_is_rejected() {
        // A ':' in the id would corrupt the `v2:<id>:<wrapped>:<payload>` framing
        // and make the value undecryptable — reject at encrypt time.
        let p = LocalKekProvider::with_id([1u8; 32], "kek:2026");
        assert!(
            encrypt_envelope(&p, "x").is_err(),
            "provider id containing ':' must be rejected"
        );
        // A clean id still works.
        let ok = LocalKekProvider::with_id([1u8; 32], "kek-2026");
        assert!(encrypt_envelope(&ok, "x").is_ok());
    }

    #[test]
    fn kms_command_times_out_instead_of_hanging() {
        // A wrapper that never returns must not hang encrypt/decrypt forever.
        std::env::set_var("DAGRON_ENV_KMS_TIMEOUT_SECS", "1");
        let p = CommandKmsProvider {
            id: "slow".to_string(),
            wrap_cmd: vec!["sleep".to_string(), "30".to_string()],
            unwrap_cmd: vec!["sleep".to_string(), "30".to_string()],
        };
        let start = std::time::Instant::now();
        let result = p.wrap(&[0u8; 32]);
        let elapsed = start.elapsed();
        std::env::remove_var("DAGRON_ENV_KMS_TIMEOUT_SECS");
        assert!(result.is_err(), "a wedged KMS command must error, not hang");
        assert!(
            elapsed < std::time::Duration::from_secs(10),
            "must give up near the 1s timeout, took {elapsed:?}"
        );
    }

    // ── binary envelope (artifacts) ──────────────────────────────────────────
    #[test]
    fn envelope_bytes_roundtrips_arbitrary_binary() {
        let p = LocalKekProvider::new([3u8; 32]);
        // Include NUL and high bytes — must survive (not a text codec).
        let data: Vec<u8> = (0u16..1000).map(|i| (i % 256) as u8).collect();
        let blob = encrypt_envelope_bytes(&p, &data).unwrap();
        assert_eq!(blob[0], ENVELOPE_BIN_TAG);
        assert!(blob.len() < data.len() + 512, "framing overhead is small + constant");
        assert_eq!(decrypt_envelope_bytes(&p, &blob).unwrap(), data);
    }

    #[test]
    fn envelope_bytes_empty_and_unique() {
        let p = LocalKekProvider::new([4u8; 32]);
        assert_eq!(decrypt_envelope_bytes(&p, &encrypt_envelope_bytes(&p, b"").unwrap()).unwrap(), b"");
        let a = encrypt_envelope_bytes(&p, b"same").unwrap();
        let b = encrypt_envelope_bytes(&p, b"same").unwrap();
        assert_ne!(a, b, "fresh DEK + nonce each time");
    }

    #[test]
    fn envelope_bytes_wrong_kek_and_provider_mismatch_fail() {
        let blob = encrypt_envelope_bytes(&LocalKekProvider::new([1u8; 32]), b"secret").unwrap();
        assert!(decrypt_envelope_bytes(&LocalKekProvider::new([2u8; 32]), &blob).is_err());
        assert!(
            decrypt_envelope_bytes(&LocalKekProvider::with_id([1u8; 32], "other"), &blob).is_err(),
            "provider-id mismatch must fail"
        );
    }

    #[test]
    fn envelope_bytes_tamper_and_truncation_rejected() {
        let p = LocalKekProvider::new([5u8; 32]);
        let good = encrypt_envelope_bytes(&p, b"do-not-touch").unwrap();
        let mut t = good.clone();
        *t.last_mut().unwrap() ^= 0xff; // flip a payload byte
        assert!(decrypt_envelope_bytes(&p, &t).is_err(), "tamper must fail");
        assert!(decrypt_envelope_bytes(&p, &good[..good.len() / 2]).is_err(), "truncation must fail");
        assert!(decrypt_envelope_bytes(&p, b"\x02\x01").is_err(), "short frame must fail");
        assert!(decrypt_envelope_bytes(&p, b"not-a-blob").is_err(), "bad tag must fail");
    }

    // ── rotation / crypto-shred (G-C3) ───────────────────────────────────────
    #[test]
    fn rewrap_moves_to_new_kek_old_can_no_longer_read() {
        let old = LocalKekProvider::with_id([1u8; 32], "kek-1");
        let new = LocalKekProvider::with_id([2u8; 32], "kek-2");
        let ct = encrypt_envelope(&old, "patient-record").unwrap();

        let rotated = rewrap_envelope(&old, &new, &ct).unwrap();
        assert!(rotated.starts_with("v2:kek-2:"), "records the new KEK id");
        // New KEK reads it; old KEK is now locked out (= crypto-shred by KEK delete).
        assert_eq!(decrypt_envelope(&new, &rotated).unwrap(), "patient-record");
        assert!(decrypt_envelope(&old, &rotated).is_err(), "old KEK must not read the rotated value");
    }

    #[test]
    fn rewrap_only_touches_the_wrapped_key_not_the_payload() {
        let old = LocalKekProvider::with_id([1u8; 32], "k1");
        let new = LocalKekProvider::with_id([2u8; 32], "k2");
        let ct = encrypt_envelope(&old, "x").unwrap();
        let payload_of = |s: &str| s.rsplit(':').next().unwrap().to_string();
        let rotated = rewrap_envelope(&old, &new, &ct).unwrap();
        assert_eq!(payload_of(&ct), payload_of(&rotated), "payload segment is byte-identical");
    }

    #[test]
    fn rewrap_with_wrong_old_provider_fails() {
        let old = LocalKekProvider::with_id([1u8; 32], "k1");
        let new = LocalKekProvider::with_id([2u8; 32], "k2");
        let ct = encrypt_envelope(&old, "x").unwrap();
        let not_old = LocalKekProvider::with_id([9u8; 32], "kX");
        assert!(rewrap_envelope(&not_old, &new, &ct).is_err());
    }

    #[test]
    fn rewrap_bytes_rotates_binary_artifacts() {
        let old = LocalKekProvider::with_id([3u8; 32], "a");
        let new = LocalKekProvider::with_id([4u8; 32], "b");
        let data: Vec<u8> = (0u16..500).map(|i| i as u8).collect();
        let blob = encrypt_envelope_bytes(&old, &data).unwrap();
        let rotated = rewrap_envelope_bytes(&old, &new, &blob).unwrap();
        assert_eq!(decrypt_envelope_bytes(&new, &rotated).unwrap(), data);
        assert!(decrypt_envelope_bytes(&old, &rotated).is_err(), "old KEK locked out");
    }

    // ── streaming envelope ───────────────────────────────────────────────────
    /// Seal `chunks` end-to-end and reassemble via the opener (helper for tests).
    fn stream_round_trip(p: &dyn KeyProvider, chunks: &[&[u8]]) -> Result<Vec<u8>> {
        let (header, mut sealer) = StreamSealer::new(p)?;
        let mut frames = Vec::new();
        for (i, c) in chunks.iter().enumerate() {
            frames.push(sealer.seal_chunk(c, i + 1 == chunks.len())?);
        }
        let mut opener = StreamOpener::new(p, &header)?;
        let mut out = Vec::new();
        for f in &frames {
            let (pt, _fin) = opener.open_chunk(f)?;
            out.extend_from_slice(&pt);
        }
        assert!(opener.finished(), "final chunk must be seen");
        Ok(out)
    }

    #[test]
    fn stream_round_trips_multiple_chunks() {
        let p = LocalKekProvider::new([6u8; 32]);
        let got = stream_round_trip(&p, &[b"epoch-1|", b"epoch-2|", b"epoch-3"]).unwrap();
        assert_eq!(got, b"epoch-1|epoch-2|epoch-3");
        // Single + empty streams also work.
        assert_eq!(stream_round_trip(&p, &[b"solo"]).unwrap(), b"solo");
        assert_eq!(stream_round_trip(&p, &[b""]).unwrap(), b"");
    }

    #[test]
    fn stream_reordered_chunks_fail() {
        let p = LocalKekProvider::new([6u8; 32]);
        let (header, mut sealer) = StreamSealer::new(&p).unwrap();
        let f0 = sealer.seal_chunk(b"first", false).unwrap();
        let f1 = sealer.seal_chunk(b"second", true).unwrap();
        let mut opener = StreamOpener::new(&p, &header).unwrap();
        // Feeding chunk 1 where chunk 0 is expected → index AAD mismatch → fail.
        assert!(opener.open_chunk(&f1).is_err(), "reordered chunk must fail auth");
        // Correct order still works on a fresh opener.
        let mut ok = StreamOpener::new(&p, &header).unwrap();
        assert!(ok.open_chunk(&f0).is_ok());
        assert!(ok.open_chunk(&f1).is_ok());
    }

    #[test]
    fn stream_wrong_kek_and_bad_header_fail() {
        let (header, _) = StreamSealer::new(&LocalKekProvider::new([1u8; 32])).unwrap();
        assert!(StreamOpener::new(&LocalKekProvider::new([2u8; 32]), &header).is_err());
        assert!(StreamOpener::new(&LocalKekProvider::new([1u8; 32]), b"\x03\x01").is_err());
        assert!(StreamOpener::new(&LocalKekProvider::new([1u8; 32]), b"not-a-stream").is_err());
    }

    #[test]
    fn stream_chunk_after_final_is_rejected() {
        let p = LocalKekProvider::new([6u8; 32]);
        let (header, mut sealer) = StreamSealer::new(&p).unwrap();
        let f0 = sealer.seal_chunk(b"only", true).unwrap();
        let f1 = sealer.seal_chunk(b"extra", true).unwrap();
        let mut opener = StreamOpener::new(&p, &header).unwrap();
        assert!(opener.open_chunk(&f0).unwrap().1, "first chunk is final");
        assert!(opener.open_chunk(&f1).is_err(), "no chunk allowed after final");
    }

    #[test]
    fn stream_final_flag_flip_is_rejected() {
        // Truncation attack: forging a non-final chunk's `is_final` byte to `1` must
        // fail GCM auth (the flag is bound into the AAD), not silently drop the tail.
        let p = LocalKekProvider::new([6u8; 32]);
        let (header, mut sealer) = StreamSealer::new(&p).unwrap();
        let mut f0 = sealer.seal_chunk(b"first", false).unwrap();
        let _f1 = sealer.seal_chunk(b"second", true).unwrap();
        assert_eq!(f0[0], 0, "chunk 0 is not final");
        f0[0] = 1; // forge "final" to try to truncate the stream after chunk 0
        let mut opener = StreamOpener::new(&p, &header).unwrap();
        assert!(opener.open_chunk(&f0).is_err(), "flipped final-flag must fail auth");
    }

    #[test]
    fn stream_header_rotates_and_chunks_still_decrypt_under_new_kek() {
        let old = LocalKekProvider::with_id([1u8; 32], "k1");
        let new = LocalKekProvider::with_id([2u8; 32], "k2");
        let (header, mut sealer) = StreamSealer::new(&old).unwrap();
        let frames = [
            sealer.seal_chunk(b"aa", false).unwrap(),
            sealer.seal_chunk(b"bb", true).unwrap(),
        ];
        // Rotate only the header; the chunk frames are untouched.
        let new_header = rewrap_stream_header(&old, &new, &header).unwrap();
        assert!(rewrap_stream_header(&old, &new, &header).is_ok());

        // New KEK opens the rotated header + original frames.
        let mut opener = StreamOpener::new(&new, &new_header).unwrap();
        let mut out = Vec::new();
        for f in &frames {
            out.extend_from_slice(&opener.open_chunk(f).unwrap().0);
        }
        assert_eq!(out, b"aabb");
        // Old KEK can no longer open the rotated header.
        assert!(StreamOpener::new(&old, &new_header).is_err(), "old KEK locked out");
    }

    // ── live cloud-KMS round-trips (gated on feature + env; see the KMS functional-test notes) ──
    // These hit a real KMS, so they run only when the KMS env is set (skip
    // otherwise). Wrap→unwrap must recover the exact data key.
    #[cfg(feature = "kms-gcp")]
    #[test]
    fn gcp_kms_round_trip_live() {
        let Ok(key) = std::env::var("DAGRON_ENV_KMS_KEY_ID") else {
            eprintln!("skipping gcp_kms_round_trip_live: DAGRON_ENV_KMS_KEY_ID not set");
            return;
        };
        let p = GcpKmsProvider::from_env(key, "gcpkms").expect("build GCP KMS provider");
        let dek = [42u8; 32];
        let wrapped = p.wrap(&dek).expect("GCP wrap");
        assert_ne!(wrapped.as_slice(), &dek[..], "wrapped key must differ from the DEK");
        assert_eq!(p.unwrap(&wrapped).expect("GCP unwrap"), dek, "round-trip must recover the DEK");
        // All three crypto surfaces must work with the network-backed provider.
        let ct = encrypt_envelope_bytes(&p, b"artifact-bytes").expect("GCP byte-envelope");
        assert_eq!(decrypt_envelope_bytes(&p, &ct).unwrap(), b"artifact-bytes");
        let streamed = stream_round_trip(&p, &[b"chunk-1|", b"chunk-2|", b"chunk-3"])
            .expect("GCP streaming envelope");
        assert_eq!(streamed, b"chunk-1|chunk-2|chunk-3", "streaming works with cloud KMS");
    }

    #[cfg(feature = "kms-azure")]
    #[test]
    fn azure_kv_round_trip_live() {
        let (Ok(vault), Ok(key)) = (
            std::env::var("DAGRON_ENV_KMS_VAULT_URL"),
            std::env::var("DAGRON_ENV_KMS_KEY_ID"),
        ) else {
            eprintln!("skipping azure_kv_round_trip_live: DAGRON_ENV_KMS_VAULT_URL/KEY_ID not set");
            return;
        };
        let version = std::env::var("DAGRON_ENV_KMS_KEY_VERSION").unwrap_or_default();
        let p = AzureKvProvider::from_env(vault, key, version, "azurekv")
            .expect("build Azure Key Vault provider");
        let dek = [42u8; 32];
        let wrapped = p.wrap(&dek).expect("Azure wrap");
        assert_ne!(wrapped.as_slice(), &dek[..], "wrapped key must differ from the DEK");
        assert_eq!(p.unwrap(&wrapped).expect("Azure unwrap"), dek, "round-trip must recover the DEK");
        let ct = encrypt_envelope_bytes(&p, b"artifact-bytes").expect("Azure byte-envelope");
        assert_eq!(decrypt_envelope_bytes(&p, &ct).unwrap(), b"artifact-bytes");
        let streamed = stream_round_trip(&p, &[b"chunk-1|", b"chunk-2|", b"chunk-3"])
            .expect("Azure streaming envelope");
        assert_eq!(streamed, b"chunk-1|chunk-2|chunk-3", "streaming works with cloud KMS");
    }

    // Like MockKms but counts KEK `wrap` calls, to assert the streaming envelope
    // wraps the stream's DEK exactly once (in the header) — never once per chunk.
    struct CountingKms {
        wraps: std::sync::atomic::AtomicUsize,
    }
    impl KeyProvider for CountingKms {
        fn id(&self) -> &str {
            "counting"
        }
        fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>> {
            self.wraps.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(dek.to_vec())
        }
        fn unwrap(&self, wrapped: &[u8]) -> Result<[u8; 32]> {
            Ok(wrapped.try_into()?)
        }
    }

    // CI (no cloud): streaming is provider-agnostic — a stand-in KMS provider (not
    // the local KEK) drives the full stream path, locking in that StreamSealer/
    // StreamOpener work with any KeyProvider (the AWS/GCP/Azure providers included).
    #[test]
    fn streaming_works_with_an_arbitrary_provider() {
        let p = CountingKms { wraps: std::sync::atomic::AtomicUsize::new(0) };
        let got = stream_round_trip(&p, &[b"a|", b"bb|", b"ccc"]).unwrap();
        assert_eq!(got, b"a|bb|ccc");
        // One DEK for the whole stream, wrapped once in the header. A 3-chunk stream
        // that re-wrapped the KEK per chunk would count 3 — a KMS round trip per chunk.
        assert_eq!(
            p.wraps.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the KEK must wrap the stream's DEK exactly once, regardless of chunk count",
        );
    }
}

#[cfg(test)]
mod open_core_tests {
    use super::*;

    /// The `v1:` path — environment secrets under `DAGRON_ENV_SECRET_KEY` — is
    /// open in every build. It shipped before the KEK layer existed and the
    /// compose stack depends on it; gating it would be a takeback.
    #[test]
    fn single_key_encryption_is_open_in_every_build() {
        let key = [7u8; 32];
        let sealed = encrypt(&key, "s3cr3t").unwrap();
        assert_eq!(decrypt(&key, &sealed).unwrap(), "s3cr3t");
        assert_eq!(version_of(&sealed), Some("v1"));
    }

    /// Envelope mode off is not an Enterprise question — both builds agree.
    #[test]
    fn provider_unset_is_none_in_every_build() {
        let get = |_: &str| None;
        assert!(matches!(build_provider(&get), Ok(None)));
        let none = |n: &str| (n == PROVIDER_ENV).then(|| "none".to_string());
        assert!(matches!(build_provider(&none), Ok(None)));
    }

    /// Asking for a KEK in an open build must FAIL, naming the edition — never
    /// silently fall back to the single-key path, which would downgrade a
    /// deployment that asked for KMS without telling anyone.
    #[cfg(not(feature = "enterprise"))]
    #[test]
    fn open_build_refuses_a_kek_with_a_signpost() {
        let get = |n: &str| match n {
            n if n == PROVIDER_ENV => Some("local".to_string()),
            n if n == KEK_ENV => Some("some-kek-material".to_string()),
            _ => None,
        };
        let err = match build_provider(&get) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("envelope mode must be refused in an open build"),
        };
        assert!(err.contains("dagron Enterprise"), "signpost names the edition: {err}");
        assert!(err.contains("DAGRON_ENV_SECRET_KEY"), "and points at the open alternative: {err}");
    }

    /// The same configuration in an Enterprise build builds a working provider.
    #[cfg(feature = "enterprise")]
    #[test]
    fn enterprise_build_accepts_a_kek() {
        let get = |n: &str| match n {
            n if n == PROVIDER_ENV => Some("local".to_string()),
            n if n == KEK_ENV => Some("some-kek-material".to_string()),
            _ => None,
        };
        let provider = match build_provider(&get) {
            Ok(Some(p)) => p,
            Ok(None) => panic!("expected a local KEK provider"),
            Err(e) => panic!("building the local KEK provider failed: {e}"),
        };
        let sealed = encrypt_envelope(provider.as_ref(), "s3cr3t").unwrap();
        assert_eq!(version_of(&sealed), Some("v2"));
        assert_eq!(decrypt_envelope(provider.as_ref(), &sealed).unwrap(), "s3cr3t");
    }
}
