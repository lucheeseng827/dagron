//! Argon2 password hashing — bounded, and off the async runtime.
//!
//! Every password hash/verify in dagron-api goes through here. Two reasons, both
//! measured on the compose stack (dagron-api RSS, fresh container):
//!
//! ```text
//! restart          67 MB
//! +20 logins      187 MB
//! +60 logins      227 MB   ← plateau
//! +30 SSE clients 230 MB   (streaming is not the cost)
//! ```
//!
//! `Argon2::default()` is Argon2id with `m_cost = 19 MiB` — that is the point of
//! a memory-hard KDF, and the parameters are deliberately **not** lowered (19 MiB
//! is the OWASP floor). But the scratch buffer is large enough that glibc raises
//! its mmap threshold after a few of them and keeps the arena, once per thread
//! that ever hashed. Hashing straight off the tokio worker pool therefore parked
//! ~19 MiB × worker-count of resident memory, and — worse — pinned an async
//! worker for the ~50-100 ms each hash takes, stalling every other request on
//! that thread including SSE.
//!
//! So: a semaphore bounds how many hashes run at once (that is the real memory
//! ceiling — `permits × 19 MiB`), and the work itself runs on `spawn_blocking`,
//! whose pool is for exactly this. The permit is taken *before* the blocking
//! task is spawned, so waiting callers queue on the semaphore rather than
//! occupying blocking threads.
//!
//! Note this bounds *cost*, not *access*: an unauthenticated caller can still
//! queue work. Rate limiting is [`crate::ratelimit`]'s job.

use std::sync::OnceLock;

use anyhow::{anyhow, Result};
use argon2::password_hash::{rand_core::OsRng, PasswordHash, PasswordVerifier, SaltString};
use argon2::{Argon2, PasswordHasher};
use tokio::sync::Semaphore;

/// Default concurrent Argon2 operations. Two is enough to keep a login from
/// queueing behind an unrelated one while capping resident scratch at ~38 MiB.
const DEFAULT_CONCURRENCY: usize = 2;

/// Concurrency gate. Override with `DAGRON_PW_HASH_CONCURRENCY` (≥ 1) when a box
/// has memory to spare and logins to burn — each permit costs ~19 MiB resident.
fn gate() -> &'static Semaphore {
    static GATE: OnceLock<Semaphore> = OnceLock::new();
    GATE.get_or_init(|| {
        let n = std::env::var("DAGRON_PW_HASH_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(DEFAULT_CONCURRENCY);
        Semaphore::new(n)
    })
}

/// Run one Argon2 operation under the concurrency gate, on the blocking pool.
async fn bounded<T, F>(work: F) -> Result<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    // `Semaphore::acquire` errs only if the semaphore is closed; ours never is.
    let _permit = gate().acquire().await.map_err(|e| anyhow!("password-hash gate closed: {e}"))?;
    tokio::task::spawn_blocking(work).await.map_err(|e| anyhow!("password-hash task: {e}"))
}

/// Verify `password` against a PHC hash.
///
/// `phc` is `None` when no such account exists. That case still runs a full
/// verify against a fixed dummy hash before returning `false`, so response time
/// doesn't reveal which emails are registered — the dummy is built inside the
/// blocking task, so even the first one never lands on an async worker.
///
/// A malformed stored hash is an `Err`, not a silent `false`: it is a data
/// problem the operator has to see, and treating it as a wrong password would
/// lock the account out with no explanation.
pub async fn verify(password: String, phc: Option<String>) -> Result<bool> {
    bounded(move || match phc {
        Some(phc) => {
            let parsed = PasswordHash::new(&phc)
                .map_err(|e| anyhow!("stored password hash is malformed: {e}"))?;
            Ok(Argon2::default().verify_password(password.as_bytes(), &parsed).is_ok())
        }
        None => {
            if let Ok(dummy) = PasswordHash::new(dummy_pw_hash()) {
                let _ = Argon2::default().verify_password(password.as_bytes(), &dummy);
            }
            Ok(false)
        }
    })
    .await?
}

/// Argon2id hash of `password` with a random salt (PHC string form).
pub async fn hash(password: String) -> Result<String> {
    bounded(move || {
        let salt = SaltString::generate(&mut OsRng);
        Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map(|h| h.to_string())
            .map_err(|e| anyhow!("argon2: {e}"))
    })
    .await?
}

/// A fixed, lazily-computed Argon2 hash used only to equalize timing on the
/// no-such-user path. Never matches a real password.
fn dummy_pw_hash() -> &'static str {
    static H: OnceLock<String> = OnceLock::new();
    H.get_or_init(|| {
        let salt = SaltString::generate(&mut OsRng);
        Argon2::default()
            .hash_password(b"timing-equalizer-not-a-real-password", &salt)
            .expect("hashing the dummy password should not fail")
            .to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trips_a_password() {
        let phc = hash("correct horse battery staple".to_string()).await.unwrap();
        assert!(verify("correct horse battery staple".to_string(), Some(phc.clone()))
            .await
            .unwrap());
        assert!(!verify("wrong".to_string(), Some(phc)).await.unwrap());
    }

    #[tokio::test]
    async fn missing_account_verifies_false_not_error() {
        assert!(!verify("anything".to_string(), None).await.unwrap());
    }

    #[tokio::test]
    async fn malformed_stored_hash_is_an_error() {
        let err = verify("x".to_string(), Some("not-a-phc-string".to_string())).await.unwrap_err();
        assert!(err.to_string().contains("malformed"), "got: {err}");
    }

    /// The gate must serialize work, not deadlock it: more concurrent callers
    /// than permits still all complete.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn more_callers_than_permits_all_complete() {
        let phc = hash("pw".to_string()).await.unwrap();
        let mut set = Vec::new();
        for _ in 0..6 {
            let phc = phc.clone();
            set.push(tokio::spawn(async move { verify("pw".to_string(), Some(phc)).await }));
        }
        for h in set {
            assert!(h.await.unwrap().unwrap());
        }
    }
}
