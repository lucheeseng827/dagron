//! dagron task executors — *how* a claimed task runs.
//!
//! * [`executor`] — the [`Executor`](executor::Executor) trait + shared
//!   [`ExecContext`](executor::ExecContext) / [`ExecOutput`](executor::ExecOutput),
//!   and the always-available `LocalExecutor` (subprocess).
//! * [`docker_executor`] — run each task in a container (Docker / podman socket).
//! * `kube_executor` — run each task as a Kubernetes pod (feature `kubernetes`).
//! * [`worker`] — the ractor worker pool that dispatches claimed tasks to the
//!   configured executor and reports results back to the reconcile loop.

pub mod docker_executor;
pub mod executor;
#[cfg(feature = "kubernetes")]
pub mod kube_executor;
pub mod worker;

/// Install the process-wide rustls `CryptoProvider` that the kube client needs
/// before it opens TLS to the apiserver. Call once at startup; safe to ignore the
/// result (a second call just returns the already-installed provider).
#[cfg(feature = "kubernetes")]
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
