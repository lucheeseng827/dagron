//! dagron task executors — *how* a claimed task runs.
//!
//! * [`executor`] — the [`Executor`](executor::Executor) trait + shared
//!   [`ExecContext`](executor::ExecContext) / [`ExecOutput`](executor::ExecOutput),
//!   and the always-available `LocalExecutor` (subprocess).
//! * [`docker_executor`] — run each task in a container (Docker / podman socket).
//! * `kube_executor` — run each task as a Kubernetes pod (feature `kubernetes`).
//! * [`worker`] — the ractor worker pool that dispatches claimed tasks to the
//!   configured executor and reports results back to the reconcile loop.
//! * [`pool`] — what an `EXECUTOR=local` pool permits: which programs a task may
//!   run, and which of the pool's own environment variables it may see. Both
//!   opt-in; unset, a local pool behaves exactly as it always has.
//! * [`workspace`] — a task's workspace on each backend: the operator's
//!   settings, the pod volumes and init container, the local directory.

pub mod checkout;
pub mod docker_executor;
pub mod images;
pub mod executor;
#[cfg(feature = "kubernetes")]
pub mod kube_executor;
#[cfg(feature = "kubernetes")]
mod kube_log;
#[cfg(feature = "kubernetes")]
mod kube_watch;
mod lines;
pub mod pool;
pub mod redact;
pub mod secrets;
pub mod worker;
pub mod workspace;

/// Install the process-wide rustls `CryptoProvider` that the kube client needs
/// before it opens TLS to the apiserver. Call once at startup; safe to ignore the
/// result (a second call just returns the already-installed provider).
#[cfg(feature = "kubernetes")]
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
