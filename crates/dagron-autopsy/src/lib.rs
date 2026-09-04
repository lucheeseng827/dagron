//! **dagron job autopsy — schedules nothing.**
//!
//! Large production GPU clusters shed a large fraction of their jobs before
//! completion, and infrastructure faults account for most of the wasted
//! GPU-hours. At thousand-GPU scale the mean time to first failure is
//! single-digit hours. The diagnosis is usually wrong: an NCCL timeout gets
//! attributed to a proximal network cause when the actual fault was a deadlock,
//! a dead device, or a rank stuck loading data.
//!
//! Everything needed to do better is already on the cluster and none of it is
//! joined:
//!
//! | source | knows | does not know |
//! |---|---|---|
//! | Slurm (`sacct`) | job state, node set, window | what a GPU is |
//! | DCGM | XIDs, ECC, row-remap, per device | what a job is |
//! | NCCL / framework logs | which ranks hung | anything below the process |
//! | InfiniBand counters | link flaps, symbol errors | which job was on the link |
//!
//! The scheduler is not an observability system and the observability vendors
//! do not own job state, so nobody joins them. This crate does exactly that
//! join and nothing else — it schedules no work, places nothing, and replaces
//! nothing. It runs beside an existing Slurm cluster, reads what is already
//! there, and emits a fault-attributed job record. `sacct` is the required
//! input — the node set and window everything else is joined against — so the
//! shipped tool is Slurm-based; a Kubernetes collector is a gap, not a
//! feature (see [`signal::Source::Kube`], which the taxonomy reserves for it).
//!
//! # Shape
//!
//! ```text
//!   sacct ──┐
//!   DCGM  ──┤                       ┌─ JSON  (fleet database, provider API,
//!   NCCL  ──┼─► [Signal] ─► correlate ─┤          dagron task_runs.fault_class)
//!   IB    ──┘   (when, where,       └─ text  (the operator, at 3am)
//!                what, evidence)
//! ```
//!
//! Adding a fifth source is a parser that emits [`signal::Signal`]s;
//! [`correlate`] does not change.
//!
//! # The one rule that matters
//!
//! A collective timeout is a **symptom**. It is printed by every rank that was
//! *waiting* — the healthy ones — and the rank that died often prints nothing
//! at all. It can be corroborating evidence for a cause found elsewhere, and
//! the pattern of *who stayed silent* can name the culprit, but it is never
//! promoted to a cause on its own. See [`correlate::correlate`].
//!
//! # Vocabulary
//!
//! The taxonomy is `dagron_core::fault`, deliberately the same enum the
//! engine's retry budgets read: two copies of a fault taxonomy is how a tool
//! and a scheduler come to disagree about what happened.

pub mod correlate;
pub mod dcgm;
pub mod ib;
pub mod nccl;
pub mod nodelist;
pub mod record;
pub mod sacct;
pub mod signal;
pub mod timestamp;

// `self::` rather than a bare path — for the reader, not the compiler. A bare
// `record::` here resolves to the local module even if a dependency by that
// name exists (a local item shadows an extern crate in a 2018-edition crate
// root), so this is not a correctness fix and there is no ambiguity error to
// avoid. It is that `record` and `signal` are entirely plausible crate names,
// and a bare path at the crate root reads like one — a static analyser flagged
// exactly that. The prefix says "these are ours" at the point of use.
pub use self::correlate::{correlate, Inputs, Window};
pub use self::record::JobAutopsy;
pub use self::signal::{Signal, Source};

/// Re-exported so a consumer of a record never has to depend on `dagron-core`
/// directly to read the class off it.
pub use dagron_core::fault;
