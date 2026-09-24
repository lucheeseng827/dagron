//! dagron-state — turn a backfill planner's *state plan* into a dagron run graph.
//!
//! ## What this is
//!
//! The backfill planner answers one question:
//! *given this SQL change, what is the minimal set of models to rebuild?* It is a
//! library with no binary and no service, deliberately — it must stay embeddable by
//! any orchestrator, which is the whole point of not being dbt-shaped.
//!
//! This crate is the other half: the surface that makes that answer *runnable* for
//! someone who already has dagron. It compiles a plan into a workflow spec and
//! submits it.
//!
//! ## The noun
//!
//! dagron already has `backfill`, and it means something else: schedule-interval
//! catch-up over `[from, to]`, driven by cron fire-times
//! (`dagron-engine::backfill_jobs`). A state plan has no cron — it is a
//! topologically ordered set of models produced by diffing SQL against committed
//! state. Overloading one word onto both would make the existing feature harder to
//! explain and this one impossible to. So this is the **`state`** noun, and it
//! targets the run-submit path rather than the `backfills` table.
//!
//! ## What it deliberately does not link
//!
//! Neither the planner nor dagron:
//!
//! * **Not the planner.** `crates/` is mirrored wholesale to the public dagron repo,
//!   where the planner's source does not exist. The plan arrives as JSON on its frozen wire
//!   contract ([`wire`]) instead.
//! * **Not `dagron-core`.** `dagron-api` links this crate and pins `dagron-core` to
//!   `postgres` while the rest of the workspace takes `sqlite`; that crate's
//!   "exactly one backend" `compile_error!` fires if both unify. `dagron-core` is a
//!   dev-dependency here, used by `tests/dagron_compat.rs` to parse this crate's
//!   output through dagron's real parser.
//!
//! What is left is a component with one seam ([`router::PlanSubmitter`]) that can be
//! split into its own service without touching dagron. That is by design — see
//! this crate's README.
//!
//! ## Shape
//!
//! ```text
//!   planner (any binding)      dagron-state                    host
//!   ─────────────────────      ─────────────────────────       ──────────────
//!   plan --json          ──►   wire::PlanResponse  (v3: + replace ops)
//!                              compile::compile()        ──►   spec::DagSpec
//!                              router::PlanSubmitter     ──►   POST /api/runs
//! ```
//!
//! ## Example
//!
//! ```
//! use dagron_state::compile::{compile, CompileOptions, PlanEnvelope};
//! use dagron_state::wire::{
//!     PlanModel, PlanResponse, Reason, Replace, ReplaceStrategy, ReplaceTarget, Unit,
//! };
//!
//! let envelope = PlanEnvelope {
//!     plan: PlanResponse {
//!         models: vec![
//!             PlanModel {
//!                 name: "stg_orders".into(),
//!                 reason: Reason::DirectlyChanged,
//!                 unit: Unit::Partitions(vec!["2026-06-20".into()]),
//!                 depends_on: Some(vec![]),
//!                 // What the rebuild does to the relation (contract v3).
//!                 // Semantics, not SQL: rendering this is the operator's
//!                 // command or an engine adapter, never this crate.
//!                 replace: Some(Replace {
//!                     strategy: ReplaceStrategy::InsertOverwrite,
//!                     partition_column: Some("dt".into()),
//!                     unique_key: vec![],
//!                     target: ReplaceTarget::Partitions(vec!["2026-06-20".into()]),
//!                     widened: None,
//!                 }),
//!                 // The SQL that performs it (contract v4), when the planner was
//!                 // asked for a dialect. Carried, never written, by this crate.
//!                 sql: None,
//!             },
//!             PlanModel {
//!                 name: "mart_revenue".into(),
//!                 reason: Reason::Downstream {
//!                     because_of: "stg_orders".into(),
//!                     via_columns: vec!["amount".into()],
//!                 },
//!                 unit: Unit::FullModel,
//!                 // The planner's own edges (contract v2). `reason` explains;
//!                 // this orders. They are not the same thing.
//!                 depends_on: Some(vec!["stg_orders".into()]),
//!                 // Undeclared, and that stays undeclared: no default is
//!                 // invented for it here or upstream.
//!                 replace: None,
//!                 sql: None,
//!             },
//!         ],
//!         ..Default::default()
//!     },
//!     graph: None,
//!     options: CompileOptions {
//!         command_template: vec![
//!             "sh".into(),
//!             "-c".into(),
//!             "dbt run --select {{ model }} --strategy {{ replace }}".into(),
//!         ],
//!         ..Default::default()
//!     },
//! };
//!
//! let spec = compile(&envelope).unwrap();
//! assert_eq!(spec.tasks.len(), 2);
//! assert_eq!(spec.tasks[1].depends_on, vec!["stg_orders"]);
//! assert_eq!(spec.tasks[0].command[2], "dbt run --select stg_orders --strategy insert_overwrite");
//! // A model with no replace op expands the placeholder to nothing, rather than
//! // leaving `{{ replace }}` to reach a shell.
//! assert_eq!(spec.tasks[1].command[2], "dbt run --select mart_revenue --strategy ");
//! ```

pub mod compile;
pub mod explain;
#[cfg(feature = "http")]
pub mod router;
pub mod spec;
pub mod wire;

#[cfg(feature = "http")]
pub use router::{router, PlanSubmitter, SubmitError, MOUNT_PREFIX};
