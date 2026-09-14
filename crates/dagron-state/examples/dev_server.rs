//! A dev dagron: the real `state` router, in front of a host that runs nothing.
//!
//! ```text
//! cargo run --example dev_server                 # binds 127.0.0.1:8787
//! cargo run --example dev_server -- 9000         # or a port you pick
//! ```
//!
//! ## What this is for
//!
//! Two audiences, both of whom would otherwise need a database and an identity
//! provider to see this component work:
//!
//! * **Planner-side.** A CLI that posts plans — `freshet submit --to …` — needs
//!   something at the other end of the URL. This is that something, and because it
//!   mounts [`dagron_state::router`] verbatim, what it exercises is the actual
//!   compile path, not a mock of it. A plan that compiles here compiles in dagron.
//! * **Host-side.** [`PlanSubmitter`] is the single seam a host implements, and
//!   [`DevSubmitter`] below is the smallest possible existence proof of that — some
//!   thirty lines, no dagron crates, no state. If your own submitter is much harder
//!   than this one, the difficulty is in your run API, not in this component.
//!
//! ## What it is not
//!
//! It does not run anything. `submit` prints the workflow YAML and hands back a
//! counter-derived id, so the run "exists" only in this process's memory. It also
//! adds **no authentication**, which is precisely the gap the router's own docs
//! warn about: `POST /plans/submit` creates runs, and a real host must layer auth
//! over the mount. Binding to loopback is this example's entire security model.
//! Do not run it anywhere else.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dagron_state::{router, PlanSubmitter, SubmitError, MOUNT_PREFIX};

/// A host that accepts every plan and starts nothing.
#[derive(Default)]
struct DevSubmitter {
    runs: AtomicU64,
}

impl PlanSubmitter for DevSubmitter {
    async fn submit(&self, yaml: String) -> Result<String, SubmitError> {
        let n = self.runs.fetch_add(1, Ordering::Relaxed) + 1;
        let id = format!("dev-run-{n:04}");
        println!("\n── {id} ─── the YAML a real dagron would have accepted ───────────");
        println!("{}", yaml.trim_end());
        println!("──────────────────────────────────────────────────────────────────");
        Ok(id)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port: u16 = match std::env::args().nth(1) {
        Some(arg) => arg.parse().map_err(|_| format!("not a port number: {arg}"))?,
        None => 8787,
    };
    // Loopback only, deliberately — see the module docs. This example has no auth,
    // so the interface it binds is the only thing keeping it private.
    let addr = SocketAddr::from(([127, 0, 0, 1], port));

    let app = axum::Router::new().nest_service(MOUNT_PREFIX, router(Arc::new(DevSubmitter::default())));
    let listener = tokio::net::TcpListener::bind(addr).await?;

    let base = format!("http://{addr}{MOUNT_PREFIX}");
    eprintln!("dev dagron on {base}");
    eprintln!("  GET  {base}/contract");
    eprintln!("  POST {base}/plans           compile only");
    eprintln!("  POST {base}/plans/explain   markdown + Mermaid");
    eprintln!("  POST {base}/plans/submit    compile, then \"run\"");
    eprintln!();
    eprintln!("from a planner project directory:");
    eprintln!(
        "  freshet submit --project ./models --to {base}/plans/submit \\\n    \
         --command 'dbt run --select {{{{ model }}}}'"
    );

    axum::serve(listener, app).await?;
    Ok(())
}
