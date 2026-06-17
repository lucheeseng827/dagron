//! Shared Postgres `task_events` listener → broadcast pump.
//!
//! ONE listener connection feeds a `tokio::sync::broadcast` channel that every
//! SSE client subscribes to (and filters by run_id). This is the N-replica-safe
//! fan-out — a per-client listener would burn one DB connection per browser tab.

use std::time::Duration;

use sqlx::postgres::{PgListener, PgPool};
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::state::TaskEvent;

/// NOTIFY channel the engine fires on every task-state change (payload = run_id).
const EVENT_CHANNEL: &str = "task_events";

/// Spawn the background listener. Connects a dedicated `PgListener`, subscribes
/// to `task_events`, and forwards each notification's run_id payload onto `tx`.
///
/// Resilient: `PgListener` auto-reconnects on transient drops; on a hard error
/// we log, back off briefly, and rebuild the listener. A zero-receiver broadcast
/// just drops sends, so it is safe to run with no clients connected.
pub fn spawn_listener(pool: PgPool, tx: broadcast::Sender<TaskEvent>) {
    tokio::spawn(async move {
        loop {
            match run_listener(&pool, &tx).await {
                Ok(()) => {
                    // run_listener only returns Ok on a clean shutdown path; in
                    // practice it loops forever. Treat as a signal to stop.
                    info!("task_events listener stopped");
                    return;
                }
                Err(err) => {
                    warn!(error = ?err, "task_events listener errored; reconnecting in 1s");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });
}

async fn run_listener(pool: &PgPool, tx: &broadcast::Sender<TaskEvent>) -> anyhow::Result<()> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen(EVENT_CHANNEL).await?;
    info!(channel = EVENT_CHANNEL, "SSE listener subscribed");

    loop {
        // recv() returns Err on a connection error that PgListener could not
        // transparently recover — propagate so the outer loop rebuilds it.
        let notification = listener.recv().await?;
        let run_id = notification.payload().to_string();
        // send() errs only when there are no receivers — that's fine, drop it.
        let _ = tx.send(TaskEvent { run_id });
    }
}
