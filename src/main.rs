//! dagron — the scheduler daemon (thin entry point).
//!
//! All logic lives in the reusable [`dagron_engine`] crate; this binary just wires
//! the default [`Seams`](dagron_engine::Seams) — built-in file/channel sources
//! only, no run sink, no usage accounting — and hands off to `dagron_engine::run`.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // `--version` first: for it nothing is read and nothing is started.
    if dagron_engine::version_requested("dagron") {
        return Ok(());
    }
    dagron_engine::run(dagron_engine::Seams::default()).await
}
