//! dagron — the scheduler daemon (thin entry point).
//!
//! All logic lives in the reusable [`dagron_engine`] crate; this binary just wires
//! the default [`Seams`](dagron_engine::Seams) — built-in file/channel sources
//! only, no run sink, no usage accounting — and hands off to `dagron_engine::run`.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dagron_engine::run(dagron_engine::Seams::default()).await
}
