//! `dagron-ci`, the open build. The commands are in [`dagron_ci::cli`].

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    dagron_ci::cli::run("dagron-ci", &args, dagron_ci::Seams::default).await
}
