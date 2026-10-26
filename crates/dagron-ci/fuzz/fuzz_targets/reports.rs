//! A job's reports, as dagron-ci-exec reads them in the job's own
//! container when its steps are over: JUnit XML, SARIF, and a step's
//! summary. The files are the job's to write, so they hold anything.
#![no_main]

use std::path::Path;

use dagron_ci_exec::report::{junit, sarif, summary_lines, Read};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|text: &str| {
    let workspace = Path::new("/workspace");
    let _ = junit(text, workspace, &mut Read::default());
    let _ = sarif(text, workspace, &mut Read::default());
    let _ = summary_lines(text);
});
