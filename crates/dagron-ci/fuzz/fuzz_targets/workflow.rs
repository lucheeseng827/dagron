//! A `.github/workflows` file, read, lowered for each kind of event and
//! compiled. Its expressions, its matrices and its jobs' conditions are all
//! evaluated on the way.
#![no_main]

use dagron_ci::pipeline::{compile, github};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|text: &str| {
    let settings = dagron_ci_fuzz::settings();
    let Ok(workflow) = github::parse(text, &settings.provided) else {
        return;
    };
    for event in dagron_ci_fuzz::events() {
        if let Ok(lowered) = workflow.lower("ci", &event, &settings) {
            let _ = compile::compile(&lowered.pipeline, &event, &compile::Settings::default());
        }
    }
});
