//! A pipeline file, read and compiled for each kind of event. Whatever the
//! file holds, the reader answers with a pipeline or with its problems, and
//! the compiler with the same bytes each time it is asked.
#![no_main]

use dagron_ci::pipeline::{self, compile};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|text: &str| {
    let Ok(pipeline) = pipeline::parse(text) else {
        return;
    };
    let settings = compile::Settings::default();
    for event in dagron_ci_fuzz::events() {
        let once = compile::compile(&pipeline, &event, &settings);
        assert_eq!(once, compile::compile(&pipeline, &event, &settings));
    }
});
