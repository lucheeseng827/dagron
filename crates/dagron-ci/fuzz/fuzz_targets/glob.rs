//! A pattern of an `on:` filter, and a name it is asked about: the first
//! line of the input is the pattern, the rest the name.
#![no_main]

use dagron_ci::pipeline::glob::{Glob, Patterns};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|text: &str| {
    let (pattern, name) = text.split_once('\n').unwrap_or((text, ""));
    if let Ok(glob) = Glob::new(pattern) {
        let _ = glob.matches(name);
    }
    let list: Vec<String> = pattern.split(',').map(str::to_string).collect();
    for patterns in [Patterns::include(&list), Patterns::ignore(&list)]
        .into_iter()
        .flatten()
    {
        let _ = patterns.matches(name);
        let _ = patterns.any(&[name, "src/lib.rs"]);
    }
});
