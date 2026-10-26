//! A task's environment, as the step runner reads its steps from it: each
//! line of the input is a variable, a name and a value.
#![no_main]

use std::collections::BTreeMap;
use std::ffi::OsString;

use dagron_ci_exec::{plan, protocol};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|text: &str| {
    let vars: BTreeMap<String, String> = text
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
    let _ = protocol::read(
        vars.iter()
            .map(|(name, value)| (OsString::from(name), OsString::from(value))),
    );
    let _ = plan::plan(&vars);
});
