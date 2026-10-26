//! An operator's fleet file, and a job's labels asked of it: the first
//! line of the input is the labels, the rest the file.
#![no_main]

use dagron_ci::fleet::Fleet;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|text: &str| {
    let (labels, file) = text.split_once('\n').unwrap_or(("", text));
    if let Ok(fleet) = Fleet::parse(file) {
        let asked: Vec<String> = labels.split(',').map(str::to_string).collect();
        let _ = fleet.serves(&asked);
        let _ = fleet.labels().count();
    }
});
