//! A pipeline key that is neither dagron-ci's nor dagron's is refused, because
//! dagron drops an unknown key without a word. dagron-ci cannot link
//! dagron-core (one database backend per build), so it keeps its own copy of
//! dagron's keys. This test keeps the copy honest: it reads the keys from
//! dagron-core's `TaskSpec` and `DagSpec` themselves.

use std::collections::BTreeSet;

use dagron_ci::pipeline::{DAGRON_SPEC_KEYS, DAGRON_TASK_KEYS};

/// The serialized field names of `pub struct {name}` in `source`: each `pub`
/// field, under its `rename` when it has one.
fn fields(source: &str, name: &str) -> BTreeSet<String> {
    let start = source
        .find(&format!("pub struct {name} {{"))
        .unwrap_or_else(|| panic!("dagron-core has no `pub struct {name}`"));
    let body = &source[start..];
    let end = body.find("\n}\n").expect("the struct ends");
    let mut renamed: Option<String> = None;
    let mut out = BTreeSet::new();
    for line in body[..end].lines().skip(1) {
        let line = line.trim();
        if line.starts_with("#[serde(") {
            if let Some(rest) = line.split("rename = \"").nth(1) {
                renamed = rest.split('"').next().map(str::to_string);
            }
        } else if let Some(field) = line.strip_prefix("pub ") {
            let field = field.split(':').next().expect("a field has a type").trim();
            out.insert(renamed.take().unwrap_or_else(|| field.to_string()));
        }
    }
    out
}

#[test]
fn dagron_ci_knows_every_key_dagron_takes() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../dagron-core/src/dag.rs");
    let source = std::fs::read_to_string(path).expect("dagron-core's dag.rs beside this crate");
    let own = |keys: &[&str]| keys.iter().map(|k| k.to_string()).collect::<BTreeSet<_>>();

    let task = fields(&source, "TaskSpec");
    assert!(
        task.contains("type") && task.contains("docker_image"),
        "{task:?}"
    );
    assert_eq!(
        own(DAGRON_TASK_KEYS),
        task,
        "update DAGRON_TASK_KEYS in crates/dagron-ci/src/pipeline/mod.rs, and the task \
         properties in crates/dagron-ci/schema/pipeline.v1.json"
    );

    let spec = fields(&source, "DagSpec");
    assert!(spec.contains("use") && spec.contains("tasks"), "{spec:?}");
    assert_eq!(
        own(DAGRON_SPEC_KEYS),
        spec,
        "update DAGRON_SPEC_KEYS in crates/dagron-ci/src/pipeline/mod.rs, and the top-level \
         properties in crates/dagron-ci/schema/pipeline.v1.json"
    );
}
