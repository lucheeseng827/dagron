//! `{{ }}` templating + `when:` evaluation for the API submit path.
//!
//! Mirrors `dagron-core::expand::{substitute, eval_when, when_output_refs}` —
//! dagron-api cannot depend on dagron-core (its sqlite/postgres feature
//! exclusivity trips under workspace feature unification, see control.rs), so
//! the same semantics are replicated here. Keep in sync with core:
//! * `{{ key }}` looks up the context; unknown keys stay verbatim;
//! * `eval_when` delegates to core's (`and` / `or` / `in [..]`, one
//!   comparison, or a bare truthy value);
//! * `tasks.<name>.output` references mark a condition as runtime-evaluated.
//!
//! (Core's 3-token arithmetic inside placeholders is intentionally not
//! mirrored — it exists for recursive template depth counters, which never
//! reach this path: the API path has no template expansion.)

use std::collections::BTreeMap;

/// Replace every `{{ key }}` with `ctx[key]`; unknown keys stay verbatim.
pub fn substitute(s: &str, ctx: &BTreeMap<String, String>) -> String {
    if !s.contains("{{") {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            out.push_str(&rest[start..]);
            return out;
        };
        let key = after[..end].trim();
        match ctx.get(key) {
            Some(v) => out.push_str(v),
            None => {
                out.push_str("{{");
                out.push_str(&after[..end]);
                out.push_str("}}");
            }
        }
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    out
}

/// Evaluate a (substituted) condition with core's grammar (`and`, `or`,
/// `in [..]`, comparisons, a bare truthy value), so the two cannot drift.
pub fn eval_when(cond: &str) -> Result<bool, String> {
    dagron_core::expand::eval_when(cond).map_err(|e| e.to_string())
}

/// Task names referenced as `{{ tasks.<name>.output }}` — the runtime-gate form.
pub fn when_output_refs(cond: &str) -> Vec<String> {
    let mut refs = Vec::new();
    let mut rest = cond;
    while let Some(start) = rest.find("{{") {
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else { break };
        let key = after[..end].trim();
        if let Some(name) = key.strip_prefix("tasks.").and_then(|k| k.strip_suffix(".output")) {
            if !name.is_empty() && !refs.iter().any(|r| r == name) {
                refs.push(name.to_string());
            }
        }
        rest = &after[end + 2..];
    }
    refs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitute_known_and_unknown() {
        let mut ctx = BTreeMap::new();
        ctx.insert("env.BUCKET".to_string(), "s3://x".to_string());
        assert_eq!(substitute("cp {{ env.BUCKET }} .", &ctx), "cp s3://x .");
        assert_eq!(substitute("{{ tasks.a.output }} == go", &ctx), "{{ tasks.a.output }} == go");
    }

    #[test]
    fn eval_when_mirrors_core() {
        assert!(eval_when("go == go").unwrap());
        assert!(!eval_when("3 < 2").unwrap());
        assert!(eval_when("yes").unwrap());
        assert!(!eval_when("false").unwrap());
        assert!(eval_when("a < b").is_err());
    }

    #[test]
    fn output_refs_extracted() {
        assert_eq!(when_output_refs("{{ tasks.check.output }} == deploy"), vec!["check"]);
        assert!(when_output_refs("{{ env.X }} == 1").is_empty());
    }
}
