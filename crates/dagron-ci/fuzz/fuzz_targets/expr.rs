//! An expression of a workflow file, by itself, in a text and as a
//! condition: read, and evaluated against contexts that hold a little of
//! everything.
#![no_main]

use dagron_ci_expr::{condition, parse, template, Host, Value};
use libfuzzer_sys::fuzz_target;

struct Contexts;

impl Host for Contexts {
    fn context(&self, name: &str) -> Option<Value> {
        let text = |s: &str| Value::String(s.to_string());
        Some(match name.to_ascii_lowercase().as_str() {
            "github" => Value::Object(vec![
                ("ref".into(), text("refs/heads/main")),
                ("event_name".into(), text("push")),
                ("run_number".into(), Value::Number(7.0)),
                (
                    "event".into(),
                    Value::Object(vec![("forced".into(), Value::Bool(false))]),
                ),
            ]),
            "matrix" => Value::Object(vec![(
                "os".into(),
                Value::Array(vec![text("linux"), Value::Null, Value::Number(f64::NAN)]),
            )]),
            "unknown" => return None,
            _ => Value::Object(Vec::new()),
        })
    }

    fn call(&self, name: &str, _args: &[Value]) -> Option<Result<Value, String>> {
        match name.to_ascii_lowercase().as_str() {
            "success" | "always" => Some(Ok(Value::Bool(true))),
            "failure" => Some(Err("not known yet".into())),
            _ => None,
        }
    }
}

fuzz_target!(|text: &str| {
    if let Ok(expr) = parse(text) {
        let _ = expr.eval(&Contexts);
        let _ = (expr.contexts(), expr.functions(), expr.asks_status());
    }
    for read in [template(text), condition(text)].into_iter().flatten() {
        let _ = read.eval(&Contexts);
        let _ = read.text(&Contexts);
        let _ = read.replace(|at, _| format!("${{__E{at}}}"));
    }
});
