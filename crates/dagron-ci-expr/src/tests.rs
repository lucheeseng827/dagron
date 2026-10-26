use super::*;

/// The contexts of a test, as JSON, and the status functions answered as a
/// job that has had a failure would answer them.
struct Contexts(serde_json::Value);

impl Host for Contexts {
    fn context(&self, name: &str) -> Option<Value> {
        self.0.get(name).cloned().map(Value::from)
    }

    fn call(&self, name: &str, args: &[Value]) -> Option<Result<Value, String>> {
        match name.to_ascii_lowercase().as_str() {
            "success" => Some(Ok(Value::Bool(false))),
            "failure" => Some(Ok(Value::Bool(true))),
            "always" => Some(Ok(Value::Bool(true))),
            "cancelled" => Some(Ok(Value::Bool(false))),
            "hashfiles" => Some(match args.first() {
                Some(pattern) => Ok(Value::String(format!("hash-of-{}", pattern.text()))),
                None => Err("names no files".to_string()),
            }),
            _ => None,
        }
    }
}

fn host() -> Contexts {
    Contexts(serde_json::json!({
        "github": {
            "event_name": "pull_request",
            "ref": "refs/heads/main",
            "sha": "0123abc",
            "event": { "number": 7, "pull_request": { "draft": false, "labels": [
                { "name": "bug" }, { "name": "ci" } ] } },
        },
        "env": { "TARGET": "release", "EMPTY": "", "Count": "3" },
        "matrix": { "os": "linux", "version": 16, "experimental": true },
        "needs": {
            "build": { "result": "success", "outputs": { "tag": "v1" } },
            "lint": { "result": "failure", "outputs": {} },
        },
        "steps": {},
        "inputs": { "list": "[\"a\", \"b\"]" },
    }))
}

fn value(text: &str) -> Value {
    parse(text)
        .unwrap_or_else(|e| panic!("{text}: {e}"))
        .eval(&host())
        .unwrap_or_else(|e| panic!("{text}: {e}"))
}

fn holds(text: &str) -> bool {
    value(text).truthy()
}

fn refused(text: &str) -> String {
    match parse(text) {
        Err(e) => e.message,
        Ok(expr) => expr.eval(&host()).expect_err(text).message,
    }
}

#[test]
fn literals_are_read_as_the_documentation_writes_them() {
    assert_eq!(value("null"), Value::Null);
    assert_eq!(value("true"), Value::Bool(true));
    assert_eq!(value("false"), Value::Bool(false));
    assert_eq!(value("711"), Value::Number(711.0));
    assert_eq!(value("-9.2"), Value::Number(-9.2));
    assert_eq!(value("0xff"), Value::Number(255.0));
    assert_eq!(value("-2.99e-2"), Value::Number(-0.0299));
    assert_eq!(value("'Mona the Octocat'"), Value::from("Mona the Octocat"));
    assert_eq!(
        value("'It''s open source!'"),
        Value::from("It's open source!")
    );
    assert_eq!(value("''"), Value::from(""));
    assert_eq!(value("  ( 1 )  "), Value::Number(1.0));

    assert!(refused("\"double\"").contains("single quotes"));
    assert!(refused("'open").contains("not closed"));
    assert!(refused("1.2.3").contains("is not a number"));
    assert!(refused("").contains("is empty"));
    assert!(refused("1 2").contains("past its end"));
    assert!(refused("(1").contains("expected a ')'"));
    assert!(refused("1 ==").contains("ends where a value is expected"));
    assert!(refused("a # b").contains("is not part of an expression"));
    assert!(refused("*").contains("follows a '.'"));
}

#[test]
fn a_context_is_read_by_name_and_what_is_not_there_is_null() {
    assert_eq!(value("github.event_name"), Value::from("pull_request"));
    assert_eq!(value("github['event_name']"), Value::from("pull_request"));
    assert_eq!(value("github.event.number"), Value::Number(7.0));
    assert_eq!(value("github.event.pull_request.draft"), Value::Bool(false));
    assert_eq!(
        value("github.event.pull_request.labels[1].name"),
        Value::from("ci")
    );
    assert_eq!(value("github.event.pull_request.labels[2]"), Value::Null);
    assert_eq!(value("github.event.nothing.at.all"), Value::Null);
    assert_eq!(value("env.NOPE"), Value::Null);
    assert_eq!(
        value("env.NOPE == ''"),
        Value::Bool(true),
        "and null is empty text"
    );
    // A property's name is read as written, and else ignoring case.
    assert_eq!(value("github.EVENT_NAME"), Value::from("pull_request"));
    assert_eq!(value("env.count"), Value::from("3"));
    assert_eq!(value("needs.build.outputs.tag"), Value::from("v1"));
    assert_eq!(value("matrix[github.event_name]"), Value::Null);
    // A context that is not known here is not taken for an empty one.
    assert_eq!(
        refused("secrets.TOKEN"),
        "the context `secrets` is not known here, or is not one"
    );
    assert_eq!(
        refused("nothing"),
        "the context `nothing` is not known here, or is not one"
    );
}

#[test]
fn every_value_of_an_object_and_what_is_read_of_each() {
    assert_eq!(
        value("needs.*.result"),
        Value::Array(vec![Value::from("success"), Value::from("failure")])
    );
    assert!(holds("contains(needs.*.result, 'failure')"));
    assert!(!holds("contains(needs.*.result, 'cancelled')"));
    assert_eq!(
        value("github.event.pull_request.labels.*.name"),
        Value::Array(vec![Value::from("bug"), Value::from("ci")])
    );
    assert_eq!(
        value("join(github.event.pull_request.labels.*.name, ', ')"),
        Value::from("bug, ci")
    );
    assert_eq!(value("env.*.nothing"), Value::Array(Vec::new()));
    assert_eq!(value("github.sha.*"), Value::Array(Vec::new()));
}

#[test]
fn operators_bind_as_the_documentation_orders_them() {
    assert!(holds("true || false && false"), "&& before ||");
    assert!(!holds("(true || false) && false"));
    assert!(holds("1 < 2 == true"), "a comparison before an equality");
    assert!(holds("!false && !null && !0 && !''"));
    assert!(!holds("!'text'"));
    assert!(holds("!!matrix.experimental"));
    // && and || give one of their operands, not a boolean.
    assert_eq!(value("env.EMPTY || 'fallback'"), Value::from("fallback"));
    assert_eq!(value("env.TARGET || 'fallback'"), Value::from("release"));
    assert_eq!(value("env.TARGET && 'set'"), Value::from("set"));
    assert_eq!(value("env.EMPTY && 'set'"), Value::from(""));
    assert_eq!(
        value("github.event_name == 'push' && 'deploy' || 'check'"),
        Value::from("check"),
        "the ternary people write"
    );
    // What is to the right of one that decides is not evaluated.
    assert!(holds("true || secrets.NOPE"));
    assert!(!holds("false && secrets.NOPE"));
}

#[test]
fn values_compare_loosely_as_the_documentation_says() {
    for yes in [
        "'Main' == 'main'",
        "github.ref == 'REFS/HEADS/MAIN'",
        "1 == 1.0",
        "'1' == 1",
        "'0x10' == 16",
        "true == 1",
        "false == 0",
        "null == 0",
        "null == ''",
        "null == false",
        "'' == 0",
        "' 12 ' == 12",
        "null == null",
        "matrix.version == 16",
        "matrix.version == '16'",
        "matrix.version >= 16 && matrix.version < 17",
        "'a' < 'B'",
        "'abc' != 'abd'",
        "true > false",
        "'text' != 0",
        "NaN != NaN",
        "matrix != matrix",
        "github.event != github.event",
    ] {
        assert!(holds(yes), "{yes}");
    }
    for no in [
        "'true' == true",
        "'text' == 0",
        "'text' < 1",
        "'text' > 1",
        "NaN == NaN",
        "matrix == matrix",
        "matrix == 'Object'",
        "github.event.pull_request.labels == 'Array'",
        "'10' < '9' == false",
        "null > 0",
        "'inf' == Infinity",
        "'nan' == 0",
    ] {
        assert!(!holds(no), "{no}");
    }
}

#[test]
fn the_functions_answer_as_the_documentation_says() {
    assert!(holds("contains('Hello world', 'LLO')"));
    assert!(!holds("contains('Hello world', 'bye')"));
    assert!(holds(
        "contains(fromJSON('[\"push\", \"pull_request\"]'), github.event_name)"
    ));
    assert!(
        holds("contains(fromJSON('[1, 2]'), '2')"),
        "an element is compared loosely"
    );
    assert!(
        !holds("contains(fromJSON('[\"pushed\"]'), 'push')"),
        "an element, not a part of one"
    );
    assert!(holds("contains(github.sha, 123)"));
    assert!(holds("startsWith(github.ref, 'REFS/heads/')"));
    assert!(!holds("startsWith(github.ref, 'refs/tags/')"));
    assert!(holds("endsWith(github.ref, '/main')"));
    assert!(
        holds("STARTSWITH('abc', 'a')"),
        "a function's name ignores case"
    );

    assert_eq!(
        value("format('Hello {0} {1} {0}', 'Mona', 7)"),
        Value::from("Hello Mona 7 Mona")
    );
    assert_eq!(
        value("format('{{Hello {0}}}', 'x')"),
        Value::from("{Hello x}")
    );
    assert_eq!(value("format('{0}', null)"), Value::from(""));
    assert_eq!(
        value("format('{0} {1}', true, matrix)"),
        Value::from("true Object")
    );
    assert!(refused("format('{1}', 'a')").contains("there is no argument 1"));
    assert!(refused("format('{x}', 'a')").contains("is not a number of an argument"));
    assert!(refused("format('{0', 'a')").contains("is not closed"));
    assert!(refused("format('0}', 'a')").contains("closes nothing"));

    assert_eq!(value("join(fromJSON(inputs.list))"), Value::from("a,b"));
    assert_eq!(
        value("join(fromJSON(inputs.list), ' and ')"),
        Value::from("a and b")
    );
    assert_eq!(value("join('alone', '-')"), Value::from("alone"));

    assert_eq!(value("toJSON(matrix.version)"), Value::from("16"));
    assert_eq!(value("toJSON('text')"), Value::from("\"text\""));
    assert_eq!(
        value("toJSON(needs.build)"),
        Value::from("{\n  \"outputs\": {\n    \"tag\": \"v1\"\n  },\n  \"result\": \"success\"\n}")
    );
    assert_eq!(
        value("fromJSON('{\"a\": {\"b\": [10, 20]}}').a.b[1]"),
        Value::Number(20.0)
    );
    assert_eq!(value("fromJSON('true')"), Value::Bool(true));
    assert!(refused("fromJSON('{nope')").contains("is not JSON"));

    assert!(refused("contains('a')").contains("takes 2 arguments, and has 1"));
    assert!(refused("join()").contains("takes 1 to 2 arguments"));
    assert!(refused("nosuch(1)").contains("`nosuch` is not a function that can be called here"));
}

#[test]
fn what_only_the_caller_knows_is_the_caller_s_to_answer() {
    assert!(holds("failure()"));
    assert!(!holds("success()"));
    assert!(holds("always() && !cancelled()"));
    assert_eq!(
        value("hashFiles('**/Cargo.lock')"),
        Value::from("hash-of-**/Cargo.lock")
    );
    assert!(refused("hashFiles()").contains("hashFiles: names no files"));

    let asks = |text: &str| parse(text).unwrap().asks_status();
    assert!(asks("failure()"));
    assert!(asks("github.ref == 'x' && !CANCELLED()"));
    assert!(asks("contains(needs.*.result, 'failure') || always()"));
    assert!(!asks("github.event_name == 'push'"));
    assert!(!asks("contains(needs.*.result, 'failure')"));
    assert_eq!(
        parse("format('{0}', hashFiles(env.X))")
            .unwrap()
            .functions(),
        ["format", "hashFiles"]
    );
}

#[test]
fn an_expression_says_which_contexts_it_reads() {
    let reads = |text: &str| parse(text).unwrap().contexts();
    let named = |c: &str, p: &str| (c.to_string(), Some(p.to_string()));
    assert_eq!(
        reads("github.ref == 'x' && env.TARGET != matrix['os']"),
        [
            named("github", "ref"),
            named("env", "TARGET"),
            named("matrix", "os")
        ]
    );
    assert_eq!(
        reads("github.event.pull_request.title"),
        [named("github", "event")]
    );
    assert_eq!(
        reads("matrix[env.KEY]"),
        [("matrix".to_string(), None), named("env", "KEY")]
    );
    assert_eq!(reads("toJSON(needs)"), [("needs".to_string(), None)]);
    assert_eq!(reads("needs.*.result"), [("needs".to_string(), None)]);
    assert_eq!(
        reads("contains(steps.build.outputs.ok, 'y')"),
        [named("steps", "build")]
    );
    assert!(reads("1 == 2 || 'env.X' == 'x'").is_empty());
}

#[test]
fn a_text_with_expressions_in_it_is_read_and_written_out() {
    let read = |text: &str| template(text).unwrap_or_else(|e| panic!("{text}: {e}"));
    assert!(read("no expression here, even with $ and {{ }}").is_plain());
    assert!(!read("a ${{ 1 }}").is_plain());

    let host = host();
    assert_eq!(
        read("build-${{ matrix.os }}-${{ matrix.version }}")
            .text(&host)
            .unwrap(),
        "build-linux-16"
    );
    assert_eq!(
        read("${{ github.event_name }}").text(&host).unwrap(),
        "pull_request"
    );
    // One expression and nothing else keeps its own type.
    assert_eq!(
        read("${{ matrix.version }}").eval(&host).unwrap(),
        Value::Number(16.0)
    );
    assert_eq!(
        read("${{ fromJSON(inputs.list) }}").eval(&host).unwrap(),
        Value::Array(vec![Value::from("a"), Value::from("b")])
    );
    assert_eq!(
        read(" ${{ matrix.version }}").eval(&host).unwrap(),
        Value::from(" 16")
    );
    // A `}}` inside a string of the expression does not end it.
    assert_eq!(
        read("${{ format('{{{0}}}', 'x') }}!").text(&host).unwrap(),
        "{x}!"
    );
    assert_eq!(read("${{ '}}' }}").text(&host).unwrap(), "}}");

    // In a script, each expression is replaced by a variable's name.
    let script = read("echo ${{ github.event.number }} and ${{ env.TARGET }}");
    assert_eq!(
        script.replace(|n, _| format!("\"$__E{n}\"")),
        "echo \"$__E0\" and \"$__E1\""
    );
    assert_eq!(script.exprs().count(), 2);

    assert!(template("${{ 1 ")
        .unwrap_err()
        .message
        .contains("not closed with }}"));
    let bad = template("ok ${{ 1 + }}").unwrap_err();
    assert!(
        bad.message.contains("is not part of an expression"),
        "{bad}"
    );
    assert_eq!(bad.at, 9, "where in the whole text");
}

#[test]
fn an_if_is_an_expression_with_or_without_its_braces() {
    let host = host();
    let holds = |text: &str| condition(text).unwrap().eval(&host).unwrap().truthy();
    assert!(holds("github.event_name == 'pull_request'"));
    assert!(holds("${{ github.event_name == 'pull_request' }}"));
    assert!(!holds("github.event_name == 'push'"));
    assert!(!holds("${{ github.event_name == 'push' }}"));
    assert!(holds("  failure()  "));
    // Partly inside its braces it is a text, which is true unless it is
    // empty: what GitHub does with one, and a mistake where it is written.
    assert!(holds("${{ github.event_name }} == 'push'"));
    assert!(condition("").is_err());
}

#[test]
fn an_expression_nested_past_reason_is_refused_and_not_run_out_of_stack_on() {
    let deep = format!("{}1{}", "(".repeat(40), ")".repeat(40));
    assert_eq!(value(&deep), Value::Number(1.0));
    let deeper = format!("{}1{}", "(".repeat(5000), ")".repeat(5000));
    assert!(refused(&deeper).contains("nests too deeply"));
    assert!(refused(&"!".repeat(5000)).contains("nests too deeply"));
    let calls = format!("{}1{}", "toJSON(".repeat(5000), ")".repeat(5000));
    assert!(refused(&calls).contains("nests too deeply"));
}

#[test]
fn a_chain_past_reason_is_refused_and_not_run_out_of_stack_on() {
    // A chain is a tree as tall as it is long, with no parenthesis in it:
    // each of these, read whole, would be one that nothing could walk.
    for link in [" || 1", " && 1", " == 1", " < 1", ".b", "[0]", ".*"] {
        let chain = format!("github{}", link.repeat(100_000));
        assert!(refused(&chain).contains("too deep"), "{link}");
    }
    // What somebody writes is read: a hundred alternatives, and no more.
    let written = format!("false{} || matrix.os", " || false".repeat(99));
    assert_eq!(value(&written), Value::String("linux".into()));
    let longer = format!("false{}", " || false".repeat(101));
    assert!(refused(&longer).contains("too deep"));
    // A chain inside a chain counts with it, whichever side it stands on,
    // and one beside another does not.
    let sixty = |what: &str| format!("{what}{}", " && true".repeat(60));
    let beside = format!("({}) || ({})", sixty("true"), sixty("true"));
    assert!(holds(&beside));
    let within = sixty(&format!("({})", sixty("true")));
    assert!(refused(&within).contains("too deep"));
    let after = format!("true && ({})", within);
    assert!(refused(&after).contains("too deep"));
    // A call's arguments, and an index, are counted as what they stand under.
    let args = format!("contains({}, 1)", sixty(&format!("({})", sixty("true"))));
    assert!(refused(&args).contains("too deep"));
    let index = format!("github[{}]", sixty(&format!("({})", sixty("true"))));
    assert!(refused(&index).contains("too deep"));
}

#[test]
fn a_value_is_written_as_text_the_way_a_workflow_shows_it() {
    assert_eq!(Value::Null.text(), "");
    assert_eq!(Value::Bool(true).text(), "true");
    assert_eq!(Value::Number(3.0).text(), "3");
    assert_eq!(Value::Number(-0.5).text(), "-0.5");
    assert_eq!(Value::Number(f64::NAN).text(), "NaN");
    assert_eq!(Value::Number(f64::INFINITY).text(), "Infinity");
    assert_eq!(Value::Array(Vec::new()).text(), "Array");
    assert_eq!(Value::Object(Vec::new()).text(), "Object");
    for (falsy, why) in [
        (Value::Null, "null"),
        (Value::Bool(false), "false"),
        (Value::Number(0.0), "0"),
        (Value::Number(f64::NAN), "NaN"),
        (Value::from(""), "an empty string"),
    ] {
        assert!(!falsy.truthy(), "{why}");
    }
    assert!(
        Value::from("false").truthy(),
        "a string that says false is a string"
    );
    assert!(Value::Array(Vec::new()).truthy());
    // And back to JSON, for a context handed on.
    let json = serde_json::Value::from(&Value::from(serde_json::json!({ "a": [1, 2.5, null] })));
    assert_eq!(json, serde_json::json!({ "a": [1, 2.5, null] }));
}
