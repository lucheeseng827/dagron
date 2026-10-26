//! GitHub Actions' expression language: what stands between `${{` and `}}`
//! in a workflow file, and the whole of an `if:`.
//!
//! Written to GitHub's documentation of it ("Evaluate expressions in
//! workflows and actions"), and to nothing else: no code of GitHub's runner
//! was read.
//!
//! - **Literals**: `null`, `true`, `false`, numbers (`711`, `-9.2`, `0xff`,
//!   `2.99e-2`), and strings in single quotes, where `''` is one quote.
//! - **Operators**, tightest first: `( )`, `[ ]`, `.`, `!`, `< <= > >=`,
//!   `== !=`, `&&`, `||`.
//! - **Contexts**: a name (`github`, `env`, `matrix`, …) and what is read of
//!   it with `.name`, `['name']` and `[0]`. A property that is not there is
//!   `null`. `.*` is every value of an object, or every element of an array,
//!   and what is read after it is read of each.
//! - **Functions**: `contains`, `startsWith`, `endsWith`, `format`, `join`,
//!   `toJSON` and `fromJSON` are here. `hashFiles` and the status functions
//!   (`success`, `always`, `cancelled`, `failure`) are the caller's to
//!   answer, since only it knows the files and how the job stands: see
//!   [`Host`].
//!
//! **How values compare.** Loosely, as the documentation has it: two values
//! of different types are compared as numbers (`null` is 0, `true` is 1, a
//! string is the number it spells or `NaN`, an array or an object is `NaN`),
//! `NaN` equals nothing, and strings are compared ignoring case.
//!
//! **What a context holds is the caller's.** The same expression is read
//! where a workflow is compiled, when only its event is known, and again
//! while a job runs. A [`Host`] that answers `None` for a context says it is
//! not known here, and the expression fails with which one: nothing is
//! taken for empty that was only not asked yet.

use std::fmt;

/// The globs a workflow's filters and `hashFiles` take.
pub mod glob;
pub mod matrix;
pub mod uses;

/// A value an expression reads or makes.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<Value>),
    /// In the order its keys were given.
    Object(Vec<(String, Value)>),
}

impl From<serde_json::Value> for Value {
    fn from(json: serde_json::Value) -> Self {
        match json {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => Value::Number(n.as_f64().unwrap_or(f64::NAN)),
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Array(items) => {
                Value::Array(items.into_iter().map(Value::from).collect())
            }
            serde_json::Value::Object(map) => {
                Value::Object(map.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
            }
        }
    }
}

impl From<&Value> for serde_json::Value {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            // A whole number is written as one, and what JSON cannot hold
            // (NaN, an infinity) as null.
            Value::Number(n) if n.fract() == 0.0 && n.abs() < 9.0e15 => {
                serde_json::Value::Number((*n as i64).into())
            }
            Value::Number(n) => serde_json::Number::from_f64(*n)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Array(items) => {
                serde_json::Value::Array(items.iter().map(serde_json::Value::from).collect())
            }
            Value::Object(fields) => serde_json::Value::Object(
                fields
                    .iter()
                    .map(|(k, v)| (k.clone(), serde_json::Value::from(v)))
                    .collect(),
            ),
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl Value {
    /// Whether a condition holds for this value: everything but `false`,
    /// `0`, `NaN`, an empty string and `null`.
    pub fn truthy(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::Number(n) => *n != 0.0 && !n.is_nan(),
            Value::String(s) => !s.is_empty(),
            Value::Array(_) | Value::Object(_) => true,
        }
    }

    /// The value as text, as it is written into a string: `null` is
    /// nothing, a whole number has no fraction, and an array or an object
    /// is its kind's name.
    pub fn text(&self) -> String {
        match self {
            Value::Null => String::new(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => number_text(*n),
            Value::String(s) => s.clone(),
            Value::Array(_) => "Array".to_string(),
            Value::Object(_) => "Object".to_string(),
        }
    }

    /// The value as a number, for a comparison across types.
    fn number(&self) -> f64 {
        match self {
            Value::Null => 0.0,
            Value::Bool(b) => f64::from(u8::from(*b)),
            Value::Number(n) => *n,
            Value::String(s) => parse_number(s.trim()).unwrap_or(f64::NAN),
            Value::Array(_) | Value::Object(_) => f64::NAN,
        }
    }

    /// A property of an object, by its name: as written, and else ignoring
    /// case. `Null` where there is none, or where this is no object.
    pub fn get(&self, name: &str) -> Value {
        let Value::Object(fields) = self else {
            return Value::Null;
        };
        fields
            .iter()
            .find(|(k, _)| k == name)
            .or_else(|| fields.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)))
            .map(|(_, v)| v.clone())
            .unwrap_or(Value::Null)
    }
}

/// A number as text: whole numbers without a fraction, the rest as short as
/// reads back the same.
fn number_text(n: f64) -> String {
    if n.is_nan() {
        "NaN".to_string()
    } else if n.is_infinite() {
        if n > 0.0 { "Infinity" } else { "-Infinity" }.to_string()
    } else if n.fract() == 0.0 && n.abs() < 1.0e15 {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

/// A number as an expression or a string spells one: decimal, with a sign, a
/// fraction or an exponent, or hexadecimal. An empty string is 0. `None` for
/// anything else, `inf` and `nan` among it.
fn parse_number(text: &str) -> Option<f64> {
    if text.is_empty() {
        return Some(0.0);
    }
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let sign = if negative { -1.0 } else { 1.0 };
    if let Some(hex) = digits
        .strip_prefix("0x")
        .or_else(|| digits.strip_prefix("0X"))
    {
        return u64::from_str_radix(hex, 16).ok().map(|n| sign * n as f64);
    }
    let shaped = digits.starts_with(|c: char| c.is_ascii_digit() || c == '.')
        && digits
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, '.' | 'e' | 'E' | '+' | '-'));
    if !shaped {
        return None;
    }
    digits.parse::<f64>().ok().map(|n| sign * n)
}

/// What is wrong with an expression, and where in its text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    /// The byte the problem is at; the text's length for one at its end.
    pub at: usize,
    pub message: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

fn error<T>(at: usize, message: impl Into<String>) -> Result<T, Error> {
    Err(Error {
        at,
        message: message.into(),
    })
}

// ---- reading ------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Number(f64),
    Text(String),
    Name(String),
    Open,
    Close,
    OpenIndex,
    CloseIndex,
    Dot,
    Comma,
    Star,
    Not,
    Op(Op),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    Equal,
    NotEqual,
    And,
    Or,
}

fn tokens(text: &str) -> Result<Vec<(usize, Token)>, Error> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        let at = i;
        let two = text.get(i..i + 2).unwrap_or_default();
        let (token, len) = match c {
            ' ' | '\t' | '\r' | '\n' => {
                i += 1;
                continue;
            }
            '(' => (Token::Open, 1),
            ')' => (Token::Close, 1),
            '[' => (Token::OpenIndex, 1),
            ']' => (Token::CloseIndex, 1),
            ',' => (Token::Comma, 1),
            '*' => (Token::Star, 1),
            '\'' => {
                let mut s = String::new();
                let mut j = i + 1;
                loop {
                    let Some(rest) = text.get(j..) else {
                        return error(at, "a string is not closed with a '");
                    };
                    let Some(quote) = rest.find('\'') else {
                        return error(at, "a string is not closed with a '");
                    };
                    s.push_str(&rest[..quote]);
                    j += quote + 1;
                    // Two quotes are one, inside the string.
                    if text[j..].starts_with('\'') {
                        s.push('\'');
                        j += 1;
                    } else {
                        break;
                    }
                }
                let len = j - i;
                (Token::Text(s), len)
            }
            '"' => {
                return error(
                    at,
                    "a string is written in single quotes: 'text', and '' for a quote in it",
                )
            }
            _ if two == "==" => (Token::Op(Op::Equal), 2),
            _ if two == "!=" => (Token::Op(Op::NotEqual), 2),
            _ if two == "<=" => (Token::Op(Op::LessOrEqual), 2),
            _ if two == ">=" => (Token::Op(Op::GreaterOrEqual), 2),
            _ if two == "&&" => (Token::Op(Op::And), 2),
            _ if two == "||" => (Token::Op(Op::Or), 2),
            '<' => (Token::Op(Op::Less), 1),
            '>' => (Token::Op(Op::Greater), 1),
            '!' => (Token::Not, 1),
            // A number: a digit, or a sign or a point with a digit after it.
            _ if c.is_ascii_digit()
                || (matches!(c, '-' | '+' | '.')
                    && bytes
                        .get(i + 1)
                        .is_some_and(|b| b.is_ascii_digit() || *b == b'.')) =>
            {
                let mut j = i + 1;
                while j < bytes.len() {
                    let b = bytes[j] as char;
                    let exponent_sign = matches!(b, '+' | '-')
                        && matches!(bytes[j - 1] as char, 'e' | 'E')
                        && !text[i..j].contains("0x")
                        && !text[i..j].contains("0X");
                    if b.is_ascii_alphanumeric() || b == '.' || exponent_sign {
                        j += 1;
                    } else {
                        break;
                    }
                }
                let Some(n) = parse_number(&text[i..j]) else {
                    return error(at, format!("{:?} is not a number", &text[i..j]));
                };
                (Token::Number(n), j - i)
            }
            '.' => (Token::Dot, 1),
            _ if c.is_ascii_alphabetic() || c == '_' => {
                let mut j = i + 1;
                while j < bytes.len()
                    && ((bytes[j] as char).is_ascii_alphanumeric()
                        || matches!(bytes[j], b'_' | b'-'))
                {
                    j += 1;
                }
                (Token::Name(text[i..j].to_string()), j - i)
            }
            _ => {
                let shown = text[i..].chars().next().unwrap_or(c);
                return error(at, format!("{shown:?} is not part of an expression"));
            }
        };
        out.push((at, token));
        i += len;
    }
    Ok(out)
}

#[derive(Clone, Debug, PartialEq)]
enum Node {
    Literal(Value),
    /// A context's name, or a literal's (`true`, `null`), told apart when read.
    Context(String),
    Property(Box<Node>, String),
    Index(Box<Node>, Box<Node>),
    /// `.*`: every value of what is before it.
    Every(Box<Node>),
    Call(usize, String, Vec<Node>),
    Not(Box<Node>),
    Binary(Op, Box<Node>, Box<Node>),
}

struct Parser<'a> {
    text: &'a str,
    tokens: Vec<(usize, Token)>,
    next: usize,
    depth: usize,
}

/// How deeply an expression may nest: a parenthesis in a parenthesis, a call
/// in a call. Past it a text is refused, which nobody writes by hand.
const MAX_DEPTH: usize = 50;

/// How tall an expression's tree may stand: its operators, properties,
/// indexes and calls, one over another. A chain is as tall as it is long
/// (`a || b || c` is two, and so is `a.b.c`), so this is also how many of
/// them may follow one another. Whatever reads a tree goes down it by calling
/// itself (evaluating it, comparing it, dropping it), and a text of some
/// thousands of `||` would take the stack, and the process with it. Past
/// this a text is refused instead, which nobody writes by hand either.
const MAX_HEIGHT: usize = 100;

/// A node as the parser holds one: with how tall it stands.
struct Tall {
    node: Node,
    height: usize,
}

/// A node that stands on no other: a literal, a context's name.
fn leaf(node: Node) -> Tall {
    Tall { node, height: 0 }
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.next).map(|(_, t)| t)
    }

    fn at(&self) -> usize {
        self.tokens
            .get(self.next)
            .map_or(self.text.len(), |(at, _)| *at)
    }

    fn take(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.next).map(|(_, t)| t.clone());
        self.next += 1;
        token
    }

    fn expect(&mut self, want: Token, what: &str) -> Result<(), Error> {
        let at = self.at();
        match self.take() {
            Some(token) if token == want => Ok(()),
            _ => error(at, format!("expected {what}")),
        }
    }

    /// The node that stands on others, the tallest of them `below` high: one
    /// higher than it, and refused past [`MAX_HEIGHT`]. `at` is where the
    /// node is written.
    fn over(&self, at: usize, below: usize, node: Node) -> Result<Tall, Error> {
        if below >= MAX_HEIGHT {
            return error(
                at,
                format!(
                    "the expression is too deep: more than {MAX_HEIGHT} of its operators, \
                     properties and calls stand one on another"
                ),
            );
        }
        Ok(Tall {
            node,
            height: below + 1,
        })
    }

    fn binary(&self, at: usize, op: Op, left: Tall, right: Tall) -> Result<Tall, Error> {
        let below = left.height.max(right.height);
        let node = Node::Binary(op, Box::new(left.node), Box::new(right.node));
        self.over(at, below, node)
    }

    // or := and ('||' and)*
    fn or(&mut self) -> Result<Tall, Error> {
        let mut left = self.and()?;
        while self.peek() == Some(&Token::Op(Op::Or)) {
            let at = self.at();
            self.next += 1;
            let right = self.and()?;
            left = self.binary(at, Op::Or, left, right)?;
        }
        Ok(left)
    }

    fn and(&mut self) -> Result<Tall, Error> {
        let mut left = self.equality()?;
        while self.peek() == Some(&Token::Op(Op::And)) {
            let at = self.at();
            self.next += 1;
            let right = self.equality()?;
            left = self.binary(at, Op::And, left, right)?;
        }
        Ok(left)
    }

    fn equality(&mut self) -> Result<Tall, Error> {
        let mut left = self.comparison()?;
        while let Some(Token::Op(op @ (Op::Equal | Op::NotEqual))) = self.peek().cloned() {
            let at = self.at();
            self.next += 1;
            let right = self.comparison()?;
            left = self.binary(at, op, left, right)?;
        }
        Ok(left)
    }

    fn comparison(&mut self) -> Result<Tall, Error> {
        let mut left = self.unary()?;
        while let Some(Token::Op(
            op @ (Op::Less | Op::LessOrEqual | Op::Greater | Op::GreaterOrEqual),
        )) = self.peek().cloned()
        {
            let at = self.at();
            self.next += 1;
            let right = self.unary()?;
            left = self.binary(at, op, left, right)?;
        }
        Ok(left)
    }

    fn unary(&mut self) -> Result<Tall, Error> {
        if self.peek() == Some(&Token::Not) {
            let at = self.at();
            self.next += 1;
            self.depth += 1;
            if self.depth > MAX_DEPTH {
                return error(at, "the expression nests too deeply");
            }
            let inner = self.unary()?;
            self.depth -= 1;
            return self.over(at, inner.height, Node::Not(Box::new(inner.node)));
        }
        self.postfix()
    }

    fn postfix(&mut self) -> Result<Tall, Error> {
        let mut tall = self.primary()?;
        loop {
            let at = self.at();
            let Tall { node, height } = tall;
            tall = match self.peek() {
                Some(Token::Dot) => {
                    self.next += 1;
                    let after = self.at();
                    let node = match self.take() {
                        Some(Token::Name(name)) => Node::Property(Box::new(node), name),
                        Some(Token::Star) => Node::Every(Box::new(node)),
                        _ => return error(after, "expected a property's name after '.'"),
                    };
                    self.over(at, height, node)?
                }
                Some(Token::OpenIndex) => {
                    self.next += 1;
                    let index = self.nested()?;
                    self.expect(Token::CloseIndex, "a ']'")?;
                    let below = height.max(index.height);
                    self.over(at, below, Node::Index(Box::new(node), Box::new(index.node)))?
                }
                _ => return Ok(Tall { node, height }),
            };
        }
    }

    /// An expression inside brackets or a call, counted against the depth.
    fn nested(&mut self) -> Result<Tall, Error> {
        let at = self.at();
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return error(at, "the expression nests too deeply");
        }
        let node = self.or()?;
        self.depth -= 1;
        Ok(node)
    }

    fn primary(&mut self) -> Result<Tall, Error> {
        let at = self.at();
        match self.take() {
            Some(Token::Number(n)) => Ok(leaf(Node::Literal(Value::Number(n)))),
            Some(Token::Text(s)) => Ok(leaf(Node::Literal(Value::String(s)))),
            Some(Token::Open) => {
                let inner = self.nested()?;
                self.expect(Token::Close, "a ')'")?;
                Ok(inner)
            }
            Some(Token::Name(name)) => {
                if self.peek() == Some(&Token::Open) {
                    self.next += 1;
                    let mut args = Vec::new();
                    let mut below = 0;
                    if self.peek() == Some(&Token::Close) {
                        self.next += 1;
                    } else {
                        loop {
                            let arg = self.nested()?;
                            below = below.max(arg.height);
                            args.push(arg.node);
                            let at = self.at();
                            match self.take() {
                                Some(Token::Comma) => {}
                                Some(Token::Close) => break,
                                _ => return error(at, "expected ',' or ')' in a call"),
                            }
                        }
                    }
                    return self.over(at, below, Node::Call(at, name, args));
                }
                Ok(leaf(match name.as_str() {
                    "true" => Node::Literal(Value::Bool(true)),
                    "false" => Node::Literal(Value::Bool(false)),
                    "null" => Node::Literal(Value::Null),
                    // As a comparison's `NaN` and `Infinity` are written.
                    "NaN" => Node::Literal(Value::Number(f64::NAN)),
                    "Infinity" => Node::Literal(Value::Number(f64::INFINITY)),
                    _ => Node::Context(name),
                }))
            }
            Some(Token::Star) => error(at, "'*' follows a '.', as in needs.*.result"),
            Some(_) => error(at, "expected a value"),
            None => error(at, "the expression ends where a value is expected"),
        }
    }
}

/// An expression, read and ready to be evaluated.
#[derive(Clone, Debug, PartialEq)]
pub struct Expr {
    node: Node,
}

/// Read one expression: what stands between `${{` and `}}`, or an `if:`
/// written without them.
pub fn parse(text: &str) -> Result<Expr, Error> {
    let tokens = tokens(text)?;
    if tokens.is_empty() {
        return error(0, "the expression is empty");
    }
    let mut parser = Parser {
        text,
        tokens,
        next: 0,
        depth: 0,
    };
    let node = parser.or()?.node;
    if parser.next < parser.tokens.len() {
        return error(parser.at(), "the expression goes on past its end");
    }
    Ok(Expr { node })
}

// ---- evaluating ---------------------------------------------------------

/// What an expression is evaluated against: the contexts, and the functions
/// only its caller can answer.
pub trait Host {
    /// The context of this name (`github`, `env`, `matrix`, …), all of it.
    /// `None` where it is not known here: the expression then fails, saying
    /// which.
    fn context(&self, name: &str) -> Option<Value>;

    /// A function this crate does not answer itself: `hashFiles`, and the
    /// status functions. `None` where the caller has no such function.
    fn call(&self, _name: &str, _args: &[Value]) -> Option<Result<Value, String>> {
        None
    }
}

/// The functions answered here, as GitHub spells them. Their names are read
/// ignoring case.
pub const FUNCTIONS: &[&str] = &[
    "contains",
    "endsWith",
    "format",
    "fromJSON",
    "join",
    "startsWith",
    "toJSON",
];

/// The status functions, which say how the job or the workflow stands. An
/// `if:` that calls none of them holds only where nothing before it failed,
/// as if it began `success() &&`.
pub const STATUS_FUNCTIONS: &[&str] = &["always", "cancelled", "failure", "success"];

/// What of the `github` context no outsider chooses: an expression that
/// reads only these, a matrix or the runner is [`Expr::pasted`].
pub const NOT_AN_OUTSIDERS: &[&str] = &[
    "action",
    "api_url",
    "event_name",
    "job",
    "ref_type",
    "repository",
    "repository_owner",
    "retention_days",
    "run_attempt",
    "run_id",
    "run_number",
    "server_url",
    "sha",
    "workflow",
    "workspace",
];

impl Expr {
    /// The value of the expression against `host`.
    pub fn eval(&self, host: &dyn Host) -> Result<Value, Error> {
        eval(&self.node, host).map(|(value, _)| value)
    }

    /// Each context the expression reads, with the first property read of
    /// it where that is written as a name: `github.ref` is `("github",
    /// Some("ref"))`, and `matrix[x]` is `("matrix", None)`.
    pub fn contexts(&self) -> Vec<(String, Option<String>)> {
        let mut out = Vec::new();
        contexts(&self.node, &mut out);
        out
    }

    /// The name of each function the expression calls, as written.
    pub fn functions(&self) -> Vec<String> {
        let mut out = Vec::new();
        functions(&self.node, &mut out);
        out
    }

    /// Whether the expression calls a status function: one that does not is
    /// an `if:` that holds only where nothing before it failed.
    pub fn asks_status(&self) -> bool {
        self.functions()
            .iter()
            .any(|f| STATUS_FUNCTIONS.iter().any(|s| s.eq_ignore_ascii_case(f)))
    }

    /// Whether the expression's value is no outsider's to choose, whatever
    /// the event: it reads only a matrix, the runner, the job, `vars`, and
    /// what of `github` is in [`NOT_AN_OUTSIDERS`]. Such an expression is
    /// pasted into a script as GitHub pastes it ([`script`]).
    pub fn pasted(&self) -> bool {
        self.contexts().iter().all(|(context, property)| {
            match (context.to_ascii_lowercase().as_str(), property) {
                ("matrix" | "strategy" | "runner" | "job" | "vars", _) => true,
                ("github", Some(property)) => {
                    NOT_AN_OUTSIDERS.contains(&property.to_ascii_lowercase().as_str())
                }
                _ => false,
            }
        })
    }
}

fn contexts(node: &Node, out: &mut Vec<(String, Option<String>)>) {
    match node {
        Node::Literal(_) => {}
        Node::Context(name) => out.push((name.clone(), None)),
        Node::Property(base, property) => match base.as_ref() {
            Node::Context(name) => out.push((name.clone(), Some(property.clone()))),
            other => contexts(other, out),
        },
        Node::Index(base, index) => {
            // A property written as an index, by a literal name.
            match (base.as_ref(), index.as_ref()) {
                (Node::Context(name), Node::Literal(Value::String(property))) => {
                    out.push((name.clone(), Some(property.clone())));
                }
                _ => {
                    contexts(base, out);
                    contexts(index, out);
                }
            }
        }
        Node::Every(base) | Node::Not(base) => contexts(base, out),
        Node::Call(_, _, args) => args.iter().for_each(|arg| contexts(arg, out)),
        Node::Binary(_, left, right) => {
            contexts(left, out);
            contexts(right, out);
        }
    }
}

fn functions(node: &Node, out: &mut Vec<String>) {
    match node {
        Node::Literal(_) | Node::Context(_) => {}
        Node::Property(base, _) | Node::Every(base) | Node::Not(base) => functions(base, out),
        Node::Index(base, index) => {
            functions(base, out);
            functions(index, out);
        }
        Node::Call(_, name, args) => {
            out.push(name.clone());
            args.iter().for_each(|arg| functions(arg, out));
        }
        Node::Binary(_, left, right) => {
            functions(left, out);
            functions(right, out);
        }
    }
}

/// A node's value, and whether it is the result of a `.*`: what is read of
/// such a value is read of each of its elements.
fn eval(node: &Node, host: &dyn Host) -> Result<(Value, bool), Error> {
    Ok(match node {
        Node::Literal(value) => (value.clone(), false),
        Node::Context(name) => match host.context(name) {
            Some(value) => (value, false),
            None => {
                return error(
                    0,
                    format!("the context `{name}` is not known here, or is not one"),
                )
            }
        },
        Node::Property(base, name) => {
            let (base, every) = eval(base, host)?;
            if every {
                let Value::Array(items) = base else {
                    return Ok((Value::Array(Vec::new()), true));
                };
                let read = items
                    .iter()
                    .filter(|item| matches!(item, Value::Object(_)))
                    .map(|item| item.get(name))
                    .collect();
                (Value::Array(read), true)
            } else {
                (base.get(name), false)
            }
        }
        Node::Index(base, index) => {
            let (base, every) = eval(base, host)?;
            let (index, _) = eval(index, host)?;
            let read = |value: &Value| match (value, &index) {
                (Value::Array(items), index) => {
                    let n = index.number();
                    if n.fract() == 0.0 && n >= 0.0 && (n as usize) < items.len() {
                        items[n as usize].clone()
                    } else {
                        Value::Null
                    }
                }
                (object @ Value::Object(_), index) => object.get(&index.text()),
                _ => Value::Null,
            };
            match (every, &base) {
                (true, Value::Array(items)) => {
                    (Value::Array(items.iter().map(read).collect()), true)
                }
                _ => (read(&base), false),
            }
        }
        Node::Every(base) => {
            let (base, every) = eval(base, host)?;
            let spread = |value: &Value| match value {
                Value::Object(fields) => fields.iter().map(|(_, v)| v.clone()).collect(),
                Value::Array(items) => items.clone(),
                _ => Vec::new(),
            };
            let values = match (every, &base) {
                (true, Value::Array(items)) => items.iter().flat_map(spread).collect(),
                _ => spread(&base),
            };
            (Value::Array(values), true)
        }
        Node::Not(inner) => (Value::Bool(!eval(inner, host)?.0.truthy()), false),
        Node::Binary(Op::And, left, right) => {
            let (left, _) = eval(left, host)?;
            if left.truthy() {
                (eval(right, host)?.0, false)
            } else {
                (left, false)
            }
        }
        Node::Binary(Op::Or, left, right) => {
            let (left, _) = eval(left, host)?;
            if left.truthy() {
                (left, false)
            } else {
                (eval(right, host)?.0, false)
            }
        }
        Node::Binary(op, left, right) => {
            let (left, _) = eval(left, host)?;
            let (right, _) = eval(right, host)?;
            (Value::Bool(compare(*op, &left, &right)), false)
        }
        Node::Call(at, name, args) => {
            let mut values = Vec::with_capacity(args.len());
            for arg in args {
                values.push(eval(arg, host)?.0);
            }
            (call(*at, name, &values, host)?, false)
        }
    })
}

/// `left OP right`, loosely: values of one type as that type, and of two as
/// numbers.
fn compare(op: Op, left: &Value, right: &Value) -> bool {
    use std::cmp::Ordering;
    let ordering: Option<Ordering> = match (left, right) {
        (Value::String(a), Value::String(b)) => Some(a.to_lowercase().cmp(&b.to_lowercase())),
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
        // The same array or object is equal to itself only, and a value
        // read twice is two: neither equals nor orders.
        (Value::Array(_), Value::Array(_)) | (Value::Object(_), Value::Object(_)) => None,
        _ => left.number().partial_cmp(&right.number()),
    };
    match (op, ordering) {
        (Op::NotEqual, None) => true,
        (_, None) => false,
        (Op::Equal, Some(o)) => o == Ordering::Equal,
        (Op::NotEqual, Some(o)) => o != Ordering::Equal,
        (Op::Less, Some(o)) => o == Ordering::Less,
        (Op::LessOrEqual, Some(o)) => o != Ordering::Greater,
        (Op::Greater, Some(o)) => o == Ordering::Greater,
        (Op::GreaterOrEqual, Some(o)) => o != Ordering::Less,
        (Op::And | Op::Or, _) => false,
    }
}

fn call(at: usize, name: &str, args: &[Value], host: &dyn Host) -> Result<Value, Error> {
    let takes = |least: usize, most: usize| -> Result<(), Error> {
        if (least..=most).contains(&args.len()) {
            Ok(())
        } else if least == most {
            error(
                at,
                format!("{name} takes {least} arguments, and has {}", args.len()),
            )
        } else {
            error(
                at,
                format!(
                    "{name} takes {least} to {most} arguments, and has {}",
                    args.len()
                ),
            )
        }
    };
    let lower = |value: &Value| value.text().to_lowercase();
    match name.to_ascii_lowercase().as_str() {
        "contains" => {
            takes(2, 2)?;
            Ok(Value::Bool(match &args[0] {
                Value::Array(items) => items.iter().any(|item| compare(Op::Equal, item, &args[1])),
                search => lower(search).contains(&lower(&args[1])),
            }))
        }
        "startswith" => {
            takes(2, 2)?;
            Ok(Value::Bool(lower(&args[0]).starts_with(&lower(&args[1]))))
        }
        "endswith" => {
            takes(2, 2)?;
            Ok(Value::Bool(lower(&args[0]).ends_with(&lower(&args[1]))))
        }
        "format" => {
            if args.is_empty() {
                return error(at, "format takes a text and what to put in it");
            }
            format(at, &args[0].text(), &args[1..]).map(Value::String)
        }
        "join" => {
            takes(1, 2)?;
            let separator = args.get(1).map_or(",".to_string(), Value::text);
            Ok(Value::String(match &args[0] {
                Value::Array(items) => items
                    .iter()
                    .map(Value::text)
                    .collect::<Vec<_>>()
                    .join(&separator),
                other => other.text(),
            }))
        }
        "tojson" => {
            takes(1, 1)?;
            let json = serde_json::Value::from(&args[0]);
            Ok(Value::String(
                serde_json::to_string_pretty(&json).unwrap_or_default(),
            ))
        }
        "fromjson" => {
            takes(1, 1)?;
            let text = args[0].text();
            serde_json::from_str::<serde_json::Value>(&text)
                .map(Value::from)
                .or_else(|e| error(at, format!("fromJSON was given what is not JSON: {e}")))
        }
        _ => match host.call(name, args) {
            Some(Ok(value)) => Ok(value),
            Some(Err(why)) => error(at, format!("{name}: {why}")),
            None => error(
                at,
                format!("`{name}` is not a function that can be called here"),
            ),
        },
    }
}

/// `format('{0} of {1}', a, b)`: each `{N}` is the Nth of `args`, and `{{`
/// and `}}` are a brace each.
fn format(at: usize, text: &str, args: &[Value]) -> Result<String, Error> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(i) = rest.find(['{', '}']) {
        out.push_str(&rest[..i]);
        let after = &rest[i..];
        if after.starts_with("{{") {
            out.push('{');
            rest = &after[2..];
        } else if after.starts_with("}}") {
            out.push('}');
            rest = &after[2..];
        } else if let Some(inner) = after.strip_prefix('{') {
            let Some(close) = inner.find('}') else {
                return error(at, "format: a '{' is not closed");
            };
            let Ok(n) = inner[..close].trim().parse::<usize>() else {
                return error(
                    at,
                    format!(
                        "format: {{{}}} is not a number of an argument",
                        &inner[..close]
                    ),
                );
            };
            let Some(arg) = args.get(n) else {
                return error(at, format!("format: there is no argument {n}"));
            };
            out.push_str(&arg.text());
            rest = &inner[close + 1..];
        } else {
            return error(at, "format: a '}' closes nothing; write '}}' for a brace");
        }
    }
    out.push_str(rest);
    Ok(out)
}

// ---- text with expressions in it ----------------------------------------

/// A text as a workflow has one: with any number of `${{ … }}` in it.
#[derive(Clone, Debug, PartialEq)]
pub struct Template {
    parts: Vec<Part>,
}

#[derive(Clone, Debug, PartialEq)]
enum Part {
    Text(String),
    Expr(Expr),
}

/// Read a text with `${{ … }}` in it. An expression's end is the first `}}`
/// outside a string of its own.
pub fn template(text: &str) -> Result<Template, Error> {
    let mut parts = Vec::new();
    let mut rest = text;
    let mut offset = 0;
    while let Some(open) = rest.find("${{") {
        if open > 0 {
            parts.push(Part::Text(rest[..open].to_string()));
        }
        let body = &rest[open + 3..];
        let Some(close) = expression_end(body) else {
            return error(offset + open, "a ${{ is not closed with }}");
        };
        let expr = parse(&body[..close]).map_err(|e| Error {
            at: offset + open + 3 + e.at,
            message: e.message,
        })?;
        parts.push(Part::Expr(expr));
        let consumed = open + 3 + close + 2;
        offset += consumed;
        rest = &rest[consumed..];
    }
    if !rest.is_empty() {
        parts.push(Part::Text(rest.to_string()));
    }
    Ok(Template { parts })
}

/// Where the `}}` that ends an expression is, in what follows its `${{`.
fn expression_end(body: &str) -> Option<usize> {
    let bytes = body.as_bytes();
    let mut i = 0;
    let mut quoted = false;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => quoted = !quoted,
            b'}' if !quoted && bytes.get(i + 1) == Some(&b'}') => return Some(i),
            _ => {}
        }
        i += 1;
    }
    None
}

impl Template {
    /// Whether the text has no expression in it.
    pub fn is_plain(&self) -> bool {
        self.parts.iter().all(|p| matches!(p, Part::Text(_)))
    }

    /// The expressions in the text, in order.
    pub fn exprs(&self) -> impl Iterator<Item = &Expr> {
        self.parts.iter().filter_map(|p| match p {
            Part::Expr(e) => Some(e),
            Part::Text(_) => None,
        })
    }

    /// The text's value. One that is a single expression and nothing else
    /// keeps the expression's own type (an array for a matrix, a boolean for
    /// a condition); any other is text, each expression written into it.
    pub fn eval(&self, host: &dyn Host) -> Result<Value, Error> {
        if let [Part::Expr(only)] = self.parts.as_slice() {
            return only.eval(host);
        }
        Ok(Value::String(self.text(host)?))
    }

    /// The text, each expression written into it.
    pub fn text(&self, host: &dyn Host) -> Result<String, Error> {
        let mut out = String::new();
        for part in &self.parts {
            match part {
                Part::Text(text) => out.push_str(text),
                Part::Expr(expr) => out.push_str(&expr.eval(host)?.text()),
            }
        }
        Ok(out)
    }

    /// The text with each expression replaced by what `with` gives for it:
    /// how an expression in a script becomes a variable's name there.
    pub fn replace(&self, mut with: impl FnMut(usize, &Expr) -> String) -> String {
        let mut out = String::new();
        let mut n = 0;
        for part in &self.parts {
            match part {
                Part::Text(text) => out.push_str(text),
                Part::Expr(expr) => {
                    out.push_str(&with(n, expr));
                    n += 1;
                }
            }
        }
        out
    }
}

/// The text of each `${{ … }}` of `text`, without its braces, in order: the
/// source of each expression a [`Template`] of it holds. A `}}` inside a
/// quoted string is the string's, as GitHub reads it.
pub fn sources(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(open) = rest.find("${{") {
        let body = &rest[open + 3..];
        let mut quoted = false;
        let bytes = body.as_bytes();
        let mut end = None;
        for i in 0..bytes.len() {
            match bytes[i] {
                b'\'' => quoted = !quoted,
                b'}' if !quoted && bytes.get(i + 1) == Some(&b'}') => {
                    end = Some(i);
                    break;
                }
                _ => {}
            }
        }
        let Some(end) = end else { break };
        out.push(body[..end].to_string());
        rest = &body[end + 2..];
    }
    out
}

/// A step's script as the step runner is handed it: each expression of it
/// that is not [`Expr::pasted`] replaced by the shell variable
/// `${__CI_E<k>}`, and the text of those expressions in order, from 1. The
/// step runner sets each variable to its expression's value, so that no
/// value an outsider chose is read as the script's own text. An expression
/// that is pasted stays as written, for the step runner to paste.
///
/// `sh` and `bash` read `${NAME}` as a variable, and another shell does
/// not: where `reads_variables` is false, a script may hold only
/// expressions that are pasted, and one that holds another is `None`.
pub fn script(run: &str, reads_variables: bool) -> Result<Option<(String, Vec<String>)>, Error> {
    let read = template(run)?;
    let mut exprs = Vec::new();
    let mut refused = false;
    // The expressions' own text, in order, cut from the script.
    let mut sources = sources(run).into_iter();
    let rewritten = read.replace(|_, e| {
        let source = sources.next().unwrap_or_default();
        if e.pasted() {
            return format!("${{{{{source}}}}}");
        }
        refused |= !reads_variables;
        exprs.push(source.trim().to_string());
        format!("${{__CI_E{}}}", exprs.len())
    });
    Ok((!refused).then_some((rewritten, exprs)))
}

/// Read an `if:`. It is an expression whether or not it is written inside
/// `${{ }}`, and one that is partly inside is a text, which is then true
/// unless it is empty.
pub fn condition(text: &str) -> Result<Template, Error> {
    let trimmed = text.trim();
    if trimmed.contains("${{") {
        return template(trimmed);
    }
    Ok(Template {
        parts: vec![Part::Expr(parse(trimmed)?)],
    })
}

#[cfg(test)]
mod tests;
