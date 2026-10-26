//! What decides whether a GitLab job is in a pipeline, read for each event
//! a pipeline here runs for: `rules:if`, `only` and `except`, and
//! `workflow:rules`.
//!
//! GitLab decides as it makes a pipeline, with the event in hand. The
//! importer decides once, for every event at once, so what a condition
//! comes to for an event is not yes or no but a set of names: the branches
//! a push may be to, the tags, the branches a merge request may merge
//! into. A condition that names a branch holds for that branch's name and
//! no other; one that names none holds for all of them or for none. Such a
//! set is every name but a few, or a few names ([`Names`]), and `&&`, `||`
//! and a rule's `when: never` keep it one.
//!
//! What cannot be read so (a variable the file does not set, a pattern
//! that is not a list of names, `changes:`) is not guessed at. The
//! condition is held between what it holds for at least and at most
//! ([`Bounds`]), and the pipeline is written for the most: a job that runs
//! for more than it did, and says so, rather than one that never runs.
//! A variable the file sets is read as the file sets it, but in a run
//! asked for, which may give it another value, as GitLab's web, API and
//! trigger pipelines may: there it is not known.

use std::collections::BTreeSet;

/// The events a pipeline here runs for, as GitLab tells its pipelines apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Event {
    /// A branch's pipeline.
    Push,
    /// A tag's pipeline.
    Tag,
    /// A merge request's pipeline.
    PullRequest,
    /// A run asked for: GitLab's `web`, `api` and `trigger` pipelines.
    Dispatch,
}

pub(crate) const EVENTS: [Event; 4] =
    [Event::Push, Event::Tag, Event::PullRequest, Event::Dispatch];

impl Event {
    /// The event's kind, as `{{ ci_event }}` says it.
    pub(crate) fn word(self) -> &'static str {
        match self {
            Event::Push => "push",
            Event::Tag => "tag",
            Event::PullRequest => "pull_request",
            Event::Dispatch => "dispatch",
        }
    }

    /// What `{{ ci_group }}` holds before the name a condition reads of the
    /// event, where it holds that name. A merge request's group is its
    /// number, and not the branch it merges into.
    pub(crate) fn ref_prefix(self) -> Option<&'static str> {
        match self {
            Event::Push | Event::Dispatch => Some("refs/heads/"),
            Event::Tag => Some("refs/tags/"),
            Event::PullRequest => None,
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// A set of names: a few, or every name but a few.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Names {
    Only(BTreeSet<String>),
    AllBut(BTreeSet<String>),
}

impl Names {
    pub(crate) fn all() -> Self {
        Names::AllBut(BTreeSet::new())
    }

    pub(crate) fn none() -> Self {
        Names::Only(BTreeSet::new())
    }

    fn of(holds: bool) -> Self {
        if holds {
            Names::all()
        } else {
            Names::none()
        }
    }

    pub(crate) fn one(name: &str) -> Self {
        Names::Only(BTreeSet::from([name.to_string()]))
    }

    pub(crate) fn is_all(&self) -> bool {
        matches!(self, Names::AllBut(but) if but.is_empty())
    }

    pub(crate) fn is_none(&self) -> bool {
        matches!(self, Names::Only(only) if only.is_empty())
    }

    pub(crate) fn not(&self) -> Self {
        match self {
            Names::Only(names) => Names::AllBut(names.clone()),
            Names::AllBut(names) => Names::Only(names.clone()),
        }
    }

    pub(crate) fn and(&self, other: &Self) -> Self {
        match (self, other) {
            (Names::Only(a), Names::Only(b)) => Names::Only(a.intersection(b).cloned().collect()),
            (Names::Only(a), Names::AllBut(b)) | (Names::AllBut(b), Names::Only(a)) => {
                Names::Only(a.difference(b).cloned().collect())
            }
            (Names::AllBut(a), Names::AllBut(b)) => Names::AllBut(a.union(b).cloned().collect()),
        }
    }

    pub(crate) fn or(&self, other: &Self) -> Self {
        self.not().and(&other.not()).not()
    }
}

/// What a condition holds for, for one event: at least `low`, at most
/// `high`. The two are one where it was read through.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Bounds {
    pub(crate) low: Names,
    pub(crate) high: Names,
}

impl Bounds {
    pub(crate) fn exact(names: Names) -> Self {
        Bounds {
            low: names.clone(),
            high: names,
        }
    }

    fn of(holds: bool) -> Self {
        Bounds::exact(Names::of(holds))
    }

    /// Not read: it may hold for none, or for all.
    fn unknown() -> Self {
        Bounds {
            low: Names::none(),
            high: Names::all(),
        }
    }

    pub(crate) fn is_exact(&self) -> bool {
        self.low == self.high
    }

    fn not(&self) -> Self {
        Bounds {
            low: self.high.not(),
            high: self.low.not(),
        }
    }

    fn and(&self, other: &Self) -> Self {
        Bounds {
            low: self.low.and(&other.low),
            high: self.high.and(&other.high),
        }
    }

    fn or(&self, other: &Self) -> Self {
        Bounds {
            low: self.low.or(&other.low),
            high: self.high.or(&other.high),
        }
    }
}

/// A condition, for every event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Holds([Bounds; 4]);

impl Holds {
    pub(crate) fn all() -> Self {
        Holds::from_fn(|_| Bounds::of(true))
    }

    pub(crate) fn none() -> Self {
        Holds::from_fn(|_| Bounds::of(false))
    }

    pub(crate) fn from_fn(f: impl FnMut(Event) -> Bounds) -> Self {
        Holds(EVENTS.map(f))
    }

    pub(crate) fn of(&self, event: Event) -> &Bounds {
        &self.0[event.index()]
    }

    /// For these events and no other, whatever the name.
    pub(crate) fn events(events: &[Event]) -> Self {
        Holds::from_fn(|event| Bounds::of(events.contains(&event)))
    }

    pub(crate) fn not(&self) -> Self {
        Holds::from_fn(|event| self.of(event).not())
    }

    pub(crate) fn and(&self, other: &Self) -> Self {
        Holds::from_fn(|event| self.of(event).and(other.of(event)))
    }

    pub(crate) fn or(&self, other: &Self) -> Self {
        Holds::from_fn(|event| self.of(event).or(other.of(event)))
    }

    /// Whether it holds for some name of some event, at most.
    pub(crate) fn may_hold(&self) -> bool {
        EVENTS.iter().any(|event| !self.of(*event).high.is_none())
    }
}

/// An expression of `rules:if`, `only:variables` and `except:variables`,
/// as GitLab documents its language: variables, strings, `null`, `==`,
/// `!=`, `=~` and `!~` against a pattern, `&&` binding tighter than `||`,
/// and parentheses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Expr {
    Or(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    /// An operand alone: a variable holds where it is set and not empty.
    Alone(Operand),
    Compare(Operand, Op, Operand),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Operand {
    Var(String),
    Str(String),
    Null,
    /// `/pattern/flags`.
    Pattern(String, String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    Eq,
    Ne,
    Match,
    NoMatch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
    Operand(Operand),
    Op(Op),
    And,
    Or,
    Open,
    Close,
}

fn tokens(text: &str) -> Result<Vec<Token>, String> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    let word = |chars: &[char], from: usize| -> usize {
        let mut end = from;
        while end < chars.len() && (chars[end].is_ascii_alphanumeric() || chars[end] == '_') {
            end += 1;
        }
        end
    };
    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        match c {
            c if c.is_whitespace() => i += 1,
            '$' if next == Some('{') => {
                let end = word(&chars, i + 2);
                if end == i + 2 || chars.get(end) != Some(&'}') {
                    return Err("a `${` that names no variable".into());
                }
                out.push(Token::Operand(Operand::Var(
                    chars[i + 2..end].iter().collect(),
                )));
                i = end + 1;
            }
            '$' => {
                let end = word(&chars, i + 1);
                if end == i + 1 {
                    return Err("a `$` that names no variable".into());
                }
                out.push(Token::Operand(Operand::Var(
                    chars[i + 1..end].iter().collect(),
                )));
                i = end;
            }
            '"' | '\'' => {
                let Some(len) = chars[i + 1..].iter().position(|q| *q == c) else {
                    return Err("a string that does not end".into());
                };
                out.push(Token::Operand(Operand::Str(
                    chars[i + 1..i + 1 + len].iter().collect(),
                )));
                i += len + 2;
            }
            '/' => {
                let mut end = i + 1;
                let mut pattern = String::new();
                loop {
                    match chars.get(end) {
                        None => return Err("a pattern that does not end".into()),
                        Some('\\') if chars.get(end + 1) == Some(&'/') => {
                            pattern.push('/');
                            end += 2;
                        }
                        Some('\\') => {
                            pattern.push('\\');
                            pattern.extend(chars.get(end + 1));
                            end += 2;
                        }
                        Some('/') => break,
                        Some(other) => {
                            pattern.push(*other);
                            end += 1;
                        }
                    }
                }
                let flags_end = word(&chars, end + 1);
                let flags: String = chars[end + 1..flags_end].iter().collect();
                out.push(Token::Operand(Operand::Pattern(pattern, flags)));
                i = flags_end;
            }
            '=' if next == Some('=') => {
                out.push(Token::Op(Op::Eq));
                i += 2;
            }
            '!' if next == Some('=') => {
                out.push(Token::Op(Op::Ne));
                i += 2;
            }
            '=' if next == Some('~') => {
                out.push(Token::Op(Op::Match));
                i += 2;
            }
            '!' if next == Some('~') => {
                out.push(Token::Op(Op::NoMatch));
                i += 2;
            }
            '&' if next == Some('&') => {
                out.push(Token::And);
                i += 2;
            }
            '|' if next == Some('|') => {
                out.push(Token::Or);
                i += 2;
            }
            '(' => {
                out.push(Token::Open);
                i += 1;
            }
            ')' => {
                out.push(Token::Close);
                i += 1;
            }
            'n' if chars[i..].starts_with(&['n', 'u', 'l', 'l']) && word(&chars, i) == i + 4 => {
                out.push(Token::Operand(Operand::Null));
                i += 4;
            }
            other => return Err(format!("`{other}` is not of the language")),
        }
    }
    Ok(out)
}

/// Read an expression, or say why it cannot be read.
pub(crate) fn parse(text: &str) -> Result<Expr, String> {
    let tokens = tokens(text)?;
    let mut at = 0;
    let expr = or(&tokens, &mut at, 0)?;
    if at != tokens.len() {
        return Err("it goes on past where an expression ends".into());
    }
    Ok(expr)
}

const MAX_DEPTH: usize = 32;

fn or(tokens: &[Token], at: &mut usize, depth: usize) -> Result<Expr, String> {
    let mut left = and(tokens, at, depth)?;
    while tokens.get(*at) == Some(&Token::Or) {
        *at += 1;
        left = Expr::Or(Box::new(left), Box::new(and(tokens, at, depth)?));
    }
    Ok(left)
}

fn and(tokens: &[Token], at: &mut usize, depth: usize) -> Result<Expr, String> {
    let mut left = term(tokens, at, depth)?;
    while tokens.get(*at) == Some(&Token::And) {
        *at += 1;
        left = Expr::And(Box::new(left), Box::new(term(tokens, at, depth)?));
    }
    Ok(left)
}

fn term(tokens: &[Token], at: &mut usize, depth: usize) -> Result<Expr, String> {
    match tokens.get(*at) {
        Some(Token::Open) => {
            if depth >= MAX_DEPTH {
                return Err("its parentheses nest too deep".into());
            }
            *at += 1;
            let inner = or(tokens, at, depth + 1)?;
            if tokens.get(*at) != Some(&Token::Close) {
                return Err("a parenthesis that is not closed".into());
            }
            *at += 1;
            Ok(inner)
        }
        Some(Token::Operand(left)) => {
            *at += 1;
            match tokens.get(*at) {
                Some(Token::Op(op)) => {
                    *at += 1;
                    match tokens.get(*at) {
                        Some(Token::Operand(right)) => {
                            *at += 1;
                            Ok(Expr::Compare(left.clone(), *op, right.clone()))
                        }
                        _ => Err("a comparison with nothing on its right".into()),
                    }
                }
                _ => Ok(Expr::Alone(left.clone())),
            }
        }
        _ => Err("an expression is missing".into()),
    }
}

/// What a variable is, for one event, as far as the importer can say.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Val {
    /// A value of its own, or `None` where it is not set.
    Text(Option<String>),
    /// The name a condition reads of the event: the branch pushed to, the
    /// tag, the branch a merge request merges into.
    Name,
    /// Set, and not empty, to a value not known here.
    Set,
    /// One of these, as the caller chose. A run asked for here stands for
    /// GitLab's `web`, `api` and `trigger` pipelines alike.
    AnyOf(&'static [&'static str]),
    /// Not known.
    Unknown,
}

/// What a condition is read with: the file's own variables, and what the
/// project's default branch is called.
pub(crate) struct Context<'a> {
    /// The variables the file sets to a plain value, a job's over the file's.
    pub(crate) vars: &'a dyn Fn(&str) -> Option<String>,
    pub(crate) default_branch: &'a str,
    /// The job whose condition it is, for `$CI_JOB_NAME`.
    pub(crate) job: Option<&'a str>,
    /// Whether a schedule's pipeline, and another pipeline's, are read as a
    /// run asked for: for a pipeline that runs for nothing else.
    pub(crate) sources_as_dispatch: bool,
}

/// What was met in reading a condition that the report has a word for.
#[derive(Default, Debug)]
pub(crate) struct Read {
    /// What could not be read, by what it is (`$CI_COMMIT_MESSAGE`,
    /// `changes:`, a pattern).
    pub(crate) unread: BTreeSet<String>,
    /// `$CI_DEFAULT_BRANCH` was read.
    pub(crate) default_branch: bool,
    /// `$CI_PIPELINE_SOURCE` was compared with `web`, `api` or `trigger`.
    pub(crate) dispatch: bool,
    /// `$CI_PIPELINE_SOURCE` was compared with a source no pipeline here
    /// has (`schedule`, `pipeline`, …).
    pub(crate) sources: BTreeSet<String>,
    /// Sources read as a run asked for ([`Context::sources_as_dispatch`]).
    pub(crate) as_dispatch: BTreeSet<String>,
    /// `$CI_OPEN_MERGE_REQUESTS` was read: taken as unset.
    pub(crate) open_merge_requests: bool,
    /// The file's own variables that were read as the file sets them: a
    /// pipeline's variables, or the project's, may set them otherwise.
    pub(crate) file_vars: BTreeSet<String>,
}

/// The sources of a pipeline no event here is: a condition that names one
/// holds for none.
const OTHER_SOURCES: &[&str] = &[
    "chat",
    "external",
    "external_pull_request_event",
    "ondemand_dast_scan",
    "ondemand_dast_validation",
    "parent_pipeline",
    "pipeline",
    "schedule",
    "security_orchestration_policy",
    "webide",
];

const DISPATCH_SOURCES: &[&str] = &["web", "api", "trigger"];

/// The sources a run asked for stands for in a pipeline that runs for
/// nothing else ([`Context::sources_as_dispatch`]): a schedule here asks for
/// a run through the run API, and so may another pipeline.
const SCHEDULED_SOURCES: &[&str] = &["schedule", "pipeline", "parent_pipeline"];
const DISPATCH_AND_SCHEDULED: &[&str] = &[
    "web",
    "api",
    "trigger",
    "schedule",
    "pipeline",
    "parent_pipeline",
];

fn value(name: &str, event: Event, cx: &Context<'_>, read: &mut Read) -> Val {
    use Event::*;
    let unset = Val::Text(None);
    match name {
        "CI" | "GITLAB_CI" => Val::Text(Some("true".into())),
        "CI_PIPELINE_SOURCE" => match event {
            Push | Tag => Val::Text(Some("push".into())),
            PullRequest => Val::Text(Some("merge_request_event".into())),
            Dispatch if cx.sources_as_dispatch => Val::AnyOf(DISPATCH_AND_SCHEDULED),
            Dispatch => Val::AnyOf(DISPATCH_SOURCES),
        },
        "CI_COMMIT_BRANCH" => match event {
            Push | Dispatch => Val::Name,
            _ => unset,
        },
        "CI_COMMIT_TAG" => match event {
            Tag => Val::Name,
            _ => unset,
        },
        "CI_COMMIT_REF_NAME" => match event {
            PullRequest => {
                read.unread
                    .insert(format!("${name} in a merge request's pipeline"));
                Val::Unknown
            }
            _ => Val::Name,
        },
        "CI_DEFAULT_BRANCH" => {
            read.default_branch = true;
            Val::Text(Some(cx.default_branch.to_string()))
        }
        "CI_JOB_NAME" if cx.job.is_some() => Val::Text(cx.job.map(str::to_string)),
        "CI_MERGE_REQUEST_TARGET_BRANCH_NAME" => match event {
            PullRequest => Val::Name,
            _ => unset,
        },
        "CI_MERGE_REQUEST_IID"
        | "CI_MERGE_REQUEST_ID"
        | "CI_MERGE_REQUEST_SOURCE_BRANCH_NAME"
        | "CI_MERGE_REQUEST_EVENT_TYPE" => match event {
            PullRequest => Val::Set,
            _ => unset,
        },
        "CI_OPEN_MERGE_REQUESTS" => {
            read.open_merge_requests = true;
            unset
        }
        "CI_PIPELINE_TRIGGERED" => match event {
            Dispatch => {
                read.unread.insert(format!("${name}"));
                Val::Unknown
            }
            _ => unset,
        },
        _ if name.starts_with("CI_MERGE_REQUEST_") => match event {
            PullRequest => {
                read.unread.insert(format!("${name}"));
                Val::Unknown
            }
            _ => unset,
        },
        // A run asked for may be given the variables of the file other
        // values, as GitLab's web, API and trigger pipelines may.
        _ => match (cx.vars)(name) {
            Some(_) if event == Dispatch => {
                read.unread
                    .insert(format!("${name} (which a run asked for may set)"));
                Val::Unknown
            }
            Some(value) => {
                read.file_vars.insert(name.to_string());
                Val::Text(Some(value))
            }
            None => {
                read.unread.insert(format!("${name}"));
                Val::Unknown
            }
        },
    }
}

fn operand(op: &Operand, event: Event, cx: &Context<'_>, read: &mut Read) -> Val {
    match op {
        Operand::Var(name) => value(name, event, cx, read),
        Operand::Str(text) => Val::Text(Some(text.clone())),
        Operand::Null => Val::Text(None),
        Operand::Pattern(..) => Val::Unknown,
    }
}

/// The names a pattern matches, where it is a list of them and nothing
/// else: `/^main$/`, `/^(main|develop)$/`, `/^release-(1|2)$/`. Anchored at
/// both ends, of plain characters and groups of plain alternatives.
fn listed(pattern: &str) -> Option<BTreeSet<String>> {
    let inner = pattern.strip_prefix('^')?.strip_suffix('$')?;
    let mut names: Vec<String> = vec![String::new()];
    let mut chars = inner.chars().peekable();
    let plain = |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '/' | '_');
    // One plain character, or an escaped one, onto every name so far.
    let one = |c: char, chars: &mut std::iter::Peekable<std::str::Chars<'_>>| -> Option<char> {
        match c {
            '\\' => match chars.next() {
                Some(escaped @ ('.' | '-' | '/' | '_')) => Some(escaped),
                _ => None,
            },
            c if plain(c) => Some(c),
            _ => None,
        }
    };
    while let Some(c) = chars.next() {
        if c != '(' {
            let c = one(c, &mut chars)?;
            names.iter_mut().for_each(|n| n.push(c));
            continue;
        }
        if chars.peek() == Some(&'?') {
            chars.next();
            if chars.next() != Some(':') {
                return None;
            }
        }
        let mut alternatives = vec![String::new()];
        loop {
            match chars.next()? {
                ')' => break,
                '|' => alternatives.push(String::new()),
                c => {
                    let c = one(c, &mut chars)?;
                    alternatives.last_mut()?.push(c);
                }
            }
        }
        if alternatives.iter().any(String::is_empty) {
            return None;
        }
        names = names
            .iter()
            .flat_map(|n| alternatives.iter().map(move |a| format!("{n}{a}")))
            .collect();
        if names.len() > 64 {
            return None;
        }
    }
    let names: BTreeSet<String> = names.into_iter().filter(|n| !n.is_empty()).collect();
    (!names.is_empty()).then_some(names)
}

fn matches(pattern: &str, flags: &str, text: &str) -> Option<bool> {
    regex::RegexBuilder::new(pattern)
        .case_insensitive(flags.contains('i'))
        .multi_line(flags.contains('m'))
        .build()
        .ok()
        .map(|re| re.is_match(text))
}

fn compare(
    left: &Operand,
    op: Op,
    right: &Operand,
    event: Event,
    cx: &Context<'_>,
    read: &mut Read,
) -> Bounds {
    match op {
        Op::Ne => return compare(left, Op::Eq, right, event, cx, read).not(),
        Op::NoMatch => return compare(left, Op::Match, right, event, cx, read).not(),
        _ => {}
    }
    let a = operand(left, event, cx, read);
    if op == Op::Match {
        let (pattern, flags) = match right {
            Operand::Pattern(pattern, flags) => (pattern.clone(), flags.clone()),
            other => match operand(other, event, cx, read) {
                Val::Text(Some(text)) => {
                    match text.strip_prefix('/').and_then(|t| t.rsplit_once('/')) {
                        Some((pattern, flags)) => (pattern.to_string(), flags.to_string()),
                        None => return Bounds::of(false),
                    }
                }
                Val::Text(None) => return Bounds::of(false),
                _ => return Bounds::unknown(),
            },
        };
        let unread = |read: &mut Read| {
            read.unread
                .insert(format!("the pattern /{pattern}/{flags}"));
            Bounds::unknown()
        };
        return match a {
            Val::Text(None) => Bounds::of(false),
            Val::Text(Some(text)) => match matches(&pattern, &flags, &text) {
                Some(holds) => Bounds::of(holds),
                None => unread(read),
            },
            Val::Name if flags.is_empty() => match listed(&pattern) {
                Some(names) => Bounds::exact(Names::Only(names)),
                None => unread(read),
            },
            Val::AnyOf(sources) => {
                let found: Option<Vec<bool>> = sources
                    .iter()
                    .map(|s| matches(&pattern, &flags, s))
                    .collect();
                match found {
                    Some(found) if found.iter().all(|f| *f) => Bounds::of(true),
                    Some(found) if found.iter().all(|f| !*f) => Bounds::of(false),
                    _ => unread(read),
                }
            }
            Val::Name | Val::Set => unread(read),
            Val::Unknown => Bounds::unknown(),
        };
    }
    let b = operand(right, event, cx, read);
    source_named(left, &b, cx, read);
    source_named(right, &a, cx, read);
    match (a, b) {
        (Val::Unknown, _) | (_, Val::Unknown) => Bounds::unknown(),
        (Val::Text(x), Val::Text(y)) => Bounds::of(x == y),
        (Val::Name, Val::Text(Some(name))) | (Val::Text(Some(name)), Val::Name) => {
            Bounds::exact(Names::one(&name))
        }
        (Val::Name, Val::Name) => Bounds::of(true),
        (Val::Name | Val::Set | Val::AnyOf(_), Val::Text(None))
        | (Val::Text(None), Val::Name | Val::Set | Val::AnyOf(_)) => Bounds::of(false),
        (Val::AnyOf(set), Val::Text(Some(text))) | (Val::Text(Some(text)), Val::AnyOf(set)) => {
            Bounds::of(set.contains(&text.as_str()))
        }
        (Val::AnyOf(_), Val::AnyOf(_)) => Bounds::of(true),
        _ => {
            read.unread.insert(format!(
                "a comparison of {} with {}",
                said_operand(left),
                said_operand(right)
            ));
            Bounds::unknown()
        }
    }
}

/// Where `$CI_PIPELINE_SOURCE` is compared with a source, whether the
/// source is a run asked for, or one no pipeline here is.
fn source_named(var: &Operand, other: &Val, cx: &Context<'_>, read: &mut Read) {
    let (Operand::Var(name), Val::Text(Some(source))) = (var, other) else {
        return;
    };
    if name != "CI_PIPELINE_SOURCE" {
        return;
    }
    if DISPATCH_SOURCES.contains(&source.as_str()) {
        read.dispatch = true;
    }
    if cx.sources_as_dispatch && SCHEDULED_SOURCES.contains(&source.as_str()) {
        read.as_dispatch.insert(source.clone());
    } else if OTHER_SOURCES.contains(&source.as_str()) {
        read.sources.insert(source.clone());
    }
}

fn said_operand(op: &Operand) -> String {
    match op {
        Operand::Var(name) => format!("${name}"),
        Operand::Str(text) => format!("{text:?}"),
        Operand::Null => "null".into(),
        Operand::Pattern(pattern, flags) => format!("/{pattern}/{flags}"),
    }
}

fn bounds(expr: &Expr, event: Event, cx: &Context<'_>, read: &mut Read) -> Bounds {
    match expr {
        Expr::Or(a, b) => bounds(a, event, cx, read).or(&bounds(b, event, cx, read)),
        Expr::And(a, b) => bounds(a, event, cx, read).and(&bounds(b, event, cx, read)),
        Expr::Alone(op) => match operand(op, event, cx, read) {
            Val::Text(Some(text)) => Bounds::of(!text.is_empty()),
            Val::Text(None) => Bounds::of(false),
            Val::Name | Val::Set | Val::AnyOf(_) => Bounds::of(true),
            Val::Unknown => Bounds::unknown(),
        },
        Expr::Compare(left, op, right) => compare(left, *op, right, event, cx, read),
    }
}

/// What an expression holds for, for each event.
pub(crate) fn holds(expr: &Expr, cx: &Context<'_>, read: &mut Read) -> Holds {
    Holds::from_fn(|event| bounds(expr, event, cx, read))
}

/// What is unknown for every event: `changes:`, `exists:`, a pipeline's
/// own kind of condition, which the importer has no files to read.
pub(crate) fn unread(what: &str, read: &mut Read) -> Holds {
    read.unread.insert(what.to_string());
    Holds::from_fn(|_| Bounds::unknown())
}

/// What `changes:` holds for: for a branch's and a merge request's
/// pipelines, what the files that changed say, which is not known here;
/// for every other, GitLab holds it true.
pub(crate) fn changes(read: &mut Read) -> Holds {
    read.unread.insert("rules:changes".to_string());
    Holds::from_fn(|event| match event {
        Event::Push | Event::PullRequest => Bounds::unknown(),
        Event::Tag | Event::Dispatch => Bounds::of(true),
    })
}

/// What one entry of `only:` or `except:` (its `refs`) holds for.
/// `sources_as_dispatch` is [`Context::sources_as_dispatch`].
pub(crate) fn reference(entry: &str, sources_as_dispatch: bool, read: &mut Read) -> Holds {
    use Event::*;
    match entry {
        "branches" => Holds::events(&[Push, Dispatch]),
        "tags" => Holds::events(&[Tag]),
        "merge_requests" => Holds::events(&[PullRequest]),
        "pushes" => Holds::events(&[Push, Tag]),
        "web" | "api" | "triggers" => {
            read.dispatch = true;
            Holds::events(&[Dispatch])
        }
        "schedules" | "pipelines" if sources_as_dispatch => {
            read.as_dispatch.insert(entry.to_string());
            Holds::events(&[Dispatch])
        }
        "schedules" | "pipelines" | "external" | "external_pull_request_events" | "chat" => {
            read.sources.insert(entry.to_string());
            Holds::none()
        }
        _ if entry.starts_with('/') => {
            let Some((pattern, flags)) = entry[1..].rsplit_once('/') else {
                return unread(&format!("the ref {entry}"), read);
            };
            match (listed(pattern), flags.is_empty()) {
                (Some(names), true) => Holds::from_fn(|event| match event {
                    PullRequest => Bounds::of(false),
                    _ => Bounds::exact(Names::Only(names.clone())),
                }),
                _ => {
                    read.unread.insert(format!("the pattern {entry}"));
                    Holds::from_fn(|event| match event {
                        PullRequest => Bounds::of(false),
                        _ => Bounds::unknown(),
                    })
                }
            }
        }
        // `name@group/project`: a ref of that project's alone.
        _ if entry.contains('@') => unread(&format!("the ref {entry}"), read),
        name => Holds::from_fn(|event| match event {
            PullRequest => Bounds::of(false),
            _ => Bounds::exact(Names::one(name)),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(text: &str) -> Holds {
        let vars = |name: &str| (name == "DEPLOY").then(|| "yes".to_string());
        let cx = Context {
            vars: &vars,
            default_branch: "main",
            job: Some("build"),
            sources_as_dispatch: false,
        };
        holds(&parse(text).unwrap(), &cx, &mut Read::default())
    }

    #[test]
    fn the_language_is_read_as_gitlab_documents_it() {
        assert_eq!(
            parse("$A == \"x\" || $B && ($C != null)").unwrap(),
            Expr::Or(
                Box::new(Expr::Compare(
                    Operand::Var("A".into()),
                    Op::Eq,
                    Operand::Str("x".into())
                )),
                Box::new(Expr::And(
                    Box::new(Expr::Alone(Operand::Var("B".into()))),
                    Box::new(Expr::Compare(
                        Operand::Var("C".into()),
                        Op::Ne,
                        Operand::Null
                    )),
                )),
            )
        );
        assert_eq!(
            parse("${CI_COMMIT_TAG} =~ /^v\\d+\\/x$/i").unwrap(),
            Expr::Compare(
                Operand::Var("CI_COMMIT_TAG".into()),
                Op::Match,
                Operand::Pattern("^v\\d+/x$".into(), "i".into())
            )
        );
        for wrong in ["$A ==", "($A", "$A $B", "", "$", "\"x", "/x", "$A = 'x'"] {
            assert!(parse(wrong).is_err(), "{wrong}");
        }
    }

    #[test]
    fn a_condition_comes_to_the_names_it_holds_for() {
        let push = |text: &str| read(text).of(Event::Push).clone();
        let only = |names: &[&str]| {
            Bounds::exact(Names::Only(names.iter().map(|n| n.to_string()).collect()))
        };
        assert_eq!(
            push("$CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH"),
            only(&["main"])
        );
        assert_eq!(
            push("$CI_COMMIT_BRANCH == \"main\" || $CI_COMMIT_BRANCH =~ /^(dev|next)$/"),
            only(&["dev", "main", "next"])
        );
        assert_eq!(
            push("$CI_COMMIT_BRANCH != \"main\""),
            Bounds::exact(Names::AllBut(BTreeSet::from(["main".to_string()])))
        );
        assert_eq!(push("$CI_COMMIT_TAG"), Bounds::of(false));
        assert_eq!(read("$CI_COMMIT_TAG").of(Event::Tag), &Bounds::of(true));
        assert_eq!(
            push("$CI_PIPELINE_SOURCE == \"merge_request_event\""),
            Bounds::of(false)
        );
        assert_eq!(
            read("$CI_PIPELINE_SOURCE == \"merge_request_event\"").of(Event::PullRequest),
            &Bounds::of(true)
        );
        assert_eq!(
            read("$CI_PIPELINE_SOURCE == \"web\"").of(Event::Dispatch),
            &Bounds::of(true)
        );
        assert_eq!(push("$DEPLOY == \"yes\""), Bounds::of(true));
        // A run asked for may give the file's variable another value.
        assert_eq!(
            read("$DEPLOY == \"yes\"").of(Event::Dispatch),
            &Bounds::unknown()
        );
        // What is not known is between none and all.
        assert_eq!(push("$CI_COMMIT_MESSAGE =~ /skip/"), Bounds::unknown());
        assert_eq!(
            push("$CI_COMMIT_BRANCH =~ /^release\\//"),
            Bounds::unknown()
        );
        // And what is known around it still counts.
        assert_eq!(push("$CI_COMMIT_TAG && $SOMETHING"), Bounds::of(false));
        assert_eq!(push("$CI_COMMIT_BRANCH || $SOMETHING"), Bounds::of(true));
    }

    #[test]
    fn sets_of_names_are_closed_under_and_or_and_not() {
        let set = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<BTreeSet<_>>();
        let a = Names::Only(set(&["x", "y"]));
        let b = Names::AllBut(set(&["y"]));
        assert_eq!(a.and(&b), Names::Only(set(&["x"])));
        assert_eq!(a.or(&b), Names::all());
        assert_eq!(b.not(), Names::Only(set(&["y"])));
        assert!(Names::all().not().is_none());
    }

    #[test]
    fn only_and_except_entries_are_events_and_names() {
        let mut said = Read::default();
        let reference = |entry: &str, said: &mut Read| reference(entry, false, said);
        assert_eq!(
            reference("branches", &mut said),
            Holds::events(&[Event::Push, Event::Dispatch])
        );
        assert_eq!(
            reference("main", &mut said).of(Event::Tag),
            &Bounds::exact(Names::one("main"))
        );
        assert_eq!(
            reference("/^main$/", &mut said).of(Event::Push),
            &Bounds::exact(Names::one("main"))
        );
        assert_eq!(
            reference("/^release-(1|2)$/", &mut said).of(Event::Push),
            &Bounds::exact(Names::Only(BTreeSet::from([
                "release-1".to_string(),
                "release-2".to_string()
            ])))
        );
        assert_eq!(listed("^v\\d+$"), None);
        assert_eq!(
            listed("^(?:main|dev)$"),
            Some(BTreeSet::from(["dev".to_string(), "main".to_string()]))
        );
        assert_eq!(reference("schedules", &mut said), Holds::none());
        assert!(said.sources.contains("schedules"));
    }

    #[test]
    fn a_schedule_is_a_run_asked_for_where_nothing_else_runs_the_pipeline() {
        let vars = |_: &str| None;
        let cx = Context {
            vars: &vars,
            default_branch: "main",
            job: None,
            sources_as_dispatch: true,
        };
        let mut said = Read::default();
        let expr = parse("$CI_PIPELINE_SOURCE == \"schedule\"").unwrap();
        let read = holds(&expr, &cx, &mut said);
        assert_eq!(read.of(Event::Dispatch), &Bounds::of(true));
        assert_eq!(read.of(Event::Push), &Bounds::of(false));
        assert!(said.as_dispatch.contains("schedule") && said.sources.is_empty());
        assert_eq!(
            reference("schedules", true, &mut said),
            Holds::events(&[Event::Dispatch])
        );
    }
}
