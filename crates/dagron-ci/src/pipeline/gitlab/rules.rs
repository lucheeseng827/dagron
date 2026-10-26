//! GitLab's `rules:if` expressions: what decides, as the pipeline is made,
//! whether a job is in it.
//!
//! The language, as GitLab documents it:
//!
//! - a variable, `$NAME` or `${NAME}`: alone, it holds where it is defined
//!   and not empty;
//! - a string, in double or single quotes, and `null`;
//! - `==` and `!=`, which compare two of those (an undefined variable is
//!   `null`);
//! - `=~` and `!~`, which match the left against a regular expression,
//!   `/…/` with an `i` after it for no case, written there or held by a
//!   variable on the right (RE2's syntax, which Rust's `regex` shares);
//! - `&&`, which binds tighter than `||`, and parentheses.
//!
//! An expression is read once ([`parse`]) and then evaluated for each job
//! against the variables it has ([`Expr::holds`]).

use std::collections::BTreeMap;

/// The longest expression read.
const MAX_LEN: usize = 4096;

/// How deep parentheses may nest.
const MAX_DEPTH: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expr {
    Or(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    /// A variable alone: defined and not empty.
    Defined(String),
    Compare {
        left: Operand,
        op: Op,
        right: Operand,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Operand {
    Var(String),
    Str(String),
    Null,
    /// `/pattern/flags`.
    Regex(String, bool),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Eq,
    Ne,
    Match,
    NoMatch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
    Var(String),
    Str(String),
    Regex(String, bool),
    Null,
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
    let name_char = |c: char| c.is_ascii_alphanumeric() || c == '_';
    while i < chars.len() {
        let c = chars[i];
        match c {
            ' ' | '\t' | '\n' | '\r' => i += 1,
            '(' => {
                out.push(Token::Open);
                i += 1;
            }
            ')' => {
                out.push(Token::Close);
                i += 1;
            }
            '$' => {
                let (name, next) = if chars.get(i + 1) == Some(&'{') {
                    let end = chars[i + 2..]
                        .iter()
                        .position(|c| *c == '}')
                        .ok_or("a ${ with no } to end it")?;
                    (
                        chars[i + 2..i + 2 + end].iter().collect::<String>(),
                        i + 3 + end,
                    )
                } else {
                    let end = chars[i + 1..]
                        .iter()
                        .position(|c| !name_char(*c))
                        .unwrap_or(chars.len() - i - 1);
                    (
                        chars[i + 1..i + 1 + end].iter().collect::<String>(),
                        i + 1 + end,
                    )
                };
                if name.is_empty() || !name.chars().all(name_char) {
                    return Err(format!("{:?} is not a variable's name", name));
                }
                out.push(Token::Var(name));
                i = next;
            }
            '"' | '\'' => {
                let end = chars[i + 1..]
                    .iter()
                    .position(|d| *d == c)
                    .ok_or("a string with no quote to end it")?;
                out.push(Token::Str(chars[i + 1..i + 1 + end].iter().collect()));
                i += end + 2;
            }
            '/' => {
                // A regular expression runs to the next `/` not escaped.
                let mut j = i + 1;
                let mut pattern = String::new();
                loop {
                    match chars.get(j) {
                        None => return Err("a regular expression with no / to end it".into()),
                        Some('\\') if chars.get(j + 1) == Some(&'/') => {
                            pattern.push('/');
                            j += 2;
                        }
                        Some('\\') => {
                            pattern.push('\\');
                            if let Some(next) = chars.get(j + 1) {
                                pattern.push(*next);
                            }
                            j += 2;
                        }
                        Some('/') => break,
                        Some(c) => {
                            pattern.push(*c);
                            j += 1;
                        }
                    }
                }
                j += 1;
                let mut flags = String::new();
                while let Some(flag) = chars.get(j).filter(|c| c.is_ascii_alphabetic()) {
                    flags.push(*flag);
                    j += 1;
                }
                let (pattern, fold) = flagged(&pattern, &flags)?;
                out.push(Token::Regex(pattern, fold));
                i = j;
            }
            '=' | '!' => {
                let op = match (c, chars.get(i + 1)) {
                    ('=', Some('=')) => Op::Eq,
                    ('!', Some('=')) => Op::Ne,
                    ('=', Some('~')) => Op::Match,
                    ('!', Some('~')) => Op::NoMatch,
                    _ => return Err(format!("{c} is not an operator: ==, !=, =~, !~")),
                };
                out.push(Token::Op(op));
                i += 2;
            }
            '&' if chars.get(i + 1) == Some(&'&') => {
                out.push(Token::And);
                i += 2;
            }
            '|' if chars.get(i + 1) == Some(&'|') => {
                out.push(Token::Or);
                i += 2;
            }
            'n' if chars[i..].iter().take(4).collect::<String>() == "null"
                && !chars.get(i + 4).is_some_and(|c| name_char(*c)) =>
            {
                out.push(Token::Null);
                i += 4;
            }
            other => return Err(format!("{other:?} is not part of an expression")),
        }
    }
    Ok(out)
}

/// Read a `rules:if` expression.
pub fn parse(text: &str) -> Result<Expr, String> {
    if text.len() > MAX_LEN {
        return Err(format!(
            "is over the {MAX_LEN} characters an expression may be"
        ));
    }
    let tokens = tokens(text)?;
    if tokens.is_empty() {
        return Err("is empty".into());
    }
    let mut at = 0;
    let expr = or(&tokens, &mut at, 0)?;
    if at != tokens.len() {
        return Err(format!(
            "{:?} is left over after the expression",
            tokens[at]
        ));
    }
    Ok(expr)
}

fn or(t: &[Token], at: &mut usize, depth: usize) -> Result<Expr, String> {
    let mut left = and(t, at, depth)?;
    while t.get(*at) == Some(&Token::Or) {
        *at += 1;
        let right = and(t, at, depth)?;
        left = Expr::Or(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn and(t: &[Token], at: &mut usize, depth: usize) -> Result<Expr, String> {
    let mut left = unit(t, at, depth)?;
    while t.get(*at) == Some(&Token::And) {
        *at += 1;
        let right = unit(t, at, depth)?;
        left = Expr::And(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn unit(t: &[Token], at: &mut usize, depth: usize) -> Result<Expr, String> {
    if t.get(*at) == Some(&Token::Open) {
        if depth >= MAX_DEPTH {
            return Err(format!("nests parentheses more than {MAX_DEPTH} deep"));
        }
        *at += 1;
        let inner = or(t, at, depth + 1)?;
        if t.get(*at) != Some(&Token::Close) {
            return Err("a ( with no ) to end it".into());
        }
        *at += 1;
        return Ok(inner);
    }
    let left = operand(t, at)?;
    match t.get(*at) {
        Some(Token::Op(op)) => {
            let op = *op;
            *at += 1;
            let right = operand(t, at)?;
            if matches!(op, Op::Match | Op::NoMatch)
                && !matches!(right, Operand::Regex(..) | Operand::Var(_))
            {
                return Err(
                    "=~ and !~ match against a /regular expression/, or a variable that holds one"
                        .into(),
                );
            }
            Ok(Expr::Compare { left, op, right })
        }
        _ => match left {
            Operand::Var(name) => Ok(Expr::Defined(name)),
            other => Err(format!(
                "{other:?} alone is no condition: compare it with == or !="
            )),
        },
    }
}

fn operand(t: &[Token], at: &mut usize) -> Result<Operand, String> {
    let o = match t.get(*at) {
        Some(Token::Var(name)) => Operand::Var(name.clone()),
        Some(Token::Str(s)) => Operand::Str(s.clone()),
        Some(Token::Regex(p, i)) => Operand::Regex(p.clone(), *i),
        Some(Token::Null) => Operand::Null,
        Some(other) => {
            return Err(format!(
                "{other:?} is where a variable, a string or null goes"
            ))
        }
        None => return Err("ends where a variable, a string or null goes".into()),
    };
    *at += 1;
    Ok(o)
}

/// A pattern and the flags after it: `i` no case, and RE2's `m` (lines),
/// `s` (`.` takes a newline) and `U` (lazy), which are written into the
/// pattern as RE2 writes them.
fn flagged(pattern: &str, flags: &str) -> Result<(String, bool), String> {
    let mut fold = false;
    let mut inline = String::new();
    for flag in flags.chars() {
        match flag {
            'i' => fold = true,
            'm' | 's' | 'U' => inline.push(flag),
            other => return Err(format!("/{pattern}/{other} has a flag GitLab has not")),
        }
    }
    if inline.is_empty() {
        Ok((pattern.to_string(), fold))
    } else {
        Ok((format!("(?{inline}){pattern}"), fold))
    }
}

/// A pattern as GitLab writes one, `/re/` and its flags, held by a variable.
fn as_regex(text: &str) -> Option<(String, bool)> {
    let rest = text.strip_prefix('/')?;
    let end = rest.rfind('/')?;
    flagged(&rest[..end], &rest[end + 1..]).ok()
}

fn regex(pattern: &str, fold: bool) -> Result<regex::Regex, String> {
    regex::RegexBuilder::new(pattern)
        .case_insensitive(fold)
        .size_limit(1 << 20)
        .build()
        .map_err(|e| format!("/{pattern}/ is not a regular expression: {e}"))
}

impl Expr {
    /// Whether the expression holds, with these variables.
    pub fn holds(&self, vars: &BTreeMap<String, String>) -> Result<bool, String> {
        Ok(match self {
            Expr::Or(a, b) => a.holds(vars)? || b.holds(vars)?,
            Expr::And(a, b) => a.holds(vars)? && b.holds(vars)?,
            Expr::Defined(name) => vars.get(name).is_some_and(|v| !v.is_empty()),
            Expr::Compare { left, op, right } => {
                let value = |o: &Operand| -> Option<String> {
                    match o {
                        Operand::Var(name) => vars.get(name).cloned(),
                        Operand::Str(s) => Some(s.clone()),
                        Operand::Null => None,
                        Operand::Regex(p, _) => Some(format!("/{p}/")),
                    }
                };
                match op {
                    Op::Eq | Op::Ne => {
                        let same = value(left) == value(right);
                        same == (*op == Op::Eq)
                    }
                    Op::Match | Op::NoMatch => {
                        let (pattern, fold) = match right {
                            Operand::Regex(p, fold) => (p.clone(), *fold),
                            Operand::Var(name) => match vars.get(name).and_then(|v| as_regex(v)) {
                                Some(re) => re,
                                // GitLab: a variable that holds no pattern
                                // matches nothing.
                                None => return Ok(*op == Op::NoMatch),
                            },
                            _ => unreachable!("refused when read"),
                        };
                        // A variable that is not set is matched as empty
                        // text, as GitLab matches it.
                        let text = value(left).unwrap_or_default();
                        let matched = regex(&pattern, fold)?.is_match(&text);
                        matched == (*op == Op::Match)
                    }
                }
            }
        })
    }

    /// Every regular expression written into it, each checked to compile:
    /// what [`parse`] cannot know until a value arrives is a variable's.
    pub fn check(&self) -> Result<(), String> {
        match self {
            Expr::Or(a, b) | Expr::And(a, b) => {
                a.check()?;
                b.check()
            }
            Expr::Compare {
                right: Operand::Regex(p, fold),
                ..
            } => regex(p, *fold).map(drop),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn holds(text: &str, with: &[(&str, &str)]) -> bool {
        parse(text).unwrap().holds(&vars(with)).unwrap()
    }

    #[test]
    fn a_variable_alone_holds_where_it_is_set_and_not_empty() {
        assert!(holds("$CI_COMMIT_TAG", &[("CI_COMMIT_TAG", "v1")]));
        assert!(!holds("$CI_COMMIT_TAG", &[("CI_COMMIT_TAG", "")]));
        assert!(!holds("$CI_COMMIT_TAG", &[]));
        assert!(holds("${CI_COMMIT_TAG}", &[("CI_COMMIT_TAG", "v1")]));
    }

    #[test]
    fn equality_compares_strings_variables_and_null() {
        let main = [("CI_COMMIT_BRANCH", "main"), ("CI_DEFAULT_BRANCH", "main")];
        assert!(holds("$CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH", &main));
        assert!(holds("$CI_COMMIT_BRANCH == \"main\"", &main));
        assert!(holds("$CI_COMMIT_BRANCH == 'main'", &main));
        assert!(holds("$CI_COMMIT_BRANCH != 'dev'", &main));
        assert!(holds("$CI_COMMIT_TAG == null", &main));
        assert!(!holds("$CI_COMMIT_BRANCH == null", &main));
        assert!(holds("$UNSET != 'x'", &main));
    }

    #[test]
    fn a_match_reads_a_pattern_written_or_held() {
        let tag = [("CI_COMMIT_TAG", "v1.2.0"), ("PATTERN", "/^V1/i")];
        assert!(holds("$CI_COMMIT_TAG =~ /^v[0-9]+\\./", &tag));
        assert!(!holds("$CI_COMMIT_TAG !~ /^v/", &tag));
        assert!(holds("$CI_COMMIT_TAG =~ /^V1/i", &tag));
        assert!(holds("$CI_COMMIT_TAG =~ $PATTERN", &tag));
        assert!(!holds("$UNSET =~ /x/", &tag));
        assert!(holds("$UNSET !~ /x/", &tag));
        assert!(holds("$CI_COMMIT_TAG =~ /^v1\\/?/", &tag));
        assert!(holds("$UNSET =~ /^$/", &tag));
        assert!(holds("$LINES =~ /^b$/m", &[("LINES", "a\nb")]));
        assert!(!holds("$LINES =~ /^b$/", &[("LINES", "a\nb")]));
        assert!(holds(
            "$CI_COMMIT_TAG =~ $FLAGGED",
            &[("CI_COMMIT_TAG", "x\ny"), ("FLAGGED", "/x.y/s")]
        ));
    }

    #[test]
    fn and_binds_tighter_than_or_and_parentheses_group() {
        let v = [("A", "1"), ("B", "")];
        assert!(holds("$A || $B && $C", &v));
        assert!(!holds("($A || $B) && $C", &v));
        assert!(holds(
            "$CI_PIPELINE_SOURCE == 'merge_request_event' || ($A && $A == '1')",
            &v
        ));
    }

    #[test]
    fn what_is_not_an_expression_is_refused_when_read() {
        for bad in [
            "",
            "$A ==",
            "'x'",
            "$A = 'x'",
            "($A",
            "$A == 'x",
            "$A =~ 'x'",
            "$A =~ /x/q",
            "$",
            "$A && && $B",
        ] {
            assert!(parse(bad).is_err(), "{bad:?}");
        }
        assert!(parse("$A =~ /(/").unwrap().check().is_err());
        assert!(parse("$A =~ /^ok$/").unwrap().check().is_ok());
    }
}
