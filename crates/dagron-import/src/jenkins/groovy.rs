//! The Groovy a Declarative Pipeline is written in, read as far as its
//! structure: sections and directives (`stage('Build') { … }`), steps
//! (`sh 'make'`, `junit testResults: '…'`), assignments (`X = 'y'`), and
//! the values they are given: strings with what they interpolate, numbers,
//! lists, maps, calls, closures.
//!
//! What is Groovy and not structure (`if`, `def`, a method chained on a
//! call) is kept as its text ([`Stmt::Other`], [`Expr::Other`]): the
//! importer says it is not imported, and shows what it was.

use std::fmt;

/// A string: its text, and what it interpolates, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GStr {
    pub(crate) parts: Vec<Part>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Part {
    Text(String),
    /// `${…}` or `$name.name`: the Groovy between the braces.
    Expr(String),
}

impl GStr {
    /// The text, where it interpolates nothing.
    pub(crate) fn plain(&self) -> Option<String> {
        let mut out = String::new();
        for part in &self.parts {
            match part {
                Part::Text(text) => out.push_str(text),
                Part::Expr(_) => return None,
            }
        }
        Some(out)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Expr {
    Str(GStr),
    Num(String),
    Bool(bool),
    Null,
    List(Vec<Expr>),
    Map(Vec<(String, Expr)>),
    /// `name(args)`, with a closure after it or not.
    Call {
        name: String,
        args: Vec<Arg>,
        block: Option<Block>,
    },
    /// `env.FOO`, `params.X`, `scm`.
    Path(String),
    Closure(Block),
    /// Groovy that is not one of the above, as written.
    Other(String),
}

impl Expr {
    /// The text of a string that interpolates nothing, a number or a
    /// boolean.
    pub(crate) fn text(&self) -> Option<String> {
        match self {
            Expr::Str(s) => s.plain(),
            Expr::Num(n) => Some(n.clone()),
            Expr::Bool(b) => Some(b.to_string()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Arg {
    Pos(Expr),
    Named(String, Expr),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Block {
    pub(crate) stmts: Vec<Stmt>,
    /// The Groovy between the braces, as written.
    pub(crate) text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Stmt {
    /// `name args`, `name(args)`, `name { … }`, `name(args) { … }`.
    Call {
        name: String,
        args: Vec<Arg>,
        block: Option<Block>,
        text: String,
    },
    /// `NAME = value`.
    Assign {
        name: String,
        value: Expr,
        text: String,
    },
    /// Groovy that is no section, directive or step: as written.
    Other(String),
}

impl Stmt {
    /// Its text, as written.
    pub(crate) fn text(&self) -> &str {
        match self {
            Stmt::Call { text, .. } | Stmt::Assign { text, .. } | Stmt::Other(text) => text,
        }
    }
}

/// Where a file could not be read, and why.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Error {
    pub(crate) line: usize,
    pub(crate) message: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}", self.line, self.message)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Tok {
    Word(String),
    Num(String),
    Str(GStr),
    Sym(&'static str),
    Newline,
}

#[derive(Clone, Debug)]
struct Token {
    tok: Tok,
    start: usize,
    end: usize,
}

/// The symbols, longest first.
const SYMBOLS: &[&str] = &[
    "==~", "<=>", "?.", "?:", "*.", "==", "!=", "<=", ">=", "&&", "||", "->", "++", "--", "+=",
    "-=", "*=", "/=", "=~", "**", "<<", ">>", "..", "::", "{", "}", "(", ")", "[", "]", ",", ":",
    ";", "=", ".", "+", "-", "*", "/", "%", "!", "<", ">", "?", "&", "|", "^", "~", "@", "$",
];

fn line_of(text: &str, at: usize) -> usize {
    text[..at.min(text.len())].matches('\n').count() + 1
}

/// What a `$` at `i` of a string that interpolates starts: `${…}` or
/// `$name.name`, as the Groovy it holds, and where the string goes on after
/// it. `Ok(None)`: a `$` that stands for itself. `Err`: a `${` that does not
/// end.
fn interpolation(
    src: &str,
    chars: &[(usize, char)],
    i: usize,
) -> Result<Option<(String, usize)>, ()> {
    let at = |i: usize| chars.get(i).map_or(src.len(), |(b, _)| *b);
    let c = |i: usize| chars.get(i).map(|(_, c)| *c);
    if c(i + 1) == Some('{') {
        // To the brace that closes it, past the strings and braces inside.
        let mut depth = 0usize;
        let mut j = i + 1;
        let mut quote: Option<char> = None;
        loop {
            let Some(cj) = c(j) else {
                return Err(());
            };
            match (quote, cj) {
                (Some(_), '\\') if c(j + 1).is_some() => j += 1,
                (Some(q), cj) if cj == q => quote = None,
                (Some(_), _) => {}
                (None, '\'' | '"') => quote = Some(cj),
                (None, '{') => depth += 1,
                (None, '}') => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            j += 1;
        }
        return Ok(Some((src[at(i + 2)..at(j)].trim().to_string(), j + 1)));
    }
    if c(i + 1).is_some_and(|c| c.is_ascii_alphabetic() || c == '_') {
        let mut j = i + 1;
        loop {
            while c(j).is_some_and(|c| c.is_ascii_alphanumeric() || c == '_') {
                j += 1;
            }
            if c(j) == Some('.') && c(j + 1).is_some_and(|c| c.is_ascii_alphabetic() || c == '_') {
                j += 1;
                continue;
            }
            break;
        }
        return Ok(Some((src[at(i + 1)..at(j)].to_string(), j)));
    }
    Ok(None)
}

/// Whether a value may start after `last`: where it does, a `/` starts a
/// slashy string (`/v\d+/`); where it does not, it divides.
fn value_expected(last: Option<&Token>) -> bool {
    match last.map(|t| &t.tok) {
        None | Some(Tok::Newline) => true,
        Some(Tok::Sym(s)) => !matches!(*s, ")" | "]" | "}"),
        Some(Tok::Word(w)) => matches!(w.as_str(), "return" | "case" | "in" | "assert"),
        Some(Tok::Num(_) | Tok::Str(_)) => false,
    }
}

fn lex(src: &str) -> Result<Vec<Token>, Error> {
    let chars: Vec<(usize, char)> = src.char_indices().collect();
    let at = |i: usize| chars.get(i).map_or(src.len(), |(b, _)| *b);
    let c = |i: usize| chars.get(i).map(|(_, c)| *c);
    let error = |i: usize, message: &str| Error {
        line: line_of(src, at(i)),
        message: message.to_string(),
    };
    let mut out = Vec::new();
    let mut i = 0;
    while let Some(ch) = c(i) {
        let start = i;
        match ch {
            '\n' => {
                out.push(Token {
                    tok: Tok::Newline,
                    start: at(i),
                    end: at(i + 1),
                });
                i += 1;
            }
            c0 if c0.is_whitespace() => i += 1,
            // A line continued.
            '\\' if c(i + 1) == Some('\n') => i += 2,
            '/' if c(i + 1) == Some('/') => {
                while c(i).is_some_and(|c| c != '\n') {
                    i += 1;
                }
            }
            '/' if c(i + 1) == Some('*') => {
                i += 2;
                while !(c(i) == Some('*') && c(i + 1) == Some('/')) {
                    if c(i).is_none() {
                        return Err(error(start, "a comment that does not end"));
                    }
                    i += 1;
                }
                i += 2;
            }
            '#' if i == 0 && c(1) == Some('!') => {
                while c(i).is_some_and(|c| c != '\n') {
                    i += 1;
                }
            }
            '\'' | '"' => {
                let triple = c(i + 1) == Some(ch) && c(i + 2) == Some(ch);
                let interpolates = ch == '"';
                i += if triple { 3 } else { 1 };
                let mut parts = Vec::new();
                let mut text = String::new();
                loop {
                    let Some(next) = c(i) else {
                        return Err(error(start, "a string that does not end"));
                    };
                    if next == ch && (!triple || (c(i + 1) == Some(ch) && c(i + 2) == Some(ch))) {
                        i += if triple { 3 } else { 1 };
                        break;
                    }
                    if next == '\n' && !triple {
                        return Err(error(start, "a string that does not end on its line"));
                    }
                    if next == '\\' {
                        let escaped =
                            c(i + 1).ok_or_else(|| error(start, "a string that does not end"))?;
                        match escaped {
                            'n' => text.push('\n'),
                            't' => text.push('\t'),
                            'r' => text.push('\r'),
                            'b' => text.push('\u{8}'),
                            'f' => text.push('\u{c}'),
                            '\n' => {}
                            '\\' | '\'' | '"' | '$' => text.push(escaped),
                            other => {
                                text.push('\\');
                                text.push(other);
                            }
                        }
                        i += 2;
                        continue;
                    }
                    if next == '$' && interpolates {
                        match interpolation(src, &chars, i) {
                            Err(()) => {
                                return Err(error(start, "an interpolation that does not end"))
                            }
                            Ok(Some((expr, after))) => {
                                if !text.is_empty() {
                                    parts.push(Part::Text(std::mem::take(&mut text)));
                                }
                                parts.push(Part::Expr(expr));
                                i = after;
                                continue;
                            }
                            Ok(None) => {}
                        }
                    }
                    text.push(next);
                    i += 1;
                }
                if !text.is_empty() || parts.is_empty() {
                    parts.push(Part::Text(text));
                }
                out.push(Token {
                    tok: Tok::Str(GStr { parts }),
                    start: at(start),
                    end: at(i),
                });
            }
            // A slashy string (`/v\d+/`) where a value stands, and a
            // dollar-slashy one (`$/…/$`) anywhere: a pattern, mostly. Each
            // interpolates; `\/` is a slashy string's `/`, and `$$` and `$/`
            // are a dollar-slashy string's `$` and `/`.
            '/' if value_expected(out.last()) => {
                i += 1;
                let mut parts = Vec::new();
                let mut text = String::new();
                loop {
                    let Some(next) = c(i) else {
                        return Err(error(start, "a string that does not end"));
                    };
                    match next {
                        '/' => {
                            i += 1;
                            break;
                        }
                        '\\' if c(i + 1) == Some('/') => {
                            text.push('/');
                            i += 2;
                        }
                        '$' => match interpolation(src, &chars, i) {
                            Err(()) => {
                                return Err(error(start, "an interpolation that does not end"))
                            }
                            Ok(Some((expr, after))) => {
                                if !text.is_empty() {
                                    parts.push(Part::Text(std::mem::take(&mut text)));
                                }
                                parts.push(Part::Expr(expr));
                                i = after;
                            }
                            Ok(None) => {
                                text.push('$');
                                i += 1;
                            }
                        },
                        other => {
                            text.push(other);
                            i += 1;
                        }
                    }
                }
                if !text.is_empty() || parts.is_empty() {
                    parts.push(Part::Text(text));
                }
                out.push(Token {
                    tok: Tok::Str(GStr { parts }),
                    start: at(start),
                    end: at(i),
                });
            }
            '$' if c(i + 1) == Some('/') => {
                i += 2;
                let mut parts = Vec::new();
                let mut text = String::new();
                loop {
                    let Some(next) = c(i) else {
                        return Err(error(start, "a string that does not end"));
                    };
                    match (next, c(i + 1)) {
                        ('/', Some('$')) => {
                            i += 2;
                            break;
                        }
                        ('$', Some(escaped @ ('$' | '/'))) => {
                            text.push(escaped);
                            i += 2;
                        }
                        ('$', _) => match interpolation(src, &chars, i) {
                            Err(()) => {
                                return Err(error(start, "an interpolation that does not end"))
                            }
                            Ok(Some((expr, after))) => {
                                if !text.is_empty() {
                                    parts.push(Part::Text(std::mem::take(&mut text)));
                                }
                                parts.push(Part::Expr(expr));
                                i = after;
                            }
                            Ok(None) => {
                                text.push('$');
                                i += 1;
                            }
                        },
                        (other, _) => {
                            text.push(other);
                            i += 1;
                        }
                    }
                }
                if !text.is_empty() || parts.is_empty() {
                    parts.push(Part::Text(text));
                }
                out.push(Token {
                    tok: Tok::Str(GStr { parts }),
                    start: at(start),
                    end: at(i),
                });
            }
            c0 if c0.is_ascii_digit() => {
                while c(i).is_some_and(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_') {
                    // A range's `..` is no part of a number.
                    if c(i) == Some('.') && c(i + 1) == Some('.') {
                        break;
                    }
                    i += 1;
                }
                out.push(Token {
                    tok: Tok::Num(src[at(start)..at(i)].to_string()),
                    start: at(start),
                    end: at(i),
                });
            }
            c0 if c0.is_alphabetic()
                || c0 == '_'
                || c0 == '$' && c(i + 1).is_some_and(|c| c.is_alphabetic()) =>
            {
                i += 1;
                while c(i).is_some_and(|c| c.is_alphanumeric() || c == '_' || c == '$') {
                    i += 1;
                }
                out.push(Token {
                    tok: Tok::Word(src[at(start)..at(i)].to_string()),
                    start: at(start),
                    end: at(i),
                });
            }
            _ => {
                let rest = &src[at(i)..];
                let Some(symbol) = SYMBOLS.iter().find(|s| rest.starts_with(**s)) else {
                    return Err(error(
                        i,
                        &format!("`{ch}` is not Groovy a pipeline is written in"),
                    ));
                };
                let len = symbol.chars().count();
                out.push(Token {
                    tok: Tok::Sym(symbol),
                    start: at(i),
                    end: at(i + len),
                });
                i += len;
            }
        }
    }
    Ok(out)
}

struct Parser<'a> {
    src: &'a str,
    tokens: Vec<Token>,
    at: usize,
}

/// The most blocks one inside another.
const MAX_DEPTH: usize = 64;

impl Parser<'_> {
    fn peek(&self) -> Option<&Tok> {
        self.tokens.get(self.at).map(|t| &t.tok)
    }

    fn peek_n(&self, n: usize) -> Option<&Tok> {
        self.tokens.get(self.at + n).map(|t| &t.tok)
    }

    fn is(&self, symbol: &str) -> bool {
        matches!(self.peek(), Some(Tok::Sym(s)) if *s == symbol)
    }

    fn start(&self) -> usize {
        self.tokens.get(self.at).map_or(self.src.len(), |t| t.start)
    }

    fn end_of(&self, last: usize) -> usize {
        self.tokens.get(last).map_or(self.src.len(), |t| t.end)
    }

    fn error(&self, message: impl Into<String>) -> Error {
        Error {
            line: line_of(self.src, self.start()),
            message: message.into(),
        }
    }

    fn skip_newlines(&mut self) {
        while matches!(self.peek(), Some(Tok::Newline)) || self.is(";") {
            self.at += 1;
        }
    }

    /// The statements of a block or the file, to its closing brace.
    fn stmts(&mut self, depth: usize, top: bool) -> Result<Vec<Stmt>, Error> {
        if depth > MAX_DEPTH {
            return Err(self.error("blocks nest too deep"));
        }
        let mut out = Vec::new();
        loop {
            self.skip_newlines();
            match self.peek() {
                None if top => return Ok(out),
                None => return Err(self.error("a block that is not closed")),
                Some(Tok::Sym("}")) if !top => return Ok(out),
                Some(Tok::Sym("}")) => return Err(self.error("a `}` that closes nothing")),
                _ => out.push(self.stmt(depth)?),
            }
        }
    }

    fn block(&mut self, depth: usize) -> Result<Block, Error> {
        // At the `{`.
        let open = self.at;
        self.at += 1;
        let stmts = self.stmts(depth + 1, false)?;
        let close = self.at;
        self.at += 1;
        let text = self.src[self.end_of(open)..self.tokens[close].start].to_string();
        Ok(Block { stmts, text })
    }

    /// From the token at `from` to the end of its statement, as written: to
    /// a new line or a `;` outside brackets, or the `}` of the block.
    fn rest_of_stmt(&mut self, from: usize) -> Result<String, Error> {
        let mut depth = 0usize;
        while let Some(tok) = self.peek() {
            match tok {
                Tok::Sym("(" | "[" | "{") => depth += 1,
                Tok::Sym(")" | "]") if depth > 0 => depth -= 1,
                Tok::Sym("}") if depth > 0 => depth -= 1,
                Tok::Sym("}") => break,
                Tok::Newline | Tok::Sym(";") if depth == 0 => {
                    // A line that ends in an operator goes on.
                    let last = self.tokens[self.at - 1].tok.clone();
                    if matches!(last, Tok::Sym(s) if !matches!(s, ")" | "]" | "}"))
                        && self.at > from
                    {
                        self.at += 1;
                        continue;
                    }
                    break;
                }
                _ => {}
            }
            self.at += 1;
        }
        if depth > 0 {
            return Err(self.error("brackets that are not closed"));
        }
        let end = self.end_of(self.at.saturating_sub(1).max(from));
        Ok(self.src[self.tokens[from].start..end].to_string())
    }

    fn at_end_of_stmt(&self) -> bool {
        matches!(
            self.peek(),
            None | Some(Tok::Newline) | Some(Tok::Sym(";" | "}"))
        )
    }

    fn stmt(&mut self, depth: usize) -> Result<Stmt, Error> {
        let from = self.at;
        let Some(Tok::Word(first)) = self.peek().cloned() else {
            return Ok(Stmt::Other(self.rest_of_stmt(from)?));
        };
        self.at += 1;
        // A name with its dots: `env.FOO`.
        let mut name = first;
        while self.is(".") && matches!(self.peek_n(1), Some(Tok::Word(_))) {
            if let Some(Tok::Word(next)) = self.peek_n(1).cloned() {
                name.push('.');
                name.push_str(&next);
            }
            self.at += 2;
        }
        let made = match self.peek() {
            Some(Tok::Sym("(")) => {
                self.at += 1;
                let Ok(args) = self.args(")", depth) else {
                    self.at = from;
                    return Ok(Stmt::Other(self.rest_of_stmt(from)?));
                };
                let block = match self.is("{") {
                    true => Some(self.block(depth)?),
                    false => None,
                };
                (args, block)
            }
            Some(Tok::Sym("{")) => (Vec::new(), Some(self.block(depth)?)),
            Some(Tok::Sym("=")) => {
                self.at += 1;
                let value = self.expr(depth);
                let Ok(value) = value
                    .map_err(|_| ())
                    .and_then(|v| match self.at_end_of_stmt() {
                        true => Ok(v),
                        false => Err(()),
                    })
                else {
                    self.at = from;
                    return Ok(Stmt::Other(self.rest_of_stmt(from)?));
                };
                if !self.at_end_of_stmt() {
                    self.at = from;
                    return Ok(Stmt::Other(self.rest_of_stmt(from)?));
                }
                let text = self.src[self.tokens[from].start..self.end_of(self.at - 1)].to_string();
                return Ok(Stmt::Assign { name, value, text });
            }
            _ if self.at_end_of_stmt() => (Vec::new(), None),
            Some(Tok::Str(_) | Tok::Num(_) | Tok::Word(_) | Tok::Sym("[" | "-")) => {
                // A call whose argument is Groovy (`def v = …`) is Groovy.
                let groovy = |args: &Vec<Arg>| {
                    args.iter().any(|a| {
                        matches!(a, Arg::Pos(Expr::Other(_)) | Arg::Named(_, Expr::Other(_)))
                    })
                };
                match self
                    .command_args(depth)
                    .and_then(|args| match groovy(&args) {
                        true => Err(self.error("Groovy")),
                        false => Ok(args),
                    }) {
                    Ok(args) => {
                        let block = match self.is("{") {
                            true => Some(self.block(depth)?),
                            false => None,
                        };
                        (args, block)
                    }
                    Err(_) => {
                        self.at = from;
                        return Ok(Stmt::Other(self.rest_of_stmt(from)?));
                    }
                }
            }
            _ => {
                self.at = from;
                return Ok(Stmt::Other(self.rest_of_stmt(from)?));
            }
        };
        if !self.at_end_of_stmt() {
            // A call that goes on (`sh(…).trim()`, `x == y`): Groovy.
            self.at = from;
            return Ok(Stmt::Other(self.rest_of_stmt(from)?));
        }
        let (args, block) = made;
        let text = self.src[self.tokens[from].start..self.end_of(self.at - 1)].to_string();
        Ok(Stmt::Call {
            name,
            args,
            block,
            text,
        })
    }

    /// The arguments of a call written without parentheses: to the end of
    /// the line, or a `{`.
    fn command_args(&mut self, depth: usize) -> Result<Vec<Arg>, Error> {
        let mut out = Vec::new();
        loop {
            out.push(self.arg(depth)?);
            if self.is(",") {
                self.at += 1;
                while matches!(self.peek(), Some(Tok::Newline)) {
                    self.at += 1;
                }
                continue;
            }
            if self.at_end_of_stmt() || self.is("{") {
                return Ok(out);
            }
            return Err(self.error("arguments that go on as Groovy"));
        }
    }

    /// Arguments in brackets, to the bracket that closes them.
    fn args(&mut self, close: &str, depth: usize) -> Result<Vec<Arg>, Error> {
        let mut out = Vec::new();
        loop {
            while matches!(self.peek(), Some(Tok::Newline)) {
                self.at += 1;
            }
            if self.is(close) {
                self.at += 1;
                return Ok(out);
            }
            out.push(self.arg(depth)?);
            while matches!(self.peek(), Some(Tok::Newline)) {
                self.at += 1;
            }
            if self.is(",") {
                self.at += 1;
            } else if !self.is(close) {
                return Err(self.error(format!("a `{close}` is missing")));
            }
        }
    }

    fn arg(&mut self, depth: usize) -> Result<Arg, Error> {
        let key = match (self.peek(), self.peek_n(1)) {
            (Some(Tok::Word(name)), Some(Tok::Sym(":"))) => Some(name.clone()),
            (Some(Tok::Str(s)), Some(Tok::Sym(":"))) => s.plain(),
            _ => None,
        };
        if let Some(key) = key {
            self.at += 2;
            while matches!(self.peek(), Some(Tok::Newline)) {
                self.at += 1;
            }
            return Ok(Arg::Named(key, self.expr(depth)?));
        }
        Ok(Arg::Pos(self.expr(depth)?))
    }

    /// A value: one this reads as such, or Groovy as written, to where the
    /// argument or statement ends.
    fn expr(&mut self, depth: usize) -> Result<Expr, Error> {
        let from = self.at;
        let value = self.primary(depth)?;
        if matches!(
            self.peek(),
            None | Some(Tok::Newline) | Some(Tok::Sym("," | ")" | "]" | "}" | ";"))
        ) {
            return Ok(value);
        }
        // Groovy past the value: its text, to the end of the argument.
        self.at = from;
        let mut depth_here = 0usize;
        while let Some(tok) = self.peek() {
            match tok {
                Tok::Sym("(" | "[" | "{") => depth_here += 1,
                Tok::Sym(")" | "]" | "}") if depth_here == 0 => break,
                Tok::Sym(")" | "]" | "}") => depth_here -= 1,
                Tok::Sym(",") if depth_here == 0 => break,
                Tok::Newline | Tok::Sym(";") if depth_here == 0 => {
                    let last = self.tokens[self.at - 1].tok.clone();
                    if matches!(last, Tok::Sym(s) if !matches!(s, ")" | "]" | "}")) {
                        self.at += 1;
                        continue;
                    }
                    break;
                }
                _ => {}
            }
            self.at += 1;
        }
        // Nothing past where the value was to start: no value is there.
        if self.at == from {
            return Err(self.error("a value is missing"));
        }
        let end = self.end_of(self.at - 1);
        Ok(Expr::Other(
            self.src[self.tokens[from].start..end].to_string(),
        ))
    }

    fn primary(&mut self, depth: usize) -> Result<Expr, Error> {
        if depth > MAX_DEPTH {
            return Err(self.error("values nest too deep"));
        }
        let Some(tok) = self.peek().cloned() else {
            return Err(self.error("a value is missing"));
        };
        match tok {
            Tok::Str(s) => {
                self.at += 1;
                Ok(Expr::Str(s))
            }
            Tok::Num(n) => {
                self.at += 1;
                Ok(Expr::Num(n))
            }
            Tok::Sym("-") => match self.peek_n(1).cloned() {
                Some(Tok::Num(n)) => {
                    self.at += 2;
                    Ok(Expr::Num(format!("-{n}")))
                }
                // A `-` that is no number's: `expr` reads it as Groovy.
                _ => {
                    self.at += 1;
                    Ok(Expr::Other(String::new()))
                }
            },
            Tok::Sym("[") => {
                self.at += 1;
                // `[:]`, an empty map.
                if self.is(":") && matches!(self.peek_n(1), Some(Tok::Sym("]"))) {
                    self.at += 2;
                    return Ok(Expr::Map(Vec::new()));
                }
                let args = self.args("]", depth + 1)?;
                if !args.is_empty() && args.iter().all(|a| matches!(a, Arg::Named(..))) {
                    return Ok(Expr::Map(
                        args.into_iter()
                            .filter_map(|a| match a {
                                Arg::Named(k, v) => Some((k, v)),
                                Arg::Pos(_) => None,
                            })
                            .collect(),
                    ));
                }
                if args.iter().any(|a| matches!(a, Arg::Named(..))) {
                    return Err(self.error("a list that is part a map"));
                }
                Ok(Expr::List(
                    args.into_iter()
                        .filter_map(|a| match a {
                            Arg::Pos(v) => Some(v),
                            Arg::Named(..) => None,
                        })
                        .collect(),
                ))
            }
            Tok::Sym("{") => Ok(Expr::Closure(self.block(depth)?)),
            Tok::Word(word) => {
                self.at += 1;
                match word.as_str() {
                    "true" => return Ok(Expr::Bool(true)),
                    "false" => return Ok(Expr::Bool(false)),
                    "null" => return Ok(Expr::Null),
                    _ => {}
                }
                let mut name = word;
                while self.is(".") && matches!(self.peek_n(1), Some(Tok::Word(_))) {
                    if let Some(Tok::Word(next)) = self.peek_n(1).cloned() {
                        name.push('.');
                        name.push_str(&next);
                    }
                    self.at += 2;
                }
                if self.is("(") {
                    self.at += 1;
                    let args = self.args(")", depth + 1)?;
                    let block = match self.is("{") {
                        true => Some(self.block(depth)?),
                        false => None,
                    };
                    return Ok(Expr::Call { name, args, block });
                }
                if self.is("{") && !name.contains('.') {
                    let block = self.block(depth)?;
                    return Ok(Expr::Call {
                        name,
                        args: Vec::new(),
                        block: Some(block),
                    });
                }
                Ok(Expr::Path(name))
            }
            // A bracket that closes what is open: no value stands here.
            Tok::Sym(")" | "]" | "}") => Err(self.error("a value is missing")),
            // Something that is no value here: `expr` reads it as Groovy.
            _ => {
                self.at += 1;
                Ok(Expr::Other(String::new()))
            }
        }
    }
}

/// Read a file's Groovy as statements.
pub(crate) fn parse(src: &str) -> Result<Vec<Stmt>, Error> {
    // A byte order mark, which some editors begin a file with, is no
    // Groovy; and a line ends at `\n` whether or not a `\r` comes before it.
    let src = src.strip_prefix('\u{feff}').unwrap_or(src);
    let src = src.replace("\r\n", "\n");
    let tokens = lex(&src)?;
    let mut parser = Parser {
        src: &src,
        tokens,
        at: 0,
    };
    parser.stmts(0, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(s: &str) -> Expr {
        Expr::Str(GStr {
            parts: vec![Part::Text(s.into())],
        })
    }

    #[test]
    fn a_pipeline_is_read_as_its_sections_and_steps() {
        let src = "pipeline {\n  agent { docker { image 'node:22' } }\n  stages {\n    stage('Build') {\n      steps {\n        sh 'npm ci'\n        sh(script: \"echo ${env.BRANCH_NAME}\", label: 'say')\n        junit testResults: 'a.xml',\n              allowEmptyResults: true\n      }\n    }\n  }\n}\n";
        let stmts = parse(src).unwrap();
        let Stmt::Call {
            name,
            block: Some(pipeline),
            ..
        } = &stmts[0]
        else {
            panic!("{stmts:?}")
        };
        assert_eq!(name, "pipeline");
        let Stmt::Call {
            name,
            block: Some(agent),
            ..
        } = &pipeline.stmts[0]
        else {
            panic!()
        };
        assert_eq!(name, "agent");
        assert!(matches!(&agent.stmts[0], Stmt::Call { name, .. } if name == "docker"));
        let Stmt::Call {
            block: Some(stages),
            ..
        } = &pipeline.stmts[1]
        else {
            panic!()
        };
        let Stmt::Call {
            name,
            args,
            block: Some(stage),
            ..
        } = &stages.stmts[0]
        else {
            panic!()
        };
        assert_eq!(
            (name.as_str(), args.as_slice()),
            ("stage", &[Arg::Pos(text("Build"))][..])
        );
        let Stmt::Call {
            block: Some(steps), ..
        } = &stage.stmts[0]
        else {
            panic!()
        };
        assert_eq!(steps.stmts.len(), 3, "{steps:#?}");
        assert!(
            matches!(&steps.stmts[0], Stmt::Call { name, args, .. } if name == "sh" && args == &[Arg::Pos(text("npm ci"))])
        );
        let Stmt::Call { args, .. } = &steps.stmts[1] else {
            panic!()
        };
        assert_eq!(
            args[0],
            Arg::Named(
                "script".into(),
                Expr::Str(GStr {
                    parts: vec![
                        Part::Text("echo ".into()),
                        Part::Expr("env.BRANCH_NAME".into())
                    ]
                })
            )
        );
        let Stmt::Call { args, .. } = &steps.stmts[2] else {
            panic!()
        };
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn strings_are_read_as_groovy_reads_them() {
        let one = |src: &str| -> Expr {
            let stmts = parse(&format!("x = {src}\n")).unwrap();
            match &stmts[0] {
                Stmt::Assign { value, .. } => value.clone(),
                other => panic!("{other:?}"),
            }
        };
        assert_eq!(one("'a\\'b $c'"), text("a'b $c"));
        assert_eq!(
            one("\"a \\$b $c.d\""),
            Expr::Str(GStr {
                parts: vec![Part::Text("a $b ".into()), Part::Expr("c.d".into())]
            })
        );
        assert_eq!(one("'''x\ny'''"), text("x\ny"));
        assert_eq!(
            one("\"\"\"${ a ? \"}\" : b }!\"\"\""),
            Expr::Str(GStr {
                parts: vec![Part::Expr("a ? \"}\" : b".into()), Part::Text("!".into())]
            })
        );
        assert_eq!(
            one("credentials('id')"),
            Expr::Call {
                name: "credentials".into(),
                args: vec![Arg::Pos(text("id"))],
                block: None
            }
        );
        assert_eq!(one("['a', 'b']"), Expr::List(vec![text("a"), text("b")]));
        assert_eq!(
            one("sh(script: 'x', returnStdout: true).trim()"),
            Expr::Other("sh(script: 'x', returnStdout: true).trim()".into())
        );
    }

    #[test]
    fn groovy_that_is_no_step_is_kept_as_written() {
        let stmts = parse(
            "script {\n  def v = readFile('VERSION').trim()\n  if (v) {\n    echo v\n  }\n}\n",
        )
        .unwrap();
        let Stmt::Call {
            block: Some(script),
            ..
        } = &stmts[0]
        else {
            panic!()
        };
        assert_eq!(
            script.stmts[0],
            Stmt::Other("def v = readFile('VERSION').trim()".into())
        );
        assert!(matches!(&script.stmts[1], Stmt::Call { name, .. } if name == "if"));
        assert!(script.text.contains("readFile"));
    }

    #[test]
    fn what_is_no_groovy_says_where() {
        assert_eq!(parse("pipeline {\n  x 'y\n}").unwrap_err().line, 2);
        assert!(parse("pipeline {\n")
            .unwrap_err()
            .message
            .contains("not closed"));
        assert!(parse("}").is_err());
    }

    #[test]
    fn a_file_half_edited_is_read_as_far_as_it_goes() {
        // A bracket that closes where a value was to be: Groovy as written,
        // and no panic.
        for src in [
            "steps {\n  timeout(time: ) { sh 'make' }\n}\n",
            "x = ) y\n",
            "foo(a: ) b)\n",
            "foo(, x y)\n",
        ] {
            let stmts = parse(src).unwrap_or_else(|e| panic!("{src:?}: {e}"));
            assert!(!stmts.is_empty(), "{src:?}");
        }
        let stmts = parse("x = ) y\n").unwrap();
        assert_eq!(stmts[0], Stmt::Other("x = ) y".into()));
    }

    #[test]
    fn values_nest_only_so_deep() {
        // Lists and calls one inside another, deeper than a file has them:
        // Groovy as written, read without a stack that overflows.
        let deep = format!(
            "steps {{\n  x({}{})\n}}\n",
            "[".repeat(2000),
            "]".repeat(2000)
        );
        let stmts = parse(&deep).unwrap();
        let Stmt::Call {
            block: Some(steps), ..
        } = &stmts[0]
        else {
            panic!("{stmts:?}")
        };
        assert!(
            matches!(&steps.stmts[0], Stmt::Other(_)),
            "{:?}",
            steps.stmts[0]
        );
        let calls = format!("x = {}1{}\n", "f(".repeat(2000), ")".repeat(2000));
        assert!(parse(&calls).is_ok());
    }

    #[test]
    fn a_byte_order_mark_and_a_windows_line_end_are_no_groovy() {
        let stmts = parse("\u{feff}pipeline {\n  agent any\n}\n").unwrap();
        assert!(matches!(&stmts[0], Stmt::Call { name, .. } if name == "pipeline"));
        let stmts = parse("x = '''make \\\r\n  all\r\n'''\r\ny = 1 + \\\r\n 2\r\n").unwrap();
        assert!(
            matches!(&stmts[0], Stmt::Assign { value, .. } if *value == text("make   all\n")),
            "{stmts:?}"
        );
        assert_eq!(stmts.len(), 2, "{stmts:?}");
    }

    #[test]
    fn slashy_strings_are_strings() {
        let one = |src: &str| -> Expr {
            let stmts = parse(&format!("x = {src}\n")).unwrap();
            match &stmts[0] {
                Stmt::Assign { value, .. } => value.clone(),
                other => panic!("{other:?}"),
            }
        };
        assert_eq!(one("/v\\d+\\.\\d+$/"), text("v\\d+\\.\\d+$"));
        assert_eq!(one("/a\\/b/"), text("a/b"));
        assert_eq!(
            one("/^${env.PREFIX}-\\d+/"),
            Expr::Str(GStr {
                parts: vec![
                    Part::Text("^".into()),
                    Part::Expr("env.PREFIX".into()),
                    Part::Text("-\\d+".into())
                ]
            })
        );
        assert_eq!(one("$/a/b$$c$/d/$"), text("a/b$c/d"));
        // A `/` after a value divides.
        assert_eq!(one("a / 2"), Expr::Other("a / 2".into()));
        // In a `when`, the expression is kept as written.
        let stmts =
            parse("when {\n  expression { env.TAG_NAME ==~ /v\\d+\\.\\d+\\.\\d+/ }\n}\n").unwrap();
        let Stmt::Call {
            block: Some(when), ..
        } = &stmts[0]
        else {
            panic!("{stmts:?}")
        };
        let Stmt::Call {
            block: Some(expression),
            ..
        } = &when.stmts[0]
        else {
            panic!("{when:?}")
        };
        assert_eq!(
            expression.text.trim(),
            "env.TAG_NAME ==~ /v\\d+\\.\\d+\\.\\d+/"
        );
        assert!(parse("x = /never ends\n").is_err());
    }
}
