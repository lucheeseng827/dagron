//! What the importers share: the report, and how a pipeline is written.
//!
//! Each importer reads another CI's file and writes the pipeline it would
//! be ([`crate::github`], [`crate::gitlab`], [`crate::jenkins`]), under the
//! same report ([`Imported`]) and by the same rules:
//!
//! - **Nothing is dropped without a word.** What has no place of its own in
//!   the pipeline is a [`Note`]: *changed*, *left* or *to do*.
//! - **A job with something to do does not pass.** It starts with a step
//!   that fails and says what is left ([`failing`]).
//! - **No brace of the file's is a template here.** The importer writes its
//!   own templates with two characters no file's text is left with
//!   ([`OPEN`], [`CLOSE`]), and gives them their braces at the end
//!   ([`settle`]), when every `{{` still there is the file's own: that one is
//!   parted, and said.

use std::collections::BTreeSet;
use std::path::Path;

use serde_yaml::{Mapping, Value};

/// What became of something the workflow held.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    /// Not there, and the job is not right without it.
    Todo,
    /// There, and it reads differently.
    Changed,
    /// Not there, and the pipeline does without.
    Left,
}

/// One line of the report: where in the workflow, and what became of it.
/// One for each place, so that what is said of ten places counts as ten.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Note {
    pub at: String,
    pub kind: Kind,
    pub text: String,
}

/// A workflow as a pipeline, and what was not carried over as it was.
#[derive(Clone, Debug)]
pub struct Imported {
    /// What was imported, as the report's first line names it: "a GitHub
    /// Actions workflow".
    pub source: &'static str,
    /// The pipeline, as YAML.
    pub pipeline: String,
    pub notes: Vec<Note>,
}

impl Imported {
    /// How many things are left to do by hand.
    pub fn todo(&self) -> usize {
        self.notes.iter().filter(|n| n.kind == Kind::Todo).count()
    }

    /// The pipeline under its report, as the comment a file begins with.
    pub fn with_report(&self) -> String {
        let mut out = format!("# Imported from {} by dagron-import.\n", self.source);
        for (kind, heading) in [
            (Kind::Todo, "To do by hand"),
            (Kind::Changed, "Changed on the way"),
            (Kind::Left, "Not carried over"),
        ] {
            let of: Vec<&Note> = self.notes.iter().filter(|n| n.kind == kind).collect();
            if of.is_empty() {
                continue;
            }
            out.push_str(&format!("#\n# {heading} ({}):\n", of.len()));
            // What is said of many places is said once, with every place.
            let mut said: Vec<(&str, Vec<&str>)> = Vec::new();
            for note in of {
                match said.iter_mut().find(|(text, _)| *text == note.text) {
                    Some((_, places)) => places.push(&note.at),
                    None => said.push((&note.text, vec![&note.at])),
                }
            }
            for (text, places) in said {
                out.push_str(&format!("#   {}: {}\n", places.join(", "), one_line(text)));
            }
        }
        out.push_str(&self.pipeline);
        out
    }
}

/// What a pipeline imported from `file` is called unless it is given a
/// name: the file's own name, without its ending. A `.gitlab-ci.yml`'s is
/// `gitlab-ci`, and a `Jenkinsfile`'s is `ci`; a GitHub workflow's is its
/// file's as it stands. `importer` is `github`, `gitlab` or `jenkins`.
pub fn pipeline_name(importer: &str, file: &Path) -> Option<String> {
    let stem = file.file_stem()?.to_str()?;
    let stem = match importer {
        "github" => stem.to_string(),
        _ => stem.trim_start_matches('.').to_string(),
    };
    match importer == "jenkins" && stem.eq_ignore_ascii_case("jenkinsfile") {
        true => Some("ci".to_string()),
        false => Some(stem),
    }
}

/// The braces of a template the importer writes itself, while the pipeline
/// is being made. No text of the workflow's holds either ([`clean`]), so
/// when the pipeline is done every `{{` in it is the workflow's own.
pub(crate) const OPEN: char = '\u{1}';
pub(crate) const CLOSE: char = '\u{2}';

/// `{{ name }}`, as the importer's own template.
pub(crate) fn tpl(name: &str) -> String {
    format!("{OPEN}{name}{CLOSE}")
}

/// The workflow without the two characters the importer's own templates
/// are marked with: they are no text a workflow means anything by.
pub(crate) fn clean(value: &mut Value) {
    let cleaned = |text: &str| text.replace([OPEN, CLOSE], "");
    let marked =
        |value: &Value| matches!(value, Value::String(text) if text.contains([OPEN, CLOSE]));
    match value {
        Value::String(text) if text.contains([OPEN, CLOSE]) => *text = cleaned(text),
        Value::Sequence(items) => items.iter_mut().for_each(clean),
        Value::Mapping(map) => {
            let entries: Vec<(Value, Value)> = std::mem::take(map).into_iter().collect();
            // A name that comes to another's once cleaned is not the other:
            // it is kept, with each of the two characters a `_` (and a
            // number, where that name is taken too), and not written over
            // the other.
            let unmarked: Vec<Value> = entries
                .iter()
                .filter(|(name, _)| !marked(name))
                .map(|(name, _)| name.clone())
                .collect();
            let taken =
                |map: &Mapping, name: &Value| map.contains_key(name) || unmarked.contains(name);
            for (mut name, mut held) in entries {
                if let Value::String(text) = &name {
                    if text.contains([OPEN, CLOSE]) {
                        let mut own = Value::String(cleaned(text));
                        if taken(map, &own) {
                            let under = text.replace([OPEN, CLOSE], "_");
                            own = Value::String(under.clone());
                            let mut n = 2;
                            while taken(map, &own) {
                                own = Value::String(format!("{under}_{n}"));
                                n += 1;
                            }
                        }
                        name = own;
                    }
                }
                clean(&mut name);
                clean(&mut held);
                map.insert(name, held);
            }
        }
        _ => {}
    }
}

/// Make a finished part of the pipeline say what it means to dagron: the
/// importer's own templates are given their braces, and every `{{` the
/// workflow held is parted (`{ {`), so that nothing of the workflow's is
/// read as a template. Where one was parted is added to `parted`, as its
/// place under `at`.
///
/// `single` is the axis of a matrix whose items are plain values: its
/// template is `{{ item }}`, where every other matrix's is
/// `{{ item.axis }}`.
pub(crate) fn settle(value: &mut Value, at: &str, single: Option<&str>, parted: &mut Vec<String>) {
    // A brace of the workflow's just before a template of the importer's
    // would be read with the template's own as the template's start.
    let before_own = format!("{{{OPEN}");
    let settled = |text: &str| {
        // Until none is left: three braces parted once are two again.
        let mut out = text.to_string();
        while out.contains("{{") {
            out = out.replace("{{", "{ {");
        }
        out = out.replace(&before_own, &format!("{{ {OPEN}"));
        if let Some(axis) = single {
            out = out.replace(&tpl(&format!("item.{axis}")), &tpl("item"));
        }
        out.replace(OPEN, "{{ ").replace(CLOSE, " }}")
    };
    match value {
        Value::String(text) => {
            if text.contains("{{") || text.contains(&before_own) {
                parted.push(at.to_string());
            }
            *text = settled(text);
        }
        Value::Sequence(items) => {
            for (n, item) in items.iter_mut().enumerate() {
                settle(item, &format!("{at}[{n}]"), single, parted);
            }
        }
        Value::Mapping(map) => {
            let entries: Vec<(Value, Value)> = std::mem::take(map).into_iter().collect();
            for (mut name, mut held) in entries {
                let under = match (at, scalar(&name)) {
                    ("", Some(name)) => name,
                    (at, Some(name)) => format!("{at}.{name}"),
                    (at, None) => at.to_string(),
                };
                settle(&mut name, &under, single, parted);
                settle(&mut held, &under, single, parted);
                map.insert(name, held);
            }
        }
        _ => {}
    }
}

/// What is said of a `{{` that was the workflow's own. It names the two
/// braces in words: the step that says what is left to do shows every two
/// of them as `[[`, which would be the wrong text here.
pub(crate) const PARTED: &str = "the text holds two opening braces together, or one just before an expression, which a pipeline reads as a template of its own: a space is put between them here (`{ {`). Keep what needs the two braces in a file of the repository, or have the script put them together";

/// `text` on one line. A control character is a space: in the comment a
/// report is written as, one would make the file no YAML. The importer's
/// own two marks are kept, for [`said`] to give their braces.
pub(crate) fn one_line(text: &str) -> String {
    text.chars()
        .map(|c| match c.is_control() && c != OPEN && c != CLOSE {
            true => ' ',
            false => c,
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// A cache of the job's own, as `workspace.cache` takes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JobCache {
    pub(crate) prefix: String,
    pub(crate) key_files: Vec<String>,
    pub(crate) paths: Vec<String>,
}

impl JobCache {
    pub(crate) fn new(prefix: &str, key_files: &[&str], paths: &[&str]) -> Self {
        Self {
            prefix: prefix.to_string(),
            key_files: key_files.iter().map(|s| s.to_string()).collect(),
            paths: paths.iter().map(|s| s.to_string()).collect(),
        }
    }

    pub(crate) fn value(&self) -> Value {
        let list =
            |items: &[String]| Value::Sequence(items.iter().cloned().map(Value::String).collect());
        let mut m = Mapping::new();
        m.insert(key("prefix"), Value::String(self.prefix.clone()));
        if !self.key_files.is_empty() {
            m.insert(key("key_files"), list(&self.key_files));
        }
        m.insert(key("paths"), list(&self.paths));
        Value::Mapping(m)
    }

    /// Said in a note: the prefix, and what makes the key.
    pub(crate) fn said(&self) -> String {
        if self.key_files.is_empty() {
            format!("the job's cache {:?}, under that key alone", self.prefix)
        } else {
            format!(
                "the job's cache {:?}, keyed by {}",
                self.prefix,
                self.key_files.join(", ")
            )
        }
    }
}

pub(crate) fn key(name: &str) -> Value {
    Value::String(name.to_string())
}

pub(crate) fn get<'a>(map: &'a Mapping, name: &str) -> Option<&'a Value> {
    map.get(key(name))
}

/// A scalar as text: a string, a number or a boolean.
pub(crate) fn scalar(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// A string, or a list of them.
pub(crate) fn strings(value: &Value) -> Vec<String> {
    match value {
        Value::Sequence(items) => items.iter().filter_map(scalar).collect(),
        other => scalar(other).into_iter().collect(),
    }
}

pub(crate) fn list(items: impl IntoIterator<Item = String>) -> Value {
    Value::Sequence(items.into_iter().map(Value::String).collect())
}

/// A name a pipeline or a task may have: letters, digits, `.`, `_`, `-`.
pub(crate) fn as_name(text: &str) -> String {
    let mut out = String::new();
    for c in text.trim().chars() {
        match c {
            c if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') => {
                out.push(c.to_ascii_lowercase())
            }
            _ if !out.ends_with('-') => out.push('-'),
            _ => {}
        }
    }
    out.trim_matches('-').chars().take(100).collect()
}

/// A shell variable's name made of `text`.
pub(crate) fn as_variable(text: &str) -> String {
    text.chars()
        .map(|c| match c {
            c if c.is_ascii_alphanumeric() => c.to_ascii_uppercase(),
            _ => '_',
        })
        .collect()
}

/// A parameter's name made of an input's: a workflow's may hold a dash.
pub(crate) fn as_parameter(text: &str) -> String {
    let name: String = text
        .chars()
        .map(|c| match c {
            c if c.is_ascii_alphanumeric() || c == '_' => c,
            _ => '_',
        })
        .collect();
    match name.chars().next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => name,
        _ => format!("_{name}"),
    }
}

/// Whether `name` is one a shell's variable may have.
pub(crate) fn is_shell_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A service's name as one dagron takes: a host name's label, which begins
/// with a letter. `None` where `text` has nothing to make one of.
pub(crate) fn as_service_name(text: &str) -> Option<String> {
    let mut out = String::new();
    for c in text.trim().chars() {
        match c {
            c if c.is_ascii_alphanumeric() => out.push(c.to_ascii_lowercase()),
            _ if !out.is_empty() && !out.ends_with('-') => out.push('-'),
            _ => {}
        }
    }
    let out: String = out.chars().take(40).collect();
    let out = out.trim_matches('-').to_string();
    (out.starts_with(|c: char| c.is_ascii_lowercase()) && out != "localhost").then_some(out)
}

/// A word of the report as a person reads it: on one line, and with a
/// template of the importer's own, should it name one, in its braces.
pub(crate) fn said(text: &str) -> String {
    one_line(text).replace(OPEN, "{{ ").replace(CLOSE, " }}")
}

pub(crate) fn is_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Where `text` reads a variable, as the shell reads one: each `$NAME` and
/// `${NAME`, as where the name starts and ends. A word that is not read so
/// (`--build-arg GITHUB_SHA=x`) is no variable being read.
pub(crate) fn variable_reads(text: &str) -> Vec<(usize, usize)> {
    let mut found = Vec::new();
    for (at, _) in text.match_indices('$') {
        let rest = &text[at + 1..];
        let start = at + 1 + usize::from(rest.starts_with('{'));
        let name: usize = text[start..]
            .chars()
            .take_while(|c| is_word_char(*c))
            .map(char::len_utf8)
            .sum();
        if name > 0 && !text[start..].starts_with(|c: char| c.is_ascii_digit()) {
            found.push((start, start + name));
        }
    }
    found
}

/// The variables `text` reads, each once.
pub(crate) fn variables(text: &str) -> BTreeSet<&str> {
    variable_reads(text)
        .into_iter()
        .map(|(start, end)| &text[start..end])
        .collect()
}

/// Whether `text` reads the variable `name`.
pub(crate) fn reads(text: &str, name: &str) -> bool {
    variables(text).contains(name)
}

/// `text` with every read of the variable `name` a read of `with`.
pub(crate) fn renamed(text: &str, name: &str, with: &str) -> String {
    let mut out = String::new();
    let mut from = 0;
    for (start, end) in variable_reads(text) {
        if &text[start..end] == name {
            out.push_str(&text[from..start]);
            out.push_str(with);
            from = end;
        }
    }
    out.push_str(&text[from..]);
    out
}

/// Whether what follows `before` in a script stands inside single quotes,
/// where a shell fills nothing in. Read as a shell reads a line, near
/// enough: quotes of both kinds, a backslash, a comment (a `#` that starts
/// a word, after a space or an operator such as `;`), and a here-document,
/// whose body is no quotes' at all.
pub(crate) fn in_single_quotes(before: &str) -> bool {
    #[derive(PartialEq)]
    enum In {
        Plain,
        Single,
        Double,
        Comment,
        /// The body of a here-document: its mark, and whether its lines
        /// may begin with tabs (`<<-`).
        Body(String, bool),
    }
    // What ends a word, beside a space: a `#` after one starts a comment.
    let operator = |c: char| matches!(c, ';' | '|' | '&' | '(' | ')' | '<' | '>');
    let mut state = In::Plain;
    let mut escaped = false;
    let mut word_starts = true;
    // The parentheses open in arithmetic (`$((…))`, `((…))`), where `<<` is
    // a shift.
    let mut arithmetic = 0usize;
    // The here-documents a line begins, whose bodies follow it in order.
    let mut bodies: Vec<(String, bool)> = Vec::new();
    let mut line = String::new();
    let mut chars = before.char_indices().peekable();
    // At the end of a line, the body of the first here-document it began.
    let next_body = |bodies: &mut Vec<(String, bool)>| match bodies.is_empty() {
        true => In::Plain,
        false => {
            let (mark, tabs) = bodies.remove(0);
            In::Body(mark, tabs)
        }
    };
    while let Some((at, c)) = chars.next() {
        match &state {
            In::Body(mark, tabs) => {
                if c == '\n' {
                    let read = match tabs {
                        true => line.trim_start_matches('\t'),
                        false => line.as_str(),
                    };
                    if read == mark {
                        state = next_body(&mut bodies);
                        word_starts = true;
                    }
                    line.clear();
                } else {
                    line.push(c);
                }
            }
            In::Comment => {
                if c == '\n' {
                    state = next_body(&mut bodies);
                    word_starts = true;
                }
            }
            In::Single => {
                if c == '\'' {
                    state = In::Plain;
                }
            }
            In::Double => match c {
                _ if escaped => escaped = false,
                '\\' => escaped = true,
                '"' => state = In::Plain,
                _ => {}
            },
            In::Plain => {
                match c {
                    _ if escaped => escaped = false,
                    '\\' => escaped = true,
                    '\'' => state = In::Single,
                    '"' => state = In::Double,
                    '#' if word_starts => state = In::Comment,
                    '\n' => state = next_body(&mut bodies),
                    '(' if arithmetic > 0 || before[at..].starts_with("((") => arithmetic += 1,
                    ')' if arithmetic > 0 => arithmetic -= 1,
                    '<' if arithmetic == 0
                        && before[at..].starts_with("<<")
                        && !before[at..].starts_with("<<<")
                        && !before[..at].ends_with('<') =>
                    {
                        // `<<MARK`, `<<-MARK`, `<< 'MARK'`: the mark is read,
                        // quotes and all, and is no quote of the line's. A
                        // here-string (`<<<`) has no body, and a shift outside
                        // arithmetic (`let x=1<<2`) has a number for its mark.
                        let rest = &before[at + 2..];
                        let tabs = rest.starts_with('-');
                        let word = rest.trim_start_matches('-').trim_start_matches([' ', '\t']);
                        let raw: String = word
                            .chars()
                            .take_while(|c| !c.is_whitespace() && !operator(*c))
                            .collect();
                        let mark = raw.replace(['\'', '"', '\\'], "");
                        if mark.starts_with(|c: char| c.is_alphabetic() || c == '_') {
                            let end = at + 2 + (rest.len() - word.len()) + raw.len();
                            while chars.next_if(|(i, _)| *i < end).is_some() {}
                            bodies.push((mark, tabs));
                        }
                    }
                    _ => {}
                }
                word_starts = c.is_whitespace() || operator(c);
            }
        }
    }
    state == In::Single
}

/// Whether a script has a here-document whose mark is quoted (`<<'EOF'`,
/// `<<"EOF"`, `<<\EOF`): the shell fills nothing in inside one.
pub(crate) fn has_quoted_heredoc(script: &str) -> bool {
    script.match_indices("<<").any(|(at, _)| {
        let mark = script[at + 2..]
            .trim_start_matches('-')
            .trim_start_matches([' ', '\t']);
        mark.starts_with(['\'', '"', '\\'])
    })
}

/// What of a script is bash's and no `sh`'s, where something is: the first
/// such thing, to say. Not all there is to find, and what it finds is what
/// most often breaks.
pub(crate) fn bashism(script: &str) -> Option<&'static str> {
    let has = |what: &str| script.contains(what);
    let words: Vec<&str> = script.split_whitespace().collect();
    // A command of bash's, where a command stands: a line's first word.
    let begins = |what: &str| {
        script.lines().any(|line| {
            line.trim_start()
                .strip_prefix(what)
                .is_some_and(|rest| rest.starts_with([' ', '\t']))
        })
    };
    // `name=(…)`: a word that gives a name a list.
    let array = words.iter().any(|word| {
        word.split_once("=(")
            .is_some_and(|(name, _)| is_shell_name(name.trim_end_matches('+')))
    });
    // `[ a == b ]`: a test with two equals signs, which `sh` does not take.
    let two_equals = script.lines().any(|line| {
        let line = line.trim_start();
        (line.contains("[ ") || line.starts_with("test "))
            && line.contains(" == ")
            && !line.contains("[[")
    });
    [
        (words.contains(&"[[") || has("[[ "), "`[[`"),
        (has("pipefail"), "`pipefail`"),
        (has("<<<"), "`<<<`"),
        (
            begins("declare") || begins("shopt") || begins("source"),
            "`declare`, `shopt` or `source`",
        ),
        (begins("function"), "`function`"),
        (has("&>"), "`&>`"),
        (array, "an array"),
        (two_equals, "`==` in a test"),
    ]
    .into_iter()
    .find_map(|(seen, what)| seen.then_some(what))
}

/// The step a job starts with while something of it is left to do.
pub(crate) fn failing(todo: &[String]) -> Value {
    failing_step("imported: to do", todo)
}

/// A step that says what is left to do, and fails. Its name is one a step
/// may have ([`step_name`]).
pub(crate) fn failing_step(name: &str, todo: &[String]) -> Value {
    let mut script =
        String::from("cat >&2 <<'DAGRON_IMPORT'\ndagron-import left this to do by hand:\n");
    for line in todo {
        // What is left to do is said in a script, where a pipeline reads
        // braces as its own: a workflow's expression is shown without its.
        let shown = line
            .replace("DAGRON_IMPORT", "DAGRON-IMPORT")
            .replace("${{", "$[[")
            .replace("{{", "[[")
            .replace("}}", "]]");
        script.push_str(&format!("  {shown}\n"));
    }
    script.push_str("DAGRON_IMPORT\nexit 1\n");
    let mut step = Mapping::new();
    step.insert(key("name"), Value::String(name.chars().take(100).collect()));
    step.insert(key("run"), Value::String(script));
    Value::Mapping(step)
}

/// A step's name as dagron-ci takes one: one line, and without the braces a
/// pipeline would read as a template of its own.
pub(crate) fn step_name(name: &str) -> String {
    let line: String = name
        .replace("{{", "")
        .replace("}}", "")
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    line.trim_end().to_string()
}

/// The report as an importer makes it, job by job: what is said of each
/// place, what is left of the pipeline as a whole, and what is said once.
#[derive(Default)]
pub(crate) struct Report {
    pub(crate) notes: Vec<Note>,
    /// What is left to do that is no one job's: every job says it.
    pub(crate) everywhere: Vec<String>,
    /// What is said once of the pipeline, where many jobs would say it:
    /// after what is said of one place.
    once: Vec<Note>,
}

impl Report {
    pub(crate) fn note(&mut self, at: &str, kind: Kind, text: impl Into<String>) {
        let note = Note {
            // One line: it is written in a comment, which a new line ends.
            at: said(at),
            kind,
            text: said(&text.into()),
        };
        // What is said of one place twice (a job and the next, of the
        // pipeline's own variable) is said once.
        if !self.notes.contains(&note) {
            self.notes.push(note);
        }
    }

    /// A thing left to do in a job: in the report, and in `job`, which the
    /// step the job starts with says. Said twice of one place, it is said
    /// once in both, as [`Report::note`] has it.
    pub(crate) fn todo(&mut self, job: &mut Vec<String>, at: &str, text: impl Into<String>) {
        let text = text.into();
        let line = format!("{}: {}", said(at), said(&text));
        if !job.contains(&line) {
            job.push(line);
        }
        self.note(at, Kind::Todo, text);
    }

    /// A thing left to do that is no one job's: in the report, and in the
    /// step every job starts with.
    pub(crate) fn todo_all(&mut self, at: &str, text: impl Into<String>) {
        let text = text.into();
        let line = format!("{}: {}", said(at), said(&text));
        if !self.everywhere.contains(&line) {
            self.everywhere.push(line);
        }
        self.note(at, Kind::Todo, text);
    }

    /// A word on the pipeline as a whole, said the first time it is asked.
    pub(crate) fn said_once(&mut self, at: &str, kind: Kind, text: impl Into<String>) {
        let text = said(&text.into());
        if !self.once.iter().any(|note| note.text == text) {
            self.once.push(Note {
                at: said(at),
                kind,
                text,
            });
        }
    }

    /// A job's task as it is written: its templates given their braces and
    /// every brace of the file's parted, and, while something of it is left
    /// to do, the step that says what first. `single` is the axis of a
    /// matrix whose items are plain values ([`settle`]); `may_fail` is the
    /// file's own word for a job that may fail (`continue-on-error`), said
    /// beside it where the file had another word than dagron's.
    pub(crate) fn finish(
        &mut self,
        at: &str,
        task: Mapping,
        mut todo: Vec<String>,
        stands: bool,
        single: Option<&str>,
        may_fail: Option<&str>,
    ) -> Value {
        let mut settled = Value::Mapping(task);
        let mut parted = Vec::new();
        settle(&mut settled, "", single, &mut parted);
        let Value::Mapping(mut task) = settled else {
            return settled;
        };
        for place in parted {
            todo.push(format!("{at}: {place}: {PARTED}"));
            self.note(&format!("{at}: {place}"), Kind::Todo, PARTED);
        }
        let mut left = self.everywhere.clone();
        left.append(&mut todo);
        // A job that may fail would pass with something left to do: with a
        // step that says so, or with a step that stands for one not imported.
        if (stands || !left.is_empty()) && task.shift_remove(key("allow_failure")).is_some() {
            let word = may_fail.map(|w| format!(" ({w})")).unwrap_or_default();
            let said = format!("the job may fail{word} once nothing of it is left to do: `allow_failure: true` is not written while something is, or the step that says so would fail nothing");
            left.push(format!("{at}: {said}"));
            self.note(at, Kind::Todo, said);
        }
        if !left.is_empty() {
            if let Some(Value::Sequence(steps)) = task.get_mut(key("steps")) {
                steps.insert(0, failing(&left));
            }
        }
        Value::Mapping(task)
    }

    /// Every note: of each place, then what was said once.
    pub(crate) fn into_notes(mut self) -> Vec<Note> {
        self.notes.append(&mut self.once);
        self.notes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_control_character_of_the_workflow_is_a_space_in_the_report() {
        assert_eq!(said("self\u{7}hosted\u{85}runner"), "self hosted runner");
        assert_eq!(one_line("a\u{1b}[31mred"), "a [31mred");
        // The importer's own marks are given their braces, not taken away.
        assert_eq!(
            said(&format!("{} is read", tpl("ci_sha"))),
            "{{ ci_sha }} is read"
        );
    }

    #[test]
    fn a_brace_just_before_a_template_is_parted_from_it() {
        let mut parted = Vec::new();
        let mut value = Value::String(format!("{{{}}}", tpl("item.os")));
        settle(&mut value, "env", None, &mut parted);
        assert_eq!(value, Value::String("{ {{ item.os }}}".into()));
        assert_eq!(parted, ["env"]);

        let mut parted = Vec::new();
        let mut value = Value::String(format!("{{{{{}", tpl("item.os")));
        settle(&mut value, "env", None, &mut parted);
        assert_eq!(value, Value::String("{ { {{ item.os }}".into()));
        assert_eq!(parted, ["env"]);

        // A template alone, or with a brace after it, is no brace parted.
        let mut parted = Vec::new();
        let mut value = Value::String(format!("{}}}", tpl("item.os")));
        settle(&mut value, "env", None, &mut parted);
        assert_eq!(value, Value::String("{{ item.os }}}".into()));
        assert!(parted.is_empty());
    }

    #[test]
    fn what_is_said_of_a_parted_brace_reads_the_same_in_the_step_that_fails() {
        let step = failing(&[format!("jobs.build: env[0].value: {PARTED}")]);
        let run = step["run"].as_str().unwrap();
        assert!(run.contains(PARTED), "{run}");
        assert!(!PARTED.contains("{{") && !PARTED.contains("}}"));
    }

    #[test]
    fn a_quote_in_a_comment_or_a_here_document_opens_no_quotes() {
        // As the shell reads them: what follows is not in single quotes.
        assert!(!in_single_quotes("echo hi;# it's a comment\necho \""));
        assert!(!in_single_quotes("make && # it's done\necho "));
        assert!(!in_single_quotes("cat <<EOF\nit's done\nEOF\necho \""));
        assert!(!in_single_quotes("cat <<-EOF\n\tit's done\n\tEOF\necho "));
        assert!(!in_single_quotes("cat <<'EOF' > f\nit's done\nEOF\necho "));
        assert!(!in_single_quotes("cat <<A <<B\n'\nA\n'\nB\necho "));
        assert!(!in_single_quotes("echo it\\'s ok\necho "));
        // What is in single quotes still is.
        assert!(in_single_quotes("echo '"));
        assert!(in_single_quotes("cat <<EOF\nx\nEOF\necho '"));
        assert!(in_single_quotes("echo $((1<<2)) '"));
        assert!(in_single_quotes("echo a#b '"));
        // A here-string and a shift begin no here-document, a shift of a
        // name's no more than a number's.
        assert!(in_single_quotes("cat <<< hello\necho '"));
        assert!(in_single_quotes("echo $(( 1<<n ))\necho '"));
        assert!(in_single_quotes("(( x = (1 << n) ))\necho '"));
        // Inside a here-document's body nothing is quoted yet.
        assert!(!in_single_quotes("cat <<EOF\n'"));
        assert!(!in_single_quotes("echo $((1<<n)); cat <<EOF\n'"));
    }

    #[test]
    fn names_that_come_to_one_once_cleaned_are_both_kept() {
        let mut value: Value = serde_yaml::from_str(
            "build: {run: first}\n\"build\\u0001\": {run: second}\n\"build\\u0002\": {run: third}\n\"te\\u0001st\": {run: fourth}\n",
        )
        .unwrap();
        clean(&mut value);
        let names: Vec<&str> = value
            .as_mapping()
            .unwrap()
            .keys()
            .map(|k| k.as_str().unwrap())
            .collect();
        assert_eq!(names, ["build", "build_", "build__2", "test"]);
        assert_eq!(value["build"]["run"], "first");
        assert_eq!(value["build_"]["run"], "second");

        // The marked name first, and the plain one after it: the plain one
        // keeps its name.
        let mut value: Value =
            serde_yaml::from_str("\"build\\u0001\": {run: first}\nbuild: {run: second}\n").unwrap();
        clean(&mut value);
        assert_eq!(value["build"]["run"], "second");
        assert_eq!(value["build_"]["run"], "first");
    }
}
