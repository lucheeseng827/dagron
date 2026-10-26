//! The globs `on:` filters take: branch names, tag names and file paths;
//! and those `hashFiles` is given, which name a job's files.
//!
//! - `*` matches any characters except `/`;
//! - `**` matches any characters, `/` included; `a/**/b` also matches `a/b`;
//! - `?` matches one character except `/`;
//! - `[abc]`, `[a-z]` match one character of the set, `[!abc]` one outside
//!   it, never `/`;
//! - every other character matches itself. `\` escapes the next character.
//!
//! In a pattern list, a pattern starting `!` excludes what it matches, and the
//! last pattern that matches a name decides, as in GitHub's filters. GitHub's
//! own `?` and `+` mean something else; its front-end translates them.

/// One compiled glob.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Glob {
    source: String,
    tokens: Vec<Token>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
    Char(char),
    /// `?`
    One,
    /// `*`
    Star,
    /// `**`, and with `slash`, `**/`: zero or more whole segments.
    Globstar {
        slash: bool,
    },
    Class {
        negated: bool,
        ranges: Vec<(char, char)>,
    },
}

impl Glob {
    /// Compile `pattern`, or say what is wrong with it.
    pub fn new(pattern: &str) -> Result<Glob, String> {
        if pattern.is_empty() {
            return Err("a pattern is empty".into());
        }
        if pattern.chars().any(char::is_control) {
            return Err(format!("pattern {pattern:?} holds a control character"));
        }
        let chars: Vec<char> = pattern.chars().collect();
        let mut tokens = Vec::new();
        let mut i = 0;
        while i < chars.len() {
            match chars[i] {
                '\\' => {
                    let Some(&c) = chars.get(i + 1) else {
                        return Err(format!("pattern {pattern:?} ends in a lone \\"));
                    };
                    tokens.push(Token::Char(c));
                    i += 2;
                }
                '*' if chars.get(i + 1) == Some(&'*') => {
                    if chars.get(i + 2) == Some(&'*') {
                        return Err(format!("pattern {pattern:?} has three *s in a row"));
                    }
                    let slash = chars.get(i + 2) == Some(&'/');
                    tokens.push(Token::Globstar { slash });
                    i += if slash { 3 } else { 2 };
                }
                '*' => {
                    tokens.push(Token::Star);
                    i += 1;
                }
                '?' => {
                    tokens.push(Token::One);
                    i += 1;
                }
                '[' => {
                    let (token, next) = class(&chars, i)
                        .ok_or_else(|| format!("pattern {pattern:?} has an unclosed ["))?;
                    tokens.push(token);
                    i = next;
                }
                c => {
                    tokens.push(Token::Char(c));
                    i += 1;
                }
            }
        }
        Ok(Glob {
            source: pattern.to_string(),
            tokens,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.source
    }

    pub fn matches(&self, text: &str) -> bool {
        let text: Vec<char> = text.chars().collect();
        // memo[t][p]: whether tokens[p..] match text[t..], once known.
        let mut memo = vec![vec![None; self.tokens.len() + 1]; text.len() + 1];
        matches_at(&self.tokens, &text, 0, 0, &mut memo)
    }
}

/// `[...]` starting at `chars[start]`: the token and the index after `]`.
fn class(chars: &[char], start: usize) -> Option<(Token, usize)> {
    let mut i = start + 1;
    let negated = matches!(chars.get(i), Some('!' | '^'));
    if negated {
        i += 1;
    }
    let mut ranges = Vec::new();
    let mut first = true;
    loop {
        let c = *chars.get(i)?;
        if c == ']' && !first {
            return Some((Token::Class { negated, ranges }, i + 1));
        }
        first = false;
        let c = if c == '\\' {
            i += 1;
            *chars.get(i)?
        } else {
            c
        };
        if chars.get(i + 1) == Some(&'-') && chars.get(i + 2).is_some_and(|&e| e != ']') {
            let end = chars[i + 2];
            if end < c {
                return None;
            }
            ranges.push((c, end));
            i += 3;
        } else {
            ranges.push((c, c));
            i += 1;
        }
    }
}

fn matches_at(
    tokens: &[Token],
    text: &[char],
    t: usize,
    p: usize,
    memo: &mut Vec<Vec<Option<bool>>>,
) -> bool {
    if let Some(known) = memo[t][p] {
        return known;
    }
    let result = match tokens.get(p) {
        None => t == text.len(),
        Some(Token::Char(c)) => {
            text.get(t) == Some(c) && matches_at(tokens, text, t + 1, p + 1, memo)
        }
        Some(Token::One) => {
            text.get(t).is_some_and(|&c| c != '/') && matches_at(tokens, text, t + 1, p + 1, memo)
        }
        Some(Token::Star) => {
            // Zero characters, or one more that is not a `/`.
            matches_at(tokens, text, t, p + 1, memo)
                || (text.get(t).is_some_and(|&c| c != '/')
                    && matches_at(tokens, text, t + 1, p, memo))
        }
        Some(Token::Globstar { slash: false }) => {
            matches_at(tokens, text, t, p + 1, memo)
                || (t < text.len() && matches_at(tokens, text, t + 1, p, memo))
        }
        Some(Token::Globstar { slash: true }) => {
            // `**/` is zero segments, or anything that ends in a `/`.
            matches_at(tokens, text, t, p + 1, memo)
                || (t..text.len())
                    .any(|end| text[end] == '/' && matches_at(tokens, text, end + 1, p + 1, memo))
        }
        Some(Token::Class { negated, ranges }) => text.get(t).is_some_and(|&c| {
            c != '/'
                && ranges.iter().any(|&(lo, hi)| (lo..=hi).contains(&c)) != *negated
                && matches_at(tokens, text, t + 1, p + 1, memo)
        }),
    };
    memo[t][p] = Some(result);
    result
}

/// An ordered pattern list, `!` excluding: the last pattern that matches a
/// name decides, and a name nothing matches is out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Patterns(Vec<(bool, Glob)>);

impl Patterns {
    /// Read a list: `include` patterns, with `!` for exclusions. At least one
    /// must include, or nothing could ever match.
    pub fn include(list: &[String]) -> Result<Patterns, String> {
        let mut patterns = Vec::with_capacity(list.len());
        for raw in list {
            let (included, pattern) = match raw.strip_prefix('!') {
                Some(rest) => (false, rest),
                None => (true, raw.as_str()),
            };
            patterns.push((included, Glob::new(pattern)?));
        }
        if !patterns.iter().any(|(included, _)| *included) {
            return Err("every pattern starts with !, so nothing could match".into());
        }
        Ok(Patterns(patterns))
    }

    /// An `_ignore` list: everything but what these match. `!` is not taken.
    pub fn ignore(list: &[String]) -> Result<Patterns, String> {
        let mut patterns = vec![(true, Glob::new("**").expect("** compiles"))];
        for raw in list {
            if raw.starts_with('!') {
                return Err(format!(
                    "{raw:?}: an _ignore list takes no ! patterns; use the list without _ignore"
                ));
            }
            patterns.push((false, Glob::new(raw)?));
        }
        Ok(Patterns(patterns))
    }

    pub fn matches(&self, name: &str) -> bool {
        self.0
            .iter()
            .rev()
            .find(|(_, glob)| glob.matches(name))
            .is_some_and(|(included, _)| *included)
    }

    /// Whether any of `names` matches: a path filter's question about a diff.
    pub fn any(&self, names: &[&str]) -> bool {
        names.iter().any(|n| self.matches(n))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(pattern: &str, text: &str) -> bool {
        Glob::new(pattern).unwrap().matches(text)
    }

    #[test]
    fn a_star_stays_inside_a_segment() {
        assert!(m("release/*", "release/1.2"));
        assert!(!m("release/*", "release/1.2/hotfix"));
        assert!(m("*.md", "README.md"));
        assert!(!m("*.md", "docs/README.md"));
        assert!(m("v*", "v1.2.3"));
        assert!(m("*", ""));
    }

    #[test]
    fn a_globstar_crosses_segments() {
        assert!(m("**", "a/b/c"));
        assert!(m("crates/**", "crates/dagron-ci/src/lib.rs"));
        assert!(m("**/*.rs", "lib.rs"), "zero segments");
        assert!(m("**/*.rs", "crates/x/src/lib.rs"));
        assert!(m("docs/**/*.md", "docs/a.md"));
        assert!(m("docs/**/*.md", "docs/x/y/a.md"));
        assert!(!m("docs/**/*.md", "src/a.md"));
        assert!(m("a**b", "a/x/b"));
    }

    #[test]
    fn one_and_classes_never_match_a_slash() {
        assert!(m("v?", "v1"));
        assert!(!m("v?", "v12"));
        assert!(!m("a?b", "a/b"));
        assert!(m("[a-c]x", "bx"));
        assert!(!m("[a-c]x", "dx"));
        assert!(m("[!a-c]x", "dx"));
        assert!(!m("[!a-c]x", "/x"));
        assert!(m("[]]", "]"), "a ] first is a member");
        assert!(m(r"a\*b", "a*b"));
        assert!(!m(r"a\*b", "axb"));
    }

    #[test]
    fn bad_patterns_are_refused() {
        for bad in ["", "[abc", "a\\", "***", "[z-a]", "a\u{7}"] {
            assert!(Glob::new(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn the_last_matching_pattern_decides() {
        let list = |l: &[&str]| l.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let p = Patterns::include(&list(&[
            "release/**",
            "!release/old/**",
            "release/old/keep",
        ]))
        .unwrap();
        assert!(p.matches("release/1.0"));
        assert!(!p.matches("release/old/x"));
        assert!(p.matches("release/old/keep"));
        assert!(!p.matches("main"));
        assert!(Patterns::include(&list(&["!main"])).is_err());

        let ignore = Patterns::ignore(&list(&["docs/**", "*.md"])).unwrap();
        assert!(ignore.matches("src/lib.rs"));
        assert!(!ignore.matches("docs/a/b.txt"));
        assert!(!ignore.matches("README.md"));
        assert!(Patterns::ignore(&list(&["!x"])).is_err());
        assert!(ignore.any(&["README.md", "src/main.rs"]));
        assert!(!ignore.any(&["README.md"]));
    }

    #[test]
    fn matching_is_linear_enough_for_hostile_patterns() {
        let pattern = "*a*a*a*a*a*a*a*a*a*a*b";
        let text = "a".repeat(200);
        assert!(!m(pattern, &text));
    }
}
