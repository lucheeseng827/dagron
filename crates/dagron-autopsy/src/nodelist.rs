//! Slurm hostlist expansion — `node-[01-04,07]` ⇄ the four names it means.
//!
//! Everything downstream joins on a node name: a DCGM XID event matters to this
//! job only if it happened on a node this job held. `sacct` reports that node
//! set in Slurm's compressed hostlist syntax, and DCGM/NCCL/IB report plain
//! hostnames — so this expansion is the join key, and getting it wrong does not
//! produce a wrong answer, it produces *no* answer: an empty intersection reads
//! exactly like a clean cluster.
//!
//! Zero-padding is preserved because `nid00007` and `nid7` are different
//! hostnames on a real machine, and a set membership test does not forgive
//! that.

use std::collections::BTreeSet;

/// Expand a Slurm hostlist into concrete node names.
///
/// Handles the forms that actually appear in `sacct` output:
/// `node1`, `node[1-4]`, `node[01-04,07]`, `node[1-2],other[5-6]`,
/// `nid[00001-00003]`, and a bracket followed by a suffix
/// (`gpu[01-02].cluster.local`). Multiple bracket groups in one term expand as
/// a cartesian product (`r[1-2]n[1-2]` → four names), which is rare but legal.
///
/// Malformed input yields the term verbatim rather than an error: a hostlist we
/// cannot parse should still let the node be *matched literally* — degrading to
/// "this one node" is recoverable, and failing the whole autopsy because one
/// term was odd is not.
pub fn expand(list: &str) -> Vec<String> {
    let mut out = Vec::new();
    // The per-group cap in `parse_ranges` bounds one bracket, but groups
    // *multiply*: `r[1-2000]n[1-2000]` passes both group checks and would
    // otherwise produce four million names. That is the "a diagnostic tool
    // becomes the outage" case this file warns about — so the budget is spent
    // *as names are produced* and expansion stops the moment it runs out.
    // Checking the length afterwards would be no defence at all: by then the
    // four million strings are already allocated.
    //
    // One budget for the whole hostlist, not one per term. A per-term budget
    // bounds `a[1-9999999]` and then lets `a[...],b[...],c[...]` past it once
    // per comma, which is the same unbounded allocation wearing a disguise —
    // and `sacct`'s NodeList is exactly where a long comma-separated list is
    // normal. Once it is spent every later term degrades to a literal, which
    // is bounded by the length of the input.
    let mut remaining = MAX_NAMES;
    for term in split_top_level(list) {
        let before = out.len();
        if !expand_term(&term, &mut out, &mut remaining, 0) {
            out.truncate(before);
            out.push(term);
        }
    }
    out
}

/// Split on commas that are **outside** brackets. `node[1,2],other` is two
/// terms, not three — the naive `split(',')` is the classic bug here and it
/// silently produces the node names `node[1` and `2]`.
fn split_top_level(list: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut cur = String::new();
    for ch in list.trim().chars() {
        match ch {
            '[' => {
                depth += 1;
                cur.push(ch);
            }
            ']' => {
                depth = depth.saturating_sub(1);
                cur.push(ch);
            }
            ',' if depth == 0 => {
                if !cur.trim().is_empty() {
                    out.push(cur.trim().to_string());
                }
                cur.clear();
            }
            _ => cur.push(ch),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

/// Expand one term, spending `remaining` as names are appended.
///
/// Returns `false` the moment the budget is exhausted — the caller then
/// discards this term's partial output and keeps the literal. The budget is
/// threaded through the recursion rather than checked by the caller afterwards
/// because bracket groups multiply, and a check that runs after the fact only
/// observes an allocation that has already happened.
fn expand_term(term: &str, out: &mut Vec<String>, remaining: &mut usize, depth: usize) -> bool {
    // One place where a name is actually produced, so one place to spend from.
    fn push(out: &mut Vec<String>, remaining: &mut usize, name: String) -> bool {
        if *remaining == 0 {
            return false;
        }
        *remaining -= 1;
        out.push(name);
        true
    }

    // Each bracket group recurses one level deeper, and the output budget does
    // not bound that: `x[1]x[1]x[1]...` produces a single name however many
    // groups it has, so a term with tens of thousands of groups overflows the
    // stack while spending one unit of budget. Depth is its own limit. Real
    // hostlists nest two or three groups (`rack[1-4]node[1-32]`).
    //
    // Fail rather than push: `term` here is a *partially substituted*
    // intermediate (`x1x1x1…x[1]`), and storing that would put a hostname in
    // the node set that never existed — neither the real node nor the original
    // text, so the correlation join matches nothing and the reason why is
    // invisible. Returning false lets `expand` roll back and append the
    // original top-level term, which is the same literal fallback the budget
    // and the unbalanced-bracket paths already give.
    if depth >= MAX_DEPTH {
        return false;
    }

    let Some(open) = term.find('[') else {
        return push(out, remaining, term.to_string());
    };
    let Some(close_rel) = term[open..].find(']') else {
        // Unbalanced: take it literally rather than dropping the node.
        return push(out, remaining, term.to_string());
    };
    let close = open + close_rel;
    let prefix = &term[..open];
    let body = &term[open + 1..close];
    let suffix = &term[close + 1..];

    let mut any = false;
    for n in parse_ranges(body) {
        any = true;
        // Recurse so a second bracket group in the suffix expands too.
        if !expand_term(&format!("{prefix}{n}{suffix}"), out, remaining, depth + 1) {
            return false;
        }
    }
    if !any {
        return push(out, remaining, term.to_string());
    }
    true
}

/// `01-04,07` → `["01","02","03","04","07"]`, zero-padding preserved.
fn parse_ranges(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    for part in body.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        match part.split_once('-') {
            Some((lo, hi)) => {
                let (lo, hi) = (lo.trim(), hi.trim());
                let (Ok(a), Ok(b)) = (lo.parse::<u64>(), hi.parse::<u64>()) else {
                    out.push(part.to_string());
                    continue;
                };
                if b < a {
                    out.push(part.to_string());
                    continue;
                }
                // A range that would expand to a million names is a
                // malformed hostlist, not a job — and materializing it is how
                // a diagnostic tool becomes the outage. Cap and take the term
                // literally instead.
                // The count is `b - a + 1`, so "more than MAX_RANGE names" is
                // `b - a >= MAX_RANGE`. Written that way rather than as
                // `b - a + 1 > MAX_RANGE`, which overflows for a maximal range
                // (`n[0-18446744073709551615]`) — panicking in a debug build,
                // and in a release build wrapping to 0, sailing past the cap
                // and then iterating the whole u64 space. A guard that a
                // hostile hostlist can wrap is worse than no guard.
                if b - a >= MAX_RANGE {
                    out.push(part.to_string());
                    continue;
                }
                // Width from the *low* bound, which is how Slurm writes them.
                let width = lo.len();
                for n in a..=b {
                    out.push(format!("{n:0width$}"));
                }
            }
            None => out.push(part.to_string()),
        }
    }
    out
}

/// A single bracket range yielding more than this many names is treated as
/// unparseable rather than expanded. The largest machines in the world are ~10⁵
/// nodes; 10⁶ names is a parse error wearing a costume.
const MAX_RANGE: u64 = 1_000_000;

/// The same ceiling, applied to one whole hostlist's expansion rather than to
/// one bracket group — see [`expand`]. Separate constant, same number, because
/// they bound different things and a future change to one is not automatically
/// right for the other.
const MAX_NAMES: usize = 1_000_000;

/// How many nested bracket groups one term may expand through. Bounds recursion
/// depth in [`expand_term`], which [`MAX_NAMES`] does not: a term can recurse
/// once per group while producing a single name. Real Slurm hostlists nest two
/// or three (`rack[1-4]node[1-32]`).
const MAX_DEPTH: usize = 32;

/// Normalize a hostname for set membership: strip a `:port`, strip the DNS
/// suffix, lowercase.
///
/// `sacct` reports the short name (`node-47`) while DCGM and NCCL logs usually
/// carry whatever `hostname` returned, which on many clusters is the FQDN
/// (`node-47.hpc.internal`). Prometheus-shaped exports go further and use the
/// scrape target as the identity — `dcgm-exporter`'s `instance` label is
/// `node-47:9400`. Comparing any of those raw makes every join miss, and a
/// missed join does not look like an error: it reports a clean cluster.
///
/// The port is only stripped when what follows the colon is **all digits**. A
/// colon in a hostname position that is not a port is left alone rather than
/// truncated on a guess.
pub fn normalize(host: &str) -> String {
    let h = host.trim().trim_end_matches('.');
    let h = match h.rsplit_once(':') {
        Some((head, port)) if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) => head,
        _ => h,
    };
    let short = h.split('.').next().unwrap_or(h);
    short.to_ascii_lowercase()
}

/// Expand and normalize in one step — the form the correlator compares against.
pub fn expand_normalized(list: &str) -> BTreeSet<String> {
    expand(list).iter().map(|n| normalize(n)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_the_forms_sacct_actually_emits() {
        assert_eq!(expand("node1"), vec!["node1"]);
        assert_eq!(expand("node[1-4]"), vec!["node1", "node2", "node3", "node4"]);
        assert_eq!(
            expand("node-[01-04,07]"),
            vec!["node-01", "node-02", "node-03", "node-04", "node-07"]
        );
        assert_eq!(
            expand("gpu[01-02].cluster.local"),
            vec!["gpu01.cluster.local", "gpu02.cluster.local"]
        );
    }

    #[test]
    fn commas_inside_brackets_do_not_split_the_term() {
        // The classic bug: split(',') turns this into `node[1` + `2]` + `other`,
        // and every downstream join misses.
        assert_eq!(expand("node[1,2],other"), vec!["node1", "node2", "other"]);
        assert_eq!(
            expand("a[1-2],b[3-4]"),
            vec!["a1", "a2", "b3", "b4"]
        );
    }

    #[test]
    fn zero_padding_is_preserved_because_hostnames_are_not_integers() {
        // nid00007 and nid7 are different machines. A set test does not forgive
        // dropping the padding.
        assert_eq!(
            expand("nid[00001-00003]"),
            vec!["nid00001", "nid00002", "nid00003"]
        );
        assert_eq!(expand("n[8-11]"), vec!["n8", "n9", "n10", "n11"]);
    }

    #[test]
    fn multiple_bracket_groups_expand_as_a_product() {
        assert_eq!(
            expand("r[1-2]n[1-2]"),
            vec!["r1n1", "r1n2", "r2n1", "r2n2"]
        );
    }

    #[test]
    fn malformed_terms_degrade_to_a_literal_instead_of_vanishing() {
        // Losing a node silently is worse than matching one odd string: an
        // empty intersection reads exactly like a healthy cluster.
        assert_eq!(expand("node[1-"), vec!["node[1-"]);
        assert_eq!(expand("node[4-1]"), vec!["node4-1"]);
        assert_eq!(expand("node[abc]"), vec!["nodeabc"]);
        assert!(expand("").is_empty());
        // `None` is what sacct writes for a job that never got an allocation.
        assert_eq!(expand("None assigned"), vec!["None assigned"]);
    }

    #[test]
    fn an_absurd_range_is_not_materialized() {
        let v = expand("n[1-99999999]");
        assert_eq!(v, vec!["n1-99999999"], "capped rather than expanded");
    }

    #[test]
    fn multiplied_bracket_groups_cannot_blow_the_term_budget() {
        // Each group is individually under MAX_RANGE, but the product is four
        // million names. The per-group cap does not see that; the term budget
        // does, and falls back to the literal rather than materializing it.
        let v = expand("r[1-2000]n[1-2000]");
        assert_eq!(v, vec!["r[1-2000]n[1-2000]"], "capped as a whole term");
        // The ordinary product still expands.
        assert_eq!(expand("r[1-2]n[1-2]"), vec!["r1n1", "r1n2", "r2n1", "r2n2"]);
    }

    #[test]
    fn the_range_cap_counts_names_not_the_gap_between_the_bounds() {
        // `b - a` is one less than the count; a naive boundary lets a range of
        // MAX_RANGE + 1 names through, which is not what the constant says.
        assert_eq!(expand("n[1-1000001]"), vec!["n1-1000001"]);
    }

    #[test]
    fn a_maximal_range_is_rejected_without_overflowing_the_check() {
        // Spelling the cap as `b - a + 1 > MAX_RANGE` overflows here: it panics
        // in a debug build, and in release wraps to 0, passes the check, and
        // then iterates the entire u64 space. The guard has to survive the
        // input it exists to reject.
        assert_eq!(
            expand("n[0-18446744073709551615]"),
            vec!["n0-18446744073709551615"]
        );
        // The boundary either side of the cap still behaves.
        assert_eq!(expand("n[1-1000000]").len(), 1_000_000, "exactly MAX_RANGE expands");
    }

    #[test]
    fn the_term_budget_is_spent_as_names_are_made_not_checked_afterwards() {
        // Four groups of 60 is 12.96 million names — well past the budget, and
        // the point is that they are never allocated. If the cap were applied
        // after expansion this test would still pass while having briefly held
        // all of them in memory; what makes it meaningful is that
        // `expand_term` stops at MAX_NAMES pushes.
        let v = expand("a[1-60]b[1-60]c[1-60]d[1-60]");
        assert_eq!(v, vec!["a[1-60]b[1-60]c[1-60]d[1-60]"], "falls back to the literal");
        // A product that fits is still expanded exactly.
        assert_eq!(expand("a[1-2]b[1-2]").len(), 4);
    }

    #[test]
    fn the_budget_is_for_the_whole_hostlist_not_reset_at_every_comma() {
        // The hole a per-term budget leaves: each term is individually under
        // the cap, so a per-term budget waves all of them through and the
        // total is unbounded — 20 x 100k here, and nothing stops 20 000 x 100k.
        // `sacct`'s NodeList is a comma-separated list by construction, so this
        // is the shape the guard actually meets, not a contrived one.
        let list = (0..20)
            .map(|i| format!("r{i}[1-100000]"))
            .collect::<Vec<_>>()
            .join(",");
        let v = expand(&list);
        // The bound is the budget plus at most one literal per term: a term
        // that overruns is replaced by its own text, which is deliberately not
        // charged to the budget (it is bounded by the length of the input, and
        // dropping the node entirely would lose a name the join still needs).
        // Unbounded, this input is 2 000 000 names.
        assert!(
            v.len() <= MAX_NAMES + 20,
            "the whole hostlist stays inside one budget, got {}",
            v.len()
        );
        // Degradation is per term and never drops a node: the terms that fit
        // expanded, and every term that did not is still present as its own
        // literal, so the node-set join can still match it by name.
        assert!(v.contains(&"r01".to_string()), "an early term expanded");
        assert!(
            v.contains(&"r19[1-100000]".to_string()),
            "a term past the budget survives as a literal rather than vanishing"
        );
    }

    #[test]
    fn nesting_depth_is_bounded_so_a_pathological_term_cannot_blow_the_stack() {
        // One name, MAX_DEPTH + 40 recursions if depth is unbounded: the output
        // budget cannot see this coming because nothing is ever produced until
        // the last level. Deep enough to be a bug, small enough that this test
        // does not itself overflow while proving the guard holds.
        let deep = "x[1]".repeat(MAX_DEPTH + 40);
        let v = expand(&deep);
        // The *original* term, not a half-substituted `x1x1x1…x[1]` hybrid:
        // that would be a hostname that never existed on any cluster, and it
        // would join against nothing while looking like a real answer.
        assert_eq!(v, vec![deep.clone()], "degrades to the original literal");
        // Below the cap it expands normally rather than degrading early.
        let shallow = "x[1]".repeat(3);
        assert_eq!(expand(&shallow), vec!["x1x1x1"]);
    }

    #[test]
    fn a_prometheus_instance_label_still_joins() {
        // dcgm-exporter identifies a scrape target as `node-47:9400`. Without
        // stripping the port the node key never matches the sacct node set and
        // the autopsy reports a clean cluster — the silent failure this whole
        // module exists to avoid.
        assert_eq!(normalize("node-47:9400"), "node-47");
        assert_eq!(normalize("node-47.hpc.internal:9400"), "node-47");
        // Only a numeric suffix is a port. A colon that is not one is left
        // alone rather than truncated on a guess.
        assert_eq!(normalize("node-47:gpu"), "node-47:gpu");
        assert_eq!(normalize("node-47:"), "node-47:");
    }

    #[test]
    fn normalization_bridges_short_names_and_fqdns() {
        // sacct says node-47; the NCCL log says node-47.hpc.internal. Without
        // this every join misses and the tool reports a clean cluster.
        assert_eq!(normalize("node-47.hpc.internal"), "node-47");
        assert_eq!(normalize("NODE-47"), "node-47");
        assert_eq!(normalize(" node-47. "), "node-47");
        let set = expand_normalized("node-[46-47].hpc.internal");
        assert!(set.contains("node-47"));
        assert!(set.contains("node-46"));
        assert_eq!(set.len(), 2);
    }
}
