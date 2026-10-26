//! A job's matrix, expanded as GitHub expands one: every combination of
//! its axes, less those `exclude` takes out, with what `include` adds.
//!
//! It is here, beside the expression language, because a matrix is expanded
//! in two places by one rule: where a run is made, for a matrix that is
//! written out, and as a run goes, for one that an earlier job computes.
//! Everything is in the order it was written: that order is the order a
//! matrix's jobs are made in, and named in.

use std::collections::BTreeSet;

use serde_json::Value;

/// One combination of a matrix, or one entry of its `include` or `exclude`:
/// its keys and values, in the order they are written.
pub type Row = Vec<(String, Value)>;

/// A matrix: its axes, each with its values, and the rows of its `include`
/// and its `exclude`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Matrix {
    pub axes: Vec<(String, Vec<Value>)>,
    pub include: Vec<Row>,
    pub exclude: Vec<Row>,
}

/// The combinations a matrix comes to. `Err` is how many its axes alone
/// made before more than `most` were counted, at which it stops: a matrix of
/// a few long axes is millions of rows.
pub fn rows(matrix: &Matrix, most: usize) -> Result<Vec<Row>, usize> {
    let mut rows: Vec<Row> = vec![Vec::new()];
    for (axis, values) in &matrix.axes {
        rows = rows
            .into_iter()
            .flat_map(|row| {
                values.iter().map(move |value| {
                    let mut row = row.clone();
                    row.push((axis.clone(), value.clone()));
                    row
                })
            })
            .collect();
        if rows.len() > most {
            return Err(rows.len());
        }
    }
    if matrix.axes.is_empty() {
        rows.clear();
    }
    let matches = |row: &Row, wanted: &Row| {
        wanted
            .iter()
            .all(|(k, v)| row.iter().any(|(rk, rv)| rk == k && rv == v))
    };
    rows.retain(|row| !matrix.exclude.iter().any(|wanted| matches(row, wanted)));
    // An `include` adds its keys to each combination it does not disagree
    // with on an axis, and is a combination of its own where it fits none.
    let axes: BTreeSet<&str> = matrix.axes.iter().map(|(a, _)| a.as_str()).collect();
    let original = rows.clone();
    for extra in &matrix.include {
        let mut fitted = false;
        for (row, was) in rows.iter_mut().zip(&original) {
            let agrees = extra
                .iter()
                .filter(|(k, _)| axes.contains(k.as_str()))
                .all(|(k, v)| was.iter().any(|(rk, rv)| rk == k && rv == v));
            if agrees && !was.is_empty() {
                for (k, v) in extra {
                    if !axes.contains(k.as_str()) {
                        row.retain(|(rk, _)| rk != k);
                        row.push((k.clone(), v.clone()));
                    }
                }
                fitted = true;
            }
        }
        if !fitted {
            rows.push(extra.clone());
        }
    }
    if rows.len() > most {
        return Err(rows.len());
    }
    Ok(rows)
}

/// Each row with a label for its job's name: the values a person would tell
/// the jobs apart by, and its place among them where two would read the
/// same, or none would read as anything. A label is letters, digits, `_`
/// and `-`, and no two rows have one: a place that reads as an earlier
/// row's own value is told apart from it in turn.
pub fn labelled(rows: Vec<Row>) -> Vec<(String, Row)> {
    let mut seen = BTreeSet::new();
    rows.into_iter()
        .enumerate()
        .map(|(index, row)| {
            let mut label: String = row
                .iter()
                .filter(|(_, v)| !v.is_object() && !v.is_array())
                .map(|(_, v)| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .collect::<Vec<_>>()
                .join("-")
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || c == '_' {
                        c
                    } else {
                        '-'
                    }
                })
                .take(40)
                .collect();
            if label.trim_matches('-').is_empty() || seen.contains(&label) {
                label = index.to_string();
                let mut again = 1;
                while seen.contains(&label) {
                    label = format!("{index}-{again}");
                    again += 1;
                }
            }
            seen.insert(label.clone());
            (label, row)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn row(pairs: &[(&str, Value)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn a_matrix_is_its_combinations_less_what_is_excluded_with_what_is_included() {
        let matrix = Matrix {
            axes: vec![
                ("os".into(), vec![json!("linux"), json!("mac")]),
                ("node".into(), vec![json!(18), json!(20)]),
            ],
            exclude: vec![row(&[("os", json!("mac")), ("node", json!(18))])],
            include: vec![
                // Fits a combination: adds its key to it.
                row(&[("os", json!("linux")), ("experimental", json!(true))]),
                // Fits none: a combination of its own.
                row(&[("os", json!("windows")), ("node", json!(22))]),
            ],
        };
        let rows = rows(&matrix, 256).unwrap();
        let said: Vec<String> = rows
            .iter()
            .map(|r| {
                serde_json::to_string(&r.iter().cloned().collect::<serde_json::Map<_, _>>())
                    .unwrap()
            })
            .collect();
        assert_eq!(
            said,
            [
                r#"{"experimental":true,"node":18,"os":"linux"}"#,
                r#"{"experimental":true,"node":20,"os":"linux"}"#,
                r#"{"node":20,"os":"mac"}"#,
                r#"{"node":22,"os":"windows"}"#,
            ]
        );
        let labels: Vec<String> = labelled(rows).into_iter().map(|(l, _)| l).collect();
        assert_eq!(
            labels,
            ["linux-18-true", "linux-20-true", "mac-20", "windows-22"]
        );
    }

    #[test]
    fn rows_that_would_read_alike_are_told_apart_by_their_place() {
        let rows = vec![
            row(&[("path", json!("a/b"))]),
            row(&[("path", json!("a-b"))]),
            row(&[("config", json!({ "x": 1 }))]),
            row(&[("name", json!("   "))]),
        ];
        let labels: Vec<String> = labelled(rows).into_iter().map(|(l, _)| l).collect();
        assert_eq!(labels, ["a-b", "1", "2", "3"]);
        // A row's place may be what an earlier row reads as, and what a
        // later one does: each is still one label, and no two are the same.
        let rows = ["2", "a", "a", "3", "3", "2-1", "2"]
            .iter()
            .map(|v| row(&[("v", json!(v))]))
            .collect();
        let labels: Vec<String> = labelled(rows).into_iter().map(|(l, _)| l).collect();
        assert_eq!(labels, ["2", "a", "2-1", "3", "4", "5", "6"]);
        let distinct: BTreeSet<&String> = labels.iter().collect();
        assert_eq!(distinct.len(), labels.len());
    }

    #[test]
    fn a_matrix_past_what_is_asked_for_stops_and_says_how_far_it_got() {
        let long: Vec<Value> = (0..100).map(|n| json!(n)).collect();
        let matrix = Matrix {
            axes: vec![
                ("a".into(), long.clone()),
                ("b".into(), long.clone()),
                ("c".into(), long),
            ],
            ..Matrix::default()
        };
        assert_eq!(rows(&matrix, 256), Err(10_000));
        // With no axis there is no combination but what `include` is.
        let only = Matrix {
            include: vec![row(&[("x", json!(1))])],
            ..Matrix::default()
        };
        assert_eq!(rows(&only, 256).unwrap().len(), 1);
        assert!(rows(&Matrix::default(), 256).unwrap().is_empty());
    }
}
