//! Translating the part of a [`Filter`] SQLite can evaluate exactly enough.
//!
//! The driver always re-checks every row with [`Filter::matches`], so the SQL
//! produced here only has to be a *superset* condition: it may let through rows
//! the filter rejects, never the reverse. That keeps it to clauses whose SQL
//! meaning is unambiguous: equality and membership on strings and booleans, and
//! on the document id. Numbers are left to the Rust check, because SQLite's
//! JSON functions turn very large integers into lossy floats.

use rusqlite::types::Value as SqlValue;
use serde_json::Value;
use tinystoragedrivers_core::{Filter, ID_FIELD};

use crate::sql::json_path;

/// SQL text and its bound parameters.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Clause {
    pub(crate) sql: String,
    pub(crate) params: Vec<SqlValue>,
}

/// The SQL form of a value an equality can be pushed down for.
fn pushable(value: &Value) -> Option<SqlValue> {
    match value {
        Value::String(text) => Some(SqlValue::Text(text.clone())),
        Value::Bool(flag) => Some(SqlValue::Integer(i64::from(*flag))),
        _ => None,
    }
}

/// The column expression for `field` and its parameters.
fn column(field: &str) -> (String, Vec<SqlValue>) {
    if field == ID_FIELD {
        ("id".to_owned(), Vec::new())
    } else {
        (
            "json_extract(doc, ?)".to_owned(),
            vec![SqlValue::Text(json_path(field))],
        )
    }
}

/// A superset condition for `filter`, or `None` when nothing can be pushed.
pub(crate) fn clause(filter: &Filter) -> Option<Clause> {
    match filter {
        Filter::Eq { field, value } => {
            let bound = pushable(value)?;
            if field == ID_FIELD && !value.is_string() {
                return None;
            }
            let (expr, mut params) = column(field);
            params.push(bound);
            Some(Clause {
                sql: format!("{expr} = ?"),
                params,
            })
        }
        Filter::In { field, values } => {
            let bound: Vec<SqlValue> = values.iter().map(pushable).collect::<Option<_>>()?;
            if bound.is_empty() || (field == ID_FIELD && !values.iter().all(Value::is_string)) {
                return None;
            }
            let (expr, mut params) = column(field);
            let marks = vec!["?"; bound.len()].join(", ");
            params.extend(bound);
            Some(Clause {
                sql: format!("{expr} IN ({marks})"),
                params,
            })
        }
        Filter::And { filters } => {
            let parts: Vec<Clause> = filters.iter().filter_map(clause).collect();
            if parts.is_empty() {
                return None;
            }
            let sql = parts
                .iter()
                .map(|part| format!("({})", part.sql))
                .collect::<Vec<_>>()
                .join(" AND ");
            Some(Clause {
                sql,
                params: parts.into_iter().flat_map(|part| part.params).collect(),
            })
        }
        _ => None,
    }
}

#[cfg(test)]
#[path = "pushdown_tests.rs"]
mod tests;
