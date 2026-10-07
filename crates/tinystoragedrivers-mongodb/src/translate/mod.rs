//! [`Filter`] and [`Sort`] translated into MongoDB queries and pipelines.
//!
//! MongoDB's query language disagrees with [`Filter::matches`] in a few
//! places: a dotted path walks through arrays (`a.b` matches `{a: [{b: 1}]}`),
//! equality on an array field means "contains", and `{f: null}` matches a
//! missing field. A translation therefore comes with a verdict:
//!
//! - **exact**: the query matches precisely the documents
//!   [`Filter::matches`] accepts. Leaves get there with guards: every
//!   intermediate path segment, and the field itself, must not be an array.
//! - **superset**: the query matches every document the filter accepts and
//!   maybe more. The caller must evaluate the filter again in Rust on what
//!   comes back. Paths with numeric segments (array positions), values that
//!   are arrays or objects, and `null` range bounds translate this way.
//!
//! A negation of a superset is not a superset, so `not` and `ne` over an
//! inexact leaf translate to "everything" and leave the work to Rust.
//!
//! Sorting cannot use Mongo's native order, which ranks types differently
//! (`bool` after `object`) and orders arrays by their smallest element.
//! [`SortPlan`] computes a type rank per key in an aggregation stage that
//! matches [`value::compare`](tinystoragedrivers_core::value::compare) for
//! every scalar, and a companion check finds documents whose sort values are
//! arrays or objects, for which the caller sorts in Rust instead.

use mongodb::bson::{Bson, Document, doc};
use serde_json::Value;
use tinystoragedrivers_core::{Direction, Filter, ID_FIELD, Sort};

use crate::convert::exact_bson;
use crate::naming::{BODY, KEY};

/// A translated filter and whether it is exact.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Translated {
    /// The Mongo query. Never narrower than the filter.
    pub(crate) query: Document,
    /// Whether the query is exactly the filter.
    pub(crate) exact: bool,
}

impl Translated {
    fn exact(query: Document) -> Self {
        Self { query, exact: true }
    }

    fn superset(query: Document) -> Self {
        Self {
            query,
            exact: false,
        }
    }

    fn everything() -> Self {
        Self::exact(Document::new())
    }

    fn nothing() -> Self {
        // Every stored document has an `_id`.
        Self::exact(doc! {"_id": {"$exists": false}})
    }
}

/// Translate `filter`, which must already have passed [`Filter::validate`].
pub(crate) fn filter(filter: &Filter) -> Translated {
    match filter {
        Filter::All => Translated::everything(),
        Filter::Eq { field, value } => eq(field, value),
        Filter::Ne { field, value } => not(&eq(field, value)),
        Filter::In { field, values } => any(values.iter().map(|value| eq(field, value)).collect()),
        Filter::Range {
            field,
            gt,
            gte,
            lt,
            lte,
        } => range(
            field,
            &[("$gt", gt), ("$gte", gte), ("$lt", lt), ("$lte", lte)],
        ),
        Filter::Exists { field, exists } => {
            let present = exists_true(field);
            if *exists { present } else { not(&present) }
        }
        Filter::And { filters } => all(filters.iter().map(self::filter).collect()),
        Filter::Or { filters } => any(filters.iter().map(self::filter).collect()),
        Filter::Not { filter } => not(&self::filter(filter)),
        // A filter shape newer than this driver: let Rust decide.
        _ => Translated::superset(Document::new()),
    }
}

fn all(parts: Vec<Translated>) -> Translated {
    let exact = parts.iter().all(|part| part.exact);
    let clauses: Vec<Document> = parts
        .into_iter()
        .map(|part| part.query)
        .filter(|query| !query.is_empty())
        .collect();
    let query = match clauses.len() {
        0 => Document::new(),
        1 => clauses.into_iter().next().unwrap_or_default(),
        _ => doc! {"$and": clauses},
    };
    Translated { query, exact }
}

fn any(parts: Vec<Translated>) -> Translated {
    if parts.is_empty() {
        return Translated::nothing();
    }
    let exact = parts.iter().all(|part| part.exact);
    if parts.iter().any(|part| part.query.is_empty()) {
        return Translated {
            query: Document::new(),
            exact,
        };
    }
    let clauses: Vec<Document> = parts.into_iter().map(|part| part.query).collect();
    Translated {
        query: doc! {"$or": clauses},
        exact,
    }
}

fn not(inner: &Translated) -> Translated {
    if inner.exact {
        Translated::exact(doc! {"$nor": [inner.query.clone()]})
    } else {
        Translated::superset(Document::new())
    }
}

/// How a dotted path can be addressed in a Mongo query.
#[derive(Debug, PartialEq)]
enum Path<'a> {
    /// [`ID_FIELD`]: the `_key` field.
    Id,
    /// Object keys only; guards make Mongo's walk match [`value::lookup`].
    ///
    /// [`value::lookup`]: tinystoragedrivers_core::value::lookup
    Plain(Vec<&'a str>),
    /// A numeric segment past the first, which may index an array. Mongo also
    /// tries it as an object key on every array element, so it over-matches.
    Positional(&'a str),
    /// A segment starting with `$`, which Mongo reads as an operator.
    Opaque,
}

fn classify(path: &str) -> Path<'_> {
    if path == ID_FIELD {
        return Path::Id;
    }
    let segments: Vec<&str> = path.split('.').collect();
    if segments.iter().any(|segment| segment.starts_with('$')) {
        return Path::Opaque;
    }
    if segments
        .iter()
        .skip(1)
        .any(|segment| segment.bytes().all(|b| b.is_ascii_digit()))
    {
        return Path::Positional(path);
    }
    Path::Plain(segments)
}

/// The stored field a body path lives at.
pub(crate) fn body_field(path: &str) -> String {
    format!("{BODY}.{path}")
}

fn not_array(field: &str) -> Document {
    doc! {field: {"$not": {"$type": "array"}}}
}

/// `condition` on a plain path, with guards that stop Mongo from walking into
/// arrays where [`Filter::matches`] would not. With `scalar`, the field itself
/// must not be an array either, so equality is not read as "contains".
fn guarded(segments: &[&str], scalar: bool, condition: Document) -> Document {
    let depth = if scalar {
        segments.len()
    } else {
        segments.len() - 1
    };
    let mut clauses: Vec<Document> = (1..=depth)
        .map(|depth| not_array(&body_field(&segments[..depth].join("."))))
        .collect();
    if clauses.is_empty() {
        return condition;
    }
    clauses.push(condition);
    doc! {"$and": clauses}
}

/// How a leaf's condition behaves on a plain path.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Leaf {
    /// Exact once the field is also known not to be an array.
    Scalar,
    /// Exact on any value, arrays included (`$exists`).
    AnyValue,
    /// Presence only: a superset of the filter.
    Presence,
}

/// A leaf whose Mongo condition is built by `condition` from the stored field
/// name.
fn leaf(path: &str, kind: Leaf, condition: impl FnOnce(&str) -> Document) -> Translated {
    match classify(path) {
        Path::Plain(segments) => {
            let query = guarded(
                &segments,
                kind == Leaf::Scalar,
                condition(&body_field(path)),
            );
            Translated {
                query,
                exact: kind != Leaf::Presence,
            }
        }
        Path::Positional(path) => Translated::superset(condition(&body_field(path))),
        Path::Opaque | Path::Id => Translated::superset(Document::new()),
    }
}

fn present(field: &str) -> Document {
    doc! {field: {"$exists": true}}
}

fn exists_true(path: &str) -> Translated {
    if classify(path) == Path::Id {
        return Translated::everything();
    }
    leaf(path, Leaf::AnyValue, present)
}

fn eq(path: &str, value: &Value) -> Translated {
    if classify(path) == Path::Id {
        return match value {
            Value::String(id) => Translated::exact(doc! {KEY: id.as_str()}),
            _ => Translated::nothing(),
        };
    }
    match (value, exact_bson(value)) {
        (Value::Null, _) => leaf(path, Leaf::Scalar, |field| doc! {field: {"$type": "null"}}),
        (Value::Bool(_) | Value::Number(_) | Value::String(_), Some(bson)) => {
            leaf(path, Leaf::Scalar, |field| doc! {field: {"$eq": bson}})
        }
        // Arrays and objects compare differently in Mongo (element order of
        // object keys, "contains" for arrays), and an integer beyond i64
        // cannot be stated exactly: only presence is checked server-side.
        _ => leaf(path, Leaf::Presence, present),
    }
}

fn range(path: &str, bounds: &[(&str, &Option<Value>)]) -> Translated {
    let set: Vec<(&str, &Value)> = bounds
        .iter()
        .filter_map(|(op, bound)| bound.as_ref().map(|bound| (*op, bound)))
        .collect();
    if classify(path) == Path::Id {
        let mut condition = Document::new();
        for (op, bound) in set {
            let Value::String(id) = bound else {
                // A bound of another type than the string id never matches.
                return Translated::nothing();
            };
            condition.insert(op, id.as_str());
        }
        return Translated::exact(doc! {KEY: condition});
    }
    let mut condition = Document::new();
    for (op, bound) in &set {
        match (bound, exact_bson(bound)) {
            (Value::Bool(_) | Value::Number(_) | Value::String(_), Some(bson)) => {
                condition.insert(*op, bson);
            }
            // Null bounds also match missing fields in Mongo; array and object
            // bounds compare differently. Check presence and let Rust decide.
            _ => return leaf(path, Leaf::Presence, present),
        }
    }
    leaf(path, Leaf::Scalar, |field| doc! {field: condition})
}

/// A server-side sort that agrees with
/// [`sort_documents`](tinystoragedrivers_core::sort_documents) whenever every
/// sort value is a scalar.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SortPlan {
    /// `$addFields` stage computing a rank and a value per key.
    pub(crate) add_fields: Document,
    /// `$match` stage keeping documents with an array or object sort value.
    /// Any such document means the caller must sort in Rust.
    pub(crate) complex: Document,
    /// `$sort` stage: rank and value per key, then `_key` ascending.
    pub(crate) sort: Document,
    /// `$unset` stage removing the computed fields.
    pub(crate) unset: Vec<String>,
}

/// Plan a server-side sort, or `None` when a sort path cannot be evaluated by
/// an aggregation expression.
pub(crate) fn sort_plan(sorts: &[Sort]) -> Option<SortPlan> {
    let mut add_fields = Document::new();
    let mut complex = Vec::new();
    let mut sort = Document::new();
    let mut unset = Vec::new();
    for (index, key) in sorts.iter().enumerate() {
        let source = if key.field == ID_FIELD {
            format!("${KEY}")
        } else if key.field.split('.').any(|segment| segment.starts_with('$')) {
            return None;
        } else {
            format!("${}", body_field(&key.field))
        };
        let value_name = format!("_tsd_k{index}");
        let rank_name = format!("_tsd_r{index}");
        let value_ref = format!("${value_name}");
        add_fields.insert(&value_name, source.as_str());
        add_fields.insert(&rank_name, rank(&source));
        complex.push(doc! {"$in": [{"$type": value_ref}, ["array", "object"]]});
        let direction = match key.direction {
            Direction::Asc => 1,
            Direction::Desc => -1,
        };
        sort.insert(&rank_name, direction);
        sort.insert(&value_name, direction);
        unset.push(value_name);
        unset.push(rank_name);
    }
    sort.insert(KEY, 1);
    Some(SortPlan {
        add_fields,
        complex: doc! {"$expr": {"$or": complex}},
        sort,
        unset,
    })
}

/// The rank of a value's type in [`value::compare`]'s cross-type order:
/// null (and missing), bool, number, string, array, object.
///
/// [`value::compare`]: tinystoragedrivers_core::value::compare
fn rank(source: &str) -> Bson {
    let kind = doc! {"$type": source};
    Bson::Document(doc! {"$switch": {
        "branches": [
            {"case": {"$in": [kind.clone(), ["missing", "null"]]}, "then": 0},
            {"case": {"$eq": [kind.clone(), "bool"]}, "then": 1},
            {"case": {"$isNumber": source}, "then": 2},
            {"case": {"$eq": [kind.clone(), "string"]}, "then": 3},
            {"case": {"$eq": [kind, "array"]}, "then": 4},
        ],
        "default": 5,
    }})
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
