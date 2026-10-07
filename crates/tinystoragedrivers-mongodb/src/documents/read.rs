//! Reads: paged queries, counts, the documents a bulk removal targets, and
//! full-text search.
//!
//! Every read evaluates the caller's filter AND the expiry rule. When the
//! translation is exact and every sort value is a scalar, MongoDB filters,
//! sorts and pages (`$skip`/`$limit`, offset paging like the memory driver).
//! Otherwise the server returns a superset, which is filtered, sorted and
//! paged in Rust with the reference semantics, still scoped server-side.

use mongodb::bson::{Document, doc};
use mongodb::options::FindOptions;
use serde_json::Value;
use tinystoragedrivers_core::{
    CollectionSpec, Filter, Result, SearchHit, Sort, StorageError, Version, Versioned,
    sort_documents, value,
};

use super::MongoDocuments;
use super::stored::{Stored, live_filter, visible};
use crate::errors;
use crate::naming::{BODY, KEY};
use crate::scoped::ScopedCollection;
use crate::translate::{self, Translated};

/// One page of matches and whether more follow.
pub(super) type Matches = (Vec<Versioned<Value>>, bool);

fn decode_all(docs: &[Document]) -> Result<Vec<Stored>> {
    docs.iter().map(Stored::decode).collect()
}

fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

impl MongoDocuments {
    /// `filter` AND the expiry rule, and its translation.
    fn effective(&self, spec: &CollectionSpec, filter: &Filter) -> (Filter, Translated) {
        let effective = filter.clone().and(live_filter(spec, self.shared.now()));
        let mut translated = translate::filter(&effective);
        translated.query = visible(translated.query);
        (effective, translated)
    }

    /// Every stored document the server query may match, narrowed in Rust to
    /// those `effective` matches.
    async fn candidates(
        &self,
        handle: &ScopedCollection,
        effective: &Filter,
        translated: &Translated,
    ) -> Result<Vec<Stored>> {
        let docs = handle
            .find(translated.query.clone(), FindOptions::default())
            .await
            .map_err(errors::failed("read documents"))?;
        Ok(decode_all(&docs)?
            .into_iter()
            .filter(|stored| !stored.deleted && effective.matches(&stored.key, &stored.body))
            .collect())
    }

    /// The page of documents matching `filter` in `sort` order starting at
    /// `offset`.
    pub(super) async fn page(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        offset: u64,
        limit: Option<usize>,
    ) -> Result<Matches> {
        let spec = self.shared.spec(collection).await?;
        let handle = self.collection(collection);
        let (effective, translated) = self.effective(&spec, filter);
        if translated.exact
            && let Some(plan) = translate::sort_plan(sort)
        {
            let complex = if sort.is_empty() {
                false
            } else {
                let found = handle
                    .aggregate(
                        translated.query.clone(),
                        vec![
                            doc! {"$addFields": plan.add_fields.clone()},
                            doc! {"$match": plan.complex.clone()},
                            doc! {"$limit": 1},
                            doc! {"$project": {"_id": 1}},
                        ],
                    )
                    .await
                    .map_err(errors::failed("inspect sort values"))?;
                !found.is_empty()
            };
            if !complex {
                let mut stages = Vec::new();
                if !plan.add_fields.is_empty() {
                    stages.push(doc! {"$addFields": plan.add_fields});
                }
                stages.push(doc! {"$sort": plan.sort});
                if offset > 0 {
                    stages.push(doc! {"$skip": to_i64(offset)});
                }
                if let Some(limit) = limit {
                    stages.push(doc! {"$limit": to_i64(limit as u64).saturating_add(1)});
                }
                if !plan.unset.is_empty() {
                    stages.push(doc! {"$unset": plan.unset});
                }
                let docs = handle
                    .aggregate(translated.query, stages)
                    .await
                    .map_err(errors::failed("query documents"))?;
                let mut items: Vec<Versioned<Value>> = decode_all(&docs)?
                    .into_iter()
                    .map(Stored::into_versioned)
                    .collect();
                let more = limit.is_some_and(|limit| items.len() > limit);
                if let Some(limit) = limit {
                    items.truncate(limit);
                }
                return Ok((items, more));
            }
        }
        let mut found: Vec<Versioned<Value>> = self
            .candidates(&handle, &effective, &translated)
            .await?
            .into_iter()
            .map(Stored::into_versioned)
            .collect();
        sort_documents(&mut found, sort, |item| (item.id.as_str(), &item.doc));
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(found.len());
        let end = limit.map_or(found.len(), |limit| {
            start.saturating_add(limit).min(found.len())
        });
        let more = end < found.len();
        Ok((found.drain(start..end).collect(), more))
    }

    /// How many live documents match `filter`.
    pub(super) async fn count_matching(&self, collection: &str, filter: &Filter) -> Result<u64> {
        let spec = self.shared.spec(collection).await?;
        let handle = self.collection(collection);
        let (effective, translated) = self.effective(&spec, filter);
        if translated.exact {
            return handle
                .count(translated.query)
                .await
                .map_err(errors::failed("count documents"));
        }
        Ok(self
            .candidates(&handle, &effective, &translated)
            .await?
            .len() as u64)
    }

    /// The `(key, version)` of every stored document `filter` matches, live
    /// or expired: the caller decides whether expiry applies.
    pub(super) async fn matching_pairs(
        &self,
        collection: &str,
        filter: &Filter,
    ) -> Result<Vec<(String, Version)>> {
        let handle = self.collection(collection);
        let mut translated = translate::filter(filter);
        translated.query = visible(translated.query);
        Ok(self
            .candidates(&handle, filter, &translated)
            .await?
            .into_iter()
            .map(|stored| (stored.key, stored.version))
            .collect())
    }

    /// The live documents matching `filter`, as removal targets.
    pub(super) async fn live_pairs(
        &self,
        collection: &str,
        filter: &Filter,
    ) -> Result<Vec<(String, Version)>> {
        let spec = self.shared.spec(collection).await?;
        let effective = filter.clone().and(live_filter(&spec, self.shared.now()));
        self.matching_pairs(collection, &effective).await
    }

    /// Full-text search over the declared fields, best first.
    pub(super) async fn search_hits(
        &self,
        collection: &str,
        text: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>> {
        let spec = self.shared.spec(collection).await?;
        let Some(search) = &spec.search else {
            return Err(StorageError::invalid_input(
                "this collection declares no search fields",
            ));
        };
        if search.fields.is_empty() {
            // Declared but empty: nothing is searchable, as on the memory
            // driver, and no text index exists to ask.
            return Ok(Vec::new());
        }
        // Tokens only: the reference tokenizer's alphanumeric runs, so no
        // `-negation` or `"phrase"` operator reaches `$text`.
        let tokens = value::tokens(&Value::String(text.to_owned()));
        if tokens.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let handle = self.collection(collection);
        let (effective, translated) = self.effective(&spec, &Filter::All);
        let mut query = doc! {"$text": {"$search": tokens.join(" ")}};
        if !translated.query.is_empty() {
            query = doc! {"$and": [query, translated.query]};
        }
        let mut options = FindOptions::builder()
            .projection(doc! {"score": {"$meta": "textScore"}, KEY: 1, BODY: 1, "_v": 1})
            .sort(doc! {"score": {"$meta": "textScore"}, KEY: 1})
            .build();
        if translated.exact {
            options.limit = Some(to_i64(limit as u64));
        }
        let docs = handle
            .find(query, options)
            .await
            .map_err(errors::failed("search documents"))?;
        let mut hits = Vec::new();
        for doc in &docs {
            let stored = Stored::decode(doc)?;
            if effective.matches(&stored.key, &stored.body) {
                hits.push(SearchHit {
                    id: stored.key,
                    score: doc.get_f64("score").unwrap_or_default(),
                });
            }
        }
        hits.truncate(limit);
        Ok(hits)
    }
}
