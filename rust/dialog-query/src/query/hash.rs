//! The identity of a query: a hash of its canonical encoding.

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter, Result as FmtResult};

use dialog_common::Blake3Hash;
use ipld_core::ipld::Ipld;
use serde::Serialize;

/// The identity of a query: the BLAKE3 hash of its canonical encoding,
/// so two queries that say the same thing share one identity however
/// they were built. Code that keeps one evaluation per query, such as a
/// stack's subscriptions, keys it by this.
///
/// The encoding is DAG-CBOR, whose maps are written in one order
/// whatever order they iterate in, the encoding a rule's identity is
/// hashed from. A query's fresh variables, the ones it names `__<n>`
/// for itself from a counter (an attribute query's handle on its
/// claims), are named again by the order they first occur in that
/// encoding, so two queries built alike agree on them. A query DAG-CBOR
/// cannot encode (a constant past the range of a 64-bit integer) is
/// hashed from its JSON instead, its fresh variables named again the
/// same way. The two encodings are hashed apart.
///
/// ```no_run
/// # use dialog_query::{AttributeQuery, QueryHash, Term};
/// # use dialog_query::attribute::Relation;
/// # use dialog_corpus::Entity;
/// fn selected() -> AttributeQuery {
///     AttributeQuery::from(
///         Term::<Relation>::from(Relation::from(
///             "ui/selected".parse::<dialog_corpus::Relation>().expect("an attribute"),
///         ))
///         .of(Term::<Entity>::var("doc"))
///         .is(Term::<bool>::var("on")),
///     )
/// }
/// assert_eq!(QueryHash::of(&selected()), QueryHash::of(&selected()));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryHash(Blake3Hash);

/// Set before a DAG-CBOR encoding, so it never hashes like a JSON one.
const CBOR: &[u8] = b"dag-cbor\0";
/// Set before a JSON encoding.
const JSON: &[u8] = b"json\0";

impl QueryHash {
    /// The identity of `query`, or `None` when it has neither a
    /// DAG-CBOR nor a JSON encoding.
    pub fn of<Q: Serialize + ?Sized>(query: &Q) -> Option<Self> {
        let cbor = serde_ipld_dagcbor::to_vec(query)
            .ok()
            .and_then(|bytes| serde_ipld_dagcbor::from_slice::<Ipld>(&bytes).ok());
        if let Some(mut ipld) = cbor {
            rename_ipld(&mut ipld, &mut BTreeMap::new());
            let bytes = serde_ipld_dagcbor::to_vec(&ipld).ok()?;
            return Some(Self(Blake3Hash::hash_iter(
                [CBOR, bytes.as_slice()].into_iter(),
            )));
        }
        let mut json = serde_json::to_value(query).ok()?;
        rename_json(&mut json, &mut BTreeMap::new());
        let bytes = serde_json::to_vec(&json).ok()?;
        Some(Self(Blake3Hash::hash_iter(
            [JSON, bytes.as_slice()].into_iter(),
        )))
    }

    /// The hash.
    pub fn hash(&self) -> &Blake3Hash {
        &self.0
    }
}

/// Whether `name` is one a query gave a variable of its own, from a
/// counter (see [`Term::unique`](crate::Term::unique)).
fn fresh(name: &str) -> bool {
    name.strip_prefix("__")
        .is_some_and(|count| !count.is_empty() && count.bytes().all(|b| b.is_ascii_digit()))
}

/// The name `names` gives the fresh variable `name`: the one it was
/// given at its first occurrence, or the next one.
fn renamed(name: &str, names: &mut BTreeMap<String, String>) -> String {
    let next = format!("__{}", names.len());
    names.entry(name.to_string()).or_insert(next).clone()
}

/// Name every fresh variable in `ipld`, a term's `{"?": {"name": ..}}`,
/// by the order it first occurs, a map's entries visited in key order.
fn rename_ipld(ipld: &mut Ipld, names: &mut BTreeMap<String, String>) {
    match ipld {
        Ipld::Map(map) => {
            if let Some(Ipld::Map(variable)) = map.get_mut("?") {
                if let Some(Ipld::String(name)) = variable.get_mut("name")
                    && fresh(name)
                {
                    *name = renamed(name, names);
                }
                return;
            }
            for value in map.values_mut() {
                rename_ipld(value, names);
            }
        }
        Ipld::List(items) => {
            for item in items {
                rename_ipld(item, names);
            }
        }
        _ => {}
    }
}

/// [`rename_ipld`] for a query's JSON.
fn rename_json(json: &mut serde_json::Value, names: &mut BTreeMap<String, String>) {
    match json {
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::Object(variable)) = map.get_mut("?") {
                if let Some(serde_json::Value::String(name)) = variable.get_mut("name")
                    && fresh(name)
                {
                    *name = renamed(name, names);
                }
                return;
            }
            let mut keys: Vec<String> = map.keys().cloned().collect();
            keys.sort();
            for key in keys {
                if let Some(value) = map.get_mut(&key) {
                    rename_json(value, names);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                rename_json(item, names);
            }
        }
        _ => {}
    }
}

impl From<Blake3Hash> for QueryHash {
    fn from(hash: Blake3Hash) -> Self {
        Self(hash)
    }
}

impl Display for QueryHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Display::fmt(&self.0, f)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::attribute::Relation;
    use crate::{AttributeQuery, ConceptQuery, Term};
    use dialog_corpus::Entity;

    fn selected(value: &str) -> AttributeQuery {
        AttributeQuery::from(
            Term::<Relation>::from(Relation::from(
                "ui/selected".parse::<dialog_corpus::Relation>().unwrap(),
            ))
            .of(Term::<Entity>::var("doc"))
            .is(Term::<bool>::var(value)),
        )
    }

    /// Two queries built alike share an identity, though each named its
    /// own handle on its claims afresh; one that differs in a variable's
    /// name does not.
    #[dialog_common::test]
    fn it_hashes_equal_queries_equally() {
        let first = QueryHash::of(&selected("on")).expect("encodes");
        assert_eq!(QueryHash::of(&selected("on")), Some(first.clone()));
        assert_ne!(QueryHash::of(&selected("off")), Some(first));
    }

    /// A concept query's identity does not depend on the order its
    /// terms were bound in.
    #[dialog_common::test]
    fn it_hashes_a_concept_query_whatever_order_its_terms_were_bound_in() {
        let json = |order: &[(&str, serde_json::Value)]| {
            let mut terms = serde_json::Map::new();
            for (name, term) in order {
                terms.insert((*name).to_string(), term.clone());
            }
            serde_json::json!({
                "assert": { "with": {
                    "name": { "the": "person/name", "as": "Text" },
                    "age": { "the": "person/age", "as": "UnsignedInteger" }
                } },
                "where": terms
            })
        };
        let this = ("this", serde_json::json!({ "?": { "name": "this" } }));
        let name = ("name", serde_json::json!({ "?": { "name": "name" } }));
        let age = ("age", serde_json::json!(41));
        let one: ConceptQuery =
            serde_json::from_value(json(&[this.clone(), name.clone(), age.clone()]))
                .expect("decodes");
        let other: ConceptQuery =
            serde_json::from_value(json(&[age, name, this])).expect("decodes");
        assert_eq!(QueryHash::of(&one), QueryHash::of(&other));
        assert!(QueryHash::of(&one).is_some());
    }
}
