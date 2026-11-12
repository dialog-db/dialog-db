use serde::{Deserialize, Serialize};

use crate::{Entity, Relation, Value};

use super::Cause;

/// A claim as recorded in the history index.
///
/// Structurally this is a [`Fact`](crate::Fact) whose `cause` is a
/// set of [`Version`](super::Version)s rather than a single content hash:
/// the cause identifies the prior claims on the same `(entity, attribute)`
/// that this claim supersedes, analogous to how a git commit records which
/// commits it builds on, but scoped to individual fact lineages.
///
/// Retractions are claims like any other and participate in the same lineage:
/// a retraction's cause identifies the claim(s) whose assertion it withdraws.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Claim {
    /// The attribute (predicate) of the claim
    pub the: Relation,
    /// The entity (subject) of the claim
    pub of: Entity,
    /// The value (object) of the claim
    // A `(value_type, bytes)` pair, the type-tagged shape
    // [`Datum`](crate::Datum) uses, rather than serde's untagged
    // representation, which cannot round-trip every variant faithfully
    // (an unsigned integer resurfaces as a float).
    #[serde(with = "crate::model::type_and_bytes")]
    pub is: Value,
    /// The versions of the prior claims on the same `(of, the)` superseded by
    /// this claim; empty on first write
    pub cause: Cause,
}
