//! The identities earlier releases gave attributes and concepts, and the
//! rewrite of the rule bodies they stored, for migrating what an earlier
//! release stored.
//!
//! The release before picks (0.2) hashed an attribute as `{domain, name,
//! cardinality, type}` ([`attribute_uri_v0`], [`concept_identity_v0`]).
//! The releases with picks (0.3 and 0.4) hashed the relations an
//! attribute read, its pick, its type and its listed values
//! ([`attribute_reference_v1`], [`concept_identity_v1`]). This one hashes
//! the relation, the cardinality, the type and the listed values (see
//! [`AttributeDescriptor::reference`]), so every attribute's identity
//! changed again, and every concept's with it, a concept's identity
//! being a hash over its attributes'. Nothing on a read path uses these
//! functions: a migration does, to find a fact an earlier release keyed
//! by an old identity and key it by the current one. dialog's own such
//! fact is the transient marker, which `Branch::upgrade_rules` moves; an
//! application that keyed its own facts by a concept or attribute
//! identity moves those with these.
//!
//! A rule body a 0.3 or 0.4 release stored spells each attribute under a
//! pick, which this release refuses to read (see
//! [`AttributeDescriptor`]). [`rewrite_body`] rewrites such a body into
//! one this release reads with the same meaning: a `last` attribute is
//! cardinality one and an `all` attribute cardinality many, and an
//! attribute read under `max`, `min` or `top` (which a listed `as`
//! without a pick implied) is a cardinality-many field aggregating with
//! that aggregate, a `top`'s list reversed, since it ranked best first and
//! a listing now ranks least first. A chain of relations has no such
//! spelling: such a body is reported as [`Rewritten::Unmapped`] and left
//! inert, never read with another meaning.

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::ops::Not;

use base58::ToBase58;
use dialog_corpus::Entity;
use dialog_corpus::identity::{ENTITY, SYMBOL};
use ipld_core::ipld::Ipld;
use merkle_reference::{Reference, Type as ReferenceType};
use serde::Serialize;

use crate::Cardinality;
use crate::attribute::{AttributeDescriptor, Keyed, The};
use crate::concept::descriptor::ConceptDescriptor;
use crate::corpus::Value;
use crate::identity::{self, ATTRIBUTE, CONCEPT, field};
use crate::selective::Selective;

/// The identity the earlier release gave `attribute`, as its
/// `the:<base58>` URI.
pub fn attribute_uri_v0(attribute: &AttributeDescriptor) -> String {
    #[derive(Serialize)]
    struct CborAttributeDescriptor<'a> {
        domain: &'a str,
        name: &'a str,
        cardinality: Cardinality,
        /// The type's name before types were entities: `Text`, not
        /// `text:`.
        #[serde(rename = "type")]
        content_type: Option<&'static str>,
    }
    let name = match attribute.the() {
        The::Relation(the) => the.name(),
        The::Collection { keyed, .. } => match keyed {
            Keyed::Dictionary => "<dictionary>",
            Keyed::Sequence => "<sequence>",
        },
    };
    let bytes = serde_ipld_dagcbor::to_vec(&CborAttributeDescriptor {
        domain: attribute.domain(),
        name,
        cardinality: attribute.cardinality(),
        content_type: attribute.content_type().map(|kind| kind.legacy_name()),
    })
    .expect("CBOR encoding should not fail");
    format!(
        "the:{}",
        blake3::hash(&bytes).as_bytes().as_ref().to_base58()
    )
}

/// The identity the earlier release gave `concept`.
pub fn concept_identity_v0(concept: &ConceptDescriptor) -> Entity {
    #[derive(Serialize)]
    struct AttributeIdentity {
        #[serde(skip_serializing_if = "Not::not")]
        optional: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        conforms: Option<String>,
    }
    let mut attributes: BTreeMap<String, AttributeIdentity> = BTreeMap::new();
    for (_, field) in concept.with().iter() {
        attributes.insert(
            attribute_uri_v0(field.descriptor()),
            AttributeIdentity {
                optional: field.is_optional(),
                conforms: field
                    .conforms()
                    .map(|target| concept_identity_v0(target).to_string()),
            },
        );
    }
    let bytes = serde_ipld_dagcbor::to_vec(&attributes).expect("CBOR encoding should not fail");
    format!(
        "concept:{}",
        blake3::hash(&bytes).as_bytes().as_ref().to_base58()
    )
    .parse()
    .expect("valid entity URI")
}

/// The reference the release before this one, which read attributes
/// under a pick (0.3 and 0.4), gave `attribute`, read under the pick its
/// cardinality stood for: `last` for one, `all` for many.
///
/// That release hashed an attribute as a map under
/// [`ATTRIBUTE`](crate::identity::ATTRIBUTE) from `the` to the list of
/// relations it read, `pick` to the entity naming its pick, `as` to the
/// entity naming its type and `among` to the values it listed. This one
/// hashes the relation alone and its cardinality where it hashed the
/// pick (see [`AttributeDescriptor::reference`]).
pub fn attribute_reference_v1(attribute: &AttributeDescriptor) -> Reference {
    let pick: &[u8] = match attribute.cardinality() {
        Cardinality::One => b"last:",
        Cardinality::Many => b"all:",
    };
    let mut entries = vec![
        (
            field::THE,
            ReferenceType::LIST.list([SYMBOL.reference_of(attribute.the().to_string().as_bytes())]),
        ),
        (field::PICK, ENTITY.reference_of(pick)),
    ];
    if let Some(content_type) = attribute.content_type() {
        entries.push((
            field::AS,
            ENTITY.reference_of(content_type.uri().as_bytes()),
        ));
    }
    if !attribute.among().is_empty() {
        entries.push((
            field::AMONG,
            ReferenceType::LIST.list(attribute.among().iter().map(Value::reference)),
        ));
    }
    ATTRIBUTE.map(entries)
}

/// The identity the release before this one gave `attribute`, as its
/// `the:<base58>` URI (see [`attribute_reference_v1`]).
pub fn attribute_uri_v1(attribute: &AttributeDescriptor) -> String {
    format!(
        "the:{}",
        attribute_reference_v1(attribute).as_bytes().to_base58()
    )
}

/// The reference the release before this one gave `concept`: the
/// concept's reference over its attributes' earlier references.
pub fn concept_reference_v1(concept: &ConceptDescriptor) -> Reference {
    let entries = concept.with().iter().map(|(_name, field)| {
        let attribute = match field.aggregate() {
            Some(aggregate) => aggregated_reference_v1(field.descriptor(), aggregate),
            None => attribute_reference_v1(field.descriptor()),
        };
        let mut options = Vec::new();
        if field.is_optional() {
            options.push((field::OPTIONAL, field::TRUE));
        }
        if let Some(target) = field.conforms() {
            options.push((field::CONFORMS, concept_reference_v1(target)));
        }
        (attribute, ReferenceType::MAP.map(options))
    });
    CONCEPT.map(entries)
}

/// The reference the release before this one gave the attribute a field
/// aggregating with `aggregate` was migrated from: the attribute read under
/// the pick of the aggregate's name, a `top`'s values listed best first as
/// that release ranked them.
fn aggregated_reference_v1(attribute: &AttributeDescriptor, aggregate: Selective) -> Reference {
    let pick = match aggregate {
        Selective::Last => return attribute_reference_v1(attribute),
        Selective::Max => b"max:".as_slice(),
        Selective::Min => b"min:".as_slice(),
        Selective::Top => b"top:".as_slice(),
    };
    let mut entries = vec![
        (
            field::THE,
            ReferenceType::LIST.list([SYMBOL.reference_of(attribute.the().to_string().as_bytes())]),
        ),
        (field::PICK, ENTITY.reference_of(pick)),
    ];
    if let Some(content_type) = attribute.content_type() {
        entries.push((
            field::AS,
            ENTITY.reference_of(content_type.uri().as_bytes()),
        ));
    }
    let mut among: Vec<&Value> = attribute.among().iter().collect();
    if aggregate == Selective::Top {
        among.reverse();
    }
    if !among.is_empty() {
        entries.push((
            field::AMONG,
            ReferenceType::LIST.list(among.into_iter().map(Value::reference)),
        ));
    }
    ATTRIBUTE.map(entries)
}

/// The identity the release before this one gave `concept`.
pub fn concept_identity_v1(concept: &ConceptDescriptor) -> Entity {
    identity::entity("concept:", concept_reference_v1(concept))
}

/// What an attribute the release before this one stored reads as in
/// this release, when it reads as anything.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Unmapped {
    /// The attribute was read under a pick no release this one migrates
    /// from defined.
    Pick {
        /// The relation read.
        the: String,
        /// The pick it was read under.
        pick: String,
    },
    /// The attribute read a chain of relations, the first holding a
    /// candidate winning.
    Chain {
        /// The relations read, best first.
        relations: Vec<String>,
    },
}

impl Display for Unmapped {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Unmapped::Pick { the, pick } => write!(f, "{the} is read under `{pick}`"),
            Unmapped::Chain { relations } => {
                write!(f, "a chain of relations: {}", relations.join(", "))
            }
        }
    }
}

/// What [`rewrite_body`] made of a stored rule body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rewritten {
    /// The body spells no pick and no chain: it reads as it is.
    Unchanged,
    /// The body, every attribute's pick replaced by the cardinality it
    /// stood for, `last` by one and `all` by many, and a ranked pick by
    /// cardinality many and the aggregate of its name.
    Rewritten(Vec<u8>),
    /// The body reads some attribute in a way no cardinality reproduces.
    /// It is left as it is, and stays inert.
    Unmapped(Vec<Unmapped>),
}

/// A rule body the release before this one stored, in the format this
/// release reads: each attribute it spells under `pick: last` (or
/// `select: last`) under cardinality one, each under `all` under
/// cardinality many, and each under `max`, `min` or `top` (or listed
/// with no pick, which read as `top`) under cardinality many with the
/// aggregate of the pick's name, a `top`'s listing reversed to rank least
/// first. An attribute over a chain of relations has no such spelling,
/// and the body is reported rather than rewritten with another meaning.
/// `Err` when the bytes are not dag-cbor.
pub fn rewrite_body(bytes: &[u8]) -> Result<Rewritten, String> {
    let mut body: Ipld = serde_ipld_dagcbor::from_slice(bytes)
        .map_err(|error| format!("dag-cbor decode failed: {error}"))?;
    let mut unmapped = Vec::new();
    let changed = rewrite(&mut body, &mut unmapped);
    if !unmapped.is_empty() {
        return Ok(Rewritten::Unmapped(unmapped));
    }
    if !changed {
        return Ok(Rewritten::Unchanged);
    }
    serde_ipld_dagcbor::to_vec(&body)
        .map(Rewritten::Rewritten)
        .map_err(|error| format!("dag-cbor encode failed: {error}"))
}

/// Rewrite every attribute `node` holds, at any depth. Whether anything
/// changed.
fn rewrite(node: &mut Ipld, unmapped: &mut Vec<Unmapped>) -> bool {
    match node {
        Ipld::Map(map) => {
            let mut changed = is_attribute(map) && rewrite_attribute(map, unmapped);
            for value in map.values_mut() {
                changed |= rewrite(value, unmapped);
            }
            changed
        }
        Ipld::List(list) => {
            let mut changed = false;
            for value in list.iter_mut() {
                changed |= rewrite(value, unmapped);
            }
            changed
        }
        _ => false,
    }
}

/// Whether `map` is an attribute's wire form: it names its relation, or
/// relations, under `the` as text.
fn is_attribute(map: &BTreeMap<String, Ipld>) -> bool {
    match map.get("the") {
        Some(Ipld::String(_)) => true,
        Some(Ipld::List(relations)) => {
            !relations.is_empty()
                && relations
                    .iter()
                    .all(|relation| matches!(relation, Ipld::String(_)))
        }
        _ => false,
    }
}

/// Rewrite one attribute's wire form to the cardinality its pick stood
/// for, or note why it has none. Whether it changed.
fn rewrite_attribute(map: &mut BTreeMap<String, Ipld>, unmapped: &mut Vec<Unmapped>) -> bool {
    let text = |value: Option<&Ipld>| match value {
        Some(Ipld::String(text)) => Some(text.trim_end_matches(':').to_string()),
        _ => None,
    };
    let relations: Vec<String> = match map.get("the") {
        Some(Ipld::String(the)) => vec![the.clone()],
        Some(Ipld::List(list)) => list
            .iter()
            .filter_map(|relation| match relation {
                Ipld::String(relation) => Some(relation.clone()),
                _ => None,
            })
            .collect(),
        _ => return false,
    };
    let spelled = map.get("pick").or_else(|| map.get("select")).cloned();
    // A `top` could carry its ranking in the pick: `{"top:": [..]}`.
    let (pick, ranking) = match &spelled {
        Some(Ipld::Map(entry)) if entry.len() == 1 => {
            let (name, ranking) = entry.iter().next().expect("one entry");
            let ranking = match ranking {
                Ipld::List(values) if !values.is_empty() => Some(values.clone()),
                _ => None,
            };
            (Some(name.trim_end_matches(':').to_string()), ranking)
        }
        other => (text(other.as_ref()), None),
    };
    let listed = matches!(map.get("as"), Some(Ipld::List(values)) if !values.is_empty());
    let many = text(map.get("cardinality")).as_deref() == Some("many");
    if relations.len() > 1 {
        unmapped.push(Unmapped::Chain { relations });
        return false;
    }
    // A single relation listed reads as the relation.
    let mut changed = false;
    // A list naming no relation is left as it is: the descriptor
    // refuses it by name, and a stored body is never trusted to be well
    // formed.
    if let Some(Ipld::List(relations)) = map.get("the")
        && let Some(the) = relations.first().cloned()
    {
        map.insert("the".to_string(), the);
        changed = true;
    }
    let (cardinality, aggregate) = match pick.as_deref() {
        // A listed domain with no pick was read under `top`.
        None if listed => (Some("many"), Some("top")),
        // `cardinality` alone is how this release spells it.
        None => return changed,
        // Read newest and written adding: what a field aggregating with `last`
        // over a cardinality-many attribute is.
        Some("last") if many => (Some("many"), Some("last")),
        Some("last") => (None, None),
        Some("all") => (Some("many"), None),
        // A `top` ranking nothing ranked every value alike and so read
        // the newest.
        Some("top") if !listed && ranking.is_none() => (Some("many"), Some("last")),
        Some(ranked @ ("top" | "max" | "min")) => (Some("many"), Some(ranked)),
        Some(other) => {
            unmapped.push(Unmapped::Pick {
                the: relations.into_iter().next().unwrap_or_default(),
                pick: other.to_string(),
            });
            return false;
        }
    };
    if aggregate == Some("top") {
        // That release ranked best first; a listing ranks least first.
        let ranked = match (ranking, map.get("as")) {
            (Some(ranking), _) => Some(ranking),
            (None, Some(Ipld::List(values))) => Some(values.clone()),
            (None, _) => None,
        };
        if let Some(mut ranked) = ranked {
            ranked.reverse();
            map.insert("as".to_string(), Ipld::List(ranked));
        }
    }
    map.remove("pick");
    map.remove("select");
    map.remove("cardinality");
    if let Some(cardinality) = cardinality {
        map.insert("cardinality".to_string(), Ipld::String(cardinality.into()));
    }
    if let Some(aggregate) = aggregate {
        map.insert("aggregate".to_string(), Ipld::String(aggregate.into()));
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stored body is never trusted to be well formed: an attribute
    /// whose `the` lists no relation is left as it is, for the
    /// descriptor to refuse by name, where indexing the list would
    /// panic on a body any peer can write.
    #[dialog_common::test]
    fn it_leaves_an_attribute_listing_no_relation_as_it_is() {
        let attribute = Ipld::Map(
            [
                ("the".to_string(), Ipld::List(Vec::new())),
                ("as".to_string(), Ipld::String("text:".to_string())),
            ]
            .into(),
        );
        let bytes = serde_ipld_dagcbor::to_vec(&attribute).expect("encodes");
        assert_eq!(rewrite_body(&bytes), Ok(Rewritten::Unchanged));
    }
}
