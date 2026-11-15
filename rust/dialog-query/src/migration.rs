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
//! one this release reads with the same meaning, where there is one: a
//! `last` attribute is cardinality one and an `all` attribute
//! cardinality many. A read under `max`, `min` or `top`, and a chain of
//! relations, has no such spelling: a ranked read is now a rule folding
//! with that aggregator, which only the author can write. Such a body is
//! reported as [`Rewritten::Unmapped`] and left inert, never read with
//! another meaning.

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
        let mut options = Vec::new();
        if field.is_optional() {
            options.push((field::OPTIONAL, field::TRUE));
        }
        if let Some(target) = field.conforms() {
            options.push((field::CONFORMS, concept_reference_v1(target)));
        }
        (
            attribute_reference_v1(field.descriptor()),
            ReferenceType::MAP.map(options),
        )
    });
    CONCEPT.map(entries)
}

/// The identity the release before this one gave `concept`.
pub fn concept_identity_v1(concept: &ConceptDescriptor) -> Entity {
    identity::entity("concept:", concept_reference_v1(concept))
}

/// What an attribute the release before this one stored reads as in
/// this release, when it reads as anything.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Unmapped {
    /// The attribute was read under a pick that ranks its candidates:
    /// `max`, `min`, or `top` (which a listed `as` without a pick
    /// implied). A ranked read is now a rule folding with that
    /// aggregator, which the migration cannot write for the author.
    Ranked {
        /// The relation read.
        the: String,
        /// The pick it was read under: `max`, `min` or `top`.
        pick: String,
    },
    /// The attribute read a chain of relations, the first holding a
    /// candidate winning.
    Chain {
        /// The relations read, best first.
        relations: Vec<String>,
    },
    /// The attribute was read under `last` and written under
    /// cardinality many, a spelling no release shipped: neither
    /// cardinality reads and writes it as it was.
    Adding {
        /// The relation read.
        the: String,
    },
}

impl Display for Unmapped {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Unmapped::Ranked { the, pick } => write!(f, "{the} is read under `{pick}`"),
            Unmapped::Chain { relations } => {
                write!(f, "a chain of relations: {}", relations.join(", "))
            }
            Unmapped::Adding { the } => {
                write!(
                    f,
                    "{the} is read under `last` and written under cardinality many"
                )
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
    /// stood for: `last` by one and `all` by many.
    Rewritten(Vec<u8>),
    /// The body reads some attribute in a way no cardinality reproduces.
    /// It is left as it is, and stays inert.
    Unmapped(Vec<Unmapped>),
}

/// A rule body the release before this one stored, in the format this
/// release reads: each attribute it spells under `pick: last` (or
/// `select: last`) under cardinality one, and each under `all` under
/// cardinality many, the cardinality each pick stood for. An attribute
/// read under `max`, `min` or `top`, or over a chain of relations, has
/// no such cardinality, and the body is reported rather than rewritten
/// with another meaning. `Err` when the bytes are not dag-cbor.
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
    let pick = text(map.get("pick")).or_else(|| text(map.get("select")));
    let listed = matches!(map.get("as"), Some(Ipld::List(_)));
    let many = text(map.get("cardinality")).as_deref() == Some("many");
    if relations.len() > 1 {
        unmapped.push(Unmapped::Chain { relations });
        return false;
    }
    let the = relations.into_iter().next().unwrap_or_default();
    // A single relation listed reads as the relation.
    let mut changed = false;
    if let Some(Ipld::List(_)) = map.get("the") {
        map.insert("the".to_string(), Ipld::String(the.clone()));
        changed = true;
    }
    let cardinality = match pick.as_deref() {
        // A listed domain with no pick was read under `top`.
        None if listed => {
            unmapped.push(Unmapped::Ranked {
                the,
                pick: "top".to_string(),
            });
            return false;
        }
        // `cardinality` alone is how this release spells it.
        None => return changed,
        Some("last") if many => {
            unmapped.push(Unmapped::Adding { the });
            return false;
        }
        Some("last") => None,
        Some("all") => Some("many"),
        Some(other) => {
            unmapped.push(Unmapped::Ranked {
                the,
                pick: other.to_string(),
            });
            return false;
        }
    };
    map.remove("pick");
    map.remove("select");
    map.remove("cardinality");
    if let Some(cardinality) = cardinality {
        map.insert("cardinality".to_string(), Ipld::String(cardinality.into()));
    }
    true
}
