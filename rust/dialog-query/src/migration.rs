//! The identities each release gave attributes, concepts and rules,
//! and the rewrite of the rule bodies earlier releases stored, for
//! migrating what an earlier release stored.
//!
//! An identity is a hash of a definition. When what it hashes changes,
//! every identity that release computes moves, and whatever holds an
//! identity as a value has to be rewritten from the old one to the new
//! one. dialog's own such fact is the transient marker, which
//! `Branch::upgrade_rules` moves. An application that writes dialog
//! identities into its own facts (a route naming a concept, a name
//! pointing at an attribute) moves those itself, and for that it
//! computes, for a definition it holds, the identity each earlier
//! generation gave it and the one this release gives it. Nothing on a
//! read path uses these functions.
//!
//! # Generations
//!
//! A generation is one way of computing identities, numbered by the
//! suffix of the functions that compute it. [`IDENTITY_GENERATION`] is
//! the one this release computes. What each release wrote, checked
//! against the code at each release's tag:
//!
//! | generation | releases | an attribute hashes | a concept hashes | a rule hashes |
//! |---|---|---|---|---|
//! | 0 | 0.1.0 | blake3 of dag-cbor `{domain, name, cardinality, type}`, the type by its legacy name (`Text`) | blake3 of dag-cbor, each attribute's generation 0 URI to `{optional, conforms}` | blake3 of its encoded body |
//! | none | 0.2.0 | blake3 of dag-cbor `{domain, name, then, type, pick, among}` | as generation 0, over these attribute URIs | blake3 of dag-cbor of its canonical descriptor |
//! | 1 | 0.3.0, 0.4.0, 0.5.0 | merkle reference under [`ATTRIBUTE`](crate::identity::ATTRIBUTE) of `{the: [relations], pick, as, among}` | merkle reference under [`CONCEPT`](crate::identity::CONCEPT) of each attribute's reference to the field's options | merkle reference under [`RULE`](crate::identity::RULE) of its head, premises and reduce, over generation 1 concepts and attributes |
//! | 2 | the next release | merkle reference under `ATTRIBUTE` of `{the, cardinality, as, among}` ([`AttributeDescriptor::reference`]) | as generation 1, over these attribute references, a field's `pick` among its options ([`ConceptDescriptor::reference`]) | as generation 1, over these concepts and attributes |
//!
//! The functions for each generation:
//!
//! | generation | attribute | concept | rule |
//! |---|---|---|---|
//! | 0 | [`attribute_uri_v0`] | [`concept_identity_v0`] | not exported |
//! | 1 | [`attribute_uri_v1`], [`attribute_reference_v1`]; read under `max`, `min` or `top`: [`picked_attribute_uri_v1`], [`picked_attribute_reference_v1`] | [`concept_identity_v1`], [`concept_reference_v1`] | not exported |
//! | 2 | [`attribute_uri_v2`] | [`concept_identity_v2`] | [`rule_identity_v2`] |
//!
//! What is not exported, and why:
//!
//! - **0.2.0's identities.** 0.2.0 wrote attribute, concept and rule
//!   identities of its own, which no function here reproduces. They
//!   have no generation number: the numbers follow the functions that
//!   existed before 0.2.0 was found to differ (`_v0` is 0.1.0, `_v1` is
//!   0.3.0 to 0.5.0). No embedder is known to hold them: tonk moved from
//!   the 0.1.0 commit straight to 0.3.0. A space 0.2.0 wrote keeps its
//!   0.2.0 identities until they are found by other means;
//!   `Branch::upgrade_rules` does not move a transient marker keyed by
//!   one.
//! - **Rule identities of generations 0 and 1.** No fact holds a rule's
//!   identity as a value: a rule's facts are stored under it, and
//!   `Branch::upgrade_rules` finds an earlier rule by the entity it is
//!   stored under rather than by computing it. Generation 0 hashed a
//!   body encoding this release no longer writes, so it cannot be
//!   computed from a rule this release reads. Generation 1 could be: it
//!   would take the rule reference this release computes, with every
//!   concept and attribute in it referenced by its generation 1
//!   reference, which means threading a generation through the code
//!   that computes a rule's current identity. A rule that holds a
//!   pattern or reads a chain of relations has no generation 1 identity.
//!
//! # Rewriting what holds an identity
//!
//! For a definition it holds, an embedder computes the identity of each
//! earlier generation and the current one, and rewrites a fact holding
//! the earlier one:
//!
//! ```
//! use dialog_query::migration::{
//!     IDENTITY_GENERATION, attribute_uri_v1, attribute_uri_v2, concept_identity_v1,
//!     concept_identity_v2,
//! };
//! use dialog_query::{AttributeDescriptor, ConceptDescriptor};
//! # fn main() -> Result<(), serde_json::Error> {
//! let title: AttributeDescriptor =
//!     serde_json::from_value(serde_json::json!({ "the": "note/title", "as": "text:" }))?;
//! let note: ConceptDescriptor = serde_json::from_value(serde_json::json!({
//!     "with": { "title": { "the": "note/title", "as": "text:" } }
//! }))?;
//!
//! // What an earlier release named them, and what this one does.
//! let moved = [
//!     (attribute_uri_v1(&title), attribute_uri_v2(&title)),
//!     (
//!         concept_identity_v1(&note).to_string(),
//!         concept_identity_v2(&note).to_string(),
//!     ),
//! ];
//!
//! // A fact of the embedder's that holds the earlier identity as its
//! // value, rewritten to hold the current one.
//! let route = concept_identity_v1(&note).to_string();
//! let rewritten = moved
//!     .iter()
//!     .find(|(earlier, _)| *earlier == route)
//!     .map(|(_, current)| current.clone())
//!     .unwrap_or(route);
//! assert_eq!(rewritten, concept_identity_v2(&note).to_string());
//!
//! // Recorded beside the rewrite, so the next release that moves
//! // identities is noticed.
//! assert_eq!(IDENTITY_GENERATION, 2);
//! # Ok(())
//! # }
//! ```
//!
//! # Rewriting rule bodies
//!
//! A rule body a 0.3 or 0.4 release stored spells each attribute under a
//! pick, which this release refuses to read (see
//! [`AttributeDescriptor`]). [`rewrite_body`] rewrites such a body into
//! one this release reads with the same meaning: a `last` attribute is
//! cardinality one and an `all` attribute cardinality many, and an
//! attribute read under `max`, `min` or `top` (which a listed `as`
//! without a pick implied) is a cardinality-many field with that pick,
//! a `top`'s list in the order it had, best first. A chain of relations
//! has no such
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
use crate::rule::{DeductiveRule, InductiveRule};
use crate::selective::Selective;

/// Generation 0 of `attribute`'s identity, as its `the:<base58>` URI:
/// what 0.1.0 named it.
///
/// The blake3 hash of the dag-cbor map of its domain, its name, its
/// cardinality and its type by the name types had before they were
/// entities (`Text`, not `text:`). A collection's name is
/// `<dictionary>` or `<sequence>`.
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

/// Generation 0 of `concept`'s identity: what 0.1.0 named it.
///
/// The blake3 hash of the dag-cbor map from each field's attribute, by
/// its [generation 0 URI](attribute_uri_v0), to the field's options:
/// `optional` when the field is optional, and `conforms` to the
/// generation 0 identity of the concept its value conforms to. Field
/// names and descriptions are not part of it.
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

/// Generation 1 of `attribute`'s reference: what 0.3.0, 0.4.0 and
/// 0.5.0, which read every attribute under a pick, gave it, read under
/// the pick its cardinality stood for: `last` for one, `all` for many.
///
/// Those releases hashed an attribute as a map under
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

/// Generation 1 of `attribute`'s identity, as its `the:<base58>` URI:
/// what 0.3.0, 0.4.0 and 0.5.0 named it. `the:` followed by its
/// [generation 1 reference](attribute_reference_v1) in base58.
pub fn attribute_uri_v1(attribute: &AttributeDescriptor) -> String {
    format!(
        "the:{}",
        attribute_reference_v1(attribute).as_bytes().to_base58()
    )
}

/// Generation 1 of `concept`'s reference: what 0.3.0, 0.4.0 and 0.5.0
/// gave it.
///
/// A map under [`CONCEPT`](crate::identity::CONCEPT) from each field's
/// attribute, by its [generation 1 reference](attribute_reference_v1),
/// to the field's options: `optional` to `true` when the field is
/// optional, and `conforms` to the generation 1 reference of the concept
/// its value conforms to. A field that picks `max`, `min` or `top`
/// enters as the attribute those releases read under that pick. Field
/// names and descriptions are not part of it.
pub fn concept_reference_v1(concept: &ConceptDescriptor) -> Reference {
    let entries = concept.with().iter().map(|(_name, field)| {
        let attribute = match field.pick() {
            Some(aggregate) => picked_attribute_reference_v1(field.descriptor(), aggregate),
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

/// Generation 1 of the reference of `attribute` read under the pick
/// `aggregate`, by itself and not through a concept: what 0.3.0, 0.4.0
/// and 0.5.0 gave an attribute they read with that pick, such as
/// `{"the": "org/salary", "as": "natural:", "pick": "max:"}`, or a
/// listed `as` with no pick, which they read under `top`.
///
/// The same map under [`ATTRIBUTE`](crate::identity::ATTRIBUTE) as
/// [`attribute_reference_v1`], with `pick` the entity naming `aggregate`
/// (`max:`, `min:` or `top:`) and `among` the values `attribute` lists in
/// the order it lists them, which for `top` is best first.
/// [`Selective::Last`] is [`attribute_reference_v1`] of the attribute.
/// This release reads such an attribute as a concept field picking with
/// `aggregate`, which is how [`concept_reference_v1`] references one.
pub fn picked_attribute_reference_v1(
    attribute: &AttributeDescriptor,
    aggregate: Selective,
) -> Reference {
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
    let among: Vec<&Value> = attribute.among().iter().collect();
    if !among.is_empty() {
        entries.push((
            field::AMONG,
            ReferenceType::LIST.list(among.into_iter().map(Value::reference)),
        ));
    }
    ATTRIBUTE.map(entries)
}

/// Generation 1 of the identity of `attribute` read under the pick
/// `aggregate`, as its `the:<base58>` URI: what 0.3.0, 0.4.0 and 0.5.0
/// named it. `the:` followed by its
/// [generation 1 reference](picked_attribute_reference_v1) in base58.
pub fn picked_attribute_uri_v1(attribute: &AttributeDescriptor, aggregate: Selective) -> String {
    format!(
        "the:{}",
        picked_attribute_reference_v1(attribute, aggregate)
            .as_bytes()
            .to_base58()
    )
}

/// Generation 1 of `concept`'s identity: what 0.3.0, 0.4.0 and 0.5.0
/// named it. `concept:` followed by its
/// [generation 1 reference](concept_reference_v1) in base58.
pub fn concept_identity_v1(concept: &ConceptDescriptor) -> Entity {
    identity::entity("concept:", concept_reference_v1(concept))
}

/// The generation of the identities this release computes: the suffix
/// of the functions here that compute them, [`attribute_uri_v2`],
/// [`concept_identity_v2`] and [`rule_identity_v2`].
///
/// An embedder that holds dialog identities as values records it beside
/// what it wrote, and rewrites those values (see the
/// [module](self)) when a release reports another.
///
/// It is bumped by any change to what an attribute's, a concept's or a
/// rule's identity hashes: a part added, removed or spelled
/// differently, a type or field constant, the hash. Whoever makes such a
/// change also freezes the functions of this generation as copies of
/// what they compute now, adds the next generation's beside them, and
/// re-pins the identities the tests of this module hold. A change that
/// moves no identity does not bump it.
pub const IDENTITY_GENERATION: u32 = 2;

/// Generation 2 of `attribute`'s identity, as its `the:<base58>` URI:
/// what the release after 0.5.0 names it, and this release.
///
/// `the:` followed by the base58 of the merkle reference under
/// [`ATTRIBUTE`](crate::identity::ATTRIBUTE) of its relation, its
/// cardinality, its type and the values it lists (see
/// [`AttributeDescriptor::reference`]). The description is not part of
/// it.
///
/// It calls the identity this release computes. When
/// [`IDENTITY_GENERATION`] moves past 2, this becomes a copy of what it
/// computes today.
pub fn attribute_uri_v2(attribute: &AttributeDescriptor) -> String {
    attribute.to_uri()
}

/// Generation 2 of `concept`'s identity: what the release after 0.5.0
/// names it, and this release.
///
/// `concept:` followed by the base58 of the merkle reference under
/// [`CONCEPT`](crate::identity::CONCEPT) from each field's attribute, by
/// its [generation 2 reference](AttributeDescriptor::reference), to the
/// field's options: `optional`, `conforms` (the target concept's
/// reference) and `pick` (see [`ConceptDescriptor::reference`]). Field
/// names and descriptions are not part of it.
///
/// It calls the identity this release computes. When
/// [`IDENTITY_GENERATION`] moves past 2, this becomes a copy of what it
/// computes today.
pub fn concept_identity_v2(concept: &ConceptDescriptor) -> Entity {
    concept.this()
}

/// A rule of either kind, as [`rule_identity_v2`] takes it.
#[derive(Clone, Copy)]
pub enum AnyRule<'a> {
    /// A deductive rule.
    Deductive(&'a DeductiveRule),
    /// An inductive rule.
    Inductive(&'a InductiveRule),
}

impl<'a> From<&'a DeductiveRule> for AnyRule<'a> {
    fn from(rule: &'a DeductiveRule) -> Self {
        AnyRule::Deductive(rule)
    }
}

impl<'a> From<&'a InductiveRule> for AnyRule<'a> {
    fn from(rule: &'a InductiveRule) -> Self {
        AnyRule::Inductive(rule)
    }
}

/// Generation 2 of `rule`'s identity: what the release after 0.5.0
/// names it, and this release. `None` for a rule with no stored form,
/// which has no identity.
///
/// `rule:` followed by the base58 of the merkle reference under
/// [`RULE`](crate::identity::RULE) of its canonical spelling: its head
/// (`deduce`, `assert!` or `retract!`) as the concluded concept's
/// [generation 2 reference](ConceptDescriptor::reference) with each
/// head field bound to the canonical variable it names, its positive
/// and its negated premises as sets, each concept application by its
/// concept's reference and its bindings keyed by the fields they bind,
/// and its reductions.
///
/// It calls the identity this release computes. When
/// [`IDENTITY_GENERATION`] moves past 2, this becomes a copy of what it
/// computes today.
pub fn rule_identity_v2<'a>(rule: impl Into<AnyRule<'a>>) -> Option<Entity> {
    match rule.into() {
        AnyRule::Deductive(rule) => rule.try_this(),
        AnyRule::Inductive(rule) => rule.try_this(),
    }
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
/// aggregate of the pick's name, a `top`'s listing kept in the order it
/// had, best first. An attribute over a chain of relations has no such
/// spelling, and the body is reported rather than rewritten with
/// another meaning.
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
    let (cardinality, picked) = match pick.as_deref() {
        // A listed domain with no pick was read under `top`.
        None if listed => (None, Some("top")),
        // `cardinality` alone is how this release spells it.
        None => return changed,
        // Read newest and written adding: what a field picking `last`
        // over a cardinality-many attribute is.
        Some("last") if many => (Some("many"), Some("last")),
        Some("last") => (None, None),
        Some("all") => (Some("many"), None),
        // A `top` ranking nothing ranked every value alike and so read
        // the newest.
        Some("top") if !listed && ranking.is_none() => (Some("many"), Some("last")),
        // A ranking pick reads a cardinality-many attribute, which the
        // pick says by itself.
        Some(ranked @ ("top" | "max" | "min")) => (None, Some(ranked)),
        Some(other) => {
            unmapped.push(Unmapped::Pick {
                the: relations.into_iter().next().unwrap_or_default(),
                pick: other.to_string(),
            });
            return false;
        }
    };
    // A `top` that carried its ranking in the pick lists it under `as`,
    // best first as it was.
    if picked == Some("top")
        && let Some(ranking) = ranking
    {
        map.insert("as".to_string(), Ipld::List(ranking));
    }
    map.remove("pick");
    map.remove("select");
    map.remove("cardinality");
    if let Some(cardinality) = cardinality {
        map.insert("cardinality".to_string(), Ipld::String(cardinality.into()));
    }
    if let Some(picked) = picked {
        map.insert("pick".to_string(), Ipld::String(picked.into()));
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Type;
    use crate::rule::InductiveRuleDescriptor;
    use crate::rule::body::format_of;
    use crate::rule::deductive::descriptor::DeductiveRuleDescriptor;
    use serde_json::json;

    /// An attribute declaring a type.
    fn typed() -> anyhow::Result<AttributeDescriptor> {
        Ok(serde_json::from_value(
            json!({ "the": "org/salary", "as": "natural:" }),
        )?)
    }

    /// An attribute whose `as` lists its values.
    fn listed() -> anyhow::Result<AttributeDescriptor> {
        Ok(serde_json::from_value(
            json!({ "the": "job/state", "as": ["todo", "done"] }),
        )?)
    }

    /// An attribute of cardinality many.
    fn many() -> anyhow::Result<AttributeDescriptor> {
        Ok(serde_json::from_value(
            json!({ "the": "org/tag", "as": "text:", "cardinality": "many" }),
        )?)
    }

    /// A concept with a field ranking its values.
    fn ranked() -> anyhow::Result<ConceptDescriptor> {
        Ok(serde_json::from_value(json!({ "with": {
            "name": { "the": "org/name", "as": "text:" },
            "salary": { "the": "org/salary", "as": "natural:", "pick": "max" }
        } }))?)
    }

    /// A deductive rule: `org/salary(x) := b :- org/bonus(x) = b`.
    fn deductive() -> anyhow::Result<DeductiveRule> {
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
            "deduce": { "with": {
                "salary": { "the": "org/salary", "as": "natural:" }
            } },
            "when": [{
                "assert": { "with": {
                    "bonus": { "the": "org/bonus", "as": "natural:" }
                } },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "bonus": { "?": { "name": "salary" } }
                }
            }]
        }))?;
        Ok(descriptor.compile()?)
    }

    /// A deductive rule holding a pattern:
    /// `note/card(x) := {title: t} :- note/title(x) = t`.
    fn patterned() -> anyhow::Result<DeductiveRule> {
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
            "deduce": { "with": {
                "card": { "the": "note/card", "as": "composite:map", "cardinality": "many" }
            } },
            "when": [
                {
                    "assert": { "with": {
                        "title": { "the": "note/title", "as": "text:" }
                    } },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "title": { "?": { "name": "title" } }
                    }
                },
                {
                    "assert": "==",
                    "where": {
                        "this": { "match": { "map": [
                            [{ "text:": "title" }, { "?": { "name": "title" } }]
                        ] } },
                        "is": { "?": { "name": "card" } }
                    }
                }
            ]
        }))?;
        Ok(descriptor.compile()?)
    }

    /// An inductive rule: `assert! derived/tag(x) := t when doc/title(x) = t`.
    fn inductive() -> anyhow::Result<InductiveRule> {
        let descriptor: InductiveRuleDescriptor = serde_json::from_value(json!({
            "assert!": { "with": {
                "tag": { "the": "derived/tag", "as": "text:", "cardinality": "many" }
            } },
            "when": [{
                "assert": { "with": {
                    "title": { "the": "doc/title", "as": "text:" }
                } },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "title": { "?": { "name": "tag" } }
                }
            }]
        }))?;
        Ok(descriptor.compile()?)
    }

    /// The fixed definitions below hold a pattern where they say so: the
    /// pinned rule is stored in the format that writes one.
    #[dialog_common::test]
    fn the_pinned_pattern_rule_holds_a_pattern() -> anyhow::Result<()> {
        let stored = patterned()?
            .try_encode()
            .ok_or_else(|| anyhow::anyhow!("the rule has a stored body"))?;
        assert_eq!(format_of(&stored), Some(5));
        Ok(())
    }

    /// The identities this release gives a small fixed set of
    /// definitions, pinned beside [`IDENTITY_GENERATION`].
    ///
    /// This test fails when any of these identities moves, and that is
    /// its purpose: an identity is a value embedders write into their
    /// own facts, and they rewrite those facts only when the generation
    /// tells them identities moved. Whoever moves one bumps
    /// [`IDENTITY_GENERATION`], freezes the generation's functions
    /// (`attribute_uri_v2`, `concept_identity_v2`, `rule_identity_v2`)
    /// as copies of what they compute now, adds the next generation's,
    /// and re-pins the literals here with the new generation's values.
    /// The literals of
    /// `it_reproduces_generation_2` stay as they are.
    #[dialog_common::test]
    fn the_identities_this_release_computes_are_its_generation() -> anyhow::Result<()> {
        let rule = |entity: Option<Entity>| entity.map(|entity| entity.to_string());
        let computed = (
            IDENTITY_GENERATION,
            [typed()?.to_uri(), listed()?.to_uri(), many()?.to_uri()],
            ranked()?.this().to_string(),
            [
                rule(deductive()?.try_this()),
                rule(patterned()?.try_this()),
                rule(inductive()?.try_this()),
            ],
        );
        assert_eq!(
            computed,
            (
                2,
                [
                    "the:HBsFHK76KD2Rt2XuVBew3QrgqGJvmK9E9A73bVNwgLSK".to_string(),
                    "the:3E56VoC3GTvVUmEMjhKBDdFbvHdd3CGtHYA7XTNfzgvm".to_string(),
                    "the:7LQg9vTrFGK44F4XM9Z4g9WEvyr9ZgSiGmZstTHzD5QY".to_string(),
                ],
                "concept:AfWxzEqSu1uz9W9JsB6m2QXrQP2QeVo62SiyFRZsYVfh".to_string(),
                [
                    Some("rule:AQgwySYGS7aLmPTjKiBc1Cm4r6aWH6KpKWSuu1aXtoQR".to_string()),
                    Some("rule:4NfJKBSbBV3VS98HiZUaKERC1TfBxgjHcwsGx9BBY13d".to_string()),
                    Some("rule:FcEXWigaQW2Wq7nbDPX2SqbbifXN3sC9gCn2kMSoqojk".to_string()),
                ],
            )
        );
        Ok(())
    }

    /// The generation 2 functions compute what this release computed
    /// when generation 2 was current. These literals never change: when
    /// the generation moves past 2, the functions become copies, and
    /// this test is what shows the copies are faithful.
    #[dialog_common::test]
    fn it_reproduces_generation_2() -> anyhow::Result<()> {
        let rule = |entity: Option<Entity>| entity.map(|entity| entity.to_string());
        assert_eq!(
            [
                attribute_uri_v2(&typed()?),
                attribute_uri_v2(&listed()?),
                attribute_uri_v2(&many()?),
                concept_identity_v2(&ranked()?).to_string(),
            ],
            [
                "the:HBsFHK76KD2Rt2XuVBew3QrgqGJvmK9E9A73bVNwgLSK",
                "the:3E56VoC3GTvVUmEMjhKBDdFbvHdd3CGtHYA7XTNfzgvm",
                "the:7LQg9vTrFGK44F4XM9Z4g9WEvyr9ZgSiGmZstTHzD5QY",
                "concept:AfWxzEqSu1uz9W9JsB6m2QXrQP2QeVo62SiyFRZsYVfh"
            ]
        );
        assert_eq!(
            [
                rule(rule_identity_v2(&deductive()?)),
                rule(rule_identity_v2(&patterned()?)),
                rule(rule_identity_v2(&inductive()?)),
            ],
            [
                Some("rule:AQgwySYGS7aLmPTjKiBc1Cm4r6aWH6KpKWSuu1aXtoQR".to_string()),
                Some("rule:4NfJKBSbBV3VS98HiZUaKERC1TfBxgjHcwsGx9BBY13d".to_string()),
                Some("rule:FcEXWigaQW2Wq7nbDPX2SqbbifXN3sC9gCn2kMSoqojk".to_string()),
            ]
        );
        Ok(())
    }

    /// Generation 0 reproduces what 0.1.0 computed. The literals were
    /// captured by running 0.1.0 itself (`main` at 120fba8, the commit
    /// `v0.1.0` tags), in `dialog-repository`'s
    /// `branch/transaction/fixtures/main-120fba8.json`.
    #[dialog_common::test]
    fn it_reproduces_generation_0() -> anyhow::Result<()> {
        let salary = AttributeDescriptor::new(
            "org/salary".parse()?,
            "",
            Cardinality::One,
            Some(Type::UnsignedInt),
        );
        let tag = AttributeDescriptor::new(
            "org/tag".parse()?,
            "",
            Cardinality::Many,
            Some(Type::String),
        );
        let concept =
            ConceptDescriptor::try_from(vec![("salary", salary.clone()), ("tag", tag.clone())])?;
        let stage: ConceptDescriptor = serde_json::from_value(json!({
            "with": { "target": { "the": "cmd.stage/target", "as": "entity:" } }
        }))?;
        assert_eq!(
            [
                attribute_uri_v0(&salary),
                attribute_uri_v0(&tag),
                concept_identity_v0(&concept).to_string(),
                concept_identity_v0(&stage).to_string(),
            ],
            [
                "the:4vXu9fi3HmrqRfUFVbhC6qkDdwQvq8qnvVBZi5KAWJp9",
                "the:56uocbCn7AYhvCCXvCN8MmDhskKw3LgfWNibieftXGYb",
                "concept:2g8y9UKMRgkk2yS2CFEoJgdXanPsjNh6AF2Usj8X5q9L",
                "concept:4K2b59UUCyizSXPK8xG2CQKtNh1KPAyGgBLMMDzJ8fNn",
            ]
        );
        Ok(())
    }

    /// Generation 1 of an attribute read under a ranking pick, by itself
    /// and through a concept, reproduces what 0.3.0 to 0.5.0 computed.
    /// The literals were computed by running `v0.4.0` itself, from a
    /// checkout of the tag: each attribute's `to_uri` and the concept's
    /// `this`, spelled as that release spelled them (`"pick": "max:"`,
    /// and a listed `as` with and without `"pick": "top:"`, which that
    /// release gave the same identity).
    #[dialog_common::test]
    fn it_reproduces_generation_1_of_a_picked_attribute() -> anyhow::Result<()> {
        let salary = typed()?;
        let state = listed()?;
        assert_eq!(
            [
                picked_attribute_uri_v1(&salary, Selective::Max),
                picked_attribute_uri_v1(&salary, Selective::Min),
                picked_attribute_uri_v1(&state, Selective::Top),
                picked_attribute_uri_v1(&salary, Selective::Last),
                concept_identity_v1(&ranked()?).to_string(),
            ],
            [
                "the:GdhnhUC8vqcN6jgYXwBhAebDtzBBQsvQ7R3QdBfaA7CB",
                "the:4ZVHsz87WH17X3Yr8B9jQctaGhCXfq2AzvTkZVyFmD36",
                "the:FYFb2UREQTpeY2qnqP1PHVrR75NVZfqs5pMvjcDdbKdc",
                "the:Hu7Btoaga8ptcLFdiPX7TAxq93rr3QHAvrrdvCX8M5Cg",
                "concept:7SNRWmoYagXg9mztxQoHspVN9J9S35vuQE8pfRgtx3KS",
            ]
        );
        Ok(())
    }

    /// Generation 1 reproduces what 0.3.0 to 0.5.0 computed. The literals
    /// were captured by running `v0.4.0` itself, in `dialog-repository`'s
    /// `branch/transaction/fixtures/v0.4.0.json`; 0.3.0 and 0.5.0 compute
    /// identities with the same code.
    #[dialog_common::test]
    fn it_reproduces_generation_1() -> anyhow::Result<()> {
        let salary = typed()?;
        let tag = many()?;
        let state: AttributeDescriptor =
            serde_json::from_value(json!({ "the": "job/state", "as": ["todo", "done"] }))?;
        let concept =
            ConceptDescriptor::try_from(vec![("salary", salary.clone()), ("tag", tag.clone())])?;
        let stage: ConceptDescriptor = serde_json::from_value(json!({
            "with": { "target": { "the": "cmd.stage/target", "as": "entity:" } }
        }))?;
        assert_eq!(
            [
                attribute_uri_v1(&salary),
                attribute_uri_v1(&tag),
                attribute_uri_v1(&state),
                concept_identity_v1(&concept).to_string(),
                concept_identity_v1(&stage).to_string(),
            ],
            [
                "the:Hu7Btoaga8ptcLFdiPX7TAxq93rr3QHAvrrdvCX8M5Cg",
                "the:zaHKHF2rjvpfqe36Hoxo1FzZmmAUNEmHV7h7S8KBJLm",
                "the:9eBg5KNN4WYEzf4AdhZ2gb1WwkYKyHaiiyGpJuvPoejZ",
                "concept:79tvwGhcqBHFdKXodbs4nKiJf6gCFppjd5S7ZNfbkNHh",
                "concept:CDJ5veQ44f7VanfyugnyzV4gZtvHXevEvKQgDWU5Gdpx",
            ]
        );
        Ok(())
    }

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
