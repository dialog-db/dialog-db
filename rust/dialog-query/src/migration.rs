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

use std::collections::BTreeMap;
use std::ops::Not;

use base58::ToBase58;
use dialog_corpus::Entity;
use dialog_corpus::identity::{ENTITY, SYMBOL};
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
