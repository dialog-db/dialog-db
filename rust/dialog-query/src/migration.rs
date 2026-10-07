//! The identities the release before select policies gave attributes
//! and concepts, for migrating what an earlier release stored under
//! them.
//!
//! That release hashed an attribute as `{domain, name, cardinality,
//! type}`. This one hashes the relations it ranks, the policy and the
//! ranked values too (see
//! [`AttributeDescriptor::to_cbor_bytes`](crate::AttributeDescriptor::to_cbor_bytes)),
//! so every attribute's identity changed, and every concept's with it,
//! a concept's identity being a hash over its attributes'. Nothing on a
//! read path uses these functions: a migration does, to find a fact an
//! earlier release keyed by an old identity and key it by the current
//! one. dialog's own such fact is the transient marker, which
//! `Branch::upgrade_rules` moves; an application that keyed its own
//! facts by a concept or attribute identity moves those with these.

use std::collections::BTreeMap;
use std::ops::Not;

use base58::ToBase58;
use dialog_artifacts::Entity;
use serde::Serialize;

use crate::attribute::{AttributeDescriptor, Keyed, Relation};
use crate::concept::descriptor::ConceptDescriptor;
use crate::{Cardinality, Type};

/// The identity the earlier release gave `attribute`, as its
/// `the:<base58>` URI.
pub fn attribute_uri_v0(attribute: &AttributeDescriptor) -> String {
    #[derive(Serialize)]
    struct CborAttributeDescriptor<'a> {
        domain: &'a str,
        name: &'a str,
        cardinality: Cardinality,
        #[serde(rename = "type")]
        content_type: Option<Type>,
    }
    let name = match attribute.the() {
        Relation::Attribute(the) => the.name(),
        Relation::Collection { keyed, .. } => match keyed {
            Keyed::Dictionary => "<dictionary>",
            Keyed::Sequence => "<sequence>",
        },
    };
    let bytes = serde_ipld_dagcbor::to_vec(&CborAttributeDescriptor {
        domain: attribute.domain(),
        name,
        cardinality: attribute.cardinality(),
        content_type: attribute.content_type(),
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
