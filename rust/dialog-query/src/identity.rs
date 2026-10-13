//! The identities of definitions: [merkle references](merkle_reference)
//! under dialog's types.
//!
//! Every definition is a struct, referenced as a map from field names to
//! values under the definition's own type, with an absent optional field left
//! out:
//!
//! - an attribute, under [`ATTRIBUTE`], maps `the` to the relations it reads
//!   (a list of symbols, best first), `as` to the entity naming its type,
//!   `pick` to the entity naming its pick and `among` to the values it lists;
//! - a concept, under [`CONCEPT`], maps each of its attributes, by the
//!   attribute's reference, to the field's options: `optional` when the field
//!   is optional and `conforms` to the reference of the concept its value must
//!   conform to.
//!
//! A definition's reference depends only on its parts' references, so a
//! concept is the same whether its attributes are embedded in it or linked
//! from it. Descriptions and field names are not part of any identity.
//!
//! A definition's entity is its type's name followed by its reference in
//! base58: `the:…`, `concept:…`.

use base58::ToBase58;
use merkle_reference::{Reference, Type};

use crate::artifact::Entity;

/// The type of attributes.
pub const ATTRIBUTE: Type = Type::from_reference(Reference::from_hex(
    "8654cfb1b79ccef0702baefb05368defe2aeda8aef53fe7dbafdac0ba215d2bd",
));
/// The type of concepts.
pub const CONCEPT: Type = Type::from_reference(Reference::from_hex(
    "83c8d8d08d8b9071503ed30243ed2e9cf60fdcdbb98367b8f4ea46702094c292",
));
/// The type of rules.
pub const RULE: Type = Type::from_reference(Reference::from_hex(
    "e7cea1a633767195582d0a3f84a5f2c5f3afd96b5c694dd647e8db1dd8cb5967",
));
/// The type of the absent value a wire form writes as unit or `None`.
pub const NULL: Type = Type::from_reference(Reference::from_hex(
    "345b62212fe549f6acc9e6de9270715fc98bb3d82fd2c0e1291bd84d569208ae",
));

/// The references of the field names definitions use, as `text:`, kept as
/// constants so that referencing a definition hashes no name.
pub(crate) mod field {
    use merkle_reference::Reference;

    /// `the`: the relations an attribute reads.
    pub(crate) const THE: Reference =
        Reference::from_hex("2314945186ce4ec32ab316d47bda2fcbe07c828d74e5c6bbb64f2bf4b75af4e2");
    /// `as`: the type an attribute declares.
    pub(crate) const AS: Reference =
        Reference::from_hex("2240668c6db6dabfff3e400bbc8376224209b067a2c991917ffde774797181cc");
    /// `pick`: an attribute's pick.
    pub(crate) const PICK: Reference =
        Reference::from_hex("0f0d6b6545c99929aac5463ab8ad362a84bb0ada084aa06500a45ceafcb5d00f");
    /// `among`: the values an attribute lists.
    pub(crate) const AMONG: Reference =
        Reference::from_hex("d1e96125c25fead1a441f04f681c25cb095b08748fbabaf12b044ae4c0f65099");
    /// `optional`: a concept field that may be absent.
    pub(crate) const OPTIONAL: Reference =
        Reference::from_hex("01eef36ab3d2ace67169e6676162fcfe9e313b226dcb4b7b892ff75fe99b5524");
    /// `conforms`: the concept a field's value conforms to.
    pub(crate) const CONFORMS: Reference =
        Reference::from_hex("d6199f32e1f4a2425c343376f9c6eed70104bb908225d3ff7aedaa3b180bf829");
    /// The boolean `true`.
    pub(crate) const TRUE: Reference =
        Reference::from_hex("e3cb54e243b031b0c67199ce21f399b77d32eebb1e598b5390acff770561a829");
}

/// The entity `scheme` names a definition by: `scheme` followed by the
/// definition's reference in base58.
pub(crate) fn entity(scheme: &str, reference: Reference) -> Entity {
    format!("{scheme}{}", reference.as_bytes().to_base58())
        .parse()
        .expect("a scheme and base58 digits make a valid entity URI")
}

/// The reference an entity of `scheme` names, if it is one.
pub(crate) fn parse(scheme: &str, uri: &str) -> Option<Reference> {
    let encoded = uri.strip_prefix(scheme)?;
    let bytes = base58::FromBase58::from_base58(encoded).ok()?;
    let bytes: [u8; 32] = bytes.try_into().ok()?;
    Some(Reference::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_artifacts::identity::{BOOLEAN, TEXT};

    #[dialog_common::test]
    fn it_names_the_definition_types() {
        assert_eq!(Type::named("the:"), Ok(ATTRIBUTE));
        assert_eq!(Type::named("concept:"), Ok(CONCEPT));
        assert_eq!(Type::named("rule:"), Ok(RULE));
        assert_eq!(Type::named("null:"), Ok(NULL));
    }

    #[dialog_common::test]
    fn it_keeps_the_field_names_references() {
        for (name, reference) in [
            ("the", field::THE),
            ("as", field::AS),
            ("pick", field::PICK),
            ("among", field::AMONG),
            ("optional", field::OPTIONAL),
            ("conforms", field::CONFORMS),
        ] {
            assert_eq!(TEXT.reference_of(name.as_bytes()), reference, "{name}");
        }
        assert_eq!(BOOLEAN.reference_of(&[1]), field::TRUE);
    }

    #[dialog_common::test]
    fn it_reads_back_the_entity_it_names() {
        let reference = TEXT.reference_of(b"x");
        let entity = entity("concept:", reference);
        assert_eq!(parse("concept:", entity.as_str()), Some(reference));
        assert_eq!(parse("the:", entity.as_str()), None);
    }
}
