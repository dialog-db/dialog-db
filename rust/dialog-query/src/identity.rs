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

/// The references of the field names definitions use, kept as constants so
/// that referencing a definition hashes no name.
pub(crate) mod field {
    use merkle_reference::Reference;

    /// `the`: the relations an attribute reads.
    pub(crate) const THE: Reference =
        Reference::from_hex("540fd5e33f04a1d60ec6428d1944683def720363d734c20424e5ebbcb4b3362b");
    /// `as`: the type an attribute declares.
    pub(crate) const AS: Reference =
        Reference::from_hex("dffdf7dfe887b2a93ce763fce1b75ccc1923a6b402ccf60b5af3f2f010e8663b");
    /// `pick`: an attribute's pick.
    pub(crate) const PICK: Reference =
        Reference::from_hex("eff250c4135679f868bdb94b6f43b7c95243b6a65e13bd84f47322404ebe2376");
    /// `among`: the values an attribute lists.
    pub(crate) const AMONG: Reference =
        Reference::from_hex("08161282b1b26db65a582550106ac655b24a87d8172e3e27ca29c91b0695553b");
    /// `optional`: a concept field that may be absent.
    pub(crate) const OPTIONAL: Reference =
        Reference::from_hex("e750b2ac25dc2861ba3333eaca6c19543f3a6a115d6d63163f3bba6ba7d3f684");
    /// `conforms`: the concept a field's value conforms to.
    pub(crate) const CONFORMS: Reference =
        Reference::from_hex("d784b699c5a4972e135e20506b90742e9d0d78aeae49ab286a19eaf12189cc85");
    /// The boolean `true`.
    pub(crate) const TRUE: Reference =
        Reference::from_hex("d24b4fe8353c1bc4a7ef19901d3481e63b31c90b37eb2476ef4b17d479446f4c");
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
    use merkle_reference::Refer;

    #[dialog_common::test]
    fn it_names_the_definition_types() {
        assert_eq!(Type::named("the:"), ATTRIBUTE);
        assert_eq!(Type::named("concept:"), CONCEPT);
        assert_eq!(Type::named("rule:"), RULE);
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
            assert_eq!(name.refer(), reference, "{name}");
        }
        assert_eq!(true.refer(), field::TRUE);
    }

    #[dialog_common::test]
    fn it_reads_back_the_entity_it_names() {
        let reference = "x".refer();
        let entity = entity("concept:", reference);
        assert_eq!(parse("concept:", entity.as_str()), Some(reference));
        assert_eq!(parse("the:", entity.as_str()), None);
    }
}
