//! Formulas decomposing index keys into their components.
//!
//! The tree procedures (`tree/node`, `tree/link`, `tree/key`) surface
//! raw key and separator bytes; these formulas make them legible:
//! apply [`KeyPart`] to a full entry key (from `tree/key` or a node's
//! `bound`) or [`SeparatorPart`] to a link separator (from
//! `tree/link`), and get one row per component — its position, kind,
//! human rendering, and raw bytes. Pure per-row computation over the
//! self-describing variable-length key encoding
//! (`dialog_artifacts::key::varkey`) — the legitimate `Formula` kind,
//! exactly like `dialog/revision-parent`.
//!
//! The two stay separate because their contracts differ: a full key
//! parses strictly under its tag's schema (with a single `opaque` row
//! as the never-empty fallback), while a separator is a front-coded
//! *prefix* whose column framing lies past the truncation, so
//! [`SeparatorPart`] is deliberately lenient — the tag component plus
//! the prefix bytes as one component. This mirrors the
//! `key_parts`/`separator_parts` split tonk's inspector proved out.

use dialog_artifacts::inspect::{key_components, separator_components};

use crate::Formula;
use crate::formula::Input;

/// Raw byte payload (`Value::Bytes`) in a form the `Formula` derive
/// can parse in field position (the macro trips on angle brackets).
type Bytes = Vec<u8>;

/// Decompose a full, variable-length index key into components: one
/// row per component, in the key's own sort order.
#[derive(Debug, Clone, Formula)]
pub struct KeyPart {
    /// The raw key bytes — a `tree/key` row's `key`, or a `tree/node`
    /// row's `bound`.
    pub of: Bytes,
    /// Position of the component within the key, from 0.
    #[output]
    pub at: u64,
    /// What the component is: `index`, `entity`, `attribute`,
    /// `vtype`, `value`, `spill`, `origin`, `edition`, `blob`,
    /// `min`, or `opaque`.
    #[output]
    pub kind: String,
    /// Human rendering of the component.
    #[output]
    pub text: String,
    /// The raw component bytes.
    #[output]
    pub bytes: Bytes,
}

impl KeyPart {
    /// One row per decoded component; unparseable input yields a
    /// single `opaque` row, never zero rows for non-empty input.
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        key_components(&input.of)
            .into_iter()
            .enumerate()
            .map(|(at, component)| KeyPart {
                of: input.of.clone(),
                at: at as u64,
                kind: component.kind.into(),
                text: component.text,
                bytes: component.bytes,
            })
            .collect()
    }
}

/// Decompose a link separator (a front-coded key *prefix*) into
/// components: the tag plus the prefix bytes, leniently.
#[derive(Debug, Clone, Formula)]
pub struct SeparatorPart {
    /// The raw separator bytes — a `tree/link` row's `separator`.
    /// Empty is the level's leftmost boundary (−∞).
    pub of: Bytes,
    /// Position of the component within the separator, from 0.
    #[output]
    pub at: u64,
    /// What the component is: `index`, `prefix`, or `min`.
    #[output]
    pub kind: String,
    /// Human rendering of the component.
    #[output]
    pub text: String,
    /// The raw component bytes.
    #[output]
    pub bytes: Bytes,
}

impl SeparatorPart {
    /// One row per component; the empty separator yields the `min`
    /// marker row.
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        separator_components(&input.of)
            .into_iter()
            .enumerate()
            .map(|(at, component)| SeparatorPart {
                of: input.of.clone(),
                at: at as u64,
                kind: component.kind.into(),
                text: component.text,
                bytes: component.bytes,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use dialog_artifacts::{Artifact, Entity, EntityKey, KeyType as _, Value};
    use dialog_search_tree::Manifest;

    use super::*;

    fn fact() -> Artifact {
        Artifact {
            the: "test/name".parse().expect("attribute parses"),
            of: Entity::new().expect("entity mints"),
            is: Value::String("Alice".into()),
            cause: None,
        }
    }

    fn entity_key(fact: &Artifact) -> Vec<u8> {
        EntityKey::from_artifact(fact, &Manifest::default())
            .into_key()
            .bytes()
            .to_vec()
    }

    fn parts(bytes: Vec<u8>) -> Vec<KeyPart> {
        KeyPart::compute(KeyPartInput { of: bytes })
    }

    /// An entity-ordered key decomposes into tag, entity, attribute,
    /// value-type and value components — in sort order, with the
    /// attribute and value legible in `text`.
    #[dialog_common::test]
    fn it_decomposes_an_entity_key() {
        let fact = fact();
        let rows = parts(entity_key(&fact));
        let kinds: Vec<&str> = rows.iter().map(|row| row.kind.as_str()).collect();
        assert_eq!(
            kinds,
            vec!["index", "entity", "attribute", "vtype", "value"],
            "components in sort order: {rows:?}"
        );
        assert!(
            rows.iter().enumerate().all(|(at, row)| row.at == at as u64),
            "positions are sequential"
        );
        assert_eq!(rows[0].text, "entity", "tag names the ordering");
        assert_eq!(rows[2].text, "test/name", "attribute is legible");
        assert_eq!(rows[4].text, "\"Alice\"", "string value quoted: {rows:?}");
    }

    /// Garbage input never yields zero rows: an unknown tag falls back
    /// to a single opaque component.
    #[dialog_common::test]
    fn it_falls_back_to_opaque_for_unknown_tags() {
        let rows = parts(vec![0xEE, 1, 2, 3]);
        assert_eq!(rows.len(), 1, "one opaque row: {rows:?}");
        assert_eq!(rows[0].kind, "opaque");
        assert_eq!(rows[0].bytes, vec![0xEE, 1, 2, 3]);
    }

    fn separator_parts(bytes: Vec<u8>) -> Vec<SeparatorPart> {
        SeparatorPart::compute(SeparatorPartInput { of: bytes })
    }

    /// The empty separator is the −∞ boundary; a non-empty one is the
    /// tag plus a lenient prefix component.
    #[dialog_common::test]
    fn it_decomposes_separators_leniently() {
        let empty = separator_parts(Vec::new());
        assert_eq!(empty.len(), 1);
        assert_eq!(empty[0].kind, "min");

        let fact = fact();
        let mut prefix = entity_key(&fact);
        prefix.truncate(9); // a front-coded prefix, mid-component
        let rows = separator_parts(prefix.clone());
        let kinds: Vec<&str> = rows.iter().map(|row| row.kind.as_str()).collect();
        assert_eq!(kinds, vec!["index", "prefix"], "tag + prefix: {rows:?}");
        assert_eq!(rows[1].bytes, prefix[1..].to_vec());
    }
}
