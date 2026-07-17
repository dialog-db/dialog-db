//! The component schema a [`Key`](crate::Key) exposes so leaf nodes can be
//! stored columnar.
//!
//! A key is a fixed, ordered sequence of typed components (for the dialog
//! artifact key: index type, entity, namespace, name, value type, value).
//! Recognizing those components lets a leaf store each one in the column that
//! fits it, independent of the sort order the tree's ordering imposes:
//!
//! - [`Column::Arena`] components (large, mostly-distinct byte strings like an
//!   entity or value) live in a per-leaf byte arena, addressed by slice.
//! - [`Column::Dictionary`] components (small, highly-repeated values like a
//!   namespace, a predicate name, or a one-byte value-type tag) live in a
//!   per-leaf sorted table of the distinct values, referenced by index. The
//!   table is derived purely from leaf content, so it is canonical and
//!   history-independent; the same value recurring across many keys, even
//!   non-adjacently, is stored once per leaf.
//!
//! Comparison walks components in schema order and compares each component's
//! bytes, which is exactly a comparison of the concatenated key, so routing
//! and range bounds are unchanged. The schema's components must therefore be
//! laid out in the same order they contribute to the key's byte comparison.

/// How a leaf column stores a given key component.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Column {
    /// Store the raw bytes contiguously in a per-leaf arena, addressed by a
    /// length column. Best for large, mostly-distinct components (entity,
    /// value reference); adjacent duplicates still front-code within the
    /// arena.
    Arena,
    /// Intern into a per-leaf sorted dictionary of distinct values and store
    /// an index per key. Best for small, highly repeated components
    /// (namespace, name, value type) whose repetition is non-adjacent.
    Dictionary,
}

/// One component of a key: its storage class and, for a fixed-width
/// component, its width. Variable-width components report `width == None`
/// and are length-prefixed in their column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Component {
    /// How this component is stored in a leaf.
    pub column: Column,
    /// The component's fixed byte width, or `None` if it is variable-width.
    /// A fixed width lets the codec split a key into components without a
    /// separate length column for that component.
    pub width: Option<usize>,
}

impl Component {
    /// A fixed-width arena component (e.g. a 32-byte value reference).
    pub const fn arena(width: usize) -> Self {
        Self {
            column: Column::Arena,
            width: Some(width),
        }
    }

    /// A variable-width arena component.
    pub const fn arena_var() -> Self {
        Self {
            column: Column::Arena,
            width: None,
        }
    }

    /// A fixed-width dictionary component (e.g. a 1-byte value-type tag).
    pub const fn dictionary(width: usize) -> Self {
        Self {
            column: Column::Dictionary,
            width: Some(width),
        }
    }

    /// A variable-width dictionary component (e.g. a predicate name).
    pub const fn dictionary_var() -> Self {
        Self {
            column: Column::Dictionary,
            width: None,
        }
    }
}

/// The component layout of a key type: an ordered list of [`Component`]s
/// whose concatenation, in order, is the key's comparison bytes.
///
/// A key type that does not decompose (the fallback `[u8; N]`) reports a
/// single variable-width arena component covering the whole key, which makes
/// the columnar leaf degrade gracefully to a single-arena front-coded leaf.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Schema {
    components: &'static [Component],
}

impl Schema {
    /// Builds a schema from a static component list.
    pub const fn new(components: &'static [Component]) -> Self {
        Self { components }
    }

    /// The whole-key single-arena schema, used by key types that do not
    /// expose a finer component structure.
    pub const fn opaque() -> Self {
        const OPAQUE: &[Component] = &[Component::arena_var()];
        Self::new(OPAQUE)
    }

    /// The components, in the order they contribute to key comparison.
    pub fn components(&self) -> &'static [Component] {
        self.components
    }

    /// The number of components in a key of this schema.
    pub fn len(&self) -> usize {
        self.components.len()
    }

    /// Whether the schema has no components (never true for a valid key
    /// type; a key always has at least the opaque whole-key component).
    pub fn is_empty(&self) -> bool {
        self.components.is_empty()
    }

    /// The number of fixed-width components at the front of the schema, and
    /// the total fixed width they occupy. A component split can place these
    /// without a length column; the first variable-width component onward
    /// needs lengths. Used by the codec to size its length columns.
    pub fn fixed_prefix(&self) -> (usize, usize) {
        let mut count = 0;
        let mut width = 0;
        for component in self.components {
            match component.width {
                Some(w) => {
                    count += 1;
                    width += w;
                }
                None => break,
            }
        }
        (count, width)
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;

    use super::{Column, Component, Schema};
    use crate::Key;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// The default `[u8; N]` key reports one opaque whole-key arena component,
    /// and its `components` split reproduces the key bytes.
    #[dialog_common::test]
    async fn it_defaults_to_a_single_opaque_component() -> Result<()> {
        let schema = <[u8; 8] as Key>::schema();
        assert_eq!(schema.len(), 1);
        assert_eq!(schema.components()[0].column, Column::Arena);
        assert_eq!(schema.components()[0].width, None);

        let key: [u8; 8] = *b"abcdefgh";
        let mut parts = Vec::new();
        key.components(&mut parts);
        assert_eq!(parts, vec![key.as_slice()]);
        assert_eq!(parts.concat(), key.to_vec());
        Ok(())
    }

    /// `fixed_prefix` counts the leading fixed-width components and their
    /// total width, stopping at the first variable-width component.
    #[dialog_common::test]
    async fn it_reports_the_fixed_width_prefix() -> Result<()> {
        const PARTS: &[Component] = &[
            Component::dictionary(1),
            Component::arena(32),
            Component::dictionary_var(),
            Component::arena(32),
        ];
        let schema = Schema::new(PARTS);
        assert_eq!(schema.fixed_prefix(), (2, 33));

        let schema = Schema::opaque();
        assert_eq!(schema.fixed_prefix(), (0, 0));
        Ok(())
    }
}
