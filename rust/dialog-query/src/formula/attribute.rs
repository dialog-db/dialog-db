//! Attribute decomposition: `dialog/attribute-parts` splits an
//! attribute into its domain and name halves.
//!
//! This is how a keyed collection's key reaches an author. The
//! collection's scan binds the whole attribute (`todo.list/N5`) to an
//! internal variable; this formula projects the name half (`N5`) onto
//! the field's key operand, so `{?key: ?value}` binds the key as a
//! plain text term that joins, filters, and feeds `dialog/position`
//! like any other. Unlike [`PositionParts`](super::position::PositionParts)
//! it admits both name shapes, because a dictionary's keys are
//! symbols and a sequence's are positions.

use dialog_artifacts::{Attribute, Name};

use crate::Formula;
use crate::formula::Input;

/// Split an attribute into `domain` and `name`. Registered as
/// `dialog/attribute-parts`.
#[derive(Debug, Clone, Formula)]
pub struct AttributeParts {
    /// The attribute, as bound by a scan's `the` slot.
    pub of: Attribute,
    /// The domain half.
    #[output]
    pub domain: String,
    /// The name half: a symbol for a named predicate, a position for
    /// an ordered member.
    #[output]
    pub name: String,
}

impl AttributeParts {
    /// One row per attribute; an attribute that does not split (which
    /// a stored one never is) projects nothing.
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        match input.of.split() {
            Ok((domain, name)) => {
                let name = match name {
                    Name::Symbol(symbol) => symbol.as_str().to_string(),
                    Name::Position(position) => position.as_str().to_string(),
                };
                vec![AttributeParts {
                    of: input.of.clone(),
                    domain: domain.to_string(),
                    name,
                }]
            }
            Err(_) => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    fn parts(attribute: &str) -> Vec<AttributeParts> {
        AttributeParts::compute(AttributePartsInput {
            of: attribute.parse().expect("attribute parses"),
        })
    }

    /// A named predicate splits into its domain and symbol name.
    #[dialog_common::test]
    fn it_splits_a_symbol_name() {
        let rows = parts("todo.list/title");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].domain, "todo.list");
        assert_eq!(rows[0].name, "title");
    }

    /// An ordered member splits into its domain and position, the
    /// same slot a symbol name lands in.
    #[dialog_common::test]
    fn it_splits_a_position_name() {
        let rows = parts("todo.list/N5");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].domain, "todo.list");
        assert_eq!(rows[0].name, "N5");
    }
}
