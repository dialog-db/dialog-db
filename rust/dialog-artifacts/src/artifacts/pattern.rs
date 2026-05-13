//! [`AttributePattern`] is a query pattern matching either a specific
//! attribute or every attribute under a domain prefix.

use crate::{Attribute, Symbol};

/// A pattern over the attribute slot of an artifact.
///
/// - [`AttributePattern::Domain`] matches every attribute whose domain
///   half equals the given [`Symbol`], regardless of name. Compiled by
///   the selector to a contiguous prefix scan over the attribute index.
/// - [`AttributePattern::Exact`] matches the single attribute equal to
///   the given [`Attribute`]. Compiled to a point lookup.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AttributePattern {
    /// Match every attribute whose domain half is this symbol.
    Domain(Symbol),
    /// Match this specific attribute.
    Exact(Attribute),
}

impl From<Symbol> for AttributePattern {
    fn from(value: Symbol) -> Self {
        AttributePattern::Domain(value)
    }
}

impl From<Attribute> for AttributePattern {
    fn from(value: Attribute) -> Self {
        AttributePattern::Exact(value)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    #[dialog_common::test]
    fn it_constructs_a_domain_pattern_from_symbol() {
        let domain: Symbol = "person".parse().unwrap();
        let pattern: AttributePattern = domain.clone().into();
        assert_eq!(pattern, AttributePattern::Domain(domain));
    }

    #[dialog_common::test]
    fn it_constructs_an_exact_pattern_from_attribute() {
        let attribute: Attribute = "person/name".parse().unwrap();
        let pattern: AttributePattern = attribute.clone().into();
        assert_eq!(pattern, AttributePattern::Exact(attribute));
    }
}
