//! The variable a scan's output stream is sorted on.
//!
//! Every fact scan reads one of the three index orderings (EAV, AEV, VAE) and
//! yields entries in that index's key order. Which ordering it reads is decided
//! by which term positions are constrained: entity wins, then value, then
//! attribute (see `dialog-artifacts` `selector_range`). The chosen ordering
//! sorts primarily by its first component, then its second, then its third; a
//! component that is already bound to a constant is fixed across the whole
//! scan, so the stream is effectively sorted on the first component that is
//! still a *variable*.
//!
//! [`SortOrder`] names that variable. It is the physical property a merge join
//! needs: two scans can be merge-joined on a variable only if both yield their
//! output sorted on it. Nothing consumes it yet; it is surfaced so the planner
//! can, without changing how any scan runs today.

use crate::Term;
use crate::attribute::query::all::AttributeQueryAll;
use crate::types::Typed;

/// The variable a scan's output is sorted on, or that it carries no useful
/// order (a fully-constrained point lookup, or an ordering led by a variable
/// the caller did not ask about).
///
/// This describes the *leading free dimension* of the index the scan reads: the
/// first of the ordering's components that is a variable rather than a bound
/// constant. A scan whose leading components are all bound to constants returns
/// at most the facts sharing those constants, in the order of whatever
/// component comes next; when that next component is also bound (a full point
/// lookup) there is no meaningful sort variable and the result is
/// [`None`](SortOrder::None).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortOrder {
    /// Output is sorted on the named variable, ascending by its key encoding.
    On(String),
    /// Output carries no join-useful order: a point lookup, or an ordering
    /// whose leading free dimension is not a variable name a caller can join
    /// on.
    None,
}

impl SortOrder {
    /// The variable name this order sorts on, if any.
    pub fn variable(&self) -> Option<&str> {
        match self {
            SortOrder::On(name) => Some(name),
            SortOrder::None => None,
        }
    }

    /// Whether two orders sort on the same variable, so streams carrying them
    /// can be merged without an intermediate sort. Two [`None`] orders do not
    /// match: neither carries a join key.
    pub fn merges_with(&self, other: &SortOrder) -> bool {
        match (self, other) {
            (SortOrder::On(a), SortOrder::On(b)) => a == b,
            _ => false,
        }
    }
}

/// The variable name a term will bind, if it is a named unbound variable.
///
/// A constant contributes no sort variable (it fixes its component). An
/// anonymous variable (`name: None`) carries no name to join on, so it is
/// treated the same as a constant here: it names no sort dimension. A variable
/// already bound upstream will have been resolved to a constant before
/// evaluation, so only a still-free named variable names a sort dimension.
fn free_variable<T: Typed>(term: &Term<T>) -> Option<String> {
    match term {
        Term::Variable { name, .. } => name.clone(),
        Term::Constant(_) => None,
    }
}

impl AttributeQueryAll {
    /// The variable this scan's output stream is sorted on.
    ///
    /// Mirrors the index-priority choice the storage layer makes from the same
    /// bound/unbound pattern (entity, then value, then attribute), and reports
    /// the leading free component of the chosen ordering.
    ///
    /// - **EAV** (entity constrained, or nothing is): sorted on entity, then
    ///   attribute, then value. The leading free dimension is entity if entity
    ///   is a variable, else attribute, else value.
    /// - **VAE** (value constrained, entity not): sorted on value, then
    ///   attribute, then entity.
    /// - **AEV** (only attribute constrained): sorted on attribute, then
    ///   entity, then value.
    ///
    /// Returns [`SortOrder::None`] when the leading dimensions are all bound to
    /// constants (a point lookup carries no join-useful order).
    pub fn sort_order(&self) -> SortOrder {
        let entity_bound = matches!(self.of(), Term::Constant(_));
        let value_bound = matches!(self.is(), Term::Constant(_));
        let attribute_bound = matches!(self.the(), Term::Constant(_));

        // The component sequence of the ordering the storage layer will pick,
        // most significant first. This is the exact priority `selector_range`
        // applies: entity index unless entity is free and something else is
        // bound, then value, then attribute.
        let sequence: [Option<String>; 3] = if entity_bound || (!value_bound && !attribute_bound) {
            // EAV: entity, attribute, value.
            [
                free_variable(self.of()),
                free_variable(self.the()),
                free_variable(self.is()),
            ]
        } else if value_bound {
            // VAE: value, attribute, entity.
            [
                free_variable(self.is()),
                free_variable(self.the()),
                free_variable(self.of()),
            ]
        } else {
            // AEV: attribute, entity, value.
            [
                free_variable(self.the()),
                free_variable(self.of()),
                free_variable(self.is()),
            ]
        };

        // The leading free dimension is the first sequence entry that is a
        // variable. Bound leading components are fixed across the scan, so the
        // order is decided by the first free one.
        match sequence.into_iter().flatten().next() {
            Some(name) => SortOrder::On(name),
            None => SortOrder::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SortOrder;
    use crate::attribute::The;
    use crate::attribute::query::all::AttributeQueryAll;
    use crate::types::{Any, Typed};
    use crate::{Entity, Term, Value};

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// A constant term for a given position. The concrete value is irrelevant
    /// to `sort_order`, which only distinguishes constant from variable.
    fn constant<T: Typed>() -> Term<T> {
        Term::Constant(Value::String("k".to_string()))
    }

    /// Build a scan; `cause` and `source` are always anonymous, which they
    /// carry no join order for.
    fn scan(the: Term<The>, of: Term<Entity>, is: Term<Any>) -> AttributeQueryAll {
        AttributeQueryAll::new(the, of, is, Term::var("cause"))
    }

    #[dialog_common::test]
    fn it_sorts_on_entity_when_entity_leads_the_eav_scan() {
        // Nothing bound: EAV scan, leading free dimension is entity.
        let query = scan(Term::var("the"), Term::var("of"), Term::var("is"));
        assert_eq!(query.sort_order(), SortOrder::On("of".to_string()));
    }

    #[dialog_common::test]
    fn it_sorts_on_attribute_when_entity_is_bound_in_eav() {
        // Entity bound: EAV scan, entity is fixed, so the order is attribute.
        let query = scan(Term::var("the"), constant(), Term::var("is"));
        assert_eq!(query.sort_order(), SortOrder::On("the".to_string()));
    }

    #[dialog_common::test]
    fn it_sorts_on_attribute_when_value_is_bound_choosing_vae() {
        // Value bound, entity free: VAE scan, value fixed, order is attribute.
        let query = scan(Term::var("the"), Term::var("of"), constant());
        assert_eq!(query.sort_order(), SortOrder::On("the".to_string()));
    }

    #[dialog_common::test]
    fn it_sorts_on_entity_when_only_attribute_is_bound_choosing_aev() {
        // Only attribute bound: AEV scan, attribute fixed, order is entity.
        let query = scan(constant(), Term::var("of"), Term::var("is"));
        assert_eq!(query.sort_order(), SortOrder::On("of".to_string()));
    }

    #[dialog_common::test]
    fn it_falls_through_bound_leading_components_to_the_first_free_one() {
        // Entity and attribute both bound: EAV scan led by two constants, so
        // the only free dimension is value.
        let query = scan(constant(), constant(), Term::var("is"));
        assert_eq!(query.sort_order(), SortOrder::On("is".to_string()));
    }

    #[dialog_common::test]
    fn it_reports_no_order_for_a_full_point_lookup() {
        // All three bound: a full point lookup, no free dimension.
        let query = scan(constant(), constant(), constant());
        assert_eq!(query.sort_order(), SortOrder::None);
    }

    #[dialog_common::test]
    fn it_treats_an_anonymous_variable_as_carrying_no_order() {
        // An anonymous `is` variable names nothing to join on, so with entity
        // and attribute bound the scan carries no useful order.
        let query = scan(constant(), constant(), Term::blank());
        assert_eq!(query.sort_order(), SortOrder::None);
    }

    #[dialog_common::test]
    fn it_merges_only_matching_variables() {
        assert!(SortOrder::On("x".to_string()).merges_with(&SortOrder::On("x".to_string())));
        assert!(!SortOrder::On("x".to_string()).merges_with(&SortOrder::On("y".to_string())));
        assert!(!SortOrder::None.merges_with(&SortOrder::None));
        assert!(!SortOrder::On("x".to_string()).merges_with(&SortOrder::None));
    }
}
