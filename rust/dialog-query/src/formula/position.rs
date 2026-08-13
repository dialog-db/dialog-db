//! Formulas for ordered relations via fractional positions.
//!
//! Dialog encodes an ordered collection as facts whose attribute
//! carries the position as its predicate — `todo.item/<position>` of
//! the collection entity, valued by the member — so one EAV range scan
//! yields the members already sorted (see
//! `notes/ordered-relations.md` and `dialog_artifacts::position`).
//! These formulas are the query-side surface:
//!
//! - [`Position`] (`dialog/position`) derives the position for a
//!   member inserted after/before/between neighbors — usable in
//!   queries, rules, and transaction-building code alike, and
//!   deterministic: every replica derives the same position for the
//!   same `(member, after, before)`, which is what makes concurrent
//!   identical inserts converge instead of duplicating.
//! - [`PositionParts`] (`dialog/position-parts`) decomposes a
//!   position-bearing attribute into its namespace and position.
//!   Predicates that cannot be positions project nothing, but note
//!   that any alphanumeric word starting with a letter *is* a
//!   syntactically valid position — the namespace prefix, not
//!   position syntax, is what scopes an ordered relation's scan.
//!
//! Both are pure per-row computation — ordinary [`Formula`]s, sound
//! under differential subscriptions with no extra machinery.

use dialog_artifacts::position as fractional;
use dialog_artifacts::position::Bias;

use dialog_artifacts::Attribute;

use crate::Formula;
use crate::artifact::Entity;
use crate::formula::Input;

/// Resolve an optional bound: empty means open, anything else must
/// parse as a position (a malformed bound projects nothing, mirroring
/// the forged-record convention).
fn bound(text: &str) -> Result<Option<fractional::Position>, ()> {
    if text.is_empty() {
        return Ok(None);
    }
    fractional::Position::try_from(text)
        .map(Some)
        .map_err(|_| ())
}

/// Derive the fractional position for `member` inserted between the
/// `after` and `before` positions (empty string = open bound; both
/// open = the first position). Registered as `dialog/position`.
#[derive(Debug, Clone, Formula)]
pub struct Position {
    /// The member being placed; its entity reference biases the
    /// derived position so identical concurrent inserts converge and
    /// distinct ones disperse.
    pub member: Entity,
    /// Position of the neighbor to insert after, or `""` for the head.
    pub after: String,
    /// Position of the neighbor to insert before, or `""` for the
    /// tail.
    pub before: String,
    /// The derived position: use it as the attribute predicate of the
    /// membership fact.
    #[output]
    pub is: String,
}

impl Position {
    /// One row carrying the derived position; malformed bounds or an
    /// exhausted range project nothing.
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        let (Ok(after), Ok(before)) = (bound(&input.after), bound(&input.before)) else {
            return Vec::new();
        };
        let bias = Bias::derive(input.member.to_string().as_bytes());
        use std::ops::Bound;
        let bound = |position: &Option<fractional::Position>| match position {
            Some(position) => Bound::Excluded(position.clone()),
            None => Bound::Unbounded,
        };
        match fractional::insert(&bias, (bound(&after), bound(&before))) {
            Ok(position) => vec![Position {
                member: input.member.clone(),
                after: input.after.clone(),
                before: input.before.clone(),
                is: position.as_str().to_string(),
            }],
            Err(_) => Vec::new(),
        }
    }
}

/// Decompose a position-bearing attribute (`namespace/<position>`)
/// into its parts. A predicate that cannot be a position (illegal
/// characters, wrong leading byte) projects nothing; scope scans by
/// the namespace prefix, since ordinary word predicates can be
/// syntactically valid positions too. Registered as
/// `dialog/position-parts`.
#[derive(Debug, Clone, Formula)]
pub struct PositionParts {
    /// The attribute, as bound by a scan's `the` slot.
    pub of: Attribute,
    /// The attribute's namespace (the ordered relation's name).
    #[output]
    pub namespace: String,
    /// The predicate, when it is a valid fractional position — the
    /// member's sort key.
    #[output]
    pub position: String,
}

impl PositionParts {
    /// One row for a position-bearing attribute; anything else
    /// projects nothing.
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        let text = input.of.as_str();
        let Some((namespace, predicate)) = text.split_once('/') else {
            return Vec::new();
        };
        if fractional::Position::try_from(predicate).is_err() {
            return Vec::new();
        }
        vec![PositionParts {
            of: input.of.clone(),
            namespace: namespace.to_string(),
            position: predicate.to_string(),
        }]
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    fn member(seed: &str) -> Entity {
        format!("test:{seed}").parse().expect("entity parses")
    }

    fn derive(member_entity: &Entity, after: &str, before: &str) -> Option<String> {
        Position::compute(PositionInput {
            member: member_entity.clone(),
            after: after.into(),
            before: before.into(),
        })
        .pop()
        .map(|row| row.is)
    }

    /// Chained inserts produce byte-ordered positions: append after
    /// the last, prepend before the first, wedge in between.
    #[dialog_common::test]
    fn it_derives_ordered_positions() {
        let first = derive(&member("a"), "", "").expect("first");
        let second = derive(&member("b"), &first, "").expect("append");
        assert!(second > first, "{second} > {first}");
        let zeroth = derive(&member("c"), "", &first).expect("prepend");
        assert!(zeroth < first, "{zeroth} < {first}");
        let wedge = derive(&member("d"), &first, &second).expect("between");
        assert!(
            first < wedge && wedge < second,
            "{first} < {wedge} < {second}"
        );
    }

    /// Determinism across replicas: same member and bounds, same
    /// position; different members disperse.
    #[dialog_common::test]
    fn it_converges_per_member() {
        let first = derive(&member("list-head"), "", "").expect("first");
        let here = derive(&member("milk"), &first, "").expect("derives");
        let there = derive(&member("milk"), &first, "").expect("derives");
        assert_eq!(here, there);
        let other = derive(&member("bread"), &first, "").expect("derives");
        assert_ne!(here, other);
    }

    /// Malformed bounds project nothing.
    #[dialog_common::test]
    fn it_rejects_malformed_bounds() {
        assert_eq!(derive(&member("x"), "not a position!", ""), None);
        assert_eq!(derive(&member("x"), "", "0leadingdigit"), None);
    }

    /// Attribute decomposition filters to valid positions.
    #[dialog_common::test]
    fn it_decomposes_position_attributes() {
        let position = derive(&member("m"), "", "").expect("derives");
        let attribute: Attribute = format!("todo.item/{position}")
            .try_into()
            .expect("attribute parses");
        let rows = PositionParts::compute(PositionPartsInput {
            of: attribute.clone(),
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].namespace, "todo.item");
        assert_eq!(rows[0].position, position);

        let plain: Attribute = "person/display_name".parse().expect("attribute parses");
        assert!(
            PositionParts::compute(PositionPartsInput { of: plain }).is_empty(),
            "predicates with non-position characters project nothing"
        );
        let wordlike: Attribute = "person/name".parse().expect("attribute parses");
        assert_eq!(
            PositionParts::compute(PositionPartsInput { of: wordlike }).len(),
            1,
            "word predicates can be syntactically valid positions; \
             the namespace is what scopes an ordered relation"
        );
    }
}
