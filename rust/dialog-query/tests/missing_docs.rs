//! Regression test for missing-docs warnings in derive-generated code.
//!
//! Every `pub` item emitted by `#[derive(Attribute)]` and
//! `#[derive(Concept)]` must carry a doc attribute so downstream crates
//! can enable `#![deny(missing_docs)]` without the lint firing on code
//! they didn't write.
//!
//! This test exercises each derive with a fully-documented user struct.
//! If a future macro change emits a new undocumented `pub` item, this
//! test stops compiling.
//!
//! `#[derive(Formula)]` is not exercised here: its generated code depends
//! on `From<_> for FormulaQuery` impls that only exist for formulas
//! defined inside `dialog-query` itself, so a downstream consumer cannot
//! derive it. Formula regressions are caught by the dialog-query crate's
//! own lints (see `#![warn(missing_docs)]` in `dialog_query::lib`).

#![deny(missing_docs)]

use dialog_query::{Attribute, Concept, Entity};

/// A person's given name. Domain derived from the module path.
#[derive(Attribute, Clone, PartialEq)]
pub struct Name(pub String);

/// A person's age in years. Domain set via a string literal.
#[derive(Attribute, Clone, PartialEq)]
#[domain("io.gozala.person")]
pub struct Age(pub u32);

/// A person's nickname. Domain set via an identifier.
#[derive(Attribute, Clone, PartialEq)]
#[domain(custom)]
pub struct Nickname(pub String);

/// A person modeled as a concept, with every field documented.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Person {
    /// The entity this person describes.
    pub this: Entity,
    /// The person's name.
    pub name: Name,
    /// The person's age.
    pub age: Age,
    /// The person's nickname.
    pub nickname: Nickname,
}

#[test]
fn it_compiles_under_deny_missing_docs() {
    // The real assertion is that this file compiles under
    // `#![deny(missing_docs)]`. Reference the generated items so dead-code
    // analysis does not elide them from the test binary and so removing a
    // doc attribute from any of them trips the lint.

    // `#[derive(Concept)]`: mirror structs and term accessors.
    let _ = PersonQuery::default();
    let _this = PersonTerms::this();
    let _name = PersonTerms::name();
    let _age = PersonTerms::age();
    let _nickname = PersonTerms::nickname();

    // `#[derive(Attribute)]`: every inherent `pub fn` the macro emits,
    // exercised across all three domain paths (derived, string literal,
    // identifier) so a regression on any branch trips the lint.
    let _ = Name::of::<Entity>;
    let _ = Name::descriptor();
    let _ = Name::the();
    let _ = Name::cardinality();
    let _ = Name::content_type();
    let _ = Age::of::<Entity>;
    let _ = Age::descriptor();
    let _ = Nickname::of::<Entity>;
    let _ = Nickname::descriptor();
}
