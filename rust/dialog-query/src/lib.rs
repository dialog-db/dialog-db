//! Dialog Query Engine
//!
//! A Datalog-inspired query engine for Dialog-DB that provides declarative
//! pattern matching and rule-based deduction over facts.
//!
//! This crate implements the core query planning and execution functionality,
//! designed to be equivalent to the TypeScript query engine in @query/.

pub mod attribute;
pub mod concept;
pub mod error;
pub mod fact;
pub mod fact_selector;
pub mod join;
pub mod plan;
pub mod predicate;
pub mod query;
pub mod rule;
pub mod selection;
pub mod selector;
pub mod stream;
pub mod syntax;
pub mod term;
pub mod types;

pub use dialog_artifacts::Entity;
pub use error::{InconsistencyError, QueryError};
pub use fact::{assert, retract, Assertion, Claim, Fact, Retraction};
pub use fact_selector::{FactSelector, FactSelectorPlan};
pub use predicate::{Predicate, PredicateForm, PredicateFormPlan};
pub use rule::{
    DerivedRule, DerivedRuleMatch, DerivedRuleMatchPlan, Rule, RuleApplication, RuleApplicationPlan,
};
pub use selection::{Match, Selection};
pub use selector::Selector;
pub use stream::*;
pub use term::*;

pub use dialog_artifacts::ValueDataType;
pub use types::IntoValueDataType;

/// Cardinality indicates whether an attribute can have one or many values
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cardinality {
    One,
    Many,
}

/// Trait for attribute types that can be used in relations
pub trait Attribute {
    fn name() -> &'static str;
    fn cardinality() -> Cardinality;
    fn value_type() -> ValueDataType;
}

pub use plan::EvaluationPlan;
pub use query::Query;
pub use syntax::{Syntax, VariableScope};

/// Re-export commonly used types
pub mod prelude {
    pub use crate::error::QueryError;
    pub use crate::fact::{Assertion, Fact, Retraction};
    pub use crate::fact_selector::{FactSelector, FactSelectorPlan};
    pub use crate::plan::EvaluationPlan;
    pub use crate::predicate::{Predicate, PredicateForm, PredicateFormPlan};
    pub use crate::query::Query;
    pub use crate::rule::{DerivedRule, DerivedRuleMatch, Rule};
    pub use crate::selector::Selector;
    pub use crate::syntax::{Syntax, VariableScope};
    pub use crate::term::Term;
    pub use crate::types::IntoValueDataType;
    pub use dialog_artifacts::{Value, ValueDataType};
    // Macros are automatically available due to #[macro_export]
}
