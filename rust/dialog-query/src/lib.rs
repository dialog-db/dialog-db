//! Dialog Query Engine
//!
//! A Datalog-inspired query engine for Dialog-DB that provides declarative
//! pattern matching and rule-based deduction over facts.
//!
//! This crate implements the core query planning and execution functionality,
//! designed to be equivalent to the TypeScript query engine in @query/.

pub mod artifact;
pub mod attribute;
pub mod concept;
pub mod error;
pub mod fact;
pub mod fact_selector;
pub mod join;
pub mod plan;
pub mod premise;
pub mod query;
pub mod rule;
pub mod selection;
pub mod selector;
pub mod statement;
pub mod stream;
pub mod syntax;
pub mod term;
pub mod types;

pub use artifact::{Entity, Value, ValueDataType};
pub use error::{InconsistencyError, QueryError};
pub use fact::{assert, retract, Assertion, Claim, Fact, Retraction};
pub use fact_selector::{FactSelector, FactSelectorPlan};
pub use premise::Premise;
pub use rule::{
    DerivedRule, DerivedRuleMatch, DerivedRuleMatchPlan, Rule, RuleApplication, RuleApplicationPlan, When,
};
pub use selection::{Match, Selection};
pub use selector::Selector;
pub use statement::{Statement, StatementPlan};
pub use stream::*;
pub use term::*;
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
    pub use crate::artifact::{Value, ValueDataType};
    pub use crate::error::QueryError;
    pub use crate::fact::{Assertion, Fact, Retraction};
    pub use crate::fact_selector::{FactSelector, FactSelectorPlan};
    pub use crate::plan::EvaluationPlan;
    pub use crate::premise::Premise;
    pub use crate::query::Query;
    pub use crate::rule::{DerivedRule, DerivedRuleMatch, Rule, When};
    pub use crate::selector::Selector;
    pub use crate::statement::{Statement, StatementPlan};
    pub use crate::syntax::{Syntax, VariableScope};
    pub use crate::term::Term;
    pub use crate::types::IntoValueDataType;
    // Macros are automatically available due to #[macro_export]
}
