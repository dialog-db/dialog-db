//! Dialog Query Engine
//!
//! A Datalog-inspired query engine for Dialog-DB that provides declarative
//! pattern matching and rule-based deduction over facts.
//!
//! This crate implements the core query planning and execution functionality,
//! designed to be equivalent to the TypeScript query engine in @query/.

pub mod error;
pub mod fact;
pub mod fact_selector;
pub mod plan;
pub mod query;
pub mod selection;
pub mod selector;
pub mod stream;
pub mod syntax;
pub mod term;
pub mod variable;

pub use error::{InconsistencyError, QueryError};
pub use fact::{assert, retract, Assertion, Claim, Fact, Retraction};
pub use fact_selector::{FactSelector, FactSelectorPlan};
pub use selection::{Match, Selection};
pub use selector::Selector;
pub use stream::*;
pub use term::Term;

pub use variable::Untyped;
pub use variable::{TypedVariable, ValueDataType, VariableName};

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
pub use syntax::Syntax;

// Re-export procedural macros
pub use dialog_query_macros::relation;

/// Re-export commonly used types
pub mod prelude {
    pub use crate::error::QueryError;
    pub use crate::fact::{Assertion, Fact, Retraction};
    pub use crate::fact_selector::{FactSelector, FactSelectorPlan};
    pub use crate::plan::EvaluationPlan;
    pub use crate::query::Query;
    pub use crate::selector::Selector;
    pub use crate::syntax::Syntax;
    pub use crate::term::Term;
    pub use crate::variable::{TypedVariable, Untyped, ValueDataType, VariableName};
    // Macros are automatically available due to #[macro_export]
}
