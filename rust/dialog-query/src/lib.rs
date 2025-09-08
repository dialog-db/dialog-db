//! Dialog Query Engine
//!
//! A Datalog-inspired query engine for Dialog-DB that provides declarative
//! pattern matching and rule-based deduction over facts.
//!
//! This crate implements the core query planning and execution functionality,
//! designed to be equivalent to the TypeScript query engine in @query/.

pub mod and;
pub mod artifact;
pub mod attribute;
pub mod concept;
pub mod cursor;
pub mod deductive_rule;
pub mod error;
pub mod fact;
pub mod fact_selector;
pub mod formula;
pub mod join;
pub mod plan;
pub mod planner;
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
pub mod value;

pub use artifact::{Entity, Value, ValueDataType};
pub use attribute::{Attribute, Cardinality};
pub use concept::Concept;
pub use dialog_query_macros::Rule;
pub use error::{InconsistencyError, QueryError};
pub use fact::{assert, retract, Assertion, Claim, Fact, Retraction};
pub use fact_selector::{FactSelector, FactSelectorPlan};
pub use plan::{EvaluationContext, EvaluationPlan};
pub use premise::Premise;
pub use query::{Query, Store};
pub use rule::{Rule, Statements, When};

pub use async_stream::try_stream;
pub use selection::{Match, MatchSet, Selection, SelectionExt};
pub use selector::Selector;
pub use statement::{Statement, StatementPlan};
pub use stream::*;
pub use syntax::VariableScope;
pub use term::*;
pub use types::IntoValueDataType;

/// Re-export commonly used types
pub mod prelude {
    pub use crate::artifact::{Value, ValueDataType};
    pub use crate::error::QueryError;
    pub use crate::fact::{Assertion, Fact, Retraction};
    pub use crate::fact_selector::{FactSelector, FactSelectorPlan};
    pub use crate::plan::EvaluationPlan;
    pub use crate::premise::Premise;
    pub use crate::query::Query;
    pub use crate::rule::{Rule, Statements, When};
    pub use crate::selector::Selector;
    pub use crate::statement::{Statement, StatementPlan};
    pub use crate::syntax::VariableScope;
    pub use crate::term::Term;
    pub use crate::types::IntoValueDataType;
    // Macros are automatically available due to #[macro_export]
}
