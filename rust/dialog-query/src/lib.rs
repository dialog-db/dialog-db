//! Dialog Query Engine
//!
//! A Datalog-inspired query engine for Dialog-DB that provides declarative
//! pattern matching and rule-based deduction over facts.
//!
//! This crate implements the core query planning and execution functionality,
//! designed to be equivalent to the TypeScript query engine in @query/.

pub mod analyzer;
pub mod application;
pub mod artifact;
pub mod attribute;
pub mod claim;
pub mod concept;
pub mod cursor;
pub mod dependencies;
pub mod error;
pub mod fact;
pub mod fact_selector;
pub mod math;
pub mod negation;
pub mod parameters;
pub mod plan;
pub mod predicate;
pub mod premise;
pub mod query;
pub mod rule;
pub mod selection;
pub mod selector;
pub mod session;
pub mod stream;
pub mod syntax;
pub mod term;
pub mod types;

pub use application::Application;
pub use artifact::{Entity, Value, ValueDataType};
pub use attribute::{Attribute, Cardinality};
pub use claim::Claims;
pub use concept::Concept;
pub use dependencies::{Dependencies, Requirement};
pub use dialog_query_macros::Rule;
pub use error::{InconsistencyError, QueryError};
pub use fact::{assert, retract, Assertion, Claim, Fact, Retraction};
pub use fact_selector::{FactSelector, FactSelectorPlan};
pub use negation::Negation;
pub use parameters::Parameters;
pub use plan::{EvaluationContext, EvaluationPlan};
pub use predicate::{Compute, DeductiveRule, Formula};
pub use premise::Premise;
pub use query::{Query, Source, Store};
pub use rule::{Premises, Rule, When};
pub use session::Session;

pub use async_stream::try_stream;
pub use selection::{Match, MatchSet, Selection, SelectionExt};
pub use selector::Selector;
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
    pub use crate::rule::{Premises, Rule, When};
    pub use crate::selector::Selector;
    pub use crate::syntax::VariableScope;
    pub use crate::term::Term;
    pub use crate::types::IntoValueDataType;
    // Macros are automatically available due to #[macro_export]
}
