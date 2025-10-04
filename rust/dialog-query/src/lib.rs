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
pub mod conversions;
pub mod cursor;
pub mod dependencies;
pub mod error;
pub mod fact;
pub mod fact_selector;
pub mod label;
pub mod logic;
pub mod math;
pub mod negation;
pub mod parameters;
pub mod plan;
pub mod planner;
pub mod predicate;
pub mod premise;
pub mod query;
pub mod rule;
pub mod schema;
pub mod selection;
pub mod selector;
pub mod session;
pub mod stream;
pub mod strings;
pub mod syntax;
pub mod term;
pub mod types;

pub use application::Application;
pub use artifact::{Entity, Type, Value};
pub use attribute::{Attribute, Cardinality};
pub use claim::fact::Relation;
pub use concept::Concept;
pub use dependencies::{Dependencies, Dependency, Group, Requirement};
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
pub use schema::{Constraint, Schema};
pub use session::transaction::{Edit, Transaction, TransactionError};
pub use session::{QuerySession, Session};

pub use async_stream::try_stream;
pub use selection::{Match, MatchSet, Selection, SelectionExt};
pub use selector::Selector;
pub use stream::*;
pub use syntax::VariableScope;
pub use term::*;
pub use types::IntoValueDataType;

/// Formula library exports
pub mod formulas {
    //! Built-in formulas for common data transformations
    //!
    //! This module provides a comprehensive library of formulas for:
    //! - Mathematical operations (sum, difference, product, quotient, modulo)
    //! - String operations (concatenate, length, uppercase, lowercase)
    //! - Type conversions (to_string, parse_number)
    //! - Boolean logic (and, or, not)

    // Mathematical formulas
    pub use crate::math::{Difference, Modulo, Product, Quotient, Sum};

    // String operation formulas
    pub use crate::strings::{Concatenate, Length, Lowercase, Uppercase};

    // Type conversion formulas
    pub use crate::conversions::{ParseNumber, ToString};

    // Boolean logic formulas
    pub use crate::logic::{And, Not, Or};
}

/// Re-export commonly used types
pub mod prelude {
    pub use crate::artifact::{Type, Value};
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
