//! Dialog Query Engine
//!
//! A Datalog-inspired query engine for Dialog-DB that provides declarative
//! pattern matching and rule-based deduction over facts.
//!
//! This crate implements the core query planning and execution functionality,
//! designed to be equivalent to the TypeScript query engine in @query/.

// Allow macro-generated code to reference this crate as `dialog_query::`
extern crate self as dialog_query;

pub mod analyzer;
pub mod application;
pub mod artifact;
pub mod attribute;

pub mod claim;
pub mod concept;
pub mod context;
pub mod conversions;
pub mod cursor;
pub mod dsl;
pub mod environment;
pub mod error;
pub mod fact;
pub mod label;
pub mod logic;
pub mod math;
pub mod negation;
pub mod parameters;
pub mod planner;
pub mod predicate;
pub mod premise;
pub mod query;
pub mod relation;
pub mod rule;
pub mod schema;
pub mod selection;
pub mod session;
pub mod stream;
pub mod strings;
pub mod term;
pub mod types;

pub use application::Application;
pub use artifact::{Entity, Type, Value};
pub use attribute::{Attribute, Cardinality};
pub use claim::Claim;
pub use concept::Concept;
pub use context::{EvaluationContext, EvaluationPlan};
pub use dialog_query_macros::{Concept, Formula};
pub use dsl::{Input, Match};
pub use error::{InconsistencyError, QueryError};
pub use fact::Fact;
pub use negation::Negation;
pub use parameters::Parameters;
pub use predicate::{DeductiveRule, Formula, Output};
pub use premise::Premise;
pub use query::{Source, Store};
pub use relation::Relation;
pub use rule::{Premises, Rule, When};
pub use schema::{Constraint, Requirement, Schema};
pub use session::transaction::{Edit, Transaction, TransactionError};
pub use session::{QuerySession, Session};

pub use async_stream::try_stream;
pub use environment::Environment;
pub use selection::{Answer, Answers};
pub use stream::*;
pub use term::*;
pub use types::IntoType;

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
    pub use crate::context::EvaluationPlan;
    pub use crate::environment::Environment;
    pub use crate::error::QueryError;
    pub use crate::premise::Premise;
    pub use crate::rule::{Premises, Rule, When};
    pub use crate::term::Term;
    pub use crate::types::IntoType;
    // Macros are automatically available due to #[macro_export]
}
