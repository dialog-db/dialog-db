//! Dialog Query Engine
//!
//! A Datalog-inspired query engine for Dialog-DB that provides declarative
//! pattern matching and rule-based deduction over facts.
//!
//! This crate implements the core query planning and execution functionality,
//! designed to be equivalent to the TypeScript query engine in @query/.

// TODO: Large error types - Many functions return Result<T, E> where E contains large types
// like QueryError (344 bytes), InconsistencyError (320 bytes), and TypeError (161 bytes).
// These error types contain large Value/Term types that make the Result enum large.
// Consider boxing error fields in the future to reduce Result sizes, but this would be
// a breaking API change. For now, we allow this clippy warning.
#![allow(clippy::result_large_err)]
#![warn(missing_docs)]

// Allow macro-generated code to reference this crate as `dialog_query::`
extern crate self as dialog_query;

/// Static analysis of rules and formulas for query planning.
pub mod analyzer;
/// Re-exports from the dialog-artifacts crate.
pub mod artifact;
/// Attribute definitions and schema metadata.
pub mod attribute;
/// Proposition types for querying the knowledge base.
pub mod proposition;

/// Entity-attribute-value assertion triples for writes.
pub mod assertion;
/// Claim trait for asserting and retracting facts.
pub mod claim;
/// Concept definitions for entity-centric pattern matching.
pub mod concept;
/// Constraint system for filtering and validating variable bindings.
pub mod constraint;
/// DSL types for constructing type-safe queries.
pub mod dsl;
/// Variable binding environment used during query planning.
pub mod environment;
/// Error types for the query engine.
pub mod error;
/// Fact and scalar types for the knowledge base.
pub mod fact;
/// Built-in formulas for data transformations and computations.
pub mod formula;
/// Negation support for excluding matching results.
pub mod negation;
/// Named parameter bindings for rule and formula applications.
pub mod parameters;
/// Query planner that compiles premises into execution plans.
pub mod planner;
/// Predicate definitions including concepts, facts, and formulas.
pub mod predicate;
/// Premise trait for rule conditions and pattern matching.
pub mod premise;
/// Query trait and store abstractions for polymorphic querying.
pub mod query;
/// Read-side relation type for query results.
pub mod relation;
/// Rule-based deduction system for deriving facts.
pub mod rule;
/// Schema system for describing parameter signatures.
pub mod schema;
/// Selection and answer types for query results.
pub mod selection;
/// Database sessions for querying and committing changes.
pub mod session;
/// Stream utilities for async query result iteration.
pub mod stream;
/// Term types for pattern matching with variables and constants.
pub mod term;
/// Type system utilities bridging Rust types to dialog-artifacts types.
pub mod types;

pub use artifact::{Attribute as ArtifactAttribute, Entity, Type, Value};
pub use assertion::Assertion;
pub use attribute::{Attribute, AttributeDescriptor, Cardinality, The};
pub use claim::Claim;
pub use concept::{Concept, With, WithQuery, WithTerms};
pub use dialog_macros::{Attribute, Concept, Formula};
pub use dsl::{Input, Match, Predicate, Query};
pub use error::{InconsistencyError, QueryError};
pub use negation::Negation;
pub use parameters::Parameters;
pub use predicate::concept::DynamicProof;
pub use predicate::{DeductiveRule, Formula};
pub use premise::Premise;
pub use proposition::Proposition;
pub use proposition::concept::ConceptRules;
pub use query::{Application, Source, Store};
pub use relation::Relation;
pub use rule::{Premises, When};
pub use schema::{Field, Requirement, Schema};
pub use session::transaction::{Edit, Transaction, TransactionError};
pub use session::{QuerySession, Session};

pub use async_stream::try_stream;
pub use environment::Environment;
pub use selection::{Answer, Answers};
pub use stream::*;
pub use term::*;
pub use types::IntoType;

/// Re-export commonly used types.
pub mod prelude {
    pub use crate::artifact::{Type, Value};
    pub use crate::environment::Environment;
    pub use crate::error::QueryError;
    pub use crate::premise::Premise;
    pub use crate::rule::Premises;
    pub use crate::term::Term;
    pub use crate::types::IntoType;
    // Macros are automatically available due to #[macro_export]
}
