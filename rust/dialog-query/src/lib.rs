//! Dialog Query Engine
//!
//! A Datalog-inspired query engine for Dialog-DB that provides declarative
//! pattern matching and rule-based deduction over facts.
//!
//! This crate implements the core query planning and execution functionality.

#![warn(missing_docs)]
#![warn(clippy::absolute_paths)]
#![warn(clippy::default_trait_access)]
#![warn(clippy::fallible_impl_from)]
#![warn(clippy::panicking_unwrap)]
#![warn(clippy::unused_async)]
#![cfg_attr(not(test), warn(clippy::large_futures))]
#![deny(clippy::partial_pub_fields)]
#![deny(clippy::unnecessary_self_imports)]
#![cfg_attr(not(test), deny(clippy::panic))]

// Allow macro-generated code to reference this crate as `dialog_query::`
extern crate self as dialog_query;

/// Re-exports from the dialog-artifacts crate.
pub mod artifact;
/// Attribute definitions and schema metadata.
pub mod attribute;
/// Proposition types for querying the knowledge base.
pub mod proposition;

/// Read-side claim type for query results.
pub mod claim;
/// Concept definitions for entity-centric pattern matching.
pub mod concept;
/// Constraint system for filtering and validating variable bindings.
pub mod constraint;
/// Static descriptor trait for attribute and concept metadata.
pub mod descriptor;
/// Variable binding environment used during query planning.
pub mod environment;
/// Error types for the query engine.
pub mod error;
/// Built-in formulas for data transformations and computations.
pub mod formula;
/// Negation support for excluding matching results.
pub mod negation;
/// Named parameter bindings for rule and formula applications.
pub mod parameters;
/// Query planner that compiles premises into execution plans.
pub mod planner;
/// Predicate trait and type aliases for type-safe queries.
pub mod predicate;
/// Premise trait for rule conditions and pattern matching.
pub mod premise;
/// Query trait and store abstractions for polymorphic querying.
pub mod query;
/// Rule-based deduction system for deriving facts.
pub mod rule;
/// Schema system for describing parameter signatures.
pub mod schema;
/// Selection and match types for query results.
pub mod selection;
/// Database sessions for querying and committing changes.
pub mod session;
/// Data source for query evaluation (branch + env + rules).
pub mod source;
/// Statement trait for asserting and retracting facts.
pub mod statement;
/// Stream utilities for async query result iteration.
pub mod stream;
/// Term types for pattern matching with variables and constants.
pub mod term;
/// Type system utilities bridging Rust types to dialog-artifacts types.
pub mod types;

pub use artifact::*;
pub use attribute::query::{AttributeQuery, DynamicAttributeQuery};
pub use attribute::*;
pub use claim::Claim;
pub use concept::descriptor::{ConceptConclusion, ConceptDescriptor};
pub use concept::query::{ConceptQuery, ConceptRules};
pub use concept::{Concept, Conclusion};
pub use constraint::Constraint;
pub use descriptor::Descriptor;
pub use environment::*;
pub use error::*;
pub use formula::*;
pub use negation::*;
pub use parameters::*;
pub use planner::*;
pub use predicate::*;
pub use premise::*;
pub use proposition::*;
pub use query::*;
pub use rule::*;
pub use schema::*;
pub use selection::*;
pub use session::*;
pub use statement::*;
pub use stream::*;
pub use term::*;
pub use types::*;

pub use async_stream::try_stream;
pub use dialog_capability::Provider;
pub use dialog_common::ConditionalSync;
pub use dialog_effects::archive;
pub use dialog_macros::{Attribute, Concept, Formula};
