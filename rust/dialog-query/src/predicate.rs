//! Premise trait for rule conditions
//!
//! This module defines the premise system used in rule conditions. Premises represent
//! patterns that can be matched against facts in the knowledge base during rule evaluation.
//!
//! Note: Premises are only used in rule conditions (the "when" part), not in conclusions.
/// Concept predicates for entity-centric queries.
pub mod concept;
/// Deductive rules that derive new facts from existing ones.
pub mod deductive_rule;
/// Formula predicates for computed values.
pub mod formula;
/// Relation predicates for querying the knowledge base.
pub mod relation;

pub use concept::ConceptDescriptor;
pub use deductive_rule::DeductiveRule;
pub use formula::Formula;
pub use relation::RelationDescriptor;
