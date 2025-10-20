//! Premise trait for rule conditions
//!
//! This module defines the premise system used in rule conditions. Premises represent
//! patterns that can be matched against facts in the knowledge base during rule evaluation.
//!
//! Note: Premises are only used in rule conditions (the "when" part), not in conclusions.
pub mod concept;
pub mod deductive_rule;
pub mod fact;
pub mod formula;

pub use concept::Concept;
pub use deductive_rule::DeductiveRule;
pub use fact::Fact;
pub use formula::{Formula, Output};
