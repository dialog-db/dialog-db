//! Syntax trait for query forms

use crate::error::QueryResult;
use crate::query::Query;
use std::collections::BTreeSet;

/// Tracks variable bindings during query planning
#[derive(Debug, Clone)]
pub struct VariableScope {
    pub bound_variables: BTreeSet<String>,
}

impl VariableScope {
    pub fn new() -> Self {
        Self {
            bound_variables: BTreeSet::new(),
        }
    }
}

/// Trait implemented by all syntax forms (Select, Rule, etc.)
pub trait Syntax {
    /// The type of execution plan this syntax form produces
    type Plan: Query;

    /// Create an execution plan for this syntax form
    fn plan(&self, scope: &VariableScope) -> QueryResult<Self::Plan>;
}
