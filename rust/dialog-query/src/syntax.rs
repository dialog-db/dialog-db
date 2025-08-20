//! Syntax trait for query forms

use crate::error::QueryResult;
use crate::query::Query;
use crate::variable::VariableScope;

/// Trait implemented by all syntax forms (Select, Rule, etc.)
pub trait Syntax {
    /// The type of execution plan this syntax form produces
    type Plan: Query;

    /// Create an execution plan for this syntax form
    fn plan(&self, scope: &VariableScope) -> QueryResult<Self::Plan>;
}
