//! Dialog Query Engine
//!
//! A Datalog-inspired query engine for Dialog-DB that provides declarative
//! pattern matching and rule-based deduction over facts.
//!
//! This crate implements the core query planning and execution functionality,
//! designed to be equivalent to the TypeScript query engine in @query/.

pub mod error;
pub mod fact;
pub mod fact_selector;
pub mod plan;
pub mod query;
pub mod selection;
pub mod selector;
pub mod stream;
pub mod syntax;
pub mod term;
pub mod variable;

pub use error::{InconsistencyError, QueryError};
pub use fact::{Assertion, Fact, Retraction};
pub use fact_selector::{FactSelector, FactSelectorPlan};
pub use selection::{Match, Selection};
pub use selector::Selector;
pub use stream::*;
pub use term::Term;

pub use variable::{
    AttributeVar, BoolVar, BytesVar, EntityVar, FloatVar, SIntVar, StringVar, UIntVar, Untyped,
    UntypedVar, ValueDataType, Variable, VariableName,
};

pub use plan::EvaluationPlan;
pub use query::Query;
pub use syntax::Syntax;

// Macros are automatically available at crate root due to #[macro_export]

/// Re-export commonly used types
pub mod prelude {
    pub use crate::error::QueryError;
    pub use crate::fact::{Assertion, Fact, Retraction};
    pub use crate::fact_selector::{FactSelector, FactSelectorPlan};
    pub use crate::plan::EvaluationPlan;
    pub use crate::query::Query;
    pub use crate::selector::Selector;
    pub use crate::syntax::Syntax;
    pub use crate::term::Term;
    pub use crate::variable::{
        AttributeVar, BoolVar, BytesVar, EntityVar, FloatVar, SIntVar, StringVar, UIntVar, Untyped,
        UntypedVar, ValueDataType, Variable, VariableName,
    };
    // Macros are automatically available due to #[macro_export]
}
