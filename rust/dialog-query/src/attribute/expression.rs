/// Dynamic (untyped) attribute expressions using `The` identifiers.
pub mod dynamic;
/// Typed attribute expressions using `Attribute` types.
pub mod typed;

use crate::Term;
use crate::artifact::{Cause, Value};

pub use dynamic::*;
pub use typed::*;

/// Cause-position in an attribute expression.
///
/// Implemented for [`Option<Cause>`] (concrete or absent) and
/// [`Term<Cause>`] (query variable), mapping each to a [`Term<Cause>`]
/// for the underlying [`AttributeQuery`].
pub trait ExpressionCause {
    /// Convert to a `Term<Cause>` for use in an [`AttributeQuery`].
    fn as_cause_term(&self) -> Term<Cause>;
}

impl ExpressionCause for Cause {
    fn as_cause_term(&self) -> Term<Cause> {
        Term::Constant(Value::from(self.clone()))
    }
}

impl ExpressionCause for Option<Cause> {
    fn as_cause_term(&self) -> Term<Cause> {
        match self {
            Some(cause) => Term::Constant(Value::from(cause.clone())),
            None => Term::blank(),
        }
    }
}

impl ExpressionCause for Term<Cause> {
    fn as_cause_term(&self) -> Term<Cause> {
        self.clone()
    }
}
