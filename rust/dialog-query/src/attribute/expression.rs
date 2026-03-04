/// Dynamic (untyped) attribute expressions using `The` identifiers.
pub mod dynamic;
/// Typed attribute expressions using `Attribute` types.
pub mod typed;

use crate::artifact::{Cause, Value};
use crate::attribute::Attribute;
use crate::attribute::AttributeDescriptor;
use crate::descriptor::Descriptor;
use crate::relation::query::RelationQuery;
use crate::types::Any;
use crate::{Entity, Term};

pub use dynamic::*;
pub use typed::*;

/// Cause-position in an attribute expression.
///
/// Implemented for [`Option<Cause>`] (concrete or absent) and
/// [`Term<Cause>`] (query variable), mapping each to a [`Term<Cause>`]
/// for the underlying [`RelationQuery`].
pub trait ExpressionCause {
    /// Convert to a `Term<Cause>` for use in a [`RelationQuery`].
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

/// Build a [`RelationQuery`] from a typed attribute's descriptor.
pub fn relation_query<A: Attribute + Descriptor<AttributeDescriptor>>(
    of: Term<Entity>,
    is: Term<Any>,
    cause: Term<Cause>,
) -> RelationQuery {
    let desc = <A as Descriptor<AttributeDescriptor>>::descriptor();
    RelationQuery::new(
        Term::Constant(Value::from(desc.the().clone())),
        of,
        is,
        cause,
        Some(desc.cardinality()),
    )
}
