use std::any::type_name;

use crate::attribute::Attribute;
use crate::attribute::AttributeDescriptor;
use crate::attribute::expression::ExpressionCause;
use crate::attribute::expression::typed::{StaticAttributeExpression, StaticAttributeStatement};
use crate::attribute::query::dynamic::DynamicAttributeQuery;
use crate::descriptor::Descriptor;
use crate::query::{Application, Source};
use crate::selection::{Match, Selection};
use crate::types::Any;
use crate::types::Scalar;
use crate::{Entity, EvaluationError, Premise, Proposition, Term, Value};

/// A typed attribute query with named fields for entity and value.
///
/// Created from a [`StaticAttributeExpression`] when the value position is a
/// [`Term`]. Implements [`Application`] to execute queries and materialize
/// results into [`StaticAttributeStatement<A>`].
///
/// # Examples
///
/// ```no_run
/// use dialog_query::{Attribute, Entity, Term};
/// use dialog_query::attribute::query::StaticAttributeQuery;
///
/// #[derive(Attribute, Clone)]
/// struct Name(pub String);
///
/// // Default query: all variables
/// let q = StaticAttributeQuery::<Name>::default();
///
/// // Specific entity
/// let entity = Entity::new().unwrap();
/// let q = StaticAttributeQuery::<Name> {
///     of: Term::from(entity),
///     is: Term::var("name"),
/// };
/// ```
#[derive(Clone)]
pub struct StaticAttributeQuery<A: Attribute> {
    /// Term matching the entity this attribute belongs to.
    pub of: Term<Entity>,
    /// Term matching the attribute value.
    pub is: Term<A::Type>,
}

impl<A: Attribute> Default for StaticAttributeQuery<A> {
    fn default() -> Self {
        Self {
            of: Term::var("of"),
            is: Term::var("is"),
        }
    }
}

impl<A> From<StaticAttributeQuery<A>> for DynamicAttributeQuery
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    A::Type: Scalar,
{
    fn from(query: StaticAttributeQuery<A>) -> Self {
        let descriptor = <A as Descriptor<AttributeDescriptor>>::descriptor();
        DynamicAttributeQuery::new(
            Term::Constant(Value::from(descriptor.the().clone())),
            query.of,
            query.is.into(),
            Term::blank(),
            Some(descriptor.cardinality()),
        )
    }
}

impl<A> Application for StaticAttributeQuery<A>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone + Send + 'static,
    A: From<A::Type>,
    A::Type: Scalar + TryFrom<Value>,
{
    type Conclusion = StaticAttributeStatement<A>;

    fn evaluate<S: Source, M: Selection>(self, selection: M, source: &S) -> impl Selection {
        let query = DynamicAttributeQuery::from(self);
        Application::evaluate(query, selection, source)
    }

    fn realize(&self, input: Match) -> Result<Self::Conclusion, EvaluationError> {
        let of_term = &self.of;
        let is_param = Term::<Any>::from(&self.is);
        let entity: Entity = Entity::try_from(input.lookup(&Term::from(of_term))?)?;
        let value: Value = input.lookup(&is_param)?;
        let typed_value = A::Type::try_from(value).map_err(|_| {
            EvaluationError::Store(format!(
                "cannot convert value to {}",
                type_name::<A::Type>()
            ))
        })?;

        Ok(StaticAttributeExpression::statement(
            entity,
            A::from(typed_value),
        ))
    }
}

impl<A, Because> From<StaticAttributeExpression<A, Entity, Term<A::Type>, Because>>
    for StaticAttributeQuery<A>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    A::Type: Scalar,
    Because: ExpressionCause,
{
    fn from(expr: StaticAttributeExpression<A, Entity, Term<A::Type>, Because>) -> Self {
        let (of, is, _) = expr.into_parts();
        StaticAttributeQuery {
            of: Term::Constant(Value::from(of.clone())),
            is,
        }
    }
}

impl<A, Because> From<StaticAttributeExpression<A, Term<Entity>, Term<A::Type>, Because>>
    for StaticAttributeQuery<A>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    A::Type: Scalar,
    Because: ExpressionCause,
{
    fn from(expr: StaticAttributeExpression<A, Term<Entity>, Term<A::Type>, Because>) -> Self {
        let (of, is, _) = expr.into_parts();
        StaticAttributeQuery { of, is }
    }
}

impl<A, Because> From<StaticAttributeExpression<A, Term<Entity>, A, Because>>
    for StaticAttributeQuery<A>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    Because: ExpressionCause,
{
    fn from(expr: StaticAttributeExpression<A, Term<Entity>, A, Because>) -> Self {
        let (of, is, _) = expr.into_parts();
        StaticAttributeQuery {
            of,
            is: Term::Constant(is.value().clone().into()),
        }
    }
}

impl<A> From<StaticAttributeQuery<A>> for Premise
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    A::Type: Scalar,
{
    fn from(query: StaticAttributeQuery<A>) -> Self {
        let query = DynamicAttributeQuery::from(query);
        Premise::Assert(Proposition::Attribute(Box::new(query)))
    }
}
