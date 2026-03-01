use crate::attribute::Attribute;
use crate::attribute::AttributeDescriptor;
use crate::attribute::expression::{
    AttributeExpression, AttributeStatement, ExpressionCause, relation_query,
};
use crate::descriptor::Descriptor;
use crate::query::{Application, Source};
use crate::selection::{Answer, Answers};
use crate::types::Scalar;
use crate::{Entity, Premise, Proposition, QueryError, Term, Value};

/// A typed attribute query with named fields for entity and value.
///
/// Created from an [`AttributeExpression`] when the value position is a
/// [`Term`]. Implements [`Application`] to execute queries and materialize
/// results into [`AttributeStatement<A>`].
///
/// # Examples
///
/// ```no_run
/// use dialog_query::{Attribute, Entity, Query, Term};
///
/// #[derive(Attribute, Clone)]
/// struct Name(pub String);
///
/// // Default query: all variables
/// let q = Query::<Name>::default();
///
/// // Specific entity
/// let entity = Entity::new().unwrap();
/// let q = Query::<Name> {
///     of: Term::from(entity),
///     is: Term::var("name"),
/// };
/// ```
#[derive(Clone)]
pub struct AttributeQuery<A: Attribute> {
    /// Term matching the entity this attribute belongs to.
    pub of: Term<Entity>,
    /// Term matching the attribute value.
    pub is: Term<A::Type>,
}

impl<A: Attribute> Default for AttributeQuery<A> {
    fn default() -> Self {
        Self {
            of: Term::var("of"),
            is: Term::var("is"),
        }
    }
}

impl<A> Application for AttributeQuery<A>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone + Send + 'static,
    A: From<A::Type>,
    A::Type: Scalar + TryFrom<Value>,
{
    type Conclusion = AttributeStatement<A>;

    fn evaluate<S: Source, M: Answers>(self, answers: M, source: &S) -> impl Answers {
        let query = relation_query::<A>(self.of, self.is.as_unknown(), Term::blank());
        query.evaluate(answers, source)
    }

    fn realize(&self, input: Answer) -> Result<Self::Conclusion, QueryError> {
        let of_term = &self.of;
        let is_term: Term<Value> = self.is.as_unknown();
        let entity: Entity = input.get(of_term)?;
        let value: Value = input.resolve(&is_term)?;
        let typed_value = A::Type::try_from(value).map_err(|_| {
            crate::error::InconsistencyError::TypeError(format!(
                "cannot convert value to {}",
                std::any::type_name::<A::Type>()
            ))
        })?;

        Ok(AttributeExpression::statement(entity, A::from(typed_value)))
    }
}

impl<A, Because> From<AttributeExpression<A, Entity, Term<A::Type>, Because>> for AttributeQuery<A>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    A::Type: Scalar,
    Because: ExpressionCause,
{
    fn from(expr: AttributeExpression<A, Entity, Term<A::Type>, Because>) -> Self {
        AttributeQuery {
            of: Term::Constant(expr.of),
            is: expr.is,
        }
    }
}

impl<A, Because> From<AttributeExpression<A, Term<Entity>, Term<A::Type>, Because>>
    for AttributeQuery<A>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    A::Type: Scalar,
    Because: ExpressionCause,
{
    fn from(expr: AttributeExpression<A, Term<Entity>, Term<A::Type>, Because>) -> Self {
        AttributeQuery {
            of: expr.of,
            is: expr.is,
        }
    }
}

impl<A, Because> From<AttributeExpression<A, Term<Entity>, A, Because>> for AttributeQuery<A>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    Because: ExpressionCause,
{
    fn from(expr: AttributeExpression<A, Term<Entity>, A, Because>) -> Self {
        AttributeQuery {
            of: expr.of,
            is: Term::Constant(expr.is.value().clone()),
        }
    }
}

impl<A> From<AttributeQuery<A>> for Premise
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    A::Type: Scalar,
{
    fn from(query: AttributeQuery<A>) -> Self {
        let relation = relation_query::<A>(query.of, query.is.as_unknown(), Term::blank());
        Premise::Assert(Proposition::Relation(Box::new(relation)))
    }
}
