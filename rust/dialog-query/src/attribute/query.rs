use crate::attribute::Attribute;
use crate::attribute::expression::{
    AttributeExpression, AttributeStatement, ExpressionCause, relation_query,
};
use crate::query::{Application, Source};
use crate::relation::query::RelationQuery;
use crate::selection::{Answer, Answers};
use crate::types::Scalar;
use crate::{Entity, Premise, Proposition, QueryError, Term, Value};
use std::marker::PhantomData;

/// A typed attribute query that wraps a [`RelationQuery`].
///
/// Created from an [`AttributeExpression`] when the value position is a
/// [`Term`]. Implements [`Application`] to execute queries and materialize
/// results into [`AttributeStatement<A>`].
#[derive(Clone)]
pub struct AttributeQuery<A: Attribute>(RelationQuery, PhantomData<A>);

impl<A: Attribute> AttributeQuery<A> {
    pub(crate) fn new(query: RelationQuery) -> Self {
        Self(query, PhantomData)
    }

    /// Get the underlying relation query.
    pub fn relation(&self) -> &RelationQuery {
        &self.0
    }
}

impl<A> Application for AttributeQuery<A>
where
    A: Attribute + Clone + Send + 'static,
    A: From<A::Type>,
    A::Type: Scalar + TryFrom<Value>,
{
    type Conclusion = AttributeStatement<A>;

    fn evaluate<S: Source, M: Answers>(self, answers: M, source: &S) -> impl Answers {
        self.0.evaluate(answers, source)
    }

    fn realize(&self, input: Answer) -> Result<Self::Conclusion, QueryError> {
        let entity: Entity = input.get(self.0.of())?;
        let value: Value = input.resolve(self.0.is())?;
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
    A: Attribute + Clone,
    A::Type: Scalar,
    Because: ExpressionCause,
{
    fn from(expr: AttributeExpression<A, Entity, Term<A::Type>, Because>) -> Self {
        AttributeQuery::new(relation_query::<A>(
            Term::Constant(expr.of),
            expr.is.as_unknown(),
            expr.cause.as_cause_term(),
        ))
    }
}

impl<A, Because> From<AttributeExpression<A, Term<Entity>, Term<A::Type>, Because>>
    for AttributeQuery<A>
where
    A: Attribute + Clone,
    A::Type: Scalar,
    Because: ExpressionCause,
{
    fn from(expr: AttributeExpression<A, Term<Entity>, Term<A::Type>, Because>) -> Self {
        AttributeQuery::new(relation_query::<A>(
            expr.of,
            expr.is.as_unknown(),
            expr.cause.as_cause_term(),
        ))
    }
}

impl<A, Because> From<AttributeExpression<A, Term<Entity>, A, Because>> for AttributeQuery<A>
where
    A: Attribute + Clone,
    Because: ExpressionCause,
{
    fn from(expr: AttributeExpression<A, Term<Entity>, A, Because>) -> Self {
        AttributeQuery::new(relation_query::<A>(
            expr.of,
            Term::Constant(expr.is.value().as_value()),
            expr.cause.as_cause_term(),
        ))
    }
}

impl<A> From<AttributeQuery<A>> for Premise
where
    A: Attribute + Clone,
{
    fn from(query: AttributeQuery<A>) -> Self {
        Premise::Assert(Proposition::Relation(Box::new(query.0)))
    }
}
