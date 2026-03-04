use crate::attribute::Attribute;
use crate::attribute::AttributeDescriptor;
use crate::attribute::expression::typed::{StaticAttributeExpression, StaticAttributeStatement};
use crate::attribute::expression::{ExpressionCause, relation_query};
use crate::descriptor::Descriptor;
use crate::query::{Application, Source};
use crate::selection::{Answer, Answers};
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
    type Conclusion = StaticAttributeStatement<A>;

    fn evaluate<S: Source, M: Answers>(self, answers: M, source: &S) -> impl Answers {
        let query = relation_query::<A>(self.of, self.is.clone().into(), Term::blank());
        query.evaluate(answers, source)
    }

    fn realize(&self, input: Answer) -> Result<Self::Conclusion, EvaluationError> {
        let of_term = &self.of;
        let is_param = Term::<Any>::from(&self.is);
        let entity: Entity = Entity::try_from(input.lookup(&Term::from(of_term))?)?;
        let value: Value = input.lookup(&is_param)?;
        let typed_value = A::Type::try_from(value).map_err(|_| {
            EvaluationError::Store(format!(
                "cannot convert value to {}",
                std::any::type_name::<A::Type>()
            ))
        })?;

        Ok(StaticAttributeExpression::statement(
            entity,
            A::from(typed_value),
        ))
    }
}

impl<A, Because> From<StaticAttributeExpression<A, Entity, Term<A::Type>, Because>>
    for AttributeQuery<A>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    A::Type: Scalar,
    Because: ExpressionCause,
{
    fn from(expr: StaticAttributeExpression<A, Entity, Term<A::Type>, Because>) -> Self {
        AttributeQuery {
            of: Term::Constant(Value::from(expr.of.clone())),
            is: expr.is,
        }
    }
}

impl<A, Because> From<StaticAttributeExpression<A, Term<Entity>, Term<A::Type>, Because>>
    for AttributeQuery<A>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    A::Type: Scalar,
    Because: ExpressionCause,
{
    fn from(expr: StaticAttributeExpression<A, Term<Entity>, Term<A::Type>, Because>) -> Self {
        AttributeQuery {
            of: expr.of,
            is: expr.is,
        }
    }
}

impl<A, Because> From<StaticAttributeExpression<A, Term<Entity>, A, Because>> for AttributeQuery<A>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    Because: ExpressionCause,
{
    fn from(expr: StaticAttributeExpression<A, Term<Entity>, A, Because>) -> Self {
        AttributeQuery {
            of: expr.of,
            is: Term::Constant(expr.is.value().clone().into()),
        }
    }
}

impl<A> From<AttributeQuery<A>> for Premise
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    A::Type: Scalar,
{
    fn from(query: AttributeQuery<A>) -> Self {
        let relation = relation_query::<A>(query.of, query.is.into(), Term::blank());
        Premise::Assert(Proposition::Relation(Box::new(relation)))
    }
}
