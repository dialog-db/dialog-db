use std::any::type_name;

use crate::attribute::Attribute;
use crate::attribute::AttributeDescriptor;
use crate::attribute::expression::ExpressionCause;
use crate::attribute::expression::typed::{StaticAttributeExpression, StaticAttributeStatement};
use crate::attribute::query::dynamic::DynamicAttributeQuery;
use crate::descriptor::Descriptor;
use crate::query::Application;
use crate::selection::{Match, Selection};
use crate::source::Source;
use crate::types::Any;
use crate::types::Scalar;
use crate::{Entity, EvaluationError, Premise, Proposition, Term, Value};
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_effects::archive;

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

    fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        selection: M,
        source: &'a Source<'a, Env>,
    ) -> impl Selection + 'a
    where
        Env: Provider<archive::Get> + Provider<archive::Put> + ConditionalSync + 'static,
    {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Output;

    mod person {
        use crate::Attribute;

        #[derive(Attribute, Clone)]
        pub struct Name(pub String);
    }

    #[dialog_common::test]
    async fn it_converts_to_dynamic_query() {
        let alice = Entity::new().unwrap();

        let query = StaticAttributeQuery::<person::Name> {
            of: Term::from(alice),
            is: Term::var("name"),
        };

        let dynamic = DynamicAttributeQuery::from(query);

        assert!(dynamic.the().is_constant());
        assert!(dynamic.of().is_constant());
        assert!(dynamic.is().is_variable());
    }

    #[dialog_common::test]
    async fn it_converts_default_to_dynamic_query() {
        let query = StaticAttributeQuery::<person::Name>::default();
        let dynamic = DynamicAttributeQuery::from(query);

        assert!(dynamic.the().is_constant());
        assert!(dynamic.of().is_variable());
        assert!(dynamic.is().is_variable());
    }

    #[dialog_common::test]
    async fn it_performs_typed_query() -> anyhow::Result<()> {
        use crate::Transaction;
        use crate::session::RuleRegistry;
        use crate::source::Source;
        use dialog_repository::helpers::{test_operator, test_repo};

        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;

        let mut tx = Transaction::new();
        tx.assert(person::Name::of(alice.clone()).is("Alice"));
        branch.commit(tx.into_stream()).perform(&operator).await?;

        let query = StaticAttributeQuery::<person::Name> {
            of: Term::from(alice.clone()),
            is: Term::var("name"),
        };

        let source = Source::new(&branch, &operator, RuleRegistry::new());
        let results = Application::perform(query, &source).try_vec().await?;

        assert_eq!(results.len(), 1);

        let (of, is, _cause) = results.into_iter().next().unwrap().into_parts();
        assert_eq!(of, alice);
        assert_eq!(is.value(), &"Alice".to_string());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_roundtrips_assert_and_typed_query() -> anyhow::Result<()> {
        use crate::Transaction;
        use crate::session::RuleRegistry;
        use crate::source::Source;
        use dialog_repository::helpers::{test_operator, test_repo};

        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let mut tx = Transaction::new();
        tx.assert(person::Name::of(alice.clone()).is("Alice"));
        branch.commit(tx.into_stream()).perform(&operator).await?;

        let mut tx = Transaction::new();
        tx.assert(person::Name::of(bob.clone()).is("Bob"));
        branch.commit(tx.into_stream()).perform(&operator).await?;

        // Query all entities with default (all-variable) query.
        let query = StaticAttributeQuery::<person::Name>::default();

        let source = Source::new(&branch, &operator, RuleRegistry::new());
        let results = Application::perform(query, &source).try_vec().await?;

        // Cardinality::One means one value per entity, so we should get two results.
        assert_eq!(results.len(), 2);

        let names: Vec<_> = results
            .into_iter()
            .map(|r| {
                let (_of, is, _cause) = r.into_parts();
                is.value().clone()
            })
            .collect();

        assert!(names.contains(&"Alice".to_string()));
        assert!(names.contains(&"Bob".to_string()));

        Ok(())
    }
}
