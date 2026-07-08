use std::any::type_name;

use crate::attribute::Attribute;
use crate::attribute::AttributeDescriptor;
use crate::attribute::expression::ExpressionCause;
use crate::attribute::expression::typed::{StaticAttributeExpression, StaticAttributeStatement};
use crate::attribute::query::dynamic::DynamicAttributeQuery;
use crate::descriptor::Descriptor;
use crate::query::Application;
use crate::selection::{Match, Selection};
use crate::source::SelectRules;
use crate::types::Any;
use crate::types::Scalar;
use crate::{Entity, EvaluationError, Premise, Proposition, Term, Value};
use dialog_artifacts::Select;
use dialog_capability::Provider;
use dialog_common::{ConditionalSend, ConditionalSync};

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
    A: Attribute + Descriptor<AttributeDescriptor> + Clone + ConditionalSend + 'static,
    A: From<A::Type>,
    A::Type: Scalar + TryFrom<Value>,
{
    type Conclusion = StaticAttributeStatement<A>;

    fn evaluate<'a, Env, M: Selection + 'a>(self, selection: M, env: &'a Env) -> impl Selection + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        let query = DynamicAttributeQuery::from(self);
        Application::evaluate(query, selection, env)
    }

    fn realize(&self, input: Match) -> Result<Self::Conclusion, EvaluationError> {
        let of_term = &self.of;
        let is_param = Term::<Any>::from(&self.is);
        let entity: Entity = Entity::try_from(input.lookup(&Term::from(of_term))?.content()?)?;
        let value: Value = input.lookup(&is_param)?.content()?;
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
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::query::Output;
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use dialog_repository::helpers::{test_operator_with_profile, test_repo};

    mod person {
        use crate::Attribute;

        #[derive(Attribute, Clone)]
        pub struct Name(pub String);
    }

    mod note {
        use crate::Attribute;
        use crate::artifact::{RecordError, RecordFormat, Recorded};

        /// A toy record format: a list of lines, encoded newline-joined.
        #[derive(Clone, Debug, PartialEq)]
        pub struct Journal(pub Vec<String>);

        impl RecordFormat for Journal {
            fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
                let text = str::from_utf8(bytes)
                    .map_err(|error| RecordError::Decode(error.to_string()))?;
                Ok(Journal(match text {
                    "" => Vec::new(),
                    text => text.split('\n').map(String::from).collect(),
                }))
            }

            fn encode(&self) -> Result<Vec<u8>, RecordError> {
                Ok(self.0.join("\n").into_bytes())
            }
        }

        /// Collaboratively edited body
        #[derive(Attribute, Clone)]
        pub struct Body(pub Recorded<Journal>);
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;

        branch
            .transaction()
            .assert(person::Name::of(alice.clone()).is("Alice"))
            .commit()
            .perform(&operator)
            .await?;

        let query = StaticAttributeQuery::<person::Name> {
            of: Term::from(alice.clone()),
            is: Term::var("name"),
        };

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = Application::perform(query, &source).try_vec().await?;

        assert_eq!(results.len(), 1);

        let (of, is, _cause) = results.into_iter().next().unwrap().into_parts();
        assert_eq!(of, alice);
        assert_eq!(is.value(), &"Alice".to_string());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_roundtrips_record_attribute_through_store() -> anyhow::Result<()> {
        use crate::artifact::Recorded;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let doc = Entity::new()?;
        let first = Recorded::new(note::Journal(vec!["hello".into()]))?;

        branch
            .transaction()
            .assert(note::Body::of(doc.clone()).is(first.clone()))
            .commit()
            .perform(&operator)
            .await?;

        let query = StaticAttributeQuery::<note::Body> {
            of: Term::from(doc.clone()),
            is: Term::var("body"),
        };
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = Application::perform(query, &source).try_vec().await?;

        assert_eq!(results.len(), 1);
        let (of, is, _cause) = results.into_iter().next().unwrap().into_parts();
        assert_eq!(of, doc);
        // The handle was hydrated from stored bytes without decoding;
        // realize decodes on this first access.
        assert_eq!(is.value(), &first);
        assert_eq!(*is.value().realize()?, note::Journal(vec!["hello".into()]));

        // A `Cardinality::One` typed assert supersedes the prior value
        // (`Instruction::Replace`), so a second write leaves one sibling.
        let second = Recorded::new(note::Journal(vec!["hello".into(), "world".into()]))?;

        branch
            .transaction()
            .assert(note::Body::of(doc.clone()).is(second.clone()))
            .commit()
            .perform(&operator)
            .await?;

        let query = StaticAttributeQuery::<note::Body> {
            of: Term::from(doc.clone()),
            is: Term::var("body"),
        };
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = Application::perform(query, &source).try_vec().await?;

        assert_eq!(results.len(), 1);
        let (_of, is, _cause) = results.into_iter().next().unwrap().into_parts();
        assert_eq!(is.value(), &second);

        // Value-bound queries: the live value matches; the superseded one
        // yields no rows.
        let query = StaticAttributeQuery::<note::Body> {
            of: Term::var("doc"),
            is: Term::from(second.clone()),
        };
        let results = Application::perform(query, &source).try_vec().await?;
        assert_eq!(results.len(), 1);

        let query = StaticAttributeQuery::<note::Body> {
            of: Term::var("doc"),
            is: Term::from(first),
        };
        let results = Application::perform(query, &source).try_vec().await?;
        assert_eq!(results.len(), 0);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_roundtrips_assert_and_typed_query() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        branch
            .transaction()
            .assert(person::Name::of(alice.clone()).is("Alice"))
            .commit()
            .perform(&operator)
            .await?;

        branch
            .transaction()
            .assert(person::Name::of(bob.clone()).is("Bob"))
            .commit()
            .perform(&operator)
            .await?;

        // Query all entities with default (all-variable) query.
        let query = StaticAttributeQuery::<person::Name>::default();

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
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
