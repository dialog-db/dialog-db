use crate::artifact::Cause;
use crate::attribute::Attribute;
use crate::attribute::AttributeDescriptor;
use crate::attribute::statement::AttributeStatement;
use crate::descriptor::Descriptor;
use crate::negation::Negation;
use crate::statement::Statement;
use crate::types::Scalar;
use crate::{Cardinality, Entity, Premise, Proposition, Term};
use dialog_artifacts::Update;
use std::iter;
use std::marker::PhantomData;
use std::ops::Not;

use crate::artifact::Value;
use crate::attribute::query::dynamic::DynamicAttributeQuery;

use super::ExpressionCause;
use super::dynamic::DynamicAttributeExpression;

/// Trait for attribute types that support building expressions via `::of()`.
///
/// Provides the fluent `Name::of(entity).is(value)` syntax for creating
/// [`StaticAttributeExpression`]s. Automatically implemented by `#[derive(Attribute)]`.
pub trait StaticAttributeExpressionBuilder: Attribute {
    /// Start building an expression for this attribute on a given entity.
    ///
    /// The entity can be either a concrete [`Entity`] or a [`Term<Entity>`].
    fn of<Of>(entity: Of) -> StaticAttributeBuilder<Self, Of>
    where
        Term<Entity>: From<Of>,
    {
        StaticAttributeBuilder::new(entity)
    }
}

/// Intermediate builder produced by [`StaticAttributeExpressionBuilder::of`].
///
/// Holds the entity (or entity term) and waits for [`.is()`](Self::is) to
/// produce a [`StaticAttributeExpression`].
pub struct StaticAttributeBuilder<A: Attribute, Of>
where
    Term<Entity>: From<Of>,
{
    of: Of,
    _marker: PhantomData<A>,
}

impl<A: Attribute, Of> StaticAttributeBuilder<A, Of>
where
    Term<Entity>: From<Of>,
{
    /// Create a new builder for the given entity-position value.
    pub fn new(of: Of) -> Self {
        Self {
            of,
            _marker: PhantomData,
        }
    }

    /// Set the value for this attribute expression.
    ///
    /// Accepts **both** concrete values and query terms — the `Is` type
    /// parameter is preserved unevaluated in the resulting
    /// [`StaticAttributeExpression`]. Downstream trait impls then impose
    /// additional constraints based on how the expression is used:
    ///
    /// - **Statement** (assert/retract): requires `Is: Into<A>`, so only
    ///   concrete values (e.g. `"Alice"`) are accepted.
    /// - **Query** (into [`Premise`]): requires `Is: Into<Term<A::Type>>`,
    ///   so both concrete values and [`Term`] variables work.
    ///
    /// The `Into<Term<A::Type>>` bound here provides *construction-time*
    /// type safety — e.g. passing an `i32` for a `String` attribute is a
    /// compile error — without eagerly converting the value, which would
    /// erase the distinction between concrete and term values.
    pub fn is<Is: Into<Term<A::Type>>>(self, value: Is) -> StaticAttributeExpression<A, Of, Is> {
        StaticAttributeExpression {
            of: self.of,
            is: value,
            cause: None,
            _marker: PhantomData,
        }
    }
}

/// A fully concrete attribute expression — entity and value are both known.
///
/// This is the form that implements [`Statement`] for assert/retract operations.
pub type StaticAttributeStatement<A> = StaticAttributeExpression<A, Entity, A>;

/// A typed expression binding an attribute to an entity and a value.
///
/// The `Is` type parameter uses **deferred conversion**: the raw value
/// passed to [`.is()`](StaticAttributeBuilder::is) is stored as-is. This lets
/// downstream trait impls impose their own constraints:
///
/// - [`Statement`] requires `Is: Into<A>` — only concrete values.
/// - [`From<...> for Premise`] requires `Is: Into<Term<A::Type>>` — concrete
///   values or [`Term`] variables.
///
/// # Type Parameters
///
/// - `A` — the attribute type
/// - `Of` — entity position (`Entity` or `Term<Entity>`)
/// - `Is` — value position (deferred; e.g. `&str`, `A`, `Term<A::Type>`)
/// - `Because` — cause position (`Option<Cause>` or `Term<Cause>`)
pub struct StaticAttributeExpression<A: Attribute, Of, Is, Because: ExpressionCause = Option<Cause>>
{
    /// The entity (or entity term) this attribute belongs to.
    of: Of,
    /// The attribute value (or value term).
    is: Is,
    /// Provenance/cause for this expression.
    cause: Because,
    _marker: PhantomData<A>,
}

impl<A: Attribute, Of, Is, Because: ExpressionCause> StaticAttributeExpression<A, Of, Is, Because> {
    /// Consume the expression and return its parts.
    pub fn into_parts(self) -> (Of, Is, Because) {
        (self.of, self.is, self.cause)
    }
}

impl<A: Attribute> StaticAttributeStatement<A> {
    /// Create a fully concrete attribute expression (statement).
    pub fn statement(of: Entity, is: A) -> Self {
        StaticAttributeExpression {
            of,
            is,
            cause: None,
            _marker: PhantomData,
        }
    }
}

impl<A: Attribute, Of, Is> StaticAttributeExpression<A, Of, Is, Option<Cause>> {
    /// Attach a cause (provenance) to this expression.
    ///
    /// Accepts a concrete [`Cause`], [`Option<Cause>`], or a [`Term<Cause>`].
    pub fn cause<C: ExpressionCause>(self, cause: C) -> StaticAttributeExpression<A, Of, Is, C> {
        let (of, is, _) = self.into_parts();
        StaticAttributeExpression {
            of,
            is,
            cause,
            _marker: PhantomData,
        }
    }
}

// Statement: requires Is: Into<A>, so only concrete values are accepted.
impl<A, Is> Statement for StaticAttributeExpression<A, Entity, Is, Option<Cause>>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    Is: Into<A>,
{
    fn assert(self, update: &mut impl Update) {
        let (of, is, _) = self.into_parts();
        let desc = <A as Descriptor<AttributeDescriptor>>::descriptor();
        let the = desc.the().clone();
        let attr: A = is.into();
        let value = attr.value().clone().into();
        if desc.cardinality() == Cardinality::One {
            update.associate_unique(the.into(), of, value);
        } else {
            update.associate(the.into(), of, value);
        }
    }

    fn retract(self, update: &mut impl Update) {
        let (of, is, _) = self.into_parts();
        let desc = <A as Descriptor<AttributeDescriptor>>::descriptor();
        let attr: A = is.into();
        update.dissociate(desc.the().clone().into(), of, attr.value().clone().into());
    }
}

// IntoIterator for concrete → single AttributeStatement (type-erased)
impl<A, Is> IntoIterator for StaticAttributeExpression<A, Entity, Is, Option<Cause>>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    Is: Into<A>,
{
    type Item = AttributeStatement;
    type IntoIter = iter::Once<AttributeStatement>;

    fn into_iter(self) -> Self::IntoIter {
        let (of, is, cause) = self.into_parts();
        let desc = <A as Descriptor<AttributeDescriptor>>::descriptor();
        let attr: A = is.into();
        iter::once(DynamicAttributeExpression {
            the: desc.the().clone(),
            of,
            is: attr.value().clone().into(),
            cause,
            cardinality: Some(desc.cardinality()),
        })
    }
}

// Into<Premise>: requires Of: Into<Term<Entity>> and Is: Into<Term<A::Type>>.
impl<A, Of, Is, Because> From<StaticAttributeExpression<A, Of, Is, Because>> for Premise
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    A::Type: Scalar,
    Of: Into<Term<Entity>>,
    Is: Into<Term<A::Type>>,
    Because: ExpressionCause,
{
    fn from(expression: StaticAttributeExpression<A, Of, Is, Because>) -> Self {
        let (of, is, cause) = expression.into_parts();
        let value: Term<A::Type> = is.into();
        let descriptor = <A as Descriptor<AttributeDescriptor>>::descriptor();
        let query = DynamicAttributeQuery::new(
            Term::Constant(Value::from(descriptor.the().clone())),
            of.into(),
            value.into(),
            cause.as_cause_term(),
            Some(descriptor.cardinality()),
        );
        Premise::Assert(Proposition::Attribute(Box::new(query)))
    }
}

// Not for query expressions → Premise::Unless
impl<A, Of, Is, Because> Not for StaticAttributeExpression<A, Of, Is, Because>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    A::Type: Scalar,
    Of: Into<Term<Entity>>,
    Is: Into<Term<A::Type>>,
    Because: ExpressionCause,
{
    type Output = Premise;

    fn not(self) -> Self::Output {
        let premise: Premise = self.into();
        match premise {
            Premise::Assert(prop) => Premise::Unless(Negation::not(prop)),
            other => other,
        }
    }
}

// From<StaticAttributeExpression> for AttributeStatement (type erasure)
impl<A, Is> From<StaticAttributeExpression<A, Entity, Is, Option<Cause>>> for AttributeStatement
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    Is: Into<A>,
{
    fn from(expr: StaticAttributeExpression<A, Entity, Is, Option<Cause>>) -> Self {
        let (of, is, cause) = expr.into_parts();
        let desc = <A as Descriptor<AttributeDescriptor>>::descriptor();
        let attr: A = is.into();
        DynamicAttributeExpression {
            the: desc.the().clone(),
            of,
            is: attr.value().clone().into(),
            cause,
            cardinality: Some(desc.cardinality()),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::Changes;
    use crate::Match;
    use crate::artifact::Value;
    use crate::premise::Premise;
    use crate::proposition::Proposition;
    use crate::statement::Statement;

    mod person {
        use crate::Attribute;

        /// Name of the person
        #[derive(Attribute, Clone)]
        pub struct Name(pub String);
    }

    #[dialog_common::test]
    fn it_asserts_with_str() {
        let alice = Entity::new().unwrap();
        let statement = person::Name::of(alice).is("Alice");

        let mut changes = Changes::new();
        statement.assert(&mut changes);
        assert!(!changes.is_empty());
    }

    #[dialog_common::test]
    fn it_asserts_with_attribute_value() {
        let alice = Entity::new().unwrap();
        let statement = person::Name::of(alice).is(person::Name("Alice".into()));

        let mut changes = Changes::new();
        statement.assert(&mut changes);
        assert!(!changes.is_empty());
    }

    #[dialog_common::test]
    fn it_retracts_with_str() {
        let alice = Entity::new().unwrap();
        let statement = person::Name::of(alice).is("Alice");

        let mut changes = Changes::new();
        statement.retract(&mut changes);
        assert!(!changes.is_empty());
    }

    #[dialog_common::test]
    fn it_negates_concrete_expression() {
        let alice = Entity::new().unwrap();
        let premise = !person::Name::of(alice).is("Alice");

        match premise {
            Premise::Unless(_) => {}
            _ => panic!("Expected Unless premise"),
        }
    }

    #[dialog_common::test]
    fn it_iterates_concrete_expression() {
        let alice = Entity::new().unwrap();
        let statement = person::Name::of(alice.clone()).is("Alice");

        let items: Vec<AttributeStatement> = statement.into_iter().collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].of, alice);
        assert_eq!(items[0].is, Value::String("Alice".into()));
    }

    #[dialog_common::test]
    fn it_queries_with_both_terms() {
        let premise: Premise = person::Name::of(Term::var("e")).is(Term::var("v")).into();

        match premise {
            Premise::Assert(Proposition::Attribute(query)) => {
                assert!(query.the().is_constant());
                assert!(query.of().is_variable());
                assert!(query.is().is_variable());
            }
            _ => panic!("Expected Relation premise"),
        }
    }

    #[dialog_common::test]
    fn it_queries_with_concrete_entity_and_term_value() {
        let alice = Entity::new().unwrap();
        let premise: Premise = person::Name::of(alice).is(Term::<String>::var("v")).into();

        match premise {
            Premise::Assert(Proposition::Attribute(query)) => {
                assert!(query.the().is_constant());
                assert!(query.of().is_constant());
                assert!(query.is().is_variable());
            }
            _ => panic!("Expected Relation premise"),
        }
    }

    #[dialog_common::test]
    fn it_queries_with_term_entity_and_concrete_value() {
        let premise: Premise = person::Name::of(Term::var("e")).is("Alice").into();

        match premise {
            Premise::Assert(Proposition::Attribute(query)) => {
                assert!(query.the().is_constant());
                assert!(query.of().is_variable());
                assert!(query.is().is_constant());
            }
            _ => panic!("Expected Relation premise"),
        }
    }

    #[dialog_common::test]
    fn it_queries_with_both_concrete() {
        let alice = Entity::new().unwrap();
        let premise: Premise = person::Name::of(alice).is("Alice").into();

        match premise {
            Premise::Assert(Proposition::Attribute(query)) => {
                assert!(query.the().is_constant());
                assert!(query.of().is_constant());
                assert!(query.is().is_constant());
            }
            _ => panic!("Expected Relation premise"),
        }
    }

    #[dialog_common::test]
    fn it_negates_term_value_expression() {
        let alice = Entity::new().unwrap();
        let premise = !person::Name::of(alice).is(Term::<String>::var("v"));

        match premise {
            Premise::Unless(_) => {}
            _ => panic!("Expected Unless premise"),
        }
    }

    #[dialog_common::test]
    fn it_negates_both_terms_expression() {
        let premise = !person::Name::of(Term::var("e")).is(Term::<String>::var("v"));

        match premise {
            Premise::Unless(_) => {}
            _ => panic!("Expected Unless premise"),
        }
    }

    #[dialog_common::test]
    fn it_attaches_concrete_cause() {
        let alice = Entity::new().unwrap();
        let provenance = Cause([1u8; 32]);
        let premise: Premise = person::Name::of(alice).is("Alice").cause(provenance).into();

        match premise {
            Premise::Assert(Proposition::Attribute(query)) => {
                assert!(query.cause().is_constant());
            }
            _ => panic!("Expected Relation premise"),
        }
    }

    #[dialog_common::test]
    fn it_attaches_term_cause() {
        let premise: Premise = person::Name::of(Term::var("e"))
            .is(Term::<String>::var("v"))
            .cause(Term::var("c"))
            .into();

        match premise {
            Premise::Assert(Proposition::Attribute(query)) => {
                assert!(query.cause().is_variable());
                assert_eq!(query.cause().name(), Some("c"));
            }
            _ => panic!("Expected Relation premise"),
        }
    }

    #[dialog_common::test]
    fn it_uses_same_is_for_statement_and_query() {
        let alice = Entity::new().unwrap();

        let expr = person::Name::of(alice.clone()).is("Alice");
        let mut changes = Changes::new();
        expr.assert(&mut changes);
        assert!(!changes.is_empty());

        let expression = person::Name::of(alice).is("Alice");
        let premise: Premise = expression.into();
        assert!(matches!(
            premise,
            Premise::Assert(Proposition::Attribute(_))
        ));
    }

    #[dialog_common::test]
    fn it_rejects_term_as_statement() {
        let alice = Entity::new().unwrap();
        let expression = person::Name::of(alice).is(Term::<String>::var("v"));
        let _premise: Premise = expression.into();
    }

    #[dialog_common::test]
    async fn it_roundtrips_assert_and_query() -> anyhow::Result<()> {
        use crate::Term;
        use crate::session::RuleRegistry;
        use crate::source::test::TestEnv;
        use dialog_repository::helpers::{test_operator, test_repo};
        use futures_util::TryStreamExt;

        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;

        branch
            .edit()
            .assert(person::Name::of(alice.clone()).is("Alice"))
            .commit()
            .perform(&operator)
            .await?;

        let premise: Premise = person::Name::of(alice.clone())
            .is(Term::<String>::var("name"))
            .into();

        let prop = match premise {
            Premise::Assert(prop) => prop,
            _ => panic!("Expected Assert"),
        };

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = prop
            .evaluate(Match::new().seed(), &source)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(results.len(), 1);

        Ok(())
    }

    #[dialog_common::test]
    fn it_converts_to_attribute_statement() {
        let alice = Entity::new().unwrap();
        let expr = person::Name::of(alice.clone()).is("Alice");
        let stmt: AttributeStatement = expr.into();
        assert_eq!(stmt.of, alice);
        assert_eq!(stmt.is, Value::String("Alice".into()));
        assert_eq!(stmt.cardinality, Some(Cardinality::One));
    }
}
