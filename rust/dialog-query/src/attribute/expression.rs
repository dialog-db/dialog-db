use crate::artifact::Cause;
use crate::attribute::Attribute;
use crate::attribute::AttributeDescriptor;
use crate::descriptor::Descriptor;
use crate::negation::Negation;
use crate::relation::descriptor::RelationDescriptor;
use crate::relation::query::RelationQuery;
use crate::statement::{Retraction, Statement};
use crate::types::Scalar;
use crate::{Association, Cardinality, Entity, Parameter, Premise, Proposition, Term, Transaction};
use std::marker::PhantomData;

/// Cause-position in an [`AttributeExpression`].
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
        Term::Constant(self.clone())
    }
}

impl ExpressionCause for Option<Cause> {
    fn as_cause_term(&self) -> Term<Cause> {
        match self {
            Some(cause) => Term::Constant(cause.clone()),
            None => Term::blank(),
        }
    }
}

impl ExpressionCause for Term<Cause> {
    fn as_cause_term(&self) -> Term<Cause> {
        self.clone()
    }
}

/// Trait for attribute types that support building expressions via `::of()`.
///
/// Provides the fluent `Name::of(entity).is(value)` syntax for creating
/// [`AttributeExpression`]s. Automatically implemented by `#[derive(Attribute)]`.
pub trait AttributeExpressionBuilder: Attribute {
    /// Start building an expression for this attribute on a given entity.
    ///
    /// The entity can be either a concrete [`Entity`] or a [`Term<Entity>`].
    /// Call `.is(value)` for concrete values or `.matches(term)` for query terms.
    fn of<Of>(entity: Of) -> AttributeBuilder<Self, Of>
    where
        Term<Entity>: From<Of>,
    {
        AttributeBuilder::new(entity)
    }
}

/// Intermediate builder produced by [`AttributeExpressionBuilder::of`].
///
/// Holds the entity (or entity term) and waits for `.is(value)` or
/// `.matches(term)` to produce an [`AttributeExpression`].
pub struct AttributeBuilder<A: Attribute, Of>
where
    Term<Entity>: From<Of>,
{
    of: Of,
    _marker: PhantomData<A>,
}

impl<A: Attribute, Of> AttributeBuilder<A, Of>
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

    /// Set a concrete value, producing a statement-capable expression.
    ///
    /// Accepts anything that converts into the attribute type:
    /// ```no_run
    /// # use dialog_query::{Attribute, Entity};
    /// # #[derive(Attribute, Clone)] struct Name(pub String);
    /// # let alice = Entity::new().unwrap();
    /// Name::of(alice).is("Alice");
    /// ```
    pub fn is(self, value: impl Into<A>) -> AttributeExpression<A, Of, A> {
        AttributeExpression {
            of: self.of,
            is: value.into(),
            cause: None,
            _marker: PhantomData,
        }
    }

    /// Set a query term, producing a query-capable expression.
    ///
    /// Accepts anything that converts into `Term<A::Type>`:
    /// ```no_run
    /// # use dialog_query::{Attribute, Entity, Term};
    /// # #[derive(Attribute, Clone)] struct Name(pub String);
    /// # let alice = Entity::new().unwrap();
    /// Name::of(alice).matches(Term::<String>::var("name"));
    /// ```
    pub fn matches(
        self,
        term: impl Into<Term<A::Type>>,
    ) -> AttributeExpression<A, Of, Term<A::Type>>
    where
        A::Type: Scalar,
    {
        AttributeExpression {
            of: self.of,
            is: term.into(),
            cause: None,
            _marker: PhantomData,
        }
    }
}

/// A fully concrete attribute expression — entity and value are both known.
///
/// This is the form that implements [`Statement`] for assert/retract operations.
pub type AttributeStatement<A> = AttributeExpression<A, Entity, A>;

/// A typed expression binding an attribute to an entity and a value.
///
/// Generic over:
/// - `A` — the attribute type
/// - `Of` — entity position (`Entity` or `Term<Entity>`)
/// - `Is` — value position (`A` for concrete, `Term<A::Type>` for queries)
/// - `Because` — cause position (`Option<Cause>` or `Term<Cause>`)
///
/// # Examples
///
/// ```no_run
/// use dialog_query::{Attribute, Entity, Term};
///
/// #[derive(Attribute, Clone)]
/// struct Name(pub String);
///
/// let alice = Entity::new().unwrap();
///
/// // Concrete: implements Statement
/// let expr = Name::of(alice.clone()).is("Alice");
///
/// // Query: converts into Premise via .matches()
/// let premise: dialog_query::Premise = Name::of(Term::<Entity>::var("e"))
///     .matches(Term::<String>::var("v"))
///     .into();
/// ```
pub struct AttributeExpression<A: Attribute, Of, Is, Because: ExpressionCause = Option<Cause>> {
    /// The entity (or entity term) this attribute belongs to.
    pub of: Of,
    /// The attribute value (or value term).
    pub is: Is,
    /// Provenance/cause for this expression.
    pub cause: Because,
    _marker: PhantomData<A>,
}

impl<A: Attribute> AttributeStatement<A> {
    /// Create a fully concrete attribute expression (statement).
    pub fn statement(of: Entity, is: A) -> Self {
        AttributeExpression {
            of,
            is,
            cause: None,
            _marker: PhantomData,
        }
    }
}

impl<A: Attribute, Of, Is> AttributeExpression<A, Of, Is, Option<Cause>> {
    /// Attach a cause (provenance) to this expression.
    ///
    /// Accepts a concrete [`Cause`], [`Option<Cause>`], or a [`Term<Cause>`].
    pub fn cause<C: ExpressionCause>(self, cause: C) -> AttributeExpression<A, Of, Is, C> {
        AttributeExpression {
            of: self.of,
            is: self.is,
            cause,
            _marker: PhantomData,
        }
    }
}

pub(crate) fn relation_query<A: Attribute + Descriptor<AttributeDescriptor>>(
    of: Term<Entity>,
    is: impl Into<Parameter>,
    cause: Term<Cause>,
) -> RelationQuery {
    let desc = <A as Descriptor<AttributeDescriptor>>::descriptor();
    RelationQuery::new(
        Term::Constant(desc.the().clone()),
        of,
        is,
        cause,
        Some(RelationDescriptor::new(
            desc.content_type(),
            desc.cardinality(),
        )),
    )
}

// Statement impl: Entity + A (concrete value), Option<Cause>
impl<A> Statement for AttributeExpression<A, Entity, A, Option<Cause>>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
{
    fn assert(self, transaction: &mut Transaction) {
        let desc = <A as Descriptor<AttributeDescriptor>>::descriptor();
        let association = Association::new(desc.the().clone(), self.of, self.is.value().as_value());
        if desc.cardinality() == Cardinality::One {
            transaction.associate_unique(association);
        } else {
            transaction.associate(association);
        }
    }

    fn retract(self, transaction: &mut Transaction) {
        let desc = <A as Descriptor<AttributeDescriptor>>::descriptor();
        Association::new(desc.the().clone(), self.of, self.is.value().as_value())
            .retract(transaction);
    }
}

// Not for concrete (Entity, A) → Retraction
impl<A> std::ops::Not for AttributeExpression<A, Entity, A, Option<Cause>>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
{
    type Output = Retraction<Self>;

    fn not(self) -> Self::Output {
        self.revert()
    }
}

// IntoIterator for concrete → single Association
impl<A> IntoIterator for AttributeExpression<A, Entity, A, Option<Cause>>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
{
    type Item = Association;
    type IntoIter = std::iter::Once<Association>;

    fn into_iter(self) -> Self::IntoIter {
        let desc = <A as Descriptor<AttributeDescriptor>>::descriptor();
        std::iter::once(Association::new(
            desc.the().clone(),
            self.of,
            self.is.value().as_value(),
        ))
    }
}

// Into<Premise> — (Entity, A): concrete entity, concrete value
impl<A, Because> From<AttributeExpression<A, Entity, A, Because>> for Premise
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    Because: ExpressionCause,
{
    fn from(expr: AttributeExpression<A, Entity, A, Because>) -> Self {
        let query = relation_query::<A>(
            Term::Constant(expr.of),
            Parameter::Constant(expr.is.value().as_value()),
            expr.cause.as_cause_term(),
        );
        Premise::Assert(Proposition::Relation(Box::new(query)))
    }
}

// Into<Premise> — (Entity, Term<A::Type>): concrete entity, term value
impl<A, Because> From<AttributeExpression<A, Entity, Term<A::Type>, Because>> for Premise
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    A::Type: Scalar,
    Because: ExpressionCause,
{
    fn from(expr: AttributeExpression<A, Entity, Term<A::Type>, Because>) -> Self {
        let query =
            relation_query::<A>(Term::Constant(expr.of), expr.is, expr.cause.as_cause_term());
        Premise::Assert(Proposition::Relation(Box::new(query)))
    }
}

// Into<Premise> — (Term<Entity>, A): term entity, concrete value
impl<A, Because> From<AttributeExpression<A, Term<Entity>, A, Because>> for Premise
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    Because: ExpressionCause,
{
    fn from(expr: AttributeExpression<A, Term<Entity>, A, Because>) -> Self {
        let query = relation_query::<A>(
            expr.of,
            Parameter::Constant(expr.is.value().as_value()),
            expr.cause.as_cause_term(),
        );
        Premise::Assert(Proposition::Relation(Box::new(query)))
    }
}

// Into<Premise> — (Term<Entity>, Term<A::Type>): both terms
impl<A, Because> From<AttributeExpression<A, Term<Entity>, Term<A::Type>, Because>> for Premise
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    A::Type: Scalar,
    Because: ExpressionCause,
{
    fn from(expr: AttributeExpression<A, Term<Entity>, Term<A::Type>, Because>) -> Self {
        let query = relation_query::<A>(expr.of, expr.is, expr.cause.as_cause_term());
        Premise::Assert(Proposition::Relation(Box::new(query)))
    }
}

// Retraction<(Entity, A)> → Premise::Unless
impl<A> From<Retraction<AttributeExpression<A, Entity, A, Option<Cause>>>> for Premise
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
{
    fn from(retraction: Retraction<AttributeExpression<A, Entity, A, Option<Cause>>>) -> Self {
        let expr = !retraction; // unwrap via Not
        let premise: Premise = expr.into();
        match premise {
            Premise::Assert(prop) => Premise::Unless(Negation::not(prop)),
            other => other,
        }
    }
}

// Not for (Entity, Term) → Premise::Unless
impl<A, Because> std::ops::Not for AttributeExpression<A, Entity, Term<A::Type>, Because>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    A::Type: Scalar,
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

// Not for (Term<Entity>, A) → Premise::Unless
impl<A, Because> std::ops::Not for AttributeExpression<A, Term<Entity>, A, Because>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
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

// Not for (Term<Entity>, Term<A::Type>) → Premise::Unless
impl<A, Because> std::ops::Not for AttributeExpression<A, Term<Entity>, Term<A::Type>, Because>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    A::Type: Scalar,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::Value;
    use crate::attribute::Attribute;
    use crate::premise::Premise;
    use crate::proposition::Proposition;
    use crate::session::transaction::Edit;
    use crate::statement::Statement;

    mod person {
        use crate::Attribute;

        /// Name of the person
        #[derive(Attribute, Clone)]
        pub struct Name(pub String);
    }

    #[dialog_common::test]
    fn it_creates_concrete_expression() {
        let alice = Entity::new().unwrap();
        let expr = person::Name::of(alice.clone()).is("Alice");

        assert_eq!(expr.of, alice);
        assert_eq!(expr.is.value(), "Alice");
    }

    #[dialog_common::test]
    fn it_asserts_concrete_expression() {
        let alice = Entity::new().unwrap();
        let expr = person::Name::of(alice).is("Alice");

        let mut transaction = Transaction::new();
        expr.assert(&mut transaction);
        assert!(!transaction.is_empty());
    }

    #[dialog_common::test]
    fn it_asserts_with_attribute_value() {
        let alice = Entity::new().unwrap();
        let expr = person::Name::of(alice).is(person::Name("Alice".into()));

        let mut transaction = Transaction::new();
        expr.assert(&mut transaction);
        assert!(!transaction.is_empty());
    }

    #[dialog_common::test]
    fn it_retracts_concrete_expression() {
        let alice = Entity::new().unwrap();
        let expr = person::Name::of(alice).is("Alice");

        let mut transaction = Transaction::new();
        expr.retract(&mut transaction);
        assert!(!transaction.is_empty());
    }

    #[dialog_common::test]
    fn it_negates_concrete_expression() {
        let alice = Entity::new().unwrap();
        let expr = person::Name::of(alice).is("Alice");
        let retraction = !expr;

        let mut transaction = Transaction::new();
        retraction.merge(&mut transaction);
        assert!(!transaction.is_empty());
    }

    #[dialog_common::test]
    fn it_iterates_concrete_expression() {
        let alice = Entity::new().unwrap();
        let expr = person::Name::of(alice.clone()).is("Alice");

        let associations: Vec<Association> = expr.into_iter().collect();
        assert_eq!(associations.len(), 1);
        assert_eq!(associations[0].of, alice);
        assert_eq!(associations[0].is, Value::String("Alice".into()));
    }

    #[dialog_common::test]
    fn it_converts_both_terms_to_premise() {
        let premise: Premise = person::Name::of(Term::<Entity>::var("e"))
            .matches(Term::<String>::var("v"))
            .into();

        match premise {
            Premise::Assert(Proposition::Relation(query)) => {
                assert!(query.the().is_constant());
                assert!(query.of().is_variable());
                assert!(query.is().is_variable());
            }
            _ => panic!("Expected Relation premise"),
        }
    }

    #[dialog_common::test]
    fn it_converts_entity_constant_value_term_to_premise() {
        let alice = Entity::new().unwrap();
        let premise: Premise = person::Name::of(alice.clone())
            .matches(Term::<String>::var("v"))
            .into();

        match premise {
            Premise::Assert(Proposition::Relation(query)) => {
                assert!(query.the().is_constant());
                assert!(query.of().is_constant());
                assert!(query.is().is_variable());
            }
            _ => panic!("Expected Relation premise"),
        }
    }

    #[dialog_common::test]
    fn it_converts_entity_term_value_constant_to_premise() {
        let premise: Premise = person::Name::of(Term::<Entity>::var("e"))
            .is("Alice")
            .into();

        match premise {
            Premise::Assert(Proposition::Relation(query)) => {
                assert!(query.the().is_constant());
                assert!(query.of().is_variable());
                assert!(query.is().is_constant());
            }
            _ => panic!("Expected Relation premise"),
        }
    }

    #[dialog_common::test]
    fn it_converts_both_concrete_to_premise() {
        let alice = Entity::new().unwrap();
        let premise: Premise = person::Name::of(alice).is("Alice").into();

        match premise {
            Premise::Assert(Proposition::Relation(query)) => {
                assert!(query.the().is_constant());
                assert!(query.of().is_constant());
                assert!(query.is().is_constant());
            }
            _ => panic!("Expected Relation premise"),
        }
    }

    #[dialog_common::test]
    fn it_negates_query_expression() {
        let alice = Entity::new().unwrap();
        let premise = !person::Name::of(alice).matches(Term::<String>::var("v"));

        match premise {
            Premise::Unless(_) => {}
            _ => panic!("Expected Unless premise"),
        }
    }

    #[dialog_common::test]
    fn it_negates_both_terms_expression() {
        let premise = !person::Name::of(Term::<Entity>::var("e")).matches(Term::<String>::var("v"));

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
            Premise::Assert(Proposition::Relation(query)) => {
                assert!(query.cause().is_constant());
            }
            _ => panic!("Expected Relation premise"),
        }
    }

    #[dialog_common::test]
    fn it_attaches_term_cause() {
        let premise: Premise = person::Name::of(Term::<Entity>::var("e"))
            .matches(Term::<String>::var("v"))
            .cause(Term::<Cause>::var("c"))
            .into();

        match premise {
            Premise::Assert(Proposition::Relation(query)) => {
                assert!(query.cause().is_variable());
                assert_eq!(query.cause().name(), Some("c"));
            }
            _ => panic!("Expected Relation premise"),
        }
    }

    #[dialog_common::test]
    async fn it_roundtrips_assert_and_query() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::{Session, Term};
        use dialog_storage::MemoryStorageBackend;
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;

        let alice = Entity::new()?;

        // Assert via expression — .is() with &str
        let mut session = Session::open(store.clone());
        session
            .transact(person::Name::of(alice.clone()).is("Alice"))
            .await?;

        // Query via expression — .matches() with Term
        let premise: Premise = person::Name::of(alice.clone())
            .matches(Term::<String>::var("name"))
            .into();

        let prop = match premise {
            Premise::Assert(prop) => prop,
            _ => panic!("Expected Assert"),
        };

        use crate::selection::Answer;
        let results = prop
            .evaluate(Answer::new().seed(), &Session::open(store.clone()))
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(results.len(), 1);

        // Retract via expression
        let mut session = Session::open(store.clone());
        session
            .transact(person::Name::of(alice.clone()).is("Alice"))
            .await?;

        Ok(())
    }
}
