use crate::artifact::{Cause, Value};
use crate::attribute::Attribute;
use crate::attribute::AttributeDescriptor;
use crate::descriptor::Descriptor;
use crate::negation::Negation;
use crate::relation::query::RelationQuery;
use crate::statement::Statement;
use crate::types::Any;
use crate::types::Scalar;
use crate::{Cardinality, Entity, Premise, Proposition, Term, Transaction};
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

/// Trait for attribute types that support building expressions via `::of()`.
///
/// Provides the fluent `Name::of(entity).is(value)` syntax for creating
/// [`AttributeExpression`]s. Automatically implemented by `#[derive(Attribute)]`.
pub trait AttributeExpressionBuilder: Attribute {
    /// Start building an expression for this attribute on a given entity.
    ///
    /// The entity can be either a concrete [`Entity`] or a [`Term<Entity>`].
    fn of<Of>(entity: Of) -> AttributeBuilder<Self, Of>
    where
        Term<Entity>: From<Of>,
    {
        AttributeBuilder::new(entity)
    }
}

/// Intermediate builder produced by [`AttributeExpressionBuilder::of`].
///
/// Holds the entity (or entity term) and waits for [`.is()`](Self::is) to
/// produce an [`AttributeExpression`].
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

    /// Set the value for this attribute expression.
    ///
    /// Accepts **both** concrete values and query terms — the `Is` type
    /// parameter is preserved unevaluated in the resulting
    /// [`AttributeExpression`]. Downstream trait impls then impose
    /// additional constraints based on how the expression is used:
    ///
    /// - **Statement** (assert/retract): requires `Is: Into<A>`, so only
    ///   concrete values (e.g. `"Alice"`) are accepted.
    /// - **Query** (into [`Premise`]): requires `Is: IntoValueTerm<A>`,
    ///   so both concrete values and [`Term`] variables work.
    ///
    /// The `Into<Term<A::Type>>` bound here provides *construction-time*
    /// type safety — e.g. passing an `i32` for a `String` attribute is a
    /// compile error — without eagerly converting the value, which would
    /// erase the distinction between concrete and term values.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use dialog_query::{Attribute, Entity, Term};
    /// # #[derive(Attribute, Clone)] struct Name(pub String);
    /// # let alice = Entity::new().unwrap();
    /// // Concrete value can be used as statement or query:
    /// Name::of(alice.clone()).is("Alice");
    ///
    /// // Term variable query only:
    /// Name::of(Term::var("e")).is(Term::var("name"));
    /// ```
    pub fn is<Is: Into<Term<A::Type>>>(self, value: Is) -> AttributeExpression<A, Of, Is> {
        AttributeExpression {
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
pub type AttributeStatement<A> = AttributeExpression<A, Entity, A>;

/// A typed expression binding an attribute to an entity and a value.
///
/// The `Is` type parameter uses **deferred conversion**: the raw value
/// passed to [`.is()`](AttributeBuilder::is) is stored as-is. This lets
/// downstream trait impls impose their own constraints:
///
/// - [`Statement`] requires `Is: Into<A>` — only concrete values.
/// - [`From<...> for Premise`] requires `Is: IntoValueTerm<A>` — concrete
///   values or [`Term`] variables.
///
/// # Type Parameters
///
/// - `A` — the attribute type
/// - `Of` — entity position (`Entity` or `Term<Entity>`)
/// - `Is` — value position (deferred; e.g. `&str`, `A`, `Term<A::Type>`)
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
/// // Concrete: implements Statement (assert/retract)
/// let expr = Name::of(alice.clone()).is("Alice");
///
/// // Query: converts into Premise
/// let premise: dialog_query::Premise = Name::of(Term::var("e"))
///     .is(Term::<String>::var("v"))
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

// Statement: requires Is: Into<A>, so only concrete values are accepted.
impl<A, Is> Statement for AttributeExpression<A, Entity, Is, Option<Cause>>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    Is: Into<A>,
{
    fn assert(self, transaction: &mut Transaction) {
        let desc = <A as Descriptor<AttributeDescriptor>>::descriptor();
        let the = desc.the().clone();
        let attr: A = self.is.into();
        let value = attr.value().clone().into();
        if desc.cardinality() == Cardinality::One {
            transaction.associate_unique(the, self.of, value);
        } else {
            transaction.associate(the, self.of, value);
        }
    }

    fn retract(self, transaction: &mut Transaction) {
        let desc = <A as Descriptor<AttributeDescriptor>>::descriptor();
        let attr: A = self.is.into();
        transaction.dissociate(desc.the().clone(), self.of, attr.value().clone().into());
    }
}

// IntoIterator for concrete → single DynamicAttributeExpression
impl<A, Is> IntoIterator for AttributeExpression<A, Entity, Is, Option<Cause>>
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    Is: Into<A>,
{
    type Item = crate::attribute::DynamicAttributeExpression;
    type IntoIter = std::iter::Once<crate::attribute::DynamicAttributeExpression>;

    fn into_iter(self) -> Self::IntoIter {
        let desc = <A as Descriptor<AttributeDescriptor>>::descriptor();
        let attr: A = self.is.into();
        std::iter::once(crate::attribute::DynamicAttributeExpression {
            the: desc.the().clone(),
            of: self.of,
            is: attr.value().clone().into(),
            cause: None,
            cardinality: Some(desc.cardinality()),
        })
    }
}

// Into<Premise>: requires Of: Into<Term<Entity>> and Is: Into<Term<A::Type>>.
// The Into<Term<A::Type>> bound is already guaranteed by .is(), so any value
// that was accepted at construction time can be used as a query.
// The Term<A::Type> is then widened to Term<Any> for the underlying RelationQuery.
impl<A, Of, Is, Because> From<AttributeExpression<A, Of, Is, Because>> for Premise
where
    A: Attribute + Descriptor<AttributeDescriptor> + Clone,
    A::Type: Scalar,
    Of: Into<Term<Entity>>,
    Is: Into<Term<A::Type>>,
    Because: ExpressionCause,
{
    fn from(expression: AttributeExpression<A, Of, Is, Because>) -> Self {
        let value: Term<A::Type> = expression.is.into();
        let query = relation_query::<A>(
            expression.of.into(),
            value.into(),
            expression.cause.as_cause_term(),
        );
        Premise::Assert(Proposition::Relation(Box::new(query)))
    }
}

// Not for query expressions → Premise::Unless
// Uses the same bounds as From<...> for Premise to build the inner
// proposition, then wraps it in Unless for negation.
impl<A, Of, Is, Because> std::ops::Not for AttributeExpression<A, Of, Is, Because>
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

#[cfg(test)]
mod tests {
    use super::*;
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

    // -- Statement tests: .is() with concrete values --

    #[dialog_common::test]
    fn it_asserts_with_str() {
        let alice = Entity::new().unwrap();
        let statement = person::Name::of(alice).is("Alice");

        let mut transaction = Transaction::new();
        statement.assert(&mut transaction);
        assert!(!transaction.is_empty());
    }

    #[dialog_common::test]
    fn it_asserts_with_attribute_value() {
        let alice = Entity::new().unwrap();
        let statement = person::Name::of(alice).is(person::Name("Alice".into()));

        let mut transaction = Transaction::new();
        statement.assert(&mut transaction);
        assert!(!transaction.is_empty());
    }

    #[dialog_common::test]
    fn it_retracts_with_str() {
        let alice = Entity::new().unwrap();
        let statement = person::Name::of(alice).is("Alice");

        let mut transaction = Transaction::new();
        statement.retract(&mut transaction);
        assert!(!transaction.is_empty());
    }

    #[dialog_common::test]
    fn it_negates_concrete_expression() {
        // !expr now produces Premise::Unless (query-level negation),
        // not Retraction. For statement-level retraction, use .retract() directly.
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

        let items: Vec<crate::attribute::DynamicAttributeExpression> =
            statement.into_iter().collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].of, alice);
        assert_eq!(items[0].is, Value::String("Alice".into()));
    }

    // -- Query tests: .is() with Term variables --

    #[dialog_common::test]
    fn it_queries_with_both_terms() {
        // Both entity and value are variables — query only.
        let premise: Premise = person::Name::of(Term::var("e")).is(Term::var("v")).into();

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
    fn it_queries_with_concrete_entity_and_term_value() {
        // Concrete entity, variable value — query only.
        let alice = Entity::new().unwrap();
        let premise: Premise = person::Name::of(alice).is(Term::<String>::var("v")).into();

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
    fn it_queries_with_term_entity_and_concrete_value() {
        // Variable entity, concrete value — query via .is(&str).
        // &str: IntoValueTerm<Name> because &str: Into<Term<String>>
        // which satisfies the IntoValueTerm blanket for Term<A::Type>.
        // Wait — &str is not Term<A::Type>. We need an IntoValueTerm
        // impl for raw values too. Let's verify this works via Into<Premise>.
        let premise: Premise = person::Name::of(Term::var("e")).is("Alice").into();

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
    fn it_queries_with_both_concrete() {
        // Both concrete — works as both statement and query.
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

    // -- Negation tests --

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

    // -- Cause tests --

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
        let premise: Premise = person::Name::of(Term::var("e"))
            .is(Term::<String>::var("v"))
            .cause(Term::var("c"))
            .into();

        match premise {
            Premise::Assert(Proposition::Relation(query)) => {
                assert!(query.cause().is_variable());
                assert_eq!(query.cause().name(), Some("c"));
            }
            _ => panic!("Expected Relation premise"),
        }
    }

    // -- Deferred inference tests --
    // These validate that .is() defers conversion, allowing the same
    // expression to be used as a statement OR a query depending on context.

    #[dialog_common::test]
    fn it_uses_same_is_for_statement_and_query() {
        let alice = Entity::new().unwrap();

        // The same .is("Alice") expression works for both:

        // As a statement (assert):
        let expr = person::Name::of(alice.clone()).is("Alice");
        let mut transaction = Transaction::new();
        expr.assert(&mut transaction);
        assert!(!transaction.is_empty());

        // As a query (into Premise):
        let expression = person::Name::of(alice).is("Alice");
        let premise: Premise = expression.into();
        assert!(matches!(premise, Premise::Assert(Proposition::Relation(_))));
    }

    #[dialog_common::test]
    fn it_rejects_term_as_statement() {
        // This test documents that Term::var("v") cannot be used as a
        // statement — the code below should NOT compile. We verify the
        // positive path (Term works as query) and rely on the type system
        // to prevent the negative path.
        let alice = Entity::new().unwrap();
        let expression = person::Name::of(alice).is(Term::<String>::var("v"));

        // This compiles — Term works as query:
        let _premise: Premise = expression.into();

        // This would NOT compile — Term<String> does not impl Into<Name>:
        // let expr = person::Name::of(alice).is(Term::<String>::var("v"));
        // expr.assert(&mut transaction);
    }

    // -- Integration test --

    #[dialog_common::test]
    async fn it_roundtrips_assert_and_query() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::{Session, Term};
        use dialog_storage::MemoryStorageBackend;
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;

        let alice = Entity::new()?;

        // Assert via .is() with concrete &str
        let mut session = Session::open(store.clone());
        session
            .transact(person::Name::of(alice.clone()).is("Alice"))
            .await?;

        // Query via .is() with Term variable
        let premise: Premise = person::Name::of(alice.clone())
            .is(Term::<String>::var("name"))
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

        // Retract via .is() with concrete &str
        let mut session = Session::open(store.clone());
        session
            .transact(person::Name::of(alice.clone()).is("Alice"))
            .await?;

        Ok(())
    }
}
