use crate::artifact::{Cause, Entity, Value};
use crate::attribute::The;
use crate::attribute::query::AttributeQuery;
use crate::attribute::statement::AttributeStatement;
use crate::negation::Negation;
use crate::query::Output;
use crate::schema::Cardinality;
use crate::source::Source;
use crate::statement::Statement;
use crate::term::Term;
use crate::types::{Scalar, Typed};
use crate::{Claim, Premise, Proposition, Transaction};
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_effects::archive;
use std::ops::Not;

/// Converts a value into a [`Term`], resolving the type unambiguously
/// from the input.
///
/// Implemented for:
/// - Concrete scalar values (`String`, `u32`, etc.) — produces a constant term.
/// - [`Term<T>`] variables — passes through unchanged.
pub trait IntoTerm {
    /// The scalar type this value represents.
    type Type: Typed;
    /// Convert into a [`Term`] of the associated type.
    fn into_term(self) -> Term<Self::Type>;
}

impl<T: Scalar> IntoTerm for T {
    type Type = T;
    fn into_term(self) -> Term<T> {
        Term::Constant(self.into())
    }
}

impl<T: Typed> IntoTerm for Term<T> {
    type Type = T;
    fn into_term(self) -> Term<T> {
        self
    }
}

/// Intermediate builder produced by [`The::of`] or [`Term<The>::of`].
/// Call [`.is()`](Self::is) to supply the value and obtain a
/// [`DynamicAttributeExpression`].
pub struct DynamicAttributeExpressionBuilder<The, Of> {
    /// The attribute (concrete or variable).
    pub the: The,
    /// The entity (or entity term).
    pub of: Of,
}

impl<T, Of> DynamicAttributeExpressionBuilder<T, Of> {
    /// Set the value for this dynamic attribute expression.
    ///
    /// Accepts concrete scalar values (`"Alice"`, `25u32`),
    /// [`Term`] variables (`Term::<String>::var("name")`), and
    /// [`The`] identifiers or [`Term<The>`] variables for querying
    /// relations themselves (`Term::<The>::var("relation")`).
    pub fn is<V: IntoTerm>(self, value: V) -> DynamicAttributeExpression<T, Of, V> {
        DynamicAttributeExpression {
            the: self.the,
            of: self.of,
            is: value,
            cause: None,
            cardinality: None,
        }
    }
}

/// A dynamic attribute expression binding an attribute name to an entity
/// and a value.
///
/// All three positions use deferred conversion — raw values are stored
/// as-is and converted only when needed for queries or statements.
///
/// - [`Statement`] requires `Relation = The`, `Of = Entity`, `Is: Scalar`
///   — all concrete positions.
/// - [`From<...> for Premise`] requires each position to convert into
///   the corresponding [`Term`].
#[derive(Clone, Debug)]
pub struct DynamicAttributeExpression<The, Of, Is> {
    /// The attribute (predicate), concrete or variable.
    pub the: The,
    /// The entity (or entity term).
    pub of: Of,
    /// The value, concrete or a [`Term`] variable.
    pub is: Is,
    /// Provenance/cause for this expression.
    pub cause: Option<Cause>,
    /// Optional cardinality override. When `Some(Cardinality::One)`,
    /// `assert` uses `associate_unique`.
    pub cardinality: Option<Cardinality>,
}

impl<The, Of, Is> DynamicAttributeExpression<The, Of, Is> {
    /// Set the cardinality for this expression.
    pub fn cardinality(mut self, cardinality: Cardinality) -> Self {
        self.cardinality = Some(cardinality);
        self
    }

    /// Set the caus for this expression
    pub fn cause(mut self, cause: Cause) -> Self {
        self.cause = Some(cause);
        self
    }
}

/// Convert a dynamic expression into an [`AttributeQuery`].
impl<Relation, Of, Is> From<DynamicAttributeExpression<Relation, Of, Is>> for AttributeQuery
where
    Relation: Into<Term<The>>,
    Of: Into<Term<Entity>>,
    Is: IntoTerm,
    Is::Type: Scalar,
{
    fn from(expression: DynamicAttributeExpression<Relation, Of, Is>) -> Self {
        let value: Term<Value> = expression.is.into_term().into();
        AttributeQuery::new(
            expression.the.into(),
            expression.of.into(),
            value.into(),
            match expression.cause {
                Some(c) => Term::Constant(Value::from(c)),
                None => Term::blank(),
            },
            expression.cardinality,
        )
    }
}

impl<Relation, Of, Is> DynamicAttributeExpression<Relation, Of, Is>
where
    Relation: Into<Term<The>>,
    Of: Into<Term<Entity>>,
    Is: IntoTerm,
    Is::Type: Scalar,
{
    /// Execute this expression as a query, returning a stream of claims.
    pub fn perform<'a, Env>(self, source: &'a Source<'a, Env>) -> impl Output<Claim> + 'a
    where
        Env: Provider<archive::Get> + Provider<archive::Put> + ConditionalSync + 'static,
    {
        let query: AttributeQuery = self.into();
        query.perform(source)
    }
}

// Statement: requires all three positions to be concrete.
impl<Is: Scalar> Statement for DynamicAttributeExpression<The, Entity, Is> {
    fn assert(self, transaction: &mut Transaction) {
        let the = self.the;
        let value: Value = self.is.into();
        match self.cardinality {
            Some(Cardinality::One) => {
                transaction.associate_unique(the, self.of, value);
            }
            _ => {
                transaction.associate(the, self.of, value);
            }
        }
    }

    fn retract(self, transaction: &mut Transaction) {
        let the = self.the;
        let value: Value = self.is.into();
        transaction.dissociate(the, self.of, value);
    }
}

// Not → Premise::Unless (query-level negation).
impl<Relation, Of, Is> Not for DynamicAttributeExpression<Relation, Of, Is>
where
    Relation: Into<Term<The>>,
    Of: Into<Term<Entity>>,
    Is: IntoTerm,
    Is::Type: Scalar,
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

// Into<Premise>: requires all positions to convert to terms.
impl<Relation, Of, Is> From<DynamicAttributeExpression<Relation, Of, Is>> for Premise
where
    Relation: Into<Term<The>>,
    Of: Into<Term<Entity>>,
    Is: IntoTerm,
    Is::Type: Scalar,
{
    fn from(expression: DynamicAttributeExpression<Relation, Of, Is>) -> Self {
        let query: AttributeQuery = expression.into();
        Premise::Assert(Proposition::Attribute(Box::new(query)))
    }
}

/// Convert a fully concrete dynamic expression into an `AttributeStatement`.
///
/// All three positions are concrete types (`The`, `Entity`, `Is: Scalar`),
/// so no runtime extraction from `Term` is needed.
impl<Is: Scalar> From<DynamicAttributeExpression<The, Entity, Is>> for AttributeStatement {
    fn from(expression: DynamicAttributeExpression<The, Entity, Is>) -> Self {
        DynamicAttributeExpression {
            the: expression.the,
            of: expression.of,
            is: expression.is.into(),
            cause: expression.cause,
            cardinality: expression.cardinality,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{Match, the};

    #[dialog_common::test]
    fn it_asserts_with_string() {
        let alice = Entity::new().unwrap();
        let statement = the!("person/name").of(alice).is("Alice".to_string());

        let mut transaction = Transaction::new();
        statement.assert(&mut transaction);
        assert!(!transaction.is_empty());
    }

    #[dialog_common::test]
    fn it_asserts_with_u32() {
        let alice = Entity::new().unwrap();
        let statement = the!("person/age").of(alice).is(25u32);

        let mut transaction = Transaction::new();
        statement.assert(&mut transaction);
        assert!(!transaction.is_empty());
    }

    #[dialog_common::test]
    fn it_retracts_with_string() {
        let alice = Entity::new().unwrap();
        let statement = the!("person/name").of(alice).is("Alice".to_string());

        let mut transaction = Transaction::new();
        statement.retract(&mut transaction);
        assert!(!transaction.is_empty());
    }

    #[dialog_common::test]
    fn it_negates_concrete_expression() {
        let alice = Entity::new().unwrap();
        let premise = !the!("person/name").of(alice).is("Alice".to_string());

        match premise {
            Premise::Unless(_) => {}
            _ => panic!("Expected Unless premise"),
        }
    }

    #[dialog_common::test]
    fn it_queries_with_both_terms() {
        let premise: Premise = the!("person/name")
            .of(Term::var("e"))
            .is(Term::<String>::var("v"))
            .into();

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
        let premise: Premise = the!("person/name")
            .of(alice)
            .is(Term::<String>::var("v"))
            .into();

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
        let premise: Premise = the!("person/name")
            .of(Term::var("e"))
            .is("Alice".to_string())
            .into();

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
        let premise: Premise = the!("person/name").of(alice).is("Alice".to_string()).into();

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
        let premise = !the!("person/name").of(alice).is(Term::<String>::var("v"));

        match premise {
            Premise::Unless(_) => {}
            _ => panic!("Expected Unless premise"),
        }
    }

    #[dialog_common::test]
    fn it_negates_both_terms_expression() {
        let premise = !the!("person/name")
            .of(Term::var("e"))
            .is(Term::<String>::var("v"));

        match premise {
            Premise::Unless(_) => {}
            _ => panic!("Expected Unless premise"),
        }
    }

    #[dialog_common::test]
    fn it_uses_same_is_for_statement_and_query() {
        let alice = Entity::new().unwrap();

        let expr = the!("person/name")
            .of(alice.clone())
            .is("Alice".to_string());
        let mut transaction = Transaction::new();
        expr.assert(&mut transaction);
        assert!(!transaction.is_empty());

        let expression = the!("person/name").of(alice).is("Alice".to_string());
        let premise: Premise = expression.into();
        assert!(matches!(
            premise,
            Premise::Assert(Proposition::Attribute(_))
        ));
    }

    #[dialog_common::test]
    fn it_converts_to_attribute_statement() {
        let alice = Entity::new().unwrap();
        let expr = the!("person/name")
            .of(alice.clone())
            .is("Alice".to_string());
        let stmt: AttributeStatement = expr.into();
        assert_eq!(stmt.of, alice);
        assert_eq!(stmt.is, Value::String("Alice".into()));
        assert_eq!(stmt.cardinality, None);
    }

    #[dialog_common::test]
    fn it_converts_to_attribute_statement_with_cardinality_one() {
        let alice = Entity::new().unwrap();
        let expr = the!("person/name")
            .of(alice.clone())
            .is("Alice".to_string())
            .cardinality(Cardinality::One);
        let stmt: AttributeStatement = expr.into();
        assert_eq!(stmt.of, alice);
        assert_eq!(stmt.is, Value::String("Alice".into()));
        assert_eq!(stmt.cardinality, Some(Cardinality::One));
    }

    #[dialog_common::test]
    fn it_asserts_with_cardinality_one() {
        let alice = Entity::new().unwrap();
        let statement = the!("person/name")
            .of(alice)
            .is("Alice".to_string())
            .cardinality(Cardinality::One);

        let mut transaction = Transaction::new();
        statement.assert(&mut transaction);
        assert!(!transaction.is_empty());
    }

    #[dialog_common::test]
    async fn it_roundtrips_assert_and_query() -> anyhow::Result<()> {
        use crate::session::RuleRegistry;
        use crate::source::Source;
        use dialog_repository::helpers::{test_operator, test_repo};
        use futures_util::TryStreamExt;

        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;

        let mut tx = Transaction::new();
        tx.assert(
            the!("person/name")
                .of(alice.clone())
                .is("Alice".to_string()),
        );
        branch.commit(tx.into_stream()).perform(&operator).await?;

        let premise: Premise = the!("person/name")
            .of(alice.clone())
            .is(Term::<String>::var("name"))
            .into();

        let prop = match premise {
            Premise::Assert(prop) => prop,
            _ => panic!("Expected Assert"),
        };

        let source = Source::new(&branch, &operator, RuleRegistry::new());
        let results = prop
            .evaluate(Match::new().seed(), &source)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(results.len(), 1);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_finds_all_relations_between_entities() -> anyhow::Result<()> {
        use crate::session::RuleRegistry;
        use crate::source::Source;
        use dialog_repository::helpers::{test_operator, test_repo};
        use futures_util::TryStreamExt;

        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        // Assert multiple relations between alice and bob
        let mut tx = Transaction::new();
        tx.assert(the!("team/colleague").of(alice.clone()).is(bob.clone()))
            .assert(the!("team/manager").of(alice.clone()).is(bob.clone()))
            .assert(the!("team/mentor").of(alice.clone()).is(bob.clone()));
        branch.commit(tx.into_stream()).perform(&operator).await?;

        // Use Term::<The>::var to find all relations between alice and bob
        let premise: Premise = Term::<The>::var("relation")
            .of(alice.clone())
            .is(bob.clone())
            .into();

        let prop = match premise {
            Premise::Assert(prop) => prop,
            _ => panic!("Expected Assert"),
        };

        let source = Source::new(&branch, &operator, RuleRegistry::new());
        let results = prop
            .evaluate(Match::new().seed(), &source)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(
            results.len(),
            3,
            "Should find all 3 relations between alice and bob"
        );

        Ok(())
    }
}
