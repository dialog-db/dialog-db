use crate::artifact::{Cause, Entity, Value};
use crate::attribute::The;
use crate::attribute::statement::AttributeStatement;
use crate::negation::Negation;
use crate::relation::query::RelationQuery;
use crate::schema::Cardinality;
use crate::statement::Statement;
use crate::term::Term;
use crate::types::{Any, Scalar, Typed};
use crate::{Premise, Proposition, Transaction};
use std::ops::Not;

/// Converts a value into a [`Term`], resolving the type unambiguously
/// from the input.
///
/// Implemented for:
/// - Concrete scalar values (`String`, `u32`, etc.) — produces a constant term.
/// - [`Term<T>`] variables — passes through unchanged.
pub trait IntoTerm {
    /// The type this value represents.
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

impl<T, Of> DynamicAttributeExpressionBuilder<T, Of>
where
    Term<Entity>: From<Of>,
    Term<The>: From<T>,
{
    /// Set the value for this dynamic attribute expression.
    ///
    /// Accepts concrete scalar values (`"Alice"`, `25u32`),
    /// [`Term`] variables (`Term::<String>::var("name")`), and
    /// [`The`] identifiers or [`Term<The>`] variables for querying
    /// relations themselves (`Term::<The>::var("relation")`).
    pub fn is<V: IntoTerm>(self, value: V) -> DynamicAttributeExpression<Of, V::Type> {
        DynamicAttributeExpression {
            the: Term::<The>::from(self.the),
            of: self.of,
            is: value.into_term(),
            cause: None,
            cardinality: None,
        }
    }
}

/// A dynamic attribute expression binding an attribute name to an entity
/// and a value.
///
/// `Of` uses deferred conversion (raw value stored as-is), while `Is`
/// is the scalar type whose value is stored as a [`Term<Is>`].
///
/// - [`Statement`] requires `Of = Entity` — only concrete entities.
/// - [`From<...> for Premise`] requires `Of: Into<Term<Entity>>` —
///   concrete values or [`Term`] variables.
pub struct DynamicAttributeExpression<Of = Entity, Is: Typed = String> {
    /// The attribute (predicate), concrete or variable.
    pub the: Term<The>,
    /// The entity (or entity term).
    pub of: Of,
    /// The value term.
    pub is: Term<Is>,
    /// Provenance/cause for this expression.
    pub cause: Option<Cause>,
    /// Optional cardinality override. When `Some(Cardinality::One)`,
    /// `assert` uses `associate_unique`.
    pub cardinality: Option<Cardinality>,
}

impl<Of, Is: Typed> DynamicAttributeExpression<Of, Is> {
    /// Set the cardinality for this expression.
    pub fn cardinality(mut self, cardinality: Cardinality) -> Self {
        self.cardinality = Some(cardinality);
        self
    }
}

// Statement: requires concrete entity and a constant value term.
impl<Is: Scalar> Statement for DynamicAttributeExpression<Entity, Is> {
    fn assert(self, transaction: &mut Transaction) {
        let the: The = self
            .the
            .as_typed_constant()
            .expect("Cannot assert a variable attribute");
        let value: Value = match self.is {
            Term::Constant(v) => v,
            Term::Variable { .. } => panic!("Cannot assert a variable term"),
        };
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
        let the: The = self
            .the
            .as_typed_constant()
            .expect("Cannot retract a variable attribute");
        let value: Value = match self.is {
            Term::Constant(v) => v,
            Term::Variable { .. } => panic!("Cannot retract a variable term"),
        };
        transaction.dissociate(the, self.of, value);
    }
}

// Not → Premise::Unless (query-level negation).
impl<Of, Is: Scalar> Not for DynamicAttributeExpression<Of, Is>
where
    Of: Into<Term<Entity>>,
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

// Into<Premise>: requires entity to convert to term.
impl<Of, Is: Scalar> From<DynamicAttributeExpression<Of, Is>> for Premise
where
    Of: Into<Term<Entity>>,
{
    fn from(expr: DynamicAttributeExpression<Of, Is>) -> Self {
        let value_term: Term<Value> = expr.is.into();
        let any_term: Term<Any> = value_term.into();
        let query = RelationQuery::new(
            expr.the,
            expr.of.into(),
            any_term,
            match expr.cause {
                Some(c) => Term::Constant(Value::from(c)),
                None => Term::blank(),
            },
            expr.cardinality,
        );
        Premise::Assert(Proposition::Relation(Box::new(query)))
    }
}

// From<DynamicAttributeExpression<Entity, Is>> for AttributeStatement (type erasure)
impl<Is: Scalar> From<DynamicAttributeExpression<Entity, Is>> for AttributeStatement {
    fn from(expr: DynamicAttributeExpression<Entity, Is>) -> Self {
        let the: The = expr
            .the
            .as_typed_constant()
            .expect("Cannot convert a variable attribute to AttributeStatement");
        let value: Value = match expr.is {
            Term::Constant(v) => v,
            Term::Variable { .. } => panic!("Cannot convert a variable term to AttributeStatement"),
        };
        AttributeStatement {
            the,
            of: expr.of,
            is: value,
            cardinality: expr.cardinality.unwrap_or(Cardinality::Many),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::the;

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
        let alice = Entity::new().unwrap();
        let premise: Premise = the!("person/name")
            .of(alice)
            .is(Term::<String>::var("v"))
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
    fn it_queries_with_term_entity_and_concrete_value() {
        let premise: Premise = the!("person/name")
            .of(Term::var("e"))
            .is("Alice".to_string())
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
    fn it_queries_with_both_concrete() {
        let alice = Entity::new().unwrap();
        let premise: Premise = the!("person/name").of(alice).is("Alice".to_string()).into();

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
        assert!(matches!(premise, Premise::Assert(Proposition::Relation(_))));
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
        assert_eq!(stmt.cardinality, Cardinality::Many);
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
        assert_eq!(stmt.cardinality, Cardinality::One);
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
        use crate::Session;
        use crate::artifact::Artifacts;
        use crate::selection::Match;
        use dialog_storage::MemoryStorageBackend;
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;

        let alice = Entity::new()?;

        let mut session = Session::open(store.clone());
        {
            let mut tx = session.edit();
            tx.assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            );
            session.commit(tx).await?;
        }

        let premise: Premise = the!("person/name")
            .of(alice.clone())
            .is(Term::<String>::var("name"))
            .into();

        let prop = match premise {
            Premise::Assert(prop) => prop,
            _ => panic!("Expected Assert"),
        };

        let results = prop
            .evaluate(Match::new().seed(), &Session::open(store.clone()))
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(results.len(), 1);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_finds_all_relations_between_entities() -> anyhow::Result<()> {
        use crate::Session;
        use crate::artifact::Artifacts;
        use crate::selection::Match;
        use dialog_storage::MemoryStorageBackend;
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        // Assert multiple relations between alice and bob
        let mut session = Session::open(store.clone());
        {
            let mut tx = session.edit();
            tx.assert(the!("team/colleague").of(alice.clone()).is(bob.clone()))
                .assert(the!("team/manager").of(alice.clone()).is(bob.clone()))
                .assert(the!("team/mentor").of(alice.clone()).is(bob.clone()));
            session.commit(tx).await?;
        }

        // Use Term::<The>::var to find all relations between alice and bob
        let premise: Premise = Term::<The>::var("relation")
            .of(alice.clone())
            .is(bob.clone())
            .into();

        let prop = match premise {
            Premise::Assert(prop) => prop,
            _ => panic!("Expected Assert"),
        };

        let results = prop
            .evaluate(Match::new().seed(), &Session::open(store.clone()))
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
