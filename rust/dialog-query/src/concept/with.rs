use crate::Predicate;
use crate::assertion::{Assertion, Retraction};
use crate::attribute::{Attribute, AttributeDescriptor};
use crate::concept::application::ConceptQuery;
use crate::concept::descriptor::ConceptDescriptor;
use crate::concept::{Concept, Conclusion};
use crate::negation::Negation;
use crate::query::{Application, Output, Source};
use crate::selection::{Answer, Answers};
use crate::types::Scalar;
use crate::{
    Association, Cardinality, Entity, Parameters, Premise, Proposition, QueryError, Term,
    Transaction,
};
use std::marker::PhantomData;

/// Represents an entity with a single attribute.
///
/// Used to assert, retract, and query entities by their attributes.
///
/// # Examples
///
/// ```rs
/// // Association
/// tr.assert(With {
///     this: alice,
///     has: person::Name("Alice".into())
/// });
///
/// // Retraction
/// tr.retract(With {
///     this: alice,
///     has: person::Name("Alice".into())
/// });
///
/// // Query
/// Query::<With<person::Name>> {
///     this: Term::var("entity"),
///     has: Term::var("name")
/// }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct With<A: Attribute> {
    /// The entity this attribute belongs to.
    pub this: Entity,
    /// The attribute value associated with the entity.
    pub has: A,
}

/// Query pattern for entities with a specific attribute.
///
/// Use with the `Query` type alias to query for entities that have an attribute.
#[derive(Clone, Debug, PartialEq)]
pub struct WithQuery<A: Attribute> {
    /// Term matching the entity. Defaults to a variable named `"this"`.
    pub this: Term<Entity>,
    /// Term matching the attribute value. Defaults to a variable named `"has"`.
    pub has: Term<A::Type>,
}

impl<A: Attribute> Default for WithQuery<A> {
    fn default() -> Self {
        Self {
            this: Term::var("this"),
            has: Term::var("has"),
        }
    }
}

impl<A: Attribute> WithQuery<A>
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    /// Query for instances matching this pattern
    pub fn perform<S: Source>(self, source: &S) -> impl Output<With<A>> {
        Application::perform(self, source)
    }
}

/// Helper methods for constructing term variables in queries.
#[derive(Clone, Debug, PartialEq)]
pub struct WithTerms<A: Attribute> {
    _marker: PhantomData<A>,
}

impl<A: Attribute> WithTerms<A> {
    /// Returns a term variable for the entity, named `"this"`.
    pub fn this() -> Term<Entity> {
        Term::var("this")
    }

    /// Returns a term variable for the attribute value, named `"has"`.
    pub fn has() -> Term<A::Type> {
        Term::var("has")
    }
}

impl<A: Attribute> From<With<A>> for ConceptDescriptor
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    fn from(_: With<A>) -> Self {
        ConceptDescriptor::from(vec![("has", A::descriptor())])
    }
}

impl<A: Attribute> From<WithQuery<A>> for ConceptDescriptor
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    fn from(_: WithQuery<A>) -> Self {
        ConceptDescriptor::from(vec![("has", A::descriptor())])
    }
}

impl<A: Attribute> Concept for With<A>
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    type Term = WithTerms<A>;

    fn description() -> &'static str {
        ""
    }

    fn this(&self) -> Entity {
        let predicate: ConceptDescriptor = self.clone().into();
        predicate.this()
    }
}

impl<A: Attribute> Predicate for With<A>
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    type Conclusion = With<A>;
    type Application = WithQuery<A>;
    type Descriptor = AttributeDescriptor;
}

impl<A: Attribute> Conclusion for With<A>
where
    A: Clone + Send,
{
    fn this(&self) -> &Entity {
        &self.this
    }
}

impl<A: Attribute> Assertion for With<A>
where
    A: Clone,
{
    fn assert(self, transaction: &mut Transaction) {
        let association = Association::new(A::the(), self.this, self.has.value().as_value());
        if A::descriptor().cardinality() == Cardinality::One {
            transaction.associate_unique(association);
        } else {
            transaction.associate(association);
        }
    }

    fn retract(self, transaction: &mut Transaction) {
        Association::new(A::the(), self.this, self.has.value().as_value()).retract(transaction);
    }
}

impl<A: Attribute> std::ops::Not for With<A>
where
    A: Clone,
{
    type Output = Retraction<With<A>>;

    fn not(self) -> Self::Output {
        self.revert()
    }
}

impl<A: Attribute> IntoIterator for With<A>
where
    A: Clone,
{
    type Item = Association;
    type IntoIter = std::iter::Once<Association>;

    fn into_iter(self) -> Self::IntoIter {
        std::iter::once(Association::new(
            A::the(),
            self.this,
            self.has.value().as_value(),
        ))
    }
}

impl<A: Attribute> Application for WithQuery<A>
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    type Conclusion = With<A>;

    fn evaluate<S: Source, M: Answers>(self, answers: M, source: &S) -> impl Answers {
        let application: ConceptQuery = self.into();
        application.evaluate(answers, source)
    }

    fn realize(&self, source: Answer) -> Result<Self::Conclusion, QueryError> {
        Ok(With {
            this: source.get(&self.this)?,
            has: A::new(source.get(&self.has)?),
        })
    }
}

impl<A: Attribute> std::ops::Not for WithQuery<A>
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    type Output = Premise;

    fn not(self) -> Self::Output {
        let application: Proposition = self.into();
        Premise::Unless(Negation::not(application))
    }
}

impl<A: Attribute> From<WithQuery<A>> for Parameters
where
    A: Clone,
{
    fn from(source: WithQuery<A>) -> Self {
        let mut params = Self::new();
        params.insert("this".to_string(), source.this.as_unknown());
        params.insert("has".to_string(), source.has.as_unknown());
        params
    }
}

impl<A: Attribute> From<WithQuery<A>> for ConceptQuery
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    fn from(source: WithQuery<A>) -> Self {
        let predicate: ConceptDescriptor = source.clone().into();
        ConceptQuery {
            terms: source.into(),
            predicate,
        }
    }
}

impl<A: Attribute> From<WithQuery<A>> for Proposition
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    fn from(source: WithQuery<A>) -> Self {
        Proposition::Concept(source.into())
    }
}

impl<A: Attribute> From<WithQuery<A>> for Premise
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    fn from(source: WithQuery<A>) -> Self {
        Premise::When(source.into())
    }
}

#[cfg(test)]
mod tests {
    use crate::attribute::Attribute;
    use crate::selection::Answer;
    use crate::the;

    mod test_pascal {
        use crate::Attribute;

        #[derive(Attribute, Clone)]
        pub struct UserName(pub String);
    }

    #[dialog_common::test]
    fn it_constructs_match_from_literal() {
        use crate::{Entity, Query, Term};

        let entity_id = Entity::new().unwrap();

        let query = Query::<crate::concept::With<test_pascal::UserName>> {
            this: Term::from(entity_id),
            has: Term::from("Alice".to_string()),
        };

        assert!(matches!(query.this, Term::Constant(_)));
        assert!(matches!(query.has, Term::Constant(_)));
    }

    #[dialog_common::test]
    fn it_constructs_queryable_match_pattern() {
        use crate::{Entity, Query, Term};

        let entity_id = Entity::new().unwrap();

        let query = Query::<crate::concept::With<test_pascal::UserName>> {
            this: Term::from(entity_id),
            has: Term::from("Alice".to_string()),
        };

        assert!(matches!(query.this, Term::Constant(_)));
        assert!(matches!(query.has, Term::Constant(_)));
    }

    #[dialog_common::test]
    fn it_constructs_default_match() {
        use crate::{Query, Term};

        let query = Query::<crate::concept::With<test_pascal::UserName>>::default();

        assert!(matches!(query.this, Term::Variable { .. }));
        assert!(matches!(query.has, Term::Variable { .. }));
    }

    mod employee_txn {
        use crate::Attribute;

        /// Name of the employee
        #[derive(Attribute, Clone)]
        pub struct Name(pub String);

        /// Job title of the employee
        #[derive(Attribute, Clone)]
        pub struct Job(pub String);

        /// Salary of the employee
        #[derive(Attribute, Clone)]
        pub struct Salary(pub u32);

        /// Employee's manager
        #[derive(Attribute, Clone)]
        pub struct Manager(pub crate::Entity);
    }

    #[dialog_common::test]
    async fn it_asserts_and_retracts_single_attribute() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Value};
        use crate::concept::With;
        use crate::relation::query::RelationQuery;
        use crate::{Entity, Session, Term};
        use dialog_storage::MemoryStorageBackend;
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;

        let alice = Entity::new()?;
        let name = employee_txn::Name("Alice".to_string());

        let mut session = Session::open(store.clone());
        session
            .transact(vec![With {
                this: alice.clone(),
                has: name.clone(),
            }])
            .await?;

        let query = RelationQuery::new(
            Term::Constant(the!("employee-txn/name")),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let facts: Vec<_> = query
            .perform(&Session::open(store.clone()))
            .try_collect()
            .await?;

        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].is, Value::String("Alice".to_string()));

        let mut session = Session::open(store.clone());
        session
            .transact(vec![!With {
                this: alice.clone(),
                has: name,
            }])
            .await?;

        let query = RelationQuery::new(
            Term::Constant(the!("employee-txn/name")),
            alice.into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let facts: Vec<_> = query.perform(&Session::open(store)).try_collect().await?;

        assert_eq!(facts.len(), 0);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_asserts_multiple_attributes() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Value};
        use crate::concept::With;
        use crate::relation::query::RelationQuery;
        use crate::{Entity, Session, Term};
        use dialog_storage::MemoryStorageBackend;
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;

        let bob = Entity::new()?;
        let name = employee_txn::Name("Bob".to_string());
        let job = employee_txn::Job("Engineer".to_string());

        let mut session = Session::open(store.clone());
        session
            .transact(vec![With {
                this: bob.clone(),
                has: name,
            }])
            .await?;
        session
            .transact(vec![With {
                this: bob.clone(),
                has: job,
            }])
            .await?;

        let name_query = RelationQuery::new(
            Term::Constant(the!("employee-txn/name")),
            bob.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let job_query = RelationQuery::new(
            Term::Constant(the!("employee-txn/job")),
            bob.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let name_facts: Vec<_> = name_query
            .perform(&Session::open(store.clone()))
            .try_collect()
            .await?;

        let job_facts: Vec<_> = job_query
            .perform(&Session::open(store))
            .try_collect()
            .await?;

        assert_eq!(name_facts.len(), 1);
        assert_eq!(name_facts[0].is, Value::String("Bob".to_string()));

        assert_eq!(job_facts.len(), 1);
        assert_eq!(job_facts[0].is, Value::String("Engineer".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_asserts_three_attributes() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Value};
        use crate::concept::With;
        use crate::relation::query::RelationQuery;
        use crate::{Entity, Session, Term};
        use dialog_storage::MemoryStorageBackend;
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;

        let charlie = Entity::new()?;
        let name = employee_txn::Name("Charlie".to_string());
        let job = employee_txn::Job("Manager".to_string());
        let salary = employee_txn::Salary(120000);

        let mut session = Session::open(store.clone());
        session
            .transact(vec![With {
                this: charlie.clone(),
                has: name,
            }])
            .await?;
        session
            .transact(vec![With {
                this: charlie.clone(),
                has: job,
            }])
            .await?;
        session
            .transact(vec![With {
                this: charlie.clone(),
                has: salary,
            }])
            .await?;

        let name_query = RelationQuery::new(
            Term::Constant(the!("employee-txn/name")),
            charlie.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let job_query = RelationQuery::new(
            Term::Constant(the!("employee-txn/job")),
            charlie.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let salary_query = RelationQuery::new(
            Term::Constant(the!("employee-txn/salary")),
            charlie.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let name_facts: Vec<_> = name_query
            .perform(&Session::open(store.clone()))
            .try_collect()
            .await?;

        let job_facts: Vec<_> = job_query
            .perform(&Session::open(store.clone()))
            .try_collect()
            .await?;

        let salary_facts: Vec<_> = salary_query
            .perform(&Session::open(store))
            .try_collect()
            .await?;

        assert_eq!(name_facts.len(), 1);
        assert_eq!(name_facts[0].is, Value::String("Charlie".to_string()));

        assert_eq!(job_facts.len(), 1);
        assert_eq!(job_facts[0].is, Value::String("Manager".to_string()));

        assert_eq!(salary_facts.len(), 1);
        assert_eq!(salary_facts[0].is, Value::UnsignedInt(120000));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_multiple_attributes() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::concept::With;
        use crate::relation::query::RelationQuery;
        use crate::{Entity, Session, Term};
        use dialog_storage::MemoryStorageBackend;
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;

        let dave = Entity::new()?;
        let name = employee_txn::Name("Dave".to_string());
        let job = employee_txn::Job("Developer".to_string());

        let mut session = Session::open(store.clone());
        session
            .transact(vec![With {
                this: dave.clone(),
                has: name.clone(),
            }])
            .await?;
        session
            .transact(vec![With {
                this: dave.clone(),
                has: job.clone(),
            }])
            .await?;

        let mut session = Session::open(store.clone());
        session
            .transact(vec![!With {
                this: dave.clone(),
                has: name,
            }])
            .await?;
        session
            .transact(vec![!With {
                this: dave.clone(),
                has: job,
            }])
            .await?;

        let name_query = RelationQuery::new(
            Term::Constant(the!("employee-txn/name")),
            dave.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let job_query = RelationQuery::new(
            Term::Constant(the!("employee-txn/job")),
            dave.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let name_facts: Vec<_> = name_query
            .perform(&Session::open(store.clone()))
            .try_collect()
            .await?;

        let job_facts: Vec<_> = job_query
            .perform(&Session::open(store))
            .try_collect()
            .await?;

        assert_eq!(name_facts.len(), 0);
        assert_eq!(job_facts.len(), 0);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_updates_attribute() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Value};
        use crate::concept::With;
        use crate::relation::query::RelationQuery;
        use crate::{Entity, Session, Term};
        use dialog_storage::MemoryStorageBackend;
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;

        let eve = Entity::new()?;
        let old_job = employee_txn::Job("Junior Developer".to_string());

        let mut session = Session::open(store.clone());
        session
            .transact(vec![With {
                this: eve.clone(),
                has: old_job.clone(),
            }])
            .await?;

        let mut session = Session::open(store.clone());
        session
            .transact(vec![!With {
                this: eve.clone(),
                has: old_job,
            }])
            .await?;

        let new_job = employee_txn::Job("Senior Developer".to_string());
        let mut session = Session::open(store.clone());
        session
            .transact(vec![With {
                this: eve.clone(),
                has: new_job,
            }])
            .await?;

        let query = RelationQuery::new(
            Term::Constant(the!("employee-txn/job")),
            eve.into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let job_facts: Vec<_> = query.perform(&Session::open(store)).try_collect().await?;

        assert_eq!(job_facts.len(), 1);
        assert_eq!(
            job_facts[0].is,
            Value::String("Senior Developer".to_string())
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_entity_reference_attribute() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Value};
        use crate::concept::With;
        use crate::relation::query::RelationQuery;
        use crate::{Entity, Session, Term};
        use dialog_storage::MemoryStorageBackend;
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;

        let manager = Entity::new()?;
        let employee_entity = Entity::new()?;

        let manager_name = employee_txn::Name("Manager Alice".to_string());
        let employee_name = employee_txn::Name("Employee Bob".to_string());
        let reports_to = employee_txn::Manager(manager.clone());

        let mut session = Session::open(store.clone());
        session
            .transact(vec![With {
                this: manager.clone(),
                has: manager_name,
            }])
            .await?;

        let mut session = Session::open(store.clone());
        session
            .transact(vec![With {
                this: employee_entity.clone(),
                has: employee_name,
            }])
            .await?;
        session
            .transact(vec![With {
                this: employee_entity.clone(),
                has: reports_to,
            }])
            .await?;

        let query = RelationQuery::new(
            Term::Constant(the!("employee-txn/manager")),
            employee_entity.into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let manager_facts: Vec<_> = query.perform(&Session::open(store)).try_collect().await?;

        assert_eq!(manager_facts.len(), 1);
        assert_eq!(manager_facts[0].is, Value::Entity(manager));

        Ok(())
    }

    mod employee_shortcut {
        use crate::Attribute;

        #[derive(Attribute, Clone)]
        pub struct Name(pub String);

        #[derive(Attribute, Clone)]
        pub struct Job(pub String);
    }

    #[dialog_common::test]
    async fn it_queries_via_with_shortcut() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::concept::With;
        use crate::{Entity, Query, Session};
        use dialog_storage::MemoryStorageBackend;
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let mut edit = session.edit();
        edit.assert(With {
            this: alice.clone(),
            has: employee_shortcut::Name("Alice".into()),
        })
        .assert(With {
            this: bob.clone(),
            has: employee_shortcut::Name("Bob".into()),
        })
        .assert(With {
            this: alice.clone(),
            has: employee_shortcut::Job("Engineer".into()),
        })
        .assert(With {
            this: bob.clone(),
            has: employee_shortcut::Job("Designer".into()),
        });
        session.commit(edit).await?;

        let names: Vec<With<employee_shortcut::Name>> =
            Query::<With<employee_shortcut::Name>>::default()
                .perform(&session)
                .try_collect()
                .await?;

        assert_eq!(names.len(), 2, "Should find 2 names");

        let mut found_alice = false;
        let mut found_bob = false;

        for name in &names {
            if name.has.value() == "Alice" {
                found_alice = true;
            } else if name.has.value() == "Bob" {
                found_bob = true;
            }
        }

        assert!(found_alice, "Should find Alice");
        assert!(found_bob, "Should find Bob");

        let jobs: Vec<With<employee_shortcut::Job>> =
            Query::<With<employee_shortcut::Job>>::default()
                .perform(&session)
                .try_collect()
                .await?;

        assert_eq!(jobs.len(), 2, "Should find 2 jobs");

        Ok(())
    }

    #[allow(dead_code)]
    mod note_concept {
        use crate::Attribute;

        #[derive(Attribute, Clone)]
        pub struct Title(pub String);

        #[derive(Attribute, Clone)]
        pub struct Body(pub String);
    }

    #[dialog_common::test]
    async fn it_claims_attribute() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::assertion::Assertion;
        use crate::concept::With;
        use crate::{Entity, Query, Session, Term, Transaction};
        use dialog_storage::MemoryStorageBackend;
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let entity = Entity::new()?;
        let title = note_concept::Title("Test Note".to_string());

        let instance = With {
            this: entity.clone(),
            has: title.clone(),
        };

        let mut transaction = Transaction::new();
        instance.clone().assert(&mut transaction);
        session.commit(transaction).await?;

        let query = Query::<With<note_concept::Title>> {
            this: Term::from(entity.clone()),
            has: Term::var("has"),
        };

        let premise: crate::Premise = query.into();
        let application = match premise {
            crate::Premise::When(app) => app,
            _ => panic!("Expected Apply premise"),
        };

        let results = application
            .evaluate(Answer::new().seed(), &session)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(results.len(), 1);

        Ok(())
    }

    mod note_rule {
        use crate::Attribute;

        #[derive(Attribute, Clone)]
        pub struct Title(pub String);
    }

    #[dialog_common::test]
    async fn it_queries_adhoc_concept() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::concept::With;
        use crate::the;
        use crate::{Association, Entity, Query, Session, Term, Value};
        use dialog_storage::MemoryStorageBackend;
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let note1 = Entity::new()?;
        let note2 = Entity::new()?;
        let note3 = Entity::new()?;

        session
            .transact(vec![
                Association {
                    the: the!("note-rule/title"),
                    of: note1.clone(),
                    is: Value::String("First Note".to_string()),
                },
                Association {
                    the: the!("note-rule/title"),
                    of: note2.clone(),
                    is: Value::String("Second Note".to_string()),
                },
                Association {
                    the: the!("note-rule/title"),
                    of: note3.clone(),
                    is: Value::String("Third Note".to_string()),
                },
            ])
            .await?;

        let query = Query::<With<note_rule::Title>> {
            this: Term::var("entity"),
            has: Term::var("has"),
        };

        let premise: crate::Premise = query.into();

        let application = match premise {
            crate::Premise::When(app) => app,
            _ => panic!("Expected Apply premise"),
        };

        let results = application
            .evaluate(Answer::new().seed(), &session)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(results.len(), 3, "Should find 3 notes with titles");

        let title_var: Term<Value> = Term::var("has");

        let mut found_titles = std::collections::HashSet::new();
        for result in &results {
            let title = result.resolve(&title_var)?;
            if let Value::String(s) = title {
                found_titles.insert(s.clone());
            }
        }

        assert!(found_titles.contains("First Note"));
        assert!(found_titles.contains("Second Note"));
        assert!(found_titles.contains("Third Note"));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_filters_adhoc_concept_query() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::concept::With;
        use crate::the;
        use crate::{Association, Entity, Query, Session, Term, Value};
        use dialog_storage::MemoryStorageBackend;
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let note1 = Entity::new()?;
        let note2 = Entity::new()?;
        let note3 = Entity::new()?;

        session
            .transact(vec![
                Association {
                    the: the!("note-rule/title"),
                    of: note1.clone(),
                    is: Value::String("Target Note".to_string()),
                },
                Association {
                    the: the!("note-rule/title"),
                    of: note2.clone(),
                    is: Value::String("Other Note".to_string()),
                },
                Association {
                    the: the!("note-rule/title"),
                    of: note3.clone(),
                    is: Value::String("Another Note".to_string()),
                },
            ])
            .await?;

        let query = Query::<With<note_rule::Title>> {
            this: Term::var("entity"),
            has: Term::from("Target Note".to_string()),
        };

        let premise: crate::Premise = query.into();

        let application = match premise {
            crate::Premise::When(app) => app,
            _ => panic!("Expected Apply premise"),
        };

        let results = application
            .evaluate(Answer::new().seed(), &session)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(
            results.len(),
            1,
            "Should find exactly 1 note with 'Target Note' title"
        );

        let entity_var: Term<Value> = Term::var("entity");
        let found_entity = results[0].resolve(&entity_var)?;
        assert_eq!(found_entity, Value::Entity(note1));

        Ok(())
    }
}
