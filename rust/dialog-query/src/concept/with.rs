use crate::application::ConceptApplication;
use crate::attribute::Attribute;
use crate::claim::Claim;
use crate::{Application, Entity, Parameters, Premise, Relation, Transaction};
use std::marker::PhantomData;

/// Represents an entity with a single attribute.
///
/// Used to assert, retract, and query entities by their attributes.
///
/// # Examples
///
/// ```ignore
/// // Assertion
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
/// Match::<With<person::Name>> {
///     this: Term::var("entity"),
///     has: Term::var("name")
/// }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct With<A: Attribute> {
    pub this: Entity,
    pub has: A,
}

/// Query pattern for entities with a specific attribute.
///
/// Use with the `Match` type alias to query for entities that have an attribute.
#[derive(Clone, Debug, PartialEq)]
pub struct WithMatch<A: Attribute> {
    pub this: crate::Term<Entity>,
    pub has: crate::Term<A::Type>,
}

impl<A: Attribute> Default for WithMatch<A> {
    fn default() -> Self {
        Self {
            this: crate::Term::var("this"),
            has: crate::Term::var("has"),
        }
    }
}

/// Helper methods for constructing term variables in queries.
#[derive(Clone, Debug, PartialEq)]
pub struct WithTerms<A: Attribute> {
    _marker: PhantomData<A>,
}

impl<A: Attribute> WithTerms<A> {
    pub fn this() -> crate::Term<Entity> {
        crate::Term::var("this")
    }

    pub fn has() -> crate::Term<A::Type> {
        crate::Term::var("has")
    }
}

impl<A: Attribute> crate::concept::Concept for With<A>
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    type Instance = With<A>;
    type Match = WithMatch<A>;
    type Term = WithTerms<A>;

    const CONCEPT: crate::predicate::concept::Concept = A::CONCEPT;
}

impl<A: Attribute> crate::dsl::Quarriable for With<A>
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    type Query = WithMatch<A>;
}

impl<A: Attribute> crate::concept::Instance for With<A>
where
    A: Clone + Send,
{
    fn this(&self) -> Entity {
        self.this.clone()
    }
}

impl<A: Attribute> Claim for With<A>
where
    A: Clone,
{
    fn assert(self, transaction: &mut Transaction) {
        use crate::types::Scalar;
        Relation::new(A::selector(), self.this, self.has.value().as_value()).assert(transaction);
    }

    fn retract(self, transaction: &mut Transaction) {
        use crate::types::Scalar;
        Relation::new(A::selector(), self.this, self.has.value().as_value()).retract(transaction);
    }
}

impl<A: Attribute> std::ops::Not for With<A>
where
    A: Clone,
{
    type Output = crate::claim::Revert<With<A>>;

    fn not(self) -> Self::Output {
        self.revert()
    }
}

impl<A: Attribute> IntoIterator for With<A>
where
    A: Clone,
{
    type Item = Relation;
    type IntoIter = std::iter::Once<Relation>;

    fn into_iter(self) -> Self::IntoIter {
        use crate::types::Scalar;
        std::iter::once(Relation::new(
            A::selector(),
            self.this,
            self.has.value().as_value(),
        ))
    }
}

impl<A: Attribute> crate::concept::Match for WithMatch<A>
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    type Concept = With<A>;
    type Instance = With<A>;

    fn realize(
        &self,
        source: crate::selection::Answer,
    ) -> Result<Self::Instance, crate::QueryError> {
        Ok(With {
            this: source.get(&self.this)?,
            has: A::new(source.get(&self.has)?),
        })
    }
}

impl<A: Attribute> std::ops::Not for WithMatch<A>
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    type Output = Premise;

    fn not(self) -> Self::Output {
        let application: Application = self.into();
        Premise::Exclude(crate::negation::Negation::not(application))
    }
}

impl<A: Attribute> From<WithMatch<A>> for Parameters
where
    A: Clone,
{
    fn from(source: WithMatch<A>) -> Self {
        let mut params = Self::new();
        params.insert("this".to_string(), source.this.as_unknown());
        params.insert("has".to_string(), source.has.as_unknown());
        params
    }
}

impl<A: Attribute> From<WithMatch<A>> for ConceptApplication
where
    A: Clone,
{
    fn from(source: WithMatch<A>) -> Self {
        ConceptApplication {
            terms: source.into(),
            concept: A::CONCEPT,
        }
    }
}

impl<A: Attribute> From<WithMatch<A>> for Application
where
    A: Clone,
{
    fn from(source: WithMatch<A>) -> Self {
        Application::Concept(source.into())
    }
}

impl<A: Attribute> From<WithMatch<A>> for Premise
where
    A: Clone,
{
    fn from(source: WithMatch<A>) -> Self {
        Premise::Apply(source.into())
    }
}

#[cfg(test)]
mod tests {
    use crate::attribute::Attribute;

    mod test_pascal {
        use crate::Attribute;

        #[derive(Attribute, Clone)]
        pub struct UserName(pub String);
    }

    #[dialog_common::test]
    fn test_match_struct_literal() {
        use crate::{Entity, Match, Term};

        let entity_id = Entity::new().unwrap();

        let query = Match::<crate::concept::With<test_pascal::UserName>> {
            this: Term::from(entity_id),
            has: Term::from("Alice".to_string()),
        };

        assert!(matches!(query.this, Term::Constant(_)));
        assert!(matches!(query.has, Term::Constant(_)));
    }

    #[dialog_common::test]
    fn test_quarriable_match_pattern() {
        use crate::{Entity, Match, Term};

        let entity_id = Entity::new().unwrap();

        let query = Match::<crate::concept::With<test_pascal::UserName>> {
            this: Term::from(entity_id),
            has: Term::from("Alice".to_string()),
        };

        assert!(matches!(query.this, Term::Constant(_)));
        assert!(matches!(query.has, Term::Constant(_)));
    }

    #[dialog_common::test]
    fn test_default_match_constructor() {
        use crate::{Match, Term};

        let query = Match::<crate::concept::With<test_pascal::UserName>>::default();

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
    async fn test_single_attribute_assert_and_retract() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Value};
        use crate::concept::With;
        use crate::{Entity, Fact, Session};
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

        let query = Fact::<Value>::select()
            .the("employee-txn/name")
            .of(alice.clone())
            .compile()?;

        let facts: Vec<_> = query
            .query(&Session::open(store.clone()))
            .try_collect()
            .await?;

        assert_eq!(facts.len(), 1);
        match &facts[0] {
            Fact::Assertion { is, .. } => {
                assert_eq!(*is, Value::String("Alice".to_string()));
            }
            _ => panic!("Expected Assertion"),
        }

        let mut session = Session::open(store.clone());
        session
            .transact(vec![!With {
                this: alice.clone(),
                has: name,
            }])
            .await?;

        let query = Fact::<Value>::select()
            .the("employee-txn/name")
            .of(alice)
            .compile()?;

        let facts: Vec<_> = query.query(&Session::open(store)).try_collect().await?;

        assert_eq!(facts.len(), 0);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_multiple_attributes_assert() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Value};
        use crate::concept::With;
        use crate::{Entity, Fact, Session};
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

        let name_query = Fact::<Value>::select()
            .the("employee-txn/name")
            .of(bob.clone())
            .compile()?;

        let job_query = Fact::<Value>::select()
            .the("employee-txn/job")
            .of(bob.clone())
            .compile()?;

        let name_facts: Vec<_> = name_query
            .query(&Session::open(store.clone()))
            .try_collect()
            .await?;

        let job_facts: Vec<_> = job_query.query(&Session::open(store)).try_collect().await?;

        assert_eq!(name_facts.len(), 1);
        match &name_facts[0] {
            Fact::Assertion { is, .. } => {
                assert_eq!(*is, Value::String("Bob".to_string()));
            }
            _ => panic!("Expected Assertion"),
        }

        assert_eq!(job_facts.len(), 1);
        match &job_facts[0] {
            Fact::Assertion { is, .. } => {
                assert_eq!(*is, Value::String("Engineer".to_string()));
            }
            _ => panic!("Expected Assertion"),
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn test_three_attributes_assert() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Value};
        use crate::concept::With;
        use crate::{Entity, Fact, Session};
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

        let name_query = Fact::<Value>::select()
            .the("employee-txn/name")
            .of(charlie.clone())
            .compile()?;

        let job_query = Fact::<Value>::select()
            .the("employee-txn/job")
            .of(charlie.clone())
            .compile()?;

        let salary_query = Fact::<Value>::select()
            .the("employee-txn/salary")
            .of(charlie.clone())
            .compile()?;

        let name_facts: Vec<_> = name_query
            .query(&Session::open(store.clone()))
            .try_collect()
            .await?;

        let job_facts: Vec<_> = job_query
            .query(&Session::open(store.clone()))
            .try_collect()
            .await?;

        let salary_facts: Vec<_> = salary_query
            .query(&Session::open(store))
            .try_collect()
            .await?;

        assert_eq!(name_facts.len(), 1);
        match &name_facts[0] {
            Fact::Assertion { is, .. } => {
                assert_eq!(*is, Value::String("Charlie".to_string()));
            }
            _ => panic!("Expected Assertion"),
        }

        assert_eq!(job_facts.len(), 1);
        match &job_facts[0] {
            Fact::Assertion { is, .. } => {
                assert_eq!(*is, Value::String("Manager".to_string()));
            }
            _ => panic!("Expected Assertion"),
        }

        assert_eq!(salary_facts.len(), 1);
        match &salary_facts[0] {
            Fact::Assertion { is, .. } => {
                assert_eq!(*is, Value::UnsignedInt(120000));
            }
            _ => panic!("Expected Assertion"),
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn test_multiple_attributes_retract() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Value};
        use crate::concept::With;
        use crate::{Entity, Fact, Session};
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

        let name_query = Fact::<Value>::select()
            .the("employee-txn/name")
            .of(dave.clone())
            .compile()?;

        let job_query = Fact::<Value>::select()
            .the("employee-txn/job")
            .of(dave.clone())
            .compile()?;

        let name_facts: Vec<_> = name_query
            .query(&Session::open(store.clone()))
            .try_collect()
            .await?;

        let job_facts: Vec<_> = job_query.query(&Session::open(store)).try_collect().await?;

        assert_eq!(name_facts.len(), 0);
        assert_eq!(job_facts.len(), 0);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_update_attribute() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Value};
        use crate::concept::With;
        use crate::{Entity, Fact, Session};
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

        let query = Fact::<Value>::select()
            .the("employee-txn/job")
            .of(eve)
            .compile()?;

        let job_facts: Vec<_> = query.query(&Session::open(store)).try_collect().await?;

        assert_eq!(job_facts.len(), 1);
        match &job_facts[0] {
            Fact::Assertion { is, .. } => {
                assert_eq!(*is, Value::String("Senior Developer".to_string()));
            }
            _ => panic!("Expected Assertion"),
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn test_entity_reference_attribute() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Value};
        use crate::concept::With;
        use crate::{Entity, Fact, Session};
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

        let query = Fact::<Value>::select()
            .the("employee-txn/manager")
            .of(employee_entity)
            .compile()?;

        let manager_facts: Vec<_> = query.query(&Session::open(store)).try_collect().await?;

        assert_eq!(manager_facts.len(), 1);
        match &manager_facts[0] {
            Fact::Assertion { is, .. } => {
                assert_eq!(*is, Value::Entity(manager));
            }
            _ => panic!("Expected Assertion"),
        }

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
    async fn test_with_query_shortcut() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::concept::With;
        use crate::{Concept, Entity, Session};
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
            With::<employee_shortcut::Name>::query(session.clone())
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
            With::<employee_shortcut::Job>::query(session.clone())
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
    async fn test_attribute_claim() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::claim::Claim;
        use crate::concept::With;
        use crate::{Entity, Match, Session, Term, Transaction};
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

        let query = Match::<With<note_concept::Title>> {
            this: Term::from(entity.clone()),
            has: Term::var("has"),
        };

        let premise: crate::Premise = query.into();
        let application = match premise {
            crate::Premise::Apply(app) => app,
            _ => panic!("Expected Apply premise"),
        };

        let results = application.query(&session).try_collect::<Vec<_>>().await?;

        assert_eq!(results.len(), 1);

        Ok(())
    }

    mod note_rule {
        use crate::Attribute;

        #[derive(Attribute, Clone)]
        pub struct Title(pub String);
    }

    #[dialog_common::test]
    async fn test_adhoc_concept_query() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::concept::With;
        use crate::{Entity, Match, Relation, Session, Term, Value};
        use dialog_artifacts::Attribute as ArtifactAttribute;
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
                Relation {
                    the: "note-rule/title".parse::<ArtifactAttribute>()?,
                    of: note1.clone(),
                    is: Value::String("First Note".to_string()),
                },
                Relation {
                    the: "note-rule/title".parse::<ArtifactAttribute>()?,
                    of: note2.clone(),
                    is: Value::String("Second Note".to_string()),
                },
                Relation {
                    the: "note-rule/title".parse::<ArtifactAttribute>()?,
                    of: note3.clone(),
                    is: Value::String("Third Note".to_string()),
                },
            ])
            .await?;

        let query = Match::<With<note_rule::Title>> {
            this: Term::var("entity"),
            has: Term::var("has"),
        };

        let premise: crate::Premise = query.into();

        let application = match premise {
            crate::Premise::Apply(app) => app,
            _ => panic!("Expected Apply premise"),
        };

        let results = application.query(&session).try_collect::<Vec<_>>().await?;

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
    async fn test_adhoc_concept_query_with_filter() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::concept::With;
        use crate::{Entity, Match, Relation, Session, Term, Value};
        use dialog_artifacts::Attribute as ArtifactAttribute;
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
                Relation {
                    the: "note-rule/title".parse::<ArtifactAttribute>()?,
                    of: note1.clone(),
                    is: Value::String("Target Note".to_string()),
                },
                Relation {
                    the: "note-rule/title".parse::<ArtifactAttribute>()?,
                    of: note2.clone(),
                    is: Value::String("Other Note".to_string()),
                },
                Relation {
                    the: "note-rule/title".parse::<ArtifactAttribute>()?,
                    of: note3.clone(),
                    is: Value::String("Another Note".to_string()),
                },
            ])
            .await?;

        let query = Match::<With<note_rule::Title>> {
            this: Term::var("entity"),
            has: Term::from("Target Note".to_string()),
        };

        let premise: crate::Premise = query.into();

        let application = match premise {
            crate::Premise::Apply(app) => app,
            _ => panic!("Expected Apply premise"),
        };

        let results = application.query(&session).try_collect::<Vec<_>>().await?;

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
