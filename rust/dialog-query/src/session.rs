//! Database sessions and query sessions
//!
//! This module provides two main types:
//! - `Session`: For committing changes and rule-aware querying
//! - `QuerySession`: For read-only rule-aware querying

pub mod transaction;

use crate::artifact::{ArtifactStore, DialogArtifactsError};
use crate::query::Source;
use crate::session::transaction::{Transaction, TransactionError};
use crate::{DeductiveRule, Store};
use std::collections::HashMap;
use transaction::Edit;

/// A database session for committing changes
///
/// Sessions provide a high-level interface for committing claims to the database.
/// Accepts various input types like `Vec<Claim>`, single claims, or `Claims` collections.
///
/// # Examples
///
/// ```ignore
/// use dialog_query::{Session, Fact};
///
/// // Open a session
/// let mut session = Session::open(store);
///
/// // Commit individual claims
/// session.commit(Fact::assert("user/name".parse()?, entity, "Alice".to_string())).await?;
///
/// // Commit multiple claims at once
/// session.commit(vec![
///     Fact::assert("user/name".parse()?, entity1, "Alice".to_string()),
///     Fact::assert("user/name".parse()?, entity2, "Bob".to_string()),
///     Fact::retract("user/email".parse()?, entity1, "old@example.com".to_string()),
/// ]).await?;
/// ```
#[derive(Debug, Clone)]
pub struct Session<S: Store> {
    /// The underlying store for database operations
    store: S,
    /// Registry of the rules
    rules: HashMap<String, Vec<DeductiveRule>>,
}

impl<S: Store> Session<S> {
    /// Open a new session with the provided store
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use dialog_query::Session;
    ///
    /// let session = Session::open(artifacts_store);
    /// ```
    pub fn open(store: S) -> Self {
        Session {
            store,
            rules: HashMap::new(),
        }
    }

    /// Register a new rule into the session
    pub fn register(mut self, rule: DeductiveRule) -> Self {
        if let Some(rules) = self.rules.get_mut(rule.conclusion.operator()) {
            if !rules.contains(&rule) {
                rules.push(rule);
            }
        } else {
            self.rules
                .insert(rule.conclusion.operator().into(), vec![rule.clone()]);
        }

        self
    }

    /// Install a rule from a function - concept inferred from function parameter.
    ///
    /// # Example
    /// ```rust,ignore
    /// use dialog_query::{Session, Match, IntoWhen};
    ///
    /// fn person_rule(person: Match<Person>) -> impl IntoWhen {
    ///     (
    ///         Match::<Employee> {
    ///             this: person.this,
    ///             name: person.name
    ///         },
    ///     )
    /// }
    ///
    /// session.install(person_rule)?;
    /// ```
    pub fn install<M, W>(self, rule: impl Fn(M) -> W) -> Result<Self, crate::error::CompileError>
    where
        M: crate::concept::Match,
        W: crate::rule::When,
    {
        let query = M::default();
        let concept = query.to_concept();
        let when = rule(query).into_premises();
        let premises = when.into_vec();
        let rule = crate::predicate::DeductiveRule::new(concept, premises)?;
        Ok(self.register(rule))
    }

    /// Create a new transaction for imperative API usage
    ///
    /// Returns a Transaction that can be used to batch operations before committing.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use dialog_query::{Session, Fact, Relation};
    ///
    /// let mut session = Session::open(store);
    /// let mut transaction = session.edit();
    ///
    /// // Add operations to the transaction
    /// transaction.assert(Relation::new(attr, entity, value));
    /// transaction.retract(Relation::new(attr2, entity, old_value));
    ///
    /// // Commit the transaction
    /// session.commit(transaction).await?;
    /// ```
    pub fn edit(&self) -> Transaction {
        Transaction::new()
    }

    /// Commit a transaction to the database
    ///
    /// Takes ownership of a Transaction and commits all its operations.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use dialog_query::{Session, Relation};
    ///
    /// let mut session = Session::open(store);
    /// let mut edit = session.edit();
    ///
    /// edit.assert(Relation::new(attr, entity, value));
    /// session.commit(edit).await?;
    /// ```
    pub async fn commit(&mut self, transaction: Transaction) -> Result<(), TransactionError> {
        self.store.commit(transaction.into_stream()).await?;
        Ok(())
    }

    /// Legacy method - converts claims to instructions directly
    ///
    /// Accepts `Vec<Claim>`, single claims, or `Claims` collections.
    /// This method is kept for backwards compatibility.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use dialog_query::{Session, Fact};
    ///
    /// let mut session = Session::open(store);
    ///
    /// // Can pass any items that implement Into<Claim>
    /// session.transact([
    ///     Employee { this: alice, name: "Alice".into(), role: "CEO".into() },
    ///     Relation { the: "user/name".parse()?, of: bob, is: "Bob".to_string() }
    /// ]).await?;
    /// ```
    pub async fn transact<E, D>(&mut self, changes: D) -> Result<(), DialogArtifactsError>
    where
        E: Edit,
        D: IntoIterator<Item = E>,
    {
        let mut transaction = self.edit();
        // Go over each change and merge it into the transaction
        for change in changes {
            change.merge(&mut transaction);
        }

        // commit transaction.
        self.store.commit(transaction.into_stream()).await?;
        Ok(())
    }
}

/// A read-only query session that provides rule-aware querying capabilities
///
/// QuerySession is a lighter-weight alternative to Session that focuses purely on querying
/// with rule resolution support. It can be created from any ArtifactStore and provides
/// an explicit way to enable rule-based inference during queries.
///
/// # Examples
///
/// ```ignore
/// use dialog_query::{QuerySession, Fact};
///
/// // Convert an artifact store to a query session
/// let query_session: QuerySession<_> = artifacts.into();
///
/// // Query with rule resolution
/// let results = concept.query(&query_session)?;
///
/// // Install rules for more advanced querying
/// let query_session = query_session.install(adult_rule);
/// ```
#[derive(Debug, Clone)]
pub struct QuerySession<S: ArtifactStore> {
    /// The underlying store for read operations
    store: S,
    /// Registry of the rules for inference
    rules: HashMap<String, Vec<DeductiveRule>>,
}

impl<S: ArtifactStore> QuerySession<S> {
    /// Create a new query session wrapping the provided store
    ///
    /// The query session starts with no rules and can have rules installed later.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use dialog_query::QuerySession;
    ///
    /// let query_session = QuerySession::new(artifacts);
    /// ```
    pub fn new(store: S) -> Self {
        Self {
            store,
            rules: HashMap::new(),
        }
    }

    /// Install a deductive rule into the query session
    ///
    /// Rules are indexed by their conclusion operator and can be used during
    /// query evaluation to derive facts that aren't directly stored.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let query_session = QuerySession::new(artifacts)
    ///     .install(adult_rule)
    ///     .install(senior_rule);
    /// ```
    pub fn install(mut self, rule: DeductiveRule) -> Self {
        if let Some(rules) = self.rules.get_mut(rule.conclusion.operator()) {
            if !rules.contains(&rule) {
                rules.push(rule);
            }
        } else {
            self.rules
                .insert(rule.conclusion.operator().into(), vec![rule]);
        }
        self
    }

    /// Get a reference to the underlying store
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Get a reference to the rules registry
    pub fn rules(&self) -> &HashMap<String, Vec<DeductiveRule>> {
        &self.rules
    }
}

/// Create QuerySession from any ArtifactStore
impl<S: ArtifactStore + Clone + Send + Sync + 'static> From<S> for QuerySession<S> {
    fn from(store: S) -> Self {
        Self::new(store)
    }
}

/// Implement Source trait for QuerySession to provide rule resolution capabilities
impl<S: ArtifactStore + Clone + Send + Sync + 'static> Source for QuerySession<S> {
    fn resolve_rules(&self, operator: &str) -> Vec<DeductiveRule> {
        self.rules.get(operator).cloned().unwrap_or_default()
    }
}

/// Forward ArtifactStore methods to the wrapped store
impl<S: ArtifactStore> ArtifactStore for QuerySession<S> {
    fn select(
        &self,
        artifact_selector: crate::artifact::ArtifactSelector<crate::artifact::Constrained>,
    ) -> impl futures_core::Stream<
        Item = Result<crate::artifact::Artifact, crate::artifact::DialogArtifactsError>,
    > + crate::artifact::ConditionalSend
           + 'static {
        self.store.select(artifact_selector)
    }
}

/// Implement Source trait for Session to provide rule resolution capabilities
///
/// This implementation allows Session to be used directly with the Query trait
/// while providing access to both stored artifacts and registered rules.
impl<S: Store + Sync + 'static> Source for Session<S> {
    fn resolve_rules(&self, operator: &str) -> Vec<DeductiveRule> {
        self.rules.get(operator).cloned().unwrap_or_default()
    }
}

/// Forward ArtifactStore methods to the wrapped store
impl<S: Store> crate::artifact::ArtifactStore for Session<S> {
    fn select(
        &self,
        artifact_selector: crate::artifact::ArtifactSelector<crate::artifact::Constrained>,
    ) -> impl futures_core::Stream<
        Item = Result<crate::artifact::Artifact, crate::artifact::DialogArtifactsError>,
    > + crate::artifact::ConditionalSend
           + 'static {
        self.store.select(artifact_selector)
    }
}

#[cfg(test)]
mod tests {
    // Allow the derive macro to reference dialog_query:: from within the crate
    extern crate self as dialog_query;

    use std::collections::HashMap;

    use crate::{
        predicate::{self, concept::Attributes, Fact},
        Attribute, Parameters, Relation, Type,
    };

    use super::*;

    #[tokio::test]
    async fn test_session() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Attribute as ArtifactAttribute, Entity, Value};
        use crate::Term;
        use dialog_storage::MemoryStorageBackend;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);
        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let mallory = Entity::new()?;

        session
            .transact(vec![
                Relation {
                    the: "person/name".parse::<ArtifactAttribute>()?,
                    of: alice.clone(),
                    is: Value::String("Alice".to_string()),
                },
                Relation {
                    the: "person/age".parse::<ArtifactAttribute>()?,
                    of: alice.clone(),
                    is: Value::UnsignedInt(25),
                },
                Relation {
                    the: "person/name".parse::<ArtifactAttribute>()?,
                    of: bob.clone(),
                    is: Value::String("Bob".to_string()),
                },
                Relation {
                    the: "person/age".parse::<ArtifactAttribute>()?,
                    of: bob.clone(),
                    is: Value::UnsignedInt(30),
                },
                Relation {
                    the: "person/name".parse::<ArtifactAttribute>()?,
                    of: mallory.clone(),
                    is: Value::String("Mallory".to_string()),
                },
            ])
            .await?;

        let person = predicate::Concept::Dynamic {
            operator: "person".into(),
            attributes: [
                (
                    "name",
                    Attribute::<Value>::new(&"person", &"name", &"person name", Type::String),
                ),
                (
                    "age",
                    Attribute::<Value>::new(&"person", &"age", &"person age", Type::UnsignedInt),
                ),
            ]
            .into(),
        };

        let name = Term::var("name");
        let age = Term::var("age");
        let mut params = Parameters::new();
        params.insert("name".into(), name.clone());
        params.insert("age".into(), age.clone());

        // Use new query API directly on application
        let application = person.apply(params)?;

        let selection =
            futures_util::TryStreamExt::try_collect::<Vec<_>>(application.query(&session)).await?;
        assert_eq!(selection.len(), 2); // Should find just Alice and Bob

        // Check that we have both Alice and Bob (order may vary)
        let mut found_alice = false;
        let mut found_bob = false;

        for match_result in selection.iter() {
            let person_name = match_result.resolve(&name)?;
            let person_age = match_result.resolve(&age)?;

            match person_name {
                Value::String(name_str) if name_str == "Alice" => {
                    assert_eq!(person_age, Value::UnsignedInt(25));
                    found_alice = true;
                }
                Value::String(name_str) if name_str == "Bob" => {
                    assert_eq!(person_age, Value::UnsignedInt(30));
                    found_bob = true;
                }
                _ => panic!("Unexpected person: {:?}", person_name),
            }
        }

        assert!(found_alice, "Should find Alice");
        assert!(found_bob, "Should find Bob");

        Ok(())
    }

    #[tokio::test]
    async fn test_matches_complete_conepts() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Entity};
        use crate::Concept;
        use dialog_storage::MemoryStorageBackend;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut _session = Session::open(store);

        #[derive(Debug, Clone, PartialEq, Concept)]
        pub struct Person {
            this: Entity,
            name: String,
            age: u32,
        }

        // let alice = Entity::new()?;
        // let bob = Entity::new()?;
        // let mallory = Entity::new()?;

        // session
        //     .transact(vec![
        //         Fact::assert(
        //             "person/name".parse::<ArtifactAttribute>()?,
        //             alice.clone(),
        //             Value::String("Alice".to_string()),
        //         ),
        //         Fact::assert(
        //             "person/age".parse::<ArtifactAttribute>()?,
        //             alice.clone(),
        //             Value::UnsignedInt(25),
        //         ),
        //         Fact::assert(
        //             "person/name".parse::<ArtifactAttribute>()?,
        //             bob.clone(),
        //             Value::String("Bob".to_string()),
        //         ),
        //         Fact::assert(
        //             "person/age".parse::<ArtifactAttribute>()?,
        //             bob.clone(),
        //             Value::UnsignedInt(30),
        //         ),
        //         Fact::assert(
        //             "person/name".parse::<ArtifactAttribute>()?,
        //             mallory.clone(),
        //             Value::String("Mallory".to_string()),
        //         ),
        //     ])
        //     .await?;

        // let person = predicate::Concept {
        //     operator: "person".into(),
        //     attributes: [
        //         (
        //             "name",
        //             Attribute::new(&"person", &"name", &"person name", Type::String),
        //         ),
        //         (
        //             "age",
        //             Attribute::new(&"person", &"age", &"person age", Type::UnsignedInt),
        //         ),
        //     ]
        //     .into(),
        // };

        // let name = Term::var("name");
        // let mut params = Parameters::new();
        // params.insert("name".into(), name.clone());

        // // Use new query API directly on application
        // let application = Person::apply(params)?;

        // let selection = application.query(&session)?.collect_matches().await?;
        // assert_eq!(selection.len(), 2); // Should find just Alice and Bob

        // // Check that we have both Alice and Bob (order may vary)
        // let mut found_alice = false;
        // let mut found_bob = false;

        // for match_result in selection.iter() {
        //     let person_name = match_result.resolve(&name)?;

        //     match person_name {
        //         Value::String(name_str) if name_str == "Alice" => {
        //             found_alice = true;
        //         }
        //         Value::String(name_str) if name_str == "Bob" => {
        //             found_bob = true;
        //         }
        //         _ => panic!("Unexpected person: {:?}", person_name),
        //     }
        // }

        // assert!(found_alice, "Should find Alice");
        // assert!(found_bob, "Should find Bob");

        Ok(())
    }

    #[tokio::test]
    #[ignore] // TODO: Migrate from obsolete planning API - this test validates planning behavior
    async fn test_concept_planning_mixed_parameters() -> anyhow::Result<()> {
        use crate::artifact::Type;
        use crate::Term;

        // Set up concept with attributes
        let mut attributes = HashMap::new();
        attributes.insert(
            "name".into(),
            Attribute::new(&"person", &"name", &"person name", Type::String),
        );
        attributes.insert(
            "age".into(),
            Attribute::new(&"person", &"age", &"person age", Type::UnsignedInt),
        );

        let person = predicate::Concept::Dynamic {
            operator: "person".into(),
            attributes: Attributes::from(attributes),
        };

        // Mixed case - valid parameters with some matching attributes (should succeed)
        let mut mixed_params = Parameters::new();
        mixed_params.insert("name".into(), Term::var("person_name")); // This matches
        mixed_params.insert("age".into(), Term::blank()); // This matches but is blank

        person.apply(mixed_params)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_assert_concept() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Value};
        use crate::Term;
        use dialog_storage::MemoryStorageBackend;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let person = predicate::Concept::Dynamic {
            operator: "person".into(),
            attributes: [
                (
                    "name",
                    Attribute::new(&"person", &"name", &"person name", Type::String),
                ),
                (
                    "age",
                    Attribute::new(&"person", &"age", &"person age", Type::UnsignedInt),
                ),
            ]
            .into(),
        };

        let alice = person
            .create()
            .with("name", "Alice".to_string())
            .with("age", 25usize)
            .build()?;

        let bob = person
            .create()
            .with("name", "Bob".to_string())
            .with("age", 30usize)
            .build()?;

        session.transact(vec![alice, bob]).await?;

        let name = Term::var("name");
        let age = Term::var("age");
        let mut params = Parameters::new();
        params.insert("name".into(), name.clone());
        params.insert("age".into(), age.clone());

        // Let's test with empty parameters first to see the exact error
        let application = person.apply(params)?;

        let selection =
            futures_util::TryStreamExt::try_collect::<Vec<_>>(application.query(&session)).await?;
        assert_eq!(selection.len(), 2); // Should find just Alice and Bob

        // Check that we have both Alice and Bob (order may vary)
        let mut found_alice = false;
        let mut found_bob = false;

        for match_result in selection.iter() {
            let person_name = match_result.resolve(&name)?;
            let person_age = match_result.resolve(&age)?;

            match person_name {
                Value::String(name_str) if name_str == "Alice" => {
                    assert_eq!(person_age, Value::UnsignedInt(25));
                    found_alice = true;
                }
                Value::String(name_str) if name_str == "Bob" => {
                    assert_eq!(person_age, Value::UnsignedInt(30));
                    found_bob = true;
                }
                _ => panic!("Unexpected person: {:?}", person_name),
            }
        }

        assert!(found_alice, "Should find Alice");
        assert!(found_bob, "Should find Bob");

        Ok(())
    }

    #[tokio::test]
    async fn test_rule() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Entity};
        use crate::query::Output;
        use crate::rule::Match;
        use crate::{Concept, Fact, Term};
        use dialog_storage::MemoryStorageBackend;

        #[derive(Clone, Debug, PartialEq, Concept)]
        pub struct Employee {
            /// Employee
            pub this: Entity,
            /// Employee Name
            pub name: String,
            /// The job title of the employee
            pub job: String,
        }

        #[derive(Clone, Debug, PartialEq, Concept)]
        pub struct Stuff {
            pub this: Entity,
            /// Name of the stuff member
            pub name: String,
            /// Role of the stuff member
            pub role: String,
        }

        // employee can be derived from the stuff concept
        let employee_from_stuff = DeductiveRule::new(
            <Employee as Concept>::CONCEPT,
            vec![
                Fact::<String>::select()
                    .the("stuff/name")
                    .of(Term::var("this"))
                    .is(Term::var("name"))
                    .compile()?
                    .into(),
                Fact::<String>::select()
                    .the("stuff/role")
                    .of(Term::var("this"))
                    .is(Term::var("job"))
                    .compile()?
                    .into(),
            ],
        )?;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store).register(employee_from_stuff);

        let alice = Stuff::CONCEPT
            .create()
            .with("name", "Alice".to_string())
            .with("role", "manager".to_string())
            .build()?;

        let bob = Stuff::CONCEPT
            .create()
            .with("name", "Bob".to_string())
            .with("role", "developer".to_string())
            .build()?;

        let _mallory = Stuff {
            this: Entity::new()?,
            name: "Mallory".into(),
            role: "developer".into(),
        };

        session.transact(vec![alice, bob]).await?;

        let query_stuff = Match::<Stuff> {
            this: Term::var("stuff"),
            name: Term::var("name"),
            role: Term::var("job"),
        };

        let stuff = query_stuff.query(session.clone()).try_vec().await?;

        assert_eq!(stuff.len(), 2);

        // Now we query for employees and expect that employee_from_stuff
        // rule will provide a translation
        let query_employee = Match::<Employee> {
            this: Term::var("employee"),
            name: Term::var("name"),
            job: Term::var("job"),
        };

        let employees = Output::try_vec(query_employee.query(session)).await?;

        assert_eq!(employees.len(), 2);
        println!("{:?}", employees);

        Ok(())
    }

    #[tokio::test]
    async fn test_install_rule_api() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Entity};
        use crate::query::Output;
        use crate::rule::When;
        use crate::{Concept, Match, Term};
        use dialog_storage::MemoryStorageBackend;

        #[derive(Clone, Debug, PartialEq, Concept)]
        pub struct Employee {
            /// Employee
            pub this: Entity,
            /// Employee Name
            pub name: String,
            /// The job title of the employee
            pub job: String,
        }

        #[derive(Clone, Debug, PartialEq, Concept)]
        pub struct Stuff {
            pub this: Entity,
            /// Name of the stuff member
            pub name: String,
            /// Role of the stuff member
            pub role: String,
        }

        // Define a rule using the clean function API - no manual DeductiveRule construction!
        fn employee_from_stuff(employee: Match<Employee>) -> impl When {
            // This rule says: "An employee exists when there's stuff with matching attributes"
            // The premises check for stuff/name and stuff/role matching employee/name and employee/job
            (
                Match::<Stuff> {
                    this: employee.this.clone(),
                    name: employee.name.clone(),
                    role: employee.job,
                },
                Fact {
                    the: "stuff/name"
                        .parse::<crate::artifact::Attribute>()
                        .unwrap()
                        .into(),
                    of: employee.this,
                    is: employee.name.as_unknown(),
                    cause: Term::blank(),
                }
                .compile()
                .unwrap(),
            )
        }

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;

        // Install the rule using the clean API - no turbofish needed!
        // The type inference works: Employee is inferred from the function parameter
        let mut session = Session::open(store).install(employee_from_stuff)?;

        // Create test data as Stuff
        let alice = Stuff::CONCEPT
            .create()
            .with("name", "Alice".to_string())
            .with("role", "manager".to_string())
            .build()?;

        let bob = Stuff::CONCEPT
            .create()
            .with("name", "Bob".to_string())
            .with("role", "developer".to_string())
            .build()?;

        session.transact(vec![alice, bob]).await?;

        // Verify Stuff records exist
        let query_stuff = Match::<Stuff> {
            this: Term::var("stuff"),
            name: Term::var("name"),
            role: Term::var("job"),
        };

        let stuff = query_stuff.query(session.clone()).try_vec().await?;
        assert_eq!(stuff.len(), 2, "Should have 2 Stuff records");

        // Query for Employees - the rule should derive them from Stuff
        let query_employee = Match::<Employee> {
            this: Term::var("employee"),
            name: Term::var("name"),
            job: Term::var("job"),
        };

        let employees = Output::try_vec(query_employee.query(session)).await?;

        // The rule should have derived 2 Employee instances from the 2 Stuff instances
        assert_eq!(
            employees.len(),
            2,
            "Rule should derive 2 employees from stuff"
        );

        // Verify the derived data is correct
        let mut found_alice = false;
        let mut found_bob = false;

        for employee in employees {
            match employee.name.as_str() {
                "Alice" => {
                    assert_eq!(employee.job, "manager");
                    found_alice = true;
                }
                "Bob" => {
                    assert_eq!(employee.job, "developer");
                    found_bob = true;
                }
                name => panic!("Unexpected employee: {}", name),
            }
        }

        assert!(found_alice, "Should find Alice as an employee");
        assert!(found_bob, "Should find Bob as an employee");

        Ok(())
    }
}
