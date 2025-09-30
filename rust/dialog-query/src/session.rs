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

    /// Install a new rule into the session
    pub fn install(mut self, rule: DeductiveRule) -> Self {
        if let Some(rules) = self.rules.get_mut(&rule.conclusion.operator) {
            if !rules.contains(&rule) {
                rules.push(rule);
            }
        } else {
            self.rules
                .insert(rule.conclusion.operator.clone(), vec![rule.clone()]);
        }

        self
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
    /// // Legacy API - use the new transaction-based methods instead
    /// session.transact(vec![
    ///     Fact::assert("user/name".parse()?, alice, "Alice".to_string()),
    /// ]).await?;
    /// ```
    pub async fn transact<I: IntoIterator<Item = crate::claim::Claim>>(
        &mut self,
        changes: I,
    ) -> Result<(), DialogArtifactsError> {
        let mut transaction = self.edit();
        // Go over each change and merge it into the transaction
        for claim in changes {
            claim.merge(&mut transaction);
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
        if let Some(rules) = self.rules.get_mut(&rule.conclusion.operator) {
            if !rules.contains(&rule) {
                rules.push(rule);
            }
        } else {
            self.rules
                .insert(rule.conclusion.operator.clone(), vec![rule]);
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
        self.rules
            .get(operator)
            .map(|rules| rules.clone())
            .unwrap_or_else(Vec::new)
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
        self.rules
            .get(operator)
            .map(|rules| rules.clone())
            .unwrap_or_else(Vec::new)
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
    use std::collections::HashMap;

    use crate::{
        predicate::{self, Concept},
        query::PlannedQuery,
        Attribute, Parameters, SelectionExt, Type, VariableScope,
    };

    use super::*;

    #[tokio::test]
    async fn test_session() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Attribute as ArtifactAttribute, Entity, Value};
        use crate::{Fact, Term};
        use dialog_storage::MemoryStorageBackend;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);
        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let mallory = Entity::new()?;

        session
            .transact(vec![
                Fact::assert(
                    "person/name".parse::<ArtifactAttribute>()?,
                    alice.clone(),
                    Value::String("Alice".to_string()),
                ),
                Fact::assert(
                    "person/age".parse::<ArtifactAttribute>()?,
                    alice.clone(),
                    Value::UnsignedInt(25),
                ),
                Fact::assert(
                    "person/name".parse::<ArtifactAttribute>()?,
                    bob.clone(),
                    Value::String("Bob".to_string()),
                ),
                Fact::assert(
                    "person/age".parse::<ArtifactAttribute>()?,
                    bob.clone(),
                    Value::UnsignedInt(30),
                ),
                Fact::assert(
                    "person/name".parse::<ArtifactAttribute>()?,
                    mallory.clone(),
                    Value::String("Mallory".to_string()),
                ),
            ])
            .await?;

        let mut attributes = HashMap::new();
        attributes.insert(
            "name".into(),
            Attribute::new(&"person", &"name", &"person name", Type::String),
        );
        attributes.insert(
            "age".into(),
            Attribute::new(&"person", &"age", &"person age", Type::UnsignedInt),
        );

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        let name = Term::var("name");
        let age = Term::var("age");
        let mut params = Parameters::new();
        params.insert("name".into(), name.clone());
        params.insert("age".into(), age.clone());

        // Let's test with empty parameters first to see the exact error
        let application = person.apply(params)?;

        let plan = application.plan(&VariableScope::new())?;

        let selection = plan.query(&session)?.collect_matches().await?;
        assert_eq!(selection.len(), 2); // Should find just Alice and Bob

        // Check that we have both Alice and Bob (order may vary)
        let mut found_alice = false;
        let mut found_bob = false;

        for match_result in selection.iter() {
            let person_name = match_result.get(&name)?;
            let person_age = match_result.get(&age)?;

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
        use crate::artifact::{Artifacts, Attribute as ArtifactAttribute, Entity, Type, Value};
        use crate::{Fact, Term};
        use dialog_storage::MemoryStorageBackend;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);
        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let mallory = Entity::new()?;

        session
            .transact(vec![
                Fact::assert(
                    "person/name".parse::<ArtifactAttribute>()?,
                    alice.clone(),
                    Value::String("Alice".to_string()),
                ),
                Fact::assert(
                    "person/age".parse::<ArtifactAttribute>()?,
                    alice.clone(),
                    Value::UnsignedInt(25),
                ),
                Fact::assert(
                    "person/name".parse::<ArtifactAttribute>()?,
                    bob.clone(),
                    Value::String("Bob".to_string()),
                ),
                Fact::assert(
                    "person/age".parse::<ArtifactAttribute>()?,
                    bob.clone(),
                    Value::UnsignedInt(30),
                ),
                Fact::assert(
                    "person/name".parse::<ArtifactAttribute>()?,
                    mallory.clone(),
                    Value::String("Mallory".to_string()),
                ),
            ])
            .await?;

        let mut attributes = HashMap::new();
        attributes.insert(
            "name".into(),
            Attribute::new(&"person", &"name", &"person name", Type::String),
        );
        attributes.insert(
            "age".into(),
            Attribute::new(&"person", &"age", &"person age", Type::UnsignedInt),
        );

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        let name = Term::var("name");
        let mut params = Parameters::new();
        params.insert("name".into(), name.clone());

        // Let's test with empty parameters first to see the exact error
        let application = person.apply(params)?;

        let plan = application.plan(&VariableScope::new())?;

        let selection = plan.query(&session)?.collect_matches().await?;
        assert_eq!(selection.len(), 2); // Should find just Alice and Bob

        // Check that we have both Alice and Bob (order may vary)
        let mut found_alice = false;
        let mut found_bob = false;

        for match_result in selection.iter() {
            let person_name = match_result.get(&name)?;

            match person_name {
                Value::String(name_str) if name_str == "Alice" => {
                    found_alice = true;
                }
                Value::String(name_str) if name_str == "Bob" => {
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
    async fn test_concept_planning_empty_parameters() -> anyhow::Result<()> {
        use crate::artifact::Type;
        use crate::error::PlanError;

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

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        // Empty parameters should return UnparameterizedApplication error
        let empty_params = Parameters::new();
        let application = person.apply(empty_params)?;
        let result = application.plan(&VariableScope::new());

        assert!(result.is_err());
        if let Err(PlanError::UnparameterizedApplication) = result {
            // Expected error
        } else {
            panic!(
                "Expected UnparameterizedApplication error, got: {:?}",
                result
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_concept_planning_all_blank_parameters() -> anyhow::Result<()> {
        use crate::artifact::Type;
        use crate::error::PlanError;
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

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        // All blank parameters should return UnparameterizedApplication error
        let mut all_blank_params = Parameters::new();
        all_blank_params.insert("name".into(), Term::blank());
        all_blank_params.insert("age".into(), Term::blank());

        let application = person.apply(all_blank_params)?;
        let result = application.plan(&VariableScope::new());

        assert!(result.is_err());
        if let Err(PlanError::UnparameterizedApplication) = result {
            // Expected error
        } else {
            panic!(
                "Expected UnparameterizedApplication error, got: {:?}",
                result
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_concept_planning_only_this_parameter() -> anyhow::Result<()> {
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

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        // Only "this" parameter provided - should fail since it doesn't constrain any attributes
        let mut only_this_params = Parameters::new();
        only_this_params.insert("this".into(), Term::var("entity")); // Non-blank "this"

        let application = person.apply(only_this_params)?;
        let result = application.plan(&VariableScope::new());

        // Should succeed because "this" parameter provides a constraint
        assert!(
            result.is_ok(),
            "Only 'this' parameter should succeed, got: {:?}",
            result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_concept_planning_unknown_parameters() -> anyhow::Result<()> {
        use crate::artifact::Type;
        use crate::error::PlanError;
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

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        // Unknown parameters only (no "this" or concept attributes) should fail
        let mut unknown_params = Parameters::new();
        unknown_params.insert("unknown_param".into(), Term::var("x"));

        let application = person.apply(unknown_params)?;
        let result = application.plan(&VariableScope::new());

        assert!(result.is_err());
        if let Err(PlanError::UnparameterizedApplication) = result {
            // Expected error - no meaningful parameters for this concept
        } else {
            panic!(
                "Expected UnparameterizedApplication error, got: {:?}",
                result
            );
        }

        Ok(())
    }

    #[tokio::test]
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

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        // Mixed case - valid parameters with some matching attributes (should succeed)
        let mut mixed_params = Parameters::new();
        mixed_params.insert("name".into(), Term::var("person_name")); // This matches
        mixed_params.insert("age".into(), Term::blank()); // This matches but is blank

        let application = person.apply(mixed_params)?;
        let result = application.plan(&VariableScope::new());

        // Should succeed because we have at least one non-blank parameter
        assert!(
            result.is_ok(),
            "Mixed parameters with at least one non-blank should succeed, got: {:?}",
            result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_improved_error_messages() -> anyhow::Result<()> {
        use crate::artifact::Type;

        // Set up concept
        let mut attributes = HashMap::new();
        attributes.insert(
            "name".into(),
            Attribute::new(&"person", &"name", &"person name", Type::String),
        );

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        // Test that error messages are now helpful instead of "UnexpectedError"
        let empty_application = person.apply(Parameters::new())?;
        let error = empty_application.plan(&VariableScope::new()).unwrap_err();

        let error_message = error.to_string();
        println!("Empty parameters error: {}", error_message);
        assert!(error_message.contains("requires at least one non-blank parameter"));
        assert!(!error_message.contains("Unexpected error"));

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

        let mut attributes = HashMap::new();
        attributes.insert(
            "name".into(),
            Attribute::new(&"person", &"name", &"person name", Type::String),
        );
        attributes.insert(
            "age".into(),
            Attribute::new(&"person", &"age", &"person age", Type::UnsignedInt),
        );

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        let alice = person
            .create()?
            .with("name", "Alice".to_string())
            .with("age", 25usize)
            .assert()?;

        let bob = person
            .create()?
            .with("name", "Bob".to_string())
            .with("age", 30usize)
            .assert()?;

        session.transact(vec![alice, bob]).await?;

        let name = Term::var("name");
        let age = Term::var("age");
        let mut params = Parameters::new();
        params.insert("name".into(), name.clone());
        params.insert("age".into(), age.clone());

        // Let's test with empty parameters first to see the exact error
        let application = person.apply(params)?;

        let plan = application.plan(&VariableScope::new())?;

        let selection = plan.query(&session)?.collect_matches().await?;
        assert_eq!(selection.len(), 2); // Should find just Alice and Bob

        // Check that we have both Alice and Bob (order may vary)
        let mut found_alice = false;
        let mut found_bob = false;

        for match_result in selection.iter() {
            let person_name = match_result.get(&name)?;
            let person_age = match_result.get(&age)?;

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
        use crate::artifact::{Artifacts, Attribute as ArtifactAttribute, Entity, Value};
        use crate::{Fact, Term};
        use dialog_storage::MemoryStorageBackend;

        let employee = Concept::new("employee".into())
            .with(
                "name",
                Attribute::new("employee", "name", "Employee Name", Type::String),
            )
            .with(
                "role",
                Attribute::new(
                    "employee",
                    "job",
                    "The job title of the employee",
                    Type::String,
                ),
            );

        let stuff = Concept::new("stuff".into())
            .with(
                "name",
                Attribute::new("stuff", "name", "Stuff Name", Type::String),
            )
            .with(
                "role",
                Attribute::new(
                    "stuff",
                    "role",
                    "The role of the stuff member",
                    Type::String,
                ),
            );

        // employee can be derived from the stuff concept
        let employee_from_stuff = DeductiveRule {
            conclusion: employee,
            premises: vec![
                Fact::select()
                    .the("stuff/name")
                    .of(Term::var("this"))
                    .is(Term::var("name"))
                    .into(),
                Fact::select()
                    .the("stuff/role")
                    .of(Term::var("this"))
                    .is(Term::var("job"))
                    .into(),
            ],
        };

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store).install(employee_from_stuff);

        let alice = stuff
            .create()?
            .with("name", "Alice".to_string())
            .with("role", "manager".to_string())
            .assert()?;
        let bob = stuff
            .create()?
            .with("name", "Bob".to_string())
            .with("role", "developer".to_string())
            .assert()?;

        session.transact(vec![alice, bob]);
        let mut parameters = Parameters::new();
        parameters.insert("name".into(), Term::var("name"));
        parameters.insert("job".into(), Term::var("job"));

        let application = stuff.apply(parameters)?;
        let plan = application.plan(&VariableScope::new())?;

        let selection = plan.query(&session)?.collect_matches().await?;
        assert_eq!(selection.len(), 2);

        Ok(())
    }
}
