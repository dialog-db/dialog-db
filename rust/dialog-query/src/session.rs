//! Database sessions for committing changes
//!
//! Sessions provide a high-level interface for committing claims to the database.

use std::collections::HashMap;

use crate::artifact::DialogArtifactsError;
use crate::{DeductiveRule, Store};

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
        if let Some(rules) = self.rules.get_mut(&rule.operator) {
            if !rules.contains(&rule) {
                rules.push(rule);
            }
        } else {
            self.rules.insert(rule.operator.clone(), vec![rule.clone()]);
        }

        self
    }

    /// Transacts changes to the database
    ///
    /// Accepts `Vec<Claim>`, single claims, or `Claims` collections.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use dialog_query::{Session, Fact};
    ///
    /// let mut session = Session::open(store);
    ///
    /// // Transact a vector of claims (preferred API)
    /// session.transact(vec![
    ///     Fact::assert("user/name".parse()?, alice, "Alice".to_string()),
    ///     Fact::assert("user/age".parse()?, alice, 30u32),
    /// ]).await?;
    ///
    /// // Transact a single claim
    /// session.transact(Fact::retract("user/email".parse()?, alice, "old@example.com".to_string())).await?;
    /// ```
    pub async fn transact<I>(&mut self, changes: I) -> Result<(), DialogArtifactsError>
    where
        I: Into<crate::claim::Claims>,
    {
        let claims: crate::claim::Claims = changes.into();
        self.store.commit(claims).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dialog_artifacts::ValueDataType;

    use crate::{
        predicate, query::PlannedQuery, Attribute, Parameters, SelectionExt, VariableScope,
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
            Attribute::new(&"person", &"name", &"person name", ValueDataType::String),
        );
        attributes.insert(
            "age".into(),
            Attribute::new(&"person", &"age", &"person age", ValueDataType::UnsignedInt),
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
        let application = person.apply(params);

        let plan = application.plan(&VariableScope::new())?;

        let selection = plan.query(&session.store)?.collect_matches().await?;
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
        use crate::artifact::{
            Artifacts, Attribute as ArtifactAttribute, Entity, Value, ValueDataType,
        };
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
            Attribute::new(&"person", &"name", &"person name", ValueDataType::String),
        );
        attributes.insert(
            "age".into(),
            Attribute::new(&"person", &"age", &"person age", ValueDataType::UnsignedInt),
        );

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        let name = Term::var("name");
        let mut params = Parameters::new();
        params.insert("name".into(), name.clone());

        // Let's test with empty parameters first to see the exact error
        let application = person.apply(params);

        let plan = application.plan(&VariableScope::new())?;

        let selection = plan.query(&session.store)?.collect_matches().await?;
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
        use crate::artifact::ValueDataType;
        use crate::error::PlanError;

        // Set up concept with attributes
        let mut attributes = HashMap::new();
        attributes.insert(
            "name".into(),
            Attribute::new(&"person", &"name", &"person name", ValueDataType::String),
        );
        attributes.insert(
            "age".into(),
            Attribute::new(&"person", &"age", &"person age", ValueDataType::UnsignedInt),
        );

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        // Empty parameters should return UnparameterizedApplication error
        let empty_params = Parameters::new();
        let application = person.apply(empty_params);
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
        use crate::artifact::ValueDataType;
        use crate::error::PlanError;
        use crate::Term;

        // Set up concept with attributes
        let mut attributes = HashMap::new();
        attributes.insert(
            "name".into(),
            Attribute::new(&"person", &"name", &"person name", ValueDataType::String),
        );
        attributes.insert(
            "age".into(),
            Attribute::new(&"person", &"age", &"person age", ValueDataType::UnsignedInt),
        );

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        // All blank parameters should return UnparameterizedApplication error
        let mut all_blank_params = Parameters::new();
        all_blank_params.insert("name".into(), Term::blank());
        all_blank_params.insert("age".into(), Term::blank());

        let application = person.apply(all_blank_params);
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
        use crate::artifact::ValueDataType;
        use crate::Term;

        // Set up concept with attributes
        let mut attributes = HashMap::new();
        attributes.insert(
            "name".into(),
            Attribute::new(&"person", &"name", &"person name", ValueDataType::String),
        );
        attributes.insert(
            "age".into(),
            Attribute::new(&"person", &"age", &"person age", ValueDataType::UnsignedInt),
        );

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        // Only "this" parameter provided - should fail since it doesn't constrain any attributes
        let mut only_this_params = Parameters::new();
        only_this_params.insert("this".into(), Term::var("entity")); // Non-blank "this"

        let application = person.apply(only_this_params);
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
        use crate::artifact::ValueDataType;
        use crate::error::PlanError;
        use crate::Term;

        // Set up concept with attributes
        let mut attributes = HashMap::new();
        attributes.insert(
            "name".into(),
            Attribute::new(&"person", &"name", &"person name", ValueDataType::String),
        );
        attributes.insert(
            "age".into(),
            Attribute::new(&"person", &"age", &"person age", ValueDataType::UnsignedInt),
        );

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        // Unknown parameters only (no "this" or concept attributes) should fail
        let mut unknown_params = Parameters::new();
        unknown_params.insert("unknown_param".into(), Term::var("x"));

        let application = person.apply(unknown_params);
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
        use crate::artifact::ValueDataType;
        use crate::Term;

        // Set up concept with attributes
        let mut attributes = HashMap::new();
        attributes.insert(
            "name".into(),
            Attribute::new(&"person", &"name", &"person name", ValueDataType::String),
        );
        attributes.insert(
            "age".into(),
            Attribute::new(&"person", &"age", &"person age", ValueDataType::UnsignedInt),
        );

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        // Mixed case - valid parameters with some matching attributes (should succeed)
        let mut mixed_params = Parameters::new();
        mixed_params.insert("name".into(), Term::var("person_name")); // This matches
        mixed_params.insert("age".into(), Term::blank()); // This matches but is blank

        let application = person.apply(mixed_params);
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
        use crate::artifact::ValueDataType;

        // Set up concept
        let mut attributes = HashMap::new();
        attributes.insert(
            "name".into(),
            Attribute::new(&"person", &"name", &"person name", ValueDataType::String),
        );

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        // Test that error messages are now helpful instead of "UnexpectedError"
        let empty_application = person.apply(Parameters::new());
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
            Attribute::new(&"person", &"name", &"person name", ValueDataType::String),
        );
        attributes.insert(
            "age".into(),
            Attribute::new(&"person", &"age", &"person age", ValueDataType::UnsignedInt),
        );

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        let alice = person
            .new()?
            .with("name", "Alice".to_string())
            .with("age", 25usize)
            .assert()?;

        let bob = person
            .new()?
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
        let application = person.apply(params);

        let plan = application.plan(&VariableScope::new())?;

        let selection = plan.query(&session.store)?.collect_matches().await?;
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
}
