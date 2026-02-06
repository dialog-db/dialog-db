//! Test for fact provenance tracking with Answer

use anyhow::Result;
use dialog_query::{
    application::fact::FactApplication,
    artifact::{Artifacts, Attribute, Entity, Value},
    query::Output,
    selection::{Answer, Answers},
    term::Term,
    Cardinality, Relation, Session,
};
use dialog_storage::MemoryStorageBackend;
use futures_util::stream::once;

#[dialog_macros::test]
async fn test_fact_application_with_provenance() -> Result<()> {
    // Setup: Create in-memory storage and artifacts store
    let storage_backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(storage_backend).await?;

    // Step 1: Create test data
    let alice = Entity::new()?;
    let name_attr = "person/name".parse::<Attribute>()?;

    let claims = vec![Relation {
        the: name_attr.clone(),
        of: alice.clone(),
        is: Value::String("Alice".to_string()),
    }];

    let mut session = Session::open(artifacts.clone());
    session.transact(claims).await?;

    // Step 2: Create FactApplication
    let fact_app = FactApplication::new(
        Term::Constant(name_attr.clone()),
        Term::var("person"),
        Term::var("name"),
        Term::var("cause"),
        Cardinality::Many,
    );

    // Step 3: Evaluate with provenance
    let session = Session::open(artifacts);
    let initial_answer = once(async move { Ok(Answer::new()) });
    let answers = fact_app.evaluate_with_provenance(session, initial_answer);

    // Step 4: Collect results
    let results = Answers::try_vec(answers).await?;

    // Verify we got one result
    assert_eq!(results.len(), 1);

    let answer = &results[0];

    // Verify the bindings
    assert!(answer.contains(&Term::<Entity>::var("person")));
    assert!(answer.contains(&Term::<Value>::var("name")));

    // Resolve the values
    let person_id: Entity = answer.get(&Term::var("person"))?;
    let name_value: Value = answer.resolve(&Term::<Value>::var("name"))?;

    assert_eq!(person_id, alice);
    assert_eq!(name_value, Value::String("Alice".to_string()));

    // Verify provenance - the answer should track the fact
    let factors = answer
        .resolve_factors(&Term::<Value>::var("name"))
        .expect("name should have factors");

    // There should be evidence for this binding
    let evidence: Vec<_> = factors.evidence().collect();
    assert!(!evidence.is_empty(), "Should have at least one factor");

    Ok(())
}

#[dialog_macros::test]
async fn test_provenance_tracks_multiple_facts() -> Result<()> {
    // Setup
    let storage_backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(storage_backend).await?;

    let alice = Entity::new()?;
    let bob = Entity::new()?;
    let name_attr = "person/name".parse::<Attribute>()?;

    let claims = vec![
        Relation {
            the: name_attr.clone(),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        },
        Relation {
            the: name_attr.clone(),
            of: bob.clone(),
            is: Value::String("Bob".to_string()),
        },
    ];

    let mut session = Session::open(artifacts.clone());
    session.transact(claims).await?;

    // Create FactApplication that matches all person names
    let fact_app = FactApplication::new(
        Term::Constant(name_attr.clone()),
        Term::var("person"),
        Term::var("name"),
        Term::var("cause"),
        Cardinality::Many,
    );

    // Evaluate with provenance
    let session = Session::open(artifacts);
    let initial_answer = once(async move { Ok(Answer::new()) });
    let answers = fact_app.evaluate_with_provenance(session, initial_answer);

    // Collect results
    let results = Answers::try_vec(answers).await?;

    // Should have two results
    assert_eq!(results.len(), 2);

    // Each result should have its own provenance
    for answer in &results {
        let factors = answer
            .resolve_factors(&Term::<Value>::var("name"))
            .expect("Each answer should have factors for name");

        let evidence: Vec<_> = factors.evidence().collect();
        assert!(!evidence.is_empty(), "Each answer should have evidence");
    }

    Ok(())
}

#[dialog_macros::test]
async fn test_fact_application_query_with_provenance() -> Result<()> {
    // Setup
    let storage_backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(storage_backend).await?;

    let alice = Entity::new()?;
    let name_attr = "person/name".parse::<Attribute>()?;

    let claims = vec![Relation {
        the: name_attr.clone(),
        of: alice.clone(),
        is: Value::String("Alice".to_string()),
    }];

    let mut session = Session::open(artifacts.clone());
    session.transact(claims).await?;

    // Test 1: Query with variables - should return Facts with proper cause
    let fact_app = FactApplication::new(
        Term::Constant(name_attr.clone()),
        Term::var("person"),
        Term::var("name"),
        Term::var("cause"),
        Cardinality::Many,
    );

    let session = Session::open(artifacts.clone());
    let results = fact_app.query(&session).try_vec().await?;

    assert_eq!(results.len(), 1);
    let fact = &results[0];
    assert_eq!(fact.the(), &name_attr);
    assert_eq!(fact.of(), &alice);
    assert_eq!(fact.is(), &Value::String("Alice".to_string()));

    // Test 2: Query with all constants - should still work and track provenance
    let fact_app_constant = FactApplication::new(
        Term::Constant(name_attr.clone()),
        Term::Constant(alice.clone()),
        Term::Constant(Value::String("Alice".to_string())),
        Term::var("cause"),
        Cardinality::Many,
    );

    let session = Session::open(artifacts.clone());
    let results_constant = fact_app_constant.query(&session).try_vec().await?;

    assert_eq!(results_constant.len(), 1);
    let fact_constant = &results_constant[0];
    assert_eq!(fact_constant.the(), &name_attr);
    assert_eq!(fact_constant.of(), &alice);
    assert_eq!(fact_constant.is(), &Value::String("Alice".to_string()));

    // Test 3: Verify both approaches return the same fact (same cause)
    assert_eq!(fact.cause(), fact_constant.cause());

    Ok(())
}

#[dialog_macros::test]
async fn test_query_with_blank_variables() -> Result<()> {
    // Test that blank variables (unnamed) work correctly - they don't get bound
    // but the fact is still tracked
    let storage_backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(storage_backend).await?;

    let alice = Entity::new()?;
    let name_attr = "person/name".parse::<Attribute>()?;

    let facts = vec![Relation {
        the: name_attr.clone(),
        of: alice.clone(),
        is: Value::String("Alice".to_string()),
    }];

    let mut session = Session::open(artifacts.clone());
    session.transact(facts).await?;

    // Query with one blank variable (no name) and one named variable
    let fact_app = FactApplication::new(
        Term::Constant(name_attr.clone()),
        Term::Variable {
            name: None,
            content_type: Default::default(),
        }, // Blank variable
        Term::var("name"),
        Term::var("cause"),
        Cardinality::Many,
    );

    let session = Session::open(artifacts);
    let results = fact_app.query(&session).try_vec().await?;

    assert_eq!(results.len(), 1);
    let fact = &results[0];

    // The fact should still be properly realized
    assert_eq!(fact.the(), &name_attr);
    assert_eq!(fact.of(), &alice);
    assert_eq!(fact.is(), &Value::String("Alice".to_string()));

    Ok(())
}
