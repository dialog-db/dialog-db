//! Test for the query helper method functionality

use anyhow::Result;
use dialog_query::{
    artifact::{Artifacts, Attribute, Entity, Value},
    term::Term,
    Concept, Match, Relation, Session,
};
use dialog_storage::MemoryStorageBackend;

/// Helper function to commit claims using the transaction-based API

#[derive(Concept, Debug, Clone)]
pub struct Person {
    /// The entity this person represents
    pub this: Entity,
    /// Name of the person
    pub name: String,
}

#[tokio::test]
async fn test_person_concept_basic() -> Result<()> {
    // Setup: Create in-memory storage and artifacts store
    let storage_backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(storage_backend).await?;

    // Step 1: Create test data
    let alice = Entity::new()?;
    let bob = Entity::new()?;

    let claims = vec![
        Relation {
            the: "person/name".parse::<Attribute>()?,
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        },
        Relation {
            the: "person/name".parse::<Attribute>()?,
            of: bob.clone(),
            is: Value::String("Bob".to_string()),
        },
    ];

    let mut session = Session::open(artifacts.clone());
    session.transact(claims).await?;

    // Step 2: Create match patterns for querying
    let alice_match = Match::<Person> {
        this: Term::var("person"),
        name: "Alice".into(),
    };

    // Test the new convenient query method
    let session = Session::open(artifacts);

    use dialog_query::query::Output;
    let people = alice_match.query(session).try_vec().await?;

    assert_eq!(people.len(), 1);
    assert_eq!(people[0].name, "Alice");

    Ok(())
}
