//! Test for the query helper method functionality

use anyhow::Result;
use dialog_query::{
    artifact::{ArtifactStoreMut, Artifacts, Attribute, Entity, Value},
    rule::Rule as RuleTrait,
    session::Changes,
    term::Term,
    Fact, Rule,
};
use dialog_storage::MemoryStorageBackend;
use futures_util::stream;

#[derive(Rule, Debug, Clone)]
pub struct Person {
    /// Name of the person
    pub name: String,
}

#[tokio::test]
async fn test_person_concept_basic() -> Result<()> {
    // Setup: Create in-memory storage and artifacts store
    let storage_backend = MemoryStorageBackend::default();
    let mut artifacts = Artifacts::anonymous(storage_backend).await?;

    // Step 1: Create test data
    let alice = Entity::new()?;
    let bob = Entity::new()?;

    let facts = vec![
        Fact::assert(
            "person/name".parse::<Attribute>()?,
            alice.clone(),
            Value::String("Alice".to_string()),
        ),
        Fact::assert(
            "person/name".parse::<Attribute>()?,
            bob.clone(),
            Value::String("Bob".to_string()),
        ),
    ];

    let instructions = facts.collect_instructions();
    artifacts.commit(stream::iter(instructions)).await?;

    // Step 2: Create match patterns for querying
    let alice_match = PersonMatch {
        this: Term::var("person"),
        name: "Alice".into(),
    };

    // Test the new convenient query method
    match alice_match.query(artifacts).await {
        Ok(people) => {
            assert_eq!(people.len(), 1);
            assert_eq!(people[0].name, "Alice");
        }
        Err(e) => {
            eprintln!("Query failed: {}", e);
            return Err(e.into());
        }
    }

    Ok(())
}
