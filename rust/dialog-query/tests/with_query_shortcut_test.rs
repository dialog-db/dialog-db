//! Test that With<A>::query() shortcut syntax works

use anyhow::Result;
use dialog_query::artifact::Artifacts;
use dialog_query::attribute::With;
use dialog_query::{Attribute, Concept, Entity, Session};
use dialog_storage::MemoryStorageBackend;
use futures_util::TryStreamExt;

mod employee {
    use dialog_query::Attribute;

    #[derive(Attribute, Clone)]
    pub struct Name(pub String);

    #[derive(Attribute, Clone)]
    pub struct Job(pub String);
}

#[tokio::test]
async fn test_with_query_shortcut() -> Result<()> {
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;
    let mut session = Session::open(store);

    // Insert test data
    let alice = Entity::new()?;
    let bob = Entity::new()?;

    let mut edit = session.edit();
    edit.assert(With {
        this: alice.clone(),
        has: employee::Name("Alice".into()),
    })
    .assert(With {
        this: bob.clone(),
        has: employee::Name("Bob".into()),
    })
    .assert(With {
        this: alice.clone(),
        has: employee::Job("Engineer".into()),
    })
    .assert(With {
        this: bob.clone(),
        has: employee::Job("Designer".into()),
    });
    session.commit(edit).await?;

    // Test the shortcut syntax: With::<employee::Name>::query(session)
    let names: Vec<With<employee::Name>> = With::<employee::Name>::query(session.clone())
        .try_collect()
        .await?;

    assert_eq!(names.len(), 2, "Should find 2 names");

    // Verify we got both names
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

    // Test with another attribute type
    let jobs: Vec<With<employee::Job>> = With::<employee::Job>::query(session.clone())
        .try_collect()
        .await?;

    assert_eq!(jobs.len(), 2, "Should find 2 jobs");

    Ok(())
}
