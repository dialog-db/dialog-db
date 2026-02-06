//! Test the concept query shortcut syntax: Employee::query(session)

use anyhow::Result;
use dialog_query::artifact::Artifacts;
use dialog_query::{Attribute, Concept, Entity, Match, Session, Term};
use dialog_storage::MemoryStorageBackend;
use futures_util::TryStreamExt;

mod employee {
    use dialog_query::Attribute;

    #[derive(Attribute, Clone)]
    pub struct Name(pub String);

    #[derive(Attribute, Clone)]
    pub struct Job(pub String);
}

#[derive(Concept, Debug, Clone)]
pub struct Employee {
    pub this: Entity,
    pub name: employee::Name,
    pub job: employee::Job,
}

#[dialog_macros::test]
async fn test_concept_query_shortcut() -> Result<()> {
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;
    let mut session = Session::open(store);

    // Insert test data
    let alice = Entity::new()?;
    let bob = Entity::new()?;

    let mut edit = session.edit();
    edit.assert(Employee {
        this: alice.clone(),
        name: employee::Name("Alice".into()),
        job: employee::Job("Engineer".into()),
    })
    .assert(Employee {
        this: bob.clone(),
        name: employee::Name("Bob".into()),
        job: employee::Job("Designer".into()),
    });
    session.commit(edit).await?;

    // Test the shortcut syntax: Employee::query(session)
    let employees_shortcut: Vec<Employee> = Employee::query(session.clone()).try_collect().await?;

    // Test the explicit syntax: Match::<Employee>::default().query(session)
    let employees_explicit: Vec<Employee> = Match::<Employee>::default()
        .query(session.clone())
        .try_collect()
        .await?;

    // Both should return the same results
    assert_eq!(employees_shortcut.len(), 2);
    assert_eq!(employees_explicit.len(), 2);

    // Verify we got both employees
    let mut found_alice = false;
    let mut found_bob = false;

    for emp in &employees_shortcut {
        if emp.name.value() == "Alice" {
            assert_eq!(emp.job.value(), "Engineer");
            found_alice = true;
        } else if emp.name.value() == "Bob" {
            assert_eq!(emp.job.value(), "Designer");
            found_bob = true;
        }
    }

    assert!(found_alice, "Should find Alice");
    assert!(found_bob, "Should find Bob");

    Ok(())
}

#[dialog_macros::test]
async fn test_both_syntaxes_equivalent() -> Result<()> {
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;
    let mut session = Session::open(store);

    // Insert test data
    let alice = Entity::new()?;

    let mut edit = session.edit();
    edit.assert(Employee {
        this: alice.clone(),
        name: employee::Name("Alice".into()),
        job: employee::Job("Engineer".into()),
    });
    session.commit(edit).await?;

    // Shortcut syntax
    let result1: Vec<Employee> = Employee::query(session.clone()).try_collect().await?;

    // Explicit Match syntax
    let result2: Vec<Employee> = Match::<Employee> {
        this: Term::var("this"),
        name: Term::var("name"),
        job: Term::var("job"),
    }
    .query(session.clone())
    .try_collect()
    .await?;

    // Default Match syntax
    let result3: Vec<Employee> = Match::<Employee>::default()
        .query(session.clone())
        .try_collect()
        .await?;

    // All three should be equivalent
    assert_eq!(result1.len(), 1);
    assert_eq!(result2.len(), 1);
    assert_eq!(result3.len(), 1);

    assert_eq!(result1[0].name.value(), result2[0].name.value());
    assert_eq!(result2[0].name.value(), result3[0].name.value());

    Ok(())
}
