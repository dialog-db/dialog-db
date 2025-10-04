//! Comprehensive test demonstrating the query helper method functionality

use anyhow::Result;
use dialog_query::{
    artifact::{Artifacts, Attribute, Entity, Value},
    rule::Match,
    term::Term,
    Fact, Rule, Session,
};
use dialog_storage::MemoryStorageBackend;

#[derive(Rule, Debug, Clone)]
pub struct Person {
    pub this: Entity,
    pub name: String,
}

#[derive(Rule, Debug, Clone, PartialEq)]
pub struct Employee {
    pub this: Entity,
    pub name: String,
    pub department: String,
}

#[tokio::test]
async fn test_single_attribute_query_works() -> Result<()> {
    let storage_backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(storage_backend).await?;

    let alice = Entity::new()?;
    let bob = Entity::new()?;

    let facts = vec![
        Fact::assert(
            "person/name".parse::<Attribute>()?,
            alice.clone(),
            Value::String("Alice".into()),
        ),
        Fact::assert(
            "person/name".parse::<Attribute>()?,
            bob.clone(),
            Value::String("Bob".into()),
        ),
    ];

    let mut session = Session::open(artifacts.clone());
    session.transact(facts).await?;

    // ✅ This works: Single attribute with constant
    let alice_query = PersonMatch {
        this: Term::var("person"),
        name: "Alice".into(),
    };

    let session = Session::open(artifacts.clone());
    let results = alice_query.query(session).await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "Alice");
    println!("✅ Single-attribute constant query: WORKS");

    // ✅ This works: Single attribute with variable
    let all_people_query = PersonMatch {
        this: Term::var("person"),
        name: Term::var("name"),
    };

    let session = Session::open(artifacts);
    let all_results = all_people_query.query(session).await?;
    assert_eq!(all_results.len(), 2);
    println!("✅ Single-attribute variable query: WORKS");

    Ok(())
}

#[tokio::test]
async fn test_multi_attribute_constant_query_works() -> Result<()> {
    let storage_backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(storage_backend).await?;

    let alice = Entity::new()?;
    let bob = Entity::new()?;

    let facts = vec![
        Fact::assert(
            "employee/name".parse::<Attribute>()?,
            alice.clone(),
            Value::String("Alice".into()),
        ),
        Fact::assert(
            "employee/department".parse::<Attribute>()?,
            alice.clone(),
            Value::String("Engineering".into()),
        ),
        Fact::assert(
            "employee/name".parse::<Attribute>()?,
            bob.clone(),
            Value::String("Bob".into()),
        ),
        Fact::assert(
            "employee/department".parse::<Attribute>()?,
            bob.clone(),
            Value::String("Sales".into()),
        ),
    ];

    let mut session = Session::open(artifacts.clone());
    session.transact(facts).await?;

    // ✅ This works: Multi-attribute with all constants
    let alice_engineering_query = Match::<Employee> {
        this: Term::var("employee"),
        name: "Alice".into(),
        department: "Engineering".into(),
    };

    let session = Session::open(artifacts);
    let results = alice_engineering_query.query(session).await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "Alice");
    assert_eq!(results[0].department, "Engineering");
    // Note: We don't compare the full struct because we don't know the entity ID in advance
    assert_eq!(results[0].this, alice); // Verify it's Alice's entity
    println!("✅ Multi-attribute constant query: WORKS");

    Ok(())
}

#[tokio::test]
async fn test_multi_attribute_variable_query_limitation() -> Result<()> {
    let storage_backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(storage_backend).await?;

    let alice = Entity::new()?;
    let bob = Entity::new()?;

    let facts = vec![
        Fact::assert(
            "employee/name".parse::<Attribute>()?,
            alice.clone(),
            Value::String("Alice".into()),
        ),
        Fact::assert(
            "employee/department".parse::<Attribute>()?,
            alice.clone(),
            Value::String("Engineering".into()),
        ),
        Fact::assert(
            "employee/name".parse::<Attribute>()?,
            bob.clone(),
            Value::String("Bob".into()),
        ),
        Fact::assert(
            "employee/department".parse::<Attribute>()?,
            bob.clone(),
            Value::String("Sales".into()),
        ),
    ];

    let mut session = Session::open(artifacts.clone());
    session.transact(facts).await?;

    // ⚠️ This has limitations: Multi-attribute with mixed constants and variables
    let engineering_query = EmployeeMatch {
        this: Term::var("employee"),
        name: Term::var("name"),          // Variable to capture
        department: "Engineering".into(), // Constant to filter
    };

    let session = Session::open(artifacts);
    match engineering_query.query(session).await {
        Ok(results) => {
            // Currently this might return more results than expected
            // because we only execute the first plan (probably the name plan)
            // instead of properly joining all plans
            println!(
                "⚠️  Multi-attribute variable query returned {} results",
                results.len()
            );
            println!("⚠️  This demonstrates the current limitation in plan joining");
        }
        Err(e) => {
            println!("❌ Multi-attribute variable query failed: {}", e);
        }
    }

    Ok(())
}
