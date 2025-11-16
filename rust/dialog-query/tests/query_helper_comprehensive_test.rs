//! Comprehensive test demonstrating the query helper method functionality

use anyhow::Result;
use dialog_query::{
    artifact::{Artifacts, Attribute as ArtifactAttribute, Entity, Value},
    query::Output,
    rule::Match,
    term::Term,
    Attribute, Claim, Concept, Relation, Session,
};
use dialog_storage::MemoryStorageBackend;

mod person {
    use dialog_query::Attribute;

    #[derive(Attribute, Clone, PartialEq)]
    pub struct Name(pub String);
}

mod employee {
    use dialog_query::Attribute;

    #[derive(Attribute, Clone, PartialEq)]
    pub struct Name(pub String);

    #[derive(Attribute, Clone, PartialEq)]
    pub struct Department(pub String);
}

#[derive(Concept, Debug, Clone)]
pub struct Person {
    pub this: Entity,
    pub name: person::Name,
}

#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Employee {
    pub this: Entity,
    pub name: employee::Name,
    pub department: employee::Department,
}

#[tokio::test]
async fn test_single_attribute_query_works() -> Result<()> {
    let storage_backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(storage_backend).await?;

    let alice = Entity::new()?;
    let bob = Entity::new()?;

    let mut session = Session::open(artifacts.clone());
    let mut transaction = session.edit();
    transaction.assert(dialog_query::attribute::With {
        this: alice,
        has: person::Name("Alice".into()),
    });
    transaction.assert(dialog_query::attribute::With {
        this: bob,
        has: person::Name("Bob".into()),
    });
    session.commit(transaction).await?;

    // ✅ This works: Single attribute with constant
    let alice_query = Match::<Person> {
        this: Term::var("person"),
        name: Term::from("Alice".to_string()),
    };

    let session = Session::open(artifacts.clone());
    let results = alice_query.query(session).try_vec().await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name.value(), "Alice");
    println!("✅ Single-attribute constant query: WORKS");

    // ✅ This works: Single attribute with variable
    let all_people_query = Match::<Person> {
        this: Term::var("person"),
        name: Term::var("name"),
    };

    let session = Session::open(artifacts);
    let all_results = all_people_query.query(session).try_vec().await?;
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

    let claims = vec![
        Relation {
            the: "employee/name".parse::<ArtifactAttribute>()?,
            of: alice.clone(),
            is: Value::String("Alice".into()),
        },
        Relation {
            the: "employee/department".parse::<ArtifactAttribute>()?,
            of: alice.clone(),
            is: Value::String("Engineering".into()),
        },
        Relation {
            the: "employee/name".parse::<ArtifactAttribute>()?,
            of: bob.clone(),
            is: Value::String("Bob".into()),
        },
        Relation {
            the: "employee/department".parse::<ArtifactAttribute>()?,
            of: bob.clone(),
            is: Value::String("Sales".into()),
        },
    ];

    let mut session = Session::open(artifacts.clone());
    let mut transaction = session.edit();
    transaction.assert(dialog_query::attribute::With {
        this: alice.clone(),
        has: employee::Name("Alice".into()),
    });
    transaction.assert(dialog_query::attribute::With {
        this: alice.clone(),
        has: employee::Department("Engineering".into()),
    });
    transaction.assert(dialog_query::attribute::With {
        this: bob.clone(),
        has: employee::Name("Bob".into()),
    });
    transaction.assert(dialog_query::attribute::With {
        this: bob,
        has: employee::Department("Sales".into()),
    });
    session.commit(transaction).await?;

    // ✅ This works: Multi-attribute with all constants
    let alice_engineering_query = Match::<Employee> {
        this: Term::var("employee"),
        name: Term::from("Alice".to_string()),
        department: Term::from("Engineering".to_string()),
    };

    let session = Session::open(artifacts);

    let results = alice_engineering_query.query(session).try_vec().await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name.value(), "Alice");
    assert_eq!(results[0].department.value(), "Engineering");
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

    let mut session = Session::open(artifacts.clone());
    let mut transaction = session.edit();
    transaction.assert(dialog_query::attribute::With {
        this: alice.clone(),
        has: employee::Name("Alice".into()),
    });
    transaction.assert(dialog_query::attribute::With {
        this: alice.clone(),
        has: employee::Department("Engineering".into()),
    });
    transaction.assert(dialog_query::attribute::With {
        this: bob.clone(),
        has: employee::Name("Bob".into()),
    });
    transaction.assert(dialog_query::attribute::With {
        this: bob.clone(),
        has: employee::Department("Sales".into()),
    });
    session.commit(transaction).await?;

    let engineering_query = Match::<Employee> {
        this: Term::var("employee"),
        name: Term::var("name"), // Variable to capture
        department: Term::from("Engineering".to_string()), // Constant to filter
    };

    let session = Session::open(artifacts);
    let results = engineering_query.query(session).try_vec().await?;
    assert_eq!(
        results,
        vec![Employee {
            this: alice.clone(),
            name: employee::Name("Alice".into()),
            department: employee::Department("Engineering".into())
        }]
    );

    Ok(())
}
