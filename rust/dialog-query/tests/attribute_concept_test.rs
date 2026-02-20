use dialog_query::artifact::{Artifacts, Value};
use dialog_query::{Attribute, Concept, Entity, Fact, Match, Session, Term};
use dialog_storage::MemoryStorageBackend;
use futures_util::TryStreamExt;

/// Define attributes in a module for proper namespacing
mod person {
    use super::*;

    /// Name of the person
    #[derive(Attribute, Clone, PartialEq)]
    pub struct Name(pub String);

    /// Birthday of the person (Unix timestamp)
    #[derive(Attribute, Clone, PartialEq)]
    pub struct Birthday(pub u32);

    /// Email address of the person
    #[derive(Attribute, Clone, PartialEq)]
    pub struct Email(pub String);
}

/// Define a Concept using Attribute-implementing fields
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Person {
    pub this: Entity,
    pub name: person::Name,
    pub birthday: person::Birthday,
}

/// Another Concept that shares the Name attribute
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct PersonWithEmail {
    pub this: Entity,
    pub name: person::Name,
    pub email: person::Email,
}

#[tokio::test]
async fn test_concept_with_attribute_fields() -> anyhow::Result<()> {
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;

    let alice_id = Entity::new()?;

    // Create a Person instance
    let alice = Person {
        this: alice_id.clone(),
        name: person::Name("Alice".to_string()),
        birthday: person::Birthday(19830703),
    };

    // Assert the person
    let mut session = Session::open(store.clone());
    session.transact(vec![alice.clone()]).await?;

    // Query to verify facts were stored with correct namespaces
    let name_query = Fact::<Value>::select()
        .the("person/name")
        .of(alice_id.clone())
        .compile()?;

    let birthday_query = Fact::<Value>::select()
        .the("person/birthday")
        .of(alice_id.clone())
        .compile()?;

    let name_facts: Vec<_> = name_query
        .query(&Session::open(store.clone()))
        .try_collect()
        .await?;

    let birthday_facts: Vec<_> = birthday_query
        .query(&Session::open(store))
        .try_collect()
        .await?;

    assert_eq!(name_facts.len(), 1);
    assert_eq!(birthday_facts.len(), 1);

    match &name_facts[0] {
        Fact::Assertion { is, .. } => {
            assert_eq!(*is, Value::String("Alice".to_string()));
        }
        _ => panic!("Expected Assertion"),
    }

    match &birthday_facts[0] {
        Fact::Assertion { is, .. } => {
            assert_eq!(*is, Value::UnsignedInt(19830703));
        }
        _ => panic!("Expected Assertion"),
    }

    Ok(())
}

#[tokio::test]
async fn test_query_concept_with_attribute_fields() -> anyhow::Result<()> {
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;

    // Create two people
    let alice_id = Entity::new()?;
    let bob_id = Entity::new()?;

    let alice = Person {
        this: alice_id.clone(),
        name: person::Name("Alice".to_string()),
        birthday: person::Birthday(19830703),
    };

    let bob = Person {
        this: bob_id.clone(),
        name: person::Name("Bob".to_string()),
        birthday: person::Birthday(19900515),
    };

    // Store both people
    let mut session = Session::open(store.clone());
    session.transact(vec![alice, bob]).await?;

    // Query for all people
    let query = Match::<Person> {
        this: Term::var("person"),
        name: Term::var("name"),
        birthday: Term::var("birthday"),
    };

    let results: Vec<Person> = query.query(Session::open(store)).try_collect().await?;

    assert_eq!(results.len(), 2);

    // Verify we got both people
    let alice_result = results.iter().find(|p| p.name.value() == "Alice");
    let bob_result = results.iter().find(|p| p.name.value() == "Bob");

    assert!(alice_result.is_some());
    assert!(bob_result.is_some());

    assert_eq!(alice_result.unwrap().birthday.value(), &19830703u32);
    assert_eq!(bob_result.unwrap().birthday.value(), &19900515u32);

    Ok(())
}

#[tokio::test]
async fn test_concept_with_constant_term() -> anyhow::Result<()> {
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;

    // Create two people
    let alice_id = Entity::new()?;
    let bob_id = Entity::new()?;

    let alice = Person {
        this: alice_id.clone(),
        name: person::Name("Alice".to_string()),
        birthday: person::Birthday(19830703),
    };

    let bob = Person {
        this: bob_id.clone(),
        name: person::Name("Bob".to_string()),
        birthday: person::Birthday(19900515),
    };

    let mut session = Session::open(store.clone());
    session.transact(vec![alice, bob]).await?;

    // Query for person with specific name
    let query = Match::<Person> {
        this: Term::var("person"),
        name: Term::from("Alice"),
        birthday: Term::var("birthday"),
    };

    let results: Vec<Person> = query.query(Session::open(store)).try_collect().await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name.value(), "Alice");
    assert_eq!(results[0].birthday.value(), &19830703u32);

    Ok(())
}

#[tokio::test]
async fn test_attribute_reuse_across_concepts() -> anyhow::Result<()> {
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;

    let alice_id = Entity::new()?;

    // Same person, stored via different concept
    let alice_with_email = PersonWithEmail {
        this: alice_id.clone(),
        name: person::Name("Alice".to_string()),
        email: person::Email("alice@example.com".to_string()),
    };

    let mut session = Session::open(store.clone());
    session.transact(vec![alice_with_email]).await?;

    // Now add birthday via Person concept
    let alice_with_birthday = Person {
        this: alice_id.clone(),
        name: person::Name("Alice".to_string()),
        birthday: person::Birthday(19830703),
    };

    let mut session = Session::open(store.clone());
    session.transact(vec![alice_with_birthday]).await?;

    // Verify all three attributes are stored
    let name_query = Fact::<Value>::select()
        .the("person/name")
        .of(alice_id.clone())
        .compile()?;

    let email_query = Fact::<Value>::select()
        .the("person/email")
        .of(alice_id.clone())
        .compile()?;

    let birthday_query = Fact::<Value>::select()
        .the("person/birthday")
        .of(alice_id.clone())
        .compile()?;

    let name_facts: Vec<_> = name_query
        .query(&Session::open(store.clone()))
        .try_collect()
        .await?;

    let email_facts: Vec<_> = email_query
        .query(&Session::open(store.clone()))
        .try_collect()
        .await?;

    let birthday_facts: Vec<_> = birthday_query
        .query(&Session::open(store))
        .try_collect()
        .await?;

    assert_eq!(name_facts.len(), 1);
    assert_eq!(email_facts.len(), 1);
    assert_eq!(birthday_facts.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_retract_concept_with_attributes() -> anyhow::Result<()> {
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;

    let alice_id = Entity::new()?;

    let alice = Person {
        this: alice_id.clone(),
        name: person::Name("Alice".to_string()),
        birthday: person::Birthday(19830703),
    };

    // Assert
    let mut session = Session::open(store.clone());
    session.transact(vec![alice.clone()]).await?;

    // Retract using !alice syntax
    let mut session = Session::open(store.clone());
    session.transact(vec![!alice]).await?;

    // Verify facts were retracted
    let name_query = Fact::<Value>::select()
        .the("person/name")
        .of(alice_id.clone())
        .compile()?;

    let birthday_query = Fact::<Value>::select()
        .the("person/birthday")
        .of(alice_id)
        .compile()?;

    let name_facts: Vec<_> = name_query
        .query(&Session::open(store.clone()))
        .try_collect()
        .await?;

    let birthday_facts: Vec<_> = birthday_query
        .query(&Session::open(store))
        .try_collect()
        .await?;

    assert_eq!(name_facts.len(), 0);
    assert_eq!(birthday_facts.len(), 0);

    Ok(())
}
