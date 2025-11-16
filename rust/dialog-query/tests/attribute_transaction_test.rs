use dialog_query::artifact::{Artifacts, Value};
use dialog_query::{Attribute, Claim, Entity, Fact, Session};
use dialog_storage::MemoryStorageBackend;
use futures_util::TryStreamExt;

/// Employee attributes using Bevy-like derive API
mod employee {
    use super::*;

    /// Name of the employee
    #[derive(Attribute, Clone)]
    pub struct Name(pub String);

    /// Job title of the employee
    #[derive(Attribute, Clone)]
    pub struct Job(pub String);

    /// Salary of the employee
    #[derive(Attribute, Clone)]
    pub struct Salary(pub u32);

    /// Employee's manager
    #[derive(Attribute, Clone)]
    pub struct Manager(pub Entity);
}

#[tokio::test]
async fn test_single_attribute_assert_and_retract() -> anyhow::Result<()> {
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;

    let alice = Entity::new()?;
    let name = employee::Name("Alice".to_string());

    // Assert using With instance
    let mut session = Session::open(store.clone());
    session
        .transact(vec![dialog_query::attribute::With {
            this: alice.clone(),
            has: name.clone(),
        }])
        .await?;

    // Query to verify the fact was asserted
    let query = Fact::<Value>::select()
        .the("employee/name")
        .of(alice.clone())
        .compile()?;

    let facts: Vec<_> = query
        .query(&Session::open(store.clone()))
        .try_collect()
        .await?;

    assert_eq!(facts.len(), 1);
    match &facts[0] {
        Fact::Assertion { is, .. } => {
            assert_eq!(*is, Value::String("Alice".to_string()));
        }
        _ => panic!("Expected Assertion"),
    }

    // Retract using the revert pattern
    let mut session = Session::open(store.clone());
    session
        .transact(vec![!dialog_query::attribute::With {
            this: alice.clone(),
            has: name,
        }])
        .await?;

    // Verify the fact was retracted
    let query = Fact::<Value>::select()
        .the("employee/name")
        .of(alice)
        .compile()?;

    let facts: Vec<_> = query
        .query(&Session::open(store))
        .try_collect()
        .await?;

    assert_eq!(facts.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_multiple_attributes_assert() -> anyhow::Result<()> {
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;

    let bob = Entity::new()?;
    let name = employee::Name("Bob".to_string());
    let job = employee::Job("Engineer".to_string());

    // Assert multiple attributes separately
    let mut session = Session::open(store.clone());
    session
        .transact(vec![dialog_query::attribute::With {
            this: bob.clone(),
            has: name,
        }])
        .await?;
    session
        .transact(vec![dialog_query::attribute::With {
            this: bob.clone(),
            has: job,
        }])
        .await?;

    // Verify both facts were asserted
    let name_query = Fact::<Value>::select()
        .the("employee/name")
        .of(bob.clone())
        .compile()?;

    let job_query = Fact::<Value>::select()
        .the("employee/job")
        .of(bob.clone())
        .compile()?;

    let name_facts: Vec<_> = name_query
        .query(&Session::open(store.clone()))
        .try_collect()
        .await?;

    let job_facts: Vec<_> = job_query
        .query(&Session::open(store))
        .try_collect()
        .await?;

    assert_eq!(name_facts.len(), 1);
    match &name_facts[0] {
        Fact::Assertion { is, .. } => {
            assert_eq!(*is, Value::String("Bob".to_string()));
        }
        _ => panic!("Expected Assertion"),
    }

    assert_eq!(job_facts.len(), 1);
    match &job_facts[0] {
        Fact::Assertion { is, .. } => {
            assert_eq!(*is, Value::String("Engineer".to_string()));
        }
        _ => panic!("Expected Assertion"),
    }

    Ok(())
}

#[tokio::test]
async fn test_three_attributes_assert() -> anyhow::Result<()> {
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;

    let charlie = Entity::new()?;
    let name = employee::Name("Charlie".to_string());
    let job = employee::Job("Manager".to_string());
    let salary = employee::Salary(120000);

    // Assert three attributes separately
    let mut session = Session::open(store.clone());
    session
        .transact(vec![dialog_query::attribute::With {
            this: charlie.clone(),
            has: name,
        }])
        .await?;
    session
        .transact(vec![dialog_query::attribute::With {
            this: charlie.clone(),
            has: job,
        }])
        .await?;
    session
        .transact(vec![dialog_query::attribute::With {
            this: charlie.clone(),
            has: salary,
        }])
        .await?;

    // Verify all three facts were asserted
    let name_query = Fact::<Value>::select()
        .the("employee/name")
        .of(charlie.clone())
        .compile()?;

    let job_query = Fact::<Value>::select()
        .the("employee/job")
        .of(charlie.clone())
        .compile()?;

    let salary_query = Fact::<Value>::select()
        .the("employee/salary")
        .of(charlie.clone())
        .compile()?;

    let name_facts: Vec<_> = name_query
        .query(&Session::open(store.clone()))
        .try_collect()
        .await?;

    let job_facts: Vec<_> = job_query
        .query(&Session::open(store.clone()))
        .try_collect()
        .await?;

    let salary_facts: Vec<_> = salary_query
        .query(&Session::open(store))
        .try_collect()
        .await?;

    assert_eq!(name_facts.len(), 1);
    match &name_facts[0] {
        Fact::Assertion { is, .. } => {
            assert_eq!(*is, Value::String("Charlie".to_string()));
        }
        _ => panic!("Expected Assertion"),
    }

    assert_eq!(job_facts.len(), 1);
    match &job_facts[0] {
        Fact::Assertion { is, .. } => {
            assert_eq!(*is, Value::String("Manager".to_string()));
        }
        _ => panic!("Expected Assertion"),
    }

    assert_eq!(salary_facts.len(), 1);
    match &salary_facts[0] {
        Fact::Assertion { is, .. } => {
            assert_eq!(*is, Value::UnsignedInt(120000));
        }
        _ => panic!("Expected Assertion"),
    }

    Ok(())
}

#[tokio::test]
async fn test_multiple_attributes_retract() -> anyhow::Result<()> {
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;

    let dave = Entity::new()?;
    let name = employee::Name("Dave".to_string());
    let job = employee::Job("Developer".to_string());

    // Assert
    let mut session = Session::open(store.clone());
    session
        .transact(vec![dialog_query::attribute::With {
            this: dave.clone(),
            has: name.clone(),
        }])
        .await?;
    session
        .transact(vec![dialog_query::attribute::With {
            this: dave.clone(),
            has: job.clone(),
        }])
        .await?;

    // Retract both attributes
    let mut session = Session::open(store.clone());
    session
        .transact(vec![!dialog_query::attribute::With {
            this: dave.clone(),
            has: name,
        }])
        .await?;
    session
        .transact(vec![!dialog_query::attribute::With {
            this: dave.clone(),
            has: job,
        }])
        .await?;

    // Verify both facts were retracted
    let name_query = Fact::<Value>::select()
        .the("employee/name")
        .of(dave.clone())
        .compile()?;

    let job_query = Fact::<Value>::select()
        .the("employee/job")
        .of(dave.clone())
        .compile()?;

    let name_facts: Vec<_> = name_query
        .query(&Session::open(store.clone()))
        .try_collect()
        .await?;

    let job_facts: Vec<_> = job_query
        .query(&Session::open(store))
        .try_collect()
        .await?;

    assert_eq!(name_facts.len(), 0);
    assert_eq!(job_facts.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_update_attribute() -> anyhow::Result<()> {
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;

    let eve = Entity::new()?;
    let old_job = employee::Job("Junior Developer".to_string());

    // Assert initial job
    let mut session = Session::open(store.clone());
    session
        .transact(vec![dialog_query::attribute::With {
            this: eve.clone(),
            has: old_job.clone(),
        }])
        .await?;

    // Update job (retract old in one transaction, assert new in another)
    let mut session = Session::open(store.clone());
    session
        .transact(vec![!dialog_query::attribute::With {
            this: eve.clone(),
            has: old_job,
        }])
        .await?;

    let new_job = employee::Job("Senior Developer".to_string());
    let mut session = Session::open(store.clone());
    session.transact(vec![dialog_query::attribute::With {
        this: eve.clone(),
        has: new_job,
    }]).await?;

    // Verify the job was updated
    let query = Fact::<Value>::select()
        .the("employee/job")
        .of(eve)
        .compile()?;

    let job_facts: Vec<_> = query
        .query(&Session::open(store))
        .try_collect()
        .await?;

    assert_eq!(job_facts.len(), 1);
    match &job_facts[0] {
        Fact::Assertion { is, .. } => {
            assert_eq!(*is, Value::String("Senior Developer".to_string()));
        }
        _ => panic!("Expected Assertion"),
    }

    Ok(())
}

#[tokio::test]
async fn test_entity_reference_attribute() -> anyhow::Result<()> {
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;

    let manager = Entity::new()?;
    let employee_entity = Entity::new()?;

    let manager_name = employee::Name("Manager Alice".to_string());
    let employee_name = employee::Name("Employee Bob".to_string());
    let reports_to = employee::Manager(manager.clone());

    // Assert entities in separate transactions
    let mut session = Session::open(store.clone());
    session
        .transact(vec![dialog_query::attribute::With {
            this: manager.clone(),
            has: manager_name,
        }])
        .await?;

    let mut session = Session::open(store.clone());
    session
        .transact(vec![dialog_query::attribute::With {
            this: employee_entity.clone(),
            has: employee_name,
        }])
        .await?;
    session
        .transact(vec![dialog_query::attribute::With {
            this: employee_entity.clone(),
            has: reports_to,
        }])
        .await?;

    // Verify the manager relationship
    let query = Fact::<Value>::select()
        .the("employee/manager")
        .of(employee_entity)
        .compile()?;

    let manager_facts: Vec<_> = query
        .query(&Session::open(store))
        .try_collect()
        .await?;

    assert_eq!(manager_facts.len(), 1);
    match &manager_facts[0] {
        Fact::Assertion { is, .. } => {
            assert_eq!(*is, Value::Entity(manager));
        }
        _ => panic!("Expected Assertion"),
    }

    Ok(())
}
