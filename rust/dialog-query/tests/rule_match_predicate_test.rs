//! Tests for rule Match types implementing Predicate

use dialog_artifacts::{Artifacts, ArtifactStoreMut, Entity, Instruction, Value};
use dialog_query::person_example::PersonMatch;
use dialog_query::predicate::Predicate;
use dialog_query::query::Query;
use dialog_query::syntax::VariableScope;
use dialog_query::{assert, Term};
use dialog_storage::MemoryStorageBackend;
use futures_util::{stream, StreamExt};

#[test]
fn test_person_match_implements_predicate() {
    // Create a PersonMatch instance
    let john = PersonMatch {
        this: Term::var("person"),
        name: Term::from("John"),
        birthday: Term::from(1983u32),
    };

    // Verify we can plan it (proving it implements Predicate)
    let scope = VariableScope::new();
    let plan = john.plan(&scope);
    assert!(plan.is_ok());
}

#[tokio::test]
async fn test_person_match_implements_query() -> Result<(), Box<dyn std::error::Error>> {
    // Setup storage
    let storage = MemoryStorageBackend::<[u8; 32], Vec<u8>>::default();
    let mut artifacts = Artifacts::anonymous(storage).await?;

    // Create test entity
    let john_entity = Entity::new()?;
    let facts: Vec<dialog_query::Claim<Value>> = vec![
        assert(
            "person/name".parse::<dialog_artifacts::Attribute>()?,
            john_entity.clone(),
            Value::String("John".to_string()),
        ),
        assert(
            "person/birthday".parse::<dialog_artifacts::Attribute>()?,
            john_entity.clone(),
            Value::UnsignedInt(1983),
        ),
    ];

    let instructions: Vec<Instruction> = facts.into_iter().map(Instruction::from).collect();
    artifacts.commit(stream::iter(instructions)).await?;

    // Create a PersonMatch with constants
    let john_match = PersonMatch {
        this: Term::from(john_entity.clone()),
        name: Term::from("John"),
        birthday: Term::from(1983u32),
    };

    // Verify we can query it (proving it implements Query)
    let results = john_match.query(&artifacts)?;
    let result_vec: Vec<_> = results.collect::<Vec<_>>().await;
    
    // Should find at least one result (the name fact)
    assert!(!result_vec.is_empty());

    Ok(())
}

#[test]
fn test_rule_trait_bounds() {
    use dialog_query::rule::Rule;

    // This test verifies the Rule trait has the correct bounds
    fn assert_rule_match_is_predicate<R: Rule>()
    where
        R::Match: Predicate,
    {
        // This function compiles only if Rule::Match implements Predicate
    }

    // If this compiles, our trait bounds are correct
    assert_rule_match_is_predicate::<dialog_query::rule::DerivedRule>();
}

#[test]
fn test_match_pattern_api() {
    // Test the intended API patterns from the design doc

    // Pattern 1: Variable match
    let person_var = PersonMatch {
        this: Term::var("person"),
        name: Term::from("John"),
        birthday: Term::from(1983u32),
    };

    // Pattern 2: Concrete entity match
    let entity = Entity::new().unwrap();
    let person_concrete = PersonMatch {
        this: Term::from(entity),
        name: Term::from("Alice"),
        birthday: Term::from(1990u32),
    };

    // Pattern 3: Fully variable match (find all people)
    let person_any = PersonMatch {
        this: Term::var("person"),
        name: Term::var("name"),
        birthday: Term::var("birthday"),
    };

    // All patterns should be plannable
    let scope = VariableScope::new();
    assert!(person_var.plan(&scope).is_ok());
    assert!(person_concrete.plan(&scope).is_ok());
    assert!(person_any.plan(&scope).is_ok());
}

#[test]
fn test_join_plan_creation() {
    // Test that we can create join plans for rule matches
    let entity = Term::var("person");
    let attributes = vec![
        ("person/name".to_string(), Term::from(Value::String("John".to_string()))),
        ("person/birthday".to_string(), Term::from(Value::UnsignedInt(1983))),
    ];

    let join_plan = dialog_query::join::create_attribute_join(entity, attributes);
    
    // Should have 2 plans to join
    assert_eq!(join_plan.plans.len(), 2);
    // Should join on "this" variable by default
    assert_eq!(join_plan.join_variables, vec!["this"]);
}