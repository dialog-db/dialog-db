use std::collections::HashMap;
use dialog_query::{
    predicate::{Concept, DeductiveRule},
    query::Source,
    session::{QuerySession, Session},
    artifact::{Artifacts, ValueDataType},
    Attribute,
};
use dialog_storage::MemoryStorageBackend;
use anyhow::Result;


#[tokio::test]
async fn test_session_source_rule_resolution() -> Result<()> {
    // Setup: Create a Session with a rule-aware store
    let backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(backend).await?;
    let session = Session::open(artifacts);

    // Test 1: Verify Session implements Source trait
    assert_eq!(session.resolve_rules("nonexistent"), Vec::new());

    // Test 2: Install a rule and verify it can be resolved
    let mut attributes = HashMap::new();
    attributes.insert(
        "name".into(),
        Attribute::new("adult", "name", "Adult name", ValueDataType::String),
    );
    attributes.insert(
        "age".into(),  
        Attribute::new("adult", "age", "Adult age", ValueDataType::UnsignedInt),
    );

    let adult_conclusion = Concept {
        operator: "adult".into(),
        attributes: attributes.clone(),
    };

    // Create a simple rule: adult(X, Age) :- person(X, Age), Age >= 18
    let rule = DeductiveRule {
        conclusion: adult_conclusion.clone(),
        premises: vec![
            // This is simplified - in real usage would have proper premise construction
        ],
    };

    let session_with_rule = session.install(rule.clone());

    // Test 3: Verify the rule can be resolved
    let resolved_rules = session_with_rule.resolve_rules("adult");
    assert_eq!(resolved_rules.len(), 1);
    assert_eq!(resolved_rules[0].conclusion.operator, "adult");

    // Test 4: Verify non-matching operator returns empty
    assert_eq!(session_with_rule.resolve_rules("person"), Vec::new());

    Ok(())
}

#[tokio::test]
async fn test_source_trait_compatibility() -> Result<()> {
    // Test that both QuerySession and Session can be used polymorphically as a Source
    
    async fn query_with_source<S: Source>(source: &S, operator: &str) -> Vec<DeductiveRule> {
        source.resolve_rules(operator)
    }

    let backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(backend).await?;

    // Test with QuerySession
    let query_session: QuerySession<_> = artifacts.clone().into();
    let concept = Concept::new("test".into());
    let rule = DeductiveRule {
        conclusion: concept,
        premises: vec![],
    };
    
    let query_session = query_session.install(rule.clone());
    let rules = query_with_source(&query_session, "test").await;
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].conclusion.operator, "test");

    // Test with Session  
    let mut session = Session::open(artifacts);
    session = session.install(rule.clone());
    let rules = query_with_source(&session, "test").await;
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].conclusion.operator, "test");

    Ok(())
}

#[tokio::test]
async fn test_multiple_rules_same_operator() -> Result<()> {
    // Test that multiple rules for the same operator are stored and resolved correctly
    
    let backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(backend).await?;
    
    // Test with QuerySession
    let query_session: QuerySession<_> = artifacts.into();

    // Create two different rules for the same concept
    let concept1 = Concept::new("person".into())
        .with("name", Attribute::new("person", "name", "Person name", ValueDataType::String));
        
    let concept2 = Concept::new("person".into())
        .with("age", Attribute::new("person", "age", "Person age", ValueDataType::UnsignedInt));

    let rule1 = DeductiveRule {
        conclusion: concept1,
        premises: vec![],
    };
    
    let rule2 = DeductiveRule {
        conclusion: concept2,
        premises: vec![],
    };

    // Install both rules
    let query_session = query_session.install(rule1).install(rule2);

    // Should resolve both rules for "person"
    let rules = query_session.resolve_rules("person");
    assert_eq!(rules.len(), 2);
    
    // Both rules should have the same operator but different attributes
    for rule in &rules {
        assert_eq!(rule.conclusion.operator, "person");
    }

    Ok(())
}

#[tokio::test]
async fn test_explicit_conversion_pattern() -> Result<()> {
    // Test the explicit conversion pattern: artifacts.into() for QuerySession
    
    let backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(backend).await?;

    // Test 1: Basic conversion - no rules
    let query_session: QuerySession<_> = artifacts.clone().into();
    assert_eq!(query_session.resolve_rules("nonexistent"), Vec::new());
    assert_eq!(query_session.rules().len(), 0);

    // Test 2: Conversion with rule installation
    let adult_concept = Concept::new("adult".into())
        .with("name", Attribute::new("person", "name", "Adult name", ValueDataType::String));
    
    let adult_rule = DeductiveRule {
        conclusion: adult_concept,
        premises: vec![],
    };

    let query_session: QuerySession<_> = artifacts.into();
    let query_session = query_session.install(adult_rule.clone());
    
    let resolved_rules = query_session.resolve_rules("adult");
    assert_eq!(resolved_rules.len(), 1);
    assert_eq!(resolved_rules[0].conclusion.operator, "adult");

    // Test 3: Verify store is still accessible
    assert!(std::ptr::addr_of!(*query_session.store()) != std::ptr::null());

    Ok(())
}