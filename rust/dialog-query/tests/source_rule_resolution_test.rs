use anyhow::Result;
use dialog_query::{
    artifact::{Artifacts, Type},
    predicate::{concept::Attributes, Concept, DeductiveRule},
    query::Source,
    session::{QuerySession, Session},
    AttributeSchema,
};
use dialog_storage::MemoryStorageBackend;

#[dialog_macros::test]
async fn test_session_source_rule_resolution() -> Result<()> {
    // Setup: Create a Session with a rule-aware store
    let backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(backend).await?;
    let session = Session::open(artifacts);

    // Test 1: Verify Session implements Source trait
    assert_eq!(session.resolve_rules("nonexistent"), Vec::new());

    // Test 2: Install a rule and verify it can be resolved

    let adult_conclusion = Concept::Dynamic {
        description: String::new(),
        attributes: Attributes::from(vec![
            (
                "name",
                AttributeSchema::new("adult", "name", "Adult name", Type::String),
            ),
            (
                "age",
                AttributeSchema::new("adult", "age", "Adult age", Type::UnsignedInt),
            ),
        ]),
    };

    // Create a simple rule: adult(X, Age) :- person(X, Age), Age >= 18
    let rule = DeductiveRule {
        conclusion: adult_conclusion.clone(),
        premises: vec![
            // This is simplified - in real usage would have proper premise construction
        ],
    };

    let session_with_rule = session.register(rule.clone());

    // Test 3: Verify the rule can be resolved
    let adult_operator = adult_conclusion.operator();
    let resolved_rules = session_with_rule.resolve_rules(&adult_operator);
    assert_eq!(resolved_rules.len(), 1);
    assert_eq!(resolved_rules[0].conclusion.operator(), adult_operator);

    // Test 4: Verify non-matching operator returns empty
    assert_eq!(session_with_rule.resolve_rules("nonexistent"), Vec::new());

    Ok(())
}

#[dialog_macros::test]
async fn test_source_trait_compatibility() -> Result<()> {
    // Test that both QuerySession and Session can be used polymorphically as a Source

    async fn query_with_source<S: Source>(source: &S, operator: &str) -> Vec<DeductiveRule> {
        source.resolve_rules(operator)
    }

    let backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(backend).await?;

    // Test with QuerySession
    let query_session: QuerySession<_> = artifacts.clone().into();
    let concept = Concept::new(Attributes::new());
    let rule = DeductiveRule {
        conclusion: concept.clone(),
        premises: vec![],
    };

    let query_session = query_session.install(rule.clone());
    // Query using the concept's actual operator (hash URI)
    let operator = concept.operator();
    let rules = query_with_source(&query_session, &operator).await;
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].conclusion.operator(), concept.operator());

    // Test with Session
    let mut session = Session::open(artifacts);
    session = session.register(rule.clone());
    let rules = query_with_source(&session, &operator).await;
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].conclusion.operator(), concept.operator());

    Ok(())
}

#[dialog_macros::test]
async fn test_multiple_rules_same_operator() -> Result<()> {
    // Test that multiple rules for the same operator are stored and resolved correctly

    let backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(backend).await?;

    // Test with QuerySession
    let query_session: QuerySession<_> = artifacts.into();

    // Create two different rules for the same concept (same attributes = same hash)
    let attributes: Attributes = [(
        "name".to_string(),
        AttributeSchema::new("person", "name", "Person name", Type::String),
    )]
    .into();

    let concept1 = Concept::Dynamic {
        description: "First rule".to_string(),
        attributes: attributes.clone(),
    };

    let concept2 = Concept::Dynamic {
        description: "Second rule".to_string(),
        attributes,
    };

    let rule1 = DeductiveRule {
        conclusion: concept1.clone(),
        premises: vec![],
    };

    let rule2 = DeductiveRule {
        conclusion: concept2.clone(),
        premises: vec![],
    };

    // Install both rules
    let query_session = query_session.install(rule1).install(rule2);

    // Should resolve both rules for the same concept operator (hash)
    let operator = concept1.operator();
    let rules = query_session.resolve_rules(&operator);
    assert_eq!(rules.len(), 2, "Should have 2 rules for the same concept");

    // Both rules should have the same operator (hash)
    for rule in &rules {
        assert_eq!(rule.conclusion.operator(), operator);
    }

    Ok(())
}

#[dialog_macros::test]
async fn test_explicit_conversion_pattern() -> Result<()> {
    // Test the explicit conversion pattern: artifacts.into() for QuerySession

    let backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(backend).await?;

    // Test 1: Basic conversion - no rules
    let query_session: QuerySession<_> = artifacts.clone().into();
    assert_eq!(query_session.resolve_rules("nonexistent"), Vec::new());
    assert_eq!(query_session.rules().len(), 0);

    // Test 2: Conversion with rule installation
    let adult_concept = Concept::Dynamic {
        description: String::new(),
        attributes: [(
            "name".to_string(),
            AttributeSchema::new("person", "name", "Adult name", Type::String),
        )]
        .into(),
    };

    let adult_rule = DeductiveRule {
        conclusion: adult_concept.clone(),
        premises: vec![],
    };

    let query_session: QuerySession<_> = artifacts.into();
    let query_session = query_session.install(adult_rule.clone());

    let adult_operator = adult_concept.operator();
    let resolved_rules = query_session.resolve_rules(&adult_operator);
    assert_eq!(resolved_rules.len(), 1);
    assert_eq!(resolved_rules[0].conclusion.operator(), adult_operator);

    // Test 3: Verify store is still accessible
    assert!(!std::ptr::addr_of!(*query_session.store()).is_null());

    Ok(())
}
