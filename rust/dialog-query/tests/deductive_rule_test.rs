use dialog_query::analyzer::Analysis;
use dialog_query::application::fact::BASE_COST;
use dialog_query::application::{ConceptApplication, PlanCandidate};
use dialog_query::artifact::ValueDataType;
use dialog_query::attribute::Attribute;
use dialog_query::error::{AnalyzerError, PlanError, QueryError};
use dialog_query::predicate::{Concept, DeductiveRule, FactSelector};
use dialog_query::term::Term;
use dialog_query::Negation;
use dialog_query::{Application, Dependencies, Parameters, Premise, Value, VariableScope};
use std::collections::HashMap;

#[test]
fn test_concept_as_conclusion_operations() {
    let mut attributes = HashMap::new();
    attributes.insert(
        "name".to_string(),
        Attribute::new("person", "name", "Person name", ValueDataType::String),
    );
    attributes.insert(
        "age".to_string(),
        Attribute::new("person", "age", "Person age", ValueDataType::UnsignedInt),
    );

    let concept = Concept {
        operator: "person".to_string(),
        attributes,
    };

    // Test contains method - should include "this" parameter
    assert!(concept.contains("this"));
    assert!(concept.contains("name"));
    assert!(concept.contains("age"));
    assert!(!concept.contains("height"));

    // Test absent method
    let mut dependencies = Dependencies::new();
    dependencies.desire("name".into(), 100);

    // Should find "this" as absent since it's not in dependencies
    assert_eq!(concept.absent(&dependencies), Some("this"));

    dependencies.desire("this".into(), 100);
    // Now should find "age" as absent
    assert_eq!(concept.absent(&dependencies), Some("age"));

    dependencies.desire("age".into(), 100);
    // Now nothing should be absent
    assert_eq!(concept.absent(&dependencies), None);
}

#[test]
fn test_concept_creation() {
    let mut attributes = HashMap::new();
    attributes.insert(
        "name".to_string(),
        Attribute::new("person", "name", "Person name", ValueDataType::String),
    );

    let concept = Concept {
        operator: "person".to_string(),
        attributes,
    };

    assert_eq!(concept.operator, "person");
    assert_eq!(concept.attributes.len(), 1);
    assert!(concept.attributes.contains_key("name"));
}

#[test]
fn test_concept_application_analysis() {
    let mut attributes = HashMap::new();
    attributes.insert(
        "name".to_string(),
        Attribute::new("person", "name", "Person name", ValueDataType::String),
    );
    attributes.insert(
        "age".to_string(),
        Attribute::new("person", "age", "Person age", ValueDataType::UnsignedInt),
    );

    let concept = Concept {
        operator: "person".to_string(),
        attributes,
    };

    let mut terms = Parameters::new();
    terms.insert("name".to_string(), Term::var("person_name"));
    terms.insert("age".to_string(), Term::var("person_age"));

    let concept_app = ConceptApplication { terms, concept };

    let analysis = concept_app.analyze().expect("Analysis should succeed");

    assert_eq!(analysis.cost, BASE_COST);
    assert!(analysis.dependencies.contains("this"));
    assert!(analysis.dependencies.contains("name"));
    assert!(analysis.dependencies.contains("age"));
    // Check that we have the expected dependencies
    let deps_count = analysis.dependencies.iter().count();
    assert_eq!(deps_count, 3);
}

#[test]
fn test_deductive_rule_parameters() {
    let rule = DeductiveRule {
        conclusion: Concept::new("adult".into())
            .with(
                "name",
                Attribute::new("person", "name", "Person name", ValueDataType::String),
            )
            .with(
                "age",
                Attribute::new("person", "age", "Person age", ValueDataType::UnsignedInt),
            ),
        premises: vec![],
    };

    let params = rule.parameters();
    assert!(params.contains("this"));
    assert!(params.contains("name"));
    assert!(params.contains("age"));
    assert_eq!(params.len(), 3);
}

#[test]
fn test_premise_construction() {
    let fact_selector = FactSelector::new()
        .the("person/name")
        .of(Term::var("person"))
        .is(Value::String("Alice".to_string()));

    let premise = Premise::from(fact_selector);

    match premise {
        Premise::Apply(Application::Fact(_)) => {
            // Expected case
        }
        _ => panic!("Expected Select application"),
    }
}

#[test]
fn test_analysis_structure() {
    let mut deps = Dependencies::new();
    deps.desire("test".into(), 50);

    let analysis = Analysis {
        cost: 100,
        dependencies: deps,
    };

    assert_eq!(analysis.cost, 100);
    assert!(analysis.dependencies.contains("test"));
}

#[test]
fn test_plan_candidate_structure() {
    let fact_selector = FactSelector::new().the("test/attr");
    let premise = Premise::from(fact_selector);

    let candidate = PlanCandidate {
        premise: &premise,
        dependencies: VariableScope::new(),
        result: Err(PlanError::UnexpectedError),
    };

    // Test that the structure exists and can be created
    assert!(matches!(candidate.result, Err(PlanError::UnexpectedError)));
}

#[test]
fn test_error_types() {
    // Test AnalyzerError creation
    let rule = DeductiveRule {
        conclusion: Concept {
            operator: "test".to_string(),
            attributes: HashMap::new(),
        },
        premises: vec![],
    };

    let analyzer_error = AnalyzerError::UnusedParameter {
        rule: rule.clone(),
        parameter: "test_param".to_string(),
    };

    // Test conversion to PlanError
    let plan_error: PlanError = analyzer_error.into();
    match &plan_error {
        PlanError::UnusedParameter { rule: r, parameter } => {
            assert_eq!(r.conclusion.operator, "test");
            assert_eq!(parameter, "test_param");
        }
        _ => panic!("Expected UnusedParameter variant"),
    }

    // Test conversion to QueryError
    let query_error: QueryError = plan_error.into();
    match query_error {
        QueryError::PlanningError { .. } => {
            // Expected
        }
        _ => panic!("Expected PlanningError variant"),
    }
}

#[test]
fn test_application_variants() {
    // Test Select application
    let selector = FactSelector::new().the("test/attr");
    let app = Application::Fact(selector);

    match app {
        Application::Fact(_) => {
            // Expected
        }
        _ => panic!("Expected Select variant"),
    }

    // Test other variants exist
    let mut terms = Parameters::new();
    terms.insert("test".to_string(), Term::var("test_var"));
    let concept = Concept {
        operator: "test".to_string(),
        attributes: HashMap::new(),
    };
    let concept_app = Application::Concept(ConceptApplication { terms, concept });

    match concept_app {
        Application::Concept(_) => {
            // Expected
        }
        _ => panic!("Expected Realize variant"),
    }
}

#[test]
fn test_negation_construction() {
    let selector = FactSelector::new().the("test/attr");
    let app = Application::Fact(selector);
    let negation = Negation(app);

    // Test that negation wraps the application
    match negation {
        Negation(Application::Fact(_)) => {
            // Expected
        }
        _ => panic!("Expected wrapped Select application"),
    }
}
