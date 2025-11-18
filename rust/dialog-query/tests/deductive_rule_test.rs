use dialog_query::application::ConceptApplication;
use dialog_query::artifact::Type;
use dialog_query::attribute::AttributeSchema;
use dialog_query::error::{AnalyzerError, PlanError, QueryError};
use dialog_query::predicate::concept::Attributes;
use dialog_query::predicate::fact::Fact;
use dialog_query::predicate::{Concept, DeductiveRule};
use dialog_query::term::Term;
use dialog_query::Negation;
use dialog_query::{Application, Environment, Parameters, Premise, Value};
use std::collections::HashSet;

#[test]
fn test_concept_as_conclusion_operations() {
    let concept = Concept::Dynamic {
        operator: "person".to_string(),
        description: String::new(),
        attributes: Attributes::from(vec![
            (
                "name",
                AttributeSchema::<Value>::new("person", "name", "Person name", Type::String),
            ),
            (
                "age",
                AttributeSchema::<Value>::new("person", "age", "Person age", Type::UnsignedInt),
            ),
        ]),
    };

    // Test that attributes are present
    let param_names: Vec<&str> = concept.attributes().keys().collect();
    assert!(param_names.contains(&"name"));
    assert!(param_names.contains(&"age"));
    assert!(!param_names.contains(&"height"));
    // "this" parameter is implied but not in attributes
}

#[test]
fn test_concept_creation() {
    let concept = Concept::Dynamic {
        operator: "person".to_string(),
        description: String::new(),
        attributes: Attributes::from(vec![(
            "name".to_string(),
            AttributeSchema::<Value>::new("person", "name", "Person name", Type::String),
        )]),
    };

    assert_eq!(concept.operator(), "person");
    assert_eq!(concept.attributes().count(), 1);
    assert!(concept.attributes().keys().any(|k| k == "name"));
}

#[test]
fn test_concept_application_analysis() {
    let concept = Concept::Dynamic {
        operator: "person".to_string(),
        description: String::new(),
        attributes: Attributes::from(vec![
            (
                "name".to_string(),
                AttributeSchema::<Value>::new("person", "name", "Person name", Type::String),
            ),
            (
                "age".to_string(),
                AttributeSchema::<Value>::new("person", "age", "Person age", Type::UnsignedInt),
            ),
        ]),
    };

    let mut terms = Parameters::new();
    terms.insert("name".to_string(), Term::var("person_name"));
    terms.insert("age".to_string(), Term::var("person_age"));

    let concept_app = ConceptApplication { terms, concept };

    let cost = concept_app.estimate(&Environment::new());
    assert_eq!(cost, Some(2100));

    let schema = concept_app.schema();
    println!("schema {:?}", concept_app.schema());

    assert_eq!(schema.iter().count(), 3);
    assert!(schema.get("this").is_some());
    assert!(schema.get("name").is_some());
    assert!(schema.get("age").is_some());
}

#[test]
fn test_deductive_rule_parameters() {
    let rule = DeductiveRule {
        conclusion: Concept::Dynamic {
            operator: "adult".into(),
            description: String::new(),
            attributes: [
                (
                    "name".to_string(),
                    AttributeSchema::new("person", "name", "Person name", Type::String),
                ),
                (
                    "age".to_string(),
                    AttributeSchema::new("person", "age", "Person age", Type::UnsignedInt),
                ),
            ]
            .into(),
        },
        premises: vec![],
    };

    let params: HashSet<&str> = rule.parameters().collect();
    assert!(params.contains("this"));
    assert!(params.contains("name"));
    assert!(params.contains("age"));
    assert_eq!(params.len(), 3);
}

#[test]
fn test_premise_construction() {
    let fact = Fact::select()
        .the("person/name")
        .of(Term::var("person"))
        .is(Value::String("Alice".to_string()));

    let premise = Premise::from(fact);

    match premise {
        Premise::Apply(Application::Fact(_)) => {
            // Expected case
        }
        _ => panic!("Expected Select application"),
    }
}

#[test]
fn test_error_types() {
    // Test AnalyzerError creation
    let rule = DeductiveRule {
        conclusion: Concept::Dynamic {
            operator: "test".to_string(),
            description: String::new(),
            attributes: Attributes::new(),
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
            assert_eq!(r.conclusion.operator(), "test");
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
    let fact = Fact::select().the("test/attr");
    let app = Application::Fact(fact.into());

    match app {
        Application::Fact(_) => {
            // Expected
        }
        _ => panic!("Expected Select variant"),
    }

    // Test other variants exist
    let mut terms = Parameters::new();
    terms.insert("test".to_string(), Term::var("test_var"));
    let concept = Concept::Dynamic {
        operator: "test".to_string(),
        description: String::new(),
        attributes: Attributes::new(),
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
    let fact = Fact::select().the("test/attr");
    let app = Application::Fact(fact.into());
    let negation = Negation(app);

    // Test that negation wraps the application
    match negation {
        Negation(Application::Fact(_)) => {
            // Expected
        }
        _ => panic!("Expected wrapped Select application"),
    }
}
