use dialog_query::{FactSelector, Term};
use dialog_artifacts::Value;

#[test]
fn test_value_deserialization() -> Result<(), Box<dyn std::error::Error>> {
    // Test how Value deserializes from plain JSON
    let alice_result: Result<Value, _> = serde_json::from_str(r#""Alice""#);
    println!("Plain string to Value: {:?}", alice_result);
    
    let num_result: Result<Value, _> = serde_json::from_str("42");
    println!("Plain number to Value: {:?}", num_result);
    
    let bool_result: Result<Value, _> = serde_json::from_str("true");
    println!("Plain boolean to Value: {:?}", bool_result);
    
    Ok(())
}

#[test]
fn test_correct_json_formats() -> Result<(), Box<dyn std::error::Error>> {
    // Test variable: should be { "?": { "name": "user", "type": "..." } }
    let var_term: Term<String> = Term::var("user");
    let var_json = serde_json::to_string(&var_term)?;
    println!("Variable JSON: {}", var_json);
    assert!(var_json.contains(r#""?""#));
    assert!(var_json.contains(r#""name":"user""#));
    
    // Test Any: should be { "?": {} }
    let any_term: Term<String> = Term::any();
    let any_json = serde_json::to_string(&any_term)?;
    println!("Any JSON: {}", any_json);
    assert_eq!(any_json, r#"{"?":{}}"#);
    
    // Test constant: should be just the value
    let const_term: Term<String> = Term::Constant("Alice".to_string());
    let const_json = serde_json::to_string(&const_term)?;
    println!("Constant JSON: {}", const_json);
    assert_eq!(const_json, r#""Alice""#);
    
    Ok(())
}

#[test]
fn test_fact_selector_with_correct_format() -> Result<(), Box<dyn std::error::Error>> {
    // This should work: "is": "Alice" (just a string)
    let simple_json = r#"{
        "the": "person/name",
        "of": { "?": { "name": "user" } },
        "is": "Alice"
    }"#;
    
    let fact_selector: FactSelector<Value> = serde_json::from_str(simple_json)?;
    
    assert!(fact_selector.the.is_some());
    assert!(fact_selector.of.is_some());
    assert!(fact_selector.is.is_some());
    
    println!("✓ Simple string value works");
    
    Ok(())
}

#[test]
fn test_term_variable_json_format() -> Result<(), Box<dyn std::error::Error>> {
    // Test variable serialization
    let var_term: Term<String> = Term::var("user");
    let json = serde_json::to_string(&var_term)?;
    
    // Should serialize as { "?": { "name": "user", "type": "String" } }
    println!("Variable JSON: {}", json);
    
    // Test deserialization  
    let variable_json = r#"{ "?": { "name": "user", "type": "String" } }"#;
    let term: Term<String> = serde_json::from_str(variable_json)?;
    
    assert!(term.is_variable());
    assert_eq!(term.name().unwrap(), "user");
    
    Ok(())
}

#[test]
fn test_term_constant_json_format() -> Result<(), Box<dyn std::error::Error>> {
    // Test string constant
    let const_term: Term<String> = Term::Constant("Alice".to_string());
    let json = serde_json::to_string(&const_term)?;
    
    // Should serialize as just "Alice"
    println!("Constant JSON: {}", json);
    assert_eq!(json, r#""Alice""#);
    
    // Test deserialization
    let const_str: Term<String> = serde_json::from_str(r#""Alice""#)?;
    assert!(const_str.is_constant());
    assert_eq!(const_str.as_constant().unwrap(), "Alice");
    
    // Test numeric constant
    let num_term: Term<i32> = Term::Constant(42);
    let num_json = serde_json::to_string(&num_term)?;
    assert_eq!(num_json, "42");
    
    let parsed_num: Term<i32> = serde_json::from_str("42")?;
    assert!(parsed_num.is_constant());
    assert_eq!(*parsed_num.as_constant().unwrap(), 42);
    
    Ok(())
}

#[test] 
fn test_term_any_json_format() -> Result<(), Box<dyn std::error::Error>> {
    // Test Any serialization
    let any_term: Term<String> = Term::any();
    let json = serde_json::to_string(&any_term)?;
    
    // Should serialize as { "?": {} }
    println!("Any JSON: {}", json);
    assert_eq!(json, r#"{"?":{}}"#);
    
    // Test deserialization
    let parsed_any: Term<String> = serde_json::from_str(r#"{"?":{}}"#)?;
    assert!(parsed_any.is_any());
    
    Ok(())
}

#[test]
fn test_fact_selector_with_new_term_format() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector JSON with new Term format
    let selector_json = r#"{
        "the": "person/name",
        "of": { "?": { "name": "user" } },
        "is": { "?": { "name": "name" } }
    }"#;
    
    let fact_selector: FactSelector<Value> = serde_json::from_str(selector_json)?;
    
    // Check structure
    assert!(fact_selector.the.is_some());
    assert!(fact_selector.of.is_some());  
    assert!(fact_selector.is.is_some());
    
    // Check that 'the' is a constant
    let the_term = fact_selector.the.as_ref().unwrap();
    assert!(the_term.is_constant());
    
    // Check that 'of' is a variable
    let of_term = fact_selector.of.as_ref().unwrap();
    assert!(of_term.is_variable());
    assert_eq!(of_term.name().unwrap(), "user");
    
    // Check that 'is' is a variable
    let is_term = fact_selector.is.as_ref().unwrap();
    assert!(is_term.is_variable());
    assert_eq!(is_term.name().unwrap(), "name");
    
    println!("✓ FactSelector with new Term JSON format works");
    
    Ok(())
}

#[test]
fn test_mixed_terms_in_fact_selector() -> Result<(), Box<dyn std::error::Error>> {
    // Test mixed constants, variables, and any
    let selector_json = r#"{
        "the": { "?": { "name": "attribute" } },
        "of": { "?": {} },
        "is": "Alice"
    }"#;
    
    let fact_selector: FactSelector<Value> = serde_json::from_str(selector_json)?;
    
    // Check 'the' is variable
    assert!(fact_selector.the.as_ref().unwrap().is_variable());
    assert_eq!(fact_selector.the.as_ref().unwrap().name().unwrap(), "attribute");
    
    // Check 'of' is Any
    assert!(fact_selector.of.as_ref().unwrap().is_any());
    
    // Check 'is' is constant
    assert!(fact_selector.is.as_ref().unwrap().is_constant());
    
    println!("✓ Mixed terms in FactSelector work correctly");
    
    Ok(())
}

#[test]
fn test_value_type_inference() -> Result<(), Box<dyn std::error::Error>> {
    // Test that Value constants are properly deserialized from JSON
    let selector_json = r#"{
        "is": 42
    }"#;
    
    let fact_selector: FactSelector<Value> = serde_json::from_str(selector_json)?;
    let is_term = fact_selector.is.as_ref().unwrap();
    
    assert!(is_term.is_constant());
    // With untagged serde, 42 could be parsed as either SignedInt or Float
    match is_term.as_constant() {
        Some(Value::SignedInt(val)) => assert_eq!(*val, 42),
        Some(Value::Float(val)) => assert_eq!(*val, 42.0),
        other => panic!("Expected SignedInt or Float value, got: {:?}", other),
    }
    
    // Test boolean
    let bool_selector_json = r#"{ "is": true }"#;
    let bool_selector: FactSelector<Value> = serde_json::from_str(bool_selector_json)?;
    let bool_term = bool_selector.is.as_ref().unwrap();
    
    assert!(bool_term.is_constant());
    if let Some(Value::Boolean(val)) = bool_term.as_constant() {
        assert_eq!(*val, true);
    } else {
        panic!("Expected Boolean value, got: {:?}", bool_term.as_constant());
    }
    
    println!("✓ Value constants work correctly");
    
    Ok(())
}