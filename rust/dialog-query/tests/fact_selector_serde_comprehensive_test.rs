use dialog_artifacts::Value;
use dialog_query::{FactSelector, Term};

#[test]
fn test_fact_selector_empty() -> Result<(), Box<dyn std::error::Error>> {
    // Test completely empty FactSelector
    let selector = FactSelector::new();
    let json = serde_json::to_string(&selector)?;
    assert_eq!(json, r#"{}"#);

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);
    assert!(deserialized.the.is_none());
    assert!(deserialized.of.is_none());
    assert!(deserialized.is.is_none());

    Ok(())
}

#[test]
fn test_fact_selector_only_attribute_constant() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with only attribute (the) as constant
    let selector = FactSelector::new().the("person/name");
    let json = serde_json::to_string(&selector)?;
    assert_eq!(json, r#"{"the":"person/name"}"#);

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);
    assert!(deserialized.the.is_some());
    assert!(deserialized.of.is_none());
    assert!(deserialized.is.is_none());

    Ok(())
}

#[test]
fn test_fact_selector_only_attribute_variable() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with only attribute (the) as variable
    let selector = FactSelector::new().the(Term::<dialog_artifacts::Attribute>::var("attr"));
    let json = serde_json::to_string(&selector)?;
    assert_eq!(json, r#"{"the":{"?":{"name":"attr","type":"Symbol"}}}"#);

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);

    Ok(())
}

#[test]
fn test_fact_selector_only_entity_constant() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with only entity (of) as constant
    let entity = dialog_artifacts::Entity::new().unwrap();
    let selector = FactSelector::new().of(entity.clone());
    let json = serde_json::to_string(&selector)?;

    // Entity serializes to its DID string representation
    let expected = format!(r#"{{"of":"{}"}}"#, entity);
    assert_eq!(json, expected);

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);

    Ok(())
}

#[test]
fn test_fact_selector_only_entity_variable() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with only entity (of) as variable
    let selector = FactSelector::new().of(Term::<dialog_artifacts::Entity>::var("user"));
    let json = serde_json::to_string(&selector)?;
    assert_eq!(json, r#"{"of":{"?":{"name":"user","type":"Entity"}}}"#);

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);

    Ok(())
}

#[test]
fn test_fact_selector_only_value_constant_string() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with only value (is) as string constant
    let selector = FactSelector::new().is("Alice");
    let json = serde_json::to_string(&selector)?;
    assert_eq!(json, r#"{"is":"Alice"}"#);

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);

    Ok(())
}

#[test]
fn test_fact_selector_only_value_constant_number() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with only value (is) as number constant
    let selector = FactSelector::new().is(Value::SignedInt(42));
    let json = serde_json::to_string(&selector)?;
    assert_eq!(json, r#"{"is":42}"#);

    // Test deserialization - NOTE: JSON numbers often deserialize as floats
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    // Check that the deserialized value is numerically equivalent,
    // but may be Float(42.0) instead of SignedInt(42)
    if let Some(Term::Constant(value)) = &deserialized.is {
        match value {
            Value::SignedInt(i) => assert_eq!(*i, 42),
            Value::Float(f) => assert_eq!(*f, 42.0),
            _ => panic!("Expected numeric value, got: {:?}", value),
        }
    } else {
        panic!("Expected constant term");
    }

    Ok(())
}

#[test]
fn test_fact_selector_only_value_constant_boolean() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with only value (is) as boolean constant
    let selector = FactSelector::new().is(Value::Boolean(true));
    let json = serde_json::to_string(&selector)?;
    assert_eq!(json, r#"{"is":true}"#);

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);

    Ok(())
}

#[test]
fn test_fact_selector_only_value_variable_typed() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with only value (is) as typed variable
    // Note: Term<String> gets converted to Term<Value> and loses type info
    let selector = FactSelector::new().is(Term::var("name"));
    let json = serde_json::to_string(&selector)?;
    // When Term<String> is used in FactSelector<Value>, it gets converted to Term<Value>
    // and loses the String type constraint, becoming just {"?":{"name":"name"}}
    assert_eq!(json, r#"{"is":{"?":{"name":"name"}}}"#);

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);

    Ok(())
}

#[test]
fn test_fact_selector_only_value_variable_untyped() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with only value (is) as untyped variable (Value type)
    let selector = FactSelector::new().is(Term::<Value>::var("value"));
    let json = serde_json::to_string(&selector)?;
    // Should NOT contain type field for Value type
    assert_eq!(json, r#"{"is":{"?":{"name":"value"}}}"#);

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);

    Ok(())
}

#[test]
fn test_fact_selector_only_value_blank_typed() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with only value (is) as blank (unnamed) typed variable
    // Note: Term<String> gets converted to Term<Value> and loses type info
    let selector = FactSelector::new().is(Term::<Value>::blank());
    let json = serde_json::to_string(&selector)?;
    // When Term<String> is used in FactSelector<Value>, it gets converted to Term<Value>
    // and loses the String type constraint, becoming just {"?":{}}}
    assert_eq!(json, r#"{"is":{"?":{}}}"#);

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);

    Ok(())
}

#[test]
fn test_fact_selector_only_value_blank_untyped() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with only value (is) as blank (unnamed) untyped variable
    let selector = FactSelector::new().is(Term::<Value>::default());
    let json = serde_json::to_string(&selector)?;
    // Should NOT contain name or type fields for blank Value variables
    assert_eq!(json, r#"{"is":{"?":{}}}"#);

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);

    Ok(())
}

#[test]
fn test_fact_selector_all_constants() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with all fields as constants
    let entity = dialog_artifacts::Entity::new().unwrap();
    let selector = FactSelector::new()
        .the("person/name")
        .of(entity.clone())
        .is("Alice");
    let json = serde_json::to_string(&selector)?;
    let expected = format!(r#"{{"the":"person/name","of":"{}","is":"Alice"}}"#, entity);
    assert_eq!(json, expected);

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);

    Ok(())
}

#[test]
fn test_fact_selector_all_variables_typed() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with all fields as typed variables
    let selector = FactSelector::new()
        .the(Term::var("attr"))
        .of(Term::var("user"))
        .is(Term::<String>::var("name")); // Typed FactSelector preserves String type
    let json = serde_json::to_string(&selector)?;
    // The "is" field loses String type constraint when converted to Term<Value>
    assert_eq!(
        json,
        r#"{"the":{"?":{"name":"attr","type":"Symbol"}},"of":{"?":{"name":"user","type":"Entity"}},"is":{"?":{"name":"name","type":"String"}}}"#
    );

    // Test deserialization
    let deserialized = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);

    Ok(())
}

#[test]
fn test_fact_selector_all_variables_untyped() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with all fields as untyped variables (Value type)
    let selector = FactSelector::new()
        .the(Term::<dialog_artifacts::Attribute>::var("attr")) // Attribute must be typed
        .of(Term::<dialog_artifacts::Entity>::var("user")) // Entity must be typed
        .is(Term::<Value>::var("value")); // Value can be untyped
    let json = serde_json::to_string(&selector)?;
    // Value should not have type field
    assert_eq!(
        json,
        r#"{"the":{"?":{"name":"attr","type":"Symbol"}},"of":{"?":{"name":"user","type":"Entity"}},"is":{"?":{"name":"value"}}}"#
    );

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);

    Ok(())
}

#[test]
fn test_fact_selector_all_blanks_typed() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with all fields as blank (unnamed) typed variables
    let selector = FactSelector::new()
        .the(Term::blank())
        .of(Term::blank())
        .is(Term::<Value>::default()); // Now uses explicit Value type
    let json = serde_json::to_string(&selector)?;
    // The "is" field loses String type constraint when converted to Term<Value>
    assert_eq!(
        json,
        r#"{"the":{"?":{"type":"Symbol"}},"of":{"?":{"type":"Entity"}},"is":{"?":{}}}"#
    );

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);

    Ok(())
}

#[test]
fn test_fact_selector_all_blanks_mixed() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with all fields as blank variables (mixed typed/untyped)
    let selector = FactSelector::new()
        .the(Term::<dialog_artifacts::Attribute>::default())
        .of(Term::<dialog_artifacts::Entity>::default())
        .is(Term::<Value>::default());
    let json = serde_json::to_string(&selector)?;
    assert_eq!(
        json,
        r#"{"the":{"?":{"type":"Symbol"}},"of":{"?":{"type":"Entity"}},"is":{"?":{}}}"#
    );

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);

    Ok(())
}

#[test]
fn test_fact_selector_mixed_constants_variables() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with mixed constants and variables
    let selector = FactSelector::new()
        .the("person/name") // constant
        .of(Term::<dialog_artifacts::Entity>::var("user")) // variable
        .is("Alice"); // constant
    let json = serde_json::to_string(&selector)?;
    assert_eq!(
        json,
        r#"{"the":"person/name","of":{"?":{"name":"user","type":"Entity"}},"is":"Alice"}"#
    );

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);

    Ok(())
}

#[test]
fn test_fact_selector_mixed_variables_blanks() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with mixed named variables and blanks
    let selector = FactSelector::new()
        .the(Term::<dialog_artifacts::Attribute>::var("attr")) // named variable
        .of(Term::<dialog_artifacts::Entity>::default()) // blank
        .is(Term::<Value>::var("name")); // Now uses explicit Value type
    let json = serde_json::to_string(&selector)?;
    // The "is" field loses String type constraint when converted to Term<Value>
    assert_eq!(
        json,
        r#"{"the":{"?":{"name":"attr","type":"Symbol"}},"of":{"?":{"type":"Entity"}},"is":{"?":{"name":"name"}}}"#
    );

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);

    Ok(())
}

#[test]
fn test_fact_selector_complex_mixed() -> Result<(), Box<dyn std::error::Error>> {
    // Test FactSelector with complex mix: constant, named variable, blank variable
    let selector = FactSelector::new()
        .the("person/name") // constant
        .of(Term::<dialog_artifacts::Entity>::var("user")) // named variable
        .is(Term::<Value>::default()); // blank untyped variable
    let json = serde_json::to_string(&selector)?;
    assert_eq!(
        json,
        r#"{"the":"person/name","of":{"?":{"name":"user","type":"Entity"}},"is":{"?":{}}}"#
    );

    // Test deserialization
    let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
    assert_eq!(selector, deserialized);

    Ok(())
}

#[test]
fn test_fact_selector_value_types() -> Result<(), Box<dyn std::error::Error>> {
    // Test different Value constant types in is field
    let entity = dialog_artifacts::Entity::new().unwrap();
    let selector_entity = FactSelector::<Value>::new().is(entity.clone());
    let json_entity = serde_json::to_string(&selector_entity)?;
    let expected_entity = format!(r#"{{"is":"{}"}}"#, entity);
    assert_eq!(json_entity, expected_entity);

    let selector_float = FactSelector::new().is(Value::Float(3.14));
    let json_float = serde_json::to_string(&selector_float)?;
    assert_eq!(json_float, r#"{"is":3.14}"#);

    let selector_bytes = FactSelector::new().is(Value::Bytes(vec![1, 2, 3]));
    let json_bytes = serde_json::to_string(&selector_bytes)?;
    assert_eq!(json_bytes, r#"{"is":[1,2,3]}"#);

    // Test deserialization
    let _deserialized_entity: FactSelector<Value> = serde_json::from_str(&json_entity)?;
    let _deserialized_float: FactSelector<Value> = serde_json::from_str(&json_float)?;
    let _deserialized_bytes: FactSelector<Value> = serde_json::from_str(&json_bytes)?;

    Ok(())
}

#[test]
fn test_fact_selector_round_trip_all_variants() -> Result<(), Box<dyn std::error::Error>> {
    // Test that all variants can round-trip (serialize -> deserialize -> compare)
    let test_cases = vec![
        ("empty", FactSelector::<Value>::new()),
        ("only_attr_const", FactSelector::new().the("test/attr")),
        (
            "only_entity_var",
            FactSelector::new().of(Term::<dialog_artifacts::Entity>::var("e")),
        ),
        (
            "only_value_blank",
            FactSelector::new().is(Term::<Value>::default()),
        ),
        (
            "mixed",
            FactSelector::new()
                .the("test/attr")
                .of(Term::<dialog_artifacts::Entity>::var("user"))
                .is("test_string"),
        ), // Use string to avoid int/float deserialization issues
    ];

    for (name, selector) in test_cases {
        let json = serde_json::to_string(&selector)?;
        let deserialized: FactSelector<Value> = serde_json::from_str(&json)?;
        assert_eq!(
            selector, deserialized,
            "Round-trip failed for case: {} with JSON: {}",
            name, json
        );
    }

    Ok(())
}
