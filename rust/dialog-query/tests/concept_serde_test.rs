use dialog_query::predicate::Concept;
use dialog_query::attribute::Attribute;
use dialog_query::artifact::ValueDataType;
use std::collections::HashMap;

#[test]
fn test_concept_serialization_to_specific_json() {
    let mut attributes = HashMap::new();
    attributes.insert(
        "name".to_string(),
        Attribute::new("user", "name", "User's name", ValueDataType::String),
    );
    attributes.insert(
        "age".to_string(),
        Attribute::new("user", "age", "User's age", ValueDataType::UnsignedInt),
    );

    let concept = Concept {
        operator: "user".to_string(),
        attributes,
    };

    // Test serialization to JSON
    let json = serde_json::to_string(&concept).expect("Should serialize");
    
    // Parse the JSON to verify structure (since HashMap order isn't guaranteed)
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");
    let obj = parsed.as_object().expect("Should be object");
    
    // Check operator
    assert_eq!(obj["operator"], "user");
    
    // Check attributes structure
    let attributes_obj = obj["attributes"].as_object().expect("Should have attributes object");
    assert_eq!(attributes_obj.len(), 2);
    
    // Check name attribute
    let name_attr = attributes_obj["name"].as_object().expect("Should have name attribute");
    assert_eq!(name_attr["namespace"], "user");
    assert_eq!(name_attr["name"], "name");
    assert_eq!(name_attr["description"], "User's name");
    assert_eq!(name_attr["data_type"], "String");
    
    // Check age attribute
    let age_attr = attributes_obj["age"].as_object().expect("Should have age attribute");
    assert_eq!(age_attr["namespace"], "user");
    assert_eq!(age_attr["name"], "age");
    assert_eq!(age_attr["description"], "User's age");
    assert_eq!(age_attr["data_type"], "UnsignedInt");
}

#[test]
fn test_concept_deserialization_from_specific_json() {
    let json = r#"{
        "operator": "person",
        "attributes": {
            "email": {
                "namespace": "person",
                "name": "email",
                "description": "Person's email address",
                "data_type": "String"
            },
            "active": {
                "namespace": "person",
                "name": "active",
                "description": "Whether person is active",
                "data_type": "Boolean"
            }
        }
    }"#;

    let concept: Concept = serde_json::from_str(json).expect("Should deserialize");
    
    assert_eq!(concept.operator, "person");
    assert_eq!(concept.attributes.len(), 2);
    
    let email_attr = concept.attributes.get("email").expect("Should have email attribute");
    assert_eq!(email_attr.namespace, "person");
    assert_eq!(email_attr.name, "email");
    assert_eq!(email_attr.description, "Person's email address");
    assert_eq!(email_attr.data_type, ValueDataType::String);
    
    let active_attr = concept.attributes.get("active").expect("Should have active attribute");
    assert_eq!(active_attr.namespace, "person");
    assert_eq!(active_attr.name, "active");
    assert_eq!(active_attr.description, "Whether person is active");
    assert_eq!(active_attr.data_type, ValueDataType::Boolean);
}

#[test]
fn test_concept_round_trip_serialization() {
    let mut attributes = HashMap::new();
    attributes.insert(
        "score".to_string(),
        Attribute::new("game", "score", "Game score", ValueDataType::UnsignedInt),
    );

    let original = Concept {
        operator: "game".to_string(),
        attributes,
    };

    // Serialize then deserialize
    let json = serde_json::to_string(&original).expect("Should serialize");
    let deserialized: Concept = serde_json::from_str(&json).expect("Should deserialize");
    
    // Should be identical
    assert_eq!(original.operator, deserialized.operator);
    assert_eq!(original.attributes.len(), deserialized.attributes.len());
    
    let orig_score = original.attributes.get("score").unwrap();
    let deser_score = deserialized.attributes.get("score").unwrap();
    assert_eq!(orig_score.namespace, deser_score.namespace);
    assert_eq!(orig_score.name, deser_score.name);
    assert_eq!(orig_score.description, deser_score.description);
    assert_eq!(orig_score.data_type, deser_score.data_type);
}

#[test]
fn test_expected_json_structure() {
    // Test that we get exactly the JSON structure we expect
    let mut attributes = HashMap::new();
    attributes.insert(
        "id".to_string(),
        Attribute::new("product", "id", "Product ID", ValueDataType::UnsignedInt),
    );

    let concept = Concept {
        operator: "product".to_string(),
        attributes,
    };

    let json = serde_json::to_string_pretty(&concept).expect("Should serialize");
    
    // Parse and check exact structure
    let expected_structure = r#"{
  "operator": "product",
  "attributes": {
    "id": {
      "namespace": "product",
      "name": "id",
      "description": "Product ID",
      "data_type": "UnsignedInt"
    }
  }
}"#;

    let actual: serde_json::Value = serde_json::from_str(&json).expect("Should parse actual");
    let expected: serde_json::Value = serde_json::from_str(expected_structure).expect("Should parse expected");
    
    assert_eq!(actual, expected, "JSON structure should match expected format");
}