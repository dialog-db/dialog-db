use dialog_query::artifact::Type;
use dialog_query::attribute::AttributeSchema;
use dialog_query::predicate::concept::Attributes;
use dialog_query::predicate::Concept;

#[test]
fn test_concept_serialization_to_specific_json() {
    let concept = Concept::Dynamic {
        operator: "user".to_string(),
        attributes: Attributes::from(vec![
            (
                "name".to_string(),
                AttributeSchema::new("user", "name", "User's name", Type::String),
            ),
            (
                "age".to_string(),
                AttributeSchema::new("user", "age", "User's age", Type::UnsignedInt),
            ),
        ]),
    };

    // Test serialization to JSON
    let json = serde_json::to_string(&concept).expect("Should serialize");

    // Parse the JSON to verify structure (since HashMap order isn't guaranteed)
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");
    let obj = parsed.as_object().expect("Should be object");

    // Check operator
    assert_eq!(obj["operator"], "user");

    // Check attributes structure
    let attributes_obj = obj["attributes"]
        .as_object()
        .expect("Should have attributes object");
    assert_eq!(attributes_obj.len(), 2);

    // Check name attribute
    let name_attr = attributes_obj["name"]
        .as_object()
        .expect("Should have name attribute");
    assert_eq!(name_attr["namespace"], "user");
    assert_eq!(name_attr["name"], "name");
    assert_eq!(name_attr["description"], "User's name");
    assert_eq!(name_attr["type"], "String");

    // Check age attribute
    let age_attr = attributes_obj["age"]
        .as_object()
        .expect("Should have age attribute");
    assert_eq!(age_attr["namespace"], "user");
    assert_eq!(age_attr["name"], "age");
    assert_eq!(age_attr["description"], "User's age");
    assert_eq!(age_attr["type"], "UnsignedInt");
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
                "type": "String"
            },
            "active": {
                "namespace": "person",
                "name": "active",
                "description": "Whether person is active",
                "type": "Boolean"
            }
        }
    }"#;

    let concept: Concept = serde_json::from_str(json).expect("Should deserialize");

    assert_eq!(concept.operator(), "person");
    assert_eq!(concept.attributes().count(), 2);

    let email_attr = concept
        .attributes()
        .iter()
        .find(|(k, _)| *k == "email")
        .map(|(_, v)| v)
        .expect("Should have email attribute");
    assert_eq!(email_attr.namespace, "person");
    assert_eq!(email_attr.name, "email");
    assert_eq!(email_attr.description, "Person's email address");
    assert_eq!(email_attr.content_type, Some(Type::String));

    let active_attr = concept
        .attributes()
        .iter()
        .find(|(k, _)| *k == "active")
        .map(|(_, v)| v)
        .expect("Should have active attribute");
    assert_eq!(active_attr.namespace, "person");
    assert_eq!(active_attr.name, "active");
    assert_eq!(active_attr.description, "Whether person is active");
    assert_eq!(active_attr.content_type, Some(Type::Boolean));
}

#[test]
fn test_concept_round_trip_serialization() {
    let original = Concept::Dynamic {
        operator: "game".to_string(),
        attributes: Attributes::from(vec![(
            "score".to_string(),
            AttributeSchema::new("game", "score", "Game score", Type::UnsignedInt),
        )]),
    };

    // Serialize then deserialize
    let json = serde_json::to_string(&original).expect("Should serialize");
    let deserialized: Concept = serde_json::from_str(&json).expect("Should deserialize");

    // Should be identical
    assert_eq!(original.operator(), deserialized.operator());
    assert_eq!(
        original.attributes().count(),
        deserialized.attributes().count()
    );

    let orig_score = original
        .attributes()
        .iter()
        .find(|(k, _)| *k == "score")
        .map(|(_, v)| v)
        .unwrap();
    let deser_score = deserialized
        .attributes()
        .iter()
        .find(|(k, _)| *k == "score")
        .map(|(_, v)| v)
        .unwrap();
    assert_eq!(orig_score.namespace, deser_score.namespace);
    assert_eq!(orig_score.name, deser_score.name);
    assert_eq!(orig_score.description, deser_score.description);
    assert_eq!(orig_score.content_type, deser_score.content_type);
}

#[test]
fn test_expected_json_structure() {
    // Test that we get exactly the JSON structure we expect
    let concept = Concept::Dynamic {
        operator: "product".to_string(),
        attributes: Attributes::from(vec![(
            "id".to_string(),
            AttributeSchema::new("product", "id", "Product ID", Type::UnsignedInt),
        )]),
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
      "type": "UnsignedInt"
    }
  }
}"#;

    let actual: serde_json::Value = serde_json::from_str(&json).expect("Should parse actual");
    let expected: serde_json::Value =
        serde_json::from_str(expected_structure).expect("Should parse expected");

    assert_eq!(
        actual, expected,
        "JSON structure should match expected format"
    );
}
