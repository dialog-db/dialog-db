//! Test concept identifier stability - concepts with same attributes should have same URI

use dialog_query::artifact::Type;
use dialog_query::attribute::AttributeSchema;
use dialog_query::predicate::concept::{Attributes, Concept};

#[test]
fn test_concept_field_names_do_not_affect_hash() {
    // Create two concepts with the same attributes but potentially used with different field names
    // This tests that the hash is based on attributes, not field names

    let attributes1 = Attributes::from(vec![
        (
            "field_a".to_string(),
            AttributeSchema::new("person", "name", "Person's name", Type::String),
        ),
        (
            "field_b".to_string(),
            AttributeSchema::new("person", "age", "Person's age", Type::UnsignedInt),
        ),
    ]);

    let attributes2 = Attributes::from(vec![
        (
            "different_field_1".to_string(),
            AttributeSchema::new("person", "name", "Person's name", Type::String),
        ),
        (
            "different_field_2".to_string(),
            AttributeSchema::new("person", "age", "Person's age", Type::UnsignedInt),
        ),
    ]);

    let concept1 = Concept::Dynamic {
        description: String::new(),
        attributes: attributes1,
    };

    let concept2 = Concept::Dynamic {
        description: String::new(),
        attributes: attributes2,
    };

    // Concepts should have the same hash and URI
    assert_eq!(
        concept1.hash(),
        concept2.hash(),
        "Concepts with same attributes but different field names should have same hash"
    );

    assert_eq!(
        concept1.to_uri(),
        concept2.to_uri(),
        "Concepts with same attributes but different field names should have same URI"
    );
}

#[test]
fn test_concept_description_does_not_affect_hash() {
    // Create two concepts with the same attributes but different descriptions

    let attributes = Attributes::from(vec![(
        "name".to_string(),
        AttributeSchema::new("user", "name", "User's name", Type::String),
    )]);

    let concept1 = Concept::Dynamic {
        description: "A user in the system".to_string(),
        attributes: attributes.clone(),
    };

    let concept2 = Concept::Dynamic {
        description: "System user account".to_string(),
        attributes,
    };

    // Concepts should have the same hash and URI
    assert_eq!(
        concept1.hash(),
        concept2.hash(),
        "Concepts with different descriptions should have same hash"
    );

    assert_eq!(
        concept1.to_uri(),
        concept2.to_uri(),
        "Concepts with different descriptions should have same URI"
    );
}

#[test]
fn test_concept_attribute_order_does_not_affect_hash() {
    // Create two concepts with the same attributes in different order

    let attributes1 = Attributes::from(vec![
        (
            "name".to_string(),
            AttributeSchema::new("person", "name", "Name", Type::String),
        ),
        (
            "age".to_string(),
            AttributeSchema::new("person", "age", "Age", Type::UnsignedInt),
        ),
    ]);

    let attributes2 = Attributes::from(vec![
        (
            "age".to_string(),
            AttributeSchema::new("person", "age", "Age", Type::UnsignedInt),
        ),
        (
            "name".to_string(),
            AttributeSchema::new("person", "name", "Name", Type::String),
        ),
    ]);

    let concept1 = Concept::Dynamic {
        description: String::new(),
        attributes: attributes1,
    };

    let concept2 = Concept::Dynamic {
        description: String::new(),
        attributes: attributes2,
    };

    // Concepts should have the same hash (because CBOR encoding should use sorted keys)
    assert_eq!(
        concept1.hash(),
        concept2.hash(),
        "Concepts with same attributes in different order should have same hash"
    );

    assert_eq!(
        concept1.to_uri(),
        concept2.to_uri(),
        "Concepts with same attributes in different order should have same URI"
    );
}

#[test]
fn test_concept_different_attributes_different_hash() {
    // Verify that concepts with different attributes have different hashes

    let attributes1 = Attributes::from(vec![(
        "name".to_string(),
        AttributeSchema::new("person", "name", "Name", Type::String),
    )]);

    let attributes2 = Attributes::from(vec![(
        "email".to_string(),
        AttributeSchema::new("person", "email", "Email", Type::String),
    )]);

    let concept1 = Concept::Dynamic {
        description: String::new(),
        attributes: attributes1,
    };

    let concept2 = Concept::Dynamic {
        description: String::new(),
        attributes: attributes2,
    };

    // Concepts should have different hashes
    assert_ne!(
        concept1.hash(),
        concept2.hash(),
        "Concepts with different attributes should have different hashes"
    );

    assert_ne!(
        concept1.to_uri(),
        concept2.to_uri(),
        "Concepts with different attributes should have different URIs"
    );
}
