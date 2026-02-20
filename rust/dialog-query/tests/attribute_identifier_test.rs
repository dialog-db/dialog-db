//! Test attribute identifier stability and URI formatting

use dialog_query::Attribute;

mod employee {
    use dialog_query::Attribute;

    #[derive(Attribute, Clone)]
    pub struct Name(pub String);

    #[derive(Attribute, Clone)]
    pub struct Salary(pub u32);

    #[derive(Attribute, Clone)]
    pub struct Job(pub String);
}

mod person {
    use dialog_query::Attribute;

    #[derive(Attribute, Clone)]
    pub struct Name(pub String);
}

#[test]
fn test_attribute_hash_stability() {
    // Same attribute should always produce the same hash
    let hash1 = employee::Name::hash();
    let hash2 = employee::Name::hash();

    assert_eq!(
        hash1, hash2,
        "Same attribute should produce identical hashes"
    );
}

#[test]
fn test_different_attributes_different_hashes() {
    // Different attributes should produce different hashes
    let name_hash = employee::Name::hash();
    let salary_hash = employee::Salary::hash();
    let job_hash = employee::Job::hash();

    assert_ne!(
        name_hash, salary_hash,
        "Name and Salary should have different hashes"
    );
    assert_ne!(
        name_hash, job_hash,
        "Name and Job should have different hashes"
    );
    assert_ne!(
        salary_hash, job_hash,
        "Salary and Job should have different hashes"
    );
}

#[test]
fn test_same_name_different_namespace_different_hashes() {
    // Same attribute name in different namespaces should have different hashes
    let employee_name_hash = employee::Name::hash();
    let person_name_hash = person::Name::hash();

    assert_ne!(
        employee_name_hash, person_name_hash,
        "employee::Name and person::Name should have different hashes"
    );
}

#[test]
fn test_attribute_uri_format() {
    // Test URI format is correct
    let uri = employee::Name::to_uri();

    assert!(
        uri.starts_with("the:"),
        "URI should start with 'the:' prefix"
    );
    assert_eq!(
        uri.len(),
        4 + 64,
        "URI should be 'the:' + 64 hex chars (32 bytes)"
    );
}

#[test]
fn test_attribute_uri_roundtrip() {
    // Test that we can parse what we format
    let uri = employee::Name::to_uri();
    let parsed_hash = dialog_query::attribute::AttributeSchema::<String>::parse_uri(&uri);

    assert!(parsed_hash.is_some(), "Should be able to parse valid URI");
    assert_eq!(
        parsed_hash.unwrap(),
        employee::Name::hash(),
        "Parsed hash should match original hash"
    );
}

#[test]
fn test_attribute_uri_parse_invalid() {
    // Test parsing invalid URIs
    assert!(
        dialog_query::attribute::AttributeSchema::<String>::parse_uri("invalid").is_none(),
        "Should fail to parse URI without 'the:' prefix"
    );

    assert!(
        dialog_query::attribute::AttributeSchema::<String>::parse_uri("the:invalid").is_none(),
        "Should fail to parse URI with invalid hash"
    );

    assert!(
        dialog_query::attribute::AttributeSchema::<String>::parse_uri("concept:abcd").is_none(),
        "Should fail to parse URI with wrong prefix"
    );
}

#[test]
fn test_attribute_schema_hash_stability() {
    // Test that AttributeSchema hash method works too
    let schema_hash = employee::Name::SCHEMA.hash();
    let trait_hash = employee::Name::hash();

    assert_eq!(
        schema_hash, trait_hash,
        "Schema hash and trait hash should match"
    );
}

#[test]
fn test_attribute_cbor_encoding() {
    // Test that CBOR encoding is deterministic
    let cbor1 = employee::Name::SCHEMA.to_cbor_bytes();
    let cbor2 = employee::Name::SCHEMA.to_cbor_bytes();

    assert_eq!(cbor1, cbor2, "CBOR encoding should be deterministic");
    assert!(!cbor1.is_empty(), "CBOR encoding should not be empty");
}

#[test]
fn test_attribute_description_does_not_affect_hash() {
    use dialog_query::artifact::Type;
    use dialog_query::attribute::AttributeSchema;

    // Create two attributes with the same properties except description
    let attr1 =
        AttributeSchema::<String>::new("user", "email", "Primary email address", Type::String);

    let attr2 = AttributeSchema::<String>::new(
        "user",
        "email",
        "User's email for notifications",
        Type::String,
    );

    // They should have the same hash
    assert_eq!(
        attr1.hash(),
        attr2.hash(),
        "Attributes with different descriptions should have the same hash"
    );

    // And the same URI
    assert_eq!(
        attr1.to_uri(),
        attr2.to_uri(),
        "Attributes with different descriptions should have the same URI"
    );
}
