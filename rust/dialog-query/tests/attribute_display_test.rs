//! Test Attribute schema generation and metadata

use dialog_query::Attribute;

mod employee {
    use dialog_query::Attribute;

    /// Name of the employee
    #[derive(Attribute, Clone, PartialEq)]
    pub struct Name(pub String);

    /// Age of the employee
    #[derive(Attribute, Clone, PartialEq)]
    pub struct Age(pub u32);
}

mod person {
    use dialog_query::Attribute;

    /// Birthday of the person
    #[derive(Attribute, Clone, PartialEq)]
    pub struct Birthday(pub u32);
}

#[test]
fn test_attribute_schema_metadata() {
    // Test that the Attribute derive macro generates correct schema metadata
    let name = employee::Name("Alice".to_string());

    // Check namespace and name from schema
    assert_eq!(employee::Name::namespace(), "employee");
    assert_eq!(employee::Name::name(), "name");
    assert_eq!(employee::Name::description(), "Name of the employee");

    // Check value extraction
    assert_eq!(name.value(), &"Alice".to_string());
}

#[test]
fn test_attribute_schema_with_number() {
    let age = employee::Age(30);

    assert_eq!(employee::Age::namespace(), "employee");
    assert_eq!(employee::Age::name(), "age");
    assert_eq!(employee::Age::description(), "Age of the employee");
    assert_eq!(age.value(), &30);
}

#[test]
fn test_attribute_selector_format() {
    use dialog_query::artifact::Attribute as ArtifactAttribute;

    // Test that selector() generates the correct attribute identifier
    assert_eq!(
        employee::Name::selector(),
        "employee/name".parse::<ArtifactAttribute>().unwrap()
    );
    assert_eq!(
        employee::Age::selector(),
        "employee/age".parse::<ArtifactAttribute>().unwrap()
    );
    assert_eq!(
        person::Birthday::selector(),
        "person/birthday".parse::<ArtifactAttribute>().unwrap()
    );
}

#[test]
fn test_attribute_value_conversion() {
    // Test new constructor
    let name = employee::Name::new("Bob".to_string());
    assert_eq!(name.value(), &"Bob".to_string());

    let age = employee::Age::new(25);
    assert_eq!(age.value(), &25);

    let birthday = person::Birthday::new(19900101);
    assert_eq!(birthday.value(), &19900101);
}
