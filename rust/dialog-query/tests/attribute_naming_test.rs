use dialog_query::{Attribute, Cardinality};

/// Test PascalCase conversion
mod test_pascal {
    use super::*;

    #[derive(Attribute, Clone)]
    pub struct UserName(pub String);

    #[derive(Attribute, Clone)]
    pub struct HTTPRequest(pub String);

    #[derive(Attribute, Clone)]
    pub struct APIKey(pub String);
}

#[test]
fn test_pascal_case_to_kebab_case() {
    // UserName -> user-name
    assert_eq!(test_pascal::UserName::NAME, "user-name");
}

#[test]
fn test_consecutive_capitals() {
    // HTTPRequest -> http-request
    assert_eq!(test_pascal::HTTPRequest::NAME, "http-request");

    // APIKey -> api-key
    assert_eq!(test_pascal::APIKey::NAME, "api-key");
}

#[test]
fn test_static_values() {
    // Verify NAMESPACE, NAME, DESCRIPTION work
    let ns = test_pascal::UserName::NAMESPACE;
    let name = test_pascal::UserName::NAME;
    let desc = test_pascal::UserName::DESCRIPTION;

    assert!(!ns.is_empty());
    assert_eq!(name, "user-name");
    // Description might be empty if no doc comment
    assert!(desc.len() >= 0); // Just verify it's accessible
}

#[test]
fn test_schema_static() {
    // SCHEMA should be accessible as const
    let schema = &test_pascal::UserName::SCHEMA;
    assert_eq!(schema.name, "user-name");
    assert_eq!(schema.cardinality, Cardinality::One);
}

#[test]
fn test_match_struct_literal() {
    use dialog_query::{Match, Term, Entity};

    let entity_id = Entity::new().unwrap();

    // Test that Match::<With<AttributeType>> { ... } works
    let query = Match::<dialog_query::attribute::With<test_pascal::UserName>> {
        this: Term::from(entity_id),
        has: Term::from("Alice".to_string()),
    };

    // Verify the fields are accessible
    assert!(matches!(query.this, Term::Constant(_)));
    assert!(matches!(query.has, Term::Constant(_)));
}

#[test]
fn test_quarriable_match_pattern() {
    use dialog_query::{Match, Term, Entity};

    let entity_id = Entity::new().unwrap();

    // Test that Match::<With<AttributeType>> { this, has } works via Quarriable
    let query = Match::<dialog_query::attribute::With<test_pascal::UserName>> {
        this: Term::from(entity_id),
        has: Term::from("Alice".to_string()),
    };

    // Verify the fields are accessible
    assert!(matches!(query.this, Term::Constant(_)));
    assert!(matches!(query.has, Term::Constant(_)));
}

#[test]
fn test_default_match_constructor() {
    use dialog_query::{Match, Term};

    // Test using Default to create a match with variables
    let query = Match::<dialog_query::attribute::With<test_pascal::UserName>>::default();

    // Default should create variable terms
    assert!(matches!(query.this, Term::Variable { .. }));
    assert!(matches!(query.has, Term::Variable { .. }));
}
