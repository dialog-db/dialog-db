use dialog_query::{Attribute, Cardinality};

/// Test 1: Underscores in module names should convert to hyphens
mod account_name {
    use super::*;

    /// Account holder's name
    #[derive(Attribute, Clone)]
    pub struct Name(pub String);
}

/// Test 2: Nested modules for dotted namespaces
mod my {
    pub mod config {
        use super::super::*;

        /// Configuration key
        #[derive(Attribute, Clone)]
        pub struct Key(pub String);
    }
}

/// Test 3: Explicit namespace overrides module path
#[derive(Attribute, Clone)]
#[namespace("my.custom.namespace")]
pub struct Value(pub String);

/// Test 3b: Explicit namespace with identifier syntax
#[derive(Attribute, Clone)]
#[namespace(custom)]
pub struct CustomValue(pub String);

/// Test 3c: Explicit namespace with string literal in parentheses (for dotted namespaces)
#[derive(Attribute, Clone)]
#[namespace("io.gozala")]
pub struct DottedValue(pub String);

/// Test 4: Nested module with underscores
mod my_app {
    pub mod user_profile {
        use super::super::*;

        /// User email address
        #[derive(Attribute, Clone)]
        pub struct Email(pub String);
    }
}

#[test]
fn test_underscore_to_hyphen_conversion() {
    // account_name module should become "account-name" namespace
    assert_eq!(account_name::Name::NAMESPACE, "account-name");
    assert_eq!(account_name::Name::NAME, "name");
    assert_eq!(
        account_name::Name::selector().to_string(),
        "account-name/name"
    );
}

#[test]
fn test_nested_module_namespace() {
    // my::config uses only last segment: "config"
    assert_eq!(my::config::Key::NAMESPACE, "config");
    assert_eq!(my::config::Key::NAME, "key");
    assert_eq!(my::config::Key::selector().to_string(), "config/key");
}

#[test]
fn test_explicit_namespace_override() {
    // Explicit namespace should override any module-based derivation
    assert_eq!(Value::NAMESPACE, "my.custom.namespace");
    assert_eq!(Value::NAME, "value");
    assert_eq!(Value::selector().to_string(), "my.custom.namespace/value");
}

#[test]
fn test_namespace_identifier_syntax() {
    // #[namespace(custom)] should use "custom" as namespace
    assert_eq!(CustomValue::NAMESPACE, "custom");
    assert_eq!(CustomValue::NAME, "custom-value");
    assert_eq!(CustomValue::selector().to_string(), "custom/custom-value");
}

#[test]
fn test_namespace_string_literal_syntax() {
    // #[namespace("io.gozala")] should use "io.gozala" as namespace
    assert_eq!(DottedValue::NAMESPACE, "io.gozala");
    assert_eq!(DottedValue::NAME, "dotted-value");
    assert_eq!(
        DottedValue::selector().to_string(),
        "io.gozala/dotted-value"
    );
}

#[test]
fn test_nested_underscore_conversion() {
    // my_app::user_profile uses only last segment: "user-profile"
    assert_eq!(my_app::user_profile::Email::NAMESPACE, "user-profile");
    assert_eq!(my_app::user_profile::Email::NAME, "email");
    assert_eq!(
        my_app::user_profile::Email::selector().to_string(),
        "user-profile/email"
    );
}

#[test]
fn test_all_metadata_preserved() {
    let name = account_name::Name("John Doe".to_string());

    // Check all trait constants work correctly
    assert_eq!(account_name::Name::NAMESPACE, "account-name");
    assert_eq!(account_name::Name::NAME, "name");
    assert_eq!(account_name::Name::DESCRIPTION, "Account holder's name");
    assert_eq!(account_name::Name::CARDINALITY, Cardinality::One);
    assert_eq!(name.value(), "John Doe");
}
