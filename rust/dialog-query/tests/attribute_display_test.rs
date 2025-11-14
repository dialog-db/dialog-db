//! Test Debug and Display implementations for Attribute types

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

#[test]
fn test_attribute_debug_format() {
    let name = employee::Name("Alice".to_string());

    // Debug should show structured format with metadata
    let debug_output = format!("{:?}", name);

    // Check that debug output contains the expected fields
    assert!(debug_output.contains("Name"));
    assert!(debug_output.contains("namespace"));
    assert!(debug_output.contains("employee"));
    assert!(debug_output.contains("name"));
    assert!(debug_output.contains("value"));
    assert!(debug_output.contains("Alice"));

    println!("Debug output: {}", debug_output);
}

#[test]
fn test_attribute_display_format() {
    let name = employee::Name("Alice".to_string());

    // Display should show clean selector: value format
    let display_output = format!("{}", name);

    assert_eq!(display_output, "employee/name: \"Alice\"");

    println!("Display output: {}", display_output);
}

#[test]
fn test_attribute_debug_with_number() {
    let age = employee::Age(30);

    let debug_output = format!("{:?}", age);

    assert!(debug_output.contains("Age"));
    assert!(debug_output.contains("namespace"));
    assert!(debug_output.contains("employee"));
    assert!(debug_output.contains("age"));
    assert!(debug_output.contains("value"));
    assert!(debug_output.contains("30"));

    println!("Debug output for number: {}", debug_output);
}

#[test]
fn test_attribute_display_with_number() {
    let age = employee::Age(30);

    let display_output = format!("{}", age);

    assert_eq!(display_output, "employee/age: 30");

    println!("Display output for number: {}", display_output);
}
