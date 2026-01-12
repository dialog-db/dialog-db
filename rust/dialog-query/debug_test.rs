use dialog_artifacts::Value;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TestTerm {
    #[serde(rename = "?")]
    Variable { name: Option<String> },
    #[serde(untagged)]
    Constant(String),
}

fn main() {
    println!("=== Testing simple constant deserialization ===");

    // Test 1: Try deserializing a simple string constant
    let json = r#""Alice""#;
    match serde_json::from_str::<TestTerm>(json) {
        Ok(term) => println!("✓ Successfully deserialized string: {:?}", term),
        Err(e) => println!("✗ Failed to deserialize string: {}", e),
    }

    // Test 2: Try deserializing a variable
    let json2 = r#"{"?":{"name":"user"}}"#;
    match serde_json::from_str::<TestTerm>(json2) {
        Ok(term) => println!("✓ Successfully deserialized variable: {:?}", term),
        Err(e) => println!("✗ Failed to deserialize variable: {}", e),
    }

    // Test 3: Try deserializing Value constant with Term<Value>
    println!("\n=== Testing Value deserialization ===");
    let json3 = r#""Alice""#;
    match serde_json::from_str::<Value>(json3) {
        Ok(value) => println!("✓ Value deserialized: {:?}", value),
        Err(e) => println!("✗ Value failed: {}", e),
    }
}
