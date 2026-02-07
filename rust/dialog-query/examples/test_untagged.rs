use serde::{Deserialize, Serialize};

// Test 1: Simple untagged enum
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum SimpleUntagged {
    Text(String),
    Number(i32),
}

// Test 2: Mixed tagged/untagged like Term
#[derive(Debug, Serialize, Deserialize)]
enum MixedEnum {
    #[serde(rename = "?")]
    Variable { name: String },
    #[serde(untagged)]
    Constant(String),
}

// Test 3: Fully untagged enum with object variant
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum FullyUntagged {
    Variable { name: String },
    Constant(String),
}

fn main() {
    println!("=== Test 1: Simple untagged enum ===");
    let text = SimpleUntagged::Text("hello".to_string());
    let num = SimpleUntagged::Number(42);

    let text_json = serde_json::to_string(&text).unwrap();
    let num_json = serde_json::to_string(&num).unwrap();

    println!("Text serialized: {}", text_json);
    println!("Number serialized: {}", num_json);

    match serde_json::from_str::<SimpleUntagged>(&text_json) {
        Ok(val) => println!("✓ Text deserialized: {:?}", val),
        Err(e) => println!("✗ Text deserialize failed: {}", e),
    }

    match serde_json::from_str::<SimpleUntagged>(&num_json) {
        Ok(val) => println!("✓ Number deserialized: {:?}", val),
        Err(e) => println!("✗ Number deserialize failed: {}", e),
    }

    println!("\n=== Test 2: Mixed tagged/untagged (like Term) ===");
    let var = MixedEnum::Variable {
        name: "user".to_string(),
    };
    let const_val = MixedEnum::Constant("Alice".to_string());

    let var_json = serde_json::to_string(&var).unwrap();
    let const_json = serde_json::to_string(&const_val).unwrap();

    println!("Variable serialized: {}", var_json);
    println!("Constant serialized: {}", const_json);

    match serde_json::from_str::<MixedEnum>(&var_json) {
        Ok(val) => println!("✓ Variable deserialized: {:?}", val),
        Err(e) => println!("✗ Variable deserialize failed: {}", e),
    }

    match serde_json::from_str::<MixedEnum>(&const_json) {
        Ok(val) => println!("✓ Constant deserialized: {:?}", val),
        Err(e) => println!("✗ Constant deserialize failed: {}", e),
    }

    println!("\n=== Test 3: Fully untagged enum ===");
    let var2 = FullyUntagged::Variable {
        name: "user".to_string(),
    };
    let const_val2 = FullyUntagged::Constant("Alice".to_string());

    let var_json2 = serde_json::to_string(&var2).unwrap();
    let const_json2 = serde_json::to_string(&const_val2).unwrap();

    println!("Variable serialized: {}", var_json2);
    println!("Constant serialized: {}", const_json2);

    match serde_json::from_str::<FullyUntagged>(&var_json2) {
        Ok(val) => println!("✓ Variable deserialized: {:?}", val),
        Err(e) => println!("✗ Variable deserialize failed: {}", e),
    }

    match serde_json::from_str::<FullyUntagged>(&const_json2) {
        Ok(val) => println!("✓ Constant deserialized: {:?}", val),
        Err(e) => println!("✗ Constant deserialize failed: {}", e),
    }

    // Extra test: What happens with the actual Term JSON?
    println!("\n=== Test 4: Actual Term JSON ===");
    let term_json = r#"{"?":{"name":"user"}}"#;

    match serde_json::from_str::<MixedEnum>(term_json) {
        Ok(val) => println!("✓ Term JSON deserialized with MixedEnum: {:?}", val),
        Err(e) => println!("✗ Term JSON deserialize failed with MixedEnum: {}", e),
    }
}
