//! Example demonstrating type constraints on Variable<T>
//!
//! This example shows how the Variable struct is constrained to only accept
//! types that implement IntoValueDataType, preventing invalid type usage.

use dialog_query::{Term, Untyped};

fn main() {
    println!("=== Variable Type Constraints Demo ===\n");

    // These work - all supported types
    println!("✓ Supported types (these compile):");

    let string_var = Term::<String>::var("name");
    println!("  String variable: {}", string_var);

    let u64_var = Term::<u64>::var("age");
    println!("  U64 variable: {}", u64_var);

    let bool_var = Term::<bool>::var("active");
    println!("  Bool variable: {}", bool_var);

    let f64_var = Term::<f64>::var("score");
    println!("  F64 variable: {}", f64_var);

    let bytes_var = Term::<Vec<u8>>::var("data");
    println!("  Bytes variable: {}", bytes_var);

    let entity_var = Term::<dialog_artifacts::Entity>::var("entity");
    println!("  Entity variable: {}", entity_var);

    let attribute_var = Term::<dialog_artifacts::Attribute>::var("attr");
    println!("  Attribute variable: {}", attribute_var);

    let untyped_var = Term::<Untyped>::var("anything");
    println!("  Untyped variable: {}", untyped_var);

    println!("\n❌ Unsupported types (these would NOT compile):");
    println!("  // Variable::<HashMap<String, String>>::new(\"map\");  // ❌ Error!");
    println!("  // Variable::<MyCustomStruct>::new(\"custom\");        // ❌ Error!");
    println!("  // Variable::<Option<String>>::new(\"maybe\");         // ❌ Error!");

    println!("\n✅ The type constraint ensures only valid Dialog value types can be used!");
    println!("   Only types implementing IntoValueDataType are allowed.");
}
