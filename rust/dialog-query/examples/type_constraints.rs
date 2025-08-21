//! Example demonstrating type constraints on Variable<T>
//!
//! This example shows how the Variable struct is constrained to only accept
//! types that implement IntoValueDataType, preventing invalid type usage.

use dialog_query::Term;
use dialog_artifacts::Value;

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
    println!("  Entity term: {:?}", entity_var.name());

    let attribute_var = Term::<dialog_artifacts::Attribute>::var("attr");
    println!("  Attribute term: {:?}", attribute_var.name());

    let flexible_var = Term::<Value>::var("anything");
    println!("  Flexible term: {:?}", flexible_var.name());

    println!("\n❌ Unsupported types (these would NOT compile):");
    println!("  // Term::<HashMap<String, String>>::var(\"map\");  // ❌ Error!");
    println!("  // Term::<MyCustomStruct>::var(\"custom\");        // ❌ Error!");
    println!("  // Term::<Option<String>>::var(\"maybe\");         // ❌ Error!");

    println!("\n✅ The type constraint ensures only valid Dialog value types can be used!");
    println!("   Only types implementing IntoValueDataType are allowed.");
}
