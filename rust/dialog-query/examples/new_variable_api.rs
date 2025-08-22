//! Example demonstrating the new Term API with clean type-safe syntax
//! This shows the clean API: Term::<T>::var() for typed variables

use dialog_artifacts::{Entity, Value};
use dialog_query::Term;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== New Variable API Demo ===");

    // New API: Term::<T>::var() for typed variables with compile-time type safety
    let name_var = Term::<String>::var("name"); // Returns Term<String>
    let age_var = Term::<u64>::var("age"); // Returns Term<u64>
    let active_var = Term::<bool>::var("active"); // Returns Term<bool>
    let entity_var = Term::<Entity>::var("user"); // Returns Term<Entity>

    println!("\n=== Typed Terms with Zero-Cost Type Safety ===");
    println!(
        "Name term: {:?} (type: {:?})",
        name_var.name(),
        name_var.data_type()
    );
    println!(
        "Age term: {:?} (type: {:?})",
        age_var.name(),
        age_var.data_type()
    );
    println!(
        "Active term: {:?} (type: {:?})",
        active_var.name(),
        active_var.data_type()
    );
    println!(
        "Entity term: {:?} (type: {:?})",
        entity_var.name(),
        entity_var.data_type()
    );

    // New API: Term::<Value>::var() returns Term<Value> for maximum flexibility
    let wildcard_var = Term::<Value>::var("wildcard"); // Returns Term<Value>

    println!("\n=== Flexible Value Terms ===");
    println!(
        "Wildcard term: {:?} (type: {:?})",
        wildcard_var.name(),
        wildcard_var.data_type()
    );

    // Type safety demonstrations
    println!("\n=== Compile-Time Type Safety ===");

    // Type safety is now enforced through compile-time generics and runtime type checking
    let _alice_value = Value::String("Alice".to_string());
    let _bool_value = Value::Boolean(true);
    let _age_value = Value::UnsignedInt(42);

    println!("Type safety is enforced at compile-time with Term<T>");
    println!("String terms work with string values");
    println!("u64 terms work with unsigned int values");
    println!("Value terms work with any value type");

    // Working with the new unified Term<T> system
    println!("\n=== Unified Term<T> System ===");

    println!("Number of terms created: 5");
    println!(
        "  Term 0: {:?} (type: {:?})",
        name_var.name(),
        name_var.data_type()
    );
    println!(
        "  Term 1: {:?} (type: {:?})",
        age_var.name(),
        age_var.data_type()
    );
    println!(
        "  Term 2: {:?} (type: {:?})",
        active_var.name(),
        active_var.data_type()
    );
    println!(
        "  Term 3: {:?} (type: {:?})",
        entity_var.name(),
        entity_var.data_type()
    );
    println!(
        "  Term 4: {:?} (type: {:?})",
        wildcard_var.name(),
        wildcard_var.data_type()
    );

    // Native Term system - no conversion needed
    println!("\n=== Native Term System ===");

    if name_var.is_variable() {
        println!(
            "Name term as variable: {:?} (type: {:?})",
            name_var.name().unwrap_or("unknown"),
            name_var.data_type()
        );
    }

    if wildcard_var.is_variable() {
        println!(
            "Wildcard term as variable: {:?} (type: {:?})",
            wildcard_var.name().unwrap_or("unknown"),
            wildcard_var.data_type()
        );
    }

    println!("\n✅ New Term API successfully demonstrates:");
    println!("   • Term::<T>::var() for typed variables with turbofish syntax");
    println!("   • Term::<Value>::var() for flexible value variables");
    println!("   • Zero-cost abstractions through phantom types (no runtime overhead)");
    println!("   • Type information encoded at compile time");
    println!("   • Clean, ergonomic API with minimal syntax");
    println!("   • Unified Term<T> type for all query operations");
    println!("   • Single Term<T> enum with phantom types for type safety");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_term_api() {
        // Test the new API
        let name_var = Term::<String>::var("name");
        let age_var = Term::<u64>::var("age");
        let any_var = Term::<Value>::var("anything");

        // Test that they have the right types and behaviors
        assert_eq!(name_var.name(), Some("name"));
        assert_eq!(name_var.data_type(), Some(ValueDataType::String));

        assert_eq!(age_var.name(), Some("age"));
        assert_eq!(age_var.data_type(), Some(ValueDataType::UnsignedInt));

        assert_eq!(any_var.name(), Some("anything"));
        assert_eq!(any_var.data_type(), None); // Value type is flexible
    }

    #[test]
    fn test_phantom_types() {
        // Test that phantom types provide zero-cost type information
        let string_var = Term::<String>::var("name");
        let int_var = Term::<u64>::var("age");
        let bool_var = Term::<bool>::var("active");

        // These should have different types at compile time
        assert_eq!(string_var.data_type(), Some(ValueDataType::String));
        assert_eq!(int_var.data_type(), Some(ValueDataType::UnsignedInt));
        assert_eq!(bool_var.data_type(), Some(ValueDataType::Boolean));

        // Type safety is enforced through the type system
        assert!(string_var.is_variable());
        assert!(int_var.is_variable());
        assert!(bool_var.is_variable());
    }

    #[test]
    fn test_unified_term_system() {
        // Test the new Term<T> system uses a single enum with phantom types
        let name_var = Term::<String>::var("name");
        let age_var = Term::<u64>::var("age");
        let wildcard_var = Term::<Value>::var("wildcard");

        // Test that all terms have expected properties
        assert_eq!(name_var.data_type(), Some(ValueDataType::String));
        assert_eq!(age_var.data_type(), Some(ValueDataType::UnsignedInt));
        assert_eq!(wildcard_var.data_type(), None); // Value is flexible
    }
}
