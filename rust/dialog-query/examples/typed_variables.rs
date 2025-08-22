//! Example demonstrating typed Terms in dialog-query
//!
//! This example shows how to use the new typed Term system
//! to provide compile-time type safety in a unified system.

use dialog_artifacts::{Entity, Value};
use dialog_query::Term;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Clean Term API Demo ===");

    // New unified Term API - Term::<T>::var() for typed variables
    let name_var = Term::<String>::var("name");
    let age_var = Term::<u64>::var("age");

    println!("\n=== Typed Terms with Term::<T>::var() ===");
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

    // More typed terms using the generic constructor
    let active_var = Term::<bool>::var("active");
    let entity_var = Term::<Entity>::var("user");
    let score_var = Term::<f64>::var("score");
    let bytes_var = Term::<Vec<u8>>::var("data");

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
    println!(
        "Score term: {:?} (type: {:?})",
        score_var.name(),
        score_var.data_type()
    );
    println!(
        "Bytes term: {:?} (type: {:?})",
        bytes_var.name(),
        bytes_var.data_type()
    );

    // Flexible terms using Term::<Value>::var()
    println!("\n=== Flexible Value Terms ===");
    let any_var = Term::<Value>::var("anything");
    let wildcard_var = Term::<Value>::var("wildcard");

    println!(
        "Any term: {:?} (type: {:?})",
        any_var.name(),
        any_var.data_type()
    );
    println!(
        "Wildcard term: {:?} (type: {:?})",
        wildcard_var.name(),
        wildcard_var.data_type()
    );

    // Type safety demonstrations
    println!("\n=== Type Safety Checks ===");

    // Type safety is enforced through the type system
    let _alice_value = Value::String("Alice".to_string());
    let _bool_value = Value::Boolean(true);

    println!("Type safety is enforced at compile-time with Term<T>");
    println!("String terms work with string values");
    println!("Value terms work with any value type");

    // Working with collections of terms
    println!("\n=== Term Collections ===");
    // Note: Different term types need to be converted to a common type for collections
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
        "Any term: {:?} (type: {:?})",
        any_var.name(),
        any_var.data_type()
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

    if any_var.is_variable() {
        println!(
            "Any term as variable: {:?} (type: {:?})",
            any_var.name().unwrap_or("unknown"),
            any_var.data_type()
        );
    }

    // API demonstrations
    println!("\n=== API Demonstrations ===");

    println!("Name term: {:?}", name_var.name());
    println!("Age term: {:?}", age_var.name());
    println!("Any term: {:?}", any_var.name());

    println!("\n✅ Clean Term API successfully demonstrates:");
    println!("   • Single Term<T> enum supporting all term types");
    println!("   • Generic constructor Term::<T>::var() for type-safe creation");
    println!("   • Term::<Value>::var() for flexible value terms");
    println!("   • Minimal, focused API with only essential constructors");
    println!("   • Strong type safety with compile-time type inference");
    println!("   • Unified system for all query operations");

    Ok(())
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_clean_term_api() {
        // Test that the clean API works correctly
        let name_var = Term::<String>::var("name");
        let age_var = Term::<u64>::var("age");
        let any_var = Term::<Value>::var("any");

        // Test type information
        assert_eq!(name_var.data_type(), Some(ValueDataType::String));
        assert_eq!(age_var.data_type(), Some(ValueDataType::UnsignedInt));
        assert_eq!(any_var.data_type(), None); // Value is flexible

        // Test variable names
        assert_eq!(name_var.name(), Some("name"));
        assert_eq!(age_var.name(), Some("age"));
        assert_eq!(any_var.name(), Some("any"));
    }

    #[test]
    fn test_term_collections() {
        // Note: Since typed terms have different types, we can't put them in the same Vec
        // This shows each term type individually
        let name_var = Term::<String>::var("name");
        let age_var = Term::<u64>::var("age");
        let active_var = Term::<bool>::var("active");
        let wildcard_var = Term::<Value>::var("wildcard");

        let vars = vec![
            (name_var.name().unwrap().to_string(), name_var.data_type()),
            (age_var.name().unwrap().to_string(), age_var.data_type()),
            (
                active_var.name().unwrap().to_string(),
                active_var.data_type(),
            ),
            (
                wildcard_var.name().unwrap().to_string(),
                wildcard_var.data_type(),
            ),
        ];

        assert_eq!(vars.len(), 4);

        // Check that we have a mix of typed and flexible terms
        let typed_count = vars
            .iter()
            .filter(|(_, data_type)| data_type.is_some())
            .count();
        let flexible_count = vars
            .iter()
            .filter(|(_, data_type)| data_type.is_none())
            .count();

        assert_eq!(typed_count, 3);
        assert_eq!(flexible_count, 1);
    }
}
