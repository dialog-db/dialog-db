//! Example demonstrating typed variables in dialog-query
//!
//! This example shows how to use the new typed variable system inspired by RhizomeDB
//! to provide compile-time type safety while maintaining compatibility with the
//! existing Variable system.

use dialog_artifacts::{Entity, Value};
use dialog_query::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Clean Variable API Demo ===");

    // New unified variable API - only Variable::<T>::typed() and Variable::<Untyped>::new()
    let name_var = TypedVariable::<String>::new("name");
    let age_var = TypedVariable::<u64>::new("age");

    println!("\n=== Typed Variables with Variable::<T>::typed() ===");
    println!(
        "Name variable: {} (type: {:?})",
        name_var,
        name_var.data_type()
    );
    println!(
        "Age variable: {} (type: {:?})",
        age_var,
        age_var.data_type()
    );

    // More typed variables using the generic constructor
    let active_var = TypedVariable::<bool>::new("active");
    let entity_var = TypedVariable::<Entity>::new("user");
    let score_var = TypedVariable::<f64>::new("score");
    let bytes_var = TypedVariable::<Vec<u8>>::new("data");

    println!(
        "Active variable: {} (type: {:?})",
        active_var,
        active_var.data_type()
    );
    println!(
        "Entity variable: {} (type: {:?})",
        entity_var,
        entity_var.data_type()
    );
    println!(
        "Score variable: {} (type: {:?})",
        score_var,
        score_var.data_type()
    );
    println!(
        "Bytes variable: {} (type: {:?})",
        bytes_var,
        bytes_var.data_type()
    );

    // Untyped variables using Variable::new
    println!("\n=== Untyped Variables with Variable::new ===");
    let any_var = TypedVariable::<Untyped>::new("anything");
    let wildcard_var = TypedVariable::<Untyped>::new("wildcard");

    println!(
        "Any variable: {} (type: {:?})",
        any_var,
        any_var.data_type()
    );
    println!(
        "Wildcard variable: {} (type: {:?})",
        wildcard_var,
        wildcard_var.data_type()
    );

    // Type safety demonstrations
    println!("\n=== Type Safety Checks ===");

    // This works - correct type
    let alice_value = Value::String("Alice".to_string());
    println!(
        "name_var can unify with 'Alice': {}",
        name_var.can_unify_with(&alice_value)
    );

    // This fails - wrong type
    let bool_value = Value::Boolean(true);
    println!(
        "name_var can unify with boolean: {}",
        name_var.can_unify_with(&bool_value)
    );

    // Any variable accepts everything
    println!(
        "any_var can unify with 'Alice': {}",
        any_var.can_unify_with(&alice_value)
    );
    println!(
        "any_var can unify with boolean: {}",
        any_var.can_unify_with(&bool_value)
    );

    // Working with collections of variables
    println!("\n=== Variable Collections ===");
    // Note: Different variable types need to be converted to a common type for collections
    println!(
        "Name variable: {} (type: {:?})",
        name_var,
        name_var.data_type()
    );
    println!(
        "Age variable: {} (type: {:?})",
        age_var,
        age_var.data_type()
    );
    println!(
        "Active variable: {} (type: {:?})",
        active_var,
        active_var.data_type()
    );
    println!(
        "Any variable: {} (type: {:?})",
        any_var,
        any_var.data_type()
    );

    // Compatibility with existing Term system
    println!("\n=== Term Integration ===");
    let name_term: Term<String> = name_var.clone().into();
    let any_term: Term<Untyped> = any_var.clone().into();

    if name_term.is_variable() {
        println!(
            "Name term as variable: {} (type: {:?})",
            name_term.name().unwrap_or("unknown"),
            name_term.data_type()
        );
    }

    if any_term.is_variable() {
        println!(
            "Any term as variable: {} (type: {:?})",
            any_term.name().unwrap_or("unknown"),
            any_term.data_type()
        );
    }

    // API demonstrations
    println!("\n=== API Demonstrations ===");

    println!("Name variable: {}", name_var);
    println!("Age variable: {}", age_var);
    println!("Any variable: {}", any_var);

    println!("\n✅ Clean Variable API successfully demonstrates:");
    println!("   • Single Variable type supporting both typed and untyped variants");
    println!("   • Generic constructor Variable::<T>::typed() for type-safe creation");
    println!("   • Variable::<Untyped>::new() for untyped variables");
    println!("   • Minimal, focused API with only essential constructors");
    println!("   • Strong type safety with compile-time type inference");
    println!("   • Seamless integration with existing Term and query systems");

    Ok(())
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_clean_variable_api() {
        // Test that the clean API works correctly
        let name_var = TypedVariable::<String>::new("name");
        let age_var = TypedVariable::<u64>::new("age");
        let any_var = TypedVariable::<Untyped>::new("any");

        // Test type safety
        assert!(name_var.can_unify_with(&Value::String("test".to_string())));
        assert!(!name_var.can_unify_with(&Value::UnsignedInt(42)));

        assert!(age_var.can_unify_with(&Value::UnsignedInt(42)));
        assert!(!age_var.can_unify_with(&Value::String("test".to_string())));

        // Test untyped variable
        assert!(any_var.can_unify_with(&Value::String("test".to_string())));
        assert!(any_var.can_unify_with(&Value::UnsignedInt(42)));
        assert!(any_var.can_unify_with(&Value::Boolean(true)));

        // Test type information
        assert_eq!(name_var.data_type(), Some(ValueDataType::String));
        assert_eq!(age_var.data_type(), Some(ValueDataType::UnsignedInt));
        assert_eq!(any_var.data_type(), None);
    }

    #[test]
    fn test_variable_collections() {
        // Note: Since typed variables have different types, we can't put them in the same Vec
        // This shows each variable type individually
        let name_var = TypedVariable::<String>::new("name");
        let age_var = TypedVariable::<u64>::new("age");
        let active_var = TypedVariable::<bool>::new("active");
        let wildcard_var = TypedVariable::<Untyped>::new("wildcard");

        let vars = vec![
            (name_var.name().to_string(), name_var.data_type()),
            (age_var.name().to_string(), age_var.data_type()),
            (active_var.name().to_string(), active_var.data_type()),
            (wildcard_var.name().to_string(), wildcard_var.data_type()),
        ];

        assert_eq!(vars.len(), 4);

        // Check that we have a mix of typed and untyped variables
        let typed_count = vars
            .iter()
            .filter(|(_, data_type)| data_type.is_some())
            .count();
        let untyped_count = vars
            .iter()
            .filter(|(_, data_type)| data_type.is_none())
            .count();

        assert_eq!(typed_count, 3);
        assert_eq!(untyped_count, 1);
    }
}
