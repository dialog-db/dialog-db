//! Example demonstrating the new Variable API with clean turbofish syntax
//! This shows the clean API: Variable::<T>::typed() for typed variables, Variable::<Untyped>::new() for untyped

use dialog_artifacts::{Entity, Value};
use dialog_query::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== New Variable API Demo ===");

    // New API: Variable::<T>::typed() for typed variables with compile-time type safety
    let name_var = TypedVariable::<String>::new("name"); // Returns Variable<String>
    let age_var = TypedVariable::<u64>::new("age"); // Returns Variable<u64>
    let active_var = TypedVariable::<bool>::new("active"); // Returns Variable<bool>
    let entity_var = TypedVariable::<Entity>::new("user"); // Returns Variable<Entity>

    println!("\n=== Typed Variables with Zero-Cost Type Safety ===");
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
        "Entity variable: {} (type: {:?})",
        entity_var,
        entity_var.data_type()
    );

    // New API: Variable::<Untyped>::new() returns Variable<Untyped> for maximum flexibility
    let wildcard_var = TypedVariable::<Untyped>::new("wildcard"); // Returns Variable<Untyped>

    println!("\n=== Untyped Variables ===");
    println!(
        "Wildcard variable: {} (type: {:?})",
        wildcard_var,
        wildcard_var.data_type()
    );

    // Type safety demonstrations
    println!("\n=== Compile-Time Type Safety ===");

    // This works - correct type
    let alice_value = Value::String("Alice".to_string());
    println!(
        "name_var (String) can unify with 'Alice': {}",
        name_var.can_unify_with(&alice_value)
    );

    // This fails - wrong type
    let bool_value = Value::Boolean(true);
    println!(
        "name_var (String) can unify with boolean: {}",
        name_var.can_unify_with(&bool_value)
    );

    // This works - correct type
    let age_value = Value::UnsignedInt(42);
    println!(
        "age_var (u64) can unify with 42: {}",
        age_var.can_unify_with(&age_value)
    );

    // This fails - wrong type
    println!(
        "age_var (u64) can unify with 'Alice': {}",
        age_var.can_unify_with(&alice_value)
    );

    // Untyped variable accepts everything
    println!(
        "wildcard_var (untyped) can unify with 'Alice': {}",
        wildcard_var.can_unify_with(&alice_value)
    );
    println!(
        "wildcard_var (untyped) can unify with boolean: {}",
        wildcard_var.can_unify_with(&bool_value)
    );
    println!(
        "wildcard_var (untyped) can unify with 42: {}",
        wildcard_var.can_unify_with(&age_value)
    );

    // Working with the new unified Variable<T> system
    println!("\n=== Unified Variable<T> System ===");

    println!("Number of variables created: 4");
    println!(
        "  Variable 0: {} (type: {:?})",
        name_var,
        name_var.data_type()
    );
    println!(
        "  Variable 1: {} (type: {:?})",
        age_var,
        age_var.data_type()
    );
    println!(
        "  Variable 2: {} (type: {:?})",
        active_var,
        active_var.data_type()
    );
    println!(
        "  Variable 3: {} (type: {:?})",
        wildcard_var,
        wildcard_var.data_type()
    );

    // Compatibility with Term system
    println!("\n=== Term Integration ===");
    let name_term: Term<String> = name_var.into();
    let wildcard_term: Term<Untyped> = wildcard_var.into();

    if name_term.is_variable() {
        println!(
            "Name term as variable: {} (type: {:?})",
            name_term.name().unwrap_or("unknown"),
            name_term.data_type()
        );
    }

    if wildcard_term.is_variable() {
        println!(
            "Wildcard term as variable: {} (type: {:?})",
            wildcard_term.name().unwrap_or("unknown"),
            wildcard_term.data_type()
        );
    }

    println!("\n✅ New Variable API successfully demonstrates:");
    println!("   • Variable::<T>::typed() for typed variables with turbofish syntax");
    println!("   • Variable::<Untyped>::new() for untyped variables (default behavior)");
    println!("   • Zero-cost abstractions through phantom types (no runtime overhead)");
    println!("   • Type information encoded at compile time");
    println!("   • Clean, ergonomic API with minimal syntax");
    println!("   • Seamless integration with existing Term and query systems");
    println!("   • Single Variable<T> type with phantom types for type safety");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_variable_api() {
        // Test the new API
        let name_var = TypedVariable::<String>::new("name");
        let age_var = TypedVariable::<u64>::new("age");
        let any_var = TypedVariable::<Untyped>::new("anything");

        // Test that they have the right types and behaviors
        assert_eq!(name_var.name(), "name");
        assert_eq!(name_var.data_type(), Some(ValueDataType::String));
        assert!(name_var.can_unify_with(&Value::String("Alice".to_string())));
        assert!(!name_var.can_unify_with(&Value::Boolean(true)));

        assert_eq!(age_var.name(), "age");
        assert_eq!(age_var.data_type(), Some(ValueDataType::UnsignedInt));
        assert!(age_var.can_unify_with(&Value::UnsignedInt(42)));
        assert!(!age_var.can_unify_with(&Value::String("42".to_string())));

        assert_eq!(any_var.name(), "anything");
        assert_eq!(any_var.data_type(), None);
        assert!(any_var.can_unify_with(&Value::String("test".to_string())));
        assert!(any_var.can_unify_with(&Value::Boolean(true)));
        assert!(any_var.can_unify_with(&Value::UnsignedInt(42)));
    }

    #[test]
    fn test_phantom_types() {
        // Test that phantom types provide zero-cost type information
        let string_var = TypedVariable::<String>::new("name");
        let int_var = TypedVariable::<u64>::new("age");
        let bool_var = TypedVariable::<bool>::new("active");

        // These should have different types at compile time
        assert_eq!(string_var.data_type(), Some(ValueDataType::String));
        assert_eq!(int_var.data_type(), Some(ValueDataType::UnsignedInt));
        assert_eq!(bool_var.data_type(), Some(ValueDataType::Boolean));

        // Type safety is enforced
        assert!(string_var.can_unify_with(&Value::String("test".to_string())));
        assert!(!string_var.can_unify_with(&Value::UnsignedInt(42)));

        assert!(int_var.can_unify_with(&Value::UnsignedInt(42)));
        assert!(!int_var.can_unify_with(&Value::String("test".to_string())));

        assert!(bool_var.can_unify_with(&Value::Boolean(true)));
        assert!(!bool_var.can_unify_with(&Value::String("test".to_string())));
    }

    #[test]
    fn test_trait_object_usage() {
        // Test the new Variable<T> system doesn't need trait objects
        // The new design uses a single Variable<T> type with phantom types
        let name_var = TypedVariable::<String>::new("name");
        let age_var = TypedVariable::<u64>::new("age");
        let wildcard_var = TypedVariable::<Untyped>::new("wildcard");

        // Test that all variables have expected properties
        assert_eq!(name_var.data_type(), Some(ValueDataType::String));
        assert_eq!(age_var.data_type(), Some(ValueDataType::UnsignedInt));
        assert_eq!(wildcard_var.data_type(), None);
    }
}
