//! Example demonstrating the unified Variable system
//!
//! This example shows how the unified Variable type supports both typed and untyped
//! variables through different constructor patterns, providing a single type that
//! unifies the previous Variable and TypedVariable approaches.

use dialog_artifacts::Value;
use dialog_query::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Unified Variable System Demo ===\n");

    // 1. Generic typed constructor - the new primary way to create typed variables
    println!("1. Generic Typed Constructor Variable::<T>::typed()");
    let name_var = Term::<String>::var("name");
    let age_var = Term::<u64>::var("age");
    let active_var = Term::<bool>::var("active");
    let score_var = Term::<f64>::var("score");

    println!(
        "  String variable: {} (type: {:?})",
        name_var,
        name_var.data_type()
    );
    println!(
        "  U64 variable: {} (type: {:?})",
        age_var,
        age_var.data_type()
    );
    println!(
        "  Bool variable: {} (type: {:?})",
        active_var,
        active_var.data_type()
    );
    println!(
        "  F64 variable: {} (type: {:?})",
        score_var,
        score_var.data_type()
    );

    // 2. Using Variable::new for untyped variables
    println!("\n2. Untyped Variables with Variable::new");
    let any_name_var = Term::<Untyped>::var("any_name");
    let any_age_var = Term::<Untyped>::var("any_age");
    let any_active_var = Term::<Untyped>::var("any_active");

    println!(
        "  Any name variable: {} (type: {:?})",
        any_name_var,
        any_name_var.data_type()
    );
    println!(
        "  Any age variable: {} (type: {:?})",
        any_age_var,
        any_age_var.data_type()
    );
    println!(
        "  Any active variable: {} (type: {:?})",
        any_active_var,
        any_active_var.data_type()
    );

    // 3. More untyped variable examples
    println!("\n3. More Untyped Variable Examples");
    let any_var = Term::<Untyped>::var("anything");
    let wildcard_var = Term::<Untyped>::var("wildcard");

    println!(
        "  Any variable: {} (type: {:?})",
        any_var,
        any_var.data_type()
    );
    println!(
        "  Wildcard variable: {} (type: {:?})",
        wildcard_var,
        wildcard_var.data_type()
    );

    // 4. Type safety demonstration
    println!("\n4. Type Safety Checks");
    let string_value = Value::String("Alice".to_string());
    let int_value = Value::UnsignedInt(25);
    let _bool_value = Value::Boolean(true);

    println!(
        "  name_var (String) can unify with 'Alice': {}",
        name_var.can_unify_with(&string_value)
    );
    println!(
        "  name_var (String) can unify with 25: {}",
        name_var.can_unify_with(&int_value)
    );
    println!(
        "  age_var (u64) can unify with 25: {}",
        age_var.can_unify_with(&int_value)
    );
    println!(
        "  age_var (u64) can unify with 'Alice': {}",
        age_var.can_unify_with(&string_value)
    );
    println!(
        "  any_var (Any) can unify with 'Alice': {}",
        any_var.can_unify_with(&string_value)
    );
    println!(
        "  any_var (Any) can unify with 25: {}",
        any_var.can_unify_with(&int_value)
    );

    // 5. Clean unified API demonstration
    println!("\n5. Clean Unified API");
    let generic_string = Term::<String>::var("test");
    let any_variable = Term::<Untyped>::var("test_any");

    println!(
        "  Typed variable: {} (type: {:?})",
        generic_string,
        generic_string.data_type()
    );
    println!(
        "  Untyped variable: {} (type: {:?})",
        any_variable,
        any_variable.data_type()
    );

    println!("  Differences:");
    println!("    Typed variable has specific type constraint");
    println!("    Untyped variable accepts any value type");

    // 6. Integration with Term system
    println!("\n6. Term System Integration");
    let term_from_generic: Term<String> = Term::<String>::var("term_test");
    let term_from_any: Term<Untyped> = Term::<Untyped>::var("term_any");

    println!(
        "  Term from generic variable: {} (type: {:?})",
        term_from_generic,
        term_from_generic.data_type()
    );
    println!(
        "  Term from Any variable: {} (type: {:?})",
        term_from_any,
        term_from_any.data_type()
    );

    println!("\n✅ Clean Variable API successfully demonstrates:");
    println!("   • Single Variable type supporting both typed and untyped variants");
    println!("   • Generic constructor Variable::<T>::typed() for type-safe creation");
    println!("   • Variable::<Untyped>::var() for untyped variables");
    println!("   • Minimal, focused API with only essential constructors");
    println!("   • data_type() method returning Option<ValueDataType>");
    println!("   • Seamless integration with existing Term and query systems");

    Ok(())
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_clean_api() {
        // Test the clean unified API with only essential constructors
        let generic_var = Term::<String>::var("test");
        let any_var = Term::<Untyped>::var("test_any");

        // Generic constructor creates typed variable
        assert_eq!(generic_var.data_type(), Some(ValueDataType::String));
        assert_eq!(generic_var.name(), Some("test"));

        // Any constructor creates untyped variable
        assert_eq!(any_var.data_type(), None);
        assert_eq!(any_var.name(), Some("test_any"));

        // Test type checking behavior
        let string_value = Value::String("hello".to_string());
        let int_value = Value::UnsignedInt(42);

        // Typed variable only accepts correct type
        assert!(generic_var.can_unify_with(&string_value));
        assert!(!generic_var.can_unify_with(&int_value));

        // Any variable accepts all types
        assert!(any_var.can_unify_with(&string_value));
        assert!(any_var.can_unify_with(&int_value));
    }

    #[test]
    fn test_type_inference() {
        // Test that the generic constructor correctly infers types
        let string_var = Term::<String>::var("str");
        let u64_var = Term::<u64>::var("num");
        let bool_var = Term::<bool>::var("flag");
        let f64_var = Term::<f64>::var("score");
        let bytes_var = Term::<Vec<u8>>::var("data");

        assert_eq!(string_var.data_type(), Some(ValueDataType::String));
        assert_eq!(u64_var.data_type(), Some(ValueDataType::UnsignedInt));
        assert_eq!(bool_var.data_type(), Some(ValueDataType::Boolean));
        assert_eq!(f64_var.data_type(), Some(ValueDataType::Float));
        assert_eq!(bytes_var.data_type(), Some(ValueDataType::Bytes));
    }

    #[test]
    fn test_untyped_variables() {
        // Test that untyped variables work correctly
        let any_var = Term::<Untyped>::var("anything");
        let another_any = Term::<Untyped>::var("other");

        assert_eq!(any_var.data_type(), None);
        assert_eq!(another_any.data_type(), None);

        // Both should unify with any value
        let values = vec![
            Value::String("test".to_string()),
            Value::UnsignedInt(42),
            Value::Boolean(true),
        ];

        for value in &values {
            assert!(any_var.can_unify_with(value));
            assert!(another_any.can_unify_with(value));
        }
    }
}
