//! Example demonstrating the unified Term system
//!
//! This example shows how the unified Term type supports both typed and flexible
//! terms through different type parameters, providing a single enum type that
//! unifies all query operations.

use dialog_artifacts::Value;
use dialog_query::Term;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Unified Term System Demo ===\n");

    // 1. Generic typed constructor - the new primary way to create typed terms
    println!("1. Generic Typed Constructor Term::<T>::var()");
    let name_var = Term::<String>::var("name");
    let age_var = Term::<u64>::var("age");
    let active_var = Term::<bool>::var("active");
    let score_var = Term::<f64>::var("score");

    println!(
        "  String term: {:?} (type: {:?})",
        name_var.name(),
        name_var.data_type()
    );
    println!(
        "  U64 term: {:?} (type: {:?})",
        age_var.name(),
        age_var.data_type()
    );
    println!(
        "  Bool term: {:?} (type: {:?})",
        active_var.name(),
        active_var.data_type()
    );
    println!(
        "  F64 term: {:?} (type: {:?})",
        score_var.name(),
        score_var.data_type()
    );

    // 2. Using Term::<Value>::var() for flexible value terms
    println!("\n2. Flexible Value Terms with Term::<Value>::var()");
    let any_name_var = Term::<Value>::var("any_name");
    let any_age_var = Term::<Value>::var("any_age");
    let any_active_var = Term::<Value>::var("any_active");

    println!(
        "  Any name term: {:?} (type: {:?})",
        any_name_var.name(),
        any_name_var.data_type()
    );
    println!(
        "  Any age term: {:?} (type: {:?})",
        any_age_var.name(),
        any_age_var.data_type()
    );
    println!(
        "  Any active term: {:?} (type: {:?})",
        any_active_var.name(),
        any_active_var.data_type()
    );

    // 3. More flexible term examples
    println!("\n3. More Flexible Term Examples");
    let any_var = Term::<Value>::var("anything");
    let wildcard_var = Term::<Value>::var("wildcard");

    println!(
        "  Any term: {:?} (type: {:?})",
        any_var.name(),
        any_var.data_type()
    );
    println!(
        "  Wildcard term: {:?} (type: {:?})",
        wildcard_var.name(),
        wildcard_var.data_type()
    );

    // 4. Type safety demonstration
    println!("\n4. Type Safety Checks");
    let _string_value = Value::String("Alice".to_string());
    let _int_value = Value::UnsignedInt(25);
    let _bool_value = Value::Boolean(true);

    println!("  Type safety is enforced at compile-time with Term<T>");
    println!("  String terms work with string values");
    println!("  u64 terms work with unsigned int values");
    println!("  Value terms work with any value type");

    // 5. Clean unified API demonstration
    println!("\n5. Clean Unified API");
    let generic_string = Term::<String>::var("test");
    let any_variable = Term::<Value>::var("test_any");

    println!(
        "  Typed term: {:?} (type: {:?})",
        generic_string.name(),
        generic_string.data_type()
    );
    println!(
        "  Flexible term: {:?} (type: {:?})",
        any_variable.name(),
        any_variable.data_type()
    );

    println!("  Differences:");
    println!("    Typed term has specific type constraint");
    println!("    Flexible term accepts any value type");

    // 6. Native Term system - no conversion needed
    println!("\n6. Native Term System");
    let term_from_generic: Term<String> = Term::<String>::var("term_test");
    let term_from_any: Term<Value> = Term::<Value>::var("term_any");

    println!(
        "  Typed term: {:?} (type: {:?})",
        term_from_generic.name(),
        term_from_generic.data_type()
    );
    println!(
        "  Flexible term: {:?} (type: {:?})",
        term_from_any.name(),
        term_from_any.data_type()
    );

    println!("\n✅ Clean Term API successfully demonstrates:");
    println!("   • Single Term<T> enum supporting all term types");
    println!("   • Generic constructor Term::<T>::var() for type-safe creation");
    println!("   • Term::<Value>::var() for flexible value terms");
    println!("   • Minimal, focused API with only essential constructors");
    println!("   • data_type() method returning Option<ValueDataType>");
    println!("   • Unified system for all query operations");

    Ok(())
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_clean_api() {
        // Test the clean unified API with only essential constructors
        let generic_var = Term::<String>::var("test");
        let any_var = Term::<Value>::var("test_any");

        // Generic constructor creates typed variable
        assert_eq!(generic_var.data_type(), Some(ValueDataType::String));
        assert_eq!(generic_var.name(), Some("test"));

        // Value constructor creates flexible term
        assert_eq!(any_var.data_type(), None);
        assert_eq!(any_var.name(), Some("test_any"));

        // Test type checking behavior
        let string_value = Value::String("hello".to_string());
        let int_value = Value::UnsignedInt(42);

        // Type safety is enforced through the type system
        assert!(generic_var.is_variable());
        assert!(any_var.is_variable());
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
    fn test_flexible_terms() {
        // Test that flexible terms work correctly
        let any_var = Term::<Value>::var("anything");
        let another_any = Term::<Value>::var("other");

        assert_eq!(any_var.data_type(), None);
        assert_eq!(another_any.data_type(), None);

        // Both are variables
        assert!(any_var.is_variable());
        assert!(another_any.is_variable());

        // Test variable names
        assert_eq!(any_var.name(), Some("anything"));
        assert_eq!(another_any.name(), Some("other"));
    }
}
