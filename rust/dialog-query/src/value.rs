//! Type casting utilities for formula value conversion
//!
//! This module provides the `Cast` trait and its implementations for converting
//! between Dialog's `Value` type and native Rust types during formula evaluation.
//!
//! # Overview
//!
//! The `Cast` trait enables type-safe conversion from the generic `Value` enum
//! to specific Rust types. This is essential for formulas that need to work with
//! typed data while maintaining compatibility with Dialog's dynamic type system.
//!
//! # Supported Types
//!
//! Currently supported conversions:
//! - `u32` - From `Value::UnsignedInt`
//! - `i32` - From `Value::SignedInt` or `Value::UnsignedInt`
//! - `f64` - From `Value::Float`, `Value::SignedInt`, or `Value::UnsignedInt`
//! - `String` - From `Value::String`
//! - `bool` - From `Value::String` ("true"/"false") or integers (0/non-zero)
//!
//! # Example
//!
//! ```ignore
//! let value = Value::UnsignedInt(42);
//! let number: u32 = u32::try_cast(&value)?;
//! assert_eq!(number, 42);
//! ```

use crate::formula::FormulaEvaluationError;
pub use crate::Value;

/// Trait for casting values from Dialog's generic Value type to specific Rust types
///
/// This trait provides a unified interface for type conversion with proper error
/// handling. Implementations should return `TypeMismatch` errors when the source
/// value cannot be meaningfully converted to the target type.
pub trait Cast: Sized {
    /// Attempt to cast a Value to this type
    ///
    /// # Arguments
    /// * `value` - The Value to cast
    ///
    /// # Returns
    /// * `Ok(Self)` - The successfully cast value
    /// * `Err(TypeMismatch)` - If the value cannot be cast to this type
    fn try_cast(value: &Value) -> Result<Self, FormulaEvaluationError>;
}

/// Cast implementation for unsigned 32-bit integers
///
/// Converts from `Value::UnsignedInt`, truncating to 32 bits if necessary.
impl Cast for u32 {
    fn try_cast(value: &Value) -> Result<Self, FormulaEvaluationError> {
        match value {
            Value::UnsignedInt(n) => Ok(*n as u32),
            _ => Err(FormulaEvaluationError::TypeMismatch {
                expected: "u32".into(),
                actual: value.data_type().to_string(),
            }),
        }
    }
}

/// Cast implementation for signed 32-bit integers
///
/// Converts from:
/// - `Value::SignedInt` - Direct conversion
/// - `Value::UnsignedInt` - With potential overflow
impl Cast for i32 {
    fn try_cast(value: &Value) -> Result<Self, FormulaEvaluationError> {
        match value {
            Value::SignedInt(n) => Ok(*n as i32),
            Value::UnsignedInt(n) => Ok(*n as i32),
            _ => Err(FormulaEvaluationError::TypeMismatch {
                expected: "i32".into(),
                actual: value.data_type().to_string(),
            }),
        }
    }
}

/// Cast implementation for 64-bit floating point numbers
///
/// Converts from:
/// - `Value::Float` - Direct conversion
/// - `Value::SignedInt` - Lossless conversion
/// - `Value::UnsignedInt` - Potentially lossy for very large values
impl Cast for f64 {
    fn try_cast(value: &Value) -> Result<Self, FormulaEvaluationError> {
        match value {
            Value::Float(f) => Ok(*f),
            Value::SignedInt(i) => Ok(*i as f64),
            Value::UnsignedInt(u) => Ok(*u as f64),
            _ => Err(FormulaEvaluationError::TypeMismatch {
                expected: "f64".into(),
                actual: value.data_type().to_string(),
            }),
        }
    }
}

/// Cast implementation for strings
///
/// Converts from `Value::String` by cloning the string value.
impl Cast for String {
    fn try_cast(value: &Value) -> Result<Self, FormulaEvaluationError> {
        match value {
            Value::String(s) => Ok(s.clone()),
            _ => Err(FormulaEvaluationError::TypeMismatch {
                expected: "String".into(),
                actual: value.data_type().to_string(),
            }),
        }
    }
}

/// Cast implementation for booleans
///
/// Since Dialog's Value type doesn't have a native boolean variant, this implementation
/// provides conversions from:
/// - `Value::String` - "true" → true, "false" → false
/// - `Value::UnsignedInt` - 0 → false, non-zero → true
/// - `Value::SignedInt` - 0 → false, non-zero → true
///
/// This follows common conventions for boolean representation in databases
/// and programming languages.
impl Cast for bool {
    fn try_cast(value: &Value) -> Result<Self, FormulaEvaluationError> {
        match value {
            Value::String(s) => match s.as_str() {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => Err(FormulaEvaluationError::TypeMismatch {
                    expected: "bool".into(),
                    actual: format!("String({})", s),
                }),
            },
            Value::UnsignedInt(n) => Ok(*n != 0),
            Value::SignedInt(n) => Ok(*n != 0),
            _ => Err(FormulaEvaluationError::TypeMismatch {
                expected: "bool".into(),
                actual: value.data_type().to_string(),
            }),
        }
    }
}

// Additional implementations for common types can be added here:
// - u64, i64 for larger integers
// - Vec<T> for array types
// - Custom domain types (Entity, Attribute, etc.)

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u32_cast() {
        let value = Value::UnsignedInt(42);
        assert_eq!(u32::try_cast(&value).unwrap(), 42);

        let value = Value::String("not a number".to_string());
        assert!(u32::try_cast(&value).is_err());
    }

    #[test]
    fn test_i32_cast() {
        let value = Value::SignedInt(-10);
        assert_eq!(i32::try_cast(&value).unwrap(), -10);

        let value = Value::UnsignedInt(42);
        assert_eq!(i32::try_cast(&value).unwrap(), 42);
    }

    #[test]
    fn test_f64_cast() {
        let value = Value::Float(3.14);
        assert_eq!(f64::try_cast(&value).unwrap(), 3.14);

        let value = Value::SignedInt(-10);
        assert_eq!(f64::try_cast(&value).unwrap(), -10.0);

        let value = Value::UnsignedInt(42);
        assert_eq!(f64::try_cast(&value).unwrap(), 42.0);
    }

    #[test]
    fn test_string_cast() {
        let value = Value::String("hello".to_string());
        assert_eq!(String::try_cast(&value).unwrap(), "hello");

        let value = Value::UnsignedInt(42);
        assert!(String::try_cast(&value).is_err());
    }

    #[test]
    fn test_bool_cast() {
        // String conversions
        let value = Value::String("true".to_string());
        assert_eq!(bool::try_cast(&value).unwrap(), true);

        let value = Value::String("false".to_string());
        assert_eq!(bool::try_cast(&value).unwrap(), false);

        let value = Value::String("yes".to_string());
        assert!(bool::try_cast(&value).is_err());

        // Integer conversions
        let value = Value::UnsignedInt(1);
        assert_eq!(bool::try_cast(&value).unwrap(), true);

        let value = Value::UnsignedInt(0);
        assert_eq!(bool::try_cast(&value).unwrap(), false);

        let value = Value::SignedInt(-1);
        assert_eq!(bool::try_cast(&value).unwrap(), true);

        let value = Value::SignedInt(0);
        assert_eq!(bool::try_cast(&value).unwrap(), false);
    }

    #[test]
    fn test_type_mismatch_errors() {
        let string_val = Value::String("hello".to_string());

        let result = u32::try_cast(&string_val);
        assert!(matches!(
            result,
            Err(FormulaEvaluationError::TypeMismatch { expected, actual })
            if expected == "u32" && actual == "String"
        ));

        let result = f64::try_cast(&string_val);
        assert!(matches!(
            result,
            Err(FormulaEvaluationError::TypeMismatch { expected, actual })
            if expected == "f64" && actual == "String"
        ));
    }
}
