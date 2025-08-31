//! Type system utilities for dialog-query
//!
//! This module contains traits and implementations for bridging between Rust types
//! and the dialog-artifacts ValueDataType system. The main purpose is to provide
//! compile-time type information that can be used for:
//!
//! - JSON serialization with type annotations
//! - Runtime type checking during pattern matching
//! - Query optimization based on type constraints
//!
//! The core insight is that Rust's type system provides static type information
//! that we can reflect into the dynamic ValueDataType enum used by dialog-artifacts.

use crate::artifact::{Attribute, Entity, Value, ValueDataType};

/// Trait for types that can provide ValueDataType metadata
///
/// This trait bridges between Rust's compile-time type system and dialog-artifacts'
/// runtime type system. It allows Term<T> to know what ValueDataType corresponds
/// to its type parameter T.
///
/// # Key Design Decision
/// Returns `Option<ValueDataType>` rather than `ValueDataType` to handle the special
/// case of `Value` type, which is itself a dynamic type that can hold any value.
/// For `Value`, we return `None` to indicate "any type".
///
/// # Usage
/// ```rust
/// use dialog_query::types::IntoValueDataType;
/// use dialog_query::artifact::{Value, ValueDataType};
///
/// // For concrete types
/// assert_eq!(String::into_value_data_type(), Some(ValueDataType::String));
/// assert_eq!(u32::into_value_data_type(), Some(ValueDataType::UnsignedInt));
///
/// // For the dynamic Value type
/// assert_eq!(Value::into_value_data_type(), None); // Can hold any type
/// ```
pub trait IntoValueDataType {
    /// Get the ValueDataType that corresponds to this Rust type
    ///
    /// Returns:
    /// - `Some(ValueDataType::X)` for concrete types like String, u32, etc.
    /// - `None` for Value type (indicates it can hold any type)
    fn into_value_data_type() -> Option<ValueDataType>;
}

/// Macro to implement IntoValueDataType for primitive types
///
/// This macro reduces boilerplate for implementing the trait on standard Rust types.
/// Each implementation returns Some(ValueDataType) for the appropriate variant.
macro_rules! impl_into_value_data_type {
    ($rust_type:ty, $value_data_type:expr) => {
        impl IntoValueDataType for $rust_type {
            fn into_value_data_type() -> Option<ValueDataType> {
                Some($value_data_type)
            }
        }
    };
}

// Implement IntoValueDataType for all supported primitive and dialog-artifacts types
//
// These implementations provide the mapping between Rust types and ValueDataType variants.
// Note that all unsigned integer types map to UnsignedInt, and all signed integers map
// to SignedInt, regardless of their specific bit width.

// String type
impl_into_value_data_type!(String, ValueDataType::String);

// Boolean type
impl_into_value_data_type!(bool, ValueDataType::Boolean);

// Unsigned integer types (all map to UnsignedInt)
impl_into_value_data_type!(u128, ValueDataType::UnsignedInt);
impl_into_value_data_type!(u64, ValueDataType::UnsignedInt);
impl_into_value_data_type!(u32, ValueDataType::UnsignedInt);
impl_into_value_data_type!(u16, ValueDataType::UnsignedInt);
impl_into_value_data_type!(u8, ValueDataType::UnsignedInt);

// Signed integer types (all map to SignedInt)
impl_into_value_data_type!(i128, ValueDataType::SignedInt);
impl_into_value_data_type!(i64, ValueDataType::SignedInt);
impl_into_value_data_type!(i32, ValueDataType::SignedInt);
impl_into_value_data_type!(i16, ValueDataType::SignedInt);
impl_into_value_data_type!(i8, ValueDataType::SignedInt);

// Floating point types (all map to Float)
impl_into_value_data_type!(f64, ValueDataType::Float);
impl_into_value_data_type!(f32, ValueDataType::Float);

// Binary data
impl_into_value_data_type!(Vec<u8>, ValueDataType::Bytes);

// Dialog-artifacts specific types
impl_into_value_data_type!(Entity, ValueDataType::Entity);
impl_into_value_data_type!(Attribute, ValueDataType::Symbol);

/// Special implementation for Value type
///
/// Value is the dynamic type that can hold any of the other types at runtime.
/// Since it's not statically typed to any specific ValueDataType variant,
/// we return None to indicate "this can be any type".
///
/// This is used by Term<Value> to indicate untyped variables in JSON serialization.
impl IntoValueDataType for Value {
    fn into_value_data_type() -> Option<ValueDataType> {
        // Value is a dynamic type, so we return None to indicate it can hold any type
        // This makes Term<Value> variables serialize without type information
        None
    }
}

pub trait Scalar: IntoValueDataType + Clone + std::fmt::Debug + 'static {
    /// Can be used to convert scalars into boxed value. It is intentionally
    /// different from `From<Scalar> impl Value` to avoid unintentional
    /// type erasure.
    fn as_value(&self) -> Value;
}

impl Scalar for bool {
    fn as_value(&self) -> Value {
        Value::from(self.to_owned())
    }
}

impl Scalar for String {
    fn as_value(&self) -> Value {
        Value::from(self.to_owned())
    }
}

impl Scalar for u16 {
    fn as_value(&self) -> Value {
        Value::from(self.to_owned())
    }
}
impl Scalar for u32 {
    fn as_value(&self) -> Value {
        Value::from(self.to_owned())
    }
}

impl Scalar for u64 {
    fn as_value(&self) -> Value {
        Value::from(self.to_owned())
    }
}

impl Scalar for u128 {
    fn as_value(&self) -> Value {
        Value::from(self.to_owned())
    }
}

impl Scalar for i16 {
    fn as_value(&self) -> Value {
        Value::from(self.to_owned())
    }
}

impl Scalar for i32 {
    fn as_value(&self) -> Value {
        Value::from(self.to_owned())
    }
}

impl Scalar for i64 {
    fn as_value(&self) -> Value {
        Value::from(self.to_owned())
    }
}
impl Scalar for i128 {
    fn as_value(&self) -> Value {
        Value::from(self.to_owned())
    }
}

impl Scalar for f32 {
    fn as_value(&self) -> Value {
        Value::from(self.to_owned())
    }
}
impl Scalar for f64 {
    fn as_value(&self) -> Value {
        Value::from(self.to_owned())
    }
}
impl Scalar for Entity {
    fn as_value(&self) -> Value {
        Value::from(self.to_owned())
    }
}
impl Scalar for Attribute {
    fn as_value(&self) -> Value {
        Value::from(self.to_owned())
    }
}
impl Scalar for Vec<u8> {
    fn as_value(&self) -> Value {
        Value::from(self.to_owned())
    }
}
impl Scalar for Value {
    fn as_value(&self) -> Value {
        self.to_owned()
    }
}
