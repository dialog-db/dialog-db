//! Type system utilities for dialog-query
//!
//! This module contains traits and implementations for bridging between Rust types
//! and dialog-artifacts ValueDataType system.

use dialog_artifacts::ValueDataType;


/// Trait for types that can be converted to ValueDataType
/// This provides the bridge between Rust types and dialog-artifacts types
pub trait IntoValueDataType {
    fn into_value_data_type() -> Option<ValueDataType>;
}

/// Macro to implement IntoValueDataType for primitive types
macro_rules! impl_into_value_data_type {
    ($rust_type:ty, $value_data_type:expr) => {
        impl IntoValueDataType for $rust_type {
            fn into_value_data_type() -> Option<ValueDataType> {
                Some($value_data_type)
            }
        }
    };
}

// Implement for all supported types
impl_into_value_data_type!(String, ValueDataType::String);
impl_into_value_data_type!(bool, ValueDataType::Boolean);
impl_into_value_data_type!(u128, ValueDataType::UnsignedInt);
impl_into_value_data_type!(u64, ValueDataType::UnsignedInt);
impl_into_value_data_type!(u32, ValueDataType::UnsignedInt);
impl_into_value_data_type!(u16, ValueDataType::UnsignedInt);
impl_into_value_data_type!(u8, ValueDataType::UnsignedInt);
impl_into_value_data_type!(i128, ValueDataType::SignedInt);
impl_into_value_data_type!(i64, ValueDataType::SignedInt);
impl_into_value_data_type!(i32, ValueDataType::SignedInt);
impl_into_value_data_type!(i16, ValueDataType::SignedInt);
impl_into_value_data_type!(i8, ValueDataType::SignedInt);
impl_into_value_data_type!(f64, ValueDataType::Float);
impl_into_value_data_type!(f32, ValueDataType::Float);
impl_into_value_data_type!(Vec<u8>, ValueDataType::Bytes);
impl_into_value_data_type!(dialog_artifacts::Entity, ValueDataType::Entity);
impl_into_value_data_type!(dialog_artifacts::Attribute, ValueDataType::Symbol);

impl IntoValueDataType for dialog_artifacts::Value {
    fn into_value_data_type() -> Option<ValueDataType> {
        // Value is a dynamic type, so we return None to indicate it can hold any type
        None
    }
}


