/// Recommended implementation for type-preserving attributes
///
/// This example shows how to integrate the enum approach with the existing
/// dialog-query codebase to achieve the goal pattern.
use std::marker::PhantomData;

// Import types from the actual codebase
use dialog_query::artifact::{Entity, Value};
use dialog_query::types::Scalar;
use dialog_query::{Cardinality, Type};

// Re-define Attribute to match existing structure
#[derive(Clone, Debug)]
pub struct Attribute<T: Scalar> {
    pub namespace: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub cardinality: Cardinality,
    pub marker: PhantomData<T>,
}

impl<T: Scalar> Attribute<T> {
    pub fn new(namespace: &'static str, name: &'static str, description: &'static str) -> Self {
        Self {
            namespace,
            name,
            description,
            cardinality: Cardinality::One,
            marker: PhantomData,
        }
    }

    pub fn the(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }

    /// Get the data type for this attribute's type parameter T
    pub fn data_type(&self) -> Option<Type> {
        T::into_value_data_type()
    }
}

// ================================================================================================
// RECOMMENDED APPROACH: Enum-based TypedAttribute
// ================================================================================================

/// Enum that holds different Attribute<T> variants while preserving type info
///
/// This enum covers all the major types supported by the dialog-artifacts Type.
/// Each variant preserves the exact type information and allows safe downcasting.
#[derive(Debug, Clone)]
pub enum TypedAttribute {
    // Primitive types
    String(Attribute<String>),
    Boolean(Attribute<bool>),

    // Integer types (using the most common ones)
    UnsignedInt(Attribute<u32>),
    SignedInt(Attribute<i32>),

    // Floating point
    Float(Attribute<f64>),

    // Binary data
    Bytes(Attribute<Vec<u8>>),

    // Dialog-specific types
    Entity(Attribute<Entity>),
    Symbol(Attribute<dialog_query::artifact::Attribute>),

    // Dynamic type (for backward compatibility)
    Value(Attribute<Value>),
}

impl TypedAttribute {
    /// Get the fully qualified name of this attribute
    pub fn the(&self) -> String {
        match self {
            TypedAttribute::String(attr) => attr.the(),
            TypedAttribute::Boolean(attr) => attr.the(),
            TypedAttribute::UnsignedInt(attr) => attr.the(),
            TypedAttribute::SignedInt(attr) => attr.the(),
            TypedAttribute::Float(attr) => attr.the(),
            TypedAttribute::Bytes(attr) => attr.the(),
            TypedAttribute::Entity(attr) => attr.the(),
            TypedAttribute::Symbol(attr) => attr.the(),
            TypedAttribute::Value(attr) => attr.the(),
        }
    }

    /// Get the data type for this attribute
    pub fn data_type(&self) -> Option<Type> {
        match self {
            TypedAttribute::String(_) => Some(Type::String),
            TypedAttribute::Boolean(_) => Some(Type::Boolean),
            TypedAttribute::UnsignedInt(_) => Some(Type::UnsignedInt),
            TypedAttribute::SignedInt(_) => Some(Type::SignedInt),
            TypedAttribute::Float(_) => Some(Type::Float),
            TypedAttribute::Bytes(_) => Some(Type::Bytes),
            TypedAttribute::Entity(_) => Some(Type::Entity),
            TypedAttribute::Symbol(_) => Some(Type::Symbol),
            TypedAttribute::Value(_) => None, // Value can hold any type
        }
    }

    /// Get the cardinality of this attribute
    pub fn cardinality(&self) -> Cardinality {
        match self {
            TypedAttribute::String(attr) => attr.cardinality,
            TypedAttribute::Boolean(attr) => attr.cardinality,
            TypedAttribute::UnsignedInt(attr) => attr.cardinality,
            TypedAttribute::SignedInt(attr) => attr.cardinality,
            TypedAttribute::Float(attr) => attr.cardinality,
            TypedAttribute::Bytes(attr) => attr.cardinality,
            TypedAttribute::Entity(attr) => attr.cardinality,
            TypedAttribute::Symbol(attr) => attr.cardinality,
            TypedAttribute::Value(attr) => attr.cardinality,
        }
    }

    // =============================================================================================
    // Safe extraction methods - these provide type-safe access to the underlying Attribute<T>
    // =============================================================================================

    pub fn as_string(&self) -> Option<&Attribute<String>> {
        match self {
            TypedAttribute::String(attr) => Some(attr),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<&Attribute<bool>> {
        match self {
            TypedAttribute::Boolean(attr) => Some(attr),
            _ => None,
        }
    }

    pub fn as_unsigned_int(&self) -> Option<&Attribute<u32>> {
        match self {
            TypedAttribute::UnsignedInt(attr) => Some(attr),
            _ => None,
        }
    }

    pub fn as_signed_int(&self) -> Option<&Attribute<i32>> {
        match self {
            TypedAttribute::SignedInt(attr) => Some(attr),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<&Attribute<f64>> {
        match self {
            TypedAttribute::Float(attr) => Some(attr),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&Attribute<Vec<u8>>> {
        match self {
            TypedAttribute::Bytes(attr) => Some(attr),
            _ => None,
        }
    }

    pub fn as_entity(&self) -> Option<&Attribute<Entity>> {
        match self {
            TypedAttribute::Entity(attr) => Some(attr),
            _ => None,
        }
    }

    pub fn as_symbol(&self) -> Option<&Attribute<dialog_query::artifact::Attribute>> {
        match self {
            TypedAttribute::Symbol(attr) => Some(attr),
            _ => None,
        }
    }

    pub fn as_value(&self) -> Option<&Attribute<Value>> {
        match self {
            TypedAttribute::Value(attr) => Some(attr),
            _ => None,
        }
    }
}

// ================================================================================================
// Conversion implementations - these allow easy creation from typed attributes
// ================================================================================================

impl From<Attribute<String>> for TypedAttribute {
    fn from(attr: Attribute<String>) -> Self {
        TypedAttribute::String(attr)
    }
}

impl From<Attribute<bool>> for TypedAttribute {
    fn from(attr: Attribute<bool>) -> Self {
        TypedAttribute::Boolean(attr)
    }
}

impl From<Attribute<u32>> for TypedAttribute {
    fn from(attr: Attribute<u32>) -> Self {
        TypedAttribute::UnsignedInt(attr)
    }
}

impl From<Attribute<i32>> for TypedAttribute {
    fn from(attr: Attribute<i32>) -> Self {
        TypedAttribute::SignedInt(attr)
    }
}

impl From<Attribute<f64>> for TypedAttribute {
    fn from(attr: Attribute<f64>) -> Self {
        TypedAttribute::Float(attr)
    }
}

impl From<Attribute<Vec<u8>>> for TypedAttribute {
    fn from(attr: Attribute<Vec<u8>>) -> Self {
        TypedAttribute::Bytes(attr)
    }
}

impl From<Attribute<Entity>> for TypedAttribute {
    fn from(attr: Attribute<Entity>) -> Self {
        TypedAttribute::Entity(attr)
    }
}

impl From<Attribute<dialog_query::artifact::Attribute>> for TypedAttribute {
    fn from(attr: Attribute<dialog_query::artifact::Attribute>) -> Self {
        TypedAttribute::Symbol(attr)
    }
}

impl From<Attribute<Value>> for TypedAttribute {
    fn from(attr: Attribute<Value>) -> Self {
        TypedAttribute::Value(attr)
    }
}

// ================================================================================================
// DEMONSTRATION OF THE GOAL PATTERN
// ================================================================================================

/// Example Person struct that demonstrates the desired usage pattern
pub struct Person;

impl Person {
    /// Returns a uniform collection of attributes that preserves type information
    ///
    /// This achieves the goal of having a collection where we can:
    /// 1. Store different Attribute<T> in a uniform collection
    /// 2. Recover the original type information
    /// 3. Safely downcast back to Attribute<String>, Attribute<u32>, etc.
    pub fn attributes() -> Vec<TypedAttribute> {
        vec![
            // Each of these maintains full type information
            Attribute::<String>::new("person", "name", "Person's full name").into(),
            Attribute::<u32>::new("person", "age", "Person's age in years").into(),
            Attribute::<bool>::new("person", "active", "Whether person is active").into(),
            Attribute::<String>::new("person", "email", "Person's email address").into(),
            Attribute::<f64>::new("person", "height", "Person's height in meters").into(),
        ]
    }

    /// Alternative using Box<dyn AttributeTrait> if needed for extensibility
    pub fn attributes_extensible() -> Vec<Box<dyn AttributeTrait>> {
        vec![
            Box::new(Attribute::<String>::new(
                "person",
                "name",
                "Person's full name",
            )),
            Box::new(Attribute::<u32>::new(
                "person",
                "age",
                "Person's age in years",
            )),
            Box::new(Attribute::<bool>::new(
                "person",
                "active",
                "Whether person is active",
            )),
        ]
    }
}

// For the trait object approach, we need this trait
pub trait AttributeTrait: std::any::Any + Send + Sync {
    fn the(&self) -> String;
    fn data_type(&self) -> Option<Type>;
    fn cardinality(&self) -> Cardinality;
    fn as_any(&self) -> &dyn std::any::Any;
}

impl<T: Scalar + Send + Sync + 'static> AttributeTrait for Attribute<T> {
    fn the(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }

    fn data_type(&self) -> Option<Type> {
        T::into_value_data_type()
    }

    fn cardinality(&self) -> Cardinality {
        self.cardinality
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// ================================================================================================
// DEMONSTRATION AND TESTING
// ================================================================================================

fn demonstrate_goal_pattern() {
    println!("=== DEMONSTRATION OF THE GOAL PATTERN ===\n");

    // Get the uniform collection of attributes
    let attrs = Person::attributes();

    println!("📋 Person attributes (uniform collection):");
    for attr in &attrs {
        println!("  - {}: {:?}", attr.the(), attr.data_type());
    }
    println!();

    // This is exactly what we wanted to achieve!
    println!("🎯 GOAL ACHIEVED: Runtime inspection and type refinement:");
    for attr in attrs {
        match attr.data_type() {
            Some(Type::String) => {
                // We can safely get back to Attribute<String>!
                if let Some(string_attr) = attr.as_string() {
                    println!(
                        "  ✅ String attribute '{}' recovered as Attribute<String>",
                        string_attr.the()
                    );
                    // Now we have full access to the original Attribute<String>
                    println!("     - Namespace: {}", string_attr.namespace);
                    println!("     - Name: {}", string_attr.name);
                    println!("     - Description: {}", string_attr.description);
                }
            }
            Some(Type::UnsignedInt) => {
                // We can safely get back to Attribute<u32>!
                if let Some(u32_attr) = attr.as_unsigned_int() {
                    println!(
                        "  ✅ UnsignedInt attribute '{}' recovered as Attribute<u32>",
                        u32_attr.the()
                    );
                    println!("     - Type is preserved: u32");
                    println!("     - Can use for type-safe operations");
                }
            }
            Some(Type::Boolean) => {
                // We can safely get back to Attribute<bool>!
                if let Some(bool_attr) = attr.as_boolean() {
                    println!(
                        "  ✅ Boolean attribute '{}' recovered as Attribute<bool>",
                        bool_attr.the()
                    );
                    println!("     - Type is preserved: bool");
                }
            }
            Some(Type::Float) => {
                if let Some(float_attr) = attr.as_float() {
                    println!(
                        "  ✅ Float attribute '{}' recovered as Attribute<f64>",
                        float_attr.the()
                    );
                    println!("     - Type is preserved: f64");
                }
            }
            None => {
                // This handles Value type attributes
                if let Some(value_attr) = attr.as_value() {
                    println!(
                        "  ✅ Dynamic attribute '{}' (can hold any type)",
                        value_attr.the()
                    );
                }
            }
            _ => {
                println!("  ℹ️  Other type: {:?}", attr.data_type());
            }
        }
    }

    println!("\n🚀 BENEFITS ACHIEVED:");
    println!("  ✅ Uniform collection: Vec<TypedAttribute>");
    println!("  ✅ Type information preserved: data_type() method");
    println!("  ✅ Safe recovery: as_string(), as_unsigned_int(), etc.");
    println!("  ✅ Compile-time guarantees: pattern matching");
    println!("  ✅ Performance: stack allocation, fast matching");
    println!("  ✅ Memory efficient: no heap allocation for attributes");
}

fn demonstrate_performance() {
    println!("\n=== PERFORMANCE DEMONSTRATION ===");

    let attrs = Person::attributes();
    let iterations = 10_000;
    let total_ops = iterations as usize * attrs.len();

    // Test pattern matching performance
    let start = std::time::Instant::now();
    let mut count = 0;
    for _ in 0..iterations {
        for attr in &attrs {
            match attr.data_type() {
                Some(Type::String) => {
                    if attr.as_string().is_some() {
                        count += 1;
                    }
                }
                Some(Type::UnsignedInt) => {
                    if attr.as_unsigned_int().is_some() {
                        count += 1;
                    }
                }
                Some(Type::Boolean) => {
                    if attr.as_boolean().is_some() {
                        count += 1;
                    }
                }
                _ => {}
            }
        }
    }
    let duration = start.elapsed();

    println!("Pattern matching performance:");
    println!(
        "  - {} iterations × {} attributes = {} operations",
        iterations,
        attrs.len(),
        total_ops
    );
    println!("  - Total time: {:?}", duration);
    println!(
        "  - Average per operation: {:?}",
        duration / total_ops as u32
    );
    println!(
        "  - Operations per second: {:.0}",
        total_ops as f64 / duration.as_secs_f64()
    );
    println!("  - Found {} typed attributes", count);
}

fn main() {
    demonstrate_goal_pattern();
    demonstrate_performance();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_preservation() {
        let attrs = Person::attributes();

        // Test that we can identify and extract each type
        let mut found_string = false;
        let mut found_uint = false;
        let mut found_bool = false;
        let mut found_float = false;

        for attr in attrs {
            match attr.data_type() {
                Some(Type::String) => {
                    if let Some(string_attr) = attr.as_string() {
                        assert!(
                            string_attr.the().contains("name")
                                || string_attr.the().contains("email")
                        );
                        found_string = true;
                    }
                }
                Some(Type::UnsignedInt) => {
                    if let Some(uint_attr) = attr.as_unsigned_int() {
                        assert_eq!(uint_attr.the(), "person/age");
                        found_uint = true;
                    }
                }
                Some(Type::Boolean) => {
                    if let Some(bool_attr) = attr.as_boolean() {
                        assert_eq!(bool_attr.the(), "person/active");
                        found_bool = true;
                    }
                }
                Some(Type::Float) => {
                    if let Some(float_attr) = attr.as_float() {
                        assert_eq!(float_attr.the(), "person/height");
                        found_float = true;
                    }
                }
                _ => {}
            }
        }

        // Verify we found all expected types
        assert!(found_string, "Should find string attributes");
        assert!(found_uint, "Should find uint attribute");
        assert!(found_bool, "Should find bool attribute");
        assert!(found_float, "Should find float attribute");
    }

    #[test]
    fn test_conversion_from_typed_attributes() {
        let name_attr = Attribute::<String>::new("test", "name", "Test name");
        let age_attr = Attribute::<u32>::new("test", "age", "Test age");

        // Test conversion
        let typed_name: TypedAttribute = name_attr.into();
        let typed_age: TypedAttribute = age_attr.into();

        // Test extraction
        assert!(typed_name.as_string().is_some());
        assert!(typed_name.as_unsigned_int().is_none());

        assert!(typed_age.as_unsigned_int().is_some());
        assert!(typed_age.as_string().is_none());
    }

    #[test]
    fn test_goal_pattern_requirements() {
        // ✅ Can we store different Attribute<T> in a uniform collection?
        let attrs: Vec<TypedAttribute> = vec![
            Attribute::<String>::new("test", "name", "Name").into(),
            Attribute::<u32>::new("test", "age", "Age").into(),
            Attribute::<bool>::new("test", "active", "Active").into(),
        ];
        assert_eq!(attrs.len(), 3);

        // ✅ Can we recover the original type information?
        for attr in &attrs {
            assert!(attr.data_type().is_some() || matches!(attr, TypedAttribute::Value(_)));
        }

        // ✅ Can we safely downcast back to Attribute<String>, Attribute<u32>, etc.?
        let name_recovered = attrs[0].as_string().is_some();
        let age_recovered = attrs[1].as_unsigned_int().is_some();
        let active_recovered = attrs[2].as_boolean().is_some();

        assert!(name_recovered, "Should recover Attribute<String>");
        assert!(age_recovered, "Should recover Attribute<u32>");
        assert!(active_recovered, "Should recover Attribute<bool>");
    }
}
