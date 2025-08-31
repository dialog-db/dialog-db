//! Exploration of type-preserving attribute patterns
//!
//! This file contains prototypes for different approaches to preserve type information
//! while allowing runtime inspection and type refinement for attributes.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::marker::PhantomData;

// Mock types for our exploration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueDataType {
    String,
    UnsignedInt,
    SignedInt,
    Boolean,
    Float,
    Bytes,
    Entity,
    Symbol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cardinality {
    One,
    Many,
}

// Base attribute type from the existing codebase
#[derive(Clone, Debug)]
pub struct Attribute<T> {
    pub namespace: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub cardinality: Cardinality,
    pub marker: PhantomData<T>,
}

impl<T> Attribute<T> {
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
}

// Trait for types that can provide ValueDataType metadata
pub trait IntoValueDataType {
    fn into_value_data_type() -> Option<ValueDataType>;
}

// Implementations for common types
impl IntoValueDataType for String {
    fn into_value_data_type() -> Option<ValueDataType> {
        Some(ValueDataType::String)
    }
}

impl IntoValueDataType for u32 {
    fn into_value_data_type() -> Option<ValueDataType> {
        Some(ValueDataType::UnsignedInt)
    }
}

impl IntoValueDataType for bool {
    fn into_value_data_type() -> Option<ValueDataType> {
        Some(ValueDataType::Boolean)
    }
}

// ================================================================================================
// APPROACH 1: Trait Objects with Type Info
// ================================================================================================

/// Trait that all typed attributes implement
pub trait AttributeTrait: Any + Send + Sync {
    /// Get the fully qualified name of this attribute
    fn the(&self) -> String;
    
    /// Get the data type this attribute holds
    fn data_type(&self) -> Option<ValueDataType>;
    
    /// Get the cardinality of this attribute
    fn cardinality(&self) -> Cardinality;
    
    /// Get type metadata for downcasting
    fn type_id(&self) -> TypeId;
    
    /// Cast to Any for downcasting
    fn as_any(&self) -> &dyn Any;
}

impl<T: IntoValueDataType + Send + Sync + 'static> AttributeTrait for Attribute<T> {
    fn the(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
    
    fn data_type(&self) -> Option<ValueDataType> {
        T::into_value_data_type()
    }
    
    fn cardinality(&self) -> Cardinality {
        self.cardinality
    }
    
    fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Type-safe downcast helper
pub fn downcast_attribute<T: 'static>(attr: &dyn AttributeTrait) -> Option<&Attribute<T>> {
    attr.as_any().downcast_ref::<Attribute<T>>()
}

// ================================================================================================
// APPROACH 2: Enum-based Approach
// ================================================================================================

/// Enum that holds different Attribute<T> variants while preserving type info
#[derive(Debug, Clone)]
pub enum TypedAttribute {
    String(Attribute<String>),
    UnsignedInt(Attribute<u32>),
    Boolean(Attribute<bool>),
    // Add more variants as needed
}

impl TypedAttribute {
    pub fn the(&self) -> String {
        match self {
            TypedAttribute::String(attr) => attr.the(),
            TypedAttribute::UnsignedInt(attr) => attr.the(),
            TypedAttribute::Boolean(attr) => attr.the(),
        }
    }
    
    pub fn data_type(&self) -> Option<ValueDataType> {
        match self {
            TypedAttribute::String(_) => Some(ValueDataType::String),
            TypedAttribute::UnsignedInt(_) => Some(ValueDataType::UnsignedInt),
            TypedAttribute::Boolean(_) => Some(ValueDataType::Boolean),
        }
    }
    
    /// Safely extract the underlying Attribute<String> if this is a String variant
    pub fn as_string(&self) -> Option<&Attribute<String>> {
        match self {
            TypedAttribute::String(attr) => Some(attr),
            _ => None,
        }
    }
    
    /// Safely extract the underlying Attribute<u32> if this is a UnsignedInt variant
    pub fn as_unsigned_int(&self) -> Option<&Attribute<u32>> {
        match self {
            TypedAttribute::UnsignedInt(attr) => Some(attr),
            _ => None,
        }
    }
    
    /// Safely extract the underlying Attribute<bool> if this is a Boolean variant
    pub fn as_boolean(&self) -> Option<&Attribute<bool>> {
        match self {
            TypedAttribute::Boolean(attr) => Some(attr),
            _ => None,
        }
    }
}

// Conversion helpers
impl From<Attribute<String>> for TypedAttribute {
    fn from(attr: Attribute<String>) -> Self {
        TypedAttribute::String(attr)
    }
}

impl From<Attribute<u32>> for TypedAttribute {
    fn from(attr: Attribute<u32>) -> Self {
        TypedAttribute::UnsignedInt(attr)
    }
}

impl From<Attribute<bool>> for TypedAttribute {
    fn from(attr: Attribute<bool>) -> Self {
        TypedAttribute::Boolean(attr)
    }
}

// ================================================================================================
// APPROACH 3: Type-erased Wrapper with Safe Casting
// ================================================================================================

/// Type-erased wrapper that holds a pointer to the actual Attribute<T> plus metadata
pub struct ErasedAttribute {
    /// Type-erased pointer to the actual attribute
    ptr: *const (),
    /// Type metadata for safe casting
    type_id: TypeId,
    /// Data type information
    data_type: Option<ValueDataType>,
    /// Vtable for common operations
    vtable: &'static ErasedAttributeVtable,
}

/// Virtual table for type-erased operations
pub struct ErasedAttributeVtable {
    pub the: fn(*const ()) -> String,
    pub cardinality: fn(*const ()) -> Cardinality,
    pub drop: fn(*const ()),
}

impl ErasedAttribute {
    /// Create a new erased attribute from a typed attribute
    pub fn new<T: IntoValueDataType + 'static>(attr: Attribute<T>) -> Self {
        let boxed = Box::new(attr);
        let ptr = Box::into_raw(boxed) as *const ();
        
        Self {
            ptr,
            type_id: TypeId::of::<T>(),
            data_type: T::into_value_data_type(),
            vtable: &ErasedAttributeVtable {
                the: |ptr| unsafe {
                    let attr = &*(ptr as *const Attribute<T>);
                    attr.the()
                },
                cardinality: |ptr| unsafe {
                    let attr = &*(ptr as *const Attribute<T>);
                    attr.cardinality
                },
                drop: |ptr| unsafe {
                    let _ = Box::from_raw(ptr as *mut Attribute<T>);
                },
            },
        }
    }
    
    pub fn the(&self) -> String {
        (self.vtable.the)(self.ptr)
    }
    
    pub fn data_type(&self) -> Option<ValueDataType> {
        self.data_type
    }
    
    pub fn cardinality(&self) -> Cardinality {
        (self.vtable.cardinality)(self.ptr)
    }
    
    /// Safely downcast to the original type
    pub fn downcast<T: 'static>(&self) -> Option<&Attribute<T>> {
        if self.type_id == TypeId::of::<T>() {
            unsafe {
                Some(&*(self.ptr as *const Attribute<T>))
            }
        } else {
            None
        }
    }
}

impl Drop for ErasedAttribute {
    fn drop(&mut self) {
        (self.vtable.drop)(self.ptr);
    }
}

// Safety: ErasedAttribute manages its own memory correctly
unsafe impl Send for ErasedAttribute {}
unsafe impl Sync for ErasedAttribute {}

// ================================================================================================
// APPROACH 4: Existential Types Pattern with Type Witness
// ================================================================================================

/// Type witness that carries type information
pub struct TypeWitness<T> {
    pub type_id: TypeId,
    pub data_type: Option<ValueDataType>,
    pub _phantom: PhantomData<T>,
}

impl<T: IntoValueDataType + 'static> TypeWitness<T> {
    pub fn new() -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            data_type: T::into_value_data_type(),
            _phantom: PhantomData,
        }
    }
}

/// Existential attribute that pairs an attribute with its type witness
pub struct ExistentialAttribute {
    /// The actual attribute (type-erased)
    attr: Box<dyn Any + Send + Sync>,
    /// Type witness for safe casting
    type_id: TypeId,
    /// Data type information
    data_type: Option<ValueDataType>,
    /// Common attribute operations
    the: String,
    cardinality: Cardinality,
}

impl ExistentialAttribute {
    /// Create a new existential attribute
    pub fn new<T: IntoValueDataType + Send + Sync + 'static>(attr: Attribute<T>) -> Self {
        Self {
            the: attr.the(),
            cardinality: attr.cardinality,
            type_id: TypeId::of::<T>(),
            data_type: T::into_value_data_type(),
            attr: Box::new(attr),
        }
    }
    
    pub fn the(&self) -> &str {
        &self.the
    }
    
    pub fn data_type(&self) -> Option<ValueDataType> {
        self.data_type
    }
    
    pub fn cardinality(&self) -> Cardinality {
        self.cardinality
    }
    
    /// Safely downcast to the original type
    pub fn downcast<T: 'static>(&self) -> Option<&Attribute<T>> {
        if self.type_id == TypeId::of::<T>() {
            self.attr.downcast_ref::<Attribute<T>>()
        } else {
            None
        }
    }
}

// ================================================================================================
// APPROACH 5: Registry-based Pattern (inspired by erased_serde)
// ================================================================================================

/// Global registry that maps type IDs to type information
pub struct TypeRegistry {
    types: HashMap<TypeId, TypeInfo>,
}

#[derive(Clone)]
pub struct TypeInfo {
    pub data_type: Option<ValueDataType>,
    pub name: &'static str,
}

impl TypeRegistry {
    pub fn new() -> Self {
        Self {
            types: HashMap::new(),
        }
    }
    
    pub fn register<T: IntoValueDataType + 'static>(&mut self, name: &'static str) {
        self.types.insert(TypeId::of::<T>(), TypeInfo {
            data_type: T::into_value_data_type(),
            name,
        });
    }
    
    pub fn get_type_info(&self, type_id: TypeId) -> Option<&TypeInfo> {
        self.types.get(&type_id)
    }
}

/// Registry-based erased attribute
pub struct RegistryAttribute {
    attr: Box<dyn Any + Send + Sync>,
    type_id: TypeId,
    the: String,
    cardinality: Cardinality,
}

impl RegistryAttribute {
    pub fn new<T: 'static + Send + Sync>(attr: Attribute<T>) -> Self {
        Self {
            the: attr.the(),
            cardinality: attr.cardinality,
            type_id: TypeId::of::<T>(),
            attr: Box::new(attr),
        }
    }
    
    pub fn the(&self) -> &str {
        &self.the
    }
    
    pub fn cardinality(&self) -> Cardinality {
        self.cardinality
    }
    
    pub fn type_id(&self) -> TypeId {
        self.type_id
    }
    
    pub fn data_type(&self, registry: &TypeRegistry) -> Option<ValueDataType> {
        registry.get_type_info(self.type_id)?.data_type
    }
    
    pub fn downcast<T: 'static>(&self) -> Option<&Attribute<T>> {
        if self.type_id == TypeId::of::<T>() {
            self.attr.downcast_ref::<Attribute<T>>()
        } else {
            None
        }
    }
}

// ================================================================================================
// EXAMPLE USAGE AND TESTING
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    
    fn create_person_attributes() -> (Attribute<String>, Attribute<u32>, Attribute<bool>) {
        let name = Attribute::<String>::new("person", "name", "Person's full name");
        let age = Attribute::<u32>::new("person", "age", "Person's age in years");
        let active = Attribute::<bool>::new("person", "active", "Whether person is active");
        (name, age, active)
    }
    
    #[test]
    fn test_trait_object_approach() {
        let (name, age, active) = create_person_attributes();
        
        // Store as trait objects
        let attrs: Vec<Box<dyn AttributeTrait>> = vec![
            Box::new(name),
            Box::new(age),
            Box::new(active),
        ];
        
        for attr in &attrs {
            println!("Attribute: {}, Type: {:?}", attr.the(), attr.data_type());
            
            // Test downcasting
            if let Some(string_attr) = downcast_attribute::<String>(attr.as_ref()) {
                println!("  -> Successfully downcast to Attribute<String>");
                assert_eq!(string_attr.the(), "person/name");
            }
            
            if let Some(u32_attr) = downcast_attribute::<u32>(attr.as_ref()) {
                println!("  -> Successfully downcast to Attribute<u32>");
                assert_eq!(u32_attr.the(), "person/age");
            }
            
            if let Some(bool_attr) = downcast_attribute::<bool>(attr.as_ref()) {
                println!("  -> Successfully downcast to Attribute<bool>");
                assert_eq!(bool_attr.the(), "person/active");
            }
        }
    }
    
    #[test]
    fn test_enum_approach() {
        let (name, age, active) = create_person_attributes();
        
        // Store as enum variants
        let attrs: Vec<TypedAttribute> = vec![
            name.into(),
            age.into(),
            active.into(),
        ];
        
        for attr in &attrs {
            println!("Attribute: {}, Type: {:?}", attr.the(), attr.data_type());
            
            // Test pattern matching and extraction
            match attr {
                TypedAttribute::String(string_attr) => {
                    println!("  -> String attribute: {}", string_attr.the());
                    assert_eq!(string_attr.the(), "person/name");
                },
                TypedAttribute::UnsignedInt(u32_attr) => {
                    println!("  -> U32 attribute: {}", u32_attr.the());
                    assert_eq!(u32_attr.the(), "person/age");
                },
                TypedAttribute::Boolean(bool_attr) => {
                    println!("  -> Bool attribute: {}", bool_attr.the());
                    assert_eq!(bool_attr.the(), "person/active");
                },
            }
            
            // Test safe extraction methods
            if let Some(string_attr) = attr.as_string() {
                println!("  -> Extracted String attribute: {}", string_attr.the());
            }
        }
    }
    
    #[test]
    fn test_erased_attribute_approach() {
        let (name, age, active) = create_person_attributes();
        
        // Store as erased attributes
        let attrs = vec![
            ErasedAttribute::new(name),
            ErasedAttribute::new(age),
            ErasedAttribute::new(active),
        ];
        
        for attr in &attrs {
            println!("Attribute: {}, Type: {:?}", attr.the(), attr.data_type());
            
            // Test downcasting
            if let Some(string_attr) = attr.downcast::<String>() {
                println!("  -> Successfully downcast to Attribute<String>");
                assert_eq!(string_attr.the(), "person/name");
            }
            
            if let Some(u32_attr) = attr.downcast::<u32>() {
                println!("  -> Successfully downcast to Attribute<u32>");
                assert_eq!(u32_attr.the(), "person/age");
            }
            
            if let Some(bool_attr) = attr.downcast::<bool>() {
                println!("  -> Successfully downcast to Attribute<bool>");
                assert_eq!(bool_attr.the(), "person/active");
            }
        }
    }
    
    #[test]
    fn test_existential_approach() {
        let (name, age, active) = create_person_attributes();
        
        // Store as existential attributes
        let attrs = vec![
            ExistentialAttribute::new(name),
            ExistentialAttribute::new(age),
            ExistentialAttribute::new(active),
        ];
        
        for attr in &attrs {
            println!("Attribute: {}, Type: {:?}", attr.the(), attr.data_type());
            
            // Test downcasting
            if let Some(string_attr) = attr.downcast::<String>() {
                println!("  -> Successfully downcast to Attribute<String>");
                assert_eq!(string_attr.the(), "person/name");
            }
        }
    }
    
    #[test]
    fn test_registry_approach() {
        let mut registry = TypeRegistry::new();
        registry.register::<String>("String");
        registry.register::<u32>("u32");
        registry.register::<bool>("bool");
        
        let (name, age, active) = create_person_attributes();
        
        let attrs = vec![
            RegistryAttribute::new(name),
            RegistryAttribute::new(age),
            RegistryAttribute::new(active),
        ];
        
        for attr in &attrs {
            println!("Attribute: {}, Type: {:?}", attr.the(), attr.data_type(&registry));
            
            if let Some(string_attr) = attr.downcast::<String>() {
                println!("  -> Successfully downcast to Attribute<String>");
                assert_eq!(string_attr.the(), "person/name");
            }
        }
    }
}

// ================================================================================================
// DEMONSTRATION OF THE GOAL PATTERN
// ================================================================================================

/// Example of what we want to achieve - a Person struct with attributes
pub struct Person;

impl Person {
    /// Returns a uniform collection of attributes that preserves type information
    pub fn attributes_trait_objects() -> Vec<Box<dyn AttributeTrait>> {
        vec![
            Box::new(Attribute::<String>::new("person", "name", "Person's full name")),
            Box::new(Attribute::<u32>::new("person", "age", "Person's age in years")),
            Box::new(Attribute::<bool>::new("person", "active", "Whether person is active")),
        ]
    }
    
    pub fn attributes_enum() -> Vec<TypedAttribute> {
        vec![
            Attribute::<String>::new("person", "name", "Person's full name").into(),
            Attribute::<u32>::new("person", "age", "Person's age in years").into(),
            Attribute::<bool>::new("person", "active", "Whether person is active").into(),
        ]
    }
    
    pub fn attributes_erased() -> Vec<ErasedAttribute> {
        vec![
            ErasedAttribute::new(Attribute::<String>::new("person", "name", "Person's full name")),
            ErasedAttribute::new(Attribute::<u32>::new("person", "age", "Person's age in years")),
            ErasedAttribute::new(Attribute::<bool>::new("person", "active", "Whether person is active")),
        ]
    }
}

/// Demonstration of the desired usage pattern
pub fn demonstrate_goal_pattern() {
    println!("=== Trait Object Approach ===");
    let attrs = Person::attributes_trait_objects();
    for attr in attrs {
        match attr.data_type() {
            Some(ValueDataType::String) => {
                if let Some(string_attr) = downcast_attribute::<String>(attr.as_ref()) {
                    println!("Found String attribute: {}", string_attr.the());
                }
            },
            Some(ValueDataType::UnsignedInt) => {
                if let Some(u32_attr) = downcast_attribute::<u32>(attr.as_ref()) {
                    println!("Found UnsignedInt attribute: {}", u32_attr.the());
                }
            },
            Some(ValueDataType::Boolean) => {
                if let Some(bool_attr) = downcast_attribute::<bool>(attr.as_ref()) {
                    println!("Found Boolean attribute: {}", bool_attr.the());
                }
            },
            _ => {}
        }
    }
    
    println!("\n=== Enum Approach ===");
    let attrs = Person::attributes_enum();
    for attr in attrs {
        match attr.data_type() {
            Some(ValueDataType::String) => {
                if let Some(string_attr) = attr.as_string() {
                    println!("Found String attribute: {}", string_attr.the());
                }
            },
            Some(ValueDataType::UnsignedInt) => {
                if let Some(u32_attr) = attr.as_unsigned_int() {
                    println!("Found UnsignedInt attribute: {}", u32_attr.the());
                }
            },
            Some(ValueDataType::Boolean) => {
                if let Some(bool_attr) = attr.as_boolean() {
                    println!("Found Boolean attribute: {}", bool_attr.the());
                }
            },
            _ => {}
        }
    }
}

fn main() {
    demonstrate_goal_pattern();
}