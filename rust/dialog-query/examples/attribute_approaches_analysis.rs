/// Comprehensive analysis of type-preserving attribute approaches
///
/// This file provides detailed analysis of the trade-offs between different approaches
/// for preserving type information while allowing uniform collections.

use std::any::{Any, TypeId};
use std::marker::PhantomData;
use std::mem;

// Mock types for our exploration (duplicate from the other example for independence)
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
// HELPER FUNCTIONS
// ================================================================================================

fn create_person_attributes() -> (Attribute<String>, Attribute<u32>, Attribute<bool>) {
    let name = Attribute::<String>::new("person", "name", "Person's full name");
    let age = Attribute::<u32>::new("person", "age", "Person's age in years");
    let active = Attribute::<bool>::new("person", "active", "Whether person is active");
    (name, age, active)
}

// ================================================================================================
// ANALYSIS AND BENCHMARKING
// ================================================================================================

/// Performance and memory usage analysis
pub fn analyze_approaches() {
    println!("=== TYPE-PRESERVING ATTRIBUTE APPROACHES ANALYSIS ===\n");

    analyze_trait_object_approach();
    analyze_enum_approach();
    analyze_erased_approach();
    analyze_existential_approach();
    
    println!("=== RECOMMENDATIONS ===\n");
    print_recommendations();
}

fn analyze_trait_object_approach() {
    println!("1. TRAIT OBJECT APPROACH");
    println!("========================");
    
    let (name, age, active) = create_person_attributes();
    let attrs: Vec<Box<dyn AttributeTrait>> = vec![
        Box::new(name),
        Box::new(age),
        Box::new(active),
    ];
    
    println!("Memory usage:");
    println!("  - Box<dyn AttributeTrait>: {} bytes per attribute", mem::size_of::<Box<dyn AttributeTrait>>());
    println!("  - Includes vtable pointer (8 bytes) + data pointer (8 bytes)");
    
    println!("Pros:");
    println!("  + Clean, idiomatic Rust approach");
    println!("  + Type-safe downcasting with Any trait");
    println!("  + No need to modify existing Attribute<T> structure");
    println!("  + Extensible - can add new types without modifying core enum");
    println!("  + Zero-cost abstractions when not downcasting");
    
    println!("Cons:");
    println!("  - Heap allocation for each attribute (Box)");
    println!("  - Double indirection (vtable + data pointer)");
    println!("  - Runtime cost of downcasting");
    println!("  - Requires All: Any + Send + Sync bounds");
    
    // Test downcasting performance
    let start = std::time::Instant::now();
    for _ in 0..1000 {
        for attr in &attrs {
            if let Some(_) = downcast_attribute::<String>(attr.as_ref()) {
                // Found string attribute
            }
            if let Some(_) = downcast_attribute::<u32>(attr.as_ref()) {
                // Found u32 attribute
            }
            if let Some(_) = downcast_attribute::<bool>(attr.as_ref()) {
                // Found bool attribute
            }
        }
    }
    let duration = start.elapsed();
    println!("Performance: 1000 iterations of 3 downcasts: {:?}", duration);
    println!();
}

fn analyze_enum_approach() {
    println!("2. ENUM APPROACH");
    println!("================");
    
    let (name, age, active) = create_person_attributes();
    let attrs: Vec<TypedAttribute> = vec![
        name.into(),
        age.into(),
        active.into(),
    ];
    
    println!("Memory usage:");
    println!("  - TypedAttribute: {} bytes per attribute", mem::size_of::<TypedAttribute>());
    println!("  - Stack allocated, size determined by largest variant");
    
    println!("Pros:");
    println!("  + Stack allocated - no heap overhead");
    println!("  + Compile-time exhaustiveness checking");
    println!("  + Pattern matching is very fast");
    println!("  + Zero-cost type refinement after match");
    println!("  + Clear, explicit type handling");
    
    println!("Cons:");
    println!("  - Must modify code for each new type (not extensible)");
    println!("  - Larger memory footprint (size of largest variant)");
    println!("  - Coupling between attribute types and enum definition");
    println!("  - Manual conversion implementations needed");
    
    // Test pattern matching performance
    let start = std::time::Instant::now();
    for _ in 0..1000 {
        for attr in &attrs {
            match attr {
                TypedAttribute::String(_) => {},
                TypedAttribute::UnsignedInt(_) => {},
                TypedAttribute::Boolean(_) => {},
            }
        }
    }
    let duration = start.elapsed();
    println!("Performance: 1000 iterations of pattern matching: {:?}", duration);
    println!();
}

fn analyze_erased_approach() {
    println!("3. TYPE-ERASED WRAPPER APPROACH");
    println!("===============================");
    
    let (name, age, active) = create_person_attributes();
    let attrs = vec![
        ErasedAttribute::new(name),
        ErasedAttribute::new(age),
        ErasedAttribute::new(active),
    ];
    
    println!("Memory usage:");
    println!("  - ErasedAttribute: {} bytes per attribute", mem::size_of::<ErasedAttribute>());
    println!("  - Contains pointer + metadata + vtable pointer");
    
    println!("Pros:");
    println!("  + Type-safe with proper lifetime management");
    println!("  + Extensible - works with any type");
    println!("  + Custom vtable allows optimized operations");
    println!("  + Memory efficient for large attributes");
    
    println!("Cons:");
    println!("  - Complex unsafe implementation");
    println!("  - Manual memory management via vtable");
    println!("  - Higher complexity and potential for bugs");
    println!("  - Requires careful lifetime management");
    
    // Test downcasting performance
    let start = std::time::Instant::now();
    for _ in 0..1000 {
        for attr in &attrs {
            if let Some(_) = attr.downcast::<String>() {}
            if let Some(_) = attr.downcast::<u32>() {}
            if let Some(_) = attr.downcast::<bool>() {}
        }
    }
    let duration = start.elapsed();
    println!("Performance: 1000 iterations of 3 downcasts: {:?}", duration);
    println!();
}

fn analyze_existential_approach() {
    println!("4. EXISTENTIAL TYPES APPROACH");
    println!("=============================");
    
    let (name, age, active) = create_person_attributes();
    let attrs = vec![
        ExistentialAttribute::new(name),
        ExistentialAttribute::new(age),
        ExistentialAttribute::new(active),
    ];
    
    println!("Memory usage:");
    println!("  - ExistentialAttribute: {} bytes per attribute", mem::size_of::<ExistentialAttribute>());
    println!("  - Contains Box + metadata + cached values");
    
    println!("Pros:");
    println!("  + Safe - uses Box<dyn Any> internally");
    println!("  + Caches common values for fast access");
    println!("  + Type-safe downcasting");
    println!("  + Extensible to new types");
    
    println!("Cons:");
    println!("  - Heap allocation for each attribute");
    println!("  - Memory overhead from cached values");
    println!("  - Less efficient than direct approaches");
    
    // Test performance
    let start = std::time::Instant::now();
    for _ in 0..1000 {
        for attr in &attrs {
            let _ = attr.data_type();
            if let Some(_) = attr.downcast::<String>() {}
        }
    }
    let duration = start.elapsed();
    println!("Performance: 1000 iterations access + downcast: {:?}", duration);
    println!();
}


fn print_recommendations() {
    println!("RECOMMENDED APPROACH BY USE CASE:");
    println!("=================================");
    
    println!("🏆 BEST OVERALL: Enum Approach");
    println!("   Use when you have a known, finite set of attribute types and");
    println!("   performance is critical. Provides best performance and safety.");
    println!();
    
    println!("🚀 MOST FLEXIBLE: Trait Object Approach");
    println!("   Use when you need maximum extensibility and don't mind the");
    println!("   heap allocation cost. Great for plugin architectures.");
    println!();
    
    println!("⚡ PERFORMANCE CRITICAL: Enum Approach");
    println!("   Stack allocation + pattern matching gives the best performance.");
    println!();
    
    println!("🔧 COMPLEX SCENARIOS: Type-Erased Wrapper");
    println!("   Use when you need fine-grained control over memory layout and");
    println!("   have expertise with unsafe Rust.");
    println!();
    
    
    println!("INTEGRATION RECOMMENDATION:");
    println!("===========================");
    println!("Start with the **Enum Approach** for your current use case:");
    println!();
    println!("```rust");
    println!("#[derive(Debug, Clone)]");
    println!("pub enum TypedAttribute {{");
    println!("    String(Attribute<String>),");
    println!("    UnsignedInt(Attribute<u32>),");
    println!("    SignedInt(Attribute<i32>),");
    println!("    Boolean(Attribute<bool>),");
    println!("    Float(Attribute<f64>),");
    println!("    Bytes(Attribute<Vec<u8>>),");
    println!("    Entity(Attribute<Entity>),");
    println!("}}");
    println!("```");
    println!();
    println!("Benefits for your use case:");
    println!("- Zero heap allocation");
    println!("- Compile-time exhaustiveness checking");
    println!("- Fast pattern matching");
    println!("- Easy to extend with macro generation");
    println!("- Clear error messages");
    println!();
    println!("If you later need more flexibility, you can migrate to the");
    println!("Trait Object approach with minimal changes to client code.");
}

fn main() {
    analyze_approaches();
}

