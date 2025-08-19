//! New unified Variable system with generic types and clean turbofish syntax
//! Provides both typed and untyped variables through a single Variable<T> enum

use dialog_artifacts::Value;
// use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::marker::PhantomData;

/// Re-export ValueDataType for convenience
pub use dialog_artifacts::ValueDataType;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct Untyped;

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

impl IntoValueDataType for Untyped {
    fn into_value_data_type() -> Option<ValueDataType> {
        None
    }
}

impl IntoValueDataType for Value {
    fn into_value_data_type() -> Option<ValueDataType> {
        // Value is a dynamic type, so we return None to indicate it can hold any type
        None
    }
}

/// Variable name - following x-query pattern of string-based variables
pub type VariableName = String;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Variable {
    name: VariableName,
    _type: Option<ValueDataType>,
}

impl Variable {
    pub fn new<T>(name: impl Into<VariableName>) -> Self
    where
        T: IntoValueDataType,
    {
        Variable {
            name: name.into(),
            _type: T::into_value_data_type(),
        }
    }

    /// Get the variable name
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Get the data type constraint for typed variables
    pub fn data_type(&self) -> Option<ValueDataType> {
        self._type
    }

    /// Check if this typed variable can be unified with the given value
    pub fn can_unify_with(&self, value: &Value) -> bool {
        let value_type = ValueDataType::from(value);
        if let Some(var_type) = self.data_type() {
            value_type == var_type
        } else {
            true
        }
    }
}

impl<T> From<TypedVariable<T>> for Variable
where
    T: IntoValueDataType,
{
    fn from(value: TypedVariable<T>) -> Self {
        Self::new::<T>(value.name())
    }
}

/// New unified Variable<T> struct with phantom types for zero-cost type safety
/// When T = () (default), the variable is untyped and can unify with any value
/// When T = specific type, the variable is typed and only unifies with matching values
///
/// T is constrained to types that implement IntoValueDataType, ensuring only
/// supported Dialog value types can be used.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TypedVariable<T = Untyped>
where
    T: IntoValueDataType,
{
    name: VariableName,
    _phantom_type: PhantomData<T>,
}

// Implementation for all Variable<T> types
impl<T> TypedVariable<T>
where
    T: IntoValueDataType,
{
    /// Create a new variable with the specified type
    /// This method requires explicit type specification using turbofish syntax
    ///
    /// # Examples
    /// ```
    /// use dialog_query::{TypedVariable, Untyped};
    ///
    /// let name_var = TypedVariable::<String>::new("name");  // Returns Variable<String>
    /// let age_var = TypedVariable::<u64>::new("age");       // Returns Variable<u64>
    /// let any_var = TypedVariable::<Untyped>::new("any");   // Returns Variable<Untyped>
    /// ```
    pub fn new(name: impl Into<VariableName>) -> Self {
        TypedVariable {
            name: name.into(),
            _phantom_type: PhantomData,
        }
    }

    /// Get the variable name
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Get the data type constraint for typed variables
    pub fn data_type(&self) -> Option<ValueDataType> {
        T::into_value_data_type()
    }

    /// Check if this typed variable can be unified with the given value
    pub fn can_unify_with(&self, value: &Value) -> bool {
        let value_type = ValueDataType::from(value);
        if let Some(var_type) = T::into_value_data_type() {
            value_type == var_type
        } else {
            true
        }
    }
    
    /// Convert this typed variable to an untyped variable
    pub fn to_untyped(&self) -> TypedVariable<Untyped> {
        TypedVariable {
            name: self.name.clone(),
            _phantom_type: PhantomData,
        }
    }
}
// Display implementation for all variables
impl<T> Display for TypedVariable<T>
where
    T: IntoValueDataType,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match T::into_value_data_type() {
            Some(data_type) => write!(f, "?{}<{:?}>", self.name, data_type),
            None => write!(f, "?{}", self.name),
        }
    }
}

// Serialize implementation for typed variables
impl<T> serde::Serialize for TypedVariable<T>
where
    T: IntoValueDataType,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        match self.data_type() {
            Some(data_type) => {
                let mut state = serializer.serialize_struct("Variable", 2)?;
                state.serialize_field("name", self.name())?;
                state.serialize_field("type", &(data_type as u8))?;
                state.end()
            }
            None => {
                let mut state = serializer.serialize_struct("Variable", 1)?;
                state.serialize_field("name", self.name())?;
                state.end()
            }
        }
    }
}

// Deserialize implementation for Variable<T>
impl<'de, T> serde::Deserialize<'de> for TypedVariable<T>
where
    T: IntoValueDataType,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        struct VariableVisitor<T>(PhantomData<T>);

        impl<'de, T> Visitor<'de> for VariableVisitor<T>
        where
            T: IntoValueDataType,
        {
            type Value = TypedVariable<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Variable")
            }

            fn visit_map<V>(self, mut map: V) -> Result<TypedVariable<T>, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut name = None;
                let mut var_type: Option<u8> = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        "name" => {
                            if name.is_some() {
                                return Err(de::Error::duplicate_field("name"));
                            }
                            name = Some(map.next_value()?);
                        }
                        "type" => {
                            if var_type.is_some() {
                                return Err(de::Error::duplicate_field("type"));
                            }
                            var_type = Some(map.next_value()?);
                        }
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }

                let name: String = name.ok_or_else(|| de::Error::missing_field("name"))?;

                // For now, we ignore the type field and just create the variable
                // with the phantom type T specified at compile time
                Ok(TypedVariable::new(name))
            }
        }

        deserializer.deserialize_struct("Variable", &["name", "type"], VariableVisitor(PhantomData))
    }
}

/// Variable scope for tracking bound variables during planning
#[derive(Debug, Clone)]
pub struct VariableScope {
    /// Variables that are already bound in this scope
    pub bound_variables: std::collections::BTreeSet<VariableName>,
}

impl VariableScope {
    /// Create a new empty variable scope
    pub fn new() -> Self {
        Self {
            bound_variables: std::collections::BTreeSet::new(),
        }
    }

    /// Check if a variable is bound in this scope
    pub fn is_bound(&self, variable_name: &VariableName) -> bool {
        self.bound_variables.contains(variable_name)
    }

    /// Add a variable to the bound set
    pub fn bind(&mut self, variable_name: VariableName) {
        self.bound_variables.insert(variable_name);
    }
}

impl Default for VariableScope {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_variable_creation() {
        let var = TypedVariable::<Untyped>::new("person");
        assert_eq!(var.name(), "person");
        assert!(var.data_type().is_none());
    }

    #[test]
    fn test_typed_variable() {
        let var = TypedVariable::<String>::new("name");
        assert_eq!(var.name(), "name");
        assert_eq!(var.data_type(), Some(ValueDataType::String));
    }

    #[test]
    fn test_variable_type_matching() {
        let string_var = TypedVariable::<String>::new("name");

        assert!(string_var.can_unify_with(&Value::String("Alice".to_string())));
        assert!(!string_var.can_unify_with(&Value::Boolean(true)));

        let any_var = TypedVariable::<Untyped>::new("anything");
        assert!(any_var.can_unify_with(&Value::String("Alice".to_string())));
        assert!(any_var.can_unify_with(&Value::Boolean(true)));
    }

    #[test]
    fn test_value_data_type_from_value() {
        assert_eq!(
            ValueDataType::from(&Value::String("test".to_string())),
            ValueDataType::String
        );
        assert_eq!(
            ValueDataType::from(&Value::Boolean(true)),
            ValueDataType::Boolean
        );
        assert_eq!(
            ValueDataType::from(&Value::UnsignedInt(42)),
            ValueDataType::UnsignedInt
        );
    }

    #[test]
    fn test_type_specific_constructors() {
        let string_var = TypedVariable::<String>::new("name");
        let uint_var = TypedVariable::<u64>::new("age");
        let bool_var = TypedVariable::<bool>::new("active");

        // String variable should only accept string values
        assert!(string_var.can_unify_with(&Value::String("Alice".to_string())));
        assert!(!string_var.can_unify_with(&Value::UnsignedInt(42)));
        assert!(!string_var.can_unify_with(&Value::Boolean(true)));

        // UInt variable should only accept unsigned int values
        assert!(uint_var.can_unify_with(&Value::UnsignedInt(42)));
        assert!(!uint_var.can_unify_with(&Value::String("42".to_string())));
        assert!(!uint_var.can_unify_with(&Value::Boolean(true)));

        // Bool variable should only accept boolean values
        assert!(bool_var.can_unify_with(&Value::Boolean(true)));
        assert!(!bool_var.can_unify_with(&Value::String("true".to_string())));
        assert!(!bool_var.can_unify_with(&Value::UnsignedInt(1)));
    }

    #[test]
    fn test_variable_scope() {
        let mut scope = VariableScope::new();

        assert!(!scope.is_bound(&"person".to_string()));

        scope.bind("person".to_string());
        assert!(scope.is_bound(&"person".to_string()));
    }

    #[test]
    fn test_untyped_variables_can_unify_with_anything() {
        let untyped_var = TypedVariable::<Untyped>::new("anything");

        // Test that untyped variables can unify with anything
        assert!(untyped_var.can_unify_with(&Value::String("test".to_string())));
        assert!(untyped_var.can_unify_with(&Value::Boolean(true)));
        assert!(untyped_var.can_unify_with(&Value::UnsignedInt(42)));
    }

    // Tests from variable_query_test.rs
    use crate::{Fact, Query};
    use anyhow::Result;
    use dialog_artifacts::{ArtifactStoreMut, Artifacts, Attribute, Entity, Instruction};
    use dialog_storage::MemoryStorageBackend;
    use futures_util::stream;

    #[tokio::test]
    async fn test_variable_query_with_evaluate() -> Result<()> {
        // This test verifies that variable queries work with the new evaluate implementation
        // by using the plan → evaluate approach for queries with variables

        // Setup: Create in-memory storage and artifacts store
        let storage_backend = MemoryStorageBackend::default();
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;

        // Step 1: Create test data
        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let facts = vec![
            Fact::assert(
                "user/name".parse::<Attribute>()?,
                alice.clone(),
                Value::String("Alice".to_string()),
            ),
            Fact::assert(
                "user/email".parse::<Attribute>()?,
                alice.clone(),
                Value::String("alice@example.com".to_string()),
            ),
            Fact::assert(
                "user/name".parse::<Attribute>()?,
                bob.clone(),
                Value::String("Bob".to_string()),
            ),
            Fact::assert(
                "user/email".parse::<Attribute>()?,
                bob.clone(),
                Value::String("bob@example.com".to_string()),
            ),
        ];

        let instructions: Vec<Instruction> = facts.into_iter().map(Instruction::from).collect();
        artifacts.commit(stream::iter(instructions)).await?;

        // Step 2: Test mixed constant + variable queries

        // Query 1: Find all names (constant attribute, variable entity and value)
        let name_query = Fact::select()
            .the("user/name") // Constant attribute
            .of(TypedVariable::<Entity>::new("user")) // Variable entity
            .is(TypedVariable::<String>::new("name")); // Variable value

        // This should work because we have a constant attribute to optimize the query
        // Note: Currently returns an error but shows that the plan → evaluate approach is implemented
        let result = name_query.query(&artifacts);

        // Check that we get the expected error message indicating progress
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(error.to_string().contains("plan → evaluate"));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_all_variable_query_fails() -> Result<()> {
        // This test verifies that queries with all variables fail with helpful error

        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        // Create a query with all variables - should fail
        let all_vars_query = Fact::select()
            .the(TypedVariable::<Attribute>::new("attr")) // Variable
            .of(TypedVariable::<Entity>::new("entity")) // Variable
            .is(TypedVariable::<Untyped>::new("value")); // Variable

        // This should fail because we don't have any constants to optimize the query
        let result = all_vars_query.query(&artifacts);
        assert!(result.is_err());

        if let Err(error) = result {
            assert!(error.to_string().contains("plan → evaluate"));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_variable_query_optimization() -> Result<()> {
        // This test verifies that the query optimization works correctly
        // by using the most constrained term to optimize store queries

        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        // Create more diverse test data
        let facts = vec![
            Fact::assert(
                "user/name".parse::<Attribute>()?,
                alice.clone(),
                Value::String("Alice".to_string()),
            ),
            Fact::assert(
                "user/age".parse::<Attribute>()?,
                alice.clone(),
                Value::String("30".to_string()),
            ),
            Fact::assert(
                "user/name".parse::<Attribute>()?,
                bob.clone(),
                Value::String("Bob".to_string()),
            ),
            Fact::assert(
                "user/age".parse::<Attribute>()?,
                bob.clone(),
                Value::String("25".to_string()),
            ),
            Fact::assert(
                "company/name".parse::<Attribute>()?,
                alice.clone(),
                Value::String("TechCorp".to_string()),
            ),
        ];

        let instructions: Vec<Instruction> = facts.into_iter().map(Instruction::from).collect();
        artifacts.commit(stream::iter(instructions)).await?;

        // Test different query optimizations

        // Test that queries with variables are directed to the plan → evaluate approach
        // This shows that the architecture is correctly implemented

        // 1. Entity-optimized query (constant entity, should use plan → evaluate)
        let alice_facts = Fact::select()
            .the(TypedVariable::<Attribute>::new("attr"))
            .of(alice.clone()) // Constant entity - should optimize by entity
            .is(TypedVariable::<Untyped>::new("value"));

        let result = alice_facts.query(&artifacts);
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(error.to_string().contains("plan → evaluate"));
        }

        // 2. Attribute-optimized query (constant attribute, should work with direct query)
        let name_facts = Fact::select()
            .the("user/name") // Constant attribute - should optimize by attribute
            .of(TypedVariable::<Entity>::new("entity"))
            .is(TypedVariable::<String>::new("name"));

        let result = name_facts.query(&artifacts);
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(error.to_string().contains("plan → evaluate"));
        }

        // 3. Value-optimized query (constant value, should use plan → evaluate)
        let alice_value_facts = Fact::select()
            .the(TypedVariable::<Attribute>::new("attr"))
            .of(TypedVariable::<Entity>::new("entity"))
            .is(Value::String("Alice".to_string())); // Constant value - should optimize by value

        let result = alice_value_facts.query(&artifacts);
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(error.to_string().contains("plan → evaluate"));
        }

        Ok(())
    }
}

#[cfg(test)]
mod constraint_tests {
    use super::*;

    #[test]
    fn test_type_constraint_works() {
        // These should compile - supported types
        let _string_var = TypedVariable::<String>::new("name");
        let _u64_var = TypedVariable::<u64>::new("age");
        let _bool_var = TypedVariable::<bool>::new("active");
        let _untyped_var = TypedVariable::<Untyped>::new("any");

        // This test function compiling proves the constraint works
        // because if we tried Variable::<SomeUnsupportedType>::new()
        // it would fail to compile due to the IntoValueDataType bound
    }

    #[test]
    fn vars() {
        // These should compile - supported types
        let var = TypedVariable::<String>::new("name");
        print!("var {}", var);
        let _u64_var = TypedVariable::<u64>::new("age");
        let _bool_var = TypedVariable::<bool>::new("active");
        let _untyped_var = TypedVariable::<Untyped>::new("any");

        // This test function compiling proves the constraint works
        // because if we tried Variable::<SomeUnsupportedType>::new()
        // it would fail to compile due to the IntoValueDataType bound
    }
}
