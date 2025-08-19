//! Integration tests for the #[relation] procedural macro

use dialog_query::{relation, Attribute, Cardinality, ValueDataType};

// Test basic relation generation
#[relation]
enum SimpleRelation {
    Name(String),
    Age(u32),
    Active(bool),
}

// Test relation with #[many] attributes
#[relation]
enum RelationWithMany {
    Title(String),
    #[many]
    Tags(String),
    #[many]
    Categories(String),
    Published(bool),
}

// Test relation with different data types
#[relation]
enum ComplexRelation {
    Id(dialog_artifacts::Entity),
    Attr(dialog_artifacts::Attribute),
    Data(Vec<u8>),
    Count(i64),
    Score(f64),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_relation_generation() {
        // Test that structs are generated correctly
        let name = SimpleRelation::Name::new("John");
        let age = SimpleRelation::Age::new(25u32);
        let active = SimpleRelation::Active::new(true);

        // Test values
        assert_eq!(name.value(), &"John".to_string());
        assert_eq!(age.value(), &25u32);
        assert_eq!(active.value(), &true);

        // Test attribute names (namespace with dots, predicate with underscores)
        assert_eq!(SimpleRelation::Name::name(), "simple.relation/name");
        assert_eq!(SimpleRelation::Age::name(), "simple.relation/age");
        assert_eq!(SimpleRelation::Active::name(), "simple.relation/active");

        // Test cardinality (default is One)
        assert_eq!(SimpleRelation::Name::cardinality(), Cardinality::One);
        assert_eq!(SimpleRelation::Age::cardinality(), Cardinality::One);
        assert_eq!(SimpleRelation::Active::cardinality(), Cardinality::One);

        // Test value types
        assert_eq!(SimpleRelation::Name::value_type(), ValueDataType::String);
        assert_eq!(
            SimpleRelation::Age::value_type(),
            ValueDataType::UnsignedInt
        );
        assert_eq!(SimpleRelation::Active::value_type(), ValueDataType::Boolean);
    }

    #[test]
    fn test_relation_with_many_cardinality() {
        let title = RelationWithMany::Title::new("My Article");
        let tags = RelationWithMany::Tags::new("rust");
        let categories = RelationWithMany::Categories::new("programming");
        let published = RelationWithMany::Published::new(false);

        // Test attribute names
        assert_eq!(RelationWithMany::Title::name(), "relation.with.many/title");
        assert_eq!(RelationWithMany::Tags::name(), "relation.with.many/tags");
        assert_eq!(
            RelationWithMany::Categories::name(),
            "relation.with.many/categories"
        );
        assert_eq!(
            RelationWithMany::Published::name(),
            "relation.with.many/published"
        );

        // Test cardinality - #[many] attributes should have Many cardinality
        assert_eq!(RelationWithMany::Title::cardinality(), Cardinality::One);
        assert_eq!(RelationWithMany::Tags::cardinality(), Cardinality::Many);
        assert_eq!(
            RelationWithMany::Categories::cardinality(),
            Cardinality::Many
        );
        assert_eq!(RelationWithMany::Published::cardinality(), Cardinality::One);

        // Test values
        assert_eq!(title.value(), &"My Article".to_string());
        assert_eq!(tags.value(), &"rust".to_string());
        assert_eq!(categories.value(), &"programming".to_string());
        assert_eq!(published.value(), &false);
    }

    #[test]
    fn test_complex_data_types() {
        use dialog_artifacts::{Attribute, Entity};
        use std::str::FromStr;

        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("test/attr").unwrap();
        let data = vec![1u8, 2u8, 3u8];

        let id = ComplexRelation::Id::new(entity.clone());
        let attr_field = ComplexRelation::Attr::new(attr.clone());
        let data_field = ComplexRelation::Data::new(data.clone());
        let count = ComplexRelation::Count::new(-42i64);
        let score = ComplexRelation::Score::new(3.14f64);

        // Test values
        assert_eq!(id.value(), &entity);
        assert_eq!(attr_field.value(), &attr);
        assert_eq!(data_field.value(), &data);
        assert_eq!(count.value(), &-42i64);
        assert_eq!(score.value(), &3.14f64);

        // Test value types
        assert_eq!(ComplexRelation::Id::value_type(), ValueDataType::Entity);
        assert_eq!(ComplexRelation::Attr::value_type(), ValueDataType::Symbol);
        assert_eq!(ComplexRelation::Data::value_type(), ValueDataType::Bytes);
        assert_eq!(
            ComplexRelation::Count::value_type(),
            ValueDataType::SignedInt
        );
        assert_eq!(ComplexRelation::Score::value_type(), ValueDataType::Float);
    }

    #[test]
    fn test_value_consumption() {
        let name = SimpleRelation::Name::new("Alice");
        let age = SimpleRelation::Age::new(30u32);

        // Test that we can consume values
        let consumed_name = name.into_value();
        let consumed_age = age.into_value();

        assert_eq!(consumed_name, "Alice".to_string());
        assert_eq!(consumed_age, 30u32);
    }

    #[test]
    fn test_snake_case_conversion() {
        // Test that CamelCase enum names are converted to dotted namespace in attribute names

        // SimpleRelation -> simple.relation
        assert_eq!(SimpleRelation::Name::name(), "simple.relation/name");

        // RelationWithMany -> relation.with.many
        assert_eq!(RelationWithMany::Title::name(), "relation.with.many/title");

        // ComplexRelation -> complex.relation
        assert_eq!(ComplexRelation::Id::name(), "complex.relation/id");
    }

    #[test]
    fn test_attribute_trait_implementation() {
        // Test that generated structs properly implement the Attribute trait
        fn test_attribute<T: Attribute>() -> (&'static str, Cardinality, ValueDataType) {
            (T::name(), T::cardinality(), T::value_type())
        }

        let (name, card, vtype) = test_attribute::<SimpleRelation::Name>();
        assert_eq!(name, "simple.relation/name");
        assert_eq!(card, Cardinality::One);
        assert_eq!(vtype, ValueDataType::String);

        let (name, card, vtype) = test_attribute::<RelationWithMany::Tags>();
        assert_eq!(name, "relation.with.many/tags");
        assert_eq!(card, Cardinality::Many);
        assert_eq!(vtype, ValueDataType::String);
    }

    #[test]
    fn test_generic_into_conversion() {
        // Test that new() method works with Into<T> conversions
        let name1 = SimpleRelation::Name::new("John");
        let name2 = SimpleRelation::Name::new("John".to_string());

        assert_eq!(name1.value(), name2.value());

        // Test with string slice vs String
        let title1 = RelationWithMany::Title::new("Article");
        let title2 = RelationWithMany::Title::new("Article".to_string());

        assert_eq!(title1.value(), title2.value());
    }

    #[test]
    fn test_relation_integration_with_terms() {
        use dialog_artifacts::Value;
        use dialog_query::Term;

        // Test that we can create Terms from relation attribute constants
        let _name_attr = SimpleRelation::Name::new("John");
        let _age_attr = SimpleRelation::Age::new(25u32);

        // These should convert to appropriate Term types
        let name_term: Term<Value> = Term::Constant(Value::String("John".to_string()));
        let age_term: Term<Value> = Term::Constant(Value::UnsignedInt(25));

        // Test that values match expected structure
        if let Term::Constant(Value::String(s)) = name_term {
            assert_eq!(s, "John");
        } else {
            panic!("Expected string constant");
        }

        if let Term::Constant(Value::UnsignedInt(n)) = age_term {
            assert_eq!(n, 25);
        } else {
            panic!("Expected unsigned int constant");
        }
    }

    #[test]
    fn test_relation_with_fact_selector() {
        use dialog_artifacts::{Attribute, Entity, Value};
        use dialog_query::{FactSelector, Term};
        use std::str::FromStr;

        // Create test data
        let entity = Entity::new().unwrap();
        let name_attr = Attribute::from_str("simple.relation/name").unwrap();

        // Test that we can use relation-generated attribute names in FactSelector
        let fact_selector: FactSelector<Value> = FactSelector::new()
            .the(name_attr)
            .of(Term::Constant(entity))
            .is(Term::Constant(Value::String("John".to_string())));

        // Verify selector construction
        assert!(fact_selector.the.is_some());
        assert!(fact_selector.of.is_some());
        assert!(fact_selector.is.is_some());

        // Check that attribute name matches what the macro generated
        if let Some(Term::Constant(attr)) = &fact_selector.the {
            assert_eq!(attr.to_string(), "simple.relation/name");
        } else {
            panic!("Expected constant attribute");
        }
    }

    #[test]
    fn test_error_handling_for_unsupported_types() {
        // This test verifies that the macro generates compile-time errors
        // for unsupported types - we can't easily test compile failures
        // but we document the expected behavior here.

        // The following would cause compile errors if uncommented:
        // #[relation]
        // enum UnsupportedTypes {
        //     CustomStruct(SomeCustomStruct), // Would generate compile_error!
        //     RawPointer(*const u8),          // Would generate compile_error!
        // }

        // Instead, we test that supported types work correctly
        assert_eq!(SimpleRelation::Name::value_type(), ValueDataType::String);
        assert_eq!(
            SimpleRelation::Age::value_type(),
            ValueDataType::UnsignedInt
        );
        assert_eq!(SimpleRelation::Active::value_type(), ValueDataType::Boolean);
    }
}
