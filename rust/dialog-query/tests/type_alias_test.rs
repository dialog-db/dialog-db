//! Test that type aliases work correctly with IntoType trait-based type detection

use dialog_query::{artifact::Entity, Concept};

// Type alias - with trait-based detection, this should properly resolve to String's type
type PersonName = String;
type Age = u32;

#[derive(Concept, Debug, Clone)]
pub struct PersonWithAliases {
    pub this: Entity,
    pub name: PersonName,
    pub age: Age,
}

#[test]
fn test_type_aliases_resolve_correctly() {
    use dialog_query::types::IntoType;

    // Type aliases should resolve to their underlying type's IntoType::TYPE
    assert_eq!(
        <PersonName as IntoType>::TYPE,
        Some(dialog_query::artifact::Type::String),
        "PersonName (alias for String) should have Type::String"
    );

    assert_eq!(
        <Age as IntoType>::TYPE,
        Some(dialog_query::artifact::Type::UnsignedInt),
        "Age (alias for u32) should have Type::UnsignedInt"
    );
}
