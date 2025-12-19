use dialog_query::{artifact::Entity, Attribute, Concept, Type};

mod person_with_aliases {
    use dialog_query::Attribute;
    type PersonName = String;
    type Years = u32;

    #[derive(Attribute, Clone, PartialEq)]
    pub struct Name(pub PersonName);

    #[derive(Attribute, Clone, PartialEq)]
    pub struct Age(pub Years);
}

#[derive(Concept, Debug, Clone)]
pub struct PersonWithAliases {
    pub this: Entity,
    pub name: person_with_aliases::Name,
    pub age: person_with_aliases::Age,
}

#[test]
fn test_attribute_types_resolve_correctly() {
    // Attribute types should have their wrapped type available via IntoType
    assert_eq!(
        person_with_aliases::Age::content_type(),
        Some(Type::UnsignedInt),
        "u32 should have Type::UnsignedInt"
    );

    assert_eq!(
        person_with_aliases::Name::content_type(),
        Some(Type::String),
        "String should have Type::String"
    );
}
