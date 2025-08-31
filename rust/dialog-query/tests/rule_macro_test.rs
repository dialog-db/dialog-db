use dialog_query::attribute::Cardinality;
use dialog_query::concept::Concept;
use dialog_query::rule::{Match, Rule};
use dialog_query::*;
use dialog_query_macros::Rule;

#[derive(Rule, Debug, Clone)]
pub struct Person {
    /// Name of the person
    pub name: String,
    /// Birthday of the person
    pub birthday: u32,
}

#[test]
fn test_derive_rule_generates_types() {
    // Test that the generated module and types exist
    let entity = Term::var("person_entity");

    // Test the generated Match struct
    let person_match = PersonMatch {
        this: entity.clone(),
        name: Term::var("person_name"),
        birthday: Term::var("person_birthday"),
    };

    // Test that Match implements Statements
    let statements: Vec<_> = person_match.statements().collect();
    assert_eq!(statements.len(), 2); // Should have 2 statements for name and birthday

    // Test that Person implements Concept
    assert_eq!(Person::name(), "person");

    // Test the attributes() method
    let attrs = Person::attributes();
    assert_eq!(attrs.len(), 2);
    assert_eq!(attrs[0].namespace, "person");
    assert_eq!(attrs[0].name, "name");
    assert_eq!(attrs[0].description, "Name of the person");
    assert_eq!(attrs[0].data_type(), Some(ValueDataType::String));
    assert_eq!(attrs[1].namespace, "person");
    assert_eq!(attrs[1].name, "birthday");
    assert_eq!(attrs[1].description, "Birthday of the person");
    assert_eq!(attrs[1].data_type(), Some(ValueDataType::UnsignedInt));

    // Test the r#match function
    let _attributes = Person::r#match(entity.clone());
    // The attributes should be created successfully
    assert_eq!(_attributes.name.the(), "person/name");
    assert_eq!(_attributes.birthday.the(), "person/birthday");
    assert_eq!(_attributes.name.attribute.cardinality, Cardinality::One);
    assert_eq!(_attributes.name.attribute.description, "Name of the person");
    assert_eq!(_attributes.birthday.attribute.cardinality, Cardinality::One);
    assert_eq!(
        _attributes.birthday.attribute.description,
        "Birthday of the person"
    );

    // Test that Person implements Rule
    let test_match = Match::<Person> {
        this: Term::var("person"),
        name: Term::var("name"),
        birthday: Term::var("birthday"),
    };

    let when_result = Person::when(test_match);
    assert_eq!(when_result.len(), 2); // Should have 2 field statements
}

#[test]
fn test_namespace_generation() {
    // Test that namespace constant is generated correctly
    assert_eq!(person::NAMESPACE, "person");
}
