use dialog_query::rule::{Match, Premises};
use dialog_query::{Concept, Entity, Term, Type};

#[derive(Concept, Debug, Clone)]
pub struct Person {
    /// Person entity
    pub this: Entity,

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
    let person_match = Match::<Person> {
        this: entity.clone(),
        name: Term::var("person_name"),
        birthday: Term::var("person_birthday"),
    };

    // Test that Match implements Premises
    let statements: Vec<_> = person_match.premises().collect();
    assert_eq!(statements.len(), 2); // Should have 2 statements for name and birthday

    // Test that Person implements Concept
    let concept = Person::concept();
    assert_eq!(concept.operator(), "person");

    // Test the attributes() method
    let attrs = concept.attributes().iter().collect::<Vec<_>>();

    assert_eq!(attrs.len(), 2);
    assert_eq!(attrs[0].0, "name");
    assert_eq!(attrs[0].1.namespace, "person");
    assert_eq!(attrs[0].1.name, "name");
    assert_eq!(attrs[0].1.description, "Name of the person");
    assert_eq!(attrs[0].1.content_type(), Some(Type::String));
    assert_eq!(attrs[1].0, "birthday");
    assert_eq!(attrs[1].1.namespace, "person");
    assert_eq!(attrs[1].1.name, "birthday");
    assert_eq!(attrs[1].1.description, "Birthday of the person");
    assert_eq!(attrs[1].1.content_type(), Some(Type::UnsignedInt));

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
fn test_static_attributes_generation() {
    // Test that static attributes are generated correctly with prefixed names
    // The prefixed attributes should exist and be accessible
    assert_eq!(PERSON_NAME.namespace, "person");
    assert_eq!(PERSON_NAME.name, "name");
    assert_eq!(PERSON_BIRTHDAY.namespace, "person");
    assert_eq!(PERSON_BIRTHDAY.name, "birthday");
}
