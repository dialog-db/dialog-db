// Test to verify the Concept macro generates correct code with the fixes
use dialog_query::{Attribute, Concept, Entity, Term};

mod test_person {
    use dialog_query::Attribute;

    #[derive(Attribute, Clone)]
    pub struct Name(pub String);

    #[derive(Attribute, Clone)]
    pub struct Age(pub u32);
}

#[derive(Concept, Debug, Clone)]
pub struct TestPerson {
    pub this: Entity,
    pub name: test_person::Name,
    pub age: test_person::Age,
}

fn main() {
    // Test that the macro generates the correct types
    let entity = Term::var("person");

    // Test Match struct generation
    let person_match = TestPersonMatch {
        this: entity.clone(),
        name: Term::var("name"),
        age: Term::var("age"),
    };

    // Test that the Concept trait is implemented
    let concept = TestPerson::CONCEPT;
    assert_eq!(concept.operator(), "test-person");

    // Test that Rule trait is implemented
    let when = TestPerson::when(person_match);
    assert_eq!(when.len(), 2); // Should have 2 statements for name and age
}
