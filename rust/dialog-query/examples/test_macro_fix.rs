// Test to verify the Rule macro generates correct code with the fixes
use dialog_query::concept::ConceptType;
use dialog_query::rule::Rule as RuleTrait;
use dialog_query::term::Term;
use dialog_query::Entity;
use dialog_query_macros::Rule;

#[derive(Rule, Debug, Clone)]
pub struct TestPerson {
    pub this: Entity,
    pub name: String,
    pub age: u32,
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
    assert_eq!(TestPerson::operator(), "test.person");

    // Test that Rule trait is implemented
    let when = TestPerson::when(person_match);
    assert_eq!(when.len(), 2); // Should have 2 statements for name and age
}
