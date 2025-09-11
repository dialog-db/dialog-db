// Test to verify the Rule macro generates correct code with the fixes
use dialog_query::concept::{Attributes, Concept};
use dialog_query::rule::Rule as RuleTrait;
use dialog_query::term::Term;
use dialog_query_macros::Rule;

#[derive(Rule, Debug, Clone)]
pub struct TestPerson {
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
    assert_eq!(TestPerson::name(), "test.person");

    // Test that r#match works
    let _attributes = TestPersonAttributes::of(entity);

    // Test that Rule trait is implemented
    let when = TestPerson::when(person_match);
    assert_eq!(when.len(), 2); // Should have 2 statements for name and age

    // Test Instance trait (note: this will panic with our placeholder implementation)
    let _instance = TestPerson {
        name: "Alice".to_string(),
        age: 30,
    };

    println!("Macro expansion successful!");
    println!("Generated namespace: {}", TestPerson::name());
    println!("Number of when statements: {}", when.len());
}
