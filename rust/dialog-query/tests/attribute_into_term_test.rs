//! Test that Attribute can be converted into Term via .into()

use dialog_query::{Attribute, Term};

mod employee {
    use dialog_query::Attribute;

    #[derive(Attribute, Clone)]
    pub struct Name(pub String);

    #[derive(Attribute, Clone)]
    pub struct Job(pub String);

    #[derive(Attribute, Clone)]
    pub struct Salary(pub u32);
}

#[test]
fn test_attribute_into_term() {
    // Test String attribute
    let name = employee::Name("Alice".into());
    let name_term: Term<String> = name.into();
    assert!(name_term.is_constant());

    // Test another String attribute
    let job = employee::Job("Engineer".into());
    let job_term: Term<String> = job.into();
    assert!(job_term.is_constant());

    // Test u32 attribute
    let salary = employee::Salary(65000);
    let salary_term: Term<u32> = salary.into();
    assert!(salary_term.is_constant());
}

#[test]
fn test_attribute_from_method() {
    // Test using the from method - much cleaner!
    let name = employee::Name::from("Alice");
    assert_eq!(name.value(), "Alice");

    let job = employee::Job::from("Engineer");
    assert_eq!(job.value(), "Engineer");

    let salary = employee::Salary::from(65000u32);
    assert_eq!(*salary.value(), 65000);

    // Can also chain with .into() for Term conversion
    let name_term: Term<String> = employee::Name::from("Bob").into();
    assert!(name_term.is_constant());
}

#[test]
fn test_attribute_into_in_match_construction() {
    use dialog_query::{Concept, Entity, Match};

    #[derive(Concept, Debug, Clone)]
    pub struct Employee {
        pub this: Entity,
        pub name: employee::Name,
        pub job: employee::Job,
        pub salary: employee::Salary,
    }

    // Can use .into() when constructing Match patterns
    let pattern = Match::<Employee> {
        this: Term::var("e"),
        name: Term::var("name"),
        salary: Term::var("salary"),
        job: employee::Job("Engineer".into()).into(),
    };

    // Verify the job term is a constant
    assert!(pattern.job.is_constant());

    // Even cleaner with ::from() and .into()
    let pattern2 = Match::<Employee> {
        this: Term::var("e"),
        name: Term::var("name"),
        salary: Term::var("salary"),
        job: employee::Job::from("Engineer").into(),
    };

    assert!(pattern2.job.is_constant());
}
