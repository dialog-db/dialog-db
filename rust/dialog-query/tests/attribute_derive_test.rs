use dialog_query::{Attribute, Cardinality};

mod employee {
    use super::*;

    /// Name of the employee
    #[derive(Attribute, Clone)]
    pub struct Name(pub String);

    /// Job title of the employee
    #[derive(Attribute, Clone)]
    pub struct Job(pub String);

    /// Salary of the employee
    #[derive(Attribute, Clone)]
    pub struct Salary(pub u32);
}

mod person {
    use super::*;

    /// Name of the person
    #[derive(Attribute, Clone)]
    pub struct Name(pub String);

    /// Employees managed by this person
    #[derive(Attribute, Clone)]
    #[cardinality(many)]
    pub struct Manages(pub dialog_query::Entity);
}

#[test]
fn test_employee_name_derives_attribute() {
    let name = employee::Name("Alice".to_string());

    // Test namespace - should extract from module name "employee"
    assert_eq!(employee::Name::namespace(), "employee");

    // Test name
    assert_eq!(employee::Name::name(), "name");

    // Test description
    assert_eq!(employee::Name::description(), "Name of the employee");

    // Test cardinality - should default to One
    assert_eq!(employee::Name::cardinality(), Cardinality::One);

    // Test value
    assert_eq!(name.value(), "Alice");

    // Test selector
    assert_eq!(employee::Name::selector().to_string(), "employee/name");
}

#[test]
fn test_employee_job_derives_attribute() {
    let job = employee::Job("Engineer".to_string());

    assert_eq!(employee::Job::namespace(), "employee");
    assert_eq!(employee::Job::name(), "job");
    assert_eq!(employee::Job::description(), "Job title of the employee");
    assert_eq!(employee::Job::cardinality(), Cardinality::One);
    assert_eq!(job.value(), "Engineer");
    assert_eq!(employee::Job::selector().to_string(), "employee/job");
}

#[test]
fn test_employee_salary_derives_attribute() {
    let salary = employee::Salary(100000);

    assert_eq!(employee::Salary::namespace(), "employee");
    assert_eq!(employee::Salary::name(), "salary");
    assert_eq!(employee::Salary::description(), "Salary of the employee");
    assert_eq!(employee::Salary::cardinality(), Cardinality::One);
    assert_eq!(salary.value(), &100000u32);
    assert_eq!(
        employee::Salary::selector().to_string(),
        "employee/salary"
    );
}

#[test]
fn test_person_namespace() {
    let name = person::Name("Bob".to_string());

    // Should extract namespace from module name "person"
    assert_eq!(person::Name::namespace(), "person");
    assert_eq!(person::Name::name(), "name");
    assert_eq!(person::Name::selector().to_string(), "person/name");
    assert_eq!(name.value(), "Bob");
}

#[test]
fn test_cardinality_many() {
    // Test that cardinality(many) works
    assert_eq!(person::Manages::cardinality(), Cardinality::Many);
    assert_eq!(
        person::Manages::description(),
        "Employees managed by this person"
    );
    assert_eq!(person::Manages::namespace(), "person");
}

mod custom_ns_test {
    use super::*;

    /// Custom namespace override test
    #[derive(Attribute, Clone)]
    #[namespace = "custom"]
    pub struct Field(pub String);
}

#[test]
fn test_custom_namespace_override() {
    let field = custom_ns_test::Field("value".to_string());

    // Should use the explicit namespace override, not the module name
    assert_eq!(custom_ns_test::Field::namespace(), "custom");
    assert_eq!(custom_ns_test::Field::name(), "field");
    assert_eq!(
        custom_ns_test::Field::selector().to_string(),
        "custom/field"
    );
    assert_eq!(field.value(), "value");
}
