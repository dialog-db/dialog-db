//! Integration tests for the formula system
//!
//! These tests demonstrate how formulas integrate with the query engine,
//! session management, and the overall dialog-query system.

use anyhow::Result;
use dialog_query::{
    artifact::{Artifacts, Attribute, Entity, Value},
    formulas::*,
    selection::Answer,
    Formula, Parameters, Relation, Session, Term,
};
use dialog_storage::MemoryStorageBackend;

#[dialog_macros::test]
async fn test_formula_integration_math_operations() -> Result<()> {
    // Setup: Create in-memory storage and session
    let storage_backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(storage_backend).await?;

    // Create entities for our test data
    let calculation_1 = Entity::new()?;
    let calculation_2 = Entity::new()?;

    // Store some basic math data
    let claims = vec![
        // First calculation: 10 + 5
        Relation {
            the: "calc/operand1".parse::<Attribute>()?,
            of: calculation_1.clone(),
            is: Value::UnsignedInt(10),
        },
        Relation {
            the: "calc/operand2".parse::<Attribute>()?,
            of: calculation_1.clone(),
            is: Value::UnsignedInt(5),
        },
        // Second calculation: 20 - 8
        Relation {
            the: "calc/operand1".parse::<Attribute>()?,
            of: calculation_2.clone(),
            is: Value::UnsignedInt(20),
        },
        Relation {
            the: "calc/operand2".parse::<Attribute>()?,
            of: calculation_2.clone(),
            is: Value::UnsignedInt(8),
        },
    ];

    let mut session = Session::open(artifacts.clone());
    session.transact(claims).await?;

    // Test Sum formula
    let mut sum_terms = Parameters::new();
    sum_terms.insert("of".to_string(), Term::var("x"));
    sum_terms.insert("with".to_string(), Term::var("y"));
    sum_terms.insert("is".to_string(), Term::var("sum_result"));

    let sum_formula = Sum::apply(sum_terms)?;

    // Create input for sum: 10 + 5
    let sum_input = Answer::new()
        .set(Term::var("x"), 10u32)
        .unwrap()
        .set(Term::var("y"), 5u32)
        .unwrap();

    let sum_results = sum_formula.derive(sum_input)?;
    assert_eq!(sum_results.len(), 1);
    assert_eq!(
        sum_results[0].get::<u32>(&Term::var("sum_result")).ok(),
        Some(15)
    );

    // Test Difference formula
    let mut diff_terms = Parameters::new();
    diff_terms.insert("of".to_string(), Term::var("a"));
    diff_terms.insert("subtract".to_string(), Term::var("b"));
    diff_terms.insert("is".to_string(), Term::var("diff_result"));

    let diff_formula = Difference::apply(diff_terms)?;

    // Create input for difference: 20 - 8
    let diff_input = Answer::new()
        .set(Term::var("a"), 20u32)
        .unwrap()
        .set(Term::var("b"), 8u32)
        .unwrap();

    let diff_results = diff_formula.derive(diff_input)?;
    assert_eq!(diff_results.len(), 1);
    assert_eq!(
        diff_results[0].get::<u32>(&Term::var("diff_result")).ok(),
        Some(12)
    );

    // Test Product formula: 6 * 7
    let mut prod_terms = Parameters::new();
    prod_terms.insert("of".to_string(), Term::var("p"));
    prod_terms.insert("times".to_string(), Term::var("q"));
    prod_terms.insert("is".to_string(), Term::var("product"));

    let product_formula = Product::apply(prod_terms)?;

    let prod_input = Answer::new()
        .set(Term::var("p"), 6u32)
        .unwrap()
        .set(Term::var("q"), 7u32)
        .unwrap();

    let prod_results = product_formula.derive(prod_input)?;
    assert_eq!(prod_results.len(), 1);
    assert_eq!(
        prod_results[0].get::<u32>(&Term::var("product")).ok(),
        Some(42)
    );

    Ok(())
}

#[dialog_macros::test]
async fn test_formula_integration_string_operations() -> Result<()> {
    // Setup
    let storage_backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(storage_backend).await?;

    let user_1 = Entity::new()?;
    let user_2 = Entity::new()?;

    // Store user name data
    let claims = vec![
        Relation {
            the: "user/first_name".parse::<Attribute>()?,
            of: user_1.clone(),
            is: Value::String("John".to_string()),
        },
        Relation {
            the: "user/last_name".parse::<Attribute>()?,
            of: user_1.clone(),
            is: Value::String("Doe".to_string()),
        },
        Relation {
            the: "user/first_name".parse::<Attribute>()?,
            of: user_2.clone(),
            is: Value::String("Jane".to_string()),
        },
        Relation {
            the: "user/last_name".parse::<Attribute>()?,
            of: user_2.clone(),
            is: Value::String("Smith".to_string()),
        },
    ];

    let mut session = Session::open(artifacts.clone());
    session.transact(claims).await?;

    // Test Concatenate formula to build full names
    let mut concat_terms = Parameters::new();
    concat_terms.insert("first".to_string(), Term::var("fname"));
    concat_terms.insert("second".to_string(), Term::var("lname"));
    concat_terms.insert("is".to_string(), Term::var("full_name"));

    let concat_formula = Concatenate::apply(concat_terms)?;

    // Test concatenating "John" + " Doe"
    let concat_input = Answer::new()
        .set(Term::var("fname"), "John".to_string())
        .unwrap()
        .set(Term::var("lname"), " Doe".to_string())
        .unwrap();

    let concat_results = concat_formula.derive(concat_input)?;
    assert_eq!(concat_results.len(), 1);
    assert_eq!(
        concat_results[0]
            .get::<String>(&Term::var("full_name"))
            .ok(),
        Some("John Doe".to_string())
    );

    // Test Length formula
    let mut length_terms = Parameters::new();
    length_terms.insert("of".to_string(), Term::var("text"));
    length_terms.insert("is".to_string(), Term::var("length"));

    let length_formula = Length::apply(length_terms)?;

    let length_input = Answer::new()
        .set(Term::var("text"), "Hello World".to_string())
        .unwrap();

    let length_results = length_formula.derive(length_input)?;
    assert_eq!(length_results.len(), 1);
    assert_eq!(
        length_results[0].get::<u32>(&Term::var("length")).ok(),
        Some(11)
    );

    // Test case conversion formulas
    let mut upper_terms = Parameters::new();
    upper_terms.insert("of".to_string(), Term::var("input"));
    upper_terms.insert("is".to_string(), Term::var("output"));

    let upper_formula = Uppercase::apply(upper_terms)?;

    let upper_input = Answer::new()
        .set(Term::var("input"), "hello world".to_string())
        .unwrap();

    let upper_results = upper_formula.derive(upper_input)?;
    assert_eq!(upper_results.len(), 1);
    assert_eq!(
        upper_results[0].get::<String>(&Term::var("output")).ok(),
        Some("HELLO WORLD".to_string())
    );

    Ok(())
}

#[dialog_macros::test]
async fn test_formula_integration_type_conversions() -> Result<()> {
    // Test ToString formula with different types
    let mut to_string_terms = Parameters::new();
    to_string_terms.insert("value".to_string(), Term::var("input"));
    to_string_terms.insert("is".to_string(), Term::var("str_result"));

    let to_string_formula = ToString::apply(to_string_terms)?;

    // Test with number
    let number_input = Answer::new().set(Term::var("input"), 42u32).unwrap();

    let string_results = to_string_formula.derive(number_input)?;
    assert_eq!(string_results.len(), 1);
    assert_eq!(
        string_results[0]
            .get::<String>(&Term::var("str_result"))
            .ok(),
        Some("42".to_string())
    );

    // Test with boolean
    let bool_input = Answer::new().set(Term::var("input"), true).unwrap();

    let bool_string_results = to_string_formula.derive(bool_input)?;
    assert_eq!(bool_string_results.len(), 1);
    assert_eq!(
        bool_string_results[0]
            .get::<String>(&Term::var("str_result"))
            .ok(),
        Some("true".to_string())
    );

    // Test ParseNumber formula
    let mut parse_terms = Parameters::new();
    parse_terms.insert("text".to_string(), Term::var("str_input"));
    parse_terms.insert("is".to_string(), Term::var("num_result"));

    let parse_formula = ParseNumber::apply(parse_terms)?;

    // Test valid number parsing
    let parse_input = Answer::new()
        .set(Term::var("str_input"), "123".to_string())
        .unwrap();

    let parse_results = parse_formula.derive(parse_input)?;
    assert_eq!(parse_results.len(), 1);
    assert_eq!(
        parse_results[0].get::<u32>(&Term::var("num_result")).ok(),
        Some(123)
    );

    // Test invalid number parsing (should return empty)
    let invalid_input = Answer::new()
        .set(Term::var("str_input"), "not a number".to_string())
        .unwrap();

    let invalid_results = parse_formula.derive(invalid_input)?;
    assert_eq!(invalid_results.len(), 0); // Empty for invalid parsing

    Ok(())
}

#[dialog_macros::test]
async fn test_formula_integration_boolean_logic() -> Result<()> {
    // Test boolean logic formulas

    // Test And formula
    let mut and_terms = Parameters::new();
    and_terms.insert("left".to_string(), Term::var("a"));
    and_terms.insert("right".to_string(), Term::var("b"));
    and_terms.insert("is".to_string(), Term::var("and_result"));

    let and_formula = And::apply(and_terms)?;

    // Test true AND true = true
    let and_input = Answer::new()
        .set(Term::var("a"), true)
        .unwrap()
        .set(Term::var("b"), true)
        .unwrap();

    let and_results = and_formula.derive(and_input)?;
    assert_eq!(and_results.len(), 1);
    assert_eq!(
        and_results[0].get::<bool>(&Term::var("and_result")).ok(),
        Some(true)
    );

    // Test Or formula
    let mut or_terms = Parameters::new();
    or_terms.insert("left".to_string(), Term::var("x"));
    or_terms.insert("right".to_string(), Term::var("y"));
    or_terms.insert("is".to_string(), Term::var("or_result"));

    let or_formula = Or::apply(or_terms)?;

    // Test false OR true = true
    let or_input = Answer::new()
        .set(Term::var("x"), false)
        .unwrap()
        .set(Term::var("y"), true)
        .unwrap();

    let or_results = or_formula.derive(or_input)?;
    assert_eq!(or_results.len(), 1);
    assert_eq!(
        or_results[0].get::<bool>(&Term::var("or_result")).ok(),
        Some(true)
    );

    // Test Not formula
    let mut not_terms = Parameters::new();
    not_terms.insert("value".to_string(), Term::var("input"));
    not_terms.insert("is".to_string(), Term::var("not_result"));

    let not_formula = Not::apply(not_terms)?;

    // Test NOT true = false
    let not_input = Answer::new().set(Term::var("input"), true).unwrap();

    let not_results = not_formula.derive(not_input)?;
    assert_eq!(not_results.len(), 1);
    assert_eq!(
        not_results[0].get::<bool>(&Term::var("not_result")).ok(),
        Some(false)
    );

    Ok(())
}

#[dialog_macros::test]
async fn test_formula_chaining_integration() -> Result<()> {
    // Test chaining multiple formulas together

    // First: Parse a number from string
    let mut parse_terms = Parameters::new();
    parse_terms.insert("text".to_string(), Term::var("str_input"));
    parse_terms.insert("is".to_string(), Term::var("parsed_num"));

    let parse_formula = ParseNumber::apply(parse_terms)?;

    let parse_input = Answer::new()
        .set(Term::var("str_input"), "10".to_string())
        .unwrap();

    let parsed_results = parse_formula.derive(parse_input)?;
    assert_eq!(parsed_results.len(), 1);
    let intermediate_result = &parsed_results[0];

    // Second: Add 5 to the parsed number
    let mut sum_terms = Parameters::new();
    sum_terms.insert("of".to_string(), Term::var("parsed_num"));
    sum_terms.insert("with".to_string(), Term::var("addend"));
    sum_terms.insert("is".to_string(), Term::var("final_sum"));

    let sum_formula = Sum::apply(sum_terms)?;

    // Add the constant 5 to our intermediate result
    let sum_input = intermediate_result
        .clone()
        .set(Term::var("addend"), 5u32)
        .unwrap();

    let final_results = sum_formula.derive(sum_input)?;
    assert_eq!(final_results.len(), 1);
    assert_eq!(
        final_results[0].get::<u32>(&Term::var("final_sum")).ok(),
        Some(15)
    );

    // Third: Convert the result back to string
    let mut to_string_terms = Parameters::new();
    to_string_terms.insert("value".to_string(), Term::var("final_sum"));
    to_string_terms.insert("is".to_string(), Term::var("final_string"));

    let to_string_formula = ToString::apply(to_string_terms)?;

    let string_results = to_string_formula.derive(final_results[0].clone())?;
    assert_eq!(string_results.len(), 1);
    assert_eq!(
        string_results[0]
            .get::<String>(&Term::var("final_string"))
            .ok(),
        Some("15".to_string())
    );

    Ok(())
}

#[dialog_macros::test]
async fn test_formula_error_handling() -> Result<()> {
    // Test error handling in formulas

    // Test division by zero in Quotient formula
    let mut quotient_terms = Parameters::new();
    quotient_terms.insert("of".to_string(), Term::var("dividend"));
    quotient_terms.insert("by".to_string(), Term::var("divisor"));
    quotient_terms.insert("is".to_string(), Term::var("quotient"));

    let quotient_formula = Quotient::apply(quotient_terms)?;

    let division_by_zero_input = Answer::new()
        .set(Term::var("dividend"), 10u32)
        .unwrap()
        .set(Term::var("divisor"), 0u32)
        .unwrap();

    // Should handle division by zero gracefully by returning empty
    let quotient_results = quotient_formula.derive(division_by_zero_input)?;
    assert_eq!(quotient_results.len(), 0);

    // Test modulo by zero
    let mut modulo_terms = Parameters::new();
    modulo_terms.insert("of".to_string(), Term::var("dividend"));
    modulo_terms.insert("by".to_string(), Term::var("divisor"));
    modulo_terms.insert("is".to_string(), Term::var("remainder"));

    let modulo_formula = Modulo::apply(modulo_terms)?;

    let modulo_by_zero_input = Answer::new()
        .set(Term::var("dividend"), 17u32)
        .unwrap()
        .set(Term::var("divisor"), 0u32)
        .unwrap();

    // Should handle modulo by zero gracefully by returning empty
    let modulo_results = modulo_formula.derive(modulo_by_zero_input)?;
    assert_eq!(modulo_results.len(), 0);

    Ok(())
}
