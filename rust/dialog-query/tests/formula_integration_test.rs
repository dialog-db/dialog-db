//! Integration tests for the formula system
//!
//! These tests demonstrate how formulas integrate with the query engine,
//! session management, and the overall dialog-query system.

use anyhow::Result;
use dialog_query::{
    artifact::{Artifacts, Attribute, Entity, Value},
    formulas::*,
    selection::Answer,
    Fact, Formula, Parameters, Session, Term,
};
use dialog_storage::MemoryStorageBackend;

#[tokio::test]
async fn test_formula_integration_math_operations() -> Result<()> {
    // Setup: Create in-memory storage and session
    let storage_backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(storage_backend).await?;

    // Create entities for our test data
    let calculation_1 = Entity::new()?;
    let calculation_2 = Entity::new()?;

    // Store some basic math data
    let facts = vec![
        // First calculation: 10 + 5
        Fact::assert(
            "calc/operand1".parse::<Attribute>()?,
            calculation_1.clone(),
            Value::UnsignedInt(10),
        ),
        Fact::assert(
            "calc/operand2".parse::<Attribute>()?,
            calculation_1.clone(),
            Value::UnsignedInt(5),
        ),
        // Second calculation: 20 - 8
        Fact::assert(
            "calc/operand1".parse::<Attribute>()?,
            calculation_2.clone(),
            Value::UnsignedInt(20),
        ),
        Fact::assert(
            "calc/operand2".parse::<Attribute>()?,
            calculation_2.clone(),
            Value::UnsignedInt(8),
        ),
    ];

    let mut session = Session::open(artifacts.clone());
    session.transact(facts).await?;

    // Test Sum formula
    let mut sum_terms = Parameters::new();
    sum_terms.insert("of".to_string(), Term::var("x").into());
    sum_terms.insert("with".to_string(), Term::var("y").into());
    sum_terms.insert("is".to_string(), Term::var("sum_result").into());

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
    diff_terms.insert("of".to_string(), Term::var("a").into());
    diff_terms.insert("subtract".to_string(), Term::var("b").into());
    diff_terms.insert("is".to_string(), Term::var("diff_result").into());

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
    prod_terms.insert("of".to_string(), Term::var("p").into());
    prod_terms.insert("times".to_string(), Term::var("q").into());
    prod_terms.insert("is".to_string(), Term::var("product").into());

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

#[tokio::test]
async fn test_formula_integration_string_operations() -> Result<()> {
    // Setup
    let storage_backend = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(storage_backend).await?;

    let user_1 = Entity::new()?;
    let user_2 = Entity::new()?;

    // Store user name data
    let facts = vec![
        Fact::assert(
            "user/first_name".parse::<Attribute>()?,
            user_1.clone(),
            Value::String("John".to_string()),
        ),
        Fact::assert(
            "user/last_name".parse::<Attribute>()?,
            user_1.clone(),
            Value::String("Doe".to_string()),
        ),
        Fact::assert(
            "user/first_name".parse::<Attribute>()?,
            user_2.clone(),
            Value::String("Jane".to_string()),
        ),
        Fact::assert(
            "user/last_name".parse::<Attribute>()?,
            user_2.clone(),
            Value::String("Smith".to_string()),
        ),
    ];

    let mut session = Session::open(artifacts.clone());
    session.transact(facts).await?;

    // Test Concatenate formula to build full names
    let mut concat_terms = Parameters::new();
    concat_terms.insert("first".to_string(), Term::var("fname").into());
    concat_terms.insert("second".to_string(), Term::var("lname").into());
    concat_terms.insert("is".to_string(), Term::var("full_name").into());

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
    length_terms.insert("of".to_string(), Term::var("text").into());
    length_terms.insert("is".to_string(), Term::var("length").into());

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
    upper_terms.insert("of".to_string(), Term::var("input").into());
    upper_terms.insert("is".to_string(), Term::var("output").into());

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

#[tokio::test]
async fn test_formula_integration_type_conversions() -> Result<()> {
    // Test ToString formula with different types
    let mut to_string_terms = Parameters::new();
    to_string_terms.insert("value".to_string(), Term::var("input").into());
    to_string_terms.insert("is".to_string(), Term::var("str_result").into());

    let to_string_formula = ToString::apply(to_string_terms)?;

    // Test with number
    let number_input = Answer::new()
        .set(Term::var("input"), 42u32)
        .unwrap();

    let string_results = to_string_formula.derive(number_input)?;
    assert_eq!(string_results.len(), 1);
    assert_eq!(
        string_results[0]
            .get::<String>(&Term::var("str_result"))
            .ok(),
        Some("42".to_string())
    );

    // Test with boolean
    let bool_input = Answer::new()
        .set(Term::var("input"), true)
        .unwrap();

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
    parse_terms.insert("text".to_string(), Term::var("str_input").into());
    parse_terms.insert("is".to_string(), Term::var("num_result").into());

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

#[tokio::test]
async fn test_formula_integration_boolean_logic() -> Result<()> {
    // Test boolean logic formulas

    // Test And formula
    let mut and_terms = Parameters::new();
    and_terms.insert("left".to_string(), Term::var("a").into());
    and_terms.insert("right".to_string(), Term::var("b").into());
    and_terms.insert("is".to_string(), Term::var("and_result").into());

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
    or_terms.insert("left".to_string(), Term::var("x").into());
    or_terms.insert("right".to_string(), Term::var("y").into());
    or_terms.insert("is".to_string(), Term::var("or_result").into());

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
    not_terms.insert("value".to_string(), Term::var("input").into());
    not_terms.insert("is".to_string(), Term::var("not_result").into());

    let not_formula = Not::apply(not_terms)?;

    // Test NOT true = false
    let not_input = Answer::new()
        .set(Term::var("input"), true)
        .unwrap();

    let not_results = not_formula.derive(not_input)?;
    assert_eq!(not_results.len(), 1);
    assert_eq!(
        not_results[0].get::<bool>(&Term::var("not_result")).ok(),
        Some(false)
    );

    Ok(())
}

#[tokio::test]
async fn test_formula_chaining_integration() -> Result<()> {
    // Test chaining multiple formulas together

    // First: Parse a number from string
    let mut parse_terms = Parameters::new();
    parse_terms.insert("text".to_string(), Term::var("str_input").into());
    parse_terms.insert("is".to_string(), Term::var("parsed_num").into());

    let parse_formula = ParseNumber::apply(parse_terms)?;

    let parse_input = Answer::new()
        .set(Term::var("str_input"), "10".to_string())
        .unwrap();

    let parsed_results = parse_formula.derive(parse_input)?;
    assert_eq!(parsed_results.len(), 1);
    let intermediate_result = &parsed_results[0];

    // Second: Add 5 to the parsed number
    let mut sum_terms = Parameters::new();
    sum_terms.insert("of".to_string(), Term::var("parsed_num").into());
    sum_terms.insert("with".to_string(), Term::var("addend").into());
    sum_terms.insert("is".to_string(), Term::var("final_sum").into());

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
    to_string_terms.insert("value".to_string(), Term::var("final_sum").into());
    to_string_terms.insert("is".to_string(), Term::var("final_string").into());

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

#[tokio::test]
async fn test_formula_error_handling() -> Result<()> {
    // Test error handling in formulas

    // Test division by zero in Quotient formula
    let mut quotient_terms = Parameters::new();
    quotient_terms.insert("of".to_string(), Term::var("dividend").into());
    quotient_terms.insert("by".to_string(), Term::var("divisor").into());
    quotient_terms.insert("is".to_string(), Term::var("quotient").into());

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
    modulo_terms.insert("of".to_string(), Term::var("dividend").into());
    modulo_terms.insert("by".to_string(), Term::var("divisor").into());
    modulo_terms.insert("is".to_string(), Term::var("remainder").into());

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
