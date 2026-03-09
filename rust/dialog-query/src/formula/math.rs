use crate::{Formula, formula::Input};

/// Sum formula that adds two numbers
#[derive(Debug, Clone, Formula)]
pub struct Sum {
    /// First operand
    pub of: u32,
    /// Second operand
    pub with: u32,
    /// Computed sum
    #[output(cost = 5)]
    pub is: u32,
}

impl Sum {
    /// Compute the sum of `of` and `with`
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        vec![Sum {
            of: input.of,
            with: input.with,
            is: input.of + input.with,
        }]
    }
}

/// Difference formula that subtracts two numbers
#[derive(Debug, Clone, Formula)]
pub struct Difference {
    /// Number to subtract from
    pub of: u32,
    /// Number to subtract
    pub subtract: u32,
    /// Difference
    #[output(cost = 2)]
    pub is: u32,
}

impl Difference {
    /// Compute the difference of `of` minus `subtract`
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        vec![Difference {
            of: input.of,
            subtract: input.subtract,
            is: input.of.saturating_sub(input.subtract),
        }]
    }
}

/// Product formula that multiplies two numbers
#[derive(Debug, Clone, Formula)]
pub struct Product {
    /// Number to multiply
    pub of: u32,
    /// Times to multiply
    pub times: u32,
    /// Result of multiplication
    #[output(cost = 5)]
    pub is: u32,
}

impl Product {
    /// Compute the product of `of` times `times`
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        vec![Product {
            of: input.of,
            times: input.times,
            is: input.of * input.times,
        }]
    }
}

/// Quotient formula that divides two numbers
#[derive(Debug, Clone, Formula)]
pub struct Quotient {
    /// Number to divide
    pub of: u32,
    /// Number to divide by
    pub by: u32,
    /// Result of division
    #[output(cost = 5)]
    pub is: u32,
}

impl Quotient {
    /// Compute the quotient of `of` divided by `by`, returning empty on division by zero
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        if input.by == 0 {
            // Return empty Vec for division by zero - this will be filtered out
            vec![]
        } else {
            vec![Quotient {
                of: input.of,
                by: input.by,
                is: input.of / input.by,
            }]
        }
    }
}

/// Modulo formula that computes remainder of division
#[derive(Debug, Clone, Formula)]
pub struct Modulo {
    /// Number to compute modulo of
    pub of: u32,
    /// Number to compute modulo by
    pub by: u32,
    /// Result of modulo operation
    #[output(cost = 10)]
    pub is: u32,
}

impl Modulo {
    /// Compute `of` modulo `by`, returning empty on modulo by zero
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        if input.by == 0 {
            // Return empty Vec for modulo by zero
            vec![]
        } else {
            vec![Modulo {
                of: input.of,
                by: input.by,
                is: input.of % input.by,
            }]
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::formula::Input;
    use crate::formula::math::*;
    use crate::formula::query::FormulaQuery;
    use crate::*;
    use futures_util::TryStreamExt;

    #[dialog_common::test]
    fn it_sums_two_values() -> anyhow::Result<()> {
        // Create Terms mapping
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("with".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        // Create input match with x=5, y=3
        let mut input = Match::new();
        input
            .bind(&Term::var("x"), 5u32.into())
            .expect("Failed to set x");
        input
            .bind(&Term::var("y"), 3u32.into())
            .expect("Failed to set y");

        // Create formula application
        let app: FormulaQuery = Sum::apply(terms)?.into();

        // Expand the formula
        let results = app.compute(input).expect("Formula expansion failed");

        // Verify results
        assert_eq!(results.len(), 1);
        let output = &results[0];

        // Check that x and y are preserved
        assert_eq!(
            output
                .lookup(&Term::var("x"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(5)
        );
        assert_eq!(
            output
                .lookup(&Term::var("y"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(3)
        );

        // Check that result is computed correctly
        assert_eq!(
            output
                .lookup(&Term::var("result"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(8)
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_errors_on_missing_sum_input() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("with".to_string(), Term::var("missing"));
        terms.insert("is".to_string(), Term::var("result"));

        let mut input = Match::new();
        input
            .bind(&Term::var("x"), 5u32.into())
            .expect("Failed to set x");

        let app: FormulaQuery = Sum::apply(terms)?.into();
        let result = app.compute(input);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            EvaluationError::UnboundFormulaVariable { .. }
        ));
        Ok(())
    }

    #[dialog_common::test]
    fn it_expands_sum_to_multiple_types() -> anyhow::Result<()> {
        // Test multiple expansions without the stream complexity
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("a"));
        terms.insert("with".to_string(), Term::var("b"));
        terms.insert("is".to_string(), Term::var("sum"));

        let app: FormulaQuery = Sum::apply(terms)?.into();

        // Test first input: 2 + 3 = 5
        let mut input1 = Match::new();
        input1.bind(&Term::var("a"), 2u32.into()).unwrap();
        input1.bind(&Term::var("b"), 3u32.into()).unwrap();

        let results1 = app.compute(input1).expect("First expansion failed");
        assert_eq!(results1.len(), 1);
        let result1 = &results1[0];
        assert_eq!(
            result1
                .lookup(&Term::var("a"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(2)
        );
        assert_eq!(
            result1
                .lookup(&Term::var("b"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(3)
        );
        assert_eq!(
            result1
                .lookup(&Term::var("sum"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(5)
        );

        // Test second input: 10 + 15 = 25
        let mut input2 = Match::new();
        input2.bind(&Term::var("a"), 10u32.into()).unwrap();
        input2.bind(&Term::var("b"), 15u32.into()).unwrap();

        let results2 = app.compute(input2).expect("Second expansion failed");
        assert_eq!(results2.len(), 1);
        let result2 = &results2[0];
        assert_eq!(
            result2
                .lookup(&Term::var("a"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(10)
        );
        assert_eq!(
            result2
                .lookup(&Term::var("b"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(15)
        );
        assert_eq!(
            result2
                .lookup(&Term::var("sum"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(25)
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_converts_between_numeric_types() -> anyhow::Result<()> {
        // Test various data types with standard TryFrom<Value>
        let bool_val = Value::Boolean(true);
        assert!(bool::try_from(bool_val).unwrap());

        let f64_val = Value::Float(2.5);
        assert_eq!(f64::try_from(f64_val).unwrap(), 2.5);

        let string_val = Value::String("hello".to_string());
        assert_eq!(String::try_from(string_val).unwrap(), "hello");

        let u32_val = Value::UnsignedInt(42);
        assert_eq!(u32::try_from(u32_val).unwrap(), 42);

        let i32_val = Value::SignedInt(-10);
        assert_eq!(i32::try_from(i32_val).unwrap(), -10);
        Ok(())
    }

    #[dialog_common::test]
    fn it_computes_difference() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("subtract".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let mut input = Match::new();
        input.bind(&Term::var("x"), 10u32.into()).unwrap();
        input.bind(&Term::var("y"), 3u32.into()).unwrap();

        let app: FormulaQuery = Difference::apply(terms)?.into();
        let results = app.compute(input).expect("Difference failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .lookup(&Term::var("result"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(7)
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_handles_difference_underflow() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("subtract".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let mut input = Match::new();
        input.bind(&Term::var("x"), 3u32.into()).unwrap();
        input.bind(&Term::var("y"), 10u32.into()).unwrap();

        let app: FormulaQuery = Difference::apply(terms)?.into();
        let results = app
            .compute(input)
            .expect("Difference underflow should be handled");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        // Should saturate at 0
        assert_eq!(
            result
                .lookup(&Term::var("result"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(0)
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_computes_product() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("times".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let mut input = Match::new();
        input.bind(&Term::var("x"), 6u32.into()).unwrap();
        input.bind(&Term::var("y"), 7u32.into()).unwrap();

        let app: FormulaQuery = Product::apply(terms)?.into();
        let results = app.compute(input).expect("Product failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .lookup(&Term::var("result"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(42)
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_computes_quotient() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("by".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let mut input = Match::new();
        input.bind(&Term::var("x"), 15u32.into()).unwrap();
        input.bind(&Term::var("y"), 3u32.into()).unwrap();

        let app: FormulaQuery = Quotient::apply(terms)?.into();
        let results = app.compute(input).expect("Quotient failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .lookup(&Term::var("result"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(5)
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_errors_on_division_by_zero() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("by".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let mut input = Match::new();
        input.bind(&Term::var("x"), 15u32.into()).unwrap();
        input.bind(&Term::var("y"), 0u32.into()).unwrap();

        let app: FormulaQuery = Quotient::apply(terms)?.into();
        let results = app
            .compute(input)
            .expect("Division by zero should be handled");

        // Should return empty Vec for division by zero
        assert_eq!(results.len(), 0);
        Ok(())
    }

    #[dialog_common::test]
    fn it_computes_modulo() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("by".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let mut input = Match::new();
        input.bind(&Term::var("x"), 17u32.into()).unwrap();
        input.bind(&Term::var("y"), 5u32.into()).unwrap();

        let app: FormulaQuery = Modulo::apply(terms)?.into();
        let results = app.compute(input).expect("Modulo failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .lookup(&Term::var("result"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(2)
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_errors_on_modulo_by_zero() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("by".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let mut input = Match::new();
        input.bind(&Term::var("x"), 17u32.into()).unwrap();
        input.bind(&Term::var("y"), 0u32.into()).unwrap();

        let app: FormulaQuery = Modulo::apply(terms)?.into();
        let results = app
            .compute(input)
            .expect("Modulo by zero should be handled");

        // Should return empty Vec for modulo by zero
        assert_eq!(results.len(), 0);
        Ok(())
    }

    #[dialog_common::test]
    fn it_chains_math_operations() -> anyhow::Result<()> {
        // Test Sum formula: 10 + 5 = 15
        let mut sum_terms = Parameters::new();
        sum_terms.insert("of".to_string(), Term::var("x"));
        sum_terms.insert("with".to_string(), Term::var("y"));
        sum_terms.insert("is".to_string(), Term::var("sum_result"));

        let sum_formula: FormulaQuery = Sum::apply(sum_terms)?.into();

        let mut sum_input = Match::new();
        sum_input.bind(&Term::var("x"), 10u32.into()).unwrap();
        sum_input.bind(&Term::var("y"), 5u32.into()).unwrap();

        let sum_results = sum_formula.compute(sum_input)?;
        assert_eq!(sum_results.len(), 1);
        assert_eq!(
            u32::try_from(sum_results[0].lookup(&Term::var("sum_result")).unwrap()).ok(),
            Some(15)
        );

        // Test Difference formula: 20 - 8 = 12
        let mut diff_terms = Parameters::new();
        diff_terms.insert("of".to_string(), Term::var("a"));
        diff_terms.insert("subtract".to_string(), Term::var("b"));
        diff_terms.insert("is".to_string(), Term::var("diff_result"));

        let diff_formula: FormulaQuery = Difference::apply(diff_terms)?.into();

        let mut diff_input = Match::new();
        diff_input.bind(&Term::var("a"), 20u32.into()).unwrap();
        diff_input.bind(&Term::var("b"), 8u32.into()).unwrap();

        let diff_results = diff_formula.compute(diff_input)?;
        assert_eq!(diff_results.len(), 1);
        assert_eq!(
            u32::try_from(diff_results[0].lookup(&Term::var("diff_result")).unwrap()).ok(),
            Some(12)
        );

        // Test Product formula: 6 * 7 = 42
        let mut prod_terms = Parameters::new();
        prod_terms.insert("of".to_string(), Term::var("p"));
        prod_terms.insert("times".to_string(), Term::var("q"));
        prod_terms.insert("is".to_string(), Term::var("product"));

        let product_formula: FormulaQuery = Product::apply(prod_terms)?.into();

        let mut prod_input = Match::new();
        prod_input.bind(&Term::var("p"), 6u32.into()).unwrap();
        prod_input.bind(&Term::var("q"), 7u32.into()).unwrap();

        let prod_results = product_formula.compute(prod_input)?;
        assert_eq!(prod_results.len(), 1);
        assert_eq!(
            u32::try_from(prod_results[0].lookup(&Term::var("product")).unwrap()).ok(),
            Some(42)
        );

        Ok(())
    }

    #[dialog_common::test]
    fn it_chains_formula_results() -> anyhow::Result<()> {
        use crate::formula::conversions::{ParseUnsignedInteger, ToString};

        // First: Parse a number from string
        let mut parse_terms = Parameters::new();
        parse_terms.insert("text".to_string(), Term::var("str_input"));
        parse_terms.insert("is".to_string(), Term::var("parsed_num"));

        let parse_formula: FormulaQuery = ParseUnsignedInteger::apply(parse_terms)?.into();

        let mut parse_input = Match::new();
        parse_input
            .bind(&Term::var("str_input"), "10".to_string().into())
            .unwrap();

        let parsed_results = parse_formula.compute(parse_input)?;
        assert_eq!(parsed_results.len(), 1);
        let intermediate_result = &parsed_results[0];

        // Second: Add 5 to the parsed number
        let mut sum_terms = Parameters::new();
        sum_terms.insert("of".to_string(), Term::var("parsed_num"));
        sum_terms.insert("with".to_string(), Term::var("addend"));
        sum_terms.insert("is".to_string(), Term::var("final_sum"));

        let sum_formula: FormulaQuery = Sum::apply(sum_terms)?.into();

        let mut sum_input = intermediate_result.clone();
        sum_input.bind(&Term::var("addend"), 5u32.into()).unwrap();

        let final_results = sum_formula.compute(sum_input)?;
        assert_eq!(final_results.len(), 1);
        assert_eq!(
            u32::try_from(final_results[0].lookup(&Term::var("final_sum")).unwrap()).ok(),
            Some(15)
        );

        // Third: Convert the result back to string
        let mut to_string_terms = Parameters::new();
        to_string_terms.insert("value".to_string(), Term::var("final_sum"));
        to_string_terms.insert("is".to_string(), Term::var("final_string"));

        let to_string_formula: FormulaQuery = ToString::apply(to_string_terms)?.into();

        let string_results = to_string_formula.compute(final_results[0].clone())?;
        assert_eq!(string_results.len(), 1);
        assert_eq!(
            String::try_from(
                string_results[0]
                    .lookup(&Term::var("final_string"))
                    .unwrap()
            )
            .ok(),
            Some("15".to_string())
        );

        Ok(())
    }

    #[dialog_common::test]
    fn it_generates_input_struct() {
        let input = Input::<Sum> { of: 5, with: 3 };
        assert_eq!(input.of, 5);
        assert_eq!(input.with, 3);
    }

    #[dialog_common::test]
    fn it_generates_match_struct() {
        let match_pattern = Query::<Sum> {
            of: Term::var("x"),
            with: Term::var("y"),
            is: Term::var("result"),
        };

        assert!(matches!(match_pattern.of, Term::Variable { .. }));
    }

    #[dialog_common::test]
    async fn it_performs_formula_with_all_variables() -> anyhow::Result<()> {
        use crate::query::Application;
        use crate::{Session, artifact::Artifacts};
        use dialog_storage::MemoryStorageBackend;

        // Create a SumQuery with all variables
        let query = Query::<Sum> {
            of: Term::var("x"),
            with: Term::var("y"),
            is: Term::var("result"),
        };

        // Create a minimal session (formulas don't need stored data)
        let storage = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage).await?;
        let session = Session::open(artifacts);

        // perform = evaluate(new_context) -> realize for each match
        // But first we need to seed the context with input values.
        // Since perform starts from an empty Match, the formula will fail
        // because input variables x and y are unbound.
        // So we use evaluate with a pre-seeded context instead.
        let mut input = Match::new();
        input.bind(&Term::var("x"), 5u32.into())?;
        input.bind(&Term::var("y"), 3u32.into())?;

        let query_copy = query.clone();
        let matches: Vec<Match> = query.evaluate(input.seed(), &session).try_collect().await?;

        assert_eq!(matches.len(), 1);

        // Now test realize — should reconstruct the Sum proof struct
        let proof = query_copy.realize(matches[0].clone())?;
        assert_eq!(proof.of, 5);
        assert_eq!(proof.with, 3);
        assert_eq!(proof.is, 8);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_performs_formula_with_constant_inputs() -> anyhow::Result<()> {
        use crate::query::Application;
        use crate::{Session, artifact::Artifacts};
        use dialog_storage::MemoryStorageBackend;

        // Input fields are constants, output field is a variable
        let query = Query::<Sum> {
            of: Term::from(5u32),
            with: Term::from(3u32),
            is: Term::var("result"),
        };

        let storage = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage).await?;
        let session = Session::open(artifacts);

        // Constants are already bound — empty starting Match should work
        let input = Match::new();

        let query_copy = query.clone();
        let selection: Vec<Match> = { query.evaluate(input.seed(), &session).try_collect().await? };

        assert_eq!(selection.len(), 1);
        let proof = query_copy.realize(selection[0].clone())?;
        assert_eq!(proof.of, 5);
        assert_eq!(proof.with, 3);
        assert_eq!(proof.is, 8);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_performs_formula_with_constant_derived() -> anyhow::Result<()> {
        use crate::query::Application;
        use crate::{Session, artifact::Artifacts};
        use dialog_storage::MemoryStorageBackend;

        // Derived field is a constant matching the expected result
        let query = Query::<Sum> {
            of: Term::var("x"),
            with: Term::var("y"),
            is: Term::from(8u32),
        };

        let storage = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage).await?;
        let session = Session::open(artifacts);

        let mut input = Match::new();
        input.bind(&Term::var("x"), 5u32.into())?;
        input.bind(&Term::var("y"), 3u32.into())?;

        let query_copy = query.clone();
        let matches: Vec<Match> = query.evaluate(input.seed(), &session).try_collect().await?;

        // Should succeed — the formula computes 8, and the constant 8 is consistent
        assert_eq!(matches.len(), 1);
        let proof = query_copy.realize(matches[0].clone())?;
        assert_eq!(proof.of, 5);
        assert_eq!(proof.with, 3);
        assert_eq!(proof.is, 8);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_rejects_inconsistent_constant_in_formula() -> anyhow::Result<()> {
        use crate::query::Application;
        use crate::{Session, artifact::Artifacts};
        use dialog_storage::MemoryStorageBackend;

        // Derived field is a constant that does NOT match (5 + 3 ≠ 99)
        let query = Query::<Sum> {
            of: Term::var("x"),
            with: Term::var("y"),
            is: Term::from(99u32),
        };

        let storage = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage).await?;
        let session = Session::open(artifacts);

        let mut input = Match::new();
        input.bind(&Term::var("x"), 5u32.into())?;
        input.bind(&Term::var("y"), 3u32.into())?;

        let selection: Vec<Match> = query.evaluate(input.seed(), &session).try_collect().await?;

        // The formula computes 8 but "is" is constant 99 — inconsistency
        // should filter this out (0 results)
        assert_eq!(
            selection.len(),
            0,
            "Inconsistent constant should produce no results"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_performs_formula_with_mixed_terms() -> anyhow::Result<()> {
        use crate::query::Application;
        use crate::{Session, artifact::Artifacts};
        use dialog_storage::MemoryStorageBackend;

        // Mix: one input is constant, one is variable, output is variable
        let query = Query::<Sum> {
            of: Term::from(10u32),
            with: Term::var("y"),
            is: Term::var("result"),
        };

        let storage = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage).await?;
        let session = Session::open(artifacts);

        let mut input = Match::new();
        input.bind(&Term::var("y"), 7u32.into())?;

        let query_copy = query.clone();
        let selection: Vec<Match> = query.evaluate(input.seed(), &session).try_collect().await?;

        assert_eq!(selection.len(), 1);
        let proof = query_copy.realize(selection[0].clone())?;
        assert_eq!(proof.of, 10);
        assert_eq!(proof.with, 7);
        assert_eq!(proof.is, 17);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_performs_formula_with_shared_variable() -> anyhow::Result<()> {
        use crate::query::Application;
        use crate::{Session, artifact::Artifacts};
        use dialog_storage::MemoryStorageBackend;

        // Both inputs use the same variable (x + x)
        let query = Query::<Sum> {
            of: Term::var("x"),
            with: Term::var("x"),
            is: Term::var("result"),
        };

        let storage = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage).await?;
        let session = Session::open(artifacts);

        let mut input = Match::new();
        input.bind(&Term::var("x"), 4u32.into())?;

        let query_copy = query.clone();
        let matches: Vec<Match> = query.evaluate(input.seed(), &session).try_collect().await?;

        assert_eq!(matches.len(), 1);
        let proof = query_copy.realize(matches[0].clone())?;
        assert_eq!(proof.of, 4);
        assert_eq!(proof.with, 4);
        assert_eq!(proof.is, 8);

        Ok(())
    }

    #[dialog_common::test]
    fn it_handles_formula_errors() -> anyhow::Result<()> {
        // Test division by zero in Quotient formula
        let mut quotient_terms = Parameters::new();
        quotient_terms.insert("of".to_string(), Term::var("dividend"));
        quotient_terms.insert("by".to_string(), Term::var("divisor"));
        quotient_terms.insert("is".to_string(), Term::var("quotient"));

        let quotient_formula: FormulaQuery = Quotient::apply(quotient_terms)?.into();

        let mut division_by_zero_input = Match::new();
        division_by_zero_input
            .bind(&Term::var("dividend"), 10u32.into())
            .unwrap();
        division_by_zero_input
            .bind(&Term::var("divisor"), 0u32.into())
            .unwrap();

        let quotient_results = quotient_formula.compute(division_by_zero_input)?;
        assert_eq!(quotient_results.len(), 0);

        // Test modulo by zero
        let mut modulo_terms = Parameters::new();
        modulo_terms.insert("of".to_string(), Term::var("dividend"));
        modulo_terms.insert("by".to_string(), Term::var("divisor"));
        modulo_terms.insert("is".to_string(), Term::var("remainder"));

        let modulo_formula: FormulaQuery = Modulo::apply(modulo_terms)?.into();

        let mut modulo_by_zero_input = Match::new();
        modulo_by_zero_input
            .bind(&Term::var("dividend"), 17u32.into())
            .unwrap();
        modulo_by_zero_input
            .bind(&Term::var("divisor"), 0u32.into())
            .unwrap();

        let modulo_results = modulo_formula.compute(modulo_by_zero_input)?;
        assert_eq!(modulo_results.len(), 0);

        Ok(())
    }
}
