use crate::Formula;
use crate::formula::number::{Number, Numeric};

/// Sum formula: `is = of + with`, generic over the numeric types.
///
/// The type parameter is the formula's *scheme variable*: all three
/// cells share it, so inference links their types (a `u64` input
/// narrows the output to `u64`) and a row whose values cannot share
/// one type is a non-match. The engine evaluates the canonical
/// [`Numeric`] instantiation; arithmetic that produces no value of
/// the shared type (mixed variants, overflow) yields no rows. See
/// `notes/formula-schemes.md`.
#[derive(Debug, Clone, Formula)]
pub struct Sum<N: Number = Numeric> {
    /// First operand
    pub of: N,
    /// Second operand
    pub with: N,
    /// Computed sum
    #[output(cost = 5)]
    pub is: N,
}

impl<N: Number> Sum<N> {
    /// Compute the sum of `of` and `with`. No rows when the
    /// operation has no value of the shared type.
    pub fn compute(input: SumInput<N>) -> Vec<Self> {
        match input.of.clone().add(input.with.clone()) {
            Some(is) => vec![Sum {
                of: input.of,
                with: input.with,
                is,
            }],
            None => vec![],
        }
    }
}

/// Difference formula: `is = of - subtract`, generic over the
/// numeric types (see [`Sum`] for the scheme semantics).
#[derive(Debug, Clone, Formula)]
pub struct Difference<N: Number = Numeric> {
    /// Number to subtract from
    pub of: N,
    /// Number to subtract
    pub subtract: N,
    /// Difference
    #[output(cost = 2)]
    pub is: N,
}

impl<N: Number> Difference<N> {
    /// Compute `of - subtract`. No rows when the operation has no
    /// value of the shared type (including unsigned underflow).
    pub fn compute(input: DifferenceInput<N>) -> Vec<Self> {
        match input.of.clone().subtract(input.subtract.clone()) {
            Some(is) => vec![Difference {
                of: input.of,
                subtract: input.subtract,
                is,
            }],
            None => vec![],
        }
    }
}

/// Product formula: `is = of * times`, generic over the numeric
/// types (see [`Sum`] for the scheme semantics).
#[derive(Debug, Clone, Formula)]
pub struct Product<N: Number = Numeric> {
    /// Number to multiply
    pub of: N,
    /// Times to multiply
    pub times: N,
    /// Result of multiplication
    #[output(cost = 5)]
    pub is: N,
}

impl<N: Number> Product<N> {
    /// Compute `of * times`. No rows when the operation has no value
    /// of the shared type.
    pub fn compute(input: ProductInput<N>) -> Vec<Self> {
        match input.of.clone().multiply(input.times.clone()) {
            Some(is) => vec![Product {
                of: input.of,
                times: input.times,
                is,
            }],
            None => vec![],
        }
    }
}

/// Quotient formula: `is = of / by`, generic over the numeric types
/// (see [`Sum`] for the scheme semantics).
#[derive(Debug, Clone, Formula)]
pub struct Quotient<N: Number = Numeric> {
    /// Number to divide
    pub of: N,
    /// Number to divide by
    pub by: N,
    /// Result of division
    #[output(cost = 5)]
    pub is: N,
}

impl<N: Number> Quotient<N> {
    /// Compute `of / by`. No rows on integer division by zero
    /// (floats follow IEEE-754 and stay total).
    pub fn compute(input: QuotientInput<N>) -> Vec<Self> {
        match input.of.clone().divide(input.by.clone()) {
            Some(is) => vec![Quotient {
                of: input.of,
                by: input.by,
                is,
            }],
            None => vec![],
        }
    }
}

/// Modulo formula: `is = of % by`, generic over the numeric types
/// (see [`Sum`] for the scheme semantics).
#[derive(Debug, Clone, Formula)]
pub struct Modulo<N: Number = Numeric> {
    /// Number to compute modulo of
    pub of: N,
    /// Number to compute modulo by
    pub by: N,
    /// Result of modulo operation
    #[output(cost = 10)]
    pub is: N,
}

impl<N: Number> Modulo<N> {
    /// Compute `of % by`. No rows on an integer zero divisor.
    pub fn compute(input: ModuloInput<N>) -> Vec<Self> {
        match input.of.clone().remainder(input.by.clone()) {
            Some(is) => vec![Modulo {
                of: input.of,
                by: input.by,
                is,
            }],
            None => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::formula::conversions::{ParseUnsignedInteger, ToString};
    use crate::formula::math::*;
    use crate::formula::query::FormulaQuery;
    use crate::query::Application;
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::*;
    use dialog_repository::helpers::{test_operator_with_profile, test_repo};
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
                .and_then(|b| b.content().ok())
                .and_then(|v| u32::try_from(v).ok()),
            Some(5)
        );
        assert_eq!(
            output
                .lookup(&Term::var("y"))
                .ok()
                .and_then(|b| b.content().ok())
                .and_then(|v| u32::try_from(v).ok()),
            Some(3)
        );

        // Check that result is computed correctly
        assert_eq!(
            output
                .lookup(&Term::var("result"))
                .ok()
                .and_then(|b| b.content().ok())
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
                .and_then(|b| b.content().ok())
                .and_then(|v| u32::try_from(v).ok()),
            Some(2)
        );
        assert_eq!(
            result1
                .lookup(&Term::var("b"))
                .ok()
                .and_then(|b| b.content().ok())
                .and_then(|v| u32::try_from(v).ok()),
            Some(3)
        );
        assert_eq!(
            result1
                .lookup(&Term::var("sum"))
                .ok()
                .and_then(|b| b.content().ok())
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
                .and_then(|b| b.content().ok())
                .and_then(|v| u32::try_from(v).ok()),
            Some(10)
        );
        assert_eq!(
            result2
                .lookup(&Term::var("b"))
                .ok()
                .and_then(|b| b.content().ok())
                .and_then(|v| u32::try_from(v).ok()),
            Some(15)
        );
        assert_eq!(
            result2
                .lookup(&Term::var("sum"))
                .ok()
                .and_then(|b| b.content().ok())
                .and_then(|v| u32::try_from(v).ok()),
            Some(25)
        );
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
                .and_then(|b| b.content().ok())
                .and_then(|v| u32::try_from(v).ok()),
            Some(7)
        );
        Ok(())
    }

    /// Unsigned underflow has no value of the shared type: the row
    /// is a non-match, never a saturated (fabricated) zero.
    #[dialog_common::test]
    fn it_filters_difference_underflow() -> anyhow::Result<()> {
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
            .expect("underflow is a non-match, not an error");

        assert_eq!(results.len(), 0, "no value of the type is the result");
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
                .and_then(|b| b.content().ok())
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
                .and_then(|b| b.content().ok())
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
                .and_then(|b| b.content().ok())
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
    fn it_chains_formula_results() -> anyhow::Result<()> {
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
            u32::try_from(
                final_results[0]
                    .lookup(&Term::var("final_sum"))
                    .unwrap()
                    .content()
                    .unwrap()
            )
            .ok(),
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
                    .content()
                    .unwrap()
            )
            .ok(),
            Some("15".to_string())
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_performs_formula_with_all_variables() -> anyhow::Result<()> {
        // Create a SumQuery with all variables
        let query = Query::<Sum> {
            of: Term::var("x"),
            with: Term::var("y"),
            is: Term::var("result"),
        };

        // Create a minimal session (formulas don't need stored data)
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        // perform = evaluate(new_context) -> realize for each match
        // But first we need to seed the context with input values.
        // Since perform starts from an empty Match, the formula will fail
        // because input variables x and y are unbound.
        // So we use evaluate with a pre-seeded context instead.
        let mut input = Match::new();
        input.bind(&Term::var("x"), 5u32.into())?;
        input.bind(&Term::var("y"), 3u32.into())?;

        let query_copy = query.clone();
        let matches: Vec<Match> = query.evaluate(input.seed(), &source).try_collect().await?;

        assert_eq!(matches.len(), 1);

        // Now test realize: should reconstruct the Sum proof struct
        let proof = query_copy.realize(matches[0].clone())?;
        assert_eq!(proof.of, Numeric::UnsignedInt(5));
        assert_eq!(proof.with, Numeric::UnsignedInt(3));
        assert_eq!(proof.is, Numeric::UnsignedInt(8));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_performs_formula_with_constant_inputs() -> anyhow::Result<()> {
        // Input fields are constants, output field is a variable
        let query = Query::<Sum> {
            of: Term::Constant(Value::from(5u32)),
            with: Term::Constant(Value::from(3u32)),
            is: Term::var("result"),
        };

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        // Constants are already bound: empty starting Match should work
        let input = Match::new();

        let query_copy = query.clone();
        let selection: Vec<Match> = { query.evaluate(input.seed(), &source).try_collect().await? };

        assert_eq!(selection.len(), 1);
        let proof = query_copy.realize(selection[0].clone())?;
        assert_eq!(proof.of, Numeric::UnsignedInt(5));
        assert_eq!(proof.with, Numeric::UnsignedInt(3));
        assert_eq!(proof.is, Numeric::UnsignedInt(8));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_performs_formula_with_constant_output() -> anyhow::Result<()> {
        // Output field is a constant matching the expected result
        let query = Query::<Sum> {
            of: Term::var("x"),
            with: Term::var("y"),
            is: Term::Constant(Value::from(8u32)),
        };

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let mut input = Match::new();
        input.bind(&Term::var("x"), 5u32.into())?;
        input.bind(&Term::var("y"), 3u32.into())?;

        let query_copy = query.clone();
        let matches: Vec<Match> = query.evaluate(input.seed(), &source).try_collect().await?;

        // Should succeed: the formula computes 8, and the constant 8 is consistent
        assert_eq!(matches.len(), 1);
        let proof = query_copy.realize(matches[0].clone())?;
        assert_eq!(proof.of, Numeric::UnsignedInt(5));
        assert_eq!(proof.with, Numeric::UnsignedInt(3));
        assert_eq!(proof.is, Numeric::UnsignedInt(8));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_rejects_inconsistent_constant_in_formula() -> anyhow::Result<()> {
        // Output field is a constant that does NOT match (5 + 3 ≠ 99)
        let query = Query::<Sum> {
            of: Term::var("x"),
            with: Term::var("y"),
            is: Term::Constant(Value::from(99u32)),
        };

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let mut input = Match::new();
        input.bind(&Term::var("x"), 5u32.into())?;
        input.bind(&Term::var("y"), 3u32.into())?;

        let selection: Vec<Match> = query.evaluate(input.seed(), &source).try_collect().await?;

        // The formula computes 8 but "is" is constant 99: inconsistency
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
        // Mix: one input is constant, one is variable, output is variable
        let query = Query::<Sum> {
            of: Term::Constant(Value::from(10u32)),
            with: Term::var("y"),
            is: Term::var("result"),
        };

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let mut input = Match::new();
        input.bind(&Term::var("y"), 7u32.into())?;

        let query_copy = query.clone();
        let selection: Vec<Match> = query.evaluate(input.seed(), &source).try_collect().await?;

        assert_eq!(selection.len(), 1);
        let proof = query_copy.realize(selection[0].clone())?;
        assert_eq!(proof.of, Numeric::UnsignedInt(10));
        assert_eq!(proof.with, Numeric::UnsignedInt(7));
        assert_eq!(proof.is, Numeric::UnsignedInt(17));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_performs_formula_with_shared_variable() -> anyhow::Result<()> {
        // Both inputs use the same variable (x + x)
        let query = Query::<Sum> {
            of: Term::var("x"),
            with: Term::var("x"),
            is: Term::var("result"),
        };

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let mut input = Match::new();
        input.bind(&Term::var("x"), 4u32.into())?;

        let query_copy = query.clone();
        let matches: Vec<Match> = query.evaluate(input.seed(), &source).try_collect().await?;

        assert_eq!(matches.len(), 1);
        let proof = query_copy.realize(matches[0].clone())?;
        assert_eq!(proof.of, Numeric::UnsignedInt(4));
        assert_eq!(proof.with, Numeric::UnsignedInt(4));
        assert_eq!(proof.is, Numeric::UnsignedInt(8));

        Ok(())
    }
}
