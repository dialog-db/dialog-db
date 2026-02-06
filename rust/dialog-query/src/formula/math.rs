use crate::{dsl::Input, Formula};

/// Sum formula that adds two numbers
#[derive(Debug, Clone, Formula)]
pub struct Sum {
    pub of: u32,
    pub with: u32,
    #[derived(cost = 5)]
    pub is: u32,
}

impl Sum {
    pub fn derive(input: Input<Self>) -> Vec<Self> {
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
    #[derived(cost = 2)]
    pub is: u32,
}

impl Difference {
    pub fn derive(input: Input<Self>) -> Vec<Self> {
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
    #[derived(cost = 5)]
    pub is: u32,
}

impl Product {
    pub fn derive(input: Input<Self>) -> Vec<Self> {
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
    #[derived(cost = 5)]
    pub is: u32,
}

impl Quotient {
    pub fn derive(input: Input<Self>) -> Vec<Self> {
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
    #[derived(cost = 10)]
    pub is: u32,
}

impl Modulo {
    pub fn derive(input: Input<Self>) -> Vec<Self> {
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
    use crate::error::FormulaEvaluationError;
    use crate::formula::math::*;
    use crate::Term;
    use crate::*;

    #[test]
    fn test_sum_formula_basic() -> anyhow::Result<()> {
        // Create Terms mapping
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("with".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        // Create input match with x=5, y=3
        let input = Answer::new()
            .set(Term::var("x"), 5u32)
            .expect("Failed to set x")
            .set(Term::var("y"), 3u32)
            .expect("Failed to set y");

        // Create formula application
        let app = Sum::apply(terms)?;

        // Expand the formula
        let results = app.derive(input).expect("Formula expansion failed");

        // Verify results
        assert_eq!(results.len(), 1);
        let output = &results[0];

        // Check that x and y are preserved
        assert_eq!(
            output
                .resolve(&Term::<u32>::var("x"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(5)
        );
        assert_eq!(
            output
                .resolve(&Term::<u32>::var("y"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(3)
        );

        // Check that result is computed correctly
        assert_eq!(
            output
                .resolve(&Term::<u32>::var("result"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(8)
        );
        Ok(())
    }

    // Removed test_cursor_read_write:
    // This test was for the deprecated Cursor.write() method which relied on
    // Match's copy-on-write semantics. With Answer, we intentionally don't
    // support mutable updates - formulas should return new Answer instances
    // instead of mutating cursors. The write() method is deprecated and will
    // be removed in query-2 when formulas are updated to work with Answer.

    #[test]
    fn test_sum_formula_missing_input() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("with".to_string(), Term::var("missing"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("x"), 5u32)
            .expect("Failed to set x");

        let app = Sum::apply(terms)?;
        let result = app.derive(input);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FormulaEvaluationError::UnboundVariable { .. }
        ));
        Ok(())
    }

    #[test]
    fn test_sum_formula_multiple_expand() -> anyhow::Result<()> {
        // Test multiple expansions without the stream complexity
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("a"));
        terms.insert("with".to_string(), Term::var("b"));
        terms.insert("is".to_string(), Term::var("sum"));

        let app = Sum::apply(terms)?;

        // Test first input: 2 + 3 = 5
        let input1 = Answer::new()
            .set(Term::var("a"), 2u32)
            .unwrap()
            .set(Term::var("b"), 3u32)
            .unwrap();

        let results1 = app.derive(input1).expect("First expansion failed");
        assert_eq!(results1.len(), 1);
        let result1 = &results1[0];
        assert_eq!(
            result1
                .resolve(&Term::<u32>::var("a"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(2)
        );
        assert_eq!(
            result1
                .resolve(&Term::<u32>::var("b"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(3)
        );
        assert_eq!(
            result1
                .resolve(&Term::<u32>::var("sum"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(5)
        );

        // Test second input: 10 + 15 = 25
        let input2 = Answer::new()
            .set(Term::var("a"), 10u32)
            .unwrap()
            .set(Term::var("b"), 15u32)
            .unwrap();

        let results2 = app.derive(input2).expect("Second expansion failed");
        assert_eq!(results2.len(), 1);
        let result2 = &results2[0];
        assert_eq!(
            result2
                .resolve(&Term::<u32>::var("a"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(10)
        );
        assert_eq!(
            result2
                .resolve(&Term::<u32>::var("b"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(15)
        );
        assert_eq!(
            result2
                .resolve(&Term::<u32>::var("sum"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(25)
        );
        Ok(())
    }

    #[test]
    fn test_multiple_try_from_types() -> anyhow::Result<()> {
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

    #[test]
    fn test_difference_formula() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("subtract".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("x"), 10u32)
            .unwrap()
            .set(Term::var("y"), 3u32)
            .unwrap();

        let app = Difference::apply(terms)?;
        let results = app.derive(input).expect("Difference failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<u32>::var("result"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(7)
        );
        Ok(())
    }

    #[test]
    fn test_difference_formula_underflow() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("subtract".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("x"), 3u32)
            .unwrap()
            .set(Term::var("y"), 10u32)
            .unwrap();

        let app = Difference::apply(terms)?;
        let results = app
            .derive(input)
            .expect("Difference underflow should be handled");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        // Should saturate at 0
        assert_eq!(
            result
                .resolve(&Term::<u32>::var("result"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(0)
        );
        Ok(())
    }

    #[test]
    fn test_product_formula() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("times".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("x"), 6u32)
            .unwrap()
            .set(Term::var("y"), 7u32)
            .unwrap();

        let app = Product::apply(terms)?;
        let results = app.derive(input).expect("Product failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<u32>::var("result"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(42)
        );
        Ok(())
    }

    #[test]
    fn test_quotient_formula() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("by".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("x"), 15u32)
            .unwrap()
            .set(Term::var("y"), 3u32)
            .unwrap();

        let app = Quotient::apply(terms)?;
        let results = app.derive(input).expect("Quotient failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<u32>::var("result"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(5)
        );
        Ok(())
    }

    #[test]
    fn test_quotient_formula_division_by_zero() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("by".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let x = Term::var("x");
        print!("{:?}", &x);
        let input = Answer::new()
            .set(x, 15u32)
            .unwrap()
            .set(Term::var("y"), 0u32)
            .unwrap();

        let app = Quotient::apply(terms)?;
        let results = app
            .derive(input)
            .expect("Division by zero should be handled");

        // Should return empty Vec for division by zero
        assert_eq!(results.len(), 0);
        Ok(())
    }

    #[test]
    fn test_modulo_formula() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("by".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("x"), 17u32)
            .unwrap()
            .set(Term::var("y"), 5u32)
            .unwrap();

        let app = Modulo::apply(terms)?;
        let results = app.derive(input).expect("Modulo failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<u32>::var("result"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(2)
        );
        Ok(())
    }

    #[test]
    fn test_modulo_formula_by_zero() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x"));
        terms.insert("by".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("x"), 17u32)
            .unwrap()
            .set(Term::var("y"), 0u32)
            .unwrap();

        let app = Modulo::apply(terms)?;
        let results = app.derive(input).expect("Modulo by zero should be handled");

        // Should return empty Vec for modulo by zero
        assert_eq!(results.len(), 0);
        Ok(())
    }
}
