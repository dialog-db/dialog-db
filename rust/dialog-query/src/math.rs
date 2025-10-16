use std::{sync::OnceLock, usize};

use crate::{
    cursor::Cursor, error::FormulaEvaluationError, predicate::formula::Cells, Compute,
    Dependencies, Formula, Term, Type, Value,
};

// ============================================================================
// Example: Sum Formula Implementation
// ============================================================================

/// Example Sum formula that adds two numbers
#[derive(Debug, Clone)]
pub struct Sum {
    pub of: u32,
    pub with: u32,
    pub is: u32,
}

/// Input structure for Sum formula
pub struct SumInput {
    pub of: u32,
    pub with: u32,
}

impl TryFrom<&mut Cursor<'_>> for SumInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: &mut Cursor) -> Result<Self, Self::Error> {
        let of = cursor.read("of")?;
        let with = cursor.read("with")?;
        Ok(SumInput { of, with })
    }
}

/// Match structure for Sum formula (for future macro generation)
pub struct SumMatch {
    pub of: Term<u32>,
    pub with: Term<u32>,
    pub is: Term<u32>,
}

impl Compute for Sum {
    fn compute(input: Self::Input) -> Vec<Self> {
        vec![Sum {
            of: input.of,
            with: input.with,
            is: input.of + input.with,
        }]
    }
}

static SUM_CELLS: OnceLock<Cells> = OnceLock::new();

impl Formula for Sum {
    type Input = SumInput;
    type Match = SumMatch;

    fn operator() -> &'static str {
        "sum"
    }

    fn cells() -> &'static Cells {
        SUM_CELLS.get_or_init(|| {
            Cells::define(|builder| {
                builder
                    .cell("of", Type::UnsignedInt)
                    .the("Number to add to")
                    .required();

                builder
                    .cell("with", Type::UnsignedInt)
                    .the("Number to add")
                    .required();

                builder
                    .cell("is", Type::UnsignedInt)
                    .the("Sum of numbers")
                    .derived(5);
            })
        })
    }

    fn cost() -> usize {
        5
    }

    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("of".into());
        dependencies.require("with".into());
        dependencies.provide("is".into());

        dependencies
    }

    fn derive(cursor: &mut Cursor) -> Result<Vec<Self>, FormulaEvaluationError> {
        let input = Self::Input::try_from(cursor)?;
        Ok(Self::compute(input))
    }

    fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError> {
        let value = Value::UnsignedInt(self.is.into());
        cursor.write("is", &value)
    }
}

// ============================================================================
// Mathematical Operations: Difference, Product, Quotient, Modulo
// ============================================================================

/// Difference formula that subtracts two numbers
#[derive(Debug, Clone)]
pub struct Difference {
    pub of: u32,
    pub subtract: u32,
    pub is: u32,
}

pub struct DifferenceInput {
    pub of: u32,
    pub subtract: u32,
}

impl TryFrom<&mut Cursor<'_>> for DifferenceInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: &mut Cursor) -> Result<Self, Self::Error> {
        let of = cursor.read::<u32>("of")?;
        let subtract = cursor.read::<u32>("subtract")?;
        Ok(DifferenceInput { of, subtract })
    }
}

impl Compute for Difference {
    fn compute(input: Self::Input) -> Vec<Self> {
        vec![Difference {
            of: input.of,
            subtract: input.subtract,
            is: input.of.saturating_sub(input.subtract),
        }]
    }
}

static DIFFERENCE_CELLS: OnceLock<Cells> = OnceLock::new();

impl Formula for Difference {
    type Input = DifferenceInput;
    type Match = ();

    fn operator() -> &'static str {
        "difference"
    }

    fn cells() -> &'static Cells {
        DIFFERENCE_CELLS.get_or_init(|| {
            Cells::define(|builder| {
                builder
                    .cell("of", Type::UnsignedInt)
                    .the("Number to subtract from")
                    .required();

                builder
                    .cell("subtract", Type::UnsignedInt)
                    .the("Number to subtract")
                    .required();

                builder
                    .cell("is", Type::UnsignedInt)
                    .the("Difference")
                    .derived(2);
            })
        })
    }

    fn cost() -> usize {
        2
    }

    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("of".into());
        dependencies.require("subtract".into());
        dependencies.provide("is".into());
        dependencies
    }

    fn derive(cursor: &mut Cursor) -> Result<Vec<Self>, FormulaEvaluationError> {
        let input = Self::Input::try_from(cursor)?;
        Ok(Self::compute(input))
    }

    fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError> {
        let value = Value::UnsignedInt(self.is.into());
        cursor.write("is", &value)
    }
}

/// Product formula that multiplies two numbers
#[derive(Debug, Clone)]
pub struct Product {
    pub of: u32,
    pub times: u32,
    pub is: u32,
}

pub struct ProductInput {
    pub of: u32,
    pub times: u32,
}

impl TryFrom<&mut Cursor<'_>> for ProductInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: &mut Cursor) -> Result<Self, Self::Error> {
        let of = cursor.read::<u32>("of")?;
        let times = cursor.read::<u32>("times")?;
        Ok(ProductInput { of, times })
    }
}

impl Compute for Product {
    fn compute(input: Self::Input) -> Vec<Self> {
        vec![Product {
            of: input.of,
            times: input.times,
            is: input.of * input.times,
        }]
    }
}

static PRODUCT_CELLS: OnceLock<Cells> = OnceLock::new();

impl Formula for Product {
    type Input = ProductInput;
    type Match = ();

    fn operator() -> &'static str {
        "product"
    }

    fn cells() -> &'static Cells {
        PRODUCT_CELLS.get_or_init(|| {
            Cells::define(|builder| {
                builder
                    .cell("of", Type::UnsignedInt)
                    .the("Number to multiply")
                    .required();

                builder
                    .cell("times", Type::UnsignedInt)
                    .the("Times to multiply")
                    .required();

                builder
                    .cell("is", Type::UnsignedInt)
                    .the("Result of multiplication")
                    .derived(5);
            })
        })
    }

    fn cost() -> usize {
        5
    }

    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("of".into());
        dependencies.require("times".into());
        dependencies.provide("is".into());
        dependencies
    }

    fn derive(cursor: &mut Cursor) -> Result<Vec<Self>, FormulaEvaluationError> {
        let input = Self::Input::try_from(cursor)?;
        Ok(Self::compute(input))
    }

    fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError> {
        let value = Value::UnsignedInt(self.is.into());
        cursor.write("is", &value)
    }
}

/// Quotient formula that divides two numbers
#[derive(Debug, Clone)]
pub struct Quotient {
    pub of: u32,
    pub by: u32,
    pub is: u32,
}

pub struct QuotientInput {
    pub of: u32,
    pub by: u32,
}

impl TryFrom<&mut Cursor<'_>> for QuotientInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: &mut Cursor) -> Result<Self, Self::Error> {
        let of = cursor.read::<u32>("of")?;
        let by = cursor.read::<u32>("by")?;
        Ok(QuotientInput { of, by })
    }
}

impl Compute for Quotient {
    fn compute(input: Self::Input) -> Vec<Self> {
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

static QUOTIENT_CELLS: OnceLock<Cells> = OnceLock::new();

impl Formula for Quotient {
    type Input = QuotientInput;
    type Match = ();

    fn operator() -> &'static str {
        "quotient"
    }

    fn cells() -> &'static Cells {
        QUOTIENT_CELLS.get_or_init(|| {
            Cells::define(|builder| {
                builder
                    .cell("of", Type::UnsignedInt)
                    .the("Number to divide")
                    .required();

                builder
                    .cell("by", Type::UnsignedInt)
                    .the("Number to divide by")
                    .required();

                builder
                    .cell("is", Type::UnsignedInt)
                    .the("Result of division")
                    .derived(5);
            })
        })
    }

    fn cost() -> usize {
        5
    }

    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("of".into());
        dependencies.require("by".into());
        dependencies.provide("is".into());
        dependencies
    }

    fn derive(cursor: &mut Cursor) -> Result<Vec<Self>, FormulaEvaluationError> {
        let input = Self::Input::try_from(cursor)?;
        Ok(Self::compute(input))
    }

    fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError> {
        let value = Value::UnsignedInt(self.is.into());
        cursor.write("is", &value)
    }
}

/// Modulo formula that computes remainder of division
#[derive(Debug, Clone)]
pub struct Modulo {
    pub of: u32,
    pub by: u32,
    pub is: u32,
}

pub struct ModuloInput {
    pub of: u32,
    pub by: u32,
}

impl TryFrom<&mut Cursor<'_>> for ModuloInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: &mut Cursor) -> Result<Self, Self::Error> {
        let of = cursor.read::<u32>("of")?;
        let by = cursor.read::<u32>("by")?;
        Ok(ModuloInput { of, by })
    }
}

impl Compute for Modulo {
    fn compute(input: Self::Input) -> Vec<Self> {
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

static MODULO_CELLS: OnceLock<Cells> = OnceLock::new();

impl Formula for Modulo {
    type Input = ModuloInput;
    type Match = ();

    fn operator() -> &'static str {
        "modulo"
    }

    fn cells() -> &'static Cells {
        MODULO_CELLS.get_or_init(|| {
            Cells::define(|builder| {
                builder
                    .cell("of", Type::UnsignedInt)
                    .the("Number to compute modulo of")
                    .required();

                builder
                    .cell("by", Type::UnsignedInt)
                    .the("Number to compute modulo by")
                    .required();

                builder
                    .cell("is", Type::UnsignedInt)
                    .the("Result of modulo operation")
                    .derived(10);
            })
        })
    }

    fn cost() -> usize {
        10
    }

    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("of".into());
        dependencies.require("by".into());
        dependencies.provide("is".into());
        dependencies
    }

    fn derive(cursor: &mut Cursor) -> Result<Vec<Self>, FormulaEvaluationError> {
        let input = Self::Input::try_from(cursor)?;
        Ok(Self::compute(input))
    }

    fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError> {
        let value = Value::UnsignedInt(self.is.into());
        cursor.write("is", &value)
    }
}

#[cfg(test)]
mod tests {
    use crate::math::*;
    use crate::Term;
    use crate::*;

    #[test]
    fn test_sum_formula_basic() -> anyhow::Result<()> {
        // Create Terms mapping
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x").into());
        terms.insert("with".to_string(), Term::var("y").into());
        terms.insert("is".to_string(), Term::var("result").into());

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
        assert_eq!(output.resolve::<u32>(&Term::var("x")).ok(), Some(5));
        assert_eq!(output.resolve::<u32>(&Term::var("y")).ok(), Some(3));

        // Check that result is computed correctly
        assert_eq!(output.resolve::<u32>(&Term::var("result")).ok(), Some(8));
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
        terms.insert("of".to_string(), Term::var("x").into());
        terms.insert("with".to_string(), Term::var("missing").into());
        terms.insert("is".to_string(), Term::var("result").into());

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
        terms.insert("of".to_string(), Term::var("a").into());
        terms.insert("with".to_string(), Term::var("b").into());
        terms.insert("is".to_string(), Term::var("sum").into());

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
        assert_eq!(result1.resolve::<u32>(&Term::var("a")).ok(), Some(2));
        assert_eq!(result1.resolve::<u32>(&Term::var("b")).ok(), Some(3));
        assert_eq!(result1.resolve::<u32>(&Term::var("sum")).ok(), Some(5));

        // Test second input: 10 + 15 = 25
        let input2 = Answer::new()
            .set(Term::var("a"), 10u32)
            .unwrap()
            .set(Term::var("b"), 15u32)
            .unwrap();

        let results2 = app.derive(input2).expect("Second expansion failed");
        assert_eq!(results2.len(), 1);
        let result2 = &results2[0];
        assert_eq!(result2.resolve::<u32>(&Term::var("a")).ok(), Some(10));
        assert_eq!(result2.resolve::<u32>(&Term::var("b")).ok(), Some(15));
        assert_eq!(result2.resolve::<u32>(&Term::var("sum")).ok(), Some(25));
        Ok(())
    }

    #[test]
    fn test_multiple_try_from_types() -> anyhow::Result<()> {
        // Test various data types with standard TryFrom<Value>
        let bool_val = Value::Boolean(true);
        assert_eq!(bool::try_from(bool_val).unwrap(), true);

        let f64_val = Value::Float(3.14);
        assert_eq!(f64::try_from(f64_val).unwrap(), 3.14);

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
        terms.insert("of".to_string(), Term::var("x").into());
        terms.insert("subtract".to_string(), Term::var("y").into());
        terms.insert("is".to_string(), Term::var("result").into());

        let input = Answer::new()
            .set(Term::var("x"), 10u32)
            .unwrap()
            .set(Term::var("y"), 3u32)
            .unwrap();

        let app = Difference::apply(terms)?;
        let results = app.derive(input).expect("Difference failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.resolve::<u32>(&Term::var("result")).ok(), Some(7));
        Ok(())
    }

    #[test]
    fn test_difference_formula_underflow() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x").into());
        terms.insert("subtract".to_string(), Term::var("y").into());
        terms.insert("is".to_string(), Term::var("result").into());

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
        assert_eq!(result.resolve::<u32>(&Term::var("result")).ok(), Some(0));
        Ok(())
    }

    #[test]
    fn test_product_formula() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x").into());
        terms.insert("times".to_string(), Term::var("y").into());
        terms.insert("is".to_string(), Term::var("result").into());

        let input = Answer::new()
            .set(Term::var("x"), 6u32)
            .unwrap()
            .set(Term::var("y"), 7u32)
            .unwrap();

        let app = Product::apply(terms)?;
        let results = app.derive(input).expect("Product failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.resolve::<u32>(&Term::var("result")).ok(), Some(42));
        Ok(())
    }

    #[test]
    fn test_quotient_formula() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x").into());
        terms.insert("by".to_string(), Term::var("y").into());
        terms.insert("is".to_string(), Term::var("result").into());

        let input = Answer::new()
            .set(Term::var("x"), 15u32)
            .unwrap()
            .set(Term::var("y"), 3u32)
            .unwrap();

        let app = Quotient::apply(terms)?;
        let results = app.derive(input).expect("Quotient failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.resolve::<u32>(&Term::var("result")).ok(), Some(5));
        Ok(())
    }

    #[test]
    fn test_quotient_formula_division_by_zero() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x").into());
        terms.insert("by".to_string(), Term::var("y").into());
        terms.insert("is".to_string(), Term::var("result").into());

        let input = Answer::new()
            .set(Term::var("x"), 15u32)
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
        terms.insert("of".to_string(), Term::var("x").into());
        terms.insert("by".to_string(), Term::var("y").into());
        terms.insert("is".to_string(), Term::var("result").into());

        let input = Answer::new()
            .set(Term::var("x"), 17u32)
            .unwrap()
            .set(Term::var("y"), 5u32)
            .unwrap();

        let app = Modulo::apply(terms)?;
        let results = app.derive(input).expect("Modulo failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.resolve::<u32>(&Term::var("result")).ok(), Some(2));
        Ok(())
    }

    #[test]
    fn test_modulo_formula_by_zero() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x").into());
        terms.insert("by".to_string(), Term::var("y").into());
        terms.insert("is".to_string(), Term::var("result").into());

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
