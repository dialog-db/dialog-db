use crate::{
    cursor::Cursor, error::FormulaEvaluationError, Compute, Dependencies, Formula, Term, Value,
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

impl TryFrom<Cursor> for SumInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: Cursor) -> Result<Self, Self::Error> {
        let of = cursor.read::<u32>("of")?;
        let with = cursor.read::<u32>("with")?;
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

impl Formula for Sum {
    type Input = SumInput;
    type Match = SumMatch;

    fn name() -> &'static str {
        "sum"
    }
    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("of".into());
        dependencies.require("with".into());
        dependencies.provide("is".into());

        dependencies
    }

    fn derive(cursor: &Cursor) -> Result<Vec<Self>, FormulaEvaluationError> {
        let input = Self::Input::try_from(cursor.clone())?;
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
    fn test_sum_formula_basic() {
        // Create Terms mapping
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x").into());
        terms.insert("with".to_string(), Term::var("y").into());
        terms.insert("is".to_string(), Term::var("result").into());

        // Create input match with x=5, y=3
        let input = Match::new()
            .set(Term::var("x"), 5u32)
            .expect("Failed to set x")
            .set(Term::var("y"), 3u32)
            .expect("Failed to set y");

        // Create formula application
        let app = Sum::apply(terms);

        // Expand the formula
        let results = app.derive(input).expect("Formula expansion failed");

        // Verify results
        assert_eq!(results.len(), 1);
        let output = &results[0];

        // Check that x and y are preserved
        assert_eq!(output.get::<u32>(&Term::var("x")).ok(), Some(5));
        assert_eq!(output.get::<u32>(&Term::var("y")).ok(), Some(3));

        // Check that result is computed correctly
        assert_eq!(output.get::<u32>(&Term::var("result")).ok(), Some(8));
    }

    #[test]
    fn test_cursor_read_write() {
        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("test").into());

        let source = Match::new()
            .set(Term::var("test"), 42u32)
            .expect("Failed to create test match");

        let cursor = Cursor::new(source, terms);

        // Test reading
        let value = cursor.read::<u32>("value").expect("Failed to read value");
        assert_eq!(value, 42);

        // Test writing
        let mut write_cursor = cursor.clone();
        let new_value = Value::UnsignedInt(100);
        write_cursor
            .write("value", &new_value)
            .expect("Failed to write value");

        let written_value = write_cursor
            .read::<u32>("value")
            .expect("Failed to read written value");
        assert_eq!(written_value, 100);
    }

    #[test]
    fn test_sum_formula_missing_input() {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("x").into());
        terms.insert("with".to_string(), Term::var("missing").into());
        terms.insert("is".to_string(), Term::var("result").into());

        let input = Match::new()
            .set(Term::var("x"), 5u32)
            .expect("Failed to set x");

        let app = Sum::apply(terms);
        let result = app.derive(input);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FormulaEvaluationError::UnboundVariable { .. }
        ));
    }

    #[test]
    fn test_sum_formula_multiple_expand() {
        // Test multiple expansions without the stream complexity
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("a").into());
        terms.insert("with".to_string(), Term::var("b").into());
        terms.insert("is".to_string(), Term::var("sum").into());

        let app = Sum::apply(terms);

        // Test first input: 2 + 3 = 5
        let input1 = Match::new()
            .set(Term::var("a"), 2u32)
            .unwrap()
            .set(Term::var("b"), 3u32)
            .unwrap();

        let results1 = app.derive(input1).expect("First expansion failed");
        assert_eq!(results1.len(), 1);
        let result1 = &results1[0];
        assert_eq!(result1.get::<u32>(&Term::var("a")).ok(), Some(2));
        assert_eq!(result1.get::<u32>(&Term::var("b")).ok(), Some(3));
        assert_eq!(result1.get::<u32>(&Term::var("sum")).ok(), Some(5));

        // Test second input: 10 + 15 = 25
        let input2 = Match::new()
            .set(Term::var("a"), 10u32)
            .unwrap()
            .set(Term::var("b"), 15u32)
            .unwrap();

        let results2 = app.derive(input2).expect("Second expansion failed");
        assert_eq!(results2.len(), 1);
        let result2 = &results2[0];
        assert_eq!(result2.get::<u32>(&Term::var("a")).ok(), Some(10));
        assert_eq!(result2.get::<u32>(&Term::var("b")).ok(), Some(15));
        assert_eq!(result2.get::<u32>(&Term::var("sum")).ok(), Some(25));
    }

    #[test]
    fn test_multiple_try_from_types() {
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
    }
}
