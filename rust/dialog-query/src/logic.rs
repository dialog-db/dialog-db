//! Boolean logic formulas for the query system
//!
//! This module provides formulas for boolean operations including
//! AND, OR, and NOT operations.

use crate::{
    cursor::Cursor, error::FormulaEvaluationError, predicate::formula::Cells, Compute,
    Dependencies, Formula, Type, Value,
};

use std::sync::OnceLock;

// ============================================================================
// Boolean Logic Operations: And, Or, Not
// ============================================================================

/// And formula that performs logical AND on two boolean values
#[derive(Debug, Clone)]
pub struct And {
    pub left: bool,
    pub right: bool,
    pub is: bool,
}

pub struct AndInput {
    pub left: bool,
    pub right: bool,
}

impl TryFrom<Cursor> for AndInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: Cursor) -> Result<Self, Self::Error> {
        let left = cursor.read::<bool>("left")?;
        let right = cursor.read::<bool>("right")?;
        Ok(AndInput { left, right })
    }
}

impl Compute for And {
    fn compute(input: Self::Input) -> Vec<Self> {
        vec![And {
            left: input.left,
            right: input.right,
            is: input.left && input.right,
        }]
    }
}

static AND_CELLS: OnceLock<Cells> = OnceLock::new();

impl Formula for And {
    type Input = AndInput;
    type Match = ();

    fn operator() -> &'static str {
        "and"
    }

    fn cells() -> &'static Cells {
        AND_CELLS.get_or_init(|| {
            Cells::define(|cell| {
                cell("left", Type::Boolean).the("Left operand").required();

                cell("right", Type::Boolean).the("Right operand").required();

                cell("is", Type::Boolean)
                    .the("Result of AND operation")
                    .derived(1);
            })
        })
    }

    fn cost() -> usize {
        1
    }

    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("left".into());
        dependencies.require("right".into());
        dependencies.provide("is".into());
        dependencies
    }

    fn derive(cursor: &Cursor) -> Result<Vec<Self>, FormulaEvaluationError> {
        let input = Self::Input::try_from(cursor.clone())?;
        Ok(Self::compute(input))
    }

    fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError> {
        let value = Value::Boolean(self.is);
        cursor.write("is", &value)
    }
}

/// Or formula that performs logical OR on two boolean values
#[derive(Debug, Clone)]
pub struct Or {
    pub left: bool,
    pub right: bool,
    pub is: bool,
}

pub struct OrInput {
    pub left: bool,
    pub right: bool,
}

impl TryFrom<Cursor> for OrInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: Cursor) -> Result<Self, Self::Error> {
        let left = cursor.read::<bool>("left")?;
        let right = cursor.read::<bool>("right")?;
        Ok(OrInput { left, right })
    }
}

impl Compute for Or {
    fn compute(input: Self::Input) -> Vec<Self> {
        vec![Or {
            left: input.left,
            right: input.right,
            is: input.left || input.right,
        }]
    }
}

static OR_CELLS: OnceLock<Cells> = OnceLock::new();

impl Formula for Or {
    type Input = OrInput;
    type Match = ();

    fn operator() -> &'static str {
        "or"
    }

    fn cells() -> &'static Cells {
        OR_CELLS.get_or_init(|| {
            Cells::define(|cell| {
                cell("left", Type::Boolean).the("Left operand").required();

                cell("right", Type::Boolean).the("Right operand").required();

                cell("is", Type::Boolean)
                    .the("Result of OR operation")
                    .derived(1);
            })
        })
    }

    fn cost() -> usize {
        1
    }

    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("left".into());
        dependencies.require("right".into());
        dependencies.provide("is".into());
        dependencies
    }

    fn derive(cursor: &Cursor) -> Result<Vec<Self>, FormulaEvaluationError> {
        let input = Self::Input::try_from(cursor.clone())?;
        Ok(Self::compute(input))
    }

    fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError> {
        let value = Value::Boolean(self.is);
        cursor.write("is", &value)
    }
}

/// Not formula that performs logical NOT on a boolean value
#[derive(Debug, Clone)]
pub struct Not {
    pub value: bool,
    pub is: bool,
}

pub struct NotInput {
    pub value: bool,
}

impl TryFrom<Cursor> for NotInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: Cursor) -> Result<Self, Self::Error> {
        let value = cursor.read::<bool>("value")?;
        Ok(NotInput { value })
    }
}

impl Compute for Not {
    fn compute(input: Self::Input) -> Vec<Self> {
        vec![Not {
            value: input.value,
            is: !input.value,
        }]
    }
}

static NOT_CELLS: OnceLock<Cells> = OnceLock::new();

impl Formula for Not {
    type Input = NotInput;
    type Match = ();

    fn operator() -> &'static str {
        "not"
    }

    fn cells() -> &'static Cells {
        NOT_CELLS.get_or_init(|| {
            Cells::define(|cell| {
                cell("value", Type::Boolean)
                    .the("Boolean value to negate")
                    .required();

                cell("is", Type::Boolean)
                    .the("Result of NOT operation")
                    .derived(1);
            })
        })
    }

    fn cost() -> usize {
        1
    }

    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("value".into());
        dependencies.provide("is".into());
        dependencies
    }

    fn derive(cursor: &Cursor) -> Result<Vec<Self>, FormulaEvaluationError> {
        let input = Self::Input::try_from(cursor.clone())?;
        Ok(Self::compute(input))
    }

    fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError> {
        let value = Value::Boolean(self.is);
        cursor.write("is", &value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Match, Parameters, Term};

    #[test]
    fn test_and_formula_true_true() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("left".to_string(), Term::var("a").into());
        terms.insert("right".to_string(), Term::var("b").into());
        terms.insert("is".to_string(), Term::var("result").into());

        let input = Match::new()
            .set(Term::var("a"), true)
            .unwrap()
            .set(Term::var("b"), true)
            .unwrap();

        let app = And::apply(terms)?;
        let results = app.derive(input).expect("And formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.get::<bool>(&Term::var("result")).ok(), Some(true));
        Ok(())
    }

    #[test]
    fn test_and_formula_true_false() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("left".to_string(), Term::var("a").into());
        terms.insert("right".to_string(), Term::var("b").into());
        terms.insert("is".to_string(), Term::var("result").into());

        let input = Match::new()
            .set(Term::var("a"), true)
            .unwrap()
            .set(Term::var("b"), false)
            .unwrap();

        let app = And::apply(terms)?;
        let results = app.derive(input).expect("And formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.get::<bool>(&Term::var("result")).ok(), Some(false));
        Ok(())
    }

    #[test]
    fn test_and_formula_false_false() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("left".to_string(), Term::var("a").into());
        terms.insert("right".to_string(), Term::var("b").into());
        terms.insert("is".to_string(), Term::var("result").into());

        let input = Match::new()
            .set(Term::var("a"), false)
            .unwrap()
            .set(Term::var("b"), false)
            .unwrap();

        let app = And::apply(terms)?;
        let results = app.derive(input).expect("And formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.get::<bool>(&Term::var("result")).ok(), Some(false));
        Ok(())
    }

    #[test]
    fn test_or_formula_true_false() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("left".to_string(), Term::var("a").into());
        terms.insert("right".to_string(), Term::var("b").into());
        terms.insert("is".to_string(), Term::var("result").into());

        let input = Match::new()
            .set(Term::var("a"), true)
            .unwrap()
            .set(Term::var("b"), false)
            .unwrap();

        let app = Or::apply(terms)?;
        let results = app.derive(input).expect("Or formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.get::<bool>(&Term::var("result")).ok(), Some(true));
        Ok(())
    }

    #[test]
    fn test_or_formula_false_false() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("left".to_string(), Term::var("a").into());
        terms.insert("right".to_string(), Term::var("b").into());
        terms.insert("is".to_string(), Term::var("result").into());

        let input = Match::new()
            .set(Term::var("a"), false)
            .unwrap()
            .set(Term::var("b"), false)
            .unwrap();

        let app = Or::apply(terms)?;
        let results = app.derive(input).expect("Or formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.get::<bool>(&Term::var("result")).ok(), Some(false));
        Ok(())
    }

    #[test]
    fn test_not_formula_true() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("bool").into());
        terms.insert("is".to_string(), Term::var("result").into());

        let input = Match::new().set(Term::var("bool"), true).unwrap();

        let app = Not::apply(terms)?;
        let results = app.derive(input).expect("Not formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.get::<bool>(&Term::var("result")).ok(), Some(false));
        Ok(())
    }

    #[test]
    fn test_not_formula_false() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("bool").into());
        terms.insert("is".to_string(), Term::var("result").into());

        let input = Match::new().set(Term::var("bool"), false).unwrap();

        let app = Not::apply(terms)?;

        let results = app.derive(input).expect("Not formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.get::<bool>(&Term::var("result")).ok(), Some(true));
        Ok(())
    }

    #[test]
    fn test_chained_logic_operations() -> anyhow::Result<()> {
        // Test AND then NOT: !(true && false) = !false = true
        let mut and_terms = Parameters::new();
        and_terms.insert("left".to_string(), Term::var("a").into());
        and_terms.insert("right".to_string(), Term::var("b").into());
        and_terms.insert("is".to_string(), Term::var("and_result").into());

        let input = Match::new()
            .set(Term::var("a"), true)
            .unwrap()
            .set(Term::var("b"), false)
            .unwrap();

        let and_app = And::apply(and_terms)?;
        let and_results = and_app.derive(input).expect("And formula failed");
        let and_result = &and_results[0];

        // Now apply NOT to the result
        let mut not_terms = Parameters::new();
        not_terms.insert("value".to_string(), Term::var("and_result").into());
        not_terms.insert("is".to_string(), Term::var("final_result").into());

        let not_app = Not::apply(not_terms)?;
        let not_results = not_app
            .derive(and_result.clone())
            .expect("Not formula failed");

        assert_eq!(not_results.len(), 1);
        let final_result = &not_results[0];
        assert_eq!(
            final_result.get::<bool>(&Term::var("final_result")).ok(),
            Some(true)
        );
        Ok(())
    }
}
