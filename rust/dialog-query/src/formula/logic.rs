//! Boolean logic formulas for the query system
//!
//! This module provides formulas for boolean operations including
//! AND, OR, and NOT operations.

use crate::Formula;
use crate::dsl::Input;

/// And formula that performs logical AND on two boolean values
#[derive(Debug, Clone, Formula)]
pub struct And {
    /// Left operand
    pub left: bool,
    /// Right operand
    pub right: bool,
    /// Result of AND operation
    #[derived]
    pub is: bool,
}

impl And {
    pub fn derive(input: Input<Self>) -> Vec<Self> {
        vec![And {
            left: input.left,
            right: input.right,
            is: input.left && input.right,
        }]
    }
}

/// Or formula that performs logical OR on two boolean values
#[derive(Debug, Clone, dialog_macros::Formula)]
pub struct Or {
    /// Left operand
    pub left: bool,
    /// Right operand
    pub right: bool,
    /// Result of OR operation
    #[derived]
    pub is: bool,
}

impl Or {
    pub fn derive(input: Input<Self>) -> Vec<Self> {
        vec![Or {
            left: input.left,
            right: input.right,
            is: input.left || input.right,
        }]
    }
}

/// Not formula that performs logical NOT on a boolean value
#[derive(Debug, Clone, dialog_macros::Formula)]
pub struct Not {
    /// Boolean value to negate
    pub value: bool,
    /// Result of NOT operation
    #[derived]
    pub is: bool,
}

impl Not {
    pub fn derive(input: Input<Self>) -> Vec<Self> {
        vec![Not {
            value: input.value,
            is: !input.value,
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Formula, Parameters, Term, selection::Answer};

    #[test]
    fn test_and_formula_true_true() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("left".to_string(), Term::var("a"));
        terms.insert("right".to_string(), Term::var("b"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("a"), true)
            .unwrap()
            .set(Term::var("b"), true)
            .unwrap();

        let app = And::apply(terms)?;
        let results = app.derive(input).expect("And formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<bool>::var("result"))
                .ok()
                .and_then(|v| bool::try_from(v).ok()),
            Some(true)
        );
        Ok(())
    }

    #[test]
    fn test_and_formula_true_false() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("left".to_string(), Term::var("a"));
        terms.insert("right".to_string(), Term::var("b"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("a"), true)
            .unwrap()
            .set(Term::var("b"), false)
            .unwrap();

        let app = And::apply(terms)?;
        let results = app.derive(input).expect("And formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<bool>::var("result"))
                .ok()
                .and_then(|v| bool::try_from(v).ok()),
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn test_and_formula_false_false() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("left".to_string(), Term::var("a"));
        terms.insert("right".to_string(), Term::var("b"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("a"), false)
            .unwrap()
            .set(Term::var("b"), false)
            .unwrap();

        let app = And::apply(terms)?;
        let results = app.derive(input).expect("And formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<bool>::var("result"))
                .ok()
                .and_then(|v| bool::try_from(v).ok()),
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn test_or_formula_true_false() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("left".to_string(), Term::var("a"));
        terms.insert("right".to_string(), Term::var("b"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("a"), true)
            .unwrap()
            .set(Term::var("b"), false)
            .unwrap();

        let app = Or::apply(terms)?;
        let results = app.derive(input).expect("Or formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<bool>::var("result"))
                .ok()
                .and_then(|v| bool::try_from(v).ok()),
            Some(true)
        );
        Ok(())
    }

    #[test]
    fn test_or_formula_false_false() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("left".to_string(), Term::var("a"));
        terms.insert("right".to_string(), Term::var("b"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("a"), false)
            .unwrap()
            .set(Term::var("b"), false)
            .unwrap();

        let app = Or::apply(terms)?;
        let results = app.derive(input).expect("Or formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<bool>::var("result"))
                .ok()
                .and_then(|v| bool::try_from(v).ok()),
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn test_not_formula_true() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("bool"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new().set(Term::var("bool"), true).unwrap();

        let app = Not::apply(terms)?;
        let results = app.derive(input).expect("Not formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<bool>::var("result"))
                .ok()
                .and_then(|v| bool::try_from(v).ok()),
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn test_not_formula_false() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("bool"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new().set(Term::var("bool"), false).unwrap();

        let app = Not::apply(terms)?;

        let results = app.derive(input).expect("Not formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<bool>::var("result"))
                .ok()
                .and_then(|v| bool::try_from(v).ok()),
            Some(true)
        );
        Ok(())
    }

    #[test]
    fn test_chained_logic_operations() -> anyhow::Result<()> {
        // Test AND then NOT: !(true && false) = !false = true
        let mut and_terms = Parameters::new();
        and_terms.insert("left".to_string(), Term::var("a"));
        and_terms.insert("right".to_string(), Term::var("b"));
        and_terms.insert("is".to_string(), Term::var("and_result"));

        let input = Answer::new()
            .set(Term::var("a"), true)
            .unwrap()
            .set(Term::var("b"), false)
            .unwrap();

        let and_app = And::apply(and_terms)?;
        let and_results = and_app.derive(input).expect("And formula failed");
        let and_result = &and_results[0];

        // Now apply NOT to the result
        let mut not_terms = Parameters::new();
        not_terms.insert("value".to_string(), Term::var("and_result"));
        not_terms.insert("is".to_string(), Term::var("final_result"));

        let not_app = Not::apply(not_terms)?;
        let not_results = not_app
            .derive(and_result.clone())
            .expect("Not formula failed");

        assert_eq!(not_results.len(), 1);
        let final_result = &not_results[0];
        assert_eq!(
            final_result
                .resolve(&Term::<bool>::var("final_result"))
                .ok()
                .and_then(|v| bool::try_from(v).ok()),
            Some(true)
        );
        Ok(())
    }

    #[test]
    fn test_integration_boolean_logic() -> anyhow::Result<()> {
        // Test And formula: true AND true = true
        let mut and_terms = Parameters::new();
        and_terms.insert("left".to_string(), Term::var("a"));
        and_terms.insert("right".to_string(), Term::var("b"));
        and_terms.insert("is".to_string(), Term::var("and_result"));

        let and_formula = And::apply(and_terms)?;

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

        // Test Or formula: false OR true = true
        let mut or_terms = Parameters::new();
        or_terms.insert("left".to_string(), Term::var("x"));
        or_terms.insert("right".to_string(), Term::var("y"));
        or_terms.insert("is".to_string(), Term::var("or_result"));

        let or_formula = Or::apply(or_terms)?;

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

        // Test Not formula: NOT true = false
        let mut not_terms = Parameters::new();
        not_terms.insert("value".to_string(), Term::var("input"));
        not_terms.insert("is".to_string(), Term::var("not_result"));

        let not_formula = Not::apply(not_terms)?;

        let not_input = Answer::new().set(Term::var("input"), true).unwrap();

        let not_results = not_formula.derive(not_input)?;
        assert_eq!(not_results.len(), 1);
        assert_eq!(
            not_results[0].get::<bool>(&Term::var("not_result")).ok(),
            Some(false)
        );

        Ok(())
    }
}
