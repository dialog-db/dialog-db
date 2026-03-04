//! Boolean logic formulas for the query system
//!
//! This module provides formulas for boolean operations including
//! AND, OR, and NOT operations.

use crate::Formula;
use crate::formula::Input;

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
    /// Compute the logical AND of `left` and `right`
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
    /// Compute the logical OR of `left` and `right`
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
    /// Compute the logical NOT of `value`
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

    use crate::formula::query::FormulaQuery;
    use crate::{Formula, Parameters, Term, selection::Answer};

    #[dialog_common::test]
    fn it_ands_true_with_true() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("left".to_string(), Term::var("a"));
        terms.insert("right".to_string(), Term::var("b"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("a"), true)
            .unwrap()
            .set(Term::var("b"), true)
            .unwrap();

        let app: FormulaQuery = And::apply(terms)?.into();
        let results = app.derive(input).expect("And formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::var("result"))
                .ok()
                .and_then(|v| bool::try_from(v).ok()),
            Some(true)
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_ands_true_with_false() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("left".to_string(), Term::var("a"));
        terms.insert("right".to_string(), Term::var("b"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("a"), true)
            .unwrap()
            .set(Term::var("b"), false)
            .unwrap();

        let app: FormulaQuery = And::apply(terms)?.into();
        let results = app.derive(input).expect("And formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::var("result"))
                .ok()
                .and_then(|v| bool::try_from(v).ok()),
            Some(false)
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_ands_false_with_false() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("left".to_string(), Term::var("a"));
        terms.insert("right".to_string(), Term::var("b"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("a"), false)
            .unwrap()
            .set(Term::var("b"), false)
            .unwrap();

        let app: FormulaQuery = And::apply(terms)?.into();
        let results = app.derive(input).expect("And formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::var("result"))
                .ok()
                .and_then(|v| bool::try_from(v).ok()),
            Some(false)
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_ors_true_with_false() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("left".to_string(), Term::var("a"));
        terms.insert("right".to_string(), Term::var("b"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("a"), true)
            .unwrap()
            .set(Term::var("b"), false)
            .unwrap();

        let app: FormulaQuery = Or::apply(terms)?.into();
        let results = app.derive(input).expect("Or formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::var("result"))
                .ok()
                .and_then(|v| bool::try_from(v).ok()),
            Some(true)
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_ors_false_with_false() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("left".to_string(), Term::var("a"));
        terms.insert("right".to_string(), Term::var("b"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("a"), false)
            .unwrap()
            .set(Term::var("b"), false)
            .unwrap();

        let app: FormulaQuery = Or::apply(terms)?.into();
        let results = app.derive(input).expect("Or formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::var("result"))
                .ok()
                .and_then(|v| bool::try_from(v).ok()),
            Some(false)
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_negates_true() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("bool"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new().set(Term::var("bool"), true).unwrap();

        let app: FormulaQuery = Not::apply(terms)?.into();
        let results = app.derive(input).expect("Not formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::var("result"))
                .ok()
                .and_then(|v| bool::try_from(v).ok()),
            Some(false)
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_negates_false() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("bool"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new().set(Term::var("bool"), false).unwrap();

        let app: FormulaQuery = Not::apply(terms)?.into();

        let results = app.derive(input).expect("Not formula failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::var("result"))
                .ok()
                .and_then(|v| bool::try_from(v).ok()),
            Some(true)
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_chains_logic_operations() -> anyhow::Result<()> {
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

        let and_app: FormulaQuery = And::apply(and_terms)?.into();
        let and_results = and_app.derive(input).expect("And formula failed");
        let and_result = &and_results[0];

        // Now apply NOT to the result
        let mut not_terms = Parameters::new();
        not_terms.insert("value".to_string(), Term::var("and_result"));
        not_terms.insert("is".to_string(), Term::var("final_result"));

        let not_app: FormulaQuery = Not::apply(not_terms)?.into();
        let not_results = not_app
            .derive(and_result.clone())
            .expect("Not formula failed");

        assert_eq!(not_results.len(), 1);
        let final_result = &not_results[0];
        assert_eq!(
            final_result
                .resolve(&Term::var("final_result"))
                .ok()
                .and_then(|v| bool::try_from(v).ok()),
            Some(true)
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_integrates_boolean_logic() -> anyhow::Result<()> {
        // Test And formula: true AND true = true
        let mut and_terms = Parameters::new();
        and_terms.insert("left".to_string(), Term::var("a"));
        and_terms.insert("right".to_string(), Term::var("b"));
        and_terms.insert("is".to_string(), Term::var("and_result"));

        let and_formula: FormulaQuery = And::apply(and_terms)?.into();

        let and_input = Answer::new()
            .set(Term::var("a"), true)
            .unwrap()
            .set(Term::var("b"), true)
            .unwrap();

        let and_results = and_formula.derive(and_input)?;
        assert_eq!(and_results.len(), 1);
        assert_eq!(
            bool::try_from(and_results[0].resolve(&Term::var("and_result")).unwrap()).ok(),
            Some(true)
        );

        // Test Or formula: false OR true = true
        let mut or_terms = Parameters::new();
        or_terms.insert("left".to_string(), Term::var("x"));
        or_terms.insert("right".to_string(), Term::var("y"));
        or_terms.insert("is".to_string(), Term::var("or_result"));

        let or_formula: FormulaQuery = Or::apply(or_terms)?.into();

        let or_input = Answer::new()
            .set(Term::var("x"), false)
            .unwrap()
            .set(Term::var("y"), true)
            .unwrap();

        let or_results = or_formula.derive(or_input)?;
        assert_eq!(or_results.len(), 1);
        assert_eq!(
            bool::try_from(or_results[0].resolve(&Term::var("or_result")).unwrap()).ok(),
            Some(true)
        );

        // Test Not formula: NOT true = false
        let mut not_terms = Parameters::new();
        not_terms.insert("value".to_string(), Term::var("input"));
        not_terms.insert("is".to_string(), Term::var("not_result"));

        let not_formula: FormulaQuery = Not::apply(not_terms)?.into();

        let not_input = Answer::new().set(Term::var("input"), true).unwrap();

        let not_results = not_formula.derive(not_input)?;
        assert_eq!(not_results.len(), 1);
        assert_eq!(
            bool::try_from(not_results[0].resolve(&Term::var("not_result")).unwrap()).ok(),
            Some(false)
        );

        Ok(())
    }
}
