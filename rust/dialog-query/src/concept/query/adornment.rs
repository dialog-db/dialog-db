//! Adornment type for compact representation of parameter binding patterns.
//!
//! An adornment captures which of a concept's parameters are bound vs free,
//! serving as a cache key for pre-planned execution strategies.
//!
//! This is inspired by the **magic set transformation** from deductive databases
//! (Bancilhon et al., 1986), where each binding pattern — called an adornment —
//! specializes a rule for goal-directed evaluation. For non-recursive rules (our
//! current case), this reduces to pushing selections into joins: if a parameter
//! is known at query time, the planner can exploit that constraint for cheaper
//! execution. The adornment also serves as a natural cache key for eventual
//! result memoization (tabling) needed for fixpoint evaluation of recursive rules
//! (Tekle & Liu, 2011).

use crate::environment::Environment;
use crate::parameters::Parameters;
use crate::selection::Match;
use crate::term::Term;

/// Compact representation of which concept parameters are bound.
///
/// Each bit represents one parameter (ordered alphabetically by name):
/// bound = 1, free = 0. Supports up to 64 parameters.
///
/// In magic set terminology, this is the "adornment string" — a sequence of
/// b(ound)/f(ree) markers that determines how a rule should be specialized
/// for a particular calling pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Adornment(u64);

impl Adornment {
    /// Derive an adornment from a concept's terms and the current match.
    ///
    /// Parameters are sorted alphabetically by name for determinism.
    /// A parameter is "bound" if:
    /// - Its term is a `Constant`
    /// - Its term is a named `Variable` that the match contains
    pub fn derive(terms: &Parameters, matched: &Match) -> Self {
        let mut sorted_keys: Vec<&String> = terms.keys().collect();
        sorted_keys.sort();

        let mut bits: u64 = 0;
        for (i, key) in sorted_keys.iter().enumerate() {
            debug_assert!(i < 64, "Adornment supports at most 64 parameters");
            if let Some(param) = terms.get(key) {
                let bound = match param {
                    Term::Constant(_) => true,
                    Term::Variable { name: Some(_), .. } => matched.contains(param),
                    Term::Variable { name: None, .. } => false,
                };
                if bound {
                    bits |= 1 << i;
                }
            }
        }

        Adornment(bits)
    }

    /// Reconstruct an `Environment` from this adornment and the concept's terms.
    ///
    /// Bridges the adornment back to the planner's `Environment` type so
    /// existing `Conjunction::plan(&scope)` works without changes to the planner.
    pub fn into_environment(self, terms: &Parameters) -> Environment {
        let mut sorted_keys: Vec<&String> = terms.keys().collect();
        sorted_keys.sort();

        let mut env = Environment::new();
        for (i, key) in sorted_keys.iter().enumerate() {
            if self.0 & (1 << i) != 0
                && let Some(param) = terms.get(key)
            {
                param.bind(&mut env);
            }
        }

        env
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Term, Value};

    fn bind(frame: &mut Match, var_name: &str, value: Value) {
        let param = Term::var(var_name);
        frame.bind(&param, value).unwrap();
    }

    #[dialog_common::test]
    fn it_marks_all_variables_as_free() {
        let mut terms = Parameters::new();
        terms.insert("age".into(), Term::var("a"));
        terms.insert("name".into(), Term::var("n"));

        let frame = Match::new();
        let adornment = Adornment::derive(&terms, &frame);

        assert_eq!(adornment, Adornment(0));
    }

    #[dialog_common::test]
    fn it_marks_constants_as_bound() {
        let mut terms = Parameters::new();
        terms.insert("age".into(), Term::constant(25u32));
        terms.insert("name".into(), Term::var("n"));

        let candidate = Match::new();
        let adornment = Adornment::derive(&terms, &candidate);

        // "age" sorts first → bit 0 (bound), "name" → bit 1 (free)
        assert_eq!(adornment, Adornment(0b01));
    }

    #[dialog_common::test]
    fn it_marks_matched_variables_as_bound() {
        let mut terms = Parameters::new();
        terms.insert("age".into(), Term::var("a"));
        terms.insert("name".into(), Term::var("n"));

        let mut frame = Match::new();
        bind(&mut frame, "n", Value::String("Alice".into()));

        let adornment = Adornment::derive(&terms, &frame);

        // "age" = bit 0 (free), "name" = bit 1 (bound via match)
        assert_eq!(adornment, Adornment(0b10));
    }

    #[dialog_common::test]
    fn it_marks_blanks_as_free() {
        let mut terms = Parameters::new();
        terms.insert("age".into(), Term::blank());
        terms.insert("name".into(), Term::constant("Bob".to_string()));

        let frame = Match::new();
        let adornment = Adornment::derive(&terms, &frame);

        // "age" = bit 0 (blank = free), "name" = bit 1 (constant = bound)
        assert_eq!(adornment, Adornment(0b10));
    }

    #[dialog_common::test]
    fn it_marks_all_as_bound() {
        let mut terms = Parameters::new();
        terms.insert("age".into(), Term::constant(25u32));
        terms.insert("name".into(), Term::constant("Bob".to_string()));

        let frame = Match::new();
        let adornment = Adornment::derive(&terms, &frame);

        assert_eq!(adornment, Adornment(0b11));
    }

    #[dialog_common::test]
    fn it_produces_order_independent_adornments() {
        let mut terms1 = Parameters::new();
        terms1.insert("name".into(), Term::var("n"));
        terms1.insert("age".into(), Term::constant(25u32));

        let mut terms2 = Parameters::new();
        terms2.insert("age".into(), Term::constant(25u32));
        terms2.insert("name".into(), Term::var("n"));

        let frame = Match::new();
        assert_eq!(
            Adornment::derive(&terms1, &frame),
            Adornment::derive(&terms2, &frame)
        );
    }

    #[dialog_common::test]
    fn it_round_trips_through_environment() {
        let mut terms = Parameters::new();
        terms.insert("age".into(), Term::var("a"));
        terms.insert("name".into(), Term::constant("Bob".to_string()));
        terms.insert("this".into(), Term::var("e"));

        let mut frame = Match::new();
        bind(&mut frame, "e", Value::String("entity1".into()));

        let adornment = Adornment::derive(&terms, &frame);
        let env = adornment.into_environment(&terms);

        // "name" is a constant — Environment.add ignores constants
        // "this" maps to var "e" which is bound → should be in env
        // "age" maps to var "a" which is free → should not be in env
        assert!(env.contains("e"));
        assert!(!env.contains("a"));
    }

    #[dialog_common::test]
    fn it_produces_same_adornment_for_same_pattern() {
        let mut terms = Parameters::new();
        terms.insert("name".into(), Term::var("n"));
        terms.insert("age".into(), Term::var("a"));

        let mut first = Match::new();
        bind(&mut first, "n", Value::String("Alice".into()));

        let mut second = Match::new();
        bind(&mut second, "n", Value::String("Bob".into()));

        assert_eq!(
            Adornment::derive(&terms, &first),
            Adornment::derive(&terms, &second),
            "Same binding pattern should produce same adornment"
        );
    }

    #[dialog_common::test]
    fn it_produces_different_adornment_for_different_pattern() {
        let mut terms = Parameters::new();
        terms.insert("name".into(), Term::var("n"));
        terms.insert("age".into(), Term::var("a"));

        let mut first = Match::new();
        bind(&mut first, "n", Value::String("Alice".into()));

        let mut second = Match::new();
        bind(&mut second, "a", Value::UnsignedInt(25));

        assert_ne!(
            Adornment::derive(&terms, &first),
            Adornment::derive(&terms, &second),
            "Different binding patterns should produce different adornments"
        );
    }
}
