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
use crate::selection::Answer;
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
    /// Derive an adornment from a concept's terms and the current answer.
    ///
    /// Parameters are sorted alphabetically by name for determinism.
    /// A parameter is "bound" if:
    /// - Its term is a `Constant`
    /// - Its term is a named `Variable` that the answer contains
    pub fn derive(terms: &Parameters, answer: &Answer) -> Self {
        let mut sorted_keys: Vec<&String> = terms.keys().collect();
        sorted_keys.sort();

        let mut bits: u64 = 0;
        for (i, key) in sorted_keys.iter().enumerate() {
            debug_assert!(i < 64, "Adornment supports at most 64 parameters");
            if let Some(term) = terms.get(key) {
                let bound = match term {
                    Term::Constant(_) => true,
                    Term::Variable { name: Some(_), .. } => answer.contains(term),
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
                && let Some(term) = terms.get(key)
            {
                env.add(term);
            }
        }

        env
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selection::Evidence;
    use crate::{Term, Value};

    fn bind(answer: &mut Answer, var_name: &str, value: Value) {
        let term = Term::<Value>::var(var_name);
        answer
            .merge(Evidence::Parameter {
                term: &term,
                value: &value,
            })
            .unwrap();
    }

    #[dialog_common::test]
    fn it_marks_all_variables_as_free() {
        let mut terms = Parameters::new();
        terms.insert("age".into(), Term::<Value>::var("a"));
        terms.insert("name".into(), Term::<Value>::var("n"));

        let answer = Answer::new();
        let adornment = Adornment::derive(&terms, &answer);

        assert_eq!(adornment, Adornment(0));
    }

    #[dialog_common::test]
    fn it_marks_constants_as_bound() {
        let mut terms = Parameters::new();
        terms.insert("age".into(), Term::Constant(Value::UnsignedInt(25)));
        terms.insert("name".into(), Term::<Value>::var("n"));

        let answer = Answer::new();
        let adornment = Adornment::derive(&terms, &answer);

        // "age" sorts first → bit 0 (bound), "name" → bit 1 (free)
        assert_eq!(adornment, Adornment(0b01));
    }

    #[dialog_common::test]
    fn it_marks_answered_variables_as_bound() {
        let mut terms = Parameters::new();
        terms.insert("age".into(), Term::<Value>::var("a"));
        terms.insert("name".into(), Term::<Value>::var("n"));

        let mut answer = Answer::new();
        bind(&mut answer, "n", Value::String("Alice".into()));

        let adornment = Adornment::derive(&terms, &answer);

        // "age" = bit 0 (free), "name" = bit 1 (bound via answer)
        assert_eq!(adornment, Adornment(0b10));
    }

    #[dialog_common::test]
    fn it_marks_blanks_as_free() {
        let mut terms = Parameters::new();
        terms.insert("age".into(), Term::<Value>::blank());
        terms.insert("name".into(), Term::Constant(Value::String("Bob".into())));

        let answer = Answer::new();
        let adornment = Adornment::derive(&terms, &answer);

        // "age" = bit 0 (blank = free), "name" = bit 1 (constant = bound)
        assert_eq!(adornment, Adornment(0b10));
    }

    #[dialog_common::test]
    fn it_marks_all_as_bound() {
        let mut terms = Parameters::new();
        terms.insert("age".into(), Term::Constant(Value::UnsignedInt(25)));
        terms.insert("name".into(), Term::Constant(Value::String("Bob".into())));

        let answer = Answer::new();
        let adornment = Adornment::derive(&terms, &answer);

        assert_eq!(adornment, Adornment(0b11));
    }

    #[dialog_common::test]
    fn it_produces_order_independent_adornments() {
        let mut terms1 = Parameters::new();
        terms1.insert("name".into(), Term::<Value>::var("n"));
        terms1.insert("age".into(), Term::Constant(Value::UnsignedInt(25)));

        let mut terms2 = Parameters::new();
        terms2.insert("age".into(), Term::Constant(Value::UnsignedInt(25)));
        terms2.insert("name".into(), Term::<Value>::var("n"));

        let answer = Answer::new();
        assert_eq!(
            Adornment::derive(&terms1, &answer),
            Adornment::derive(&terms2, &answer)
        );
    }

    #[dialog_common::test]
    fn it_round_trips_through_environment() {
        let mut terms = Parameters::new();
        terms.insert("age".into(), Term::<Value>::var("a"));
        terms.insert("name".into(), Term::Constant(Value::String("Bob".into())));
        terms.insert("this".into(), Term::<Value>::var("e"));

        let mut answer = Answer::new();
        bind(&mut answer, "e", Value::String("entity1".into()));

        let adornment = Adornment::derive(&terms, &answer);
        let env = adornment.into_environment(&terms);

        // "name" is a constant — Environment.add ignores constants
        // "this" maps to var "e" which is bound → should be in env
        // "age" maps to var "a" which is free → should not be in env
        assert!(env.contains(&Term::<Value>::var("e")));
        assert!(!env.contains(&Term::<Value>::var("a")));
    }

    #[dialog_common::test]
    fn it_produces_same_adornment_for_same_pattern() {
        let mut terms = Parameters::new();
        terms.insert("name".into(), Term::<Value>::var("n"));
        terms.insert("age".into(), Term::<Value>::var("a"));

        let mut answer1 = Answer::new();
        bind(&mut answer1, "n", Value::String("Alice".into()));

        let mut answer2 = Answer::new();
        bind(&mut answer2, "n", Value::String("Bob".into()));

        assert_eq!(
            Adornment::derive(&terms, &answer1),
            Adornment::derive(&terms, &answer2),
            "Same binding pattern should produce same adornment"
        );
    }

    #[dialog_common::test]
    fn it_produces_different_adornment_for_different_pattern() {
        let mut terms = Parameters::new();
        terms.insert("name".into(), Term::<Value>::var("n"));
        terms.insert("age".into(), Term::<Value>::var("a"));

        let mut answer1 = Answer::new();
        bind(&mut answer1, "n", Value::String("Alice".into()));

        let mut answer2 = Answer::new();
        bind(&mut answer2, "a", Value::UnsignedInt(25));

        assert_ne!(
            Adornment::derive(&terms, &answer1),
            Adornment::derive(&terms, &answer2),
            "Different binding patterns should produce different adornments"
        );
    }
}
