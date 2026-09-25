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
/// Each bit is one of the concept's operands, in the order the concept
/// lists them (see [`ConceptDescriptor::sorted_operands`]): bound = 1,
/// free = 0. Supports up to 64 operands.
///
/// The bits are numbered over the concept, not over the call. A call
/// names only the operands it uses, so numbering by its own terms gave
/// `{name, this}` with `this` bound and `{age, name, this}` with `name`
/// bound the same adornment, and one call was planned as the other.
///
/// In magic set terminology, this is the "adornment string" — a sequence of
/// b(ound)/f(ree) markers that determines how a rule should be specialized
/// for a particular calling pattern.
///
/// [`ConceptDescriptor::sorted_operands`]: crate::concept::descriptor::ConceptDescriptor::sorted_operands
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Adornment(u64);

impl Adornment {
    /// Derive an adornment for a call of a concept whose operands are
    /// `operands`, from the call's terms and the current match.
    ///
    /// An operand is "bound" if the call gives it a term that is
    /// - a `Constant`, or
    /// - a named `Variable` that the match contains.
    ///
    /// An operand the call does not name is free.
    pub fn derive(operands: &[String], terms: &Parameters, matched: &Match) -> Self {
        let mut bits: u64 = 0;
        for (i, operand) in operands.iter().enumerate() {
            debug_assert!(i < 64, "Adornment supports at most 64 operands");
            let bound = match terms.get(operand) {
                Some(Term::Constant(_)) => true,
                Some(param @ Term::Variable { name: Some(_), .. }) => matched.contains(param),
                Some(Term::Variable { name: None, .. }) | None => false,
            };
            if bound {
                bits |= 1 << i;
            }
        }

        Adornment(bits)
    }

    /// Reconstruct the scope a rule is planned in from this adornment and
    /// the concept's operands.
    ///
    /// A rule's body is evaluated over the concept's own parameter names:
    /// the caller's bindings are carried over under the field they are
    /// given for, constants included (see `extract_parameters`). So the
    /// scope names the *fields* that are bound -- not the caller's
    /// variables, which the body never sees. Naming the caller's instead
    /// planned every rule called with a constant, or with variables named
    /// unlike its fields, as though those fields were free: a lookup by a
    /// known entity became a scan of every entity.
    pub fn into_environment(self, operands: &[String]) -> Environment {
        let mut env = Environment::new();
        for (i, operand) in operands.iter().enumerate() {
            if self.0 & (1 << i) != 0 {
                env.add(operand.clone());
            }
        }

        env
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Term, Value};

    /// The operands of a concept with the given names, in order.
    fn operands(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

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
        let adornment = Adornment::derive(&operands(&["age", "name"]), &terms, &frame);

        assert_eq!(adornment, Adornment(0));
    }

    #[dialog_common::test]
    fn it_marks_constants_as_bound() {
        let mut terms = Parameters::new();
        terms.insert("age".into(), Term::constant(25u32));
        terms.insert("name".into(), Term::var("n"));

        let candidate = Match::new();
        let adornment = Adornment::derive(&operands(&["age", "name"]), &terms, &candidate);

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

        let adornment = Adornment::derive(&operands(&["age", "name"]), &terms, &frame);

        // "age" = bit 0 (free), "name" = bit 1 (bound via match)
        assert_eq!(adornment, Adornment(0b10));
    }

    #[dialog_common::test]
    fn it_marks_blanks_as_free() {
        let mut terms = Parameters::new();
        terms.insert("age".into(), Term::blank());
        terms.insert("name".into(), Term::constant("Bob".to_string()));

        let frame = Match::new();
        let adornment = Adornment::derive(&operands(&["age", "name"]), &terms, &frame);

        // "age" = bit 0 (blank = free), "name" = bit 1 (constant = bound)
        assert_eq!(adornment, Adornment(0b10));
    }

    #[dialog_common::test]
    fn it_marks_all_as_bound() {
        let mut terms = Parameters::new();
        terms.insert("age".into(), Term::constant(25u32));
        terms.insert("name".into(), Term::constant("Bob".to_string()));

        let frame = Match::new();
        let adornment = Adornment::derive(&operands(&["age", "name"]), &terms, &frame);

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
        let names = operands(&["age", "name"]);
        assert_eq!(
            Adornment::derive(&names, &terms1, &frame),
            Adornment::derive(&names, &terms2, &frame)
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

        let names = operands(&["age", "name", "this"]);
        let adornment = Adornment::derive(&names, &terms, &frame);
        let env = adornment.into_environment(&names);

        // The scope names the concept's fields, which the rule body is
        // evaluated over -- never the caller's variables.
        // "this" is bound through the caller's "e", "name" by a constant,
        // and "age" is free.
        assert!(env.contains("this"));
        assert!(env.contains("name"));
        assert!(!env.contains("age"));
        assert!(!env.contains("e"), "the caller's variable is not in scope");
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
            Adornment::derive(&operands(&["age", "name"]), &terms, &first),
            Adornment::derive(&operands(&["age", "name"]), &terms, &second),
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
            Adornment::derive(&operands(&["age", "name"]), &terms, &first),
            Adornment::derive(&operands(&["age", "name"]), &terms, &second),
            "Different binding patterns should produce different adornments"
        );
    }

    /// An operand the call does not name keeps its own bit, free, so
    /// calls naming different subsets of a concept's operands never
    /// collide.
    #[dialog_common::test]
    fn it_numbers_bits_over_the_concept_not_the_call() {
        let names = operands(&["age", "name", "this"]);

        let mut by_entity = Parameters::new();
        by_entity.insert("name".into(), Term::var("n"));
        by_entity.insert("this".into(), Term::var("e"));
        let mut entity_bound = Match::new();
        bind(&mut entity_bound, "e", Value::String("entity1".into()));

        let mut by_name = Parameters::new();
        by_name.insert("age".into(), Term::var("a"));
        by_name.insert("name".into(), Term::var("n"));
        by_name.insert("this".into(), Term::var("e"));
        let mut name_bound = Match::new();
        bind(&mut name_bound, "n", Value::String("Alice".into()));

        assert_eq!(
            Adornment::derive(&names, &by_entity, &entity_bound),
            Adornment(0b100)
        );
        assert_eq!(
            Adornment::derive(&names, &by_name, &name_bound),
            Adornment(0b010)
        );
    }
}
