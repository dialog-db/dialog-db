//! Renaming the variables of a rule body.
//!
//! A head binds its body by variable name, so re-heading a rule onto a
//! concept whose operands are named differently means renaming the
//! body's variables to the new operand names, and renaming any body
//! variable the new names would capture out of the way first.

use std::collections::{BTreeMap, BTreeSet};

use crate::attribute::query::AttributeQuery;
use crate::concept::query::ConceptQuery;
use crate::error::TypeError;
use crate::negation::Negation;
use crate::optional::OptionalAttributeQuery;
use crate::parameters::Parameters;
use crate::premise::Premise;
use crate::proposition::Proposition;
use crate::term::Term;
use crate::types::Typed;

/// A map from variable names to the names that replace them.
pub type Rename = BTreeMap<String, String>;

/// Every named variable the premises mention.
pub fn variables(premises: &[Premise]) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for premise in premises {
        for (_, term) in premise.parameters().iter() {
            if let Some(name) = term.name() {
                names.insert(name.to_string());
            }
        }
    }
    names
}

/// A variable name derived from `base` that neither `taken` nor the
/// targets of `map` use.
pub(crate) fn fresh_name(base: &str, taken: &BTreeSet<String>, map: &Rename) -> String {
    let mut counter = 1usize;
    loop {
        let candidate = format!("{base}~{counter}");
        if !taken.contains(&candidate) && !map.values().any(|value| *value == candidate) {
            return candidate;
        }
        counter += 1;
    }
}

/// Rename the variables of each premise per `map`, leaving every other
/// term untouched. Names absent from the map are unchanged, and every
/// renaming applies simultaneously: a map swapping two names swaps
/// them.
pub fn rename_premises(premises: &[Premise], map: &Rename) -> Result<Vec<Premise>, TypeError> {
    if map.is_empty() {
        return Ok(premises.to_vec());
    }
    premises
        .iter()
        .map(|premise| {
            Ok(match premise {
                Premise::Assert(proposition) => {
                    Premise::Assert(rename_proposition(proposition, map)?)
                }
                Premise::Unless(Negation(proposition)) => {
                    Premise::Unless(Negation(rename_proposition(proposition, map)?))
                }
            })
        })
        .collect()
}

fn rename_proposition(proposition: &Proposition, map: &Rename) -> Result<Proposition, TypeError> {
    Ok(match proposition {
        Proposition::Concept(query) => Proposition::Concept(ConceptQuery {
            terms: rename_parameters(&query.terms, map),
            predicate: query.predicate.clone(),
        }),
        Proposition::Attribute(query) => Proposition::Attribute(Box::new(AttributeQuery::new(
            rename_term(query.the(), map),
            rename_term(query.of(), map),
            rename_term(query.is(), map),
            rename_term(query.cause(), map),
            Some(query.cardinality()),
        ))),
        Proposition::OptionalAttribute(query) => {
            let inner = query.query();
            Proposition::OptionalAttribute(Box::new(OptionalAttributeQuery::new(
                rename_term(inner.the(), map),
                rename_term(inner.of(), map),
                rename_term(inner.is(), map),
                rename_term(inner.cause(), map),
                Some(inner.cardinality()),
            )))
        }
        // Formulas, resolvers and constraints rebuild from their own
        // parameter maps, so a constant never leaves its typed form.
        Proposition::Formula(formula) => Proposition::Formula(
            formula.with_parameters(rename_parameters(&formula.parameters(), map))?,
        ),
        Proposition::Resolver(resolver) => Proposition::Resolver(
            resolver.with_parameters(&rename_parameters(&resolver.parameters(), map)),
        ),
        Proposition::Constraint(constraint) => Proposition::Constraint(
            constraint.with_parameters(&rename_parameters(&constraint.parameters(), map)),
        ),
    })
}

fn rename_parameters(parameters: &Parameters, map: &Rename) -> Parameters {
    let mut renamed = Parameters::new();
    for (key, term) in parameters.iter() {
        renamed.insert(key.clone(), rename_term(term, map));
    }
    renamed
}

fn rename_term<T>(term: &Term<T>, map: &Rename) -> Term<T>
where
    T: Typed,
    <T as Typed>::Descriptor: Clone,
    Term<T>: Clone,
{
    match term {
        Term::Variable {
            name: Some(name),
            descriptor,
        } => match map.get(name.as_ref()) {
            Some(renamed) => Term::Variable {
                name: Some(renamed.as_str().into()),
                descriptor: descriptor.clone(),
            },
            None => term.clone(),
        },
        other => other.clone(),
    }
}
