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

/// What a pass over a body does to its named variables.
enum Renaming<'a> {
    /// Rename each per the map, in every premise.
    To(&'a Rename),
    /// Make each of these a blank, in the premises that read facts.
    Blank(&'a BTreeSet<String>),
}

/// Every named variable the premises mention.
pub fn variables(premises: &[Premise]) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for premise in premises {
        // The variables inside a map or a list of terms are
        // the premise's own; the variable standing for the whole is not
        // one anybody wrote.
        let parameters = premise.parameters();
        if parameters.has_shapes() {
            for term in parameters.leaves() {
                if let Some(name) = term.name() {
                    names.insert(name.to_string());
                }
            }
        } else {
            for (_, term) in parameters.iter() {
                if let Some(name) = term.name() {
                    names.insert(name.to_string());
                }
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
    change_premises(premises, &Renaming::To(map))
}

/// Make each variable in `names` a blank where a premise reads facts
/// (a concept, an attribute, an optional attribute), inside `unless`
/// too. A formula, a resolver and a constraint keep their names: a
/// blank there would not mean what the name meant.
pub(crate) fn blank_premises(
    premises: &[Premise],
    names: &BTreeSet<String>,
) -> Result<Vec<Premise>, TypeError> {
    if names.is_empty() {
        return Ok(premises.to_vec());
    }
    change_premises(premises, &Renaming::Blank(names))
}

fn change_premises(premises: &[Premise], map: &Renaming<'_>) -> Result<Vec<Premise>, TypeError> {
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

fn rename_proposition(
    proposition: &Proposition,
    map: &Renaming<'_>,
) -> Result<Proposition, TypeError> {
    Ok(match proposition {
        Proposition::Concept(query) => Proposition::Concept(ConceptQuery {
            terms: rename_parameters(&query.terms, map),
            predicate: query.predicate.clone(),
        }),
        Proposition::Attribute(query) => Proposition::Attribute(Box::new(
            AttributeQuery::new(
                change_term(query.the(), map),
                change_term(query.of(), map),
                change_term(query.is(), map),
                change_term(query.cause(), map),
                Some(query.cardinality()),
            )
            .with_layer(change_term(query.layer(), map)),
        )),
        Proposition::OptionalAttribute(query) => {
            let inner = query.query();
            Proposition::OptionalAttribute(Box::new(
                OptionalAttributeQuery::new(
                    change_term(inner.the(), map),
                    change_term(inner.of(), map),
                    change_term(inner.is(), map),
                    change_term(inner.cause(), map),
                    Some(inner.cardinality()),
                )
                .with_layer(change_term(inner.layer(), map)),
            ))
        }
        Proposition::Formula(_) | Proposition::Resolver(_) | Proposition::Constraint(_)
            if matches!(map, Renaming::Blank(_)) =>
        {
            proposition.clone()
        }
        // Formulas, resolvers and constraints rebuild from their own
        // parameter maps, so a constant never leaves its typed form.
        Proposition::Formula(formula) => Proposition::Formula(
            formula.with_parameters(rename_parameters(&formula.parameters(), map))?,
        ),
        Proposition::Resolver(resolver) => Proposition::Resolver(Box::new(
            resolver.with_parameters(&rename_parameters(&resolver.parameters(), map)),
        )),
        Proposition::Constraint(constraint) => Proposition::Constraint(
            constraint.with_parameters(&rename_parameters(&constraint.parameters(), map)),
        ),
    })
}

fn rename_parameters(parameters: &Parameters, map: &Renaming<'_>) -> Parameters {
    parameters.map_terms(|term| change_term(term, map))
}

pub(crate) fn rename_term<T>(term: &Term<T>, map: &Rename) -> Term<T>
where
    T: Typed,
    <T as Typed>::Descriptor: Clone,
    Term<T>: Clone,
{
    change_term(term, &Renaming::To(map))
}

fn change_term<T>(term: &Term<T>, map: &Renaming<'_>) -> Term<T>
where
    T: Typed,
    <T as Typed>::Descriptor: Clone,
    Term<T>: Clone,
{
    let Term::Variable {
        name: Some(name),
        descriptor,
    } = term
    else {
        return term.clone();
    };
    match map {
        Renaming::To(map) => match map.get(name.as_ref()) {
            Some(renamed) => Term::Variable {
                name: Some(renamed.as_str().into()),
                descriptor: descriptor.clone(),
            },
            None => term.clone(),
        },
        Renaming::Blank(names) if names.contains(name.as_ref()) => Term::Variable {
            name: None,
            descriptor: descriptor.clone(),
        },
        Renaming::Blank(_) => term.clone(),
    }
}
