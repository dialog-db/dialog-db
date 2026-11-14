//! Patterns and templates: taking a tree apart and building one.
//!
//! A pattern is a tree with variables in it. Matching binds each
//! variable to the part of a tree that stands where it stands. A
//! template is filled from those bindings.

use std::collections::BTreeMap;

use crate::Error;
use crate::syntax::{Parameters, Syntax};

/// What a pattern's variable took.
#[derive(Clone, Debug, PartialEq)]
pub enum Binding {
    /// One tree.
    One(Syntax),
    /// The trees a repeated part took, in order.
    Many(Vec<Syntax>),
    /// The labelled parts a `... ?rest` took. The parts with no label
    /// it took are [`Binding::Many`] under its name, and these are kept
    /// beside them, under a key of their own.
    Row(BTreeMap<String, Syntax>),
}

/// The symbol that stands before what takes the parts not named. The
/// reader gives it no meaning: it is read here, where a pattern is
/// matched and a template filled.
pub const REST: &str = "...";

/// What the labelled parts a `... ?name` took are kept under: a key no
/// variable has, since the name holds the parts with no label.
fn row(name: &str) -> String {
    format!("{REST}{name}")
}

/// A run of parts as a pattern and a template read it.
struct Run<'a> {
    /// The parts before `...`.
    before: &'a [Syntax],
    /// What follows `...`, which each part not named has to match.
    /// `...` with nothing after it is `... _`.
    rest: Option<&'a Syntax>,
    /// The parts after that.
    after: &'a [Syntax],
}

fn split(parts: &[Syntax]) -> Run<'_> {
    const BLANK: &Syntax = &Syntax::Blank;
    match parts
        .iter()
        .position(|part| matches!(part, Syntax::Reference(name) if name == REST))
    {
        None => Run {
            before: parts,
            rest: None,
            after: &[],
        },
        Some(at) => Run {
            before: &parts[..at],
            rest: Some(parts.get(at + 1).unwrap_or(BLANK)),
            after: parts.get(at + 2..).unwrap_or_default(),
        },
    }
}

/// What each variable of a pattern took.
pub type Bindings = BTreeMap<String, Binding>;

fn bind(mut bindings: Bindings, name: &str, binding: Binding) -> Option<Bindings> {
    match bindings.get(name) {
        Some(bound) if *bound != binding => None,
        Some(_) => Some(bindings),
        None => {
            bindings.insert(name.to_string(), binding);
            Some(bindings)
        }
    }
}

/// What a run of patterns binds against a run of trees. `... p` takes
/// any number of trees, each matching `p`, and every variable in `p`
/// takes them all.
fn run(given: &[Syntax], wanted: &[Syntax], mut bindings: Bindings) -> Option<Bindings> {
    let Run {
        before,
        rest,
        after,
    } = split(wanted);
    let Some(repeated) = rest else {
        if given.len() != wanted.len() {
            return None;
        }
        return given
            .iter()
            .zip(wanted)
            .try_fold(bindings, |bindings, (item, pattern)| {
                matches(item, pattern, bindings)
            });
    };
    if given.len() < before.len() + after.len() {
        return None;
    }
    let tail = given.len() - after.len();
    let mut names = Vec::new();
    repeated.variables(&mut names);
    let mut columns: BTreeMap<&str, Vec<Syntax>> = names
        .iter()
        .map(|name| (name.as_str(), Vec::new()))
        .collect();
    for item in &given[before.len()..tail] {
        let one = matches(item, repeated, Bindings::new())?;
        for name in &names {
            let Some(Binding::One(taken)) = one.get(name) else {
                return None;
            };
            columns.get_mut(name.as_str())?.push(taken.clone());
        }
    }
    for (name, column) in columns {
        bindings = bind(bindings, name, Binding::Many(column))?;
    }
    given[..before.len()]
        .iter()
        .zip(before)
        .chain(given[tail..].iter().zip(after))
        .try_fold(bindings, |bindings, (item, pattern)| {
            matches(item, pattern, bindings)
        })
}

/// What `pattern` binds against `tree`, on top of `bindings`, or `None`
/// when the tree does not have the pattern's shape.
///
/// - A variable takes any one tree, and the same tree wherever it
///   recurs. `_` takes anything.
/// - Parts with no label match in order and in number; `... p` takes
///   any number of them, each matching `p`, and `...` alone is `... _`.
/// - A labelled part the tree leaves out leaves its variables unbound.
///   The labelled parts are one record with the part that has no
///   label, so `... ?rest` takes the labelled parts the pattern does
///   not name as well, and `... _` lets them be. Without either the
///   tree may have none.
pub fn matches(tree: &Syntax, pattern: &Syntax, bindings: Bindings) -> Option<Bindings> {
    match pattern {
        Syntax::Variable(name) => bind(bindings, name, Binding::One(tree.clone())),
        Syntax::Blank => Some(bindings),
        Syntax::Tuple(patterns) => match tree {
            Syntax::Tuple(items) => run(items, patterns, bindings),
            _ => None,
        },
        // A map is taken apart by key: the pattern's keys are what they
        // are, and each has to be a key of the tree.
        Syntax::Map(patterns) => {
            let Syntax::Map(entries) = tree else {
                return None;
            };
            if entries.len() != patterns.len() {
                return None;
            }
            patterns
                .iter()
                .try_fold(bindings, |bindings, (key, pattern)| {
                    let (_, value) = entries.iter().find(|(given, _)| given == key)?;
                    matches(value, pattern, bindings)
                })
        }
        Syntax::Application(wanted, named) => {
            let Syntax::Application(operator, operand) = tree else {
                return None;
            };
            let bindings = matches(operator, wanted, bindings)?;
            let mut bindings = run(&operand.items, &named.items, bindings)?;
            for (label, pattern) in &named.terms {
                if let Some(item) = operand.terms.get(label) {
                    bindings = matches(item, pattern, bindings)?;
                }
            }
            let extra: BTreeMap<String, Syntax> = operand
                .terms
                .iter()
                .filter(|(label, _)| !named.terms.contains_key(*label))
                .map(|(label, item)| (label.clone(), item.clone()))
                .collect();
            match split(&named.items).rest {
                Some(Syntax::Variable(name)) => bind(bindings, &row(name), Binding::Row(extra)),
                Some(Syntax::Blank) => Some(bindings),
                _ => extra.is_empty().then_some(bindings),
            }
        }
        literal => (tree == literal).then_some(bindings),
    }
}

/// A run of templates, filled. `... p` is `p` filled once for each of
/// what its variables took.
fn fill_run(templates: &[Syntax], bindings: &Bindings, fresh: &str) -> Result<Vec<Syntax>, Error> {
    let Run {
        before,
        rest,
        after,
    } = split(templates);
    let mut filled = Vec::new();
    for template in before {
        filled.push(fill(template, bindings, fresh)?);
    }
    if let Some(repeated) = rest {
        let mut names = Vec::new();
        repeated.variables(&mut names);
        let many: Vec<(&String, &Vec<Syntax>)> = names
            .iter()
            .filter_map(|name| match bindings.get(name) {
                Some(Binding::Many(trees)) => Some((name, trees)),
                _ => None,
            })
            .collect();
        let count = many.first().map_or(0, |(_, trees)| trees.len());
        for index in 0..count {
            let mut each = bindings.clone();
            for (name, trees) in &many {
                if let Some(tree) = trees.get(index) {
                    each.insert((*name).clone(), Binding::One(tree.clone()));
                }
            }
            filled.push(fill(repeated, &each, fresh)?);
        }
    }
    for template in after {
        filled.push(fill(template, bindings, fresh)?);
    }
    Ok(filled)
}

/// `template` with every variable the bindings hold replaced by what it
/// took. Any other variable is the template's own: it is renamed with
/// `fresh`, so no two expansions share it and it cannot capture a
/// variable of the tree being expanded.
///
/// A label the template writes and a `... ?rest` also brings is refused:
/// a tree says a label once.
pub fn fill(template: &Syntax, bindings: &Bindings, fresh: &str) -> Result<Syntax, Error> {
    Ok(match template {
        Syntax::Variable(name) => match bindings.get(name) {
            Some(Binding::One(tree)) => tree.clone(),
            Some(Binding::Many(trees)) => Syntax::Tuple(trees.clone()),
            Some(Binding::Row(_)) | None => Syntax::Variable(format!("{name}.{fresh}")),
        },
        Syntax::Tuple(items) => Syntax::Tuple(fill_run(items, bindings, fresh)?),
        Syntax::Map(entries) => Syntax::Map(
            entries
                .iter()
                .map(|(key, value)| {
                    Ok((fill(key, bindings, fresh)?, fill(value, bindings, fresh)?))
                })
                .collect::<Result<_, Error>>()?,
        ),
        Syntax::Application(operator, operand) => {
            let mut filled = Parameters {
                items: fill_run(&operand.items, bindings, fresh)?,
                ..Parameters::default()
            };
            for (label, item) in &operand.terms {
                filled
                    .terms
                    .insert(label.clone(), fill(item, bindings, fresh)?);
            }
            let taken = match split(&operand.items).rest {
                Some(Syntax::Variable(name)) => bindings.get(&row(name)),
                _ => None,
            };
            if let Some(Binding::Row(taken)) = taken {
                for (label, item) in taken {
                    if filled.terms.insert(label.clone(), item.clone()).is_some() {
                        return Err(Error::Refused(format!(
                            "{template} says `{label}:`, and so does what it is given"
                        )));
                    }
                }
            }
            Syntax::Application(Box::new(fill(operator, bindings, fresh)?), filled)
        }
        literal => literal.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;

    fn one(text: &str) -> Syntax {
        parse(text).unwrap().remove(0)
    }

    fn rewrite(tree: &str, pattern: &str, template: &str) -> Option<String> {
        let bindings = matches(&one(tree), &one(pattern), Bindings::new())?;
        Some(fill(&one(template), &bindings, "s").unwrap().to_string())
    }

    #[test]
    fn it_replaces_what_the_pattern_bound() {
        assert_eq!(
            rewrite(
                "(query (todo ?t))",
                "(query ?head)",
                "(rule (report ?head) ?head)"
            )
            .as_deref(),
            Some("(rule (report (todo ?t)) (todo ?t))")
        );
    }

    #[test]
    fn it_repeats_what_an_ellipsis_took() {
        assert_eq!(
            rewrite(
                "(query (all (a ?x) (b ?y)) (c ?x) (d ?y))",
                "(query (all ... ?head) ... ?premise)",
                "(rule (all ... (report ?head)) ... ?premise)"
            )
            .as_deref(),
            Some("(rule (all (report (a ?x)) (report (b ?y))) (c ?x) (d ?y))")
        );
        assert_eq!(
            rewrite(
                "(query (a ?x))",
                "(query (all ... ?head) ... ?premise)",
                "x"
            ),
            None
        );
    }

    #[test]
    fn it_ties_a_parameter_to_a_field_by_the_variable_not_the_name() {
        assert_eq!(
            rewrite(
                "(task ?mine title: \"Buy milk\")",
                "(task ?t title: ?x)",
                "(xyz.todo ?t text: (:last (:text ?x)))"
            )
            .as_deref(),
            Some("(xyz.todo ?mine text: (:last (:text \"Buy milk\")))")
        );
    }

    #[test]
    fn it_renames_a_variable_the_pattern_did_not_bind() {
        assert_eq!(
            rewrite("(todo ?t)", "(todo ?t text: ?x)", "(xyz.todo ?t text: ?x)").as_deref(),
            Some("(xyz.todo ?t text: ?x.s)")
        );
    }

    #[test]
    fn it_takes_the_labelled_parts_a_pattern_does_not_name() {
        assert_eq!(
            rewrite(
                "(add (todo list: list:home text: \"x\"))",
                "(?command (?concept ... ?fields))",
                "(?command.domain _ ... ?fields)"
            )
            .as_deref(),
            Some("(?command.domain.s _ list: list:home text: \"x\")")
        );
        assert_eq!(
            rewrite("(todo ?t colour: ?c)", "(todo ?t text: ?x)", "x"),
            None
        );
    }

    /// A definition runs both ways: the template taken off what a call
    /// expanded to gives what the pattern's variables took, and the
    /// pattern filled with them is the call. So what a call was need
    /// not be kept beside what it became, only which definition it was.
    #[test]
    fn it_gives_back_a_call_by_running_its_definition_backwards() {
        for (call, pattern, template) in [
            (
                "(query (all (a ?x) (b ?y)) (c ?x) (d ?y))",
                "(query (all ... ?head) ... ?premise)",
                "(rule (all ... (report ?head)) ... ?premise)",
            ),
            (
                "(task ?mine title: \"Buy milk\")",
                "(task ?t title: ?x)",
                "(xyz.todo ?t text: (:last (:text ?x)))",
            ),
            (
                "(add (todo list: list:home text: \"x\"))",
                "(?command (?concept ... ?fields))",
                "(command ?command about: ?concept ... ?fields)",
            ),
        ] {
            let (call, pattern, template) = (one(call), one(pattern), one(template));
            let became = fill(
                &template,
                &matches(&call, &pattern, Bindings::new()).unwrap(),
                "s",
            )
            .unwrap();
            let took = matches(&became, &template, Bindings::new()).unwrap();
            assert_eq!(fill(&pattern, &took, "s").unwrap(), call, "{became}");
        }
    }

    /// What does not come back: a labelled part the call left out. The
    /// template names it with a variable of its own, and run backwards
    /// that variable stands where nothing was written.
    #[test]
    fn it_gives_back_a_part_the_call_left_out_as_the_templates_own_variable() {
        let (call, pattern, template) = (
            one("(task ?mine)"),
            one("(task ?t title: ?x)"),
            one("(xyz.todo ?t text: (:last (:text ?x)))"),
        );
        let became = fill(
            &template,
            &matches(&call, &pattern, Bindings::new()).unwrap(),
            "s",
        )
        .unwrap();
        let took = matches(&became, &template, Bindings::new()).unwrap();
        assert_eq!(
            fill(&pattern, &took, "s").unwrap().to_string(),
            "(task ?mine title: ?x.s)"
        );
    }

    #[test]
    fn it_refuses_a_label_the_template_and_the_rest_both_say() {
        let bindings = matches(
            &one("(add (todo state: :done text: \"x\"))"),
            &one("(?command (?concept ... ?fields))"),
            Bindings::new(),
        )
        .unwrap();
        assert!(matches!(
            fill(
                &one("(xyz.todo _ state: :open ... ?fields)"),
                &bindings,
                "s"
            ),
            Err(Error::Refused(_))
        ));
    }
}
