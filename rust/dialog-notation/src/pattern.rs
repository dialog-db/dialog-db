//! Patterns and templates: taking a form apart and building one.
//!
//! A pattern is a form with variables in it. Matching binds each
//! variable to the part of a form that stands where it stands. A
//! template is filled from those bindings.

use std::collections::BTreeMap;

use crate::form::{Form, Part};

/// What a pattern's variable took.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Binding {
    /// One form.
    One(Form),
    /// The forms a repeated part took, in order.
    Many(Vec<Form>),
    /// The labelled parts a `...?rest` took.
    Row(Vec<(String, Form)>),
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

/// The names of the variables in a pattern, in the order they occur.
pub fn variables(pattern: &Form, into: &mut Vec<String>) {
    match pattern {
        Form::Variable(name) => into.push(name.clone()),
        Form::Vector(items) => items.iter().for_each(|item| variables(item, into)),
        Form::Application(head, parts) => {
            variables(head, into);
            for part in parts {
                match part {
                    Part::Bare(form) | Part::Labelled(_, form) => variables(form, into),
                }
            }
        }
        _ => {}
    }
}

/// What `pattern` binds against `form`, on top of `bindings`, or `None`
/// when the form does not have the pattern's shape.
///
/// - A variable takes any one form, and the same form wherever it
///   recurs. `_` takes anything.
/// - Bare parts match in order and in number; `p ...` takes any number
///   of them, each matching `p`.
/// - A labelled part the form leaves out leaves its variables unbound.
///   `...?rest` takes the labelled parts the pattern does not name;
///   without it the form may have none.
pub fn matches(form: &Form, pattern: &Form, bindings: Bindings) -> Option<Bindings> {
    match pattern {
        Form::Variable(name) => bind(bindings, name, Binding::One(form.clone())),
        Form::Blank => Some(bindings),
        Form::Vector(patterns) => {
            let Form::Vector(items) = form else {
                return None;
            };
            if items.len() != patterns.len() {
                return None;
            }
            items
                .iter()
                .zip(patterns)
                .try_fold(bindings, |bindings, (item, pattern)| {
                    matches(item, pattern, bindings)
                })
        }
        Form::Application(pattern_head, _) => {
            let (head, _) = form.as_application()?;
            let mut bindings = matches(head, pattern_head, bindings)?;

            let given = form.bare();
            let wanted = pattern.bare();
            let pairs: Vec<(&Form, &Form)> = match wanted.iter().position(|p| **p == Form::Ellipsis)
            {
                Some(0) => return None,
                Some(at) => {
                    let (before, repeated, after) =
                        (&wanted[..at - 1], wanted[at - 1], &wanted[at + 1..]);
                    if given.len() < before.len() + after.len() {
                        return None;
                    }
                    let tail = given.len() - after.len();
                    let mut names = Vec::new();
                    variables(repeated, &mut names);
                    let mut columns: BTreeMap<&str, Vec<Form>> = names
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
                        .copied()
                        .zip(before.iter().copied())
                        .chain(given[tail..].iter().copied().zip(after.iter().copied()))
                        .collect()
                }
                None => {
                    if given.len() != wanted.len() {
                        return None;
                    }
                    given.into_iter().zip(wanted).collect()
                }
            };
            for (item, pattern) in pairs {
                bindings = matches(item, pattern, bindings)?;
            }

            let labelled = form.labelled();
            let named = pattern.labelled();
            for (label, pattern) in &named {
                if let Some((_, item)) = labelled.iter().find(|(name, _)| name == label) {
                    bindings = matches(item, pattern, bindings)?;
                }
            }
            let extra: Vec<(String, Form)> = labelled
                .iter()
                .filter(|(label, _)| !named.iter().any(|(name, _)| name == label))
                .map(|(label, item)| (label.to_string(), (*item).clone()))
                .collect();
            let rest = pattern
                .as_application()?
                .1
                .iter()
                .find_map(|part| match part {
                    Part::Bare(Form::Rest(name)) => Some(name),
                    _ => None,
                });
            match rest {
                Some(name) => bind(bindings, name, Binding::Row(extra)),
                None => extra.is_empty().then_some(bindings),
            }
        }
        literal => (form == literal).then_some(bindings),
    }
}

/// `template` with every variable the bindings hold replaced by what it
/// took. Any other variable is the template's own: it is renamed with
/// `fresh`, so no two expansions share it and it cannot capture a
/// variable of the form being expanded.
pub fn fill(template: &Form, bindings: &Bindings, fresh: &str) -> Form {
    match template {
        Form::Variable(name) => match bindings.get(name) {
            Some(Binding::One(form)) => form.clone(),
            Some(Binding::Many(forms)) => Form::Vector(forms.clone()),
            Some(Binding::Row(_)) | None => Form::Variable(format!("{name}.{fresh}")),
        },
        Form::Vector(items) => Form::Vector(
            items
                .iter()
                .map(|item| fill(item, bindings, fresh))
                .collect(),
        ),
        Form::Application(head, parts) => {
            let mut filled = Vec::new();
            let mut at = 0;
            while at < parts.len() {
                match &parts[at] {
                    Part::Labelled(label, item) => {
                        filled.push(Part::Labelled(label.clone(), fill(item, bindings, fresh)));
                    }
                    Part::Bare(Form::Ellipsis) => {}
                    Part::Bare(Form::Rest(name)) => {
                        if let Some(Binding::Row(row)) = bindings.get(name) {
                            filled.extend(
                                row.iter().map(|(label, item)| {
                                    Part::Labelled(label.clone(), item.clone())
                                }),
                            );
                        }
                    }
                    Part::Bare(item) => {
                        let repeats = matches!(parts.get(at + 1), Some(Part::Bare(Form::Ellipsis)));
                        if repeats {
                            let mut names = Vec::new();
                            variables(item, &mut names);
                            let many: Vec<(&String, &Vec<Form>)> = names
                                .iter()
                                .filter_map(|name| match bindings.get(name) {
                                    Some(Binding::Many(forms)) => Some((name, forms)),
                                    _ => None,
                                })
                                .collect();
                            let count = many.first().map_or(0, |(_, forms)| forms.len());
                            for index in 0..count {
                                let mut each = bindings.clone();
                                for (name, forms) in &many {
                                    if let Some(form) = forms.get(index) {
                                        each.insert((*name).clone(), Binding::One(form.clone()));
                                    }
                                }
                                filled.push(Part::Bare(fill(item, &each, fresh)));
                            }
                        } else {
                            filled.push(Part::Bare(fill(item, bindings, fresh)));
                        }
                    }
                }
                at += 1;
            }
            Form::application(fill(head, bindings, fresh), filled)
        }
        literal => literal.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::form::read;

    fn one(text: &str) -> Form {
        read(text).unwrap().remove(0)
    }

    fn rewrite(form: &str, pattern: &str, template: &str) -> Option<String> {
        let bindings = matches(&one(form), &one(pattern), Bindings::new())?;
        Some(fill(&one(template), &bindings, "s").to_string())
    }

    #[test]
    fn it_replaces_what_the_pattern_bound() {
        assert_eq!(
            rewrite(
                "(query (todo ?t))",
                "(query ?head)",
                "(rule (report ?head) (when ?head))"
            )
            .as_deref(),
            Some("(rule (report (todo ?t)) (when (todo ?t)))")
        );
    }

    #[test]
    fn it_repeats_what_an_ellipsis_took() {
        assert_eq!(
            rewrite(
                "(query (a ?x) (b ?y) (when (c ?x) (d ?y)))",
                "(query ?head ... (when ?premise ...))",
                "(rule (report ?head) ... (when ?premise ...))"
            )
            .as_deref(),
            Some("(rule (report (a ?x)) (report (b ?y)) (when (c ?x) (d ?y)))")
        );
        assert_eq!(
            rewrite(
                "(query (a ?x))",
                "(query ?head ... (when ?premise ...))",
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
                "(?command (?concept ...?fields))",
                "(?command.domain _ ...?fields)"
            )
            .as_deref(),
            Some("(?command.domain.s _ list: list:home text: \"x\")")
        );
        assert_eq!(
            rewrite("(todo ?t colour: ?c)", "(todo ?t text: ?x)", "x"),
            None
        );
    }
}
