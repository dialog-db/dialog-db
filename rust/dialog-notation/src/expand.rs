//! Expansion: rewriting a form until every part of it is kernel.
//!
//! The kernel is small: `rule` with its heads, `when` and `unless`; the
//! application of a relation and of a formula; `assert!`, `retract!`,
//! `assert` and `report` as a rule's head. Every other form is given
//! its meaning by a rule about syntax:
//!
//! ```clj
//! (rule (syntax (rule (report ?head) (when ?head)))
//!   (when (expand (query ?head))))
//! ```
//!
//! `(expand pattern)` holds for the form being expanded when it has the
//! pattern's shape, and `(syntax template)` is what the form becomes.
//! Neither is walked into: what is inside them is syntax.
//!
//! The [`Expander`] is the walker. It rewrites a top-level form until
//! it is a `rule`, then every head and premise until each applies a
//! relation or a formula, asking the rules about syntax at each step.
//! In this proof of concept those rules are kept and evaluated here,
//! not stored in a replica and not run by the query engine, which
//! cannot yet take a form apart.

use std::collections::BTreeSet;

use crate::Error;
use crate::form::{Form, Kind, Part, read};
use crate::pattern::{Binding, Bindings, fill, matches, variables};

/// The core forms, written in the kernel.
pub const PRELUDE: &str = include_str!("prelude.clj");

/// Where a form stands when it is expanded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Position {
    /// A rule's head, or the top level of a document.
    Head,
    /// A premise, in a `when` or an `unless`.
    Premise,
}

/// Something a rule said about a form without refusing it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diagnostic {
    /// The form it is about, as written.
    pub form: Form,
    /// What was said.
    pub message: String,
}

/// What a rule about syntax concludes.
#[derive(Clone, Debug)]
enum Answer {
    /// `(syntax template)`: the form becomes the template, filled.
    Syntax(Form),
    /// `(warning "…")`: the form stands, and this is said about it.
    Warning(String),
}

/// A rule about syntax: `(expand pattern)` among its premises, and
/// `(syntax template)` or `(warning "…")` as its head.
#[derive(Clone, Debug)]
struct Syntax {
    answer: Answer,
    pattern: Form,
    position: Option<Position>,
    when: Vec<Form>,
    unless: Vec<Form>,
}

/// The words that make a rule's head a write or an answer.
const HEAD_WORDS: [&str; 4] = ["assert!", "retract!", "assert", "report"];

/// The words whose parts are syntax, never walked into.
const DIRECTIVES: [&str; 3] = ["expand", "syntax", "warning"];

/// The parts of a kernel rule.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Clauses {
    /// What the rule says about itself: its labelled parts.
    pub said: Vec<(String, Form)>,
    /// Its heads.
    pub heads: Vec<Form>,
    /// Its `when` premises.
    pub when: Vec<Form>,
    /// Its `unless` premises.
    pub unless: Vec<Form>,
}

impl Clauses {
    /// The parts of a `(rule …)` form.
    pub fn of(rule: &Form) -> Self {
        let mut clauses = Clauses::default();
        let Some((_, parts)) = rule.as_application() else {
            return clauses;
        };
        for part in parts {
            match part {
                Part::Labelled(label, form) => clauses.said.push((label.clone(), form.clone())),
                // What must not hold may stand among what must, as in
                // `A, not B, not C`: each is one more "and not".
                Part::Bare(form) if form.is("when") => {
                    for premise in form.bare() {
                        if premise.is("unless") {
                            clauses.unless.extend(premise.bare().into_iter().cloned());
                        } else {
                            clauses.when.push(premise.clone());
                        }
                    }
                }
                Part::Bare(form) if form.is("unless") => {
                    clauses.unless.extend(form.bare().into_iter().cloned());
                }
                Part::Bare(form) => clauses.heads.push(form.clone()),
            }
        }
        clauses
    }

    /// These parts as a `(rule …)` form.
    pub fn into_rule(self) -> Form {
        let mut parts: Vec<Part> = self
            .said
            .into_iter()
            .map(|(label, form)| Part::Labelled(label, form))
            .collect();
        parts.extend(self.heads.into_iter().map(Part::Bare));
        for (word, premises) in [("when", self.when), ("unless", self.unless)] {
            if !premises.is_empty() {
                parts.push(Part::Bare(Form::application(
                    Form::symbol(word),
                    premises.into_iter().map(Part::Bare).collect(),
                )));
            }
        }
        Form::application(Form::symbol("rule"), parts)
    }
}

/// The forms a form stands for: itself, or what `(all …)` holds.
fn members(form: Form) -> Vec<Form> {
    if form.is("all") {
        form.bare().into_iter().cloned().flat_map(members).collect()
    } else {
        vec![form]
    }
}

/// Several reads of one entity as one read, so that their absence is
/// the absence of all of them together. Dialog negates one proposition,
/// and a read of one entity may span domains.
fn together(mut forms: Vec<Form>, written: &Form) -> Result<Form, Error> {
    if forms.len() == 1 {
        return Ok(forms.remove(0));
    }
    let entities: BTreeSet<Option<&Form>> = forms
        .iter()
        .map(|form| form.bare().first().copied())
        .collect();
    if !forms.iter().all(Form::is_read) || entities.len() != 1 {
        return Err(Error::Refused(format!(
            "the absence of {written} is not one read: dialog negates one proposition, and this spans entities"
        )));
    }
    let domain = forms[0].head().unwrap_or_default().to_string();
    let mut parts: Vec<Part> = forms[0]
        .bare()
        .into_iter()
        .take(1)
        .cloned()
        .map(Part::Bare)
        .collect();
    for form in &forms {
        let other = form.head().unwrap_or_default();
        for (label, term) in form.labelled() {
            let whole = if label.contains('/') || other == domain {
                label.to_string()
            } else {
                format!("{other}/{label}")
            };
            parts.push(Part::Labelled(whole, term.clone()));
        }
    }
    Ok(Form::application(Form::symbol(domain), parts))
}

/// The walker, with the rules about syntax it has been given.
#[derive(Clone, Debug, Default)]
pub struct Expander {
    syntax: Vec<Syntax>,
    /// The names some rule about syntax is for.
    names: BTreeSet<String>,
    /// The domains some installed rule reads.
    reads: BTreeSet<String>,
    /// Every form the rules were asked about since the last report.
    asked: Vec<(Form, Position)>,
}

impl Expander {
    /// A walker that knows the core forms.
    pub fn new() -> Result<Self, Error> {
        let mut expander = Expander::default();
        expander.submit(PRELUDE)?;
        Ok(expander)
    }

    /// Read `text` and expand every form in it. A rule about syntax is
    /// kept here; every other form comes back as a kernel rule, in the
    /// order written, with what the rules said about the document.
    pub fn submit(&mut self, text: &str) -> Result<(Vec<Form>, Vec<Diagnostic>), Error> {
        self.asked.clear();
        let mut kernel = Vec::new();
        for form in read(text)? {
            let rule = self.top(form)?;
            if let Some(rule) = self.install(rule)? {
                kernel.push(rule);
            }
        }
        Ok((kernel, self.diagnose()))
    }

    /// Keep a rule about syntax, and note what any other rule reads.
    fn install(&mut self, rule: Form) -> Result<Option<Form>, Error> {
        let clauses = Clauses::of(&rule);
        let answer = match clauses.heads.as_slice() {
            [head] if head.is("syntax") => match head.bare().as_slice() {
                [template] => Some(Answer::Syntax((*template).clone())),
                _ => return Err(Error::Refused(format!("{head} takes one template"))),
            },
            [head] if head.is("warning") => match head.bare().as_slice() {
                [Form::Text(message)] => Some(Answer::Warning(message.clone())),
                _ => return Err(Error::Refused(format!("{head} takes one text"))),
            },
            _ => None,
        };
        let Some(answer) = answer else {
            for premise in &clauses.when {
                if let (true, Some(domain)) = (premise.is_read(), premise.head()) {
                    self.reads.insert(domain.to_string());
                }
            }
            return Ok(Some(rule));
        };

        let mut when = clauses.when;
        let at = when
            .iter()
            .position(|premise| premise.is("expand"))
            .ok_or_else(|| {
                Error::Refused(format!(
                    "a rule about syntax needs an (expand …) premise: {rule}"
                ))
            })?;
        let expand = when.remove(at);
        let pattern = match expand.bare().as_slice() {
            [pattern] => (*pattern).clone(),
            _ => return Err(Error::Refused(format!("{expand} takes one pattern"))),
        };
        let position = match expand.at("in") {
            None => None,
            Some(Form::Keyword(word)) if word == "head" => Some(Position::Head),
            Some(Form::Keyword(word)) if word == "premise" => Some(Position::Premise),
            Some(other) => return Err(Error::Refused(format!("{other} is not a position"))),
        };

        if let Answer::Syntax(template) = &answer {
            Self::check_concept(&pattern, template)?;
        }
        if let Some(name) = pattern.head() {
            self.names.insert(name.to_string());
        }
        self.syntax.push(Syntax {
            answer,
            pattern,
            position,
            when,
            unless: clauses.unless,
        });
        Ok(None)
    }

    /// A concept maps facts to itself and back. So what it stands for is
    /// reads and nothing else, since a check has no inverse, and every
    /// variable in them is one of its parameters, since a write must
    /// know every value. The reads may be of several entities.
    fn check_concept(pattern: &Form, template: &Form) -> Result<(), Error> {
        let said = members(template.clone());
        if !said.iter().any(Form::is_read)
            || !said.iter().all(|form| form.is_read() || form.is_formula())
        {
            return Ok(());
        }
        if let Some(check) = said.iter().find(|form| form.is_formula()) {
            return Err(Error::Refused(format!(
                "a concept maps both ways, and a check has no inverse: {check}"
            )));
        }
        let mut given = Vec::new();
        variables(pattern, &mut given);
        let mut used = Vec::new();
        said.iter().for_each(|form| variables(form, &mut used));
        match used.iter().find(|name| !given.contains(name)) {
            Some(own) => Err(Error::Refused(format!(
                "a concept maps both ways, so every variable in it is a parameter: ?{own}"
            ))),
            None => Ok(()),
        }
    }

    /// The bindings under which a rule about syntax applies to `form`.
    fn applies(&self, rule: &Syntax, form: &Form, position: Position) -> Option<Bindings> {
        if rule.position.is_some_and(|wanted| wanted != position) {
            return None;
        }
        let mut bindings = matches(form, &rule.pattern, Bindings::new())?;
        for premise in &rule.when {
            bindings = self.holds(premise, bindings)?;
        }
        if rule
            .unless
            .iter()
            .any(|premise| self.holds(premise, bindings.clone()).is_some())
        {
            return None;
        }
        Some(bindings)
    }

    /// Whether a premise of a rule about syntax holds, and what it binds.
    /// Such a premise applies one of a few formulas over symbols, or
    /// reads what this walker knows: the names syntax is defined for,
    /// `(dialog.name _ is: ?name)`, and the domains installed rules
    /// read, `(dialog.rule _ on: ?domain)`.
    fn holds(&self, premise: &Form, bindings: Bindings) -> Option<Bindings> {
        let value = |term: &Form| match term {
            Form::Variable(name) => match bindings.get(name) {
                Some(Binding::One(form)) => Some(form.clone()),
                _ => None,
            },
            other => Some(other.clone()),
        };
        let symbol = |term: &Form| match value(term)? {
            Form::Symbol(name) => Some(name),
            _ => None,
        };
        let (head, _) = premise.as_application()?;
        let subject = premise.bare().first().copied();
        match head {
            Form::Keyword(formula) => match formula.as_str() {
                "command" => {
                    let domain =
                        format!("command.{}.{}", symbol(subject?)?, symbol(premise.at("")?)?);
                    match premise.at("is")? {
                        Form::Variable(name) => {
                            let mut bindings = bindings.clone();
                            match bindings.get(name) {
                                Some(Binding::One(Form::Symbol(bound))) if *bound == domain => {}
                                Some(_) => return None,
                                None => {
                                    bindings
                                        .insert(name.clone(), Binding::One(Form::symbol(domain)));
                                }
                            }
                            Some(bindings)
                        }
                        _ => None,
                    }
                }
                "domain" => {
                    (value(subject?)?.kind() == Some(Kind::Domain)).then_some(bindings.clone())
                }
                "name" => (value(subject?)?.kind() == Some(Kind::Name)).then_some(bindings.clone()),
                _ => None,
            },
            Form::Symbol(relation) if relation == "dialog.name" => self
                .names
                .contains(&symbol(premise.at("is")?)?)
                .then_some(bindings.clone()),
            Form::Symbol(relation) if relation == "dialog.rule" => self
                .reads
                .contains(&symbol(premise.at("on")?)?)
                .then_some(bindings.clone()),
            _ => None,
        }
    }

    /// What the rules say `form` expands to in `position`. No answer and
    /// two different answers are both refused.
    fn ask(&mut self, form: &Form, position: Position) -> Result<Form, Error> {
        // What the template's own variables are named apart by: the form
        // being expanded and where it stands, by reference, so the names
        // do not move with how either is spelled.
        let site = Form::application(
            Form::symbol("site"),
            vec![
                Part::Bare(form.clone()),
                Part::Bare(Form::symbol(match position {
                    Position::Head => "head",
                    Position::Premise => "premise",
                })),
            ],
        )
        .reference()?
        .to_string()[..4]
            .to_string();
        let mut found: Vec<Form> = Vec::new();
        for rule in &self.syntax {
            if let (Answer::Syntax(template), Some(bindings)) =
                (&rule.answer, self.applies(rule, form, position))
            {
                let answer = fill(template, &bindings, &site);
                if !found.contains(&answer) {
                    found.push(answer);
                }
            }
        }
        self.asked.push((form.clone(), position));
        match found.len() {
            0 => Err(Error::Unknown(form.to_string())),
            1 => Ok(found.remove(0)),
            _ => Err(Error::Ambiguous(
                form.to_string(),
                found
                    .iter()
                    .map(Form::to_string)
                    .collect::<Vec<_>>()
                    .join(" | "),
            )),
        }
    }

    /// What the rules say about the forms they were asked about, now that
    /// the whole document is in: a rule further down may be what reads a
    /// command issued further up.
    fn diagnose(&mut self) -> Vec<Diagnostic> {
        let mut diagnostics: Vec<Diagnostic> = Vec::new();
        for (form, position) in std::mem::take(&mut self.asked) {
            for rule in &self.syntax {
                if let (Answer::Warning(message), Some(_)) =
                    (&rule.answer, self.applies(rule, &form, position))
                {
                    let diagnostic = Diagnostic {
                        form: form.clone(),
                        message: message.clone(),
                    };
                    if !diagnostics.contains(&diagnostic) {
                        diagnostics.push(diagnostic);
                    }
                }
            }
        }
        diagnostics
    }

    /// A head or a premise, rewritten until it applies a relation or a
    /// formula. `(all …)` comes back when it stands for several.
    fn application(&mut self, mut form: Form, position: Position) -> Result<Form, Error> {
        loop {
            if form.is_read() || form.is_formula() || DIRECTIVES.iter().any(|word| form.is(word)) {
                return Ok(form);
            }
            if form.is("all") {
                let each = form
                    .bare()
                    .into_iter()
                    .cloned()
                    .map(|member| self.application(member, position).map(Part::Bare))
                    .collect::<Result<Vec<_>, _>>()?;
                return Ok(Form::application(Form::symbol("all"), each));
            }
            if let (Position::Head, Some(word)) = (
                position,
                form.head().filter(|word| HEAD_WORDS.contains(word)),
            ) {
                let word = word.to_string();
                let inner = match form.bare().as_slice() {
                    [inner] => (*inner).clone(),
                    _ => return Err(Error::Refused(format!("{form} takes one application"))),
                };
                let mut each: Vec<Part> = members(self.application(inner, Position::Head)?)
                    .into_iter()
                    .map(|member| {
                        Part::Bare(Form::application(
                            Form::symbol(&word),
                            vec![Part::Bare(member)],
                        ))
                    })
                    .collect();
                return Ok(if each.len() == 1 {
                    match each.remove(0) {
                        Part::Bare(only) => only,
                        Part::Labelled(_, only) => only,
                    }
                } else {
                    Form::application(Form::symbol("all"), each)
                });
            }
            form = self.ask(&form, position)?;
        }
    }

    /// A top-level form, rewritten until it is a kernel rule. A write at
    /// the top level is a rule with that head and whatever clauses
    /// follow it.
    fn top(&mut self, mut form: Form) -> Result<Form, Error> {
        while !form.is("rule") {
            form = match form.head().filter(|word| HEAD_WORDS.contains(word)) {
                Some(word) => {
                    let (_, parts) = form
                        .as_application()
                        .ok_or_else(|| Error::Unknown(form.to_string()))?;
                    let Some((Part::Bare(first), rest)) = parts.split_first() else {
                        return Err(Error::Refused(format!("{form} takes an application")));
                    };
                    let head =
                        Form::application(Form::symbol(word), vec![Part::Bare(first.clone())]);
                    let mut parts = vec![Part::Bare(head)];
                    parts.extend(rest.iter().cloned());
                    Form::application(Form::symbol("rule"), parts)
                }
                None => self.ask(&form, Position::Head)?,
            };
        }
        let written = Clauses::of(&form);
        let mut walked = Clauses {
            said: written.said,
            ..Clauses::default()
        };
        for head in written.heads {
            walked
                .heads
                .extend(members(self.application(head, Position::Head)?));
        }
        for premise in written.when {
            walked
                .when
                .extend(members(self.application(premise, Position::Premise)?));
        }
        for premise in written.unless {
            let stands = members(self.application(premise.clone(), Position::Premise)?);
            walked.unless.push(together(stands, &premise)?);
        }
        Ok(walked.into_rule())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TODO: &str = r#"
        (concept (todo ?t list: ?l text: ?x state: ?s)
          (xyz.todo ?t list: (:last (:entity ?l)) text: (:last (:text ?x)) state: (:last ?s)))
        (rule (assert! (todo ?t list: ?l text: ?x state: :open))
          (when (add (todo ?t list: ?l text: ?x))))
    "#;

    fn expanded(setup: &str, text: &str) -> Result<Vec<String>, Error> {
        let mut expander = Expander::new()?;
        expander.submit(setup)?;
        let (kernel, _) = expander.submit(text)?;
        Ok(kernel.iter().map(Form::to_string).collect())
    }

    /// `unless` beside `when` and `unless` inside it are one rule.
    #[test]
    fn it_reads_unless_beside_when_or_inside_it() {
        let beside = expanded(
            TODO,
            "(rule (xyz.lonely ?t text: ?x)
               (when (todo ?t text: ?x))
               (unless (xyz.tag ?t name: _) (xyz.star ?t by: _)))",
        )
        .unwrap();
        let inside = expanded(
            TODO,
            "(rule (xyz.lonely ?t text: ?x)
               (when (todo ?t text: ?x)
                     (unless (xyz.tag ?t name: _))
                     (unless (xyz.star ?t by: _))))",
        )
        .unwrap();
        assert_eq!(beside, inside);
        assert!(beside[0].contains("(unless (xyz.tag ?t name: _) (xyz.star ?t by: _))"));
    }

    /// A pattern is any form, not an application only: here a form in
    /// brackets at the top level.
    #[test]
    fn it_expands_a_form_that_is_not_an_application() {
        let kernel = expanded(
            r#"(rule (syntax (assert! (xyz.note ?n text: (:text ?x))))
                 (when (expand [note ?n ?x])))"#,
            r#"[note note:1 "hi"]"#,
        )
        .unwrap();
        assert_eq!(
            kernel,
            vec![r#"(rule (assert! (xyz.note note:1 text: (:text "hi"))))"#]
        );
    }

    #[test]
    fn it_expands_a_concept_where_it_is_read_and_where_it_is_written() {
        let mut expander = Expander::new().unwrap();
        let (kernel, diagnostics) = expander.submit(TODO).unwrap();
        assert_eq!(
            kernel.iter().map(Form::to_string).collect::<Vec<_>>(),
            vec![
                "(rule (assert! (xyz.todo ?t list: (:last (:entity ?l)) text: (:last (:text ?x)) state: (:last :open))) \
                 (when (command.add.todo _ about: ?t list: ?l text: ?x)))"
            ]
        );
        assert!(diagnostics.is_empty());
    }

    /// A field the query leaves out is still read, under a variable of
    /// the expansion's own: one where the concept stands as the head,
    /// another where it stands as the premise.
    #[test]
    fn it_expands_a_query_to_a_rule_that_reports() {
        let kernel = expanded(TODO, "(query (todo ?t text: ?x))").unwrap();
        let rule = Clauses::of(&read(&kernel[0]).unwrap()[0]);
        let own = |form: &Form| match form.at("list") {
            Some(list) => list.to_string(),
            None => panic!("{form} reads the list"),
        };
        assert!(rule.heads[0].is("report"));
        let head = rule.heads[0].bare()[0];
        assert_eq!(head.at("text"), rule.when[0].at("text"));
        assert!(own(head).starts_with("(:last (:entity ?l."));
        assert!(own(&rule.when[0]).starts_with("(:last (:entity ?l."));
        assert_ne!(own(head), own(&rule.when[0]));
    }

    #[test]
    fn it_issues_a_command_and_warns_when_nothing_reads_it() {
        let mut expander = Expander::new().unwrap();
        expander.submit(TODO).unwrap();
        let (kernel, diagnostics) = expander
            .submit(r#"(add (todo todo:1 list: list:home text: "Buy milk"))"#)
            .unwrap();
        assert_eq!(kernel.len(), 1);
        assert!(
            kernel[0]
                .to_string()
                .starts_with("(rule (assert (command.add.todo (:id command.add.todo about: todo:1")
        );
        assert!(diagnostics.is_empty());

        let (_, diagnostics) = expander
            .submit("(ad (todo todo:1 list: list:home text: \"x\"))")
            .unwrap();
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].message, "no rule reads this command");
    }

    #[test]
    fn it_does_not_warn_about_a_command_a_later_rule_reads() {
        let mut expander = Expander::new().unwrap();
        let (_, diagnostics) = expander
            .submit(
                "(increment (xyz.counter counter:a))
                 (rule (assert! (xyz.counter ?c count: (+ ?n : 1)))
                   (when (increment (xyz.counter ?c)) (xyz.counter ?c count: ?n)))",
            )
            .unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn it_refuses_what_nothing_gives_a_meaning_and_what_two_rules_do() {
        assert!(matches!(
            expanded(TODO, "(query (todo ?t) (when (frobnicate ?t)))"),
            Err(Error::Unknown(_))
        ));
        assert!(matches!(
            expanded(
                TODO,
                "(concept (todo ?t title: ?x) (xyz.note ?t title: ?x)) (query (todo ?t))"
            ),
            Err(Error::Ambiguous(..))
        ));
    }

    #[test]
    fn it_refuses_a_concept_that_cannot_map_both_ways() {
        assert!(matches!(
            expanded(
                "",
                "(concept (cheap ?i price: ?n) (shop.item ?i price: ?n) (< ?n : 100))"
            ),
            Err(Error::Refused(_))
        ));
        assert!(matches!(
            expanded("", "(concept (titled ?t) (xyz.todo ?t text: ?x))"),
            Err(Error::Refused(_))
        ));
    }

    #[test]
    fn it_reads_a_concept_from_the_other_side() {
        assert_eq!(
            expanded(
                "(concept (list-item ?l todo: ?t) (xyz.todo ?t list: ?l))",
                "(query (list-item list:home todo: ?t))"
            )
            .unwrap(),
            vec![
                "(rule (report (xyz.todo ?t list: list:home)) (when (xyz.todo ?t list: list:home)))"
            ]
        );
    }

    #[test]
    fn it_joins_the_reads_of_one_entity_under_unless() {
        let kernel = expanded(
            "(concept (author ?p name: ?n email: ?e) (io.person ?p name: ?n) (io.contact ?p email: ?e))",
            "(query (io.person ?p name: ?n) (when (io.person ?p name: ?n)) (unless (author ?p)))",
        )
        .unwrap();
        assert!(kernel[0].contains("(unless (io.person ?p name: ?n."));
        assert!(kernel[0].contains("io.contact/email: ?e."));
    }
}
