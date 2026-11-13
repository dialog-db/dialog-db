//! Expansion: rewriting a tree until every part of it is kernel.
//!
//! The kernel is small: `rule`, whose first part is its head and whose
//! other parts are its premises, `(unless …)` among them for what must
//! not hold; the application of a relation and of a formula; `assert!`,
//! `retract!`, `assert` and `report` as a rule's head. A rule has one
//! head. Every other form is given its meaning by a rule about syntax:
//!
//! ```clj
//! (rule (syntax (rule (report ?head) ?head))
//!   (expand (query ?head)))
//! ```
//!
//! `(expand pattern)` holds for the tree being expanded when it has the
//! pattern's shape, and `(syntax template)` is what the tree becomes.
//! Neither is walked into: what is inside them is syntax.
//!
//! A template answers `(all a b)` when the tree stands for several, as
//! a concept does for the reads of its body. Among the premises they
//! stand one after another. In the head's place they are a rule each,
//! with the premises again. `(all …)` is not written in a rule: it is
//! refused there.
//!
//! The [`Expander`] is the walker. It rewrites a top-level tree until
//! it is a `rule`, then every head and premise until each applies a
//! relation or a formula, asking the rules about syntax at each step.
//! In this proof of concept those rules are kept and evaluated here,
//! not stored in a replica and not run by the query engine, which
//! cannot yet take a tree apart.

use std::collections::{BTreeMap, BTreeSet};

use crate::Error;
use crate::diagnostic::Diagnostic;
use crate::form::Kind;
use crate::kernel::QUOTE;
use crate::kernel::{Head, Rule};
use crate::parse::parse;
use crate::pattern::{Binding, Bindings, REST, fill, matches};
use crate::syntax::{Parameters, Signature, Syntax};
use crate::written::{Path, Segment};

/// The core forms, written in the kernel.
pub const PRELUDE: &str = include_str!("prelude.clj");

/// Where a form stands when it is expanded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Position {
    /// A rule's head, or the top level of a document.
    Head,
    /// A premise, `(unless …)` or not.
    Premise,
}

/// What a rule about syntax concludes.
#[derive(Clone, Debug)]
enum Answer {
    /// `(syntax template)`: the form becomes the template, filled.
    Syntax(Syntax),
    /// `(warning "…")`: the form stands, and this is said about it.
    Warning(String),
}

/// A rule about syntax: `(expand pattern)` among its premises, and
/// `(syntax template)` or `(warning "…")` as its head.
#[derive(Clone, Debug)]
struct Definition {
    answer: Answer,
    pattern: Syntax,
    position: Option<Position>,
    when: Vec<Syntax>,
    unless: Vec<Syntax>,
    /// What the rule defines, where it defines a concept: a name
    /// applied with labels, standing for reads and nothing else.
    signature: Option<Signature>,
}

/// One form that several definitions take, and which of them stands
/// for it in the rule being expanded.
#[derive(Clone, Debug)]
struct Choice {
    form: Syntax,
    taken: usize,
    of: usize,
}

/// The words that make a rule's head a write or an answer.
const HEAD_WORDS: [&str; 4] = ["assert!", "retract!", "assert", "report"];

/// The words whose parts are syntax, never walked into.
const DIRECTIVES: [&str; 3] = ["expand", "syntax", "warning"];

/// `name` applied to `items`, with no labelled part.
fn apply(name: &str, items: Vec<Syntax>) -> Syntax {
    Syntax::Application(
        Box::new(Syntax::Reference(name.to_string())),
        Parameters {
            items,
            ..Parameters::default()
        },
    )
}

/// The parts with no label of an application, or none.
fn items(tree: &Syntax) -> &[Syntax] {
    tree.operand().map_or(&[], |operand| &operand.items)
}

/// The parts of a kernel rule.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Clauses {
    /// What the rule says about itself: its labelled parts.
    pub said: BTreeMap<String, Syntax>,
    /// Its head: the first part.
    pub head: Option<Syntax>,
    /// Its premises: what must hold.
    pub when: Vec<Syntax>,
    /// What `(unless …)` holds: what must not.
    pub unless: Vec<Syntax>,
}

impl Clauses {
    /// The parts of a `(rule …)` tree: its first part is its head, every
    /// part after it is a premise, and what `(unless …)` holds must not
    /// hold.
    pub fn of(rule: &Syntax) -> Self {
        let mut clauses = Clauses::default();
        let Some(operand) = rule.operand() else {
            return clauses;
        };
        clauses.said = operand.terms.clone();
        let Some((head, premises)) = operand.items.split_first() else {
            return clauses;
        };
        clauses.head = Some(head.clone());
        for premise in premises {
            if premise.applies() == Some("unless") {
                clauses.unless.extend(items(premise).iter().cloned());
            } else {
                clauses.when.push(premise.clone());
            }
        }
        clauses
    }

    /// These parts as a `(rule …)` tree.
    pub fn into_rule(self) -> Syntax {
        let mut parts: Vec<Syntax> = self.head.into_iter().collect();
        parts.extend(self.when);
        if !self.unless.is_empty() {
            parts.push(apply("unless", self.unless));
        }
        Syntax::Application(
            Box::new(Syntax::Reference("rule".into())),
            Parameters {
                items: parts,
                terms: self.said,
            },
        )
    }
}

/// The trees a tree stands for: itself, or what `(all …)` holds.
fn members(tree: Syntax) -> Vec<Syntax> {
    if tree.applies() == Some("all") {
        items(&tree).iter().cloned().flat_map(members).collect()
    } else {
        vec![tree]
    }
}

/// Whether `tree` is written as a form may be, and otherwise the part
/// that is not and why. The reader gives labels and `...` no meaning,
/// so what they may not say is said here.
///
/// A form with a label written is one record, and its one part with no
/// label is the record's `of:`. So beside labels a second part with no
/// label is refused, and so is `of:` beside a part with none. `...` and
/// what follows it are the parts not named, and no part of their own.
fn valid(tree: &Syntax) -> Result<(), (Syntax, Error)> {
    let refuse = |why: String| Err((tree.clone(), Error::Refused(why)));
    match tree {
        Syntax::Reference(name) if name.starts_with(REST) && name != REST => refuse(format!(
            "`{name}`: `...` stands apart from what follows it, `... {}`",
            &name[REST.len()..]
        )),
        // What is quoted is not a form that says anything.
        Syntax::Application(..) if tree.applies() == Some(QUOTE) => Ok(()),
        Syntax::Application(operator, operand) => {
            // What is inside first, so the part that is refused is the
            // innermost that is.
            valid(operator)?;
            operand
                .items
                .iter()
                .chain(operand.terms.values())
                .try_for_each(valid)?;
            let named = operand
                .items
                .iter()
                .take_while(|item| !matches!(item, Syntax::Reference(name) if name == REST))
                .count();
            if !operand.terms.is_empty() && named > 1 {
                return refuse(format!(
                    "{tree} has {named} parts with no label beside its labelled parts: the \
                     labelled parts are one record, and its one part with no label is its `of:`"
                ));
            }
            if named > 0 && operand.terms.contains_key("of") {
                return refuse(format!(
                    "{tree} says `of:` twice: the part with no label is its `of:`"
                ));
            }
            Ok(())
        }
        Syntax::Tuple(items) => items.iter().try_for_each(valid),
        Syntax::Map(entries) => entries.iter().try_for_each(|(key, value)| {
            valid(key)?;
            valid(value)
        }),
        _ => Ok(()),
    }
}

/// `tree` with the variables an expansion at `site` named for itself
/// renamed by where each first occurs, so two answers that differ only
/// in what a definition called its own variables are equal.
fn unnamed(tree: &Syntax, site: &str) -> Syntax {
    fn walk(tree: &Syntax, suffix: &str, seen: &mut Vec<String>) -> Syntax {
        match tree {
            Syntax::Variable(name) if name.ends_with(suffix) => {
                let at = seen
                    .iter()
                    .position(|other| other == name)
                    .unwrap_or_else(|| {
                        seen.push(name.clone());
                        seen.len() - 1
                    });
                Syntax::Variable(format!("{at}{suffix}"))
            }
            Syntax::Tuple(items) => {
                Syntax::Tuple(items.iter().map(|item| walk(item, suffix, seen)).collect())
            }
            Syntax::Map(entries) => Syntax::Map(
                entries
                    .iter()
                    .map(|(key, value)| (walk(key, suffix, seen), walk(value, suffix, seen)))
                    .collect(),
            ),
            Syntax::Application(operator, operand) => {
                let operator = walk(operator, suffix, seen);
                let items = operand
                    .items
                    .iter()
                    .map(|item| walk(item, suffix, seen))
                    .collect();
                let terms = operand
                    .terms
                    .iter()
                    .map(|(label, term)| (label.clone(), walk(term, suffix, seen)))
                    .collect();
                Syntax::Application(Box::new(operator), Parameters { items, terms })
            }
            other => other.clone(),
        }
    }
    walk(tree, &format!(".{site}"), &mut Vec::new())
}

/// Several reads of one entity as one read, so that their absence is
/// the absence of all of them together. Dialog negates one proposition,
/// and a read of one entity may span domains.
fn together(mut trees: Vec<Syntax>, written: &Syntax) -> Result<Syntax, Error> {
    if trees.len() == 1 {
        return Ok(trees.remove(0));
    }
    let spans = || {
        Error::Refused(format!(
            "the absence of {written} is not one read: dialog negates one proposition, and this spans entities"
        ))
    };
    let entity = items(&trees[0]).first().cloned();
    if !trees
        .iter()
        .all(|tree| tree.is_read() && items(tree).first() == entity.as_ref())
    {
        return Err(spans());
    }
    let domain = trees[0].applies().unwrap_or_default().to_string();
    let mut operand = Parameters {
        items: entity.into_iter().collect(),
        ..Parameters::default()
    };
    for tree in &trees {
        let other = tree.applies().unwrap_or_default();
        for (label, term) in tree
            .operand()
            .into_iter()
            .flat_map(|operand| &operand.terms)
        {
            let whole = if label.contains('/') || other == domain {
                label.clone()
            } else {
                format!("{other}/{label}")
            };
            if let Some(said) = operand.terms.insert(whole.clone(), term.clone())
                && said != *term
            {
                return Err(Error::Refused(format!(
                    "the absence of {written} says `{whole}:` twice"
                )));
            }
        }
    }
    Ok(Syntax::Application(
        Box::new(Syntax::Reference(domain)),
        operand,
    ))
}

/// The walker, with the rules about syntax it has been given.
#[derive(Clone, Debug, Default)]
pub struct Expander {
    syntax: Vec<Definition>,
    /// The names some rule about syntax is for.
    names: BTreeSet<String>,
    /// The domains some installed rule reads.
    reads: BTreeSet<String>,
    /// Every form the rules were asked about since the last report,
    /// and which tree of the document it was asked for.
    asked: Vec<(Syntax, Position, usize)>,
    /// The trees of the document being expanded, as given.
    document: Vec<Syntax>,
    /// Which of them is being expanded.
    tree: usize,
    /// For each kernel rule of the last document, which tree it came of.
    origin: Vec<usize>,
    /// The parts of the tree being expanded that are being worked on,
    /// outermost first: what a refusal is about.
    blame: Vec<Syntax>,
    /// The forms several definitions take in the rule being expanded.
    choices: Vec<Choice>,
    /// Whether the head being expanded reports, and writes nothing.
    reporting: bool,
    /// Whether the premise being expanded is one that must not hold.
    negating: bool,
}

impl Expander {
    /// A walker that knows the core forms.
    pub fn new() -> Result<Self, Error> {
        let mut expander = Expander::default();
        expander.submit(PRELUDE)?;
        Ok(expander)
    }

    /// Parse `text` and expand every tree of it. A rule about syntax is
    /// kept here; every other comes back as kernel rules, in the order
    /// written, with what the rules said about the document. A tree
    /// whose head stands for several comes back as a rule each.
    pub fn submit(&mut self, text: &str) -> Result<(Vec<Syntax>, Vec<Diagnostic>), Error> {
        self.expand(parse(text)?)
    }

    /// Expand every tree of `trees`, as [`submit`](Self::submit) does
    /// the trees a text parses to. The first refusal is the error.
    pub fn expand(&mut self, trees: Vec<Syntax>) -> Result<(Vec<Syntax>, Vec<Diagnostic>), Error> {
        let (kernel, diagnostics) = self.check(trees);
        match diagnostics
            .iter()
            .find_map(|diagnostic| diagnostic.refusal.clone())
        {
            Some(error) => Err(error),
            None => Ok((kernel, diagnostics)),
        }
    }

    /// Expand every tree of `trees` and say everything there is to say
    /// about them. A tree that is refused gives no rule and a
    /// diagnostic that holds the refusal, and the trees after it are
    /// expanded all the same. The refusals come first, in the order of
    /// their trees, and then what the rules said without refusing.
    pub fn check(&mut self, trees: Vec<Syntax>) -> (Vec<Syntax>, Vec<Diagnostic>) {
        self.asked.clear();
        self.origin.clear();
        self.document = trees.clone();
        let mut kernel = Vec::new();
        let mut diagnostics = Vec::new();
        for (at, tree) in trees.into_iter().enumerate() {
            self.tree = at;
            self.blame.clear();
            let kept = kernel.len();
            if let Err(error) = self.each(at, &tree, &mut kernel) {
                kernel.truncate(kept);
                self.origin.truncate(kept);
                // The refusal is about the innermost part being worked
                // on that is a part of the tree as written.
                let (form, inside) = self
                    .blame
                    .iter()
                    .rev()
                    .find_map(|part| Some((part.clone(), tree.locate(part)?)))
                    .unwrap_or((tree.clone(), Path::new()));
                let mut path = vec![Segment::Item(at)];
                path.extend(inside);
                diagnostics.push(Diagnostic::refused(Some(form), path, error));
            }
        }
        diagnostics.extend(self.diagnose());
        (kernel, diagnostics)
    }

    /// The kernel rules one tree of a document expands to, added to
    /// `kernel`.
    fn each(&mut self, at: usize, tree: &Syntax, kernel: &mut Vec<Syntax>) -> Result<(), Error> {
        // Where several definitions take a form that is read, the rule
        // is expanded once a definition: what is read is what any of
        // them reads. Within one rule a form that recurs is the same
        // definition's each time.
        if let Err((part, error)) = valid(tree) {
            return self.about(&part, |_| Err(error));
        }
        let mut choices = Vec::new();
        loop {
            self.choices = choices;
            let rules = self.top(tree.clone())?;
            choices = std::mem::take(&mut self.choices);
            for rule in rules {
                if let Some(rule) = self.install(rule)?
                    && !kernel.contains(&rule)
                {
                    kernel.push(rule);
                    self.origin.push(at);
                }
            }
            while let Some(last) = choices.last_mut() {
                if last.taken + 1 < last.of {
                    last.taken += 1;
                    break;
                }
                choices.pop();
            }
            if choices.is_empty() {
                return Ok(());
            }
        }
    }

    /// Do `work` with `part` as what a refusal in it is about.
    fn about<T>(
        &mut self,
        part: &Syntax,
        work: impl FnOnce(&mut Self) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let depth = self.blame.len();
        self.blame.push(part.clone());
        let done = work(self);
        if done.is_ok() {
            self.blame.truncate(depth);
        }
        done
    }

    /// For each kernel rule the last document expanded to, which of its
    /// trees the rule came of.
    pub fn origin(&self) -> &[usize] {
        &self.origin
    }

    /// Keep a rule about syntax, and note what any other rule reads.
    fn install(&mut self, rule: Syntax) -> Result<Option<Syntax>, Error> {
        let clauses = Clauses::of(&rule);
        let answer = match clauses.head.as_ref() {
            Some(head) if head.applies() == Some("syntax") => match items(head) {
                [template] => Some(Answer::Syntax(template.clone())),
                _ => return Err(Error::Refused(format!("{head} takes one template"))),
            },
            Some(head) if head.applies() == Some("warning") => match items(head) {
                [Syntax::Constant(dialog_corpus::Value::String(message))] => {
                    Some(Answer::Warning(message.clone()))
                }
                _ => return Err(Error::Refused(format!("{head} takes one text"))),
            },
            _ => None,
        };
        let Some(answer) = answer else {
            for premise in &clauses.when {
                if let (true, Some(domain)) = (premise.is_read(), premise.applies()) {
                    self.reads.insert(domain.to_string());
                }
            }
            return Ok(Some(rule));
        };

        let mut when = clauses.when;
        let at = when
            .iter()
            .position(|premise| premise.applies() == Some("expand"))
            .ok_or_else(|| {
                Error::Refused(format!(
                    "a rule about syntax needs an (expand …) premise: {rule}"
                ))
            })?;
        let expand = when.remove(at);
        let pattern = match items(&expand) {
            [pattern] => pattern.clone(),
            _ => return Err(Error::Refused(format!("{expand} takes one pattern"))),
        };
        let position = match expand.operand().and_then(|operand| operand.terms.get("in")) {
            None => None,
            Some(word) => match word.keyword() {
                Some("head") => Some(Position::Head),
                Some("premise") => Some(Position::Premise),
                _ => return Err(Error::Refused(format!("{word} is not a position"))),
            },
        };

        if let Answer::Syntax(template) = &answer {
            Self::check_concept(&pattern, template)?;
        }
        if let Some(name) = pattern.applies() {
            self.names.insert(name.to_string());
        }
        // A definition under a signature replaces the one before it
        // under the same: the last write wins. Under another signature
        // it stands beside it.
        let signature = match &answer {
            Answer::Syntax(template)
                if position.is_none()
                    && when.is_empty()
                    && clauses.unless.is_empty()
                    && members(template.clone()).iter().all(Syntax::is_read) =>
            {
                Signature::of(&pattern, "concept")
            }
            _ => None,
        };
        if signature.is_some() {
            self.syntax.retain(|rule| rule.signature != signature);
        }
        self.syntax.push(Definition {
            answer,
            pattern,
            position,
            when,
            unless: clauses.unless,
            signature,
        });
        Ok(None)
    }

    /// A concept maps facts to itself and back. So what it stands for is
    /// reads and nothing else, since a check has no inverse, and every
    /// variable in them is one of its parameters, since a write must
    /// know every value. The reads may be of several entities.
    fn check_concept(pattern: &Syntax, template: &Syntax) -> Result<(), Error> {
        let said = members(template.clone());
        if !said.iter().any(Syntax::is_read)
            || !said.iter().all(|tree| tree.is_read() || tree.is_formula())
        {
            return Ok(());
        }
        if let Some(check) = said.iter().find(|tree| tree.is_formula()) {
            return Err(Error::Refused(format!(
                "a concept maps both ways, and a check has no inverse: {check}"
            )));
        }
        let mut given = Vec::new();
        pattern.variables(&mut given);
        let mut used = Vec::new();
        said.iter().for_each(|tree| tree.variables(&mut used));
        match used.iter().find(|name| !given.contains(name)) {
            Some(own) => Err(Error::Refused(format!(
                "a concept maps both ways, so every variable in it is a parameter: ?{own}"
            ))),
            None => Ok(()),
        }
    }

    /// The bindings under which a rule about syntax applies to `tree`.
    fn applies(&self, rule: &Definition, tree: &Syntax, position: Position) -> Option<Bindings> {
        if rule.position.is_some_and(|wanted| wanted != position) {
            return None;
        }
        let mut bindings = matches(tree, &rule.pattern, Bindings::new())?;
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
    /// Such a premise applies one of a few formulas over names, or
    /// reads what this walker knows: the names syntax is defined for,
    /// `(dialog.name _ is: ?name)`, and the domains installed rules
    /// read, `(dialog.rule _ on: ?domain)`.
    fn holds(&self, premise: &Syntax, bindings: Bindings) -> Option<Bindings> {
        let value = |term: &Syntax| match term {
            Syntax::Variable(name) => match bindings.get(name) {
                Some(Binding::One(tree)) => Some(tree.clone()),
                _ => None,
            },
            other => Some(other.clone()),
        };
        let name = |term: &Syntax| match value(term)? {
            Syntax::Reference(name) => Some(name),
            _ => None,
        };
        let Syntax::Application(operator, operand) = premise else {
            return None;
        };
        let subject = operand.items.first();
        match (operator.keyword(), &**operator) {
            (Some("command"), _) => {
                let domain = format!(
                    "command.{}.{}",
                    name(subject?)?,
                    name(operand.terms.get("")?)?
                );
                let Syntax::Variable(is) = operand.terms.get("is")? else {
                    return None;
                };
                let domain = Binding::One(Syntax::Reference(domain));
                let mut bindings = bindings.clone();
                match bindings.get(is) {
                    Some(bound) if *bound == domain => {}
                    Some(_) => return None,
                    None => {
                        bindings.insert(is.clone(), domain);
                    }
                }
                Some(bindings)
            }
            (Some("domain"), _) => {
                (Kind::of(&name(subject?)?) == Kind::Domain).then_some(bindings.clone())
            }
            (Some("name"), _) => {
                (Kind::of(&name(subject?)?) == Kind::Name).then_some(bindings.clone())
            }
            (None, Syntax::Reference(relation)) if relation == "dialog.name" => self
                .names
                .contains(&name(operand.terms.get("is")?)?)
                .then_some(bindings.clone()),
            (None, Syntax::Reference(relation)) if relation == "dialog.rule" => self
                .reads
                .contains(&name(operand.terms.get("on")?)?)
                .then_some(bindings.clone()),
            _ => None,
        }
    }

    /// What the rules say `tree` expands to in `position`. No answer is
    /// refused. Of several:
    ///
    /// - Answers that differ only in the names of their own variables
    ///   are one answer.
    /// - A write takes the definition whose signature is the labels
    ///   written, since a write says every field. None or several such
    ///   is refused.
    /// - A read takes each in turn, the rule expanded once for each.
    /// - What must not hold is refused: its absence under any of the
    ///   definitions would be a rule of several negations, not several
    ///   rules.
    fn ask(&mut self, tree: &Syntax, position: Position) -> Result<Syntax, Error> {
        // What the template's own variables are named apart by: the
        // tree being expanded and where it stands, by reference, so the
        // names do not move with how either is spelled.
        let site = apply(
            "site",
            vec![
                tree.clone(),
                Syntax::Reference(
                    match position {
                        Position::Head => "head",
                        Position::Premise => "premise",
                    }
                    .into(),
                ),
            ],
        )
        .reference()?
        .to_string()[..4]
            .to_string();
        // Of the rules that apply, the ones that name the most: a
        // pattern that leaves a part to its `...` gives way to one that
        // names it. `(?concept ... ?fields)` and
        // `(?concept ?subject ... ?fields)` both match a call with a
        // part that has no label, and the second is the one meant.
        let mut found: Vec<(Syntax, Option<Signature>)> = Vec::new();
        let mut least = usize::MAX;
        for rule in &self.syntax {
            if let (Answer::Syntax(template), Some(bindings)) =
                (&rule.answer, self.applies(rule, tree, position))
            {
                let left: usize = bindings
                    .values()
                    .map(|binding| match binding {
                        Binding::Many(taken) => taken.len(),
                        Binding::Row(taken) => taken.len(),
                        Binding::One(_) => 0,
                    })
                    .sum();
                if left > least {
                    continue;
                }
                if left < least {
                    least = left;
                    found.clear();
                }
                let answer = fill(template, &bindings, &site)?;
                let same = unnamed(&answer, &site);
                if !found.iter().any(|(other, _)| unnamed(other, &site) == same) {
                    found.push((answer, rule.signature.clone()));
                }
            }
        }
        self.asked.push((tree.clone(), position, self.tree));
        let ambiguous = |found: &[(Syntax, Option<Signature>)]| {
            Error::Ambiguous(
                tree.to_string(),
                found
                    .iter()
                    .map(|(answer, _)| answer.to_string())
                    .collect::<Vec<_>>()
                    .join(" | "),
            )
        };
        match found.len() {
            0 => Err(Error::Unknown(tree.to_string())),
            1 => Ok(found.remove(0).0),
            _ if self.negating => Err(ambiguous(&found)),
            _ if position == Position::Head && !self.reporting => {
                let written = Signature::of(tree, "concept");
                let mut exact = found.iter().filter(|(_, signature)| {
                    signature.is_some()
                        && signature.as_ref().map(|signature| &signature.parameters)
                            == written.as_ref().map(|written| &written.parameters)
                });
                match (exact.next(), exact.next()) {
                    (Some((answer, _)), None) => Ok(answer.clone()),
                    _ => Err(ambiguous(&found)),
                }
            }
            of => {
                let taken = match self.choices.iter().find(|choice| choice.form == *tree) {
                    Some(choice) => choice.taken,
                    None => {
                        self.choices.push(Choice {
                            form: tree.clone(),
                            taken: 0,
                            of,
                        });
                        0
                    }
                };
                found
                    .get(taken)
                    .map(|(answer, _)| answer.clone())
                    .ok_or_else(|| ambiguous(&found))
            }
        }
    }

    /// What the rules say about the forms they were asked about, now that
    /// the whole document is in: a rule further down may be what reads a
    /// command issued further up.
    fn diagnose(&mut self) -> Vec<Diagnostic> {
        let mut diagnostics: Vec<Diagnostic> = Vec::new();
        for (form, position, tree) in std::mem::take(&mut self.asked) {
            for rule in &self.syntax {
                if let (Answer::Warning(message), Some(_)) =
                    (&rule.answer, self.applies(rule, &form, position))
                {
                    let mut path = vec![Segment::Item(tree)];
                    path.extend(
                        self.document
                            .get(tree)
                            .and_then(|written| written.locate(&form))
                            .unwrap_or_default(),
                    );
                    let diagnostic = Diagnostic {
                        form: Some(form.clone()),
                        message: message.clone(),
                        path,
                        span: None,
                        refusal: None,
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
    fn application(&mut self, mut tree: Syntax, position: Position) -> Result<Syntax, Error> {
        loop {
            let word = tree.applies().map(str::to_string);
            if tree.is_read()
                || tree.is_formula()
                || word
                    .as_deref()
                    .is_some_and(|word| DIRECTIVES.contains(&word))
            {
                return Ok(tree);
            }
            if word.as_deref() == Some("all") {
                let each = items(&tree)
                    .iter()
                    .cloned()
                    .map(|member| self.application(member, position))
                    .collect::<Result<Vec<_>, _>>()?;
                return Ok(apply("all", each));
            }
            if let (Position::Head, Some(word)) = (
                position,
                word.as_deref().filter(|word| HEAD_WORDS.contains(word)),
            ) {
                let inner = match items(&tree) {
                    [inner] => inner.clone(),
                    _ => return Err(Error::Refused(format!("{tree} takes one application"))),
                };
                let reporting = std::mem::replace(&mut self.reporting, word == "report");
                let stands = self.application(inner, Position::Head);
                self.reporting = reporting;
                let mut each: Vec<Syntax> = members(stands?)
                    .into_iter()
                    .map(|member| apply(word, vec![member]))
                    .collect();
                return Ok(if each.len() == 1 {
                    each.remove(0)
                } else {
                    apply("all", each)
                });
            }
            let asked = tree;
            tree = self.about(&asked, |this| this.ask(&asked, position))?;
        }
    }

    /// A top-level tree, rewritten until it is kernel rules. A write at
    /// the top level is a rule with that head and whatever clauses
    /// follow it. A kernel rule has one head, so a head that stands for
    /// several, as a concept of several reads does, is a rule each, with
    /// the premises again.
    fn top(&mut self, mut tree: Syntax) -> Result<Vec<Syntax>, Error> {
        while tree.applies() != Some("rule") {
            tree = match (
                tree.applies().filter(|word| HEAD_WORDS.contains(word)),
                tree.operand(),
            ) {
                (Some(word), Some(operand)) => {
                    let Some((first, rest)) = operand.items.split_first() else {
                        return Err(Error::Refused(format!("{tree} takes an application")));
                    };
                    let mut parts = vec![apply(word, vec![first.clone()])];
                    parts.extend(rest.iter().cloned());
                    Syntax::Application(
                        Box::new(Syntax::Reference("rule".into())),
                        Parameters {
                            items: parts,
                            terms: operand.terms.clone(),
                        },
                    )
                }
                _ => self.about(&tree, |this| this.ask(&tree, Position::Head))?,
            };
        }
        let written = Clauses::of(&tree);
        // `(all …)` is what a template answers when a tree stands for
        // several. Nobody writes it: several premises are written one
        // after another, and several heads are a rule each.
        let several = |part: &Syntax| part.applies() == Some("all");
        if let Some(head) = &written.head
            && (several(head)
                || (head
                    .applies()
                    .is_some_and(|word| HEAD_WORDS.contains(&word))
                    && items(head).iter().any(several)))
        {
            self.blame.push(head.clone());
            return Err(Error::Refused(format!(
                "{head} is several heads: a rule has one head, so write a rule for each"
            )));
        }
        if let Some(part) = written
            .when
            .iter()
            .chain(&written.unless)
            .find(|part| several(part))
        {
            self.blame.push(part.clone());
            return Err(Error::Refused(format!(
                "{part} is not a premise: premises are written one after another"
            )));
        }
        let heads = match written.head {
            Some(head) => {
                members(self.about(&head, |this| this.application(head.clone(), Position::Head))?)
            }
            None => Vec::new(),
        };
        let mut walked = Clauses {
            said: written.said,
            ..Clauses::default()
        };
        for premise in written.when {
            // Nothing but its place tells a head from a premise, so a
            // write after the first part is said to be misplaced, and
            // not asked about as a premise.
            if premise
                .applies()
                .is_some_and(|word| HEAD_WORDS.contains(&word))
            {
                self.blame.push(premise.clone());
                return Err(Error::Refused(format!(
                    "{premise} is a head, and stands among the premises: the first part of a rule is its head, and a rule has one, so write a rule for each"
                )));
            }
            walked.when.extend(members(self.about(&premise, |this| {
                this.application(premise.clone(), Position::Premise)
            })?));
        }
        for premise in written.unless {
            let joined = self.about(&premise, |this| {
                this.negating = true;
                let stands = this.application(premise.clone(), Position::Premise);
                this.negating = false;
                together(members(stands?), &premise)
            })?;
            walked.unless.push(joined);
        }
        let split = heads.len() > 1;
        let mut rules = Vec::new();
        for head in heads {
            let rule = Clauses {
                head: Some(head),
                ..walked.clone()
            }
            .into_rule();
            // A concept of several reads is reported a rule a read. A
            // read the query named no field of says nothing, so it gets
            // no rule.
            if split
                && let Ok(typed) = Rule::parse(&rule)
                && let Head::Report(read) = &typed.head
                && read.named(&typed.bound()).is_empty()
            {
                continue;
            }
            rules.push(rule);
        }
        if rules.is_empty() {
            return Err(Error::Refused(format!("{tree} reports no field")));
        }
        Ok(rules)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TODO: &str = r#"
        (concept (todo ?t list: ?l text: ?x state: ?s)
          (xyz.todo ?t list: (:last (:entity ?l)) text: (:last (:text ?x)) state: (:last ?s)))
        (rule (assert! (todo ?t list: ?l text: ?x state: :open))
          (add (todo ?t list: ?l text: ?x)))
    "#;

    fn expanded(setup: &str, text: &str) -> Result<Vec<String>, Error> {
        let mut expander = Expander::new()?;
        expander.submit(setup)?;
        let (kernel, _) = expander.submit(text)?;
        Ok(kernel.iter().map(Syntax::to_string).collect())
    }

    /// `(unless …)` once, last, or several times among the premises, is
    /// one rule.
    #[test]
    fn it_reads_unless_beside_when_or_inside_it() {
        let beside = expanded(
            TODO,
            "(rule (xyz.lonely ?t text: ?x)
               (todo ?t text: ?x)
               (unless (xyz.tag ?t name: _) (xyz.star ?t by: _)))",
        )
        .unwrap();
        let inside = expanded(
            TODO,
            "(rule (xyz.lonely ?t text: ?x)
               (todo ?t text: ?x)
               (unless (xyz.tag ?t name: _))
               (unless (xyz.star ?t by: _)))",
        )
        .unwrap();
        assert_eq!(beside, inside);
        assert!(beside[0].contains("(unless (xyz.tag ?t name: _) (xyz.star ?t by: _))"));
    }

    /// Nothing but its place tells a head from a premise, so a write
    /// after the first part is refused. A rule has one head: `(all …)`
    /// in its place is refused too, and both refusals say to write a
    /// rule for each.
    #[test]
    fn it_refuses_a_second_head_however_it_is_written() {
        let among = expanded(
            "",
            "(rule (assert! (xyz.todo ?t list: ?to))
                   (retract! (xyz.todo ?t list: ?from))
               (xyz.move ?t to: ?to from: ?from))",
        );
        assert!(
            matches!(&among, Err(Error::Refused(why))
                if why.contains("stands among the premises") && why.contains("a rule for each")),
            "{among:?}"
        );
        for written in [
            "(rule (all (assert! (xyz.todo ?t list: ?to))
                        (retract! (xyz.todo ?t list: ?from)))
               (xyz.move ?t to: ?to from: ?from))",
            "(rule (assert! (all (xyz.todo ?t list: ?to) (xyz.seen ?t at: ?to)))
               (xyz.move ?t to: ?to from: ?from))",
            "(assert! (all (xyz.todo ?t list: ?to) (xyz.seen ?t at: ?to))
               (xyz.move ?t to: ?to from: ?from))",
        ] {
            let several = expanded("", written);
            assert!(
                matches!(&several, Err(Error::Refused(why))
                    if why.contains("is several heads") && why.contains("a rule for each")),
                "{several:?}"
            );
        }
        let premise = expanded(
            "",
            "(rule (xyz.done ?t at: ?to)
               (all (xyz.todo ?t list: ?to) (xyz.seen ?t at: ?to)))",
        );
        assert!(
            matches!(&premise, Err(Error::Refused(why)) if why.contains("is not a premise")),
            "{premise:?}"
        );
        // The two rules that say it.
        assert_eq!(
            expanded(
                "",
                "(rule (assert! (xyz.todo ?t list: ?to))
                   (xyz.move ?t to: ?to from: ?from))
                 (rule (retract! (xyz.todo ?t list: ?from))
                   (xyz.move ?t to: ?to from: ?from))",
            )
            .unwrap(),
            vec![
                "(rule (assert! (xyz.todo ?t list: ?to)) (xyz.move ?t from: ?from to: ?to))",
                "(rule (retract! (xyz.todo ?t list: ?from)) (xyz.move ?t from: ?from to: ?to))",
            ]
        );
    }

    /// A concept of several reads stands for them all. Written, it is a
    /// rule a read, each with the premises again, since a kernel rule
    /// has one head.
    #[test]
    fn it_writes_a_concept_of_several_reads_as_a_rule_each() {
        let kernel = expanded(
            "(concept (card ?c title: ?x owner: ?o)
               (xyz.card ?c title: (:last (:text ?x)))
               (xyz.owned ?c by: (:last (:entity ?o))))",
            "(rule (assert! (card ?c title: ?x owner: ?o))
               (xyz.draft ?c title: ?x by: ?o))",
        )
        .unwrap();
        assert_eq!(
            kernel,
            vec![
                "(rule (assert! (xyz.card ?c title: (:last (:text ?x)))) (xyz.draft ?c by: ?o title: ?x))",
                "(rule (assert! (xyz.owned ?c by: (:last (:entity ?o)))) (xyz.draft ?c by: ?o title: ?x))",
            ]
        );
    }

    /// Reported, a read the query named no field of gets no rule.
    #[test]
    fn it_reports_only_the_reads_a_query_names_a_field_of() {
        let kernel = expanded(
            "(concept (card ?c title: ?x owner: ?o)
               (xyz.card ?c title: (:last (:text ?x)))
               (xyz.owned ?c by: (:last (:entity ?o))))",
            "(query (card ?c title: ?x))",
        )
        .unwrap();
        assert_eq!(kernel.len(), 1, "{kernel:?}");
        assert!(kernel[0].starts_with("(rule (report (xyz.card ?c title: (:last (:text ?x))))"));
    }

    /// A pattern is any form, not an application only: here a form in
    /// brackets at the top level.
    #[test]
    fn it_expands_a_form_that_is_not_an_application() {
        let kernel = expanded(
            r#"(rule (syntax (assert! (xyz.note ?n text: (:text ?x))))
                 (expand [note ?n ?x]))"#,
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
            kernel.iter().map(Syntax::to_string).collect::<Vec<_>>(),
            vec![
                "(rule (assert! (xyz.todo ?t list: (:last (:entity ?l)) state: (:last :open) text: (:last (:text ?x)))) \
                 (command.add.todo _ about: ?t list: ?l text: ?x))"
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
        let rule = Clauses::of(&parse(&kernel[0]).unwrap()[0]);
        let at = |tree: &Syntax, label: &str| {
            tree.operand()
                .and_then(|operand| operand.terms.get(label))
                .cloned()
        };
        let own = |tree: &Syntax| match at(tree, "list") {
            Some(list) => list.to_string(),
            None => panic!("{tree} reads the list"),
        };
        let reported = rule.head.as_ref().unwrap();
        assert_eq!(reported.applies(), Some("report"));
        let head = &items(reported)[0];
        assert_eq!(at(head, "text"), at(&rule.when[0], "text"));
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
                   (increment (xyz.counter ?c)) (xyz.counter ?c count: ?n))",
            )
            .unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn it_refuses_what_nothing_gives_a_meaning_and_what_two_rules_do() {
        assert!(matches!(
            expanded(TODO, "(query (todo ?t) (frobnicate ?t))"),
            Err(Error::Unknown(_))
        ));
        // A write that says the fields of neither definition.
        assert!(matches!(
            expanded(
                TODO,
                "(concept (todo ?t title: ?x) (xyz.note ?t title: ?x)) (assert! (todo todo:1))"
            ),
            Err(Error::Ambiguous(..))
        ));
    }

    /// Two definitions under one name and other labels stand side by
    /// side, and the labels an application writes say which it is.
    #[test]
    fn it_resolves_a_name_by_the_labels_written() {
        let note = "(concept (todo ?t title: ?x) (xyz.note ?t title: ?x))";
        assert_eq!(
            expanded(TODO, &format!("{note} (query (todo ?t title: ?x))")).unwrap(),
            vec!["(rule (report (xyz.note ?t title: ?x)) (xyz.note ?t title: ?x))"]
        );
        assert_eq!(
            expanded(TODO, &format!("{note} (query (todo ?t text: ?x))")).unwrap(),
            expanded(TODO, "(query (todo ?t text: ?x))").unwrap()
        );
    }

    /// A definition under the signature of an earlier one replaces it:
    /// the last write wins.
    #[test]
    fn it_replaces_a_definition_by_a_later_one_of_the_same_signature() {
        let task = "(concept (todo ?t list: ?l text: ?x state: ?s)
                      (abc.task ?t list: ?l text: ?x state: ?s))";
        // The order the labels are written in is not another signature.
        let reordered = "(concept (todo ?t state: ?s text: ?x list: ?l)
                           (abc.task ?t list: ?l text: ?x state: ?s))";
        for later in [task, reordered] {
            let rules = expanded(TODO, &format!("{later} (query (todo ?t text: ?x))")).unwrap();
            assert_eq!(rules.len(), 1, "{rules:?}");
            assert!(rules[0].contains(")) (abc.task ?t "), "{rules:?}");
            assert!(!rules[0].contains("xyz.todo"), "{rules:?}");
        }
    }

    /// Where the labels written fit several definitions, a read is of
    /// every one of them: the rule is expanded once a definition, and a
    /// form that recurs in it is the same definition's each time.
    #[test]
    fn it_reads_through_every_definition_the_labels_fit() {
        let both = "(concept (todo ?t title: ?x done: ?d) (xyz.todo ?t title: ?x done: ?d))
                    (concept (todo ?t title: ?x pinned: ?p) (xyz.note ?t title: ?x pinned: ?p))";
        let rules = expanded(both, "(query (todo ?t title: ?x))").unwrap();
        assert_eq!(rules.len(), 2, "{rules:?}");
        for (rule, domain) in rules.iter().zip(["xyz.todo", "xyz.note"]) {
            // The labels come in their own order, so the field the
            // query left out stands before or after the one it named.
            assert!(
                rule.starts_with(&format!("(rule (report ({domain} ?t "))
                    && rule.contains(" title: ?x"),
                "{rule}"
            );
            assert!(rule.contains(&format!(")) ({domain} ?t ")), "{rule}");
        }
        // A write says every field, so the labels written are one
        // definition's signature.
        assert_eq!(
            expanded(both, r#"(assert! (todo todo:1 title: "x" pinned: true))"#).unwrap(),
            vec![r#"(rule (assert! (xyz.note todo:1 pinned: true title: "x")))"#]
        );
        // A write that fits both and is neither is refused, and so is
        // the absence of what several definitions take.
        assert!(matches!(
            expanded(both, r#"(assert! (todo todo:1 title: "x"))"#),
            Err(Error::Ambiguous(..))
        ));
        assert!(matches!(
            expanded(
                both,
                "(query (xyz.tag ?t name: ?n) (xyz.tag ?t name: ?n) (unless (todo ?t title: _)))"
            ),
            Err(Error::Ambiguous(..))
        ));
    }

    /// Two definitions of one concept, which call its field by other
    /// names. An application that writes no label is the same read
    /// through either, so nothing is chosen and the rule is one.
    #[test]
    fn it_reads_one_concept_once_however_many_names_its_fields_have() {
        let both = "(concept (todo ?t title: ?a) (xyz.todo ?t text: ?a))
                    (concept (todo ?t name: ?b) (xyz.todo ?t text: ?b))";
        let rules = expanded(
            both,
            "(query (xyz.tag ?t name: ?n) (xyz.tag ?t name: ?n) (todo ?t))",
        )
        .unwrap();
        assert_eq!(rules.len(), 1, "{rules:?}");
        // Each name for the field reads the same attribute.
        assert_eq!(
            expanded(both, "(query (todo ?t title: ?x))").unwrap(),
            expanded(both, "(query (todo ?t name: ?x))").unwrap()
        );
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
            vec!["(rule (report (xyz.todo ?t list: list:home)) (xyz.todo ?t list: list:home))"]
        );
    }

    #[test]
    fn it_joins_the_reads_of_one_entity_under_unless() {
        let kernel = expanded(
            "(concept (author ?p name: ?n email: ?e) (io.person ?p name: ?n) (io.contact ?p email: ?e))",
            "(query (io.person ?p name: ?n) (io.person ?p name: ?n) (unless (author ?p)))",
        )
        .unwrap();
        assert!(kernel[0].contains("(unless (io.person ?p io.contact/email: ?e."));
        assert!(kernel[0].contains(" name: ?n."));
    }
}
