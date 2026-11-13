//! An inductive rule as commit-time induction holds it: the rule, and
//! what its body is to the other rules of a round.
//!
//! Several rules may state one body and differ only in what they
//! conclude from it: a notation with one head a rule writes a rule a
//! head, the premises repeated. [`SharedBody`] is what makes that
//! visible. Two rules whose `SharedBody`s are [`same`](SharedBody::same)
//! evaluate one conjunction, under one typing, up to the names of their
//! variables, so a round evaluates it once and each rule writes its own
//! head from the matches (see `induce`).

use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;

use dialog_query::rule::canonical;
use dialog_query::InductiveRule;
#[cfg(test)]
use dialog_query::Premise;
use dialog_query::type_system::Type as Kind;

/// An inductive rule decoded from its stored bytes, with what a round
/// may share of its body worked out the first time a round asks.
#[derive(Debug)]
pub(crate) struct Induced {
    rule: InductiveRule,
    body: OnceLock<Option<SharedBody>>,
}

impl Induced {
    /// `rule`, with its shared body not yet worked out.
    pub(crate) fn new(rule: InductiveRule) -> Self {
        Induced {
            rule,
            body: OnceLock::new(),
        }
    }

    /// The rule.
    pub(crate) fn rule(&self) -> &InductiveRule {
        &self.rule
    }

    /// What this rule's body is to another rule's: `None` when the body
    /// cannot be told apart from its head (see [`SharedBody::of`]), and
    /// the rule is then always evaluated on its own.
    pub(crate) fn body(&self) -> Option<&SharedBody> {
        self.body
            .get_or_init(|| SharedBody::of(&self.rule))
            .as_ref()
    }
}

/// A rule's body as its evaluation reads it, spelled without the head:
/// the premises and the type of every variable, under names that depend
/// on the premises alone.
///
/// The evaluation of a seeded rule reads three things of the rule: its
/// premises, to seed and to plan; the types analysis inferred, which
/// the planner stamps on the premises' variables; and nothing else. A
/// head reaches the evaluation only through those types, where a
/// numeric head field narrows a variable the body left open
/// (`TypeEnv::infer_for`). So two rules with the same premises and the
/// same types, under a renaming of variables, find the same matches
/// under that renaming, and a head that narrows a variable differently
/// makes the types differ and the bodies with them.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SharedBody {
    /// The premises renamed and sorted, and each variable's name there.
    spelled: canonical::Body,
    /// The type of every variable analysis typed, under its name in
    /// `spelled`, in name order.
    types: Vec<(String, Kind)>,
}

impl SharedBody {
    /// The shared body of `rule`. `None` for a rule that reduces, whose
    /// matches are folded before any head is written, for a body the
    /// formal notation cannot express, and for one whose types name
    /// something its premises do not.
    pub(crate) fn of(rule: &InductiveRule) -> Option<Self> {
        let analysis = rule.analysis();
        if !analysis.reduce.is_empty() {
            return None;
        }
        let spelled = canonical::body(&analysis.premises).ok()??;
        let mut types: Vec<(String, Kind)> = Vec::with_capacity(analysis.types.len());
        for (name, kind) in analysis.types.iter() {
            let name = if name == "this" {
                name.clone()
            } else {
                spelled.names.get(name)?.clone()
            };
            types.push((name, kind.clone()));
        }
        types.sort_by(|a, b| a.0.cmp(&b.0));
        Some(SharedBody { spelled, types })
    }

    /// What buckets bodies: equal for two bodies that are the
    /// [`same`](Self::same), and for few that are not.
    pub(crate) fn bucket(&self) -> &[u8] {
        &self.spelled.encoded
    }

    /// Whether `other` is this body under a renaming of variables: the
    /// same premises, compared as the evaluation holds them and not as
    /// they encode, and the same type for every variable.
    pub(crate) fn same(&self, other: &SharedBody) -> bool {
        self.spelled.premises == other.spelled.premises && self.types == other.types
    }

    /// For each variable of this body, the variable of `other` that
    /// stands in its place. `other` must be the [`same`](Self::same)
    /// body: every variable of one then has its place in the other.
    pub(crate) fn reads(&self, other: &SharedBody) -> Reads {
        let theirs: BTreeMap<&str, &str> = other
            .spelled
            .names
            .iter()
            .map(|(given, spelled)| (spelled.as_str(), given.as_str()))
            .collect();
        Reads(
            self.spelled
                .names
                .iter()
                .filter_map(|(given, spelled)| {
                    let theirs = theirs.get(spelled.as_str())?;
                    Some((given.clone(), (*theirs).to_string()))
                })
                .collect(),
        )
    }

    /// The premises under the names that depend on them alone.
    #[cfg(test)]
    pub(crate) fn premises(&self) -> &[Premise] {
        &self.spelled.premises
    }
}

/// How a rule reads the matches of another rule that states its body:
/// for each of its variables, the variable holding that value there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Reads(HashMap<String, String>);

impl Reads {
    /// The variable that holds `name`'s value in the other rule's
    /// matches. `this` is `this` in both. `None` for a name the body
    /// does not bind, such as an optional head field no premise names:
    /// it must not be looked up there under its own name, where another
    /// variable may happen to carry it.
    pub(crate) fn variable<'a>(&'a self, name: &'a str) -> Option<&'a str> {
        if name == "this" {
            return Some(name);
        }
        self.0.get(name).map(String::as_str)
    }
}
