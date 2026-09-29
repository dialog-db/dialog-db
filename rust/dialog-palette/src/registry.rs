//! What the parser knows: verbs, their arguments, and the candidates
//! each noun offers. All of it is plain data a host loads from its
//! store (in tonk, from `palette/verb`, `palette/argument`,
//! `palette/noun` rows and one subscription per noun concept), so the
//! parser itself does no IO.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A command made sayable: Ubiquity's `CreateCommand` minus `execute`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Verb {
    /// The command's identity (its concept entity).
    pub id: String,
    /// Words the command answers to. Every word, and every word-suffix
    /// of a multi-word name, is a match target.
    pub names: Vec<String>,
    /// At most one argument per role.
    pub arguments: Vec<Argument>,
}

/// How one field of a command is filled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Argument {
    /// The role that fills it (`object`, `goal`, …).
    pub role: String,
    /// The command field it fills (in tonk, the field's attribute).
    pub field: String,
    /// What may fill it.
    pub noun: Noun,
    /// Shown for the argument while it is empty ("expel [member]").
    pub label: String,
}

/// A noun type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "concept", rename_all = "lowercase")]
pub enum Noun {
    /// Any text, as typed. Ubiquity's `noun_arb_text`: it accepts
    /// everything, so it scores low (0.3) and specific nouns win.
    Text,
    /// Rows of a concept, by the concept's identity. The candidates
    /// come from [`Registry::candidates`].
    Concept(String),
}

/// One row of a noun concept, as the palette offers it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Candidate {
    /// The row's entity: what the command field receives.
    pub entity: String,
    /// The row as text: its rendered `label` facet. Typed text is
    /// matched against this.
    pub label: String,
}

/// Everything the parser consults.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Registry {
    /// The verbs.
    pub verbs: Vec<Verb>,
    /// Candidates per noun concept.
    pub candidates: BTreeMap<String, Vec<Candidate>>,
}

impl Registry {
    /// Candidates of `concept`, empty when the host has none.
    pub fn candidates(&self, concept: &str) -> &[Candidate] {
        self.candidates
            .get(concept)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
}

/// Something the user pointed at, which arguments can refer to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selection {
    /// The selection as text.
    pub text: String,
    /// The entity selected, when there is one. A noun whose candidates
    /// include it takes it directly instead of re-reading the text.
    pub entity: Option<String>,
}

/// Where the palette was opened.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Context {
    /// An explicit selection (selected text, a selected row). As in
    /// Ubiquity, it is tried as the object of every parse and stands in
    /// for anaphora.
    pub selection: Option<Selection>,
    /// The entity the page shows. Ambient rather than chosen: it stands
    /// in for anaphora ("this", "it") and fills an empty argument whose
    /// noun it belongs to, as a default, but it is never interpolated
    /// into every parse the way a selection is.
    pub this: Option<Selection>,
}

/// Ubiquity's suggestion memory: how many times each verb was chosen
/// after typing a given verb prefix. The empty prefix counts every use.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Memory {
    counts: BTreeMap<String, BTreeMap<String, u32>>,
}

impl Memory {
    /// How often `verb` was chosen after typing `input`.
    pub fn score(&self, input: &str, verb: &str) -> u32 {
        self.counts
            .get(input)
            .and_then(|verbs| verbs.get(verb))
            .copied()
            .unwrap_or(0)
    }

    /// Record `count` choices of `verb` after typing `input`.
    pub fn set(&mut self, input: &str, verb: &str, count: u32) {
        self.counts
            .entry(input.to_owned())
            .or_default()
            .insert(verb.to_owned(), count);
    }

    /// Record one choice: under the typed verb prefix, and under "".
    pub fn remember(&mut self, input: Option<&str>, verb: &str) {
        if let Some(input) = input.filter(|input| !input.is_empty()) {
            let count = self.score(input, verb) + 1;
            self.set(input, verb, count);
        }
        let count = self.score("", verb) + 1;
        self.set("", verb, count);
    }
}
