//! A language's grammar: which words introduce which roles, where
//! arguments sit relative to those words, and where the verb goes.
//!
//! This is Ubiquity's per-language parser file (`parser/new/en.js`,
//! `ja.js`, …) as data. A command never names a connecting word; it
//! only says which role each of its fields plays. The grammar maps
//! roles to words, so every command parses in every language that has
//! a grammar.

use serde::{Deserialize, Serialize};

/// The role an unmarked argument falls into.
pub const OBJECT: &str = "object";

/// Which side of its delimiter an argument sits on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Branching {
    /// Prepositional: the argument follows its delimiter ("to *bob*").
    Right,
    /// Postpositional: the argument precedes its delimiter ("*ボブ*に").
    Left,
}

/// One word that can introduce a role. Roles and delimiters are
/// many-to-many: "at" introduces both `location` and `time`, and
/// `location` is introduced by "near", "on", "at" and "in".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Marker {
    /// The role's name, as the `palette/role` instance names it.
    pub role: String,
    /// The word that introduces it.
    pub delimiter: String,
}

/// A language's parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Grammar {
    /// Locale code, e.g. `en`.
    pub locale: String,
    /// Which side of a delimiter an argument sits on.
    pub branching: Branching,
    /// Whether words are separated by spaces. When they are not, the
    /// input is broken at every delimiter before splitting.
    pub spaces: bool,
    /// What joins words back together for display.
    pub join: String,
    /// Score multiplier for a verb found at the start of the input.
    pub verb_initial: f64,
    /// Score multiplier for a verb found at the end of the input.
    pub verb_final: f64,
    /// Whether a suggested verb is displayed at the end.
    pub verb_final_display: bool,
    /// Words that stand for the selection ("this", "it").
    pub anaphora: Vec<String>,
    /// Words that introduce roles.
    pub markers: Vec<Marker>,
    /// Leading words stripped from an argument (articles), kept for
    /// display. Ubiquity's `normalizeArgument`.
    pub articles: Vec<String>,
}

fn markers(pairs: &[(&str, &str)]) -> Vec<Marker> {
    pairs
        .iter()
        .map(|(role, delimiter)| Marker {
            role: (*role).to_owned(),
            delimiter: (*delimiter).to_owned(),
        })
        .collect()
}

impl Grammar {
    /// English, transcribed from Ubiquity's `en.js`.
    pub fn english() -> Self {
        Self {
            locale: "en".into(),
            branching: Branching::Right,
            spaces: true,
            join: " ".into(),
            verb_initial: 1.0,
            verb_final: 0.3,
            verb_final_display: false,
            anaphora: ["this", "that", "it", "selection", "him", "her", "them"]
                .map(String::from)
                .to_vec(),
            markers: markers(&[
                ("goal", "to"),
                ("source", "from"),
                ("location", "near"),
                ("location", "on"),
                ("location", "at"),
                ("location", "in"),
                ("time", "at"),
                ("time", "on"),
                ("instrument", "with"),
                ("instrument", "using"),
                ("format", "in"),
                ("modifier", "of"),
                ("modifier", "for"),
                ("alias", "as"),
                ("alias", "named"),
            ]),
            articles: Vec::new(),
        }
    }

    /// Japanese, transcribed from Ubiquity's `ja.js`.
    pub fn japanese() -> Self {
        Self {
            locale: "ja".into(),
            branching: Branching::Left,
            spaces: false,
            join: String::new(),
            verb_initial: 0.3,
            verb_final: 1.0,
            verb_final_display: true,
            anaphora: ["これ", "それ", "あれ"].map(String::from).to_vec(),
            markers: markers(&[
                (OBJECT, "を"),
                (OBJECT, "と"),
                ("goal", "に"),
                ("goal", "へ"),
                ("source", "から"),
                ("time", "に"),
                ("location", "で"),
                ("location", "に"),
                ("instrument", "で"),
                ("alias", "として"),
                ("modifier", "の"),
                ("format", "で"),
            ]),
            articles: Vec::new(),
        }
    }

    /// The delimiter an object carries in this language, if objects are
    /// marked (Japanese を). An unmarked object then scores lower.
    pub(crate) fn object_delimiter(&self) -> Option<&str> {
        self.markers
            .iter()
            .find(|marker| marker.role == OBJECT && !marker.delimiter.is_empty())
            .map(|marker| marker.delimiter.as_str())
    }

    /// The first delimiter of `role`, used when an argument is moved
    /// into that role or filled by a default.
    pub(crate) fn first_delimiter(&self, role: &str) -> Option<&str> {
        self.markers
            .iter()
            .find(|marker| marker.role == role && !marker.delimiter.is_empty())
            .map(|marker| marker.delimiter.as_str())
    }

    /// Ubiquity's `wordBreaker`: a language without spaces gets a
    /// zero-width space around every delimiter, so splitting can find
    /// the argument boundaries.
    pub(crate) fn word_break(&self, input: &str) -> String {
        if self.spaces {
            return input.to_owned();
        }
        let mut delimiters: Vec<&str> = self
            .markers
            .iter()
            .map(|marker| marker.delimiter.as_str())
            .filter(|delimiter| !delimiter.is_empty())
            .collect();
        // Longest first, so "から" is not broken as "か" + "ら".
        delimiters.sort_by_key(|delimiter| std::cmp::Reverse(delimiter.chars().count()));
        delimiters.dedup();
        let mut out = String::new();
        let mut rest = input;
        'scan: while !rest.is_empty() {
            for delimiter in &delimiters {
                if let Some(after) = rest.strip_prefix(delimiter) {
                    out.push('\u{200b}');
                    out.push_str(delimiter);
                    out.push('\u{200b}');
                    rest = after;
                    continue 'scan;
                }
            }
            let mut chars = rest.chars();
            if let Some(ch) = chars.next() {
                out.push(ch);
            }
            rest = chars.as_str();
        }
        out
    }
}
