//! Noun detection: turning an argument's text into scored readings.
//!
//! Ubiquity's noun types each had a `suggest(text)` returning zero or
//! more `{text, html, data, summary, score}`. Here the two kinds of
//! noun are built in: arbitrary text, and rows of a concept matched by
//! their label.

use serde::{Deserialize, Serialize};

use crate::registry::{Candidate, Noun, Registry};

/// The value an argument resolves to.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "lowercase")]
pub enum Value {
    /// A concept row, by entity.
    Entity(String),
    /// Text as typed.
    Text(String),
}

/// One reading of an argument's text.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Suggestion {
    /// How the reading is shown.
    pub text: String,
    /// What the command field receives. `None` for an empty default.
    pub value: Option<Value>,
    /// The noun's confidence. Ubiquity's scale: 1 is a match, 0.3 a
    /// guess; nothing caps it at 1.
    pub score: f64,
}

/// Ubiquity's `noun_arb_text` score.
pub const ARBITRARY_TEXT: f64 = 0.3;

/// How many rows of a concept are kept per argument text. Ubiquity
/// bounded noun types that could return many rows the same way.
const MAX_CANDIDATES: usize = 10;

/// Ubiquity's `NounUtils.matchScore`, for `typed` found in `label`:
/// an early, long match scores high; the whole label scores 1.
pub fn match_score(typed: &str, label: &str) -> Option<f64> {
    if typed.is_empty() {
        return None;
    }
    let label_lower = label.to_lowercase();
    let typed_lower = typed.to_lowercase();
    let byte_index = label_lower.find(&typed_lower)?;
    let length = label_lower.chars().count() as f64;
    let index = label_lower[..byte_index].chars().count() as f64;
    let matched = typed_lower.chars().count() as f64;
    Some(0.3 + 0.25 * (matched / length).sqrt() + 0.45 * (1.0 - index / length))
}

/// Read `text` as `noun`. `entity` is the selected entity when `text`
/// came from a selection or stood in for an anaphor.
pub(crate) fn detect(
    registry: &Registry,
    noun: &Noun,
    text: &str,
    entity: Option<&str>,
) -> Vec<Suggestion> {
    match noun {
        Noun::Text => vec![Suggestion {
            text: text.to_owned(),
            value: Some(Value::Text(text.to_owned())),
            score: ARBITRARY_TEXT,
        }],
        Noun::Concept(concept) => {
            let candidates = registry.candidates(concept);
            if let Some(entity) = entity
                && let Some(candidate) = candidates.iter().find(|row| row.entity == entity)
            {
                return vec![exact(candidate)];
            }
            let mut matches: Vec<Suggestion> = candidates
                .iter()
                .filter_map(|candidate| {
                    match_score(text, &candidate.label).map(|score| Suggestion {
                        text: candidate.label.clone(),
                        value: Some(Value::Entity(candidate.entity.clone())),
                        score,
                    })
                })
                .collect();
            matches.sort_by(|a, b| b.score.total_cmp(&a.score));
            matches.truncate(MAX_CANDIDATES);
            matches
        }
    }
}

/// The default for an empty argument: the entity the page shows, when
/// it is a row of the noun's concept; otherwise the noun's default, when
/// the host gave one; otherwise nothing.
pub(crate) fn default(registry: &Registry, noun: &Noun, this: Option<&str>) -> Suggestion {
    if let (Noun::Concept(concept), Some(this)) = (noun, this)
        && let Some(candidate) = registry
            .candidates(concept)
            .iter()
            .find(|row| row.entity == this)
    {
        return exact(candidate);
    }
    if let Noun::Concept(concept) = noun
        && let Some(candidate) = registry.defaults.get(concept)
    {
        return exact(candidate);
    }
    Suggestion {
        text: String::new(),
        value: None,
        score: 1.0,
    }
}

fn exact(candidate: &Candidate) -> Suggestion {
    Suggestion {
        text: candidate.label.clone(),
        value: Some(Value::Entity(candidate.entity.clone())),
        score: 1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(entity: &str, label: &str) -> Candidate {
        Candidate {
            entity: entity.to_owned(),
            label: label.to_owned(),
        }
    }

    #[test]
    fn it_defaults_to_the_page_entity_before_the_nouns_default() {
        let mut registry = Registry::default();
        let notebook = Noun::Concept("notebook".to_owned());
        registry.candidates.insert(
            "notebook".to_owned(),
            vec![candidate("nb:a", "Alpha"), candidate("nb:b", "Beta")],
        );
        assert_eq!(default(&registry, &notebook, None).value, None);

        registry
            .defaults
            .insert("notebook".to_owned(), candidate("nb:b", "Beta"));
        let filled = default(&registry, &notebook, None);
        assert_eq!(filled.text, "Beta");
        assert_eq!(filled.value, Some(Value::Entity("nb:b".to_owned())));

        let shown = default(&registry, &notebook, Some("nb:a"));
        assert_eq!(shown.value, Some(Value::Entity("nb:a".to_owned())));
    }

    #[test]
    fn it_scores_a_whole_label_as_one() {
        assert_eq!(match_score("Roadmap", "roadmap"), Some(1.0));
    }

    #[test]
    fn it_scores_an_early_long_match_above_a_late_short_one() {
        let early = match_score("road", "roadmap").unwrap();
        let late = match_score("map", "roadmap").unwrap();
        assert!(early > late);
    }

    #[test]
    fn it_does_not_match_absent_text() {
        assert_eq!(match_score("budget", "roadmap"), None);
        assert_eq!(match_score("", "roadmap"), None);
    }
}
