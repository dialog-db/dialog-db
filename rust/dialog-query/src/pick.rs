//! Which of a relation's candidates an attribute reads.

use crate::corpus::{Value, encode_value_owned};
use dialog_corpus::{Cardinality, Contender};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter, Result as FmtResult};

/// Which of a relation's candidates an attribute picks: the newest
/// (`last`), every one (`all`), the best ranked among listed values
/// (`top`), the greatest (`max`) or the least (`min`). An attribute is a
/// relation qualified by a value type and a pick. A write through an
/// attribute carries the pick's [`cardinality`](Pick::cardinality):
/// under `all` it appends, and under every other pick it succeeds the
/// newest claim.
///
/// Each pick is named by an entity, `last:`, `all:`, `top:`, `max:` or
/// `min:`, which is what a batch carries and what an attribute's
/// identity hashes; the plain names an earlier release wrote are still
/// read.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Pick {
    /// The newest claim.
    #[serde(rename = "last:", alias = "last")]
    Last,
    /// Every claim.
    #[serde(rename = "all:", alias = "all")]
    All,
    /// The claim with the greatest value, the newest among equals.
    #[serde(rename = "max:", alias = "max")]
    Max,
    /// The claim with the least value, the newest among equals.
    #[serde(rename = "min:", alias = "min")]
    Min,
    /// The claim whose value is listed first, best first; an unlisted
    /// value ranks last, and the newest wins among equals.
    #[serde(rename = "top:", alias = "top")]
    Top(Vec<Value>),
}

impl Display for Pick {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(self.name())
    }
}

impl Pick {
    /// The pick's name as the notation spells it: `last`, `all`, `top`,
    /// `max` or `min`.
    pub fn name(&self) -> &'static str {
        let uri = self.uri();
        &uri[..uri.len() - 1]
    }

    /// The entity that names this pick: `last:`, `all:`, `top:`, `max:`
    /// or `min:`.
    pub fn uri(&self) -> &'static str {
        match self {
            Pick::Last => "last:",
            Pick::All => "all:",
            Pick::Top(_) => "top:",
            Pick::Max => "max:",
            Pick::Min => "min:",
        }
    }

    /// Whether the pick returns the whole candidate set (`all`) rather
    /// than one candidate.
    pub fn is_set(&self) -> bool {
        matches!(self, Pick::All)
    }

    /// The values a `top` ranks, best first; empty for every other pick.
    pub fn ranked(&self) -> &[Value] {
        match self {
            Pick::Top(values) => values,
            _ => &[],
        }
    }

    /// Where `value` ranks under this pick: its position among
    /// the values a `top` lists, best first, an unlisted value last;
    /// zero under every other pick and under a `top` listing nothing.
    pub fn rank_of(&self, value: &Value) -> usize {
        match self {
            Pick::Top(among) => Self::rank_among(among, value),
            _ => 0,
        }
    }

    /// Where `value` ranks among `listed`, best first: an unlisted
    /// value ranks last, and every value ranks first when nothing is
    /// listed.
    pub fn rank_among(listed: &[Value], value: &Value) -> usize {
        if listed.is_empty() {
            return 0;
        }
        listed
            .iter()
            .position(|candidate| candidate == value)
            .unwrap_or(usize::MAX)
    }

    /// Whether `candidate` is the newer of two claims: the greater
    /// standing, then, between claims that stand equal (two values one
    /// revision wrote under one cause), the greater hash of the value
    /// ([`Value::tiebreak`]). This is the whole of `last`, and what every
    /// other pick falls back to among equals.
    pub fn newer(candidate: Contender<'_>, incumbent: Contender<'_>) -> bool {
        Cardinality::newer(candidate, incumbent)
    }

    /// Whether `candidate` displaces `incumbent` under this pick:
    /// the one a read under the pick returns of the two. `last` takes
    /// the newer; `top` the better rank, then the newer; `max` and `min`
    /// the greater or the lesser value, then the greater standing. This
    /// is the one ordering every election runs on, whether it elects
    /// among a cell's stored claims at the tree or among stored and
    /// derived candidates in a query.
    pub fn prefers(&self, candidate: Contender<'_>, incumbent: Contender<'_>) -> bool {
        match self {
            Pick::Last | Pick::All => Self::newer(candidate, incumbent),
            Pick::Top(_) => {
                let (mine, theirs) = (self.rank_of(candidate.0), self.rank_of(incumbent.0));
                mine < theirs || (mine == theirs && Self::newer(candidate, incumbent))
            }
            Pick::Max | Pick::Min => {
                let ordering = candidate.0.partial_cmp(incumbent.0).unwrap_or_else(|| {
                    encode_value_owned(candidate.0).cmp(&encode_value_owned(incumbent.0))
                });
                let wanted = match self {
                    Pick::Max => Ordering::Greater,
                    _ => Ordering::Less,
                };
                ordering == wanted || (ordering.is_eq() && candidate.1 > incumbent.1)
            }
        }
    }

    /// The cardinality of an attribute read under this pick: many
    /// under `all`, one under every other pick.
    pub fn cardinality(&self) -> Cardinality {
        if self.is_set() {
            Cardinality::Many
        } else {
            Cardinality::One
        }
    }

    /// The pick an attribute of `cardinality` reads under when none is
    /// declared: `last` for one, `all` for many.
    pub fn of(cardinality: Cardinality) -> Self {
        match cardinality {
            Cardinality::One => Pick::Last,
            Cardinality::Many => Pick::All,
        }
    }

    /// Whether a read under this pick elects one claim: every pick but
    /// `all`.
    pub fn elects(&self) -> bool {
        !matches!(self, Pick::All)
    }

    /// The claim this pick elects among `claims`, by index: the one
    /// a read under it returns. `None` over no claims, and under `all`,
    /// which elects nothing.
    pub fn elect<'a, I>(&self, claims: I) -> Option<usize>
    where
        I: IntoIterator<Item = Contender<'a>>,
    {
        if !self.elects() {
            return None;
        }
        let mut best: Option<(usize, Contender<'a>)> = None;
        for (index, claim) in claims.into_iter().enumerate() {
            best = Some(match best {
                Some(incumbent) if !self.prefers(claim, incumbent.1) => incumbent,
                _ => (index, claim),
            });
        }
        best.map(|(index, _)| index)
    }
}
