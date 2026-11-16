//! Electors: how a concept field chooses one of its relation's claims.
//!
//! A field's attribute says what a write does (its cardinality). Its
//! elector says which of the cell's claims a read of the field returns.
//! Each elector is the greatest claim under a total order, so every
//! replica that holds the same claims elects the same one:
//!
//! | elector | orders by                                                    |
//! |---------|--------------------------------------------------------------|
//! | `last`  | standing: the newest claim                                   |
//! | `max`   | value, greatest first, then standing                         |
//! | `min`   | value, least first, then standing                            |
//! | `top`   | the attribute's listed `as`, the last listed greatest, then standing |
//!
//! Ties fall to standing and then to the value's hash
//! ([`Cardinality::newer`]). `top` is `max` under the order the
//! attribute's listing gives its values, which is bounded: the last
//! listed value is the greatest any claim can hold.

use std::cmp::Ordering;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use dialog_corpus::{Cardinality, Contender};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Value;

/// How a concept field chooses one of its relation's claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Elector {
    /// The newest claim: what a field with no elector reads under
    /// cardinality one.
    Last,
    /// The claim holding the greatest value.
    Max,
    /// The claim holding the least value.
    Min,
    /// The claim holding the value listed last in the attribute's `as`,
    /// which ranks its values from least to greatest. A value the list
    /// does not hold ranks below every listed one.
    Top,
}

impl Elector {
    /// The elector's name as the notation spells it.
    pub fn name(self) -> &'static str {
        match self {
            Elector::Last => "last",
            Elector::Max => "max",
            Elector::Min => "min",
            Elector::Top => "top",
        }
    }

    /// Whether the elector compares values, so its claims must be of a
    /// comparable type.
    pub fn compares(self) -> bool {
        matches!(self, Elector::Max | Elector::Min)
    }

    /// Whether `candidate` is elected over `incumbent`. `listed` is the
    /// attribute's `as` list, least first, which only `top` reads.
    pub fn prefers(
        self,
        listed: &[Value],
        candidate: Contender<'_>,
        incumbent: Contender<'_>,
    ) -> bool {
        match self.order(listed, candidate.0, incumbent.0) {
            Ordering::Greater => true,
            Ordering::Less => false,
            Ordering::Equal => Cardinality::newer(candidate, incumbent),
        }
    }

    /// How the elector orders two values before standing decides:
    /// `Greater` when it elects `a` over `b` on value alone, `Equal` when
    /// the value does not decide (always, under `last`).
    pub fn order(self, listed: &[Value], a: &Value, b: &Value) -> Ordering {
        match self {
            Elector::Last => Ordering::Equal,
            Elector::Top => rank(listed, a).cmp(&rank(listed, b)),
            Elector::Max => compare(a, b),
            Elector::Min => compare(b, a),
        }
    }

    /// The index of the claim elected among `claims`, `None` over none.
    pub fn elect<'a, I>(self, listed: &[Value], claims: I) -> Option<usize>
    where
        I: IntoIterator<Item = Contender<'a>>,
    {
        let mut best: Option<(usize, Contender<'a>)> = None;
        for (index, claim) in claims.into_iter().enumerate() {
            best = Some(match best {
                Some(incumbent) if !self.prefers(listed, claim, incumbent.1) => incumbent,
                _ => (index, claim),
            });
        }
        best.map(|(index, _)| index)
    }

    /// Whether `value` is the greatest any claim can hold, so no other
    /// claim can be elected over it on value: `top`'s last listed value.
    /// Unbounded orders (`max`, `min` over numbers) and `last` have none.
    pub fn bounds(self, listed: &[Value], value: &Value) -> bool {
        matches!(self, Elector::Top) && listed.last() == Some(value)
    }
}

/// Where `value` ranks in `listed`, least first: one past its position,
/// and zero for a value the list does not hold.
fn rank(listed: &[Value], value: &Value) -> usize {
    listed
        .iter()
        .position(|candidate| candidate == value)
        .map_or(0, |position| position + 1)
}

/// The order of two values, falling back on their encoding for values
/// the comparable order does not relate, so the order stays total.
fn compare(a: &Value, b: &Value) -> Ordering {
    a.partial_cmp(b)
        .unwrap_or_else(|| a.tiebreak().cmp(&b.tiebreak()))
}

impl Display for Elector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for Elector {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        match name {
            "last" => Ok(Elector::Last),
            "max" => Ok(Elector::Max),
            "min" => Ok(Elector::Min),
            "top" => Ok(Elector::Top),
            other => Err(format!(
                "`{other}` is not an elector: one of `last`, `max`, `min` or `top`"
            )),
        }
    }
}

impl Serialize for Elector {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

impl<'de> Deserialize<'de> for Elector {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        name.parse().map_err(D::Error::custom)
    }
}

/// An elector with the values its attribute lists, least first: what a
/// read of a cell needs to elect one of its claims.
#[derive(Debug, Clone, Serialize)]
pub struct Ranking {
    /// The elector.
    pub elector: Elector,
    /// The attribute's listed values, least first; `top` ranks by them.
    pub listed: Vec<Value>,
}

impl Ranking {
    /// `elector` over an attribute listing `listed`.
    pub fn new(elector: Elector, listed: Vec<Value>) -> Self {
        Ranking { elector, listed }
    }

    /// How the ranking orders two values before standing decides (see
    /// [`Elector::order`]).
    pub fn order(&self, a: &Value, b: &Value) -> Ordering {
        self.elector.order(&self.listed, a, b)
    }

    fn encoded(&self) -> Vec<Vec<u8>> {
        self.listed
            .iter()
            .map(dialog_corpus::encode_value_owned)
            .collect()
    }
}

impl PartialEq for Ranking {
    fn eq(&self, other: &Self) -> bool {
        self.elector == other.elector && self.encoded() == other.encoded()
    }
}

impl Eq for Ranking {}

impl Hash for Ranking {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.elector.hash(state);
        self.encoded().hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(value: &str) -> Value {
        Value::String(value.to_string())
    }

    #[test]
    fn top_ranks_the_last_listed_greatest() {
        let listed = [text("registered"), text("active"), text("suspended")];
        let claims = [text("active"), text("suspended"), text("registered")];
        let elected = Elector::Top.elect(&listed, claims.iter().map(|value| (value, None)));
        assert_eq!(elected, Some(1));
    }

    #[test]
    fn top_ranks_an_unlisted_value_below_every_listed_one() {
        let listed = [text("registered"), text("active")];
        let claims = [text("unknown"), text("registered")];
        let elected = Elector::Top.elect(&listed, claims.iter().map(|value| (value, None)));
        assert_eq!(elected, Some(1));
    }

    #[test]
    fn max_and_min_choose_by_value() {
        let claims = [
            Value::UnsignedInt(3),
            Value::UnsignedInt(9),
            Value::UnsignedInt(1),
        ];
        let contenders = || claims.iter().map(|value| (value, None));
        assert_eq!(Elector::Max.elect(&[], contenders()), Some(1));
        assert_eq!(Elector::Min.elect(&[], contenders()), Some(2));
    }

    #[test]
    fn an_election_ignores_the_order_of_its_claims() {
        let listed = [text("registered"), text("active"), text("suspended")];
        let mut claims = [text("active"), text("suspended"), text("registered")];
        for _ in 0..claims.len() {
            claims.rotate_left(1);
            let elected = Elector::Top
                .elect(&listed, claims.iter().map(|value| (value, None)))
                .map(|index| claims[index].clone());
            assert_eq!(elected, Some(text("suspended")));
        }
    }

    #[test]
    fn top_is_bounded_by_its_last_listed_value() {
        let listed = [text("registered"), text("suspended")];
        assert!(Elector::Top.bounds(&listed, &text("suspended")));
        assert!(!Elector::Top.bounds(&listed, &text("registered")));
        assert!(!Elector::Max.bounds(&listed, &text("suspended")));
    }

    #[test]
    fn it_round_trips_by_name() {
        for elector in [Elector::Last, Elector::Max, Elector::Min, Elector::Top] {
            let encoded = serde_json::to_string(&elector).unwrap();
            assert_eq!(serde_json::from_str::<Elector>(&encoded).unwrap(), elector);
        }
        assert!(serde_json::from_str::<Elector>("\"sum\"").is_err());
    }
}
