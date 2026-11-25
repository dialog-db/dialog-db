//! Selective aggregators: how a concept field reads one of its
//! relation's claims.
//!
//! An aggregator is *selective* when its result is always one of its
//! inputs (`x ⊕ y ∈ {x, y}`), as `max` and `min` are; a *computing*
//! aggregator, `count`, `sum` or `avg`, produces a value none of its
//! inputs holds. A concept field may declare a selective aggregator to
//! choose one claim of its cell: the attribute says what a write does
//! (its cardinality), the aggregator which claim a read returns. Each is
//! the greatest claim under a total order, so every replica holding the
//! same claims reads the same one:
//!
//! | aggregator | orders by                                                      |
//! |------------|----------------------------------------------------------------|
//! | `last`     | standing: the newest claim                                     |
//! | `max`      | value, greatest first, then standing; over a listed `as`, the last listed first |
//! | `min`      | value, least first, then standing; over a listed `as`, the first listed first   |
//! | `top`      | the attribute's listed `as`, the first listed first, then standing |
//!
//! A listed `as` is a scale written least first, `[low, medium, high]`:
//! `max` over it reads the claim holding the last listed value present,
//! `min` the first, whatever the values' own order. A value the listing
//! does not hold loses to every listed one under all three.
//!
//! `top` is deprecated and goes in the next version: it reads the first
//! listed value present, which is `min` over the same list, and a list
//! written least first is read with `max`. It is still read, so the
//! concepts already stored with it keep their meaning and identity.
//!
//! Ties fall to standing and then to the value's hash
//! ([`Cardinality::newer`]).

use std::cmp::Ordering;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use dialog_corpus::{Cardinality, Contender};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Value;

/// A selective aggregator a concept field reads its claims with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Selective {
    /// The newest claim: what a field with no aggregator reads under
    /// cardinality one.
    Last,
    /// The claim holding the greatest value; over an attribute listing
    /// its values, the value listed last.
    Max,
    /// The claim holding the least value; over an attribute listing its
    /// values, the value listed first.
    Min,
    /// Deprecated, and gone in the next version: the claim holding the
    /// value listed first in the attribute's `as`, which is `min` over
    /// the same list (a list written least first is read with `max`). A
    /// value the list does not hold ranks below every listed one. Not
    /// marked `#[deprecated]`, which would cost an
    /// `#[allow(deprecated)]` on every match that still reads it.
    Top,
}

impl Selective {
    /// The aggregator's name as the notation spells it.
    pub fn name(self) -> &'static str {
        match self {
            Selective::Last => "last",
            Selective::Max => "max",
            Selective::Min => "min",
            Selective::Top => "top",
        }
    }

    /// Whether the aggregator compares values, so its claims must be of a
    /// comparable type, when the attribute lists none (`listed`): over a
    /// listing `max` and `min` rank by it, as `top` does.
    pub fn compares(self, listed: &[Value]) -> bool {
        matches!(self, Selective::Max | Selective::Min) && listed.is_empty()
    }

    /// Whether `candidate` is elected over `incumbent`. `listed` is the
    /// attribute's `as` list, least first, which `top` reads, and `max`
    /// and `min` when it lists anything.
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

    /// How the aggregator orders two values before standing decides:
    /// `Greater` when it elects `a` over `b` on value alone, `Equal` when
    /// the value does not decide (always, under `last`).
    pub fn order(self, listed: &[Value], a: &Value, b: &Value) -> Ordering {
        match self {
            Selective::Last => Ordering::Equal,
            Selective::Top => first(listed, a).cmp(&first(listed, b)),
            Selective::Min if !listed.is_empty() => first(listed, a).cmp(&first(listed, b)),
            Selective::Max if !listed.is_empty() => last(listed, a).cmp(&last(listed, b)),
            Selective::Max => compare(a, b),
            Selective::Min => compare(b, a),
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
}

/// Where `value` ranks in `listed` when the first listed is elected:
/// the greater the rank the earlier it is listed, and zero for a value
/// the list does not hold.
fn first(listed: &[Value], value: &Value) -> usize {
    listed
        .iter()
        .position(|candidate| candidate == value)
        .map_or(0, |position| listed.len() - position)
}

/// Where `value` ranks in `listed` when the last listed is elected: the
/// greater the rank the later it is listed, and zero for a value the
/// list does not hold.
fn last(listed: &[Value], value: &Value) -> usize {
    listed
        .iter()
        .position(|candidate| candidate == value)
        .map_or(0, |position| position + 1)
}

/// The order of two values, falling back on their encoding for values
/// the comparable order does not relate, so the order stays total. Two
/// numbers of different types order by value ([`Value::cmp_number`]),
/// numbers being one stratum: a float does not outrank every natural.
/// Values of one type, and values of different types that are not both
/// numbers, order as [`Value`]'s own order does: by type first, which
/// only a field of no type holding several types meets.
fn compare(a: &Value, b: &Value) -> Ordering {
    if a.data_type() != b.data_type()
        && let Some(order) = a.cmp_number(b)
    {
        // One number held as two types (the natural 3 and the float
        // 3.0) orders by type, so which of the two a pick returns does
        // not depend on the order it meets them in: their standing may
        // be equal, and their hash ([`Value::tiebreak`]) is the band's
        // identity, equal too.
        return order.then_with(|| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    }
    a.partial_cmp(b)
        .unwrap_or_else(|| a.tiebreak().cmp(&b.tiebreak()))
}

impl Display for Selective {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for Selective {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        match name {
            "last" => Ok(Selective::Last),
            "max" => Ok(Selective::Max),
            "min" => Ok(Selective::Min),
            "top" => Ok(Selective::Top),
            other => Err(format!(
                "`{other}` is not a selective aggregator: one of `last`, `max`, `min` or `top`"
            )),
        }
    }
}

impl Serialize for Selective {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

impl<'de> Deserialize<'de> for Selective {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        name.parse().map_err(D::Error::custom)
    }
}

/// A selective aggregator with the values its attribute lists, least
/// first: what a read of a cell needs to elect one of its claims.
#[derive(Debug, Clone, Serialize)]
pub struct Ranking {
    /// The aggregator.
    pub aggregate: Selective,
    /// The attribute's listed values, least first; `top` ranks by them,
    /// and `max` and `min` when there are any.
    pub listed: Vec<Value>,
}

impl Ranking {
    /// `aggregate` over an attribute listing `listed`.
    pub fn new(aggregate: Selective, listed: Vec<Value>) -> Self {
        Ranking { aggregate, listed }
    }

    /// How the ranking orders two values before standing decides (see
    /// [`Selective::order`]).
    pub fn order(&self, a: &Value, b: &Value) -> Ordering {
        self.aggregate.order(&self.listed, a, b)
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
        self.aggregate == other.aggregate && self.encoded() == other.encoded()
    }
}

impl Eq for Ranking {}

impl Hash for Ranking {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.aggregate.hash(state);
        self.encoded().hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One number held as two types (the natural `3` and the float
    /// `3.0`) is one value to a pick: their value, standing and hash are
    /// all equal, and the election's tie takes the narrowest type, as a
    /// read of the band yields, whichever comes first and for `max`,
    /// `min` and `last` alike. The two zeros resolve the same way, `0.0`
    /// over `-0.0`, and the natural `0` over either. Numbers of
    /// different values order by value.
    #[dialog_common::test]
    fn it_picks_one_number_of_two_types_the_same_in_either_order() {
        let natural = Value::UnsignedInt(3);
        let integer = Value::SignedInt(3);
        let float = Value::Float(3.0);
        assert_eq!(natural.tiebreak(), float.tiebreak(), "one number, one hash");
        let cases = [
            (&natural, &float, &natural),
            (&natural, &integer, &natural),
            (&integer, &float, &integer),
            (&Value::Float(0.0), &Value::Float(-0.0), &Value::Float(0.0)),
            (
                &Value::UnsignedInt(0),
                &Value::Float(-0.0),
                &Value::UnsignedInt(0),
            ),
        ];
        for aggregate in [Selective::Max, Selective::Min, Selective::Last] {
            for (a, b, expected) in cases {
                let picked = |first: &Value, second: &Value| {
                    if aggregate.prefers(&[], (second, None), (first, None)) {
                        second.clone()
                    } else {
                        first.clone()
                    }
                };
                assert_eq!(&picked(a, b), expected, "{aggregate:?}, {a:?} first");
                assert_eq!(&picked(b, a), expected, "{aggregate:?}, {b:?} first");
            }
        }
        assert!(Selective::Max.prefers(&[], (&Value::UnsignedInt(4), None), (&float, None)));
        assert!(Selective::Min.prefers(&[], (&Value::Float(2.5), None), (&natural, None)));
    }
    use dialog_corpus::history::Edition;
    use dialog_corpus::{Cause, Standing};

    fn text(value: &str) -> Value {
        Value::String(value.to_string())
    }

    #[test]
    fn top_ranks_the_first_listed_best() {
        let listed = [text("suspended"), text("active"), text("registered")];
        let claims = [text("active"), text("suspended"), text("registered")];
        let elected = Selective::Top.elect(&listed, claims.iter().map(|value| (value, None)));
        assert_eq!(elected, Some(1));
    }

    #[test]
    fn top_ranks_an_unlisted_value_below_every_listed_one() {
        let listed = [text("active"), text("registered")];
        let claims = [text("unknown"), text("registered")];
        let elected = Selective::Top.elect(&listed, claims.iter().map(|value| (value, None)));
        assert_eq!(elected, Some(1));
    }

    fn standing(edition: u8) -> Standing {
        Standing::new(
            Some((Edition::new(u64::from(edition)), [0; 32])),
            Cause([edition; 32]),
        )
    }

    #[test]
    fn claims_holding_one_listed_value_fall_to_the_newest() {
        let listed = [text("low"), text("medium"), text("high")];
        let (older, newer) = (standing(3), standing(4));
        for (aggregate, value) in [
            (Selective::Top, text("low")),
            (Selective::Min, text("low")),
            (Selective::Max, text("high")),
        ] {
            let claims = [
                (&value, Some(&older)),
                (&value, Some(&newer)),
                (&listed[1], Some(&newer)),
            ];
            assert_eq!(aggregate.elect(&listed, claims), Some(1), "{aggregate}");
            let reversed = [claims[1], claims[0], claims[2]];
            assert_eq!(aggregate.elect(&listed, reversed), Some(0), "{aggregate}");
        }
    }

    #[test]
    fn a_cell_holding_no_listed_value_falls_to_the_newest() {
        let listed = [text("low"), text("medium"), text("high")];
        let (older, newer) = (standing(3), standing(4));
        let (urgent, later) = (text("urgent"), text("later"));
        let claims = [(&urgent, Some(&newer)), (&later, Some(&older))];
        for aggregate in [Selective::Top, Selective::Min, Selective::Max] {
            assert_eq!(aggregate.elect(&listed, claims), Some(0), "{aggregate}");
        }
    }

    #[test]
    fn max_and_min_choose_by_value() {
        let claims = [
            Value::UnsignedInt(3),
            Value::UnsignedInt(9),
            Value::UnsignedInt(1),
        ];
        let contenders = || claims.iter().map(|value| (value, None));
        assert_eq!(Selective::Max.elect(&[], contenders()), Some(1));
        assert_eq!(Selective::Min.elect(&[], contenders()), Some(2));
    }

    #[test]
    fn an_election_ignores_the_order_of_its_claims() {
        let listed = [text("suspended"), text("active"), text("registered")];
        let mut claims = [text("active"), text("suspended"), text("registered")];
        for _ in 0..claims.len() {
            claims.rotate_left(1);
            let elected = Selective::Top
                .elect(&listed, claims.iter().map(|value| (value, None)))
                .map(|index| claims[index].clone());
            assert_eq!(elected, Some(text("suspended")));
        }
    }

    #[test]
    fn max_and_min_over_a_listing_rank_by_it_least_first() {
        let listed = [text("low"), text("medium"), text("high")];
        // Bytewise `medium` is the greatest and `high` the least.
        let claims = [text("medium"), text("high"), text("low")];
        let contenders = || claims.iter().map(|value| (value, None));
        assert_eq!(Selective::Max.elect(&listed, contenders()), Some(1));
        assert_eq!(Selective::Min.elect(&listed, contenders()), Some(2));
        assert_eq!(
            Selective::Min.elect(&listed, contenders()),
            Selective::Top.elect(&listed, contenders())
        );
    }

    #[test]
    fn min_over_a_listing_reads_what_max_over_it_reversed_reads() {
        let listed = [text("a"), text("b"), text("c")];
        let reversed = [text("c"), text("b"), text("a")];
        for claims in [
            vec![text("b"), text("c"), text("a")],
            vec![text("c"), text("b")],
            vec![text("zzz"), text("c")],
        ] {
            let contenders = || claims.iter().map(|value| (value, None));
            assert_eq!(
                Selective::Min.elect(&listed, contenders()),
                Selective::Max.elect(&reversed, contenders()),
                "{claims:?}"
            );
        }
    }

    #[test]
    fn max_and_min_over_a_listing_rank_an_unlisted_value_below_every_listed_one() {
        let listed = [text("low"), text("medium"), text("high")];
        let claims = [text("unknown"), text("medium"), text("zzz")];
        let contenders = || claims.iter().map(|value| (value, None));
        for aggregate in [Selective::Max, Selective::Min, Selective::Top] {
            assert_eq!(
                aggregate.elect(&listed, contenders()),
                Some(1),
                "{aggregate}"
            );
        }
    }

    #[test]
    fn it_round_trips_by_name() {
        for aggregate in [
            Selective::Last,
            Selective::Max,
            Selective::Min,
            Selective::Top,
        ] {
            let encoded = serde_json::to_string(&aggregate).unwrap();
            assert_eq!(
                serde_json::from_str::<Selective>(&encoded).unwrap(),
                aggregate
            );
        }
        assert!(serde_json::from_str::<Selective>("\"sum\"").is_err());
    }
}
