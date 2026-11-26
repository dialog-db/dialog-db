//! A group's fold kept as its rows come and go: a [`Running`] value.
//!
//! [`Reduce::fold_rows`] folds a group from all of its rows. A standing
//! fold that keeps a group of ten thousand rows pays all of them for one
//! row changed. A [`Running`] value is what the group's folds keep
//! instead, so that a row added or taken away is a step:
//!
//! - `count`: the number of present inputs, one more or one less.
//! - `count-distinct`: each present value's dag-cbor bytes and how many
//!   rows hold it. The count moves only when a value's multiplicity
//!   crosses zero. A set of a group's values (`distinct`) is this same
//!   multiset.
//! - `sum` over the integer band: the sum of the non-negative inputs and
//!   the sum of the negative ones, each checked. A float, a value that is
//!   not a number, an unsigned value past `i128::MAX` or a sum that
//!   overflows declines (see below): a float sum is added in sorted
//!   order to be the same bits whatever order its rows come in, which a
//!   running sum is not.
//! - `min` and `max`: the chosen input and how many rows hold exactly it.
//!   A row added is one comparison, the one [`Reduce::fold_rows`] makes;
//!   a row taken away that is not the chosen one changes nothing; the
//!   last row holding the chosen one declines. A value that does not
//!   order against the chosen one (another variant, a NaN) declines too.
//!
//! Any other fold (`avg`, and `last` and `top`, which order by standing)
//! has no running value: [`Running::new`] gives none.
//!
//! A step that declines leaves the value unusable: the caller drops it
//! and folds the group again from its rows with [`Reduce::fold_rows`],
//! which gives the answer, or the error, a fold of those rows gives.
//! What a running value gives is otherwise the row
//! [`Reduce::fold_rows`] gives for the same rows: every fold above is a
//! function of the multiset of its present inputs, and each keeps
//! exactly what that function needs.

use std::collections::HashMap;
use std::sync::Arc;

use super::{Aggregator, Input, Reduce, ReduceEntry, encode, order, prefers};
use crate::corpus::Value;
use crate::error::EvaluationError;
use crate::selection::{Binding, Match};
use crate::term::Term;
use crate::types::Any;

/// The running value of one group of a [`Reduce`]: the values of its
/// grouping fields, how many rows it folds, and what each entry keeps
/// (see the [module docs](self)).
#[derive(Debug, Clone)]
pub struct Running {
    /// The values the group's rows bind the grouping fields to, from the
    /// first row added.
    key: Option<Vec<Option<Value>>>,
    /// How many rows the value folds.
    rows: usize,
    /// What each entry keeps, in the order of the entries.
    entries: Vec<Accumulator>,
}

/// What one entry of a running value keeps.
#[derive(Debug, Clone)]
enum Accumulator {
    /// `count`: the present inputs.
    Count(u128),
    /// `count-distinct`: each present value's bytes, and how many rows
    /// hold it.
    Distinct(HashMap<Vec<u8>, usize>),
    /// `sum` over the integer band: the sum of the inputs that are not
    /// negative, and of those that are.
    Sum { positive: i128, negative: i128 },
    /// `min` or `max`: the chosen input with its bytes, and how many rows
    /// hold exactly it.
    Extreme {
        chosen: Option<(Input, Vec<u8>)>,
        held: usize,
    },
}

/// A row added to a running value, or taken away.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Step {
    Add,
    Remove,
}

impl Reduce {
    /// Whether every entry has a running value (see [`Running`]). A
    /// `max` or `min` over a listing has none: it ranks by the listing,
    /// as `top` does, and the values the listing leaves out tie and fall
    /// to standing, which a running value does not keep.
    pub fn runs(&self) -> bool {
        self.entries.iter().all(|entry| {
            matches!(
                entry.typed(),
                Aggregator::Count
                    | Aggregator::CountDistinct
                    | Aggregator::Sum
                    | Aggregator::Min
                    | Aggregator::Max
            )
        })
    }
}

impl Running {
    /// The running value of a group no row has been added to, or `None`
    /// when an entry of `reduce` has none (see [`Reduce::runs`]).
    pub fn new(reduce: &Reduce) -> Option<Self> {
        let entries = reduce
            .entries
            .iter()
            .map(|entry| match entry.typed() {
                Aggregator::Count => Some(Accumulator::Count(0)),
                Aggregator::CountDistinct => Some(Accumulator::Distinct(HashMap::new())),
                Aggregator::Sum => Some(Accumulator::Sum {
                    positive: 0,
                    negative: 0,
                }),
                Aggregator::Min | Aggregator::Max => Some(Accumulator::Extreme {
                    chosen: None,
                    held: 0,
                }),
                Aggregator::Avg | Aggregator::Last | Aggregator::Top => None,
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            key: None,
            rows: 0,
            entries,
        })
    }

    /// How many rows the value folds.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Fold `row` in, as [`Reduce::fold_rows`] would among the rest.
    /// `false` when the value cannot take it (see the [module
    /// docs](self)): the value is then unusable, and the group is folded
    /// again from its rows.
    pub fn add(&mut self, reduce: &Reduce, row: &Match) -> bool {
        self.step(reduce, row, Step::Add)
    }

    /// Take `row` out, a row added before. `false` when the value cannot
    /// say what is left without the rest of the rows: the value is then
    /// unusable, and the group is folded again from its rows.
    pub fn remove(&mut self, reduce: &Reduce, row: &Match) -> bool {
        self.step(reduce, row, Step::Remove)
    }

    fn step(&mut self, reduce: &Reduce, row: &Match, step: Step) -> bool {
        if reduce.entries.len() != self.entries.len() {
            return false;
        }
        if self.key.is_none() {
            let mut key = Vec::with_capacity(reduce.groups.len());
            for name in &reduce.groups {
                match row.lookup(&Term::<Any>::var(name.as_str())) {
                    Ok(Binding::Present(value)) => key.push(Some(value)),
                    Ok(Binding::Absent) => key.push(None),
                    Err(_) => return false,
                }
            }
            self.key = Some(key);
        }
        self.rows = match step {
            Step::Add => self.rows + 1,
            Step::Remove => match self.rows.checked_sub(1) {
                Some(rows) => rows,
                None => return false,
            },
        };
        for (entry, accumulator) in reduce.entries.iter().zip(&mut self.entries) {
            let input = match row.lookup(&entry.input) {
                Ok(Binding::Present(value)) => {
                    let standing = entry
                        .input
                        .name()
                        .and_then(|variable| row.standing_of(variable));
                    (value, standing)
                }
                // An absent input is skipped by every fold.
                Ok(Binding::Absent) => continue,
                Err(_) => return false,
            };
            if !accumulator.step(entry, input, step) {
                return false;
            }
        }
        true
    }

    /// The row [`Reduce::fold_rows`] gives for the rows added and not
    /// taken away, or `None` when no row is: a fold of no rows has no
    /// group, which the caller learns by folding them.
    pub fn finish(&self, reduce: &Reduce) -> Result<Option<Match>, EvaluationError> {
        let Some(key) = &self.key else {
            return Ok(None);
        };
        if self.rows == 0 {
            return Ok(None);
        }
        let mut row = Match::new();
        for (name, value) in reduce.groups.iter().zip(key) {
            let term = Term::<Any>::var(name.as_str());
            match value {
                Some(value) => row.bind(&term, value.clone())?,
                None => row.bind_absent(&term)?,
            }
        }
        for (entry, accumulator) in reduce.entries.iter().zip(&self.entries) {
            let out: Term<Any> = Term::var(entry.field.as_str());
            match accumulator {
                Accumulator::Count(count) => row.bind(&out, Value::UnsignedInt(*count))?,
                Accumulator::Distinct(values) => {
                    row.bind(&out, Value::UnsignedInt(values.len() as u128))?
                }
                Accumulator::Sum { positive, negative } => {
                    // One is not negative and the other is, so their sum
                    // lies between them.
                    let total = positive + negative;
                    let value = if total >= 0 {
                        Value::UnsignedInt(total as u128)
                    } else {
                        Value::SignedInt(total)
                    };
                    row.bind(&out, value)?
                }
                Accumulator::Extreme { chosen, .. } => match chosen {
                    Some(((value, standing), _)) => {
                        row.bind(&out, value.clone())?;
                        // A chosen value stands as the fact it was bound
                        // from, as a fold of the rows cites it.
                        if let Some(standing) = standing {
                            row.cite_variable_standing(
                                &Arc::from(entry.field.as_str()),
                                standing.clone(),
                            );
                        }
                    }
                    None => row.bind_absent(&out)?,
                },
            }
        }
        Ok(Some(row))
    }
}

impl Accumulator {
    /// Add one present input, or take it away; `false` when this
    /// accumulator cannot.
    fn step(&mut self, entry: &ReduceEntry, input: Input, step: Step) -> bool {
        match self {
            Accumulator::Count(count) => {
                *count = match step {
                    Step::Add => match count.checked_add(1) {
                        Some(count) => count,
                        None => return false,
                    },
                    Step::Remove => match count.checked_sub(1) {
                        Some(count) => count,
                        None => return false,
                    },
                };
                true
            }
            Accumulator::Distinct(values) => {
                let bytes = input.0.identity_key();
                match step {
                    Step::Add => *values.entry(bytes).or_insert(0) += 1,
                    Step::Remove => match values.get_mut(&bytes) {
                        Some(held) if *held > 1 => *held -= 1,
                        Some(_) => {
                            values.remove(&bytes);
                        }
                        None => return false,
                    },
                }
                true
            }
            Accumulator::Sum { positive, negative } => {
                let value = match input.0 {
                    Value::UnsignedInt(value) => match i128::try_from(value) {
                        Ok(value) => value,
                        Err(_) => return false,
                    },
                    Value::SignedInt(value) => value,
                    _ => return false,
                };
                let side = if value >= 0 { positive } else { negative };
                let next = match step {
                    Step::Add => side.checked_add(value),
                    Step::Remove => side.checked_sub(value),
                };
                match next {
                    Some(next) => {
                        *side = next;
                        true
                    }
                    None => false,
                }
            }
            Accumulator::Extreme { chosen, held } => {
                let Ok(bytes) = encode(&input.0) else {
                    return false;
                };
                let Some(((value, standing), chosen_bytes)) = chosen.as_ref() else {
                    return match step {
                        Step::Add => {
                            *chosen = Some((input, bytes));
                            *held = 1;
                            true
                        }
                        Step::Remove => false,
                    };
                };
                // Every value kept orders against the chosen one, so the
                // group's values are all of one variant and a fold of its
                // rows refuses none of them.
                if order(&input.0, value).is_none() {
                    return false;
                }
                let candidate = (&input.0, input.1.as_ref());
                let incumbent = (value, standing.as_ref());
                let ahead = prefers(entry, candidate, incumbent);
                let behind = prefers(entry, incumbent, candidate);
                // What a fold of the rows keeps between the two: its
                // choice over every row is the greatest under one total
                // order (the value, its standing, then its bytes), so
                // taking rows one at a time chooses the same.
                let same = !ahead && !behind && bytes == *chosen_bytes;
                let wins = ahead || (!behind && bytes < *chosen_bytes);
                match step {
                    Step::Add if same => *held += 1,
                    Step::Add if wins => {
                        *chosen = Some((input, bytes));
                        *held = 1;
                    }
                    Step::Add => {}
                    Step::Remove if same => {
                        *held -= 1;
                        // The last row holding the choice is gone: the
                        // next choice is among rows not kept here.
                        return *held > 0;
                    }
                    // A row the choice was preferred over: the choice
                    // stands. One preferred over the choice was never
                    // added.
                    Step::Remove => return !wins,
                }
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::reduce::ReduceEntry;

    fn row(group: &str, value: Option<Value>) -> Match {
        let mut row = Match::new();
        row.bind(&Term::<Any>::var("g"), Value::String(group.into()))
            .expect("binds");
        match value {
            Some(value) => row.bind(&Term::<Any>::var("x"), value).expect("binds"),
            None => row.bind_absent(&Term::<Any>::var("x")).expect("binds"),
        }
        row
    }

    fn reduce(aggregator: Aggregator) -> Reduce {
        Reduce::new(
            vec!["g".into()],
            vec![ReduceEntry::new("out", aggregator, Term::var("x"))],
        )
    }

    /// The fold of `rows`, as [`Reduce::fold_rows`] gives it.
    fn folded(reduce: &Reduce, rows: &[Match]) -> Result<Option<Binding>, EvaluationError> {
        reduce
            .clone()
            .fold_rows(rows)?
            .first()
            .map(|row| row.lookup(&Term::<Any>::var("out")))
            .transpose()
    }

    fn running(reduce: &Reduce, running: &Running) -> Result<Option<Binding>, EvaluationError> {
        running
            .finish(reduce)?
            .map(|row| row.lookup(&Term::<Any>::var("out")))
            .transpose()
    }

    /// A small deterministic generator (xorshift64).
    struct Random(u64);

    impl Random {
        fn below(&mut self, n: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            (self.0 % n as u64) as usize
        }
    }

    /// Over random adds and removes of rows, a value repeated among them
    /// and an absent input among them, a running value that takes every
    /// step gives what a fold of the rows left gives; one that declines a
    /// step is built again from the rows, as a caller does, and the
    /// rebuilt one gives it too. Each fold that runs, over values that
    /// order and over values a fold refuses to order.
    #[dialog_common::test]
    fn it_runs_each_fold_as_a_fold_of_its_rows() -> anyhow::Result<()> {
        let values = [
            vec![
                Some(Value::UnsignedInt(3)),
                Some(Value::UnsignedInt(3)),
                Some(Value::SignedInt(-7)),
                Some(Value::UnsignedInt(0)),
                Some(Value::UnsignedInt(12)),
                None,
            ],
            vec![
                Some(Value::String("home".into())),
                Some(Value::String("errand".into())),
                Some(Value::String("work".into())),
                None,
            ],
            vec![
                Some(Value::UnsignedInt(u128::MAX)),
                Some(Value::Float(0.5)),
                Some(Value::UnsignedInt(1)),
            ],
        ];
        let aggregators = [
            Aggregator::Count,
            Aggregator::CountDistinct,
            Aggregator::Sum,
            Aggregator::Min,
            Aggregator::Max,
        ];
        let mut random = Random(0x5eed_0001);
        let mut stepped = 0;
        for aggregator in aggregators {
            let reduce = reduce(aggregator);
            for values in &values {
                let mut rows: Vec<Match> = Vec::new();
                let mut value = Running::new(&reduce);
                for step in 0..300 {
                    let add = rows.is_empty() || random.below(3) != 0;
                    let changed = if add {
                        let added = row("a", values[random.below(values.len())].clone());
                        rows.push(added.clone());
                        added
                    } else {
                        rows.swap_remove(random.below(rows.len()))
                    };
                    let took = match value.as_mut() {
                        Some(value) if add => value.add(&reduce, &changed),
                        Some(value) => value.remove(&reduce, &changed),
                        None => false,
                    };
                    if !took {
                        let mut rebuilt = Running::new(&reduce);
                        for row in &rows {
                            if let Some(value) = rebuilt.as_mut()
                                && !value.add(&reduce, row)
                            {
                                rebuilt = None;
                            }
                        }
                        value = rebuilt;
                    } else {
                        stepped += 1;
                    }
                    if let Some(value) = &value {
                        let expected = folded(&reduce, &rows);
                        match running(&reduce, value)? {
                            Some(binding) => assert_eq!(
                                Ok(Some(binding)),
                                expected,
                                "{aggregator}, step {step}: the running value"
                            ),
                            None => assert!(rows.is_empty(), "no row, no group"),
                        }
                    }
                }
            }
        }
        assert!(stepped > 1000, "{stepped} steps taken: the test ran little");
        Ok(())
    }

    /// `count-distinct`: the last row holding a value leaves and the
    /// count drops by one; one of two rows holding a value leaves and
    /// the count stays.
    /// A running count of distinct values counts one number held as
    /// two types once, as a fold of the rows does.
    #[dialog_common::test]
    fn it_counts_one_number_of_two_types_once() -> anyhow::Result<()> {
        let reduce = reduce(Aggregator::CountDistinct);
        let mut value = Running::new(&reduce).expect("count-distinct runs");
        for row in [
            row("a", Some(Value::UnsignedInt(3))),
            row("a", Some(Value::Float(3.0))),
        ] {
            assert!(value.add(&reduce, &row));
        }
        assert_eq!(
            running(&reduce, &value)?,
            Some(Binding::Present(Value::UnsignedInt(1)))
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_counts_a_value_until_its_last_row_leaves() -> anyhow::Result<()> {
        let reduce = reduce(Aggregator::CountDistinct);
        let home = row("a", Some(Value::String("home".into())));
        let work = row("a", Some(Value::String("work".into())));
        let mut value = Running::new(&reduce).expect("count-distinct runs");
        for row in [&home, &home, &work] {
            assert!(value.add(&reduce, row));
        }
        let count = |value: &Running| running(&reduce, value);
        assert_eq!(
            count(&value)?,
            Some(Binding::Present(Value::UnsignedInt(2)))
        );
        assert!(value.remove(&reduce, &home), "one of two rows of `home`");
        assert_eq!(
            count(&value)?,
            Some(Binding::Present(Value::UnsignedInt(2)))
        );
        assert!(value.remove(&reduce, &home), "the last row of `home`");
        assert_eq!(
            count(&value)?,
            Some(Binding::Present(Value::UnsignedInt(1)))
        );
        Ok(())
    }

    /// `max`: a row taken away below the maximum changes nothing, one of
    /// two rows holding the maximum leaves it standing, and the last row
    /// holding it declines, since the next maximum is among rows a
    /// running value does not keep.
    #[dialog_common::test]
    fn it_declines_when_the_last_row_of_the_maximum_leaves() -> anyhow::Result<()> {
        let reduce = reduce(Aggregator::Max);
        let three = row("a", Some(Value::UnsignedInt(3)));
        let nine = row("a", Some(Value::UnsignedInt(9)));
        let mut value = Running::new(&reduce).expect("max runs");
        for row in [&three, &nine, &nine] {
            assert!(value.add(&reduce, row));
        }
        let max = |value: &Running| running(&reduce, value);
        assert!(value.remove(&reduce, &three), "below the maximum");
        assert_eq!(max(&value)?, Some(Binding::Present(Value::UnsignedInt(9))));
        assert!(value.remove(&reduce, &nine), "one of two rows of it");
        assert_eq!(max(&value)?, Some(Binding::Present(Value::UnsignedInt(9))));
        assert!(!value.remove(&reduce, &nine), "the last row of it");
        Ok(())
    }

    /// `avg`, `last` and `top` have no running value.
    #[dialog_common::test]
    fn it_runs_no_average_and_nothing_ordered_by_standing() {
        for aggregator in [Aggregator::Avg, Aggregator::Last, Aggregator::Top] {
            assert!(!reduce(aggregator).runs());
            assert!(Running::new(&reduce(aggregator)).is_none());
        }
    }

    /// "A `max` or `min` over a listing has no running value": it ranks
    /// by the listing, and the values it leaves out fall to standing.
    /// The same fold over no listing has one.
    #[dialog_common::test]
    fn it_runs_no_max_or_min_over_a_listing() {
        for aggregator in [Aggregator::Min, Aggregator::Max] {
            let mut entry = ReduceEntry::new("out", aggregator, Term::var("x"));
            entry.listed = vec![Value::String("low".into()), Value::String("high".into())];
            let listed = Reduce::new(vec!["g".into()], vec![entry]);
            assert!(!listed.runs(), "{aggregator}");
            assert!(Running::new(&listed).is_none(), "{aggregator}");
            assert!(reduce(aggregator).runs(), "{aggregator}");
            assert!(Running::new(&reduce(aggregator)).is_some(), "{aggregator}");
        }
    }
}
