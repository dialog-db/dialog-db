//! A merge join over two streams sorted on a shared variable.
//!
//! The nested-loop join this codebase uses today feeds one scan's rows into
//! the other, issuing a fresh index probe per row (see
//! [`AttributeQueryAll::evaluate`](crate::attribute::query::all::AttributeQueryAll)).
//! A merge join instead evaluates both sides *independently* and correlates
//! their outputs by walking two sorted cursors in lockstep, so neither side is
//! re-scanned per row of the other.
//!
//! It applies only when both inputs are already sorted on the join variable
//! (see [`SortOrder`](crate::SortOrder)). This module is the operator alone;
//! nothing plans it yet. It exists to be proven correct against the nested-loop
//! join as an oracle before the planner learns to emit it.
//!
//! # The join key is the caller's, not the operator's
//!
//! Rows arrive already ordered because the scans read an ordered index, and
//! that order is the index's *key encoding*, which is not the same as any
//! `Ord` on the bound value. So the operator does not derive the ordering
//! itself: the caller supplies a `key` function mapping each row to a
//! comparable key in the *same* order both scans produced. The operator then
//! only assumes the two streams agree on that ordering and compares keys with
//! their `Ord`. This keeps the index-encoding concern in the scan layer, where
//! it belongs, and out of the join.

use std::cmp::Ordering;
use std::pin::Pin;

use futures_util::StreamExt;
use futures_util::stream::Peekable;

use dialog_common::ConditionalSync;

use crate::error::EvaluationError;
use crate::selection::{Match, Selection};
use crate::try_stream;

/// Joins two selections sorted on a shared variable by merging their sorted
/// runs.
///
/// `key` maps a row to the value it is ordered by; both `left` and `right` must
/// already yield rows in ascending `key` order (the caller establishes this
/// from the scans' [`SortOrder`](crate::SortOrder)). A row for which `key`
/// returns `None` does not participate and is dropped, which is how a row that
/// fails to bind the join variable is handled.
///
/// For each key present on both sides, every left row with that key is combined
/// (via [`Match::combine`]) with every right row with that key. Combinations
/// that disagree on some other shared variable are filtered out. A key on only
/// one side contributes nothing, exactly as an inner join.
///
/// The result carries no ordering guarantee: within an equal-key group the
/// cross product is emitted in an unspecified order, so a join that must feed a
/// further merge would need its output order re-established.
pub fn merge_join<'a, L, R, K, Key>(left: L, right: R, key: K) -> impl Selection + 'a
where
    L: Selection + 'a,
    R: Selection + 'a,
    K: Fn(&Match) -> Option<Key> + ConditionalSync + 'a,
    Key: Ord + Clone + ConditionalSync + 'a,
{
    try_stream! {
        let mut left = Box::pin(left.peekable());
        let mut right = Box::pin(right.peekable());

        // The current equal-key run buffered from the right side, reused across
        // every left row sharing its key so the right side is read once even in
        // the many-to-many case.
        let mut right_run: Vec<Match> = Vec::new();
        let mut right_run_key: Option<Key> = None;

        while let Some((left_row, left_key)) = next_keyed(&mut left, &key).await? {
            // Advance the right cursor until its buffered run covers left_key,
            // or has moved past it. When the run already holds left_key, reuse
            // it untouched.
            if right_run_key.as_ref() != Some(&left_key) {
                advance_right_to(&mut right, &key, &left_key, &mut right_run, &mut right_run_key)
                    .await?;
            }

            if right_run_key.as_ref() == Some(&left_key) {
                for right_row in &right_run {
                    if let Some(joined) = left_row.clone().combine(right_row) {
                        yield joined;
                    }
                }
            }
        }
    }
}

/// Pull the next row with a key, returning it paired with that key. Rows whose
/// `key` is `None` are skipped.
async fn next_keyed<S, K, Key>(
    stream: &mut Pin<Box<Peekable<S>>>,
    key: &K,
) -> Result<Option<(Match, Key)>, EvaluationError>
where
    S: Selection,
    K: Fn(&Match) -> Option<Key>,
{
    while let Some(item) = stream.next().await {
        let row = item?;
        if let Some(k) = key(&row) {
            return Ok(Some((row, k)));
        }
    }
    Ok(None)
}

/// Advance the right cursor until the buffered run holds the rows whose key is
/// `target`, or the right side has moved to a key past `target` (which leaves
/// the run holding that later group, so a subsequent larger left key can reuse
/// it, or an empty run if the right side is exhausted).
///
/// Both sides are sorted ascending, so once a right key reaches or exceeds
/// `target` no earlier right row can match it; keys strictly below `target` are
/// discarded as they pass.
async fn advance_right_to<S, K, Key>(
    right: &mut Pin<Box<Peekable<S>>>,
    key: &K,
    target: &Key,
    run: &mut Vec<Match>,
    run_key: &mut Option<Key>,
) -> Result<(), EvaluationError>
where
    S: Selection,
    K: Fn(&Match) -> Option<Key>,
    Key: Ord + Clone,
{
    // A buffered run at or past target is already the answer: either it is
    // target's group, or target has no group and this is the next one.
    if let Some(current) = run_key.as_ref()
        && *current >= *target
    {
        return Ok(());
    }

    loop {
        let Some((row, k)) = next_keyed(right, key).await? else {
            run.clear();
            *run_key = None;
            return Ok(());
        };

        match k.cmp(target) {
            Ordering::Less => continue,
            // At target, or overshot it: buffer this key's whole group. The
            // overshoot case leaves a run the next larger left key may reuse.
            Ordering::Equal | Ordering::Greater => {
                run.clear();
                run.push(row);
                *run_key = Some(k.clone());
                buffer_equal_run(right, key, &k, run).await?;
                return Ok(());
            }
        }
    }
}

/// Extend `run` with every subsequent right row whose key equals `key_value`,
/// leaving the first differing row buffered in the peekable stream for the next
/// call.
async fn buffer_equal_run<S, K, Key>(
    right: &mut Pin<Box<Peekable<S>>>,
    key: &K,
    key_value: &Key,
    run: &mut Vec<Match>,
) -> Result<(), EvaluationError>
where
    S: Selection,
    K: Fn(&Match) -> Option<Key>,
    Key: Ord,
{
    loop {
        match right.as_mut().peek().await {
            Some(Ok(row)) => match key(row) {
                Some(next_key) if next_key == *key_value => {
                    let row = right.next().await.expect("peek observed a row")?;
                    run.push(row);
                }
                _ => return Ok(()),
            },
            // A pending error is surfaced when `next_keyed` next polls the
            // stream; leave it in place.
            Some(Err(_)) => return Ok(()),
            None => return Ok(()),
        }
    }
}

/// One input to [`multi_merge_join`]: a sorted selection advanced as an
/// equal-key group buffer.
struct Cursor<S: Selection, Key> {
    stream: Pin<Box<Peekable<S>>>,
    /// The buffered group for the cursor's current key.
    run: Vec<Match>,
    /// The current key, or `None` once the stream is exhausted.
    key: Option<Key>,
}

impl<S, Key> Cursor<S, Key>
where
    S: Selection,
    Key: Ord + Clone,
{
    async fn new<K>(stream: S, key_of: &K) -> Result<Self, EvaluationError>
    where
        K: Fn(&Match) -> Option<Key>,
    {
        let mut cursor = Self {
            stream: Box::pin(stream.peekable()),
            run: Vec::new(),
            key: None,
        };
        cursor.load_next_group(key_of).await?;
        Ok(cursor)
    }

    /// Replace the buffered group with the next key's group (the smallest key
    /// strictly greater than the current one), leaving the cursor exhausted if
    /// the stream ends.
    async fn load_next_group<K>(&mut self, key_of: &K) -> Result<(), EvaluationError>
    where
        K: Fn(&Match) -> Option<Key>,
    {
        self.run.clear();
        let Some((row, key)) = next_keyed(&mut self.stream, key_of).await? else {
            self.key = None;
            return Ok(());
        };
        self.run.push(row);
        buffer_equal_run(&mut self.stream, key_of, &key, &mut self.run).await?;
        self.key = Some(key);
        Ok(())
    }

    /// Advance until the cursor's key is at least `target`, skipping groups
    /// below it. Returns after loading the first group whose key `>= target`,
    /// or on exhaustion.
    async fn advance_to<K>(&mut self, key_of: &K, target: &Key) -> Result<(), EvaluationError>
    where
        K: Fn(&Match) -> Option<Key>,
    {
        while let Some(current) = self.key.as_ref() {
            if current >= target {
                return Ok(());
            }
            self.load_next_group(key_of).await?;
        }
        Ok(())
    }
}

/// Intersects N selections all sorted on the same shared variable, joining
/// every N-way group whose key is present on *every* input.
///
/// This is the single-variable case of a worst-case-optimal join: all N
/// cursors advance in lockstep on one key, so an entity absent from any one
/// input is skipped in a single pass across all inputs, and no intermediate
/// join is ever materialized. A concept's implicit rule (one `(this, attr, v)`
/// premise per field, all sharing `this`) is exactly this shape.
///
/// Every input must yield rows in ascending `key` order under the same
/// encoding (the caller establishes this from each scan's
/// [`SortOrder`](crate::SortOrder)). An empty input list yields nothing. A
/// single input passes its rows through, keyed. Rows within an equal-key group
/// are combined as a cross product across all inputs, and any combination that
/// disagrees on a non-key variable is dropped, exactly as the binary
/// [`merge_join`].
///
/// This is an *inner* intersection: an input that lacks the key contributes
/// nothing, so an entity missing any required attribute is excluded. Optional
/// (left-join) attributes are therefore NOT handled here; a concept with
/// optional fields cannot use this directly without widening the missing side.
pub fn multi_merge_join<'a, S, K, Key>(inputs: Vec<S>, key: K) -> impl Selection + 'a
where
    S: Selection + 'a,
    K: Fn(&Match) -> Option<Key> + ConditionalSync + 'a,
    Key: Ord + Clone + ConditionalSync + 'a,
{
    try_stream! {
        if inputs.is_empty() {
            return;
        }

        let mut cursors = Vec::with_capacity(inputs.len());
        for input in inputs {
            cursors.push(Cursor::new(input, &key).await?);
        }

        // Lockstep intersection: repeatedly bring every cursor up to the
        // current maximum key. When they all agree, that key is in every
        // input; emit its N-way cross product and step all cursors past it.
        loop {
            // The largest current key across all cursors, or stop if any
            // cursor is exhausted (no further key can be in every input).
            let mut max: Option<Key> = None;
            let mut exhausted = false;
            for cursor in &cursors {
                match cursor.key.as_ref() {
                    None => {
                        exhausted = true;
                        break;
                    }
                    Some(k) => {
                        if max.as_ref().is_none_or(|m| k > m) {
                            max = Some(k.clone());
                        }
                    }
                }
            }
            if exhausted {
                break;
            }
            let max = max.expect("non-empty, non-exhausted cursors have a max");

            // Pull every cursor up to max.
            for cursor in &mut cursors {
                cursor.advance_to(&key, &max).await?;
            }

            // All at max means the key is in every input. If any overshot,
            // restart the round with the new maximum.
            if cursors.iter().all(|c| c.key.as_ref() == Some(&max)) {
                for combined in cross_combine(&cursors) {
                    yield combined;
                }
                // Step every cursor to its next group so the next round makes
                // progress.
                for cursor in &mut cursors {
                    cursor.load_next_group(&key).await?;
                }
            }
        }
    }
}

/// The cross product of every cursor's buffered group, folded left to right via
/// [`Match::combine`]; combinations that disagree on a non-key variable are
/// dropped.
fn cross_combine<S, Key>(cursors: &[Cursor<S, Key>]) -> Vec<Match>
where
    S: Selection,
    Key: Ord + Clone,
{
    let mut acc: Vec<Match> = vec![Match::new()];
    for cursor in cursors {
        let mut next = Vec::new();
        for partial in &acc {
            for row in &cursor.run {
                if let Some(combined) = partial.clone().combine(row) {
                    next.push(combined);
                }
            }
        }
        acc = next;
        if acc.is_empty() {
            break;
        }
    }
    acc
}

#[cfg(test)]
mod tests {
    use super::{merge_join, multi_merge_join};
    use crate::selection::{Match, Selection};
    use crate::{Term, Value};
    use futures_util::TryStreamExt;
    use futures_util::stream;

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// A row binding `join` to `key` and `tag` to a per-row marker, so the
    /// oracle can tell which side each binding came from.
    fn row(join: i64, tag_var: &str, tag: i64) -> Match {
        let mut m = Match::new();
        m.bind(&Term::var("j"), Value::from(join)).unwrap();
        m.bind(&Term::var(tag_var), Value::from(tag)).unwrap();
        m
    }

    /// The join key: the `Present` value of `j`, compared as i64 so the test's
    /// ordering is unambiguous (the real caller supplies encoded-order bytes).
    fn key(m: &Match) -> Option<i64> {
        m.value_of("j").and_then(|v| match v {
            Value::SignedInt(n) => Some(*n as i64),
            _ => None,
        })
    }

    fn seed(rows: Vec<Match>) -> impl Selection {
        stream::iter(rows.into_iter().map(Ok))
    }

    /// Reference inner join: the O(n*m) nested-loop combine, the oracle the
    /// merge must match. Independent of sort order.
    fn nested_loop(left: &[Match], right: &[Match]) -> Vec<Match> {
        let mut out = Vec::new();
        for l in left {
            for r in right {
                // Only correlate rows that agree on the join key, mirroring
                // what an equi-join on `j` does.
                if key(l).is_some()
                    && key(l) == key(r)
                    && let Some(joined) = l.clone().combine(r)
                {
                    out.push(joined);
                }
            }
        }
        out
    }

    /// Compare two result sets ignoring order: sort each by (j, l, r) markers.
    fn normalize(mut rows: Vec<Match>) -> Vec<(i64, Option<i64>, Option<i64>)> {
        let mut keyed: Vec<_> = rows
            .drain(..)
            .map(|m| {
                let j = key(&m).unwrap_or(i64::MIN);
                let l = m.value_of("l").and_then(as_int);
                let r = m.value_of("r").and_then(as_int);
                (j, l, r)
            })
            .collect();
        keyed.sort();
        keyed
    }

    fn as_int(v: &Value) -> Option<i64> {
        match v {
            Value::SignedInt(n) => Some(*n as i64),
            _ => None,
        }
    }

    async fn run_merge(left: Vec<Match>, right: Vec<Match>) -> Vec<Match> {
        merge_join(seed(left), seed(right), key)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    /// A row binding `j` (the join key) and one tag var; used for N-way tests
    /// where each input tags with a distinct variable so the oracle can see
    /// every side contributed.
    fn nrow(join: i64, tag_var: &str, tag: i64) -> Match {
        row(join, tag_var, tag)
    }

    /// Reference N-way inner intersection: the O(product) nested combine over
    /// all inputs, keeping only entities present in every input.
    fn nested_intersect(inputs: &[Vec<Match>]) -> Vec<Match> {
        let mut acc: Vec<Match> = vec![Match::new()];
        for input in inputs {
            let mut next = Vec::new();
            for partial in &acc {
                for row in input {
                    if key(row).is_some()
                        && let Some(combined) = partial.clone().combine(row)
                    {
                        next.push(combined);
                    }
                }
            }
            acc = next;
        }
        // Keep only combinations that bound the join key from some input (an
        // all-empty-input product would otherwise leak the empty seed).
        acc.into_iter().filter(|m| key(m).is_some()).collect()
    }

    fn normalize_keys(mut rows: Vec<Match>) -> Vec<i64> {
        let mut keys: Vec<i64> = rows.drain(..).filter_map(|m| key(&m)).collect();
        keys.sort();
        keys
    }

    async fn run_multi(inputs: Vec<Vec<Match>>) -> Vec<Match> {
        let streams: Vec<_> = inputs.into_iter().map(seed).collect();
        multi_merge_join(streams, key)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    #[dialog_common::test]
    async fn it_intersects_three_inputs_on_the_shared_key() {
        // Only keys present in ALL THREE survive: 2 and 5.
        let a = vec![
            nrow(1, "a", 1),
            nrow(2, "a", 2),
            nrow(5, "a", 5),
            nrow(7, "a", 7),
        ];
        let b = vec![nrow(2, "b", 2), nrow(3, "b", 3), nrow(5, "b", 5)];
        let c = vec![
            nrow(0, "c", 0),
            nrow(2, "c", 2),
            nrow(5, "c", 5),
            nrow(9, "c", 9),
        ];

        let got = run_multi(vec![a.clone(), b.clone(), c.clone()]).await;
        assert_eq!(normalize_keys(got.clone()), vec![2, 5]);
        // And it agrees with the nested oracle exactly.
        let oracle = nested_intersect(&[a, b, c]);
        assert_eq!(normalize_keys(got), normalize_keys(oracle));
    }

    #[dialog_common::test]
    async fn it_matches_the_nested_intersect_over_five_inputs() {
        // Five inputs (the Bug concept's arity), small key domain so the
        // intersection is non-trivial and many-to-one collisions occur.
        let mk = |tag: &str, keys: &[i64]| -> Vec<Match> {
            keys.iter()
                .enumerate()
                .map(|(i, k)| nrow(*k, tag, i as i64))
                .collect()
        };
        let inputs = vec![
            mk("a", &[1, 2, 3, 4, 5, 6]),
            mk("b", &[2, 3, 4, 5, 6]),
            mk("c", &[3, 4, 5, 6, 7]),
            mk("d", &[3, 4, 5]),
            mk("e", &[4, 5, 8]),
        ];
        // Present in all five: 4 and 5.
        let got = run_multi(inputs.clone()).await;
        assert_eq!(normalize_keys(got.clone()), vec![4, 5]);
        assert_eq!(
            normalize_keys(got),
            normalize_keys(nested_intersect(&inputs))
        );
    }

    #[dialog_common::test]
    async fn it_yields_nothing_when_one_input_is_empty() {
        let a = vec![nrow(1, "a", 1), nrow(2, "a", 2)];
        let empty: Vec<Match> = vec![];
        let c = vec![nrow(1, "c", 1), nrow(2, "c", 2)];
        assert!(run_multi(vec![a, empty, c]).await.is_empty());
    }

    #[dialog_common::test]
    async fn it_passes_a_single_input_through() {
        let a = vec![nrow(1, "a", 1), nrow(3, "a", 3)];
        let got = run_multi(vec![a]).await;
        assert_eq!(normalize_keys(got), vec![1, 3]);
    }

    #[dialog_common::test]
    async fn it_combines_many_to_many_across_inputs() {
        // Key 5 has two rows in input a and two in input b: the intersection
        // must produce the 2x2 cross product for that key.
        let a = vec![nrow(5, "a", 1), nrow(5, "a", 2)];
        let b = vec![nrow(5, "b", 1), nrow(5, "b", 2)];
        let got = run_multi(vec![a.clone(), b.clone()]).await;
        assert_eq!(got.len(), 4);
        assert_eq!(normalize_keys(got), vec![5, 5, 5, 5]);
    }

    #[dialog_common::test]
    async fn it_matches_the_nested_intersect_over_random_shapes() {
        let mut state: u64 = 0xD1B54A32D192ED03;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as i64
        };
        for _ in 0..150 {
            let arity = 2 + (next().unsigned_abs() % 4) as usize; // 2..5 inputs
            let inputs: Vec<Vec<Match>> = (0..arity)
                .map(|t| {
                    let len = (next().unsigned_abs() % 7) as usize;
                    let mut keys: Vec<i64> =
                        (0..len).map(|_| next().unsigned_abs() as i64 % 6).collect();
                    keys.sort();
                    keys.iter()
                        .enumerate()
                        .map(|(i, k)| nrow(*k, &format!("t{t}"), i as i64))
                        .collect()
                })
                .collect();
            let got = run_multi(inputs.clone()).await;
            assert_eq!(
                normalize_keys(got),
                normalize_keys(nested_intersect(&inputs)),
                "diverged on inputs {inputs:?}"
            );
        }
    }

    #[dialog_common::test]
    async fn it_matches_the_nested_loop_on_a_one_to_one_join() {
        let left = vec![row(1, "l", 10), row(2, "l", 20), row(3, "l", 30)];
        let right = vec![row(2, "r", 200), row(3, "r", 300), row(4, "r", 400)];

        let merged = run_merge(left.clone(), right.clone()).await;
        assert_eq!(normalize(merged), normalize(nested_loop(&left, &right)));
    }

    #[dialog_common::test]
    async fn it_matches_the_nested_loop_on_a_many_to_many_join() {
        // Two left rows and three right rows all share key 5: the equal-key
        // group must produce the full 2x3 cross product.
        let left = vec![row(5, "l", 1), row(5, "l", 2), row(7, "l", 3)];
        let right = vec![
            row(5, "r", 100),
            row(5, "r", 200),
            row(5, "r", 300),
            row(7, "r", 400),
        ];

        let merged = run_merge(left.clone(), right.clone()).await;
        let oracle = nested_loop(&left, &right);
        assert_eq!(merged.len(), oracle.len());
        assert_eq!(normalize(merged), normalize(oracle));
    }

    #[dialog_common::test]
    async fn it_matches_the_nested_loop_with_disjoint_and_gapped_keys() {
        // Interleaved keys with gaps on both sides, so the cursor has to skip
        // right keys below a left key and left keys with no right match.
        let left = vec![
            row(1, "l", 1),
            row(4, "l", 4),
            row(6, "l", 6),
            row(9, "l", 9),
        ];
        let right = vec![
            row(2, "r", 2),
            row(4, "r", 4),
            row(5, "r", 5),
            row(9, "r", 9),
        ];

        let merged = run_merge(left.clone(), right.clone()).await;
        assert_eq!(normalize(merged), normalize(nested_loop(&left, &right)));
    }

    #[dialog_common::test]
    async fn it_yields_nothing_when_no_keys_overlap() {
        let left = vec![row(1, "l", 1), row(3, "l", 3)];
        let right = vec![row(2, "r", 2), row(4, "r", 4)];

        let merged = run_merge(left, right).await;
        assert!(merged.is_empty());
    }

    #[dialog_common::test]
    async fn it_handles_an_empty_side() {
        let left = vec![row(1, "l", 1), row(2, "l", 2)];
        let merged = run_merge(left.clone(), vec![]).await;
        assert!(merged.is_empty());

        let right = vec![row(1, "r", 1)];
        let merged = run_merge(vec![], right).await;
        assert!(merged.is_empty());
    }

    /// A deterministic pseudo-random sweep: many left/right key multisets built
    /// from a linear-congruential generator, each joined both ways and checked
    /// against the oracle. This exercises the cursor's reuse path (consecutive
    /// left keys falling in right-side gaps, then hitting a buffered group) far
    /// more thoroughly than the hand-picked cases.
    #[dialog_common::test]
    async fn it_matches_the_nested_loop_over_many_random_shapes() {
        // No Math.random in this environment; a fixed LCG keeps it reproducible.
        let mut state: u64 = 0x9E3779B97F4A7C15;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as i64
        };

        for _ in 0..200 {
            let left_len = (next().unsigned_abs() % 8) as usize;
            let right_len = (next().unsigned_abs() % 8) as usize;
            // Keys drawn from a small domain so collisions (many-to-many) are
            // common; sorted ascending to satisfy the merge precondition.
            let mut left_keys: Vec<i64> = (0..left_len)
                .map(|_| next().unsigned_abs() as i64 % 5)
                .collect();
            let mut right_keys: Vec<i64> = (0..right_len)
                .map(|_| next().unsigned_abs() as i64 % 5)
                .collect();
            left_keys.sort();
            right_keys.sort();

            let left: Vec<Match> = left_keys
                .iter()
                .enumerate()
                .map(|(i, k)| row(*k, "l", i as i64))
                .collect();
            let right: Vec<Match> = right_keys
                .iter()
                .enumerate()
                .map(|(i, k)| row(*k, "r", i as i64))
                .collect();

            let merged = run_merge(left.clone(), right.clone()).await;
            assert_eq!(
                normalize(merged),
                normalize(nested_loop(&left, &right)),
                "diverged on left={left_keys:?} right={right_keys:?}"
            );
        }
    }

    #[dialog_common::test]
    async fn it_filters_rows_that_disagree_on_a_non_join_variable() {
        // Both sides bind a shared variable `x`; only rows agreeing on it
        // survive the combine, even when the join key matches.
        let mut l1 = Match::new();
        l1.bind(&Term::var("j"), Value::from(1i64)).unwrap();
        l1.bind(&Term::var("x"), Value::from(100i64)).unwrap();
        let mut l2 = Match::new();
        l2.bind(&Term::var("j"), Value::from(1i64)).unwrap();
        l2.bind(&Term::var("x"), Value::from(999i64)).unwrap();

        let mut r = Match::new();
        r.bind(&Term::var("j"), Value::from(1i64)).unwrap();
        r.bind(&Term::var("x"), Value::from(100i64)).unwrap();

        let merged = merge_join(seed(vec![l1, l2]), seed(vec![r]), key)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        // Only l1 (x=100) unifies with r (x=100); l2 (x=999) is filtered.
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].value_of("x").and_then(as_int), Some(100));
    }
}
