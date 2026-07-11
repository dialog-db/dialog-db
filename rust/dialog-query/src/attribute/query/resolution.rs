//! Sibling-resolution strategies for `Cardinality::One` reads.
//!
//! Storage is value-blind: two different values for one `(the, of)` pair are
//! two different index keys, so concurrent writes coexist as sibling claims
//! until a write supersedes them. `Cardinality::One` reads must nevertheless
//! project **one deterministic row** per `(the, of)` group. How that
//! projection combines a group's siblings is a [`Resolution`]:
//!
//! - [`Resolution::Choose`] picks a winner — higher cause wins, fact-hash
//!   tiebreak ([`choose`]). This is the strategy every scalar attribute uses,
//!   and the default.
//! - [`Resolution::Fold`] merges the siblings through a [`RecordFormat`]'s
//!   `merge`, so readers of a diverged record attribute see the *merged*
//!   document rather than one arbitrary fork.
//!
//! The strategy is a pure function of the attribute and its format — both
//! already part of attribute identity — so it never participates in query
//! identity, serialization, or comparison.
//!
//! See `notes/automerge-integration-spec.md` §4.4 for the design record.

use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::sync::Arc;

use crate::Value;
use dialog_artifacts::{Artifact, Cause, Record, RecordFormat};

/// Given two artifacts for the same `(attribute, entity)` pair, true when the
/// challenger beats the current winner. The winner is the artifact with the
/// higher cause; when causes are equal (including both `None`), the fact hash
/// (`Cause::from`) breaks the tie, and the incumbent survives an exact tie.
pub(crate) fn prefers_challenger(current: &Artifact, challenger: &Artifact) -> bool {
    match (&current.cause, &challenger.cause) {
        (Some(a), Some(b)) if a > b => false,
        (Some(a), Some(b)) if a < b => true,
        (Some(_), None) => false,
        (None, Some(_)) => true,
        _ => Cause::from(current) < Cause::from(challenger),
    }
}

/// Given two artifacts for the same `(attribute, entity)` pair, return the
/// winner. The winner is the artifact with the higher cause; when causes are
/// equal (including both `None`), the fact hash (`Cause::from`) breaks the tie.
pub(crate) fn choose(current: Artifact, challenger: Artifact) -> Artifact {
    if prefers_challenger(&current, &challenger) {
        challenger
    } else {
        current
    }
}

/// The type-erased group fold: all format knowledge is captured at
/// construction ([`RecordFold::new`]), where the concrete [`RecordFormat`] is
/// still statically known. On native targets the fold is shared across
/// threads by the evaluation stream; on `wasm32` neither bound applies. This
/// mirrors the `ConditionalSend`/`ConditionalSync` bounds of [`RecordFormat`]
/// using the actual auto traits.
#[cfg(not(target_arch = "wasm32"))]
type ErasedFold = dyn Fn(Vec<Artifact>) -> Artifact + Send + Sync;
#[cfg(target_arch = "wasm32")]
type ErasedFold = dyn Fn(Vec<Artifact>) -> Artifact;

/// A format-aware fold over one `(attribute, entity)` sibling group.
///
/// Constructed from a concrete [`RecordFormat`] before type erasure; cloning
/// bumps a reference count.
#[derive(Clone)]
pub struct RecordFold(Arc<ErasedFold>);

impl RecordFold {
    /// Capture format `F`'s merge into a type-erased group fold.
    pub fn new<F: RecordFormat>() -> Self {
        let fold: Arc<ErasedFold> = Arc::new(fold_group::<F>);
        RecordFold(fold)
    }

    /// Resolve a non-empty sibling group to its fold product.
    fn resolve(&self, group: Vec<Artifact>) -> Artifact {
        (self.0)(group)
    }
}

impl Debug for RecordFold {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str("RecordFold")
    }
}

/// How a `Cardinality::One` read combines competing siblings for one
/// `(attribute, entity)` pair into the single row it yields.
#[derive(Clone, Debug, Default)]
pub enum Resolution {
    /// Pick one winner via [`choose`]: higher cause wins, fact-hash tiebreak.
    /// Today's behavior for every attribute, and the default.
    #[default]
    Choose,
    /// Merge the siblings through a [`RecordFormat`]: every replica reads the
    /// same merged value even while storage still holds the forks.
    Fold(RecordFold),
}

impl Resolution {
    /// A resolution that folds sibling groups through format `F`'s merge.
    pub fn fold<F: RecordFormat>() -> Self {
        Resolution::Fold(RecordFold::new::<F>())
    }

    /// Open a sibling group with its first artifact.
    pub(crate) fn begin(&self, artifact: Artifact) -> Accumulator {
        match self {
            Resolution::Choose => Accumulator::Choose(artifact),
            Resolution::Fold(_) => Accumulator::Fold(vec![artifact]),
        }
    }

    /// Close a sibling group, producing the one artifact its row carries.
    pub(crate) fn resolve(&self, accumulator: Accumulator) -> Artifact {
        match (self, accumulator) {
            (_, Accumulator::Choose(artifact)) => artifact,
            (Resolution::Fold(fold), Accumulator::Fold(group)) => fold.resolve(group),
            // An accumulator never migrates between strategies; combine
            // deterministically rather than panic if one ever does.
            (Resolution::Choose, Accumulator::Fold(group)) => group
                .into_iter()
                .reduce(choose)
                .expect("accumulator group is never empty"),
        }
    }
}

/// The in-flight state of one `(attribute, entity)` sibling group during a
/// scan. [`Resolution::Choose`] keeps only the winner so far — O(1) per
/// group, exactly the pre-existing behavior. [`Resolution::Fold`] buffers the
/// group's artifacts: groups are overwhelmingly singletons, and only actual
/// divergence buffers more than one.
// Boxing the large `Choose` variant would put a heap allocation on every
// group of the pick-one path, which was allocation-free; the accumulator
// only ever lives on a single stream frame's stack.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Accumulator {
    /// Winner so far under [`Resolution::Choose`].
    Choose(Artifact),
    /// Buffered siblings under [`Resolution::Fold`], in stream order.
    Fold(Vec<Artifact>),
}

impl Accumulator {
    /// True when the artifact belongs to this accumulator's
    /// `(attribute, entity)` group.
    pub(crate) fn groups_with(&self, artifact: &Artifact) -> bool {
        let head = match self {
            Accumulator::Choose(current) => current,
            Accumulator::Fold(group) => &group[0],
        };
        head.the == artifact.the && head.of == artifact.of
    }

    /// Absorb the next sibling of the same group.
    pub(crate) fn absorb(self, artifact: Artifact) -> Self {
        match self {
            Accumulator::Choose(current) => Accumulator::Choose(choose(current, artifact)),
            Accumulator::Fold(mut group) => {
                group.push(artifact);
                Accumulator::Fold(group)
            }
        }
    }
}

/// Fold one sibling group through format `F`.
///
/// - A singleton group passes through untouched: no decode, no added cost —
///   the overwhelmingly common case.
/// - Siblings that fail to decode as `F` (foreign bytes written by a
///   schema-ignoring tool, corrupt or hostile payloads) are dropped from the
///   fold deterministically; decode resource bounds are the format's own
///   responsibility (e.g. automerge's canonicalization options).
/// - If exactly one sibling decodes, it passes through byte-identical.
/// - If none decode — or encoding the merged form fails — the group degrades
///   to [`choose`] over the raw artifacts. A read never fails outright:
///   failing the query would hand any collaborator a denial of service.
///
/// When several siblings decode they are merged pairwise in stream order,
/// with the arguments oriented so the [`choose`] winner of each pair sits in
/// `merge`'s second slot: the default last-write-wins merge then resolves
/// exactly as `choose` does — one convention system-wide — while an
/// order-insensitive CRDT merge is unaffected by the orientation. The fold
/// product carries the winning sibling's cause.
fn fold_group<F: RecordFormat>(mut group: Vec<Artifact>) -> Artifact {
    if group.len() == 1 {
        return group.pop().expect("group has one artifact");
    }

    let mut decoded: Vec<(usize, Arc<F>)> = Vec::new();
    for (index, artifact) in group.iter().enumerate() {
        if let Value::Record(record) = &artifact.is
            && let Ok(form) = record.realize::<F>()
        {
            decoded.push((index, form));
        }
    }

    match decoded.len() {
        0 => group
            .into_iter()
            .reduce(choose)
            .expect("group is never empty"),
        1 => group.swap_remove(decoded[0].0),
        _ => {
            let mut siblings = decoded.into_iter();
            let (winner, form) = siblings.next().expect("at least two decoded siblings");
            let mut winner = winner;
            let mut merged: F = (*form).clone();
            for (index, form) in siblings {
                if prefers_challenger(&group[winner], &group[index]) {
                    merged = F::merge(&merged, &form);
                    winner = index;
                } else {
                    merged = F::merge(&form, &merged);
                }
            }

            match Record::from_format(merged) {
                Ok(record) => Artifact {
                    the: group[winner].the.clone(),
                    of: group[winner].of.clone(),
                    is: Value::Record(record),
                    cause: group[winner].cause.clone(),
                },
                Err(_) => group
                    .into_iter()
                    .reduce(choose)
                    .expect("group is never empty"),
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_artifacts::{Attribute, RecordError};
    use std::str::FromStr;

    /// A toy CRDT-ish format: a sorted, deduplicated byte set that merges by
    /// union — commutative, associative, and idempotent, like a real CRDT's
    /// canonical form.
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct ByteSet(pub Vec<u8>);

    impl RecordFormat for ByteSet {
        fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
            Ok(ByteSet(bytes.to_vec()))
        }

        fn encode(&self) -> Result<Vec<u8>, RecordError> {
            Ok(self.0.clone())
        }

        fn merge(a: &Self, b: &Self) -> Self {
            let mut merged = a.0.clone();
            merged.extend_from_slice(&b.0);
            merged.sort_unstable();
            merged.dedup();
            ByteSet(merged)
        }
    }

    /// A format with the trait's default merge (last write wins).
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct Lww(pub Vec<u8>);

    impl RecordFormat for Lww {
        fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
            Ok(Lww(bytes.to_vec()))
        }

        fn encode(&self) -> Result<Vec<u8>, RecordError> {
            Ok(self.0.clone())
        }
    }

    /// A format that rejects any payload starting with `0xFF`.
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct Picky(Vec<u8>);

    impl RecordFormat for Picky {
        fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
            if bytes.first() == Some(&0xFF) {
                Err(RecordError::Decode("payload rejected".into()))
            } else {
                Ok(Picky(bytes.to_vec()))
            }
        }

        fn encode(&self) -> Result<Vec<u8>, RecordError> {
            Ok(self.0.clone())
        }

        fn merge(a: &Self, b: &Self) -> Self {
            let mut merged = a.0.clone();
            merged.extend_from_slice(&b.0);
            merged.sort_unstable();
            merged.dedup();
            Picky(merged)
        }
    }

    /// A format whose merge products cannot be encoded: decode always
    /// succeeds, merge concatenates, encode rejects anything longer than
    /// four bytes.
    #[derive(Debug, Clone, PartialEq)]
    struct Fragile(Vec<u8>);

    impl RecordFormat for Fragile {
        fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
            Ok(Fragile(bytes.to_vec()))
        }

        fn encode(&self) -> Result<Vec<u8>, RecordError> {
            if self.0.len() > 4 {
                Err(RecordError::Encode("too large".into()))
            } else {
                Ok(self.0.clone())
            }
        }

        fn merge(a: &Self, b: &Self) -> Self {
            let mut merged = a.0.clone();
            merged.extend_from_slice(&b.0);
            Fragile(merged)
        }
    }

    pub(crate) fn record_artifact(entity: &crate::Entity, bytes: Vec<u8>) -> Artifact {
        Artifact {
            the: Attribute::from_str("note/body").unwrap(),
            of: entity.clone(),
            is: Value::Record(Record::from(bytes)),
            cause: None,
        }
    }

    fn fold_via_resolution(resolution: &Resolution, group: Vec<Artifact>) -> Artifact {
        let mut siblings = group.into_iter();
        let mut accumulator = resolution.begin(siblings.next().unwrap());
        for artifact in siblings {
            accumulator = accumulator.absorb(artifact);
        }
        resolution.resolve(accumulator)
    }

    #[dialog_common::test]
    fn singleton_group_passes_through_without_decoding() {
        let entity = crate::Entity::new().unwrap();
        // 0xFF-prefixed bytes are undecodable as `Picky`; the row still
        // passes through untouched because a singleton group never decodes.
        let artifact = record_artifact(&entity, vec![0xFF, 1, 2]);

        let resolution = Resolution::fold::<Picky>();
        let product = fold_via_resolution(&resolution, vec![artifact.clone()]);

        assert_eq!(product, artifact);
    }

    #[dialog_common::test]
    fn fold_merges_all_decodable_siblings() {
        let entity = crate::Entity::new().unwrap();
        let group = vec![
            record_artifact(&entity, vec![1, 2]),
            record_artifact(&entity, vec![2, 3]),
            record_artifact(&entity, vec![9]),
        ];

        let resolution = Resolution::fold::<ByteSet>();
        let product = fold_via_resolution(&resolution, group);

        assert_eq!(
            product.is,
            Value::Record(Record::from(vec![1, 2, 3, 9])),
            "fold product is the union of every sibling"
        );
    }

    #[dialog_common::test]
    fn fold_is_deterministic_across_stream_orders() {
        let entity = crate::Entity::new().unwrap();
        let a = record_artifact(&entity, vec![1, 2]);
        let b = record_artifact(&entity, vec![2, 3]);
        let c = record_artifact(&entity, vec![9]);

        let resolution = Resolution::fold::<ByteSet>();
        let forward = fold_via_resolution(&resolution, vec![a.clone(), b.clone(), c.clone()]);
        let reverse = fold_via_resolution(&resolution, vec![c, b, a]);

        assert_eq!(forward, reverse);
    }

    /// The default (last-write-wins) merge folded with choose-oriented
    /// arguments resolves exactly as `choose` does: one convention
    /// system-wide for scalars and non-CRDT records alike (§4.4).
    #[dialog_common::test]
    fn default_merge_fold_matches_choose() {
        let entity = crate::Entity::new().unwrap();
        let a = record_artifact(&entity, vec![1]);
        let b = record_artifact(&entity, vec![2]);
        let c = record_artifact(&entity, vec![3]);

        let expected = [a.clone(), b.clone(), c.clone()]
            .into_iter()
            .reduce(choose)
            .unwrap();

        let resolution = Resolution::fold::<Lww>();
        let forward = fold_via_resolution(&resolution, vec![a.clone(), b.clone(), c.clone()]);
        let reverse = fold_via_resolution(&resolution, vec![c, b, a]);

        assert_eq!(forward.is, expected.is);
        assert_eq!(reverse.is, expected.is);
    }

    #[dialog_common::test]
    fn fold_drops_undecodable_siblings() {
        let entity = crate::Entity::new().unwrap();
        let good = record_artifact(&entity, vec![1, 2]);
        let bad = record_artifact(&entity, vec![0xFF, 9]);

        let resolution = Resolution::fold::<Picky>();
        let product = fold_via_resolution(&resolution, vec![bad, good.clone()]);

        assert_eq!(
            product, good,
            "the single decodable sibling passes through byte-identical"
        );
    }

    #[dialog_common::test]
    fn fold_ignores_non_record_siblings() {
        let entity = crate::Entity::new().unwrap();
        let record = record_artifact(&entity, vec![1, 2]);
        let mut scalar = record_artifact(&entity, vec![]);
        scalar.is = Value::String("not a record".into());

        let resolution = Resolution::fold::<ByteSet>();
        let product = fold_via_resolution(&resolution, vec![scalar, record.clone()]);

        assert_eq!(product, record);
    }

    #[dialog_common::test]
    fn fold_falls_back_to_choose_when_nothing_decodes() {
        let entity = crate::Entity::new().unwrap();
        let a = record_artifact(&entity, vec![0xFF, 1]);
        let b = record_artifact(&entity, vec![0xFF, 2]);

        let expected = choose(a.clone(), b.clone());

        let resolution = Resolution::fold::<Picky>();
        let product = fold_via_resolution(&resolution, vec![a, b]);

        assert_eq!(product, expected, "undecodable group degrades to choose");
    }

    #[dialog_common::test]
    fn fold_falls_back_to_choose_when_encode_fails() {
        let entity = crate::Entity::new().unwrap();
        let a = record_artifact(&entity, vec![1, 2, 3]);
        let b = record_artifact(&entity, vec![4, 5, 6]);

        let expected = choose(a.clone(), b.clone());

        let resolution = Resolution::fold::<Fragile>();
        let product = fold_via_resolution(&resolution, vec![a, b]);

        assert_eq!(
            product, expected,
            "an unencodable merge product degrades to choose"
        );
    }

    #[dialog_common::test]
    fn choose_prefers_higher_cause() {
        let attr = Attribute::from_str("person/name").unwrap();
        let entity = crate::Entity::new().unwrap();

        let older = Artifact {
            the: attr.clone(),
            of: entity.clone(),
            is: Value::String("Alice".into()),
            cause: Some(Cause([1u8; 32])),
        };

        let newer = Artifact {
            the: attr,
            of: entity,
            is: Value::String("Alicia".into()),
            cause: Some(Cause([2u8; 32])),
        };

        let winner = choose(older.clone(), newer.clone());
        assert_eq!(winner.cause, newer.cause, "Higher cause should win");

        // Reversed argument order should produce the same winner.
        let winner2 = choose(newer.clone(), older.clone());
        assert_eq!(winner2.cause, newer.cause);
    }

    #[dialog_common::test]
    fn choose_uses_fact_hash_for_equal_causes() {
        let attr = Attribute::from_str("person/name").unwrap();
        let entity = crate::Entity::new().unwrap();

        let a = Artifact {
            the: attr.clone(),
            of: entity.clone(),
            is: Value::String("Alice".into()),
            cause: Some(Cause([1u8; 32])),
        };

        let b = Artifact {
            the: attr,
            of: entity,
            is: Value::String("Alicia".into()),
            cause: Some(Cause([1u8; 32])),
        };

        let winner_ab = choose(a.clone(), b.clone());
        let winner_ba = choose(b.clone(), a.clone());

        // The winner should be deterministic regardless of argument order.
        assert_eq!(
            Cause::from(&winner_ab),
            Cause::from(&winner_ba),
            "Tiebreaker should be deterministic"
        );
    }

    #[dialog_common::test]
    fn choose_accumulator_matches_pairwise_choose() {
        let entity = crate::Entity::new().unwrap();
        let a = record_artifact(&entity, vec![1]);
        let b = record_artifact(&entity, vec![2]);
        let c = record_artifact(&entity, vec![3]);

        let expected = [a.clone(), b.clone(), c.clone()]
            .into_iter()
            .reduce(choose)
            .unwrap();

        let product = fold_via_resolution(&Resolution::Choose, vec![a, b, c]);

        assert_eq!(product, expected);
    }
}
