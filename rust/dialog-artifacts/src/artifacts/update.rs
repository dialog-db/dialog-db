use crate::artifacts::query::Select;
use crate::key::{default_manifest, value_tail_bytes};
use crate::selector::Constrained;
use crate::{
    Artifact, ArtifactSelector, ArtifactStream, Attribute, DialogArtifactsError, Entity,
    Instruction, Value,
};
use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_search_tree::Manifest;
use futures_util::Stream;
use futures_util::stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::vec::IntoIter;

/// A single write operation on an `(entity, attribute)` pair.
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    /// Assert a value for an entity-attribute pair (cardinality-many).
    Assert(Value),
    /// Replace any prior value(s) at this `(entity, attribute)` with this one
    /// (cardinality-one). Supersession of priors happens at commit time.
    Replace(Value),
    /// Retract a value from an entity-attribute pair.
    Retract(Value),
}

/// The write side of the triple store.
///
/// Implementors accumulate fact changes (associations and dissociations)
/// that can later be committed atomically.
pub trait Update {
    /// Assert that the `attribute` of `entity` is `value`.
    fn associate(&mut self, the: Attribute, of: Entity, is: Value);

    /// Assert with cardinality-one semantics: replaces any previous
    /// value for the same `(attribute, entity)` pair in this batch.
    fn associate_unique(&mut self, the: Attribute, of: Entity, is: Value) {
        self.associate(the, of, is);
    }

    /// Retract that the `attribute` of `entity` is `value`.
    fn dissociate(&mut self, the: Attribute, of: Entity, is: Value);
}

/// A domain-level write operation that can be asserted or retracted.
///
/// Types like concept structs and attribute expressions implement this
/// trait. Asserting a statement adds facts; retracting removes them.
pub trait Statement: Sized {
    /// Assert this statement into an update target.
    fn assert(self, update: &mut impl Update);

    /// Retract this statement from an update target.
    fn retract(self, update: &mut impl Update);
}

/// A batch of pending writes, organized by entity and attribute.
#[derive(Debug, Default, Clone)]
pub struct Changes(HashMap<Entity, HashMap<Attribute, Vec<Change>>>);

impl Changes {
    /// Create an empty changeset.
    pub fn new() -> Self {
        Self::default()
    }

    /// Assert a claim.
    pub fn assert<C: Statement>(&mut self, claim: C) -> &mut Self {
        claim.assert(self);
        self
    }

    /// Retract a claim.
    pub fn retract<C: Statement>(&mut self, claim: C) -> &mut Self {
        claim.retract(self);
        self
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Convert to an instruction stream.
    pub fn into_stream(self) -> ChangeStream {
        ChangeStream::from(self)
    }

    /// Drop every change recorded for entities that fail `keep`,
    /// asserts and retracts alike. Returns `true` when anything was
    /// removed. Unlike [`retract`](Self::retract), which records a
    /// tombstone alongside the prior changes, this removes the
    /// entity's entries from the batch outright — the primitive a
    /// session overlay needs to garbage-collect per-client facts
    /// without growing.
    pub fn retain_entities<F: FnMut(&Entity) -> bool>(&mut self, mut keep: F) -> bool {
        let before = self.0.len();
        self.0.retain(|entity, _| keep(entity));
        self.0.len() != before
    }

    /// Borrowing iterator over every recorded `(entity, attribute,
    /// change)` triple. Use this when you need to inspect the batch
    /// without consuming it — e.g. to extract tombstones from
    /// retracts without cloning the whole structure.
    pub fn iter(&self) -> impl Iterator<Item = (&Entity, &Attribute, &Change)> {
        self.0.iter().flat_map(|(entity, attrs)| {
            attrs
                .iter()
                .flat_map(move |(attr, changes)| changes.iter().map(move |c| (entity, attr, c)))
        })
    }

    /// Convert to a vec of instructions.
    pub fn into_instructions(self) -> Vec<Instruction> {
        let mut instructions = Vec::new();
        for (entity, attributes) in self.0 {
            for (attribute, operations) in attributes {
                for operation in operations {
                    let instruction = match operation {
                        Change::Assert(value) => Instruction::Assert(Artifact {
                            the: attribute.clone(),
                            of: entity.clone(),
                            is: value,
                            cause: None,
                        }),
                        Change::Replace(value) => Instruction::Replace(Artifact {
                            the: attribute.clone(),
                            of: entity.clone(),
                            is: value,
                            cause: None,
                        }),
                        Change::Retract(value) => Instruction::Retract(Artifact {
                            the: attribute.clone(),
                            of: entity.clone(),
                            is: value,
                            cause: None,
                        }),
                    };
                    instructions.push(instruction);
                }
            }
        }
        instructions
    }
}

impl Update for Changes {
    fn associate(&mut self, the: Attribute, of: Entity, is: Value) {
        self.0
            .entry(of)
            .or_default()
            .entry(the)
            .or_default()
            .push(Change::Assert(is));
    }

    fn associate_unique(&mut self, the: Attribute, of: Entity, is: Value) {
        self.0
            .entry(of)
            .or_default()
            .insert(the, vec![Change::Replace(is)]);
    }

    fn dissociate(&mut self, the: Attribute, of: Entity, is: Value) {
        self.0
            .entry(of)
            .or_default()
            .entry(the)
            .or_default()
            .push(Change::Retract(is));
    }
}

impl IntoIterator for Changes {
    type Item = Instruction;
    type IntoIter = IntoIter<Instruction>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_instructions().into_iter()
    }
}

/// A [`Stream`] adapter that drains [`Changes`] into [`Instruction`]s.
pub struct ChangeStream {
    iter: IntoIter<Instruction>,
}

impl From<Changes> for ChangeStream {
    fn from(changes: Changes) -> Self {
        Self {
            iter: changes.into_iter(),
        }
    }
}

impl Stream for ChangeStream {
    type Item = Instruction;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.iter.next())
    }
}

/// The full sort key for an [`Artifact`] — `(the, of, value_tail)`.
///
/// - `the` / `of` — raw attribute / entity key bytes.
/// - `value_tail` — the key's value tail bytes (see below).
///
/// # Why this exact component order
///
/// The artifact prolly tree keeps three indexes, each a byte key (see
/// `dialog_artifacts::key`):
///
/// ```text
///   EAV:  tag | entity    | attribute | value_tail
///   AEV:  tag | attribute | entity    | value_tail
///   VAE:  tag | value_tail | attribute | entity
/// ```
///
/// A query scan pins whichever dimension the selector constrains and
/// streams the rest in that index's byte order. The pinned dimension
/// is constant across the whole scan, so it drops out of the
/// comparison — what's left is the index's *residual* order:
///
/// ```text
///   .of(entity)    → EAV → residual (attribute, value_tail)
///   .the(attr)     → AEV → residual (entity,    value_tail)
///   .is(value)     → VAE → residual (attribute, entity)
/// ```
///
/// `SortKey = (attribute, entity, value_tail)` is the **unique**
/// total order whose restriction (delete the pinned component)
/// reproduces every one of those residuals:
///
/// - lock `entity`  → `attribute` is the next live component ✓ (EAV)
/// - lock `value`   → `value_tail` drops out, `attribute` is next ✓ (VAE)
/// - lock `attribute` → `attribute` itself drops out, `entity` is
///   next ✓ (AEV)
///
/// In every mode the next live component after the pinned one is
/// exactly the dimension that index sorts by next. So sorting any
/// source's output by `SortKey` yields the same order the tree's
/// scan would for that selector — which is what lets the query
/// layer's k-way merge interleave a branch scan and a `Changes`
/// overlay (or two branches) into the order a single physical tree
/// containing all of them would produce. It also holds for
/// multi-constraint selectors:
/// pinning two dimensions just removes both from the comparison.
///
/// The value-tail component (vs. the bare `(the, of)` group key) also
/// fixes interleaving *within* a cardinality-many group: same-`(the,
/// of)` items from different streams order by their value tail rather
/// than by stream index.
///
/// The third component is the key's *value tail* (the type byte followed by
/// the value slot, plus a spilled value's trailing whole-value hash), not a
/// bare type discriminant plus reference: the tree orders same-`(the, of)`
/// facts by exactly those tail bytes. A spilled value's slot holds the encoded
/// prefix of its raw bytes, so it sorts INTO its type band next to inline
/// values, and folding the whole tail into one component reproduces that
/// ordering; splitting the type out and comparing a reference separately would
/// not.
pub type SortKey = (Vec<u8>, Vec<u8>, Vec<u8>);

/// Compute the [`SortKey`] for an artifact.
///
/// Uses the same entity/attribute bytes and value tail the tree's own index
/// keys are built from (`EntityKey::from(&Artifact)` and friends), so a
/// `SortKey` sort reproduces the tree's byte order exactly, not just an
/// approximation of it. In particular the value component is the value tail the
/// key carries, so same-`(the, of, type)` facts order by value exactly as the
/// tree does. See [`SortKey`] for why the component order is correct across all
/// three scan modes.
///
/// `manifest` must be the format of the tree this ordering is compared against:
/// it decides whether the value spills and how much of it the tail carries, so
/// a different manifest would sort a boundary-sized value into a different
/// position than the tree puts it.
pub fn sort_key(artifact: &Artifact, manifest: &Manifest) -> SortKey {
    (
        artifact.the.as_str().as_bytes().to_vec(),
        artifact.of.as_str().as_bytes().to_vec(),
        value_tail_bytes(&artifact.is, manifest),
    )
}

/// `Statement` for a [`Changes`] batch — replays every recorded
/// [`Change`] into the target [`Update`].
///
/// Lets a `Changes` value act anywhere a single statement does: e.g.
/// folding pre-built changes into another transaction, or asserting
/// a changes-shaped overlay into a query session. `Assert` and
/// `Replace` map to `associate` / `associate_unique` on the target;
/// `Retract` maps to `dissociate`.
impl Statement for Changes {
    fn assert(self, update: &mut impl Update) {
        for instruction in self.into_instructions() {
            match instruction {
                Instruction::Assert(a) => update.associate(a.the, a.of, a.is),
                Instruction::Replace(a) => update.associate_unique(a.the, a.of, a.is),
                Instruction::Retract(a) => update.dissociate(a.the, a.of, a.is),
            }
        }
    }

    fn retract(self, update: &mut impl Update) {
        // Inverse: asserts/replaces become retracts; existing
        // retracts become asserts. Symmetric so `c.assert(t);
        // c.retract(t);` round-trips when `t` is a fresh target.
        for instruction in self.into_instructions() {
            match instruction {
                Instruction::Assert(a) | Instruction::Replace(a) => {
                    update.dissociate(a.the, a.of, a.is)
                }
                Instruction::Retract(a) => update.associate(a.the, a.of, a.is),
            }
        }
    }
}

/// `Provider<Select>` for an in-memory [`Changes`] batch.
///
/// Treats `Changes` as a queryable source: `Assert` and `Replace`
/// entries surface as [`Artifact`]s matching the [`ArtifactSelector`]'s
/// `the` / `of` / `is` constraints (whichever are present), sorted by
/// [`sort_key`] so the result interleaves cleanly with branch / layer
/// scans in a `merge_grouped`-style union.
///
/// `Retract` entries are **deliberately not yielded**. A retract in a
/// changes batch means "this fact should not appear" — a negative
/// signal that doesn't fit `ArtifactStream`'s positive `Result<Artifact, _>`
/// shape. Tombstone filtering against another source is a separate
/// concern handled by the composition layer that owns the merge.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<'a> Provider<Select<'a>> for Changes {
    async fn execute(
        &self,
        input: ArtifactSelector<Constrained>,
    ) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
        let the = input.attribute();
        let of = input.entity();
        let is = input.value();

        // Linear filter over the batch. A `Changes` overlay is small
        // by construction — a few auto-injected metadata facts plus
        // whatever the caller asserted via `.with(...)` — so scanning
        // it per query is negligible and not worth indexing.
        let mut matched: Vec<Artifact> = Vec::new();
        for (entity, attrs) in &self.0 {
            if let Some(of_target) = of
                && entity != of_target
            {
                continue;
            }
            for (attribute, changes) in attrs {
                if let Some(the_target) = the
                    && attribute != the_target
                {
                    continue;
                }
                for change in changes {
                    let value = match change {
                        Change::Assert(v) | Change::Replace(v) => v,
                        // Retracts don't surface from a Changes-as-source
                        // view — see impl docs.
                        Change::Retract(_) => continue,
                    };
                    if let Some(is_target) = is
                        && value != is_target
                    {
                        continue;
                    }
                    matched.push(Artifact {
                        the: attribute.clone(),
                        of: entity.clone(),
                        is: value.clone(),
                        cause: None,
                    });
                }
            }
        }
        // Sort by `sort_key` so this overlay's output is in the same
        // order the prolly tree would scan for this selector — see
        // `SortKey` docs. That's the precondition `merge_grouped`
        // relies on when it unions this stream with a branch scan.
        // This overlay is sorted in memory against a branch scan that is
        // itself in `sort_key` order. Both sides order by the same function,
        // and the comparison never touches stored key bytes, so the default
        // threshold is sound here (see `default_sort_key`).
        matched.sort_by_key(default_sort_key);
        Ok(Box::pin(stream::iter(matched.into_iter().map(Ok))))
    }
}

/// [`sort_key`] under the default format [`Manifest`], for callers with no tree
/// in scope.
///
/// Sound only where the key is used as an in-memory ordering/identity key
/// compared against OTHER `default_sort_key` values within the same process,
/// never against bytes read out of a tree. Under that use the format merely has
/// to be a consistent function of the value, and every participant applies the
/// same one, so which manifest it is cannot change any comparison's outcome.
/// Callers that compare against stored keys must pass the tree's own manifest
/// to [`sort_key`] instead.
pub fn default_sort_key(artifact: &Artifact) -> SortKey {
    sort_key(artifact, &default_manifest())
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use futures_util::StreamExt as _;

    fn alice() -> Entity {
        "id:alice".parse().expect("valid entity")
    }
    fn bob() -> Entity {
        "id:bob".parse().expect("valid entity")
    }
    fn name_attr() -> Attribute {
        "test/name".parse().expect("valid attribute")
    }
    fn role_attr() -> Attribute {
        "test/role".parse().expect("valid attribute")
    }

    /// `sort_key` must reproduce the tree's EAV key byte order exactly,
    /// including when a value spills: the tree orders same-`(the, of)` facts
    /// by the spill-FLAGGED type byte leading the value tail, so a bare
    /// (unflagged) type component would order a spilled String (tail `0x83…`)
    /// before an inline UnsignedInt (tail `0x04…`) while the tree does the
    /// opposite, corrupting the k-way merge order.
    #[dialog_common::test]
    fn it_orders_sort_keys_exactly_as_the_tree_orders_keys() {
        let inline_n = dialog_search_tree::Manifest::default().inline_n as usize;
        let facts: Vec<Artifact> = vec![
            Value::String("z".repeat(inline_n + 1)), // spilled: tail 0x83…
            Value::UnsignedInt(1),                   // inline: tail 0x04…
            Value::String("abc".into()),             // inline: tail 0x03…
            Value::Float(1.5),                       // inline: tail 0x06…
        ]
        .into_iter()
        .map(|is| Artifact {
            the: name_attr(),
            of: alice(),
            is,
            cause: None,
        })
        .collect();

        // Both orderings must be built under the SAME manifest: that
        // agreement is the property under test.
        let manifest = default_manifest();
        let mut by_sort_key = facts.clone();
        by_sort_key.sort_by_key(|fact| sort_key(fact, &manifest));
        let mut by_tree_key = facts;
        by_tree_key.sort_by_key(|fact| crate::EntityKey::from_artifact(fact, &manifest).into_key());

        let sorted: Vec<&Value> = by_sort_key.iter().map(|fact| &fact.is).collect();
        let expected: Vec<&Value> = by_tree_key.iter().map(|fact| &fact.is).collect();
        assert_eq!(
            sorted, expected,
            "sort_key order must equal tree key byte order"
        );
    }

    #[dialog_common::test]
    fn it_replays_changes_into_a_target_via_statement_assert() {
        let mut source = Changes::new();
        source.associate(name_attr(), alice(), Value::String("Alice".into()));
        source.dissociate(name_attr(), bob(), Value::String("Bob".into()));

        let mut target = Changes::new();
        source.assert(&mut target);

        // Replay produced one Assert + one Retract on `target`.
        let instructions: Vec<_> = target.into_instructions();
        assert_eq!(instructions.len(), 2);
        assert!(
            instructions
                .iter()
                .any(|i| matches!(i, Instruction::Assert(_)))
        );
        assert!(
            instructions
                .iter()
                .any(|i| matches!(i, Instruction::Retract(_)))
        );
    }

    #[dialog_common::test]
    fn it_inverts_changes_under_statement_retract() {
        let mut source = Changes::new();
        source.associate(name_attr(), alice(), Value::String("Alice".into()));

        let mut target = Changes::new();
        source.retract(&mut target);

        let instructions: Vec<_> = target.into_instructions();
        assert_eq!(instructions.len(), 1);
        assert!(matches!(instructions[0], Instruction::Retract(_)));
    }

    async fn artifacts(
        changes: &Changes,
        selector: ArtifactSelector<Constrained>,
    ) -> Vec<Artifact> {
        let stream = Provider::<Select<'_>>::execute(changes, selector)
            .await
            .expect("execute");
        stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("collect")
    }

    #[dialog_common::test]
    async fn it_yields_asserts_as_artifacts() {
        let mut changes = Changes::new();
        changes.associate(name_attr(), alice(), Value::String("Alice".into()));

        let results = artifacts(&changes, ArtifactSelector::new().the(name_attr())).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].of, alice());
        assert_eq!(results[0].is, Value::String("Alice".into()));
    }

    #[dialog_common::test]
    async fn it_yields_replaces_as_artifacts() {
        let mut changes = Changes::new();
        changes.associate_unique(name_attr(), alice(), Value::String("Alicia".into()));

        let results = artifacts(&changes, ArtifactSelector::new().of(alice())).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, Value::String("Alicia".into()));
    }

    #[dialog_common::test]
    async fn it_omits_retracts_from_the_selection() {
        let mut changes = Changes::new();
        changes.associate(name_attr(), alice(), Value::String("Alice".into()));
        changes.dissociate(name_attr(), bob(), Value::String("Bob".into()));

        // Only the assert should surface. Retracts are deliberately
        // dropped because there's no negative-fact channel in
        // ArtifactStream.
        let results = artifacts(&changes, ArtifactSelector::new().the(name_attr())).await;
        let entities: Vec<&Entity> = results.iter().map(|a| &a.of).collect();
        assert_eq!(entities, vec![&alice()]);
    }

    #[dialog_common::test]
    async fn it_filters_by_the_of_and_is() {
        let mut changes = Changes::new();
        changes.associate(name_attr(), alice(), Value::String("Alice".into()));
        changes.associate(name_attr(), bob(), Value::String("Bob".into()));
        changes.associate(role_attr(), alice(), Value::String("Engineer".into()));

        // Filter by `the` only
        let by_attr = artifacts(&changes, ArtifactSelector::new().the(name_attr())).await;
        assert_eq!(by_attr.len(), 2);

        // Filter by `the` + `of`
        let by_attr_entity = artifacts(
            &changes,
            ArtifactSelector::new().the(name_attr()).of(alice()),
        )
        .await;
        assert_eq!(by_attr_entity.len(), 1);
        assert_eq!(by_attr_entity[0].of, alice());

        // Filter by `is`
        let by_value = artifacts(
            &changes,
            ArtifactSelector::new()
                .the(name_attr())
                .is(Value::String("Bob".into())),
        )
        .await;
        assert_eq!(by_value.len(), 1);
        assert_eq!(by_value[0].of, bob());
    }

    #[dialog_common::test]
    async fn it_emits_artifacts_in_sort_key_order() {
        // Insert in deliberately wrong order; expect output sorted by
        // sort_key so cross-source merges interleave consistently.
        let mut changes = Changes::new();
        // Different attributes — sort by attribute key first.
        changes.associate(role_attr(), alice(), Value::String("Engineer".into()));
        changes.associate(name_attr(), alice(), Value::String("Alice".into()));

        let results = artifacts(&changes, ArtifactSelector::new().of(alice())).await;
        assert_eq!(results.len(), 2);
        // Attributes ordered by their key bytes — verify by checking
        // the output is monotonic under sort_key.
        let keys: Vec<_> = results.iter().map(default_sort_key).collect();
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        assert_eq!(keys, sorted_keys);
    }
}
