//! In-memory layers and source composition for query evaluation.
//!
//! A *layer* is an auxiliary fact + rule source that gets unioned with a
//! primary source during query evaluation. Layers expose synthetic,
//! in-memory information — branch metadata, system state, derived views —
//! alongside the real artifact store, queried with the same
//! [`Application`](dialog_query::query::Application) API.
//!
//! [`Layer`] is implemented on top of the same prolly tree the branch uses,
//! backed by a `dialog_storage::Volatile` provider through a [`LocalIndex`].
//! That symmetry matters: a layer's `Provider<Select<'_>>` yields artifacts
//! in exactly the same `(the, of)` order a branch does, so the
//! cardinality-one sliding window in
//! [`AttributeQueryOnly`](dialog_query::attribute::query::AttributeQuery)
//! sees correctly grouped streams when [`Union`] merges them.
//!
//! [`Union<P, O>`] is the data-source counterpart to the planner's
//! [`Disjunction`](dialog_query::planner::Disjunction): where `Disjunction`
//! unions the result streams of alternative plans, `Union` unions the fact
//! streams (and rule sets) of alternative sources.
//!
//! ```ignore
//! use dialog_repository::layer::{Layer, Union};
//!
//! let layer = Layer::new()
//!     .assert(Employee { this: id, name: Name("Alice".into()) })
//!     .register(my_rule)?;
//!
//! let env = Union::new(branch_env, layer);
//! let results = my_query.perform(&env).try_vec().await?;
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::tree as artifact_tree;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStream, Attribute, Cause, Changes, DialogArtifactsError,
    Entity, Select, Statement, Update, Value,
};
use dialog_capability::{Capability, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_effects::archive::Catalog;
use dialog_effects::archive::prelude::{ArchiveExt as _, ArchiveSubjectExt as _};
use dialog_prolly_tree::{EMPT_TREE_HASH, Tree};
use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::concept::query::ConceptRules;
use dialog_query::error::EvaluationError;
use dialog_query::query::Application;
use dialog_query::rule::When;
use dialog_query::rule::deductive::DeductiveRule;
use dialog_query::session::RuleRegistry;
use dialog_query::source::SelectRules;
use dialog_storage::Blake3Hash;
use dialog_storage::provider::Volatile;
use dialog_varsig::did;
use futures_util::StreamExt;
use futures_util::stream;
use parking_lot::Mutex;
use std::mem;

use crate::LocalIndex;

/// The canonical group key for artifacts traveling through a query stream.
///
/// Consumers — notably the cardinality-one sliding window in
/// [`AttributeQueryOnly::evaluate`](dialog_query::attribute::query::AttributeQuery) —
/// assume that artifacts sharing the same `(the, of)` pair arrive
/// consecutively. Anything that unions facts from multiple sources must
/// preserve that invariant; this helper produces the comparable key used
/// when merging.
pub fn group_key(artifact: &Artifact) -> (Vec<u8>, Vec<u8>) {
    (
        artifact.the.key_bytes().to_vec(),
        artifact.of.key_bytes().to_vec(),
    )
}

/// Merge already-grouped sorted streams into one stream that preserves
/// `(the, of)` grouping across all sources, deduplicating identical
/// claims that appear in more than one source.
///
/// Each input is assumed sorted by [`group_key`] (true of branch selects
/// and of layer selects, by construction). The output stream interleaves
/// all sources by the same key so cross-source items with the same
/// `(the, of)` become consecutive. Implemented as a streaming k-way merge
/// with peekable inputs.
///
/// "The same claim from two sources is still one claim." When the same
/// `(the, of, is, cause)` artifact appears in multiple inputs, only the
/// first occurrence within a `(the, of)` run is yielded. Dedup uses
/// [`Cause::from(&artifact)`] (a hash over all four fields) so it is
/// position-independent: duplicates that don't end up adjacent — e.g.
/// stream A: `[X, Y]`, stream B: `[X]` — are still collapsed because we
/// track the set of fingerprints seen for the current `(the, of)` group
/// and reset it when the group key advances.
pub fn merge_grouped<'a>(streams: Vec<ArtifactStream<'a>>) -> ArtifactStream<'a> {
    use std::collections::HashSet;
    use std::pin::Pin;

    if streams.is_empty() {
        return Box::pin(stream::empty());
    }
    if streams.len() == 1 {
        // A single-stream merge can still surface duplicates if the
        // caller passes an already-unioned stream, but for branch /
        // layer scans every key is unique within a single stream so the
        // dedup pass would be pure overhead. Pass through unchanged.
        return streams.into_iter().next().expect("len == 1");
    }

    let mut peekable: Vec<_> = streams.into_iter().map(StreamExt::peekable).collect();

    Box::pin(async_stream::try_stream! {
        // Fingerprints already yielded within the current (the, of) run.
        // Cleared whenever the run advances to a new group_key.
        let mut current_key: Option<(Vec<u8>, Vec<u8>)> = None;
        let mut seen: HashSet<Cause> = HashSet::new();

        loop {
            let mut min_idx: Option<usize> = None;
            let mut min_key: Option<(Vec<u8>, Vec<u8>)> = None;
            for (i, s) in peekable.iter_mut().enumerate() {
                match Pin::new(s).peek().await {
                    None => continue,
                    Some(Err(_)) => {
                        min_idx = Some(i);
                        break;
                    }
                    Some(Ok(head)) => {
                        let key = group_key(head);
                        if min_key.as_ref().is_none_or(|cur| &key < cur) {
                            min_key = Some(key);
                            min_idx = Some(i);
                        }
                    }
                }
            }
            let Some(idx) = min_idx else { break };
            let item = peekable[idx]
                .next()
                .await
                .expect("peek returned Some, so next must too")?;

            let key = group_key(&item);
            if current_key.as_ref() != Some(&key) {
                current_key = Some(key);
                seen.clear();
            }
            // `Cause::from(&Artifact)` hashes (the, of, is, cause) — two
            // artifacts with identical fields produce identical fingerprints.
            if seen.insert(Cause::from(&item)) {
                yield item;
            }
        }
    })
}

/// Internal mutable state of a [`Layer`]. Kept behind an Arc<Mutex<...>>
/// so the chained synchronous `assert`/`retract` API works on `Self` while
/// `Provider<Select>::execute` (which only has `&self`) can still flush.
struct State {
    /// Current root of the layer's prolly tree, or `EMPT_TREE_HASH` when
    /// the layer has only ever held unflushed changes.
    tree: Blake3Hash,
    /// Synchronously accumulated writes that have not yet been folded
    /// into the tree. Flushed on first select.
    pending: Changes,
}

/// An in-memory layer carrying both synthetic facts and deductive rules.
///
/// `Layer` is the bundle a [`QuerySession`](crate::session) wires onto a
/// real fact source via [`Union`] (or, more commonly, via the
/// `.with(layer)` chain on a session). Facts asserted here are unioned
/// with the primary during evaluation; rules registered here merge with
/// the primary's rules per concept.
///
/// The fact-mutation surface mirrors
/// [`Transaction`](crate::repository::branch::Transaction): use
/// [`assert`](Self::assert) / [`retract`](Self::retract) with any
/// [`Statement`]. Mutations are buffered in a [`Changes`] and flushed into
/// the underlying prolly tree the first time a select reads from the
/// layer. The tree itself lives entirely in a `Volatile` provider — no
/// operator, profile, or persisted state.
#[derive(Clone)]
pub struct Layer {
    env: Volatile,
    catalog: Capability<Catalog>,
    state: Arc<Mutex<State>>,
    rules: RuleRegistry,
}

impl Default for Layer {
    fn default() -> Self {
        Self::new()
    }
}

impl Layer {
    /// Create an empty layer backed by a fresh in-memory store.
    pub fn new() -> Self {
        Self {
            env: Volatile::new(),
            // The catalog is scoped to a fixed synthetic DID; isolation
            // between layers comes from each one owning its own Volatile,
            // not from the DID.
            catalog: Subject::from(did!("key:zDialogLayer"))
                .archive()
                .catalog("index"),
            state: Arc::new(Mutex::new(State {
                tree: EMPT_TREE_HASH,
                pending: Changes::new(),
            })),
            rules: RuleRegistry::new(),
        }
    }

    /// Assert a [`Statement`] into the layer — the same surface as
    /// [`Transaction::assert`](crate::repository::branch::Transaction::assert).
    ///
    /// Changes are buffered; the underlying tree is materialized the
    /// first time the layer is read from.
    pub fn assert<S: Statement>(self, statement: S) -> Self {
        {
            let mut state = self.state.lock();
            statement.assert(&mut state.pending);
        }
        self
    }

    /// Retract a [`Statement`] from the layer.
    pub fn retract<S: Statement>(self, statement: S) -> Self {
        {
            let mut state = self.state.lock();
            statement.retract(&mut state.pending);
        }
        self
    }

    /// Register a pre-built deductive rule on this layer.
    pub fn register(mut self, rule: DeductiveRule) -> Result<Self, EvaluationError> {
        self.rules.register(rule)?;
        Ok(self)
    }

    /// Install a deductive rule from a closure that builds its body from a
    /// fresh query of the conclusion concept.
    ///
    /// The closure receives a default-constructed `Query<M>`; whatever
    /// premises it returns become the rule body.
    pub fn install<M, W>(self, rule: impl Fn(M) -> W) -> Result<Self, EvaluationError>
    where
        M: Application + Default + Into<ConceptDescriptor>,
        W: When,
    {
        let query = M::default();
        let concept: ConceptDescriptor = query.clone().into();
        let when = rule(query).into_premises();
        let premises = when.into_vec();
        let rule =
            DeductiveRule::new(concept, premises).map_err(|e| EvaluationError::Planning {
                message: e.to_string(),
            })?;
        self.register(rule)
    }

    /// Borrow the underlying rule registry.
    pub fn rules(&self) -> &RuleRegistry {
        &self.rules
    }

    /// Mutably borrow the rule registry.
    pub fn rules_mut(&mut self) -> &mut RuleRegistry {
        &mut self.rules
    }

    /// Flush any buffered changes into the prolly tree, returning the
    /// resulting root hash.
    async fn flush(&self) -> Result<Blake3Hash, DialogArtifactsError> {
        // Snapshot under the lock, then release: tree work happens off-lock
        // so other `Self`-holding tasks (clones) can still queue writes.
        let (base, pending) = {
            let mut state = self.state.lock();
            let pending = mem::take(&mut state.pending);
            (state.tree, pending)
        };

        if pending.is_empty() {
            return Ok(base);
        }

        let mut store = LocalIndex::new(&self.env, self.catalog.clone());
        let mut tree: artifact_tree::ArtifactTree = Tree::from_hash(&base, &store)
            .await
            .map_err(DialogArtifactsError::from)?;
        artifact_tree::apply(&mut tree, &mut store, pending.into_stream()).await?;

        let new_hash = tree.hash().copied().unwrap_or(EMPT_TREE_HASH);
        {
            let mut state = self.state.lock();
            state.tree = new_hash;
        }
        Ok(new_hash)
    }
}

impl Update for Layer {
    fn associate(&mut self, the: Attribute, of: Entity, is: Value) {
        self.state.lock().pending.associate(the, of, is);
    }
    fn associate_unique(&mut self, the: Attribute, of: Entity, is: Value) {
        self.state.lock().pending.associate_unique(the, of, is);
    }
    fn dissociate(&mut self, the: Attribute, of: Entity, is: Value) {
        self.state.lock().pending.dissociate(the, of, is);
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<'a> Provider<Select<'a>> for Layer {
    async fn execute(
        &self,
        input: ArtifactSelector<Constrained>,
    ) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
        // Materialize pending writes up front so the lock is released
        // before the returned stream is polled. Then clone the env and
        // catalog into the stream so the resulting `ArtifactStream<'a>`
        // doesn't borrow from `&self` — Volatile is Arc-backed so the
        // clone is cheap.
        let tree_hash = self.flush().await?;
        let env = self.env.clone();
        let catalog = self.catalog.clone();
        Ok(Box::pin(async_stream::try_stream! {
            let store = LocalIndex::new(&env, catalog);
            let tree: artifact_tree::ArtifactTree = Tree::from_hash(&tree_hash, &store)
                .await
                .map_err(DialogArtifactsError::from)?;
            let stream = artifact_tree::scan(tree, store, input);
            tokio::pin!(stream);
            while let Some(item) = stream.next().await {
                yield item?;
            }
        }))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<SelectRules> for Layer {
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        self.rules.acquire(&input)
    }
}

/// A query environment that unions two sources during evaluation.
///
/// `Union` is the data-source counterpart to the planner's
/// [`Disjunction`](dialog_query::planner::Disjunction): where
/// `Disjunction` unions the result streams of alternative plans, `Union`
/// unions the fact streams (and rule sets) of alternative sources. The
/// query sees artifacts from both sides at once, with rules merged per
/// concept so every installed rule contributes to planning.
pub struct Union<P, O> {
    primary: P,
    secondary: O,
}

impl<P, O> Union<P, O> {
    /// Union a primary source with a secondary one.
    pub fn new(primary: P, secondary: O) -> Self {
        Self { primary, secondary }
    }

    /// Borrow the primary source.
    pub fn primary(&self) -> &P {
        &self.primary
    }

    /// Borrow the secondary source.
    pub fn secondary(&self) -> &O {
        &self.secondary
    }
}

impl<P: Clone, O: Clone> Clone for Union<P, O> {
    fn clone(&self) -> Self {
        Self {
            primary: self.primary.clone(),
            secondary: self.secondary.clone(),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<'a, P, O> Provider<Select<'a>> for Union<P, O>
where
    P: Provider<Select<'a>> + ConditionalSync,
    O: Provider<Select<'a>> + ConditionalSync,
{
    async fn execute(
        &self,
        input: ArtifactSelector<Constrained>,
    ) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
        let primary = self.primary.execute(input.clone()).await?;
        let secondary = self.secondary.execute(input).await?;
        Ok(merge_grouped(vec![primary, secondary]))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<P, O> Provider<SelectRules> for Union<P, O>
where
    P: Provider<SelectRules> + ConditionalSync,
    O: Provider<SelectRules> + ConditionalSync,
{
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        let mut primary = self.primary.execute(input.clone()).await?;
        let secondary = self.secondary.execute(input).await?;
        primary.extend(&secondary);
        Ok(primary)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_artifacts::Artifact;
    use dialog_query::the;
    use futures_util::StreamExt;

    fn artifact(of: &str, the: &str, is: &str) -> Artifact {
        Artifact {
            the: the.parse().unwrap(),
            of: of.parse().unwrap(),
            is: Value::String(is.into()),
            cause: None,
        }
    }

    #[dialog_common::test]
    async fn layer_assert_then_select_round_trips() -> anyhow::Result<()> {
        let alice: Entity = "id:alice".parse()?;
        let layer = Layer::new().assert(
            the!("person/name")
                .of(alice.clone())
                .is("Alice".to_string()),
        );

        let selector = ArtifactSelector::new().the("person/name".parse()?);
        let stream = Provider::<Select<'_>>::execute(&layer, selector).await?;
        let results: Vec<_> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].of, alice);
        assert_eq!(results[0].is, Value::String("Alice".into()));
        Ok(())
    }

    #[dialog_common::test]
    async fn layer_retract_undoes_assert() -> anyhow::Result<()> {
        let alice: Entity = "id:alice".parse()?;
        let stmt = the!("person/name")
            .of(alice.clone())
            .is("Alice".to_string());
        let layer = Layer::new().assert(stmt.clone()).retract(stmt);

        let selector = ArtifactSelector::new().the("person/name".parse()?);
        let stream = Provider::<Select<'_>>::execute(&layer, selector).await?;
        let results: Vec<_> = stream.collect::<Vec<_>>().await;
        assert!(results.is_empty());
        Ok(())
    }

    #[dialog_common::test]
    async fn layer_cardinality_one_replace_supersedes() -> anyhow::Result<()> {
        // Drive `associate_unique` (cardinality-one) directly so the tree
        // takes the Replace path: only the latest value should survive.
        let alice: Entity = "id:alice".parse()?;
        let mut layer = Layer::new();
        Update::associate_unique(
            &mut layer,
            "person/name".parse()?,
            alice.clone(),
            Value::String("Alice".into()),
        );
        Update::associate_unique(
            &mut layer,
            "person/name".parse()?,
            alice.clone(),
            Value::String("Alicia".into()),
        );

        let selector = ArtifactSelector::new().of(alice);
        let stream = Provider::<Select<'_>>::execute(&layer, selector).await?;
        let results: Vec<Artifact> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, Value::String("Alicia".into()));
        Ok(())
    }

    #[dialog_common::test]
    async fn union_chains_select_streams_from_both_sides() -> anyhow::Result<()> {
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        let primary = Layer::new().assert(
            the!("person/name")
                .of(alice.clone())
                .is("Alice".to_string()),
        );
        let secondary =
            Layer::new().assert(the!("person/name").of(bob.clone()).is("Bob".to_string()));

        let combined = Union::new(primary, secondary);
        let selector = ArtifactSelector::new().the("person/name".parse()?);
        let stream = Provider::<Select<'_>>::execute(&combined, selector).await?;
        let results: Vec<Artifact> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;

        assert_eq!(results.len(), 2);
        let entities: Vec<_> = results.into_iter().map(|a| a.of).collect();
        assert!(entities.contains(&alice));
        assert!(entities.contains(&bob));
        Ok(())
    }

    /// Two layers each holding the same set of facts. Run the same dedup
    /// expectation through all three scan modes (EAV / AEV / VAE) by
    /// varying which selector field is constrained, and check that the
    /// duplicates collapse to exactly the expected unique count in every
    /// mode.
    ///
    /// The reason every mode works: the per-group HashSet in
    /// `merge_grouped` resets when `group_key = (the, of)` advances,
    /// which is correct only if every input stream is sorted by
    /// `group_key`. That holds because:
    ///
    /// - EAV scan with `.of()`: entity fixed → effective order is
    ///   `(attribute, value)`, group_key `(attribute, fixed_entity)`
    ///   sorts monotonically by attribute.
    /// - AEV scan with `.the()`: attribute fixed → effective order
    ///   `(entity, value)`, group_key `(fixed_attribute, entity)` sorts
    ///   monotonically by entity.
    /// - VAE scan with `.is()`: value fixed and the VAE key byte layout
    ///   is `[TAG, VALUE_TYPE, VALUE_REF, ATTRIBUTE, ENTITY]` — so the
    ///   effective order is `(attribute, entity)`, exactly equal to
    ///   `group_key`. If the VAE key layout ever changes to put entity
    ///   before attribute, the VAE assertion below will start failing
    ///   loudly.
    async fn union_dedupes_for_selector(
        selector_for: impl Fn(&Entity) -> ArtifactSelector<Constrained>,
        expected_unique: usize,
        label: &str,
    ) -> anyhow::Result<()> {
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        let facts = [
            (alice.clone(), "person/name"),
            (alice.clone(), "person/role"),
            (bob.clone(), "person/name"),
            (bob.clone(), "person/role"),
        ];

        let build = || -> anyhow::Result<Layer> {
            let mut layer = Layer::new();
            for (entity, attribute) in &facts {
                Update::associate(
                    &mut layer,
                    attribute.parse()?,
                    entity.clone(),
                    Value::String("X".into()),
                );
            }
            Ok(layer)
        };

        let combined = Union::new(build()?, build()?);
        let stream = Provider::<Select<'_>>::execute(&combined, selector_for(&alice)).await?;
        let results: Vec<Artifact> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;

        use std::collections::HashMap;
        let mut counts: HashMap<Cause, usize> = HashMap::new();
        for a in &results {
            *counts.entry(Cause::from(a)).or_default() += 1;
        }
        for (fingerprint, count) in &counts {
            assert_eq!(
                *count, 1,
                "[{label}] artifact with fingerprint {fingerprint} appeared {count} times \
                 in the union output; expected exactly once. Full results: {results:?}"
            );
        }
        assert_eq!(
            counts.len(),
            expected_unique,
            "[{label}] expected {expected_unique} unique artifacts; got {} in {results:?}",
            counts.len()
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn union_dedupes_in_eav_mode() -> anyhow::Result<()> {
        // selector constrains entity → tree scan uses EAV. Expect
        // alice's two facts (alice/name, alice/role), deduplicated to 2.
        union_dedupes_for_selector(
            |alice| ArtifactSelector::new().of(alice.clone()),
            2,
            "EAV",
        )
        .await
    }

    #[dialog_common::test]
    async fn union_dedupes_in_aev_mode() -> anyhow::Result<()> {
        // selector constrains attribute (no entity) → tree scan uses
        // AEV. Expect (alice/name, bob/name), deduplicated to 2.
        union_dedupes_for_selector(
            |_| ArtifactSelector::new().the("person/name".parse().unwrap()),
            2,
            "AEV",
        )
        .await
    }

    #[dialog_common::test]
    async fn union_dedupes_in_vae_mode() -> anyhow::Result<()> {
        // selector constrains value only → tree scan uses VAE. Expect
        // all four (alice/name, alice/role, bob/name, bob/role)
        // collapsed across the two layers to 4 unique artifacts.
        union_dedupes_for_selector(
            |_| ArtifactSelector::new().is(Value::String("X".into())),
            4,
            "VAE",
        )
        .await
    }

    #[dialog_common::test]
    async fn union_dedupes_identical_claims_from_two_layers() -> anyhow::Result<()> {
        // Two layers asserting the literally same (the, of, is, cause).
        // The same fact existing in two places is still one fact — the
        // user should see one row, not two.
        let alice: Entity = "id:alice".parse()?;
        let primary = Layer::new().assert(
            the!("person/name")
                .of(alice.clone())
                .is("Alice".to_string()),
        );
        let secondary = Layer::new().assert(
            the!("person/name")
                .of(alice.clone())
                .is("Alice".to_string()),
        );

        let combined = Union::new(primary, secondary);
        let selector = ArtifactSelector::new().the("person/name".parse()?);
        let stream = Provider::<Select<'_>>::execute(&combined, selector).await?;
        let results: Vec<Artifact> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;

        assert_eq!(
            results.len(),
            1,
            "identical claims from two layers must be deduplicated; got {results:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn union_dedupes_when_duplicate_interleaves_with_distinct_value() -> anyhow::Result<()> {
        // Primary asserts both (alice, AliceA) and (alice, AliceB);
        // secondary asserts just (alice, AliceA). The duplicate AliceA
        // sits across streams with a distinct AliceB in between in
        // primary's scan order, so naive consecutive-only dedup would
        // miss it. We still expect two unique rows total.
        let alice: Entity = "id:alice".parse()?;
        let primary = Layer::new()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("AliceA".to_string()),
            )
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("AliceB".to_string()),
            );
        let secondary = Layer::new().assert(
            the!("person/name")
                .of(alice.clone())
                .is("AliceA".to_string()),
        );

        let combined = Union::new(primary, secondary);
        let selector = ArtifactSelector::new().the("person/name".parse()?);
        let stream = Provider::<Select<'_>>::execute(&combined, selector).await?;
        let results: Vec<Artifact> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;

        let mut values: Vec<_> = results.iter().map(|a| a.is.clone()).collect();
        values.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        assert_eq!(
            values,
            vec![
                Value::String("AliceA".into()),
                Value::String("AliceB".into())
            ],
            "duplicate (alice, AliceA) across layers must dedupe to one row; got {results:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn union_preserves_cardinality_one_grouping_across_layers() -> anyhow::Result<()> {
        // Two layers each holding a fact for `(alice, name)` and
        // `(bob, name)`. `merge_grouped` inside Union must keep
        // same-(the, of) items consecutive so the cardinality-one
        // sliding window in only.rs yields one winner per entity.
        use dialog_query::query::Output;
        use dialog_query::{Cardinality, Term};

        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;

        let primary = Layer::new()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("AliceA".to_string()),
            )
            .assert(the!("person/name").of(bob.clone()).is("BobA".to_string()));
        let secondary = Layer::new()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("AliceB".to_string()),
            )
            .assert(the!("person/name").of(bob.clone()).is("BobB".to_string()));

        let env = Union::new(primary, secondary);
        let results = the!("person/name")
            .of(Term::<Entity>::var("person"))
            .is(Term::<String>::var("name"))
            .cardinality(Cardinality::One)
            .perform(&env)
            .try_vec()
            .await?;
        assert_eq!(
            results.len(),
            2,
            "cardinality-one must yield exactly one row per entity \
             when the same (the, of) appears across union members"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn merge_grouped_interleaves_by_group_key() -> anyhow::Result<()> {
        use futures_util::stream;

        // Pre-sorted by (the, of); merge_grouped should still produce a
        // single stream where same-(the, of) items are consecutive.
        let alice_a = artifact("id:alice", "person/name", "AliceA");
        let bob_a = artifact("id:bob", "person/name", "BobA");
        let alice_b = artifact("id:alice", "person/name", "AliceB");
        let bob_b = artifact("id:bob", "person/name", "BobB");

        let mut a = vec![alice_a.clone(), bob_a.clone()];
        a.sort_by_key(group_key);
        let mut b = vec![alice_b.clone(), bob_b.clone()];
        b.sort_by_key(group_key);

        let s_a: ArtifactStream<'_> = Box::pin(stream::iter(a.into_iter().map(Ok)));
        let s_b: ArtifactStream<'_> = Box::pin(stream::iter(b.into_iter().map(Ok)));
        let merged = merge_grouped(vec![s_a, s_b]);
        let results: Vec<Artifact> = merged
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;

        assert_eq!(results.len(), 4);
        // Every group key appears in at most one contiguous run.
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        let mut current: Option<(Vec<u8>, Vec<u8>)> = None;
        for r in &results {
            let key = group_key(r);
            if current.as_ref() != Some(&key) {
                assert!(
                    seen.insert(key.clone()),
                    "merge_grouped lost (the, of) grouping"
                );
                current = Some(key);
            }
        }
        Ok(())
    }

    #[dialog_common::test]
    async fn merge_grouped_empty_inputs_yield_empty_stream() -> anyhow::Result<()> {
        let merged: ArtifactStream<'static> = merge_grouped(Vec::new());
        let results: Vec<_> = merged.collect::<Vec<_>>().await;
        assert!(results.is_empty());
        Ok(())
    }

    #[dialog_common::test]
    async fn merge_grouped_single_stream_passes_through() -> anyhow::Result<()> {
        use futures_util::stream;
        let one = artifact("id:alice", "person/name", "Alice");
        let s: ArtifactStream<'_> =
            Box::pin(stream::iter(vec![one.clone()].into_iter().map(Ok)));
        let merged = merge_grouped(vec![s]);
        let results: Vec<Artifact> = merged
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        assert_eq!(results, vec![one]);
        Ok(())
    }

    #[dialog_common::test]
    async fn union_primary_and_secondary_accessors_borrow_underlying() {
        let primary = Layer::new();
        let secondary = Layer::new();
        let combined = Union::new(primary, secondary);
        // Accessors compile-check return types; both Layers stay borrowable.
        let _ = combined.primary();
        let _ = combined.secondary();
    }
}
