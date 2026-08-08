//! Commit-time induction: trigger-indexed dispatch of inductive rules.
//!
//! Runs as step 0 of a transaction commit
//! ([`TransactionCommit::perform`](super::TransactionCommit::perform)),
//! before the durable batch is applied. The commit's delta — durable
//! asserts, durable retracts, and dispatched transients alike — seeds a
//! round loop:
//!
//! 1. The round's stimulus yields the set of *touched attributes*; each
//!    probes the `dialog.rule/on` trigger index (against the transaction
//!    view, so rules installed in the same commit fire). Rules that
//!    don't watch a touched attribute are never loaded, planned, or
//!    evaluated — dispatch cost follows the delta, not the rule
//!    population.
//! 2. Each candidate's body evaluates against the frozen round view
//!    (branch ⊕ durable changes ⊕ this round's transients). Sibling
//!    rules in a round read identical state.
//! 3. Bound heads emit facts by cardinality (`Replace` for one,
//!    `Assert` for many). A head concept carrying the
//!    `dialog.concept/transient` marker routes to the next round's
//!    stimulus and is never committed; a durable head passes a novelty
//!    check (an instruction that leaves the view unchanged contributes
//!    nothing) and folds into the commit.
//! 4. The loop ends when a round produces no novelty and no
//!    transients, or errors after [`MAX_ROUNDS`] rounds — the runaway
//!    guard for self-feeding cascades.
//!
//! What is deliberately *not* here yet (see `notes/inductive-rules.md`):
//! the per-head trigger footprint and alpha-discrimination caches that
//! keep hot-attribute fan-out flat, delta-restricted body evaluation,
//! and the `retract!` head polarity.

use std::collections::{BTreeSet, HashMap};

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, Attribute, Change, Changes, Entity, Instruction, Select, Statement,
    Value,
};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::memory::Resolve;
use dialog_query::rule::inductive::Polarity;
use dialog_query::{Any, Binding, Cardinality, Environment, InductiveRule, Match, Term};
use futures_util::TryStreamExt;

use crate::layer::tombstones_from;
use crate::repository::branch::QueryLayer;
use crate::repository::branch::session::QueryEnv;
use crate::rules::{
    TriggerFootprint, hydrate, hydrate_inductive, on_attr, on_entity, reads_attr, source_attr,
    transient_attr,
};
use crate::{Branch, CommitError, RemoteSite, Revision};

/// Round bound for the induction loop: a cascade still emitting
/// transients or novelty after this many rounds fails the commit
/// rather than diverging (the same posture SQL databases take with
/// recursive trigger depth).
pub(crate) const MAX_ROUNDS: u32 = 16;

/// Run commit-time induction over `changes` + `transients`, folding
/// durable novelty into `changes`. Transients never enter `changes`;
/// they are visible to rule bodies for exactly one round.
pub(crate) async fn induce<Env>(
    branch: &Branch,
    changes: &mut Changes,
    transients: Changes,
    env: &Env,
) -> Result<(), CommitError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Identify>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    // Round 1 stimulus: everything the commit changes, plus the
    // watermark lag — facts that entered the branch since the last
    // inducing instant (a pull, a raw commit, a crash between publish
    // and induce). Every head advance is an instant; the lag is how a
    // missed one is caught up.
    let mut stimulus: Vec<Instruction> = changes.clone().into_instructions();
    stimulus.extend(transients.clone().into_instructions());
    stimulus.extend(lag_delta(branch, env).await?);
    if stimulus.is_empty() {
        return Ok(());
    }

    // Committed trigger structures, resolved once per induction: the
    // footprint (which `on:` keys exist at all — the O(1) gate) and
    // the head it was scanned at, which keys every committed-slice
    // cache lookup below. The overlay slice is never head-cached; it
    // is re-scanned each round (cheap, in-memory) so rules installed
    // by this very commit — or by a rule during induction — fire.
    let dispatch = Dispatch::resolve(branch, env).await?;

    // The identity is resolved once: it only feeds the schema-metadata
    // overlay of the round view, which does not change across rounds.
    let operator = Identify.perform(env).await?;

    // This round's transient facts. Round 1 sees the dispatched
    // commands; round N+1 sees only the transients round N emitted —
    // a transient lives for exactly one round.
    let mut transient_overlay = transients;

    let mut round: u32 = 0;
    while !stimulus.is_empty() {
        round += 1;
        if round > MAX_ROUNDS {
            return Err(CommitError::InductionDivergence(MAX_ROUNDS));
        }

        // Probe keys straight off the instructions — no schema lookup.
        // The rows are kept too: assert/replace rows seed
        // delta-restricted evaluation; retract and replace *attributes*
        // decide when a candidate needs the full-body fallback (a
        // removal can newly enable a rule only through `unless`, which
        // a seed cannot express).
        let mut assert_rows: Vec<Artifact> = Vec::new();
        let mut retract_attrs: BTreeSet<Attribute> = BTreeSet::new();
        let mut replace_attrs: BTreeSet<Attribute> = BTreeSet::new();
        for instruction in &stimulus {
            match instruction {
                Instruction::Assert(a) => assert_rows.push(a.clone()),
                Instruction::Replace(a) => {
                    assert_rows.push(a.clone());
                    replace_attrs.insert(a.the.clone());
                }
                Instruction::Retract(a) => {
                    retract_attrs.insert(a.the.clone());
                }
            }
        }
        let mut touched: BTreeSet<Attribute> = stimulus
            .iter()
            .map(|instruction| match instruction {
                Instruction::Assert(a) | Instruction::Replace(a) | Instruction::Retract(a) => {
                    a.the.clone()
                }
            })
            .collect();
        let direct = touched.clone();

        // The overlay's trigger slice: rules, markers, and support
        // edges staged in this transaction (including novelty from
        // earlier rounds).
        let overlay = OverlayTriggers::scan(changes);

        // Installation handles current state: a rule is itself a fact,
        // so the circumstance "rule exists ∧ premises hold" completes
        // at the commit that installs it — the same instant semantics
        // as any other conjunction, and the propagator discipline
        // (attaching alerts once over current contents). An installed
        // inductive rule (its `dialog.rule/on` rows in this stimulus —
        // whether staged here or arriving through the watermark lag)
        // becomes a full-evaluation candidate; an installed deductive
        // rule (its `dialog.rule/reads` rows) makes its conclusions
        // derived-touched, so rules premised on the newly derivable
        // concepts re-evaluate.
        let on = on_attr();
        let reads = reads_attr();
        let mut installed: BTreeSet<Entity> = BTreeSet::new();
        let mut installed_deductive: BTreeSet<Entity> = BTreeSet::new();
        for instruction in &stimulus {
            if let Instruction::Assert(a) | Instruction::Replace(a) = instruction {
                if a.the == on {
                    installed.insert(a.of.clone());
                } else if a.the == reads {
                    installed_deductive.insert(a.of.clone());
                }
            }
        }
        for entity in &installed_deductive {
            let Some(body) = dispatch.deductive(entity, &overlay, env).await? else {
                continue;
            };
            for (_, field) in body.conclusion().with().iter() {
                // Inserted after the `direct` snapshot, so these land
                // in the expanded set and force full evaluation.
                touched.insert(field.descriptor().the().clone().into());
            }
        }

        // The frozen round view: branch ⊕ durable changes ⊕ this
        // round's transients, through the same layered QueryEnv a
        // transaction query uses, so rule bodies read exactly what a
        // mid-transaction query would.
        let mut view_changes = changes.clone();
        transient_overlay.clone().assert(&mut view_changes);
        let layered = QueryLayer::from(branch)
            .with(view_changes)
            .overlay(&operator);
        let tombstones = tombstones_from(&layered);
        let view = QueryEnv::new(vec![branch.clone()], layered, tombstones, env);

        // Close the touched set over derivation: a base-fact write
        // reaches inductive rules premised on the derived concepts it
        // (transitively) supports through deductive rules.
        dispatch
            .expand_through_deduction(&mut touched, &overlay, env)
            .await?;

        // Trigger-indexed discovery: footprint gate, then one
        // `dialog.rule/on` lookup (head-cached) per surviving attribute.
        // Nothing ever enumerates all rules.
        let mut candidates: BTreeSet<Entity> = BTreeSet::new();
        for attribute in &touched {
            let Some(on) = on_entity(attribute) else {
                continue;
            };
            candidates.extend(dispatch.triggers(&on, &overlay, env).await?);
        }
        candidates.extend(installed.iter().cloned());

        // Attributes only reachable through the deductive closure: a
        // candidate premised on one changed *derivedly*, which a base
        // row cannot seed.
        let expanded: BTreeSet<Attribute> = touched.difference(&direct).cloned().collect();

        let mut novelty = Changes::new();
        let mut emitted_transients = Changes::new();
        for entity in candidates {
            let Some(rule) = dispatch.load(&entity, &overlay, env).await? else {
                continue;
            };
            let transient_head = dispatch
                .is_transient(&rule.conclusion().this(), &overlay, env)
                .await?;

            // Delta restriction: bind stimulus rows into the premises
            // they match and evaluate with those bindings fixed, so
            // cost follows the delta's join fan-out rather than
            // relation size. The full-body fallback covers what a
            // seed cannot express: enabling by removal (`unless` over
            // a retracted or superseded fact) and premises that
            // changed derivedly through the deductive closure.
            let (positive_attrs, unless_attrs) = premise_attrs(&rule);
            let full = installed.contains(&entity)
                || expanded
                    .iter()
                    .any(|a| positive_attrs.contains(a) || unless_attrs.contains(a))
                || retract_attrs.iter().any(|a| unless_attrs.contains(a))
                || replace_attrs.iter().any(|a| unless_attrs.contains(a));
            if full {
                fire(
                    &rule,
                    transient_head,
                    &view,
                    &mut novelty,
                    &mut emitted_transients,
                )
                .await?;
            } else {
                fire_seeded(
                    &rule,
                    transient_head,
                    &assert_rows,
                    &view,
                    &mut novelty,
                    &mut emitted_transients,
                )
                .await?;
            }
        }

        // Fold durable novelty into the commit; promote this round's
        // emissions to the next round's stimulus. The incoming
        // transients simply expire — nothing entered `changes`, so
        // there is no sweep.
        stimulus = novelty.clone().into_instructions();
        stimulus.extend(emitted_transients.clone().into_instructions());
        novelty.assert(changes);
        transient_overlay = emitted_transients;
    }

    Ok(())
}

/// The committed side of trigger dispatch for one induction run: the
/// branch, the head every cache entry is keyed by, and the trigger
/// footprint (the O(1) gate). All committed lookups flow through the
/// branch's shared [`RuleCache`](crate::RuleCache) under the
/// established disciplines — discovery head-keyed, hydrated bodies
/// content-addressed, the overlay never head-cached.
struct Dispatch<'a> {
    branch: &'a Branch,
    head: Option<Revision>,
    footprint: TriggerFootprint,
}

/// The transaction overlay's trigger slice, re-scanned each round:
/// rules, support edges, transience markers, and their retractions
/// staged (or derived) in this very commit.
#[derive(Default)]
struct OverlayTriggers {
    /// `on:` entity → inductive-rule entities asserted in the overlay.
    on: HashMap<Entity, Vec<Entity>>,
    /// `on:` entity → deductive-rule entities asserted in the overlay.
    reads: HashMap<Entity, Vec<Entity>>,
    /// Rule entity → staged `dialog.rule/source` bytes.
    sources: HashMap<Entity, Vec<u8>>,
    /// Concepts marked transient in the overlay.
    transient: BTreeSet<Entity>,
    /// Concepts whose transient marker is retracted in the overlay.
    unmarked: BTreeSet<Entity>,
    /// Rule entities whose `dialog.rule/source` is retracted in the
    /// overlay — excluded from dispatch even if the committed slice
    /// still lists them.
    removed: BTreeSet<Entity>,
}

impl OverlayTriggers {
    fn scan(changes: &Changes) -> Self {
        let on = on_attr();
        let reads = reads_attr();
        let source = source_attr();
        let transient = transient_attr();

        let mut slice = OverlayTriggers::default();
        for (entity, attribute, change) in changes.iter() {
            if *attribute == on {
                if let Change::Assert(Value::Entity(key)) | Change::Replace(Value::Entity(key)) =
                    change
                {
                    slice
                        .on
                        .entry(key.clone())
                        .or_default()
                        .push(entity.clone());
                }
            } else if *attribute == reads {
                if let Change::Assert(Value::Entity(key)) | Change::Replace(Value::Entity(key)) =
                    change
                {
                    slice
                        .reads
                        .entry(key.clone())
                        .or_default()
                        .push(entity.clone());
                }
            } else if *attribute == source {
                match change {
                    Change::Assert(Value::Bytes(bytes)) | Change::Replace(Value::Bytes(bytes)) => {
                        slice.sources.insert(entity.clone(), bytes.clone());
                    }
                    Change::Retract(_) => {
                        slice.removed.insert(entity.clone());
                    }
                    _ => {}
                }
            } else if *attribute == transient {
                match change {
                    Change::Assert(_) | Change::Replace(_) => {
                        slice.transient.insert(entity.clone());
                    }
                    Change::Retract(_) => {
                        slice.unmarked.insert(entity.clone());
                    }
                }
            }
        }
        slice
    }
}

impl<'a> Dispatch<'a> {
    /// Resolve the committed dispatch state: the branch head and the
    /// trigger footprint at it (cached per head; one range scan over
    /// each of `dialog.rule/on` and `dialog.rule/reads` on a miss).
    async fn resolve<Env>(branch: &'a Branch, env: &Env) -> Result<Dispatch<'a>, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let head = branch.revision();
        let Some(head) = head else {
            // A branch with no commits has no committed rules.
            return Ok(Dispatch {
                branch,
                head: None,
                footprint: TriggerFootprint::default(),
            });
        };

        let cache = branch.rule_cache();
        let footprint = match cache.footprint(&head) {
            Some(footprint) => footprint,
            None => {
                let mut footprint = TriggerFootprint::default();
                for claim in committed(branch, ArtifactSelector::new().the(on_attr()), env).await? {
                    if let Value::Entity(key) = claim.is {
                        footprint.on.insert(key);
                    }
                }
                for claim in
                    committed(branch, ArtifactSelector::new().the(reads_attr()), env).await?
                {
                    if let Value::Entity(key) = claim.is {
                        footprint.reads.insert(key);
                    }
                }
                cache.record_footprint(head.clone(), footprint.clone());
                footprint
            }
        };
        Ok(Dispatch {
            branch,
            head: Some(head),
            footprint,
        })
    }

    /// The inductive-rule entities watching `on`: the committed slice
    /// (footprint-gated, head-cached) unioned with the overlay's,
    /// minus rules the overlay retracts.
    async fn triggers<Env>(
        &self,
        on: &Entity,
        overlay: &OverlayTriggers,
        env: &Env,
    ) -> Result<Vec<Entity>, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let mut entities: Vec<Entity> = Vec::new();
        if let Some(head) = &self.head
            && self.footprint.on.contains(on)
        {
            let cache = self.branch.rule_cache();
            let committed_entities = match cache.triggers(on, head) {
                Some(entities) => entities,
                None => {
                    let selector = ArtifactSelector::new()
                        .the(on_attr())
                        .is(Value::Entity(on.clone()));
                    let entities: Vec<Entity> = committed(self.branch, selector, env)
                        .await?
                        .into_iter()
                        .map(|claim| claim.of)
                        .collect();
                    cache.record_triggers(on.clone(), head.clone(), entities.clone());
                    entities
                }
            };
            entities.extend(committed_entities);
        }
        if let Some(staged) = overlay.on.get(on) {
            entities.extend(staged.iter().cloned());
        }
        entities.retain(|entity| !overlay.removed.contains(entity));
        Ok(entities)
    }

    /// Close `touched` over the deductive support graph: for each
    /// touched attribute, `dialog.rule/reads` names the deductive rules
    /// whose bodies read it; their conclusions' attributes are
    /// *derived-touched* and recurse until the frontier is exhausted.
    /// Composed from per-rule facts at dispatch time, never stored, so
    /// late-installed deductive rules are picked up automatically.
    /// Polarity is deliberately ignored across derived edges: through
    /// negation, an assertion of a base fact can retract a derived one.
    async fn expand_through_deduction<Env>(
        &self,
        touched: &mut BTreeSet<Attribute>,
        overlay: &OverlayTriggers,
        env: &Env,
    ) -> Result<(), CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let cache = self.branch.rule_cache();
        let mut frontier: Vec<Attribute> = touched.iter().cloned().collect();
        while let Some(attribute) = frontier.pop() {
            let Some(on) = on_entity(&attribute) else {
                continue;
            };

            let mut readers: Vec<Entity> = Vec::new();
            if let Some(head) = &self.head
                && self.footprint.reads.contains(&on)
            {
                let committed_readers = match cache.reads(&on, head) {
                    Some(entities) => entities,
                    None => {
                        let selector = ArtifactSelector::new()
                            .the(reads_attr())
                            .is(Value::Entity(on.clone()));
                        let entities: Vec<Entity> = committed(self.branch, selector, env)
                            .await?
                            .into_iter()
                            .map(|claim| claim.of)
                            .collect();
                        cache.record_reads(on.clone(), head.clone(), entities.clone());
                        entities
                    }
                };
                readers.extend(committed_readers);
            }
            if let Some(staged) = overlay.reads.get(&on) {
                readers.extend(staged.iter().cloned());
            }
            readers.retain(|entity| !overlay.removed.contains(entity));

            for reader in readers {
                let Some(body) = self.deductive(&reader, overlay, env).await? else {
                    continue;
                };
                for (_, field) in body.conclusion().with().iter() {
                    let derived: Attribute = field.descriptor().the().clone().into();
                    if touched.insert(derived.clone()) {
                        frontier.push(derived);
                    }
                }
            }
        }
        Ok(())
    }

    /// Hydrate a deductive body: content-addressed cache, then overlay
    /// bytes, then the committed source claim. A dangling or
    /// undecodable entry yields `None`, skipped like any dangling
    /// index entry.
    async fn deductive<Env>(
        &self,
        entity: &Entity,
        overlay: &OverlayTriggers,
        env: &Env,
    ) -> Result<Option<dialog_query::DeductiveRule>, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let cache = self.branch.rule_cache();
        if let Some(body) = cache.body(entity) {
            return Ok(Some(body));
        }
        let bytes = match overlay.sources.get(entity) {
            Some(bytes) => Some(bytes.clone()),
            None => self.source_bytes(entity, env).await?,
        };
        Ok(bytes
            .and_then(|bytes| hydrate(&bytes).ok())
            // Content-address check: forged bytes stored under a
            // mismatching entity are inert.
            .filter(|body| body.try_this() == Some(entity.clone()))
            .inspect(|body| {
                cache.record_body(entity.clone(), body.clone());
            }))
    }

    /// Hydrate the inductive rule stored at `entity`:
    /// content-addressed cache, then overlay bytes, then the committed
    /// source claim. A dangling trigger-index entry is skipped rather
    /// than failing the commit.
    async fn load<Env>(
        &self,
        entity: &Entity,
        overlay: &OverlayTriggers,
        env: &Env,
    ) -> Result<Option<InductiveRule>, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let cache = self.branch.rule_cache();
        if let Some(rule) = cache.inductive(entity) {
            return Ok(Some(rule));
        }
        let bytes = match overlay.sources.get(entity) {
            Some(bytes) => Some(bytes.clone()),
            None => self.source_bytes(entity, env).await?,
        };
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        // Undecodable bytes, or bytes whose content address is not the
        // entity they were stored under, are forged or corrupt entries
        // in the carved-out namespace — inert, like any dangling index
        // entry. This check is what makes the `dialog.rule/*`
        // reserved-namespace carve-out safe.
        let Ok(rule) = hydrate_inductive(&bytes) else {
            return Ok(None);
        };
        if rule.try_this() != Some(entity.clone()) {
            return Ok(None);
        }
        cache.record_inductive(entity.clone(), rule.clone());
        Ok(Some(rule))
    }

    /// Whether the concept at `entity` carries the
    /// `dialog.concept/transient` marker: the overlay's verdict wins
    /// (marked or unmarked in this very commit), else the committed
    /// slice, head-cached.
    async fn is_transient<Env>(
        &self,
        concept: &Entity,
        overlay: &OverlayTriggers,
        env: &Env,
    ) -> Result<bool, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        if overlay.transient.contains(concept) {
            return Ok(true);
        }
        if overlay.unmarked.contains(concept) {
            return Ok(false);
        }
        let Some(head) = &self.head else {
            return Ok(false);
        };
        let cache = self.branch.rule_cache();
        if let Some(verdict) = cache.transient(concept, head) {
            return Ok(verdict);
        }
        let selector = ArtifactSelector::new()
            .the(transient_attr())
            .of(concept.clone());
        let verdict = !committed(self.branch, selector, env).await?.is_empty();
        cache.record_transient(concept.clone(), head.clone(), verdict);
        Ok(verdict)
    }

    /// The committed `dialog.rule/source` bytes for a rule entity, if any.
    async fn source_bytes<Env>(
        &self,
        entity: &Entity,
        env: &Env,
    ) -> Result<Option<Vec<u8>>, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        if self.head.is_none() {
            return Ok(None);
        }
        let selector = ArtifactSelector::new()
            .the(source_attr())
            .of(entity.clone());
        Ok(committed(self.branch, selector, env)
            .await?
            .into_iter()
            .find_map(|claim| match claim.is {
                Value::Bytes(bytes) => Some(bytes),
                _ => None,
            }))
    }
}

/// The watermark lag: instructions for every fact that entered or
/// left the branch between the induction watermark and the current
/// head — arrivals as `Assert`, departures as `Retract`. Empty when
/// the watermark is at the head (the steady state: the previous
/// inducing instant advanced it).
///
/// A `None` watermark (this replica has never induced) adopts the
/// current head *without* catch-up: induction is fire-forward — a
/// newly installed rule does not fire retroactively over existing
/// state, and neither does a newly adopted engine over an existing
/// branch. Reserved-namespace facts (`dialog.*` version-control
/// records, which every commit writes) are excluded from the lag, so
/// catching up over N commits stimulates rules with the *data* those
/// commits changed, not their bookkeeping.
async fn lag_delta<Env>(branch: &Branch, env: &Env) -> Result<Vec<Instruction>, CommitError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    use crate::{RepositoryArchiveExt as _, RepositoryMemoryExt as _};
    use dialog_artifacts::tree::{TreeStorageBridge, fetch_spilled};
    use dialog_artifacts::{EntityKey, Key, KeyViewConstruct, State};
    use dialog_common::Blake3Hash as NodeHash;
    use dialog_search_tree::{Change as TreeChange, ContentAddressedStorage};

    let Some(head) = branch.revision() else {
        return Ok(Vec::new());
    };
    let cell = branch.induction_cell();
    cell.resolve().perform(env).await?;
    let Some(watermark) = cell.content() else {
        // Never induced: adopt the head, fire-forward only.
        return Ok(Vec::new());
    };
    if watermark.tree == head.tree {
        return Ok(Vec::new());
    }

    // Walk the tree diff over the EAV region only — each changed fact
    // surfaces once. Reads go through the networked store exactly as a
    // select does: a pulled head's changed paths may reference
    // remote-only blocks.
    let upstreams = branch.upstreams();
    let remote = match upstreams.remote_name() {
        Some(name) => branch
            .subject()
            .remote(name.to_string())
            .load()
            .perform(env)
            .await
            .ok(),
        None => None,
    };
    let store = crate::NetworkedIndex::new(env, branch.archive().index(), remote);
    let raw_store = store.clone();
    let storage = ContentAddressedStorage::new(TreeStorageBridge(store));
    let previous = crate::Index::from_hash_with_cache(
        NodeHash::from(*watermark.tree.hash()),
        branch.node_cache(),
    );
    let next =
        crate::Index::from_hash_with_cache(NodeHash::from(*head.tree.hash()), branch.node_cache());

    let scope = vec![
        <EntityKey<Key> as KeyViewConstruct>::min().into_key()
            ..=<EntityKey<Key> as KeyViewConstruct>::max().into_key(),
    ];
    let diff = previous.differentiate_within(&next, &scope, &storage, &storage);
    let mut diff = Box::pin(diff);

    let mut lag = Vec::new();
    let mut seen = BTreeSet::new();
    while let Some(change) = diff
        .try_next()
        .await
        .map_err(|error| CommitError::Induction(format!("watermark diff: {error}")))?
    {
        let (entry, arriving) = match &change {
            TreeChange::Add(entry) => (entry, true),
            TreeChange::Remove(entry) => (entry, false),
        };
        let State::Added(datum) = &entry.value else {
            continue;
        };
        let spilled = fetch_spilled(&raw_store, &entry.key)
            .await
            .map_err(|error| CommitError::Induction(format!("watermark spilled: {error:?}")))?;
        let fact = Artifact::from_key_datum_with_value(&entry.key, datum, spilled)
            .map_err(|error| CommitError::Induction(format!("watermark datum: {error:?}")))?;
        // Version-control records (which every commit writes) are
        // excluded from the lag; the carved-out rule and marker
        // prefixes pass through, so a rule arriving by pull or raw
        // commit installs at this instant.
        let the = fact.the.to_string();
        if the.starts_with("dialog.")
            && !the.starts_with("dialog.rule/")
            && !the.starts_with("dialog.concept/")
        {
            continue;
        }
        if !seen.insert((
            arriving,
            fact.of.to_string(),
            fact.the.to_string(),
            fact.is.to_bytes(),
        )) {
            continue;
        }
        lag.push(if arriving {
            Instruction::Assert(fact)
        } else {
            Instruction::Retract(fact)
        });
    }
    Ok(lag)
}

/// Collect the artifacts a selector matches on the branch's committed
/// tree (no overlay — the cacheable slice).
async fn committed<Env>(
    branch: &Branch,
    selector: ArtifactSelector<Constrained>,
    env: &Env,
) -> Result<Vec<Artifact>, CommitError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let stream = branch
        .claims()
        .select(selector)
        .perform(env)
        .await
        .map_err(|error| CommitError::Induction(format!("committed probe: {error}")))?;
    stream
        .try_collect()
        .await
        .map_err(|error| CommitError::Induction(format!("committed probe: {error}")))
}

/// Collect the artifacts a selector matches in the layered view.
async fn select<'a, Env>(
    view: &QueryEnv<'a, Env>,
    selector: ArtifactSelector<Constrained>,
) -> Result<Vec<Artifact>, CommitError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let stream = Provider::<Select<'_>>::execute(view, selector)
        .await
        .map_err(|error| CommitError::Induction(format!("dispatch probe: {error}")))?;
    stream
        .try_collect()
        .await
        .map_err(|error| CommitError::Induction(format!("dispatch probe: {error}")))
}

/// Evaluate one rule's body against the frozen round view and emit its
/// head for every binding: transient heads into `transients`, durable
/// heads (novelty-checked against the view) into `novelty`.
async fn fire<'a, Env>(
    rule: &InductiveRule,
    transient_head: bool,
    view: &QueryEnv<'a, Env>,
    novelty: &mut Changes,
    transients: &mut Changes,
) -> Result<(), CommitError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let plan = rule.plan(&Environment::new());
    let matches: Vec<Match> = plan
        .evaluate(Match::new().seed(), view)
        .try_collect()
        .await
        .map_err(|error| CommitError::Induction(format!("rule body: {error}")))?;
    if matches.is_empty() {
        return Ok(());
    }
    emit_matches(rule, transient_head, matches, view, novelty, transients).await
}

/// The attributes a rule's concept premises name, split by polarity:
/// positive premise attributes (seedable by an assert/replace row) and
/// `unless` attributes (only enabled by removal — never seedable).
fn premise_attrs(rule: &InductiveRule) -> (BTreeSet<Attribute>, BTreeSet<Attribute>) {
    use dialog_query::{Negation, Premise, Proposition};

    let mut positive = BTreeSet::new();
    let mut unless = BTreeSet::new();
    for premise in &rule.analysis().premises {
        let (target, query) = match premise {
            Premise::Assert(Proposition::Concept(query)) => (&mut positive, query),
            Premise::Unless(Negation(Proposition::Concept(query))) => (&mut unless, query),
            _ => continue,
        };
        for (_, field) in query.predicate.with().iter() {
            target.insert(field.descriptor().the().clone().into());
        }
    }
    (positive, unless)
}

/// Delta-restricted firing: bind each stimulus row into every positive
/// concept premise that names its attribute, then evaluate the body
/// with those bindings fixed — the remaining premises join against the
/// frozen view through the planner as usual. Every new match this
/// round must bind at least one new row into at least one positive
/// premise (removal-enabled and derived-premise firings take the
/// full-body path instead), so seeding is complete for this candidate
/// class while costing the delta's join fan-out, not relation size.
async fn fire_seeded<'a, Env>(
    rule: &InductiveRule,
    transient_head: bool,
    rows: &[Artifact],
    view: &QueryEnv<'a, Env>,
    novelty: &mut Changes,
    transients: &mut Changes,
) -> Result<(), CommitError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    use dialog_query::{Premise, Proposition};

    let mut matches: Vec<Match> = Vec::new();
    for premise in &rule.analysis().premises {
        let Premise::Assert(Proposition::Concept(query)) = premise else {
            continue;
        };
        for row in rows {
            // The premise fields this row's attribute matches — the
            // row's value binds there, its subject binds `this`.
            let mut matched = Match::new();
            let mut scope = Environment::new();
            let mut seeded = false;
            let mut compatible = true;
            for (name, field) in query.predicate.with().iter() {
                let attribute: Attribute = field.descriptor().the().clone().into();
                if attribute != row.the {
                    continue;
                }
                if !bind_seed(
                    &mut matched,
                    &mut scope,
                    query.terms.get(name),
                    row.is.clone(),
                ) {
                    compatible = false;
                    break;
                }
                seeded = true;
            }
            if !seeded || !compatible {
                continue;
            }
            if !bind_seed(
                &mut matched,
                &mut scope,
                query.terms.get("this"),
                Value::Entity(row.of.clone()),
            ) {
                continue;
            }

            let plan = rule.plan(&scope);
            let seeded_matches: Vec<Match> = plan
                .evaluate(matched.seed(), view)
                .try_collect()
                .await
                .map_err(|error| CommitError::Induction(format!("seeded rule body: {error}")))?;
            matches.extend(seeded_matches);
        }
    }
    if matches.is_empty() {
        return Ok(());
    }
    emit_matches(rule, transient_head, matches, view, novelty, transients).await
}

/// Bind a seed value into a premise term: a named variable binds (and
/// enters the planning scope), a constant must agree, an anonymous or
/// absent term constrains nothing. Returns `false` when the row is
/// incompatible with the premise.
fn bind_seed(
    matched: &mut Match,
    scope: &mut Environment,
    term: Option<&Term<Any>>,
    value: Value,
) -> bool {
    match term {
        Some(
            term @ Term::Variable {
                name: Some(name), ..
            },
        ) => {
            if matched.bind(term, value).is_err() {
                return false;
            }
            scope.add(name);
            true
        }
        Some(Term::Constant(expected)) => *expected == value,
        Some(Term::Variable { name: None, .. }) | None => true,
    }
}

/// Emit a rule's head for every produced match: transient heads into
/// `transients`, durable heads (novelty-checked against the frozen
/// view) into `novelty`.
async fn emit_matches<'a, Env>(
    rule: &InductiveRule,
    transient_head: bool,
    matches: Vec<Match>,
    view: &QueryEnv<'a, Env>,
    novelty: &mut Changes,
    transients: &mut Changes,
) -> Result<(), CommitError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let conclusion = rule.conclusion();
    for matched in matches {
        // The head subject. A rule whose premises leave `this` unbound
        // has no cell to write; skip the binding.
        let this = match matched.lookup(&Term::<Any>::var("this")) {
            Ok(Binding::Present(Value::Entity(entity))) => entity,
            _ => continue,
        };

        let mut head = Changes::new();
        for (name, field) in conclusion.with().iter() {
            let Ok(Binding::Present(value)) = matched.lookup(&Term::<Any>::var(name)) else {
                // Optional head fields the frame didn't bind emit
                // nothing.
                continue;
            };
            let attribute: Attribute = field.descriptor().the().clone().into();
            match rule.polarity() {
                // A retracting head dissociates the exact bound
                // triple, cardinality-independent.
                Polarity::Retract => {
                    dialog_artifacts::Update::dissociate(&mut head, attribute, this.clone(), value);
                }
                Polarity::Assert => match field.descriptor().cardinality() {
                    Cardinality::One => {
                        dialog_artifacts::Update::associate_unique(
                            &mut head,
                            attribute,
                            this.clone(),
                            value,
                        );
                    }
                    Cardinality::Many => {
                        dialog_artifacts::Update::associate(
                            &mut head,
                            attribute,
                            this.clone(),
                            value,
                        );
                    }
                },
            }
        }

        if transient_head {
            head.assert(transients);
            continue;
        }
        for instruction in head.into_instructions() {
            if is_novel(view, &instruction).await? {
                match instruction {
                    Instruction::Assert(a) => {
                        dialog_artifacts::Update::associate(novelty, a.the, a.of, a.is)
                    }
                    Instruction::Replace(a) => {
                        dialog_artifacts::Update::associate_unique(novelty, a.the, a.of, a.is)
                    }
                    Instruction::Retract(a) => {
                        dialog_artifacts::Update::dissociate(novelty, a.the, a.of, a.is)
                    }
                }
            }
        }
    }
    Ok(())
}

/// Whether applying `instruction` would change the view: asserting a
/// triple already present (or replacing with the value already held)
/// contributes nothing, and neither does retracting an absent triple.
/// This is the fixpoint's convergence test — a rule whose head
/// re-derives existing state terminates for free. The view is the
/// frozen round view, so siblings within a round judge novelty against
/// identical state.
async fn is_novel<'a, Env>(
    view: &QueryEnv<'a, Env>,
    instruction: &Instruction,
) -> Result<bool, CommitError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let artifact = match instruction {
        Instruction::Assert(a) | Instruction::Replace(a) | Instruction::Retract(a) => a,
    };
    let selector = ArtifactSelector::new()
        .the(artifact.the.clone())
        .of(artifact.of.clone())
        .is(artifact.is.clone());
    let present = !select(view, selector).await?.is_empty();
    Ok(match instruction {
        Instruction::Assert(_) | Instruction::Replace(_) => !present,
        Instruction::Retract(_) => present,
    })
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};
    use crate::rules::{Induct, Transient, on_entities};
    use crate::{Branch, CommitError, RemoteSite};
    use anyhow::Result;
    use dialog_artifacts::{ArtifactSelector, Entity, Value};
    use dialog_capability::{Fork, Provider};
    use dialog_common::ConditionalSync;
    use dialog_effects::archive::{Get, Put};
    use dialog_effects::memory::Resolve;
    use dialog_query::{ConceptDescriptor, InductiveRule};
    use futures_util::StreamExt as _;
    use serde_json::json;

    /// Collect the values a `(the, of)` pair holds on the branch.
    async fn values<Env>(branch: &Branch, env: &Env, the: &str, of: &Entity) -> Result<Vec<Value>>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let selector = ArtifactSelector::new().the(the.parse()?).of(of.clone());
        let stream = branch.claims().select(selector).perform(env).await?;
        let artifacts: Vec<_> = stream.collect().await;
        Ok(artifacts
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|artifact| artifact.is)
            .collect())
    }

    /// The increment rule: `assert! counter{count: ?prev + 1} when
    /// increment{counter: ?this}, counter{this: ?this, count: ?prev}`.
    fn increment_rule() -> InductiveRule {
        serde_json::from_value(json!({
            "description": "Increment a counter on an increment command",
            "assert!": {
                "with": {
                    "count": { "the": "counter/count", "as": "UnsignedInteger" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "counter": { "the": "cmd.increment/counter", "as": "Entity" }
                        }
                    },
                    "where": {
                        "counter": { "?": { "name": "this" } }
                    }
                },
                {
                    "assert": {
                        "with": {
                            "count": { "the": "counter/count", "as": "UnsignedInteger" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "count": { "?": { "name": "prev" } }
                    }
                },
                {
                    "assert": "math/sum",
                    "where": {
                        "of": { "?": { "name": "prev" } },
                        "with": 1,
                        "is": { "?": { "name": "count" } }
                    }
                }
            ]
        }))
        .expect("increment rule compiles")
    }

    /// The trigger index covers every attribute the body names — the
    /// command's and the durable counter's alike. Durable entries are
    /// what durable-change triggering probes.
    #[dialog_common::test]
    fn it_indexes_all_concept_premise_attributes() {
        let rule = increment_rule();
        let entities: Vec<String> = on_entities(&rule)
            .into_iter()
            .map(|entity| entity.to_string())
            .collect();
        assert_eq!(
            entities,
            vec![
                "on:cmd.increment/counter".to_string(),
                "on:counter/count".to_string()
            ]
        );
    }

    /// A dispatched command fires the rule watching its attribute; the
    /// durable head folds into the same commit; the command itself
    /// never reaches the branch.
    #[dialog_common::test]
    async fn it_induces_counter_increment_from_a_dispatched_command() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let counter: Entity = "ctr:1".parse()?;

        branch
            .transaction()
            .assert(Induct(increment_rule()))
            .assert(
                dialog_query::the!("counter/count")
                    .of(counter.clone())
                    .is(1u64),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let command: Entity = "cmd:1".parse()?;
        branch
            .transaction()
            .dispatch(
                dialog_query::the!("cmd.increment/counter")
                    .of(command)
                    .is(counter.clone()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        assert_eq!(
            values(&branch, &operator, "counter/count", &counter).await?,
            vec![Value::UnsignedInt(2)],
            "the increment must supersede the prior count"
        );
        let command_entity: Entity = "cmd:1".parse()?;
        assert!(
            values(&branch, &operator, "cmd.increment/counter", &command_entity)
                .await?
                .is_empty(),
            "the dispatched command must never reach the branch"
        );
        Ok(())
    }

    /// A durable write triggers a rule with no command anywhere; the
    /// `unless` guard blocks it while its fact holds; *retracting* the
    /// guard's fact newly enables the rule — the retraction is the
    /// trigger, reached through the `unless` premise's index entry.
    #[dialog_common::test]
    async fn it_triggers_on_durable_change_and_on_retraction_via_unless() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let rule: InductiveRule = serde_json::from_value(json!({
            "description": "A described task that is not done is open",
            "assert!": {
                "with": {
                    "desc": { "the": "task.open/desc", "as": "Text" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "desc": { "the": "task/desc", "as": "Text" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "desc": { "?": { "name": "desc" } }
                    }
                }
            ],
            "unless": [
                {
                    "assert": {
                        "with": {
                            "done": { "the": "task/done", "as": "Boolean" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "done": true
                    }
                }
            ]
        }))?;

        branch
            .transaction()
            .assert(Induct(rule))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        // A described-but-done task: the durable write probes the rule
        // (task/desc is indexed), the body matches, the guard blocks.
        let task: Entity = "task:1".parse()?;
        branch
            .transaction()
            .assert(
                dialog_query::the!("task/desc")
                    .of(task.clone())
                    .is("write the note".to_string()),
            )
            .assert(dialog_query::the!("task/done").of(task.clone()).is(true))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert!(
            values(&branch, &operator, "task.open/desc", &task)
                .await?
                .is_empty(),
            "the unless guard must block the firing while task/done holds"
        );

        // Retracting the guard's fact is the trigger: the commit
        // touches task/done, the unless premise's index entry matches,
        // and the now-unguarded rule fires.
        branch
            .transaction()
            .retract(dialog_query::the!("task/done").of(task.clone()).is(true))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert_eq!(
            values(&branch, &operator, "task.open/desc", &task).await?,
            vec![Value::String("write the note".to_string())],
            "retracting the guard must fire the rule"
        );
        Ok(())
    }

    /// A rule's transient head (marked `dialog.concept/transient`) becomes
    /// the next round's stimulus instead of durable novelty: commands
    /// cascade through rounds, only the final durable head lands, and
    /// neither command leaves a trace.
    #[dialog_common::test]
    async fn it_cascades_through_a_transient_intermediate() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let intermediate: ConceptDescriptor = serde_json::from_value(json!({
            "with": {
                "target": { "the": "cmd.stage/target", "as": "Entity" }
            }
        }))?;

        let stage: InductiveRule = serde_json::from_value(json!({
            "assert!": {
                "with": {
                    "target": { "the": "cmd.stage/target", "as": "Entity" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "target": { "the": "cmd.start/target", "as": "Entity" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "target": { "?": { "name": "target" } }
                    }
                }
            ]
        }))?;

        let finish: InductiveRule = serde_json::from_value(json!({
            "assert!": {
                "with": {
                    "target": { "the": "result/target", "as": "Entity" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "target": { "the": "cmd.stage/target", "as": "Entity" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "target": { "?": { "name": "target" } }
                    }
                }
            ]
        }))?;

        branch
            .transaction()
            .assert(Transient(intermediate.this()))
            .assert(Induct(stage))
            .assert(Induct(finish))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let command: Entity = "cmd:start".parse()?;
        let target: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .dispatch(
                dialog_query::the!("cmd.start/target")
                    .of(command.clone())
                    .is(target.clone()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        assert_eq!(
            values(&branch, &operator, "result/target", &command).await?,
            vec![Value::Entity(target)],
            "the cascade must land the final durable head"
        );
        assert!(
            values(&branch, &operator, "cmd.start/target", &command)
                .await?
                .is_empty(),
            "the dispatched command must never reach the branch"
        );
        assert!(
            values(&branch, &operator, "cmd.stage/target", &command)
                .await?
                .is_empty(),
            "the transient intermediate must never reach the branch"
        );
        Ok(())
    }

    /// Two transient-headed rules feeding each other exhaust the round
    /// bound and fail the commit instead of diverging.
    #[dialog_common::test]
    async fn it_errors_on_a_runaway_cascade() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let ping: ConceptDescriptor = serde_json::from_value(json!({
            "with": { "n": { "the": "cmd.ping/n", "as": "UnsignedInteger" } }
        }))?;
        let pong: ConceptDescriptor = serde_json::from_value(json!({
            "with": { "n": { "the": "cmd.pong/n", "as": "UnsignedInteger" } }
        }))?;

        let ping_to_pong: InductiveRule = serde_json::from_value(json!({
            "assert!": {
                "with": { "n": { "the": "cmd.pong/n", "as": "UnsignedInteger" } }
            },
            "when": [{
                "assert": {
                    "with": { "n": { "the": "cmd.ping/n", "as": "UnsignedInteger" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "n": { "?": { "name": "n" } }
                }
            }]
        }))?;
        let pong_to_ping: InductiveRule = serde_json::from_value(json!({
            "assert!": {
                "with": { "n": { "the": "cmd.ping/n", "as": "UnsignedInteger" } }
            },
            "when": [{
                "assert": {
                    "with": { "n": { "the": "cmd.pong/n", "as": "UnsignedInteger" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "n": { "?": { "name": "n" } }
                }
            }]
        }))?;

        branch
            .transaction()
            .assert(Transient(ping.this()))
            .assert(Transient(pong.this()))
            .assert(Induct(ping_to_pong))
            .assert(Induct(pong_to_ping))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let result = branch
            .transaction()
            .dispatch(
                dialog_query::the!("cmd.ping/n")
                    .of("cmd:ping".parse::<Entity>()?)
                    .is(1u64),
            )
            .commit()
            .perform(&operator)
            .await;
        assert!(
            matches!(result, Err(CommitError::InductionDivergence(_))),
            "a transient ping-pong must exhaust the round bound: {result:?}"
        );
        Ok(())
    }

    /// Dispatch is trigger-indexed: a commit touching one command's
    /// attribute fires only the rules watching it — the other
    /// installed rule contributes nothing.
    #[dialog_common::test]
    async fn it_fires_only_rules_watching_touched_attributes() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let stamp = |command: &str, result: &str| -> InductiveRule {
            serde_json::from_value(json!({
                "assert!": {
                    "with": { "target": { "the": result, "as": "Entity" } }
                },
                "when": [{
                    "assert": {
                        "with": { "target": { "the": command, "as": "Entity" } }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "target": { "?": { "name": "target" } }
                    }
                }]
            }))
            .expect("stamp rule compiles")
        };

        branch
            .transaction()
            .assert(Induct(stamp("cmd.x/target", "result.x/target")))
            .assert(Induct(stamp("cmd.y/target", "result.y/target")))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let command: Entity = "cmd:x".parse()?;
        let target: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .dispatch(
                dialog_query::the!("cmd.x/target")
                    .of(command.clone())
                    .is(target.clone()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        assert_eq!(
            values(&branch, &operator, "result.x/target", &command).await?,
            vec![Value::Entity(target)],
            "the watching rule must fire"
        );
        assert!(
            values(&branch, &operator, "result.y/target", &command)
                .await?
                .is_empty(),
            "the rule watching an untouched attribute must not fire"
        );
        Ok(())
    }

    /// The mailbox-with-ack pattern: the message is durable (it
    /// replicates), the ack is a dispatched command, and consumption
    /// is a `retract!` rule joining the ack to its message. The
    /// message's facts are gone after the ack commit; the ack itself
    /// never lands.
    #[dialog_common::test]
    async fn it_consumes_a_message_via_a_retract_rule() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let consume: InductiveRule = serde_json::from_value(json!({
            "retract!": {
                "with": {
                    "body": { "the": "mailbox.message/body", "as": "Text" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "message": { "the": "cmd.ack/message", "as": "Entity" }
                        }
                    },
                    "where": {
                        "message": { "?": { "name": "this" } }
                    }
                },
                {
                    "assert": {
                        "with": {
                            "body": { "the": "mailbox.message/body", "as": "Text" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "body": { "?": { "name": "body" } }
                    }
                }
            ]
        }))?;

        let message: Entity = "msg:1".parse()?;
        let other: Entity = "msg:2".parse()?;
        branch
            .transaction()
            .assert(Induct(consume))
            .assert(
                dialog_query::the!("mailbox.message/body")
                    .of(message.clone())
                    .is("first".to_string()),
            )
            .assert(
                dialog_query::the!("mailbox.message/body")
                    .of(other.clone())
                    .is("second".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        branch
            .transaction()
            .dispatch(
                dialog_query::the!("cmd.ack/message")
                    .of("cmd:ack".parse::<Entity>()?)
                    .is(message.clone()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        assert!(
            values(&branch, &operator, "mailbox.message/body", &message)
                .await?
                .is_empty(),
            "the acked message must be consumed"
        );
        assert_eq!(
            values(&branch, &operator, "mailbox.message/body", &other).await?,
            vec![Value::String("second".to_string())],
            "the shared ?this join must scope consumption to the acked message"
        );
        Ok(())
    }

    /// The inbox/duty scenario with the duty status *derived*: the
    /// inductive rule's premise names `actor.status/duty`, which no
    /// commit ever writes — a deductive rule concludes it from
    /// `shift/duty`. A message arrives while the actor is off duty
    /// (rule probed via the inbox attributes, join fails); then a
    /// `shift/duty` write flips the derived status. The dispatch
    /// closure must carry that base write through `dialog.rule/reads` to
    /// the derived attribute so the inductive rule fires against the
    /// message already in the store.
    #[dialog_common::test]
    async fn it_triggers_through_a_deductive_premise() -> Result<()> {
        use crate::rules::Deduce;
        use dialog_query::DeductiveRule;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // actor.status/duty of ?a is ?d  when  shift/duty of ?a is ?d
        let status: DeductiveRule = serde_json::from_value(json!({
            "deduce": {
                "with": { "duty": { "the": "actor.status/duty", "as": "Text" } }
            },
            "when": [{
                "assert": {
                    "with": { "duty": { "the": "shift/duty", "as": "Text" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "duty": { "?": { "name": "duty" } }
                }
            }]
        }))?;

        // task/note when a message's actor is (derivedly) on duty.
        let notify: InductiveRule = serde_json::from_value(json!({
            "assert!": {
                "with": { "note": { "the": "task/note", "as": "Text" } }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "actor": { "the": "inbox.message/actor", "as": "Entity" },
                            "body": { "the": "inbox.message/body", "as": "Text" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "actor": { "?": { "name": "actor" } },
                        "body": { "?": { "name": "note" } }
                    }
                },
                {
                    "assert": {
                        "with": { "duty": { "the": "actor.status/duty", "as": "Text" } }
                    },
                    "where": {
                        "this": { "?": { "name": "actor" } },
                        "duty": "on-duty"
                    }
                }
            ]
        }))?;

        branch
            .transaction()
            .assert(Deduce(status))
            .assert(Induct(notify))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        // A message for an off-duty actor: probed via the inbox
        // attributes, the derived-status premise fails, nothing fires.
        let message: Entity = "msg:1".parse()?;
        let actor: Entity = "actor:1".parse()?;
        branch
            .transaction()
            .assert(
                dialog_query::the!("inbox.message/actor")
                    .of(message.clone())
                    .is(actor.clone()),
            )
            .assert(
                dialog_query::the!("inbox.message/body")
                    .of(message.clone())
                    .is("hello".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert!(
            values(&branch, &operator, "task/note", &message)
                .await?
                .is_empty(),
            "an off-duty actor's message must not fire the rule"
        );

        // The base-fact write that flips the derived status: touches
        // only shift/duty, which the inductive rule never names — the
        // deductive closure is what must carry it through.
        branch
            .transaction()
            .assert(
                dialog_query::the!("shift/duty")
                    .of(actor.clone())
                    .is("on-duty".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert_eq!(
            values(&branch, &operator, "task/note", &message).await?,
            vec![Value::String("hello".to_string())],
            "the shift write must reach the rule through the derived premise"
        );
        Ok(())
    }

    /// Trigger discovery is head-cached: a rule installed *after* a
    /// dispatch at an earlier head must still be found when the same
    /// attribute is touched again — the head advance invalidates the
    /// cached discovery and the re-scan picks the new rule up.
    #[dialog_common::test]
    async fn it_rescans_triggers_after_a_head_advance() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let stamp = |result: &str| -> InductiveRule {
            serde_json::from_value(json!({
                "assert!": {
                    "with": { "target": { "the": result, "as": "Entity" } }
                },
                "when": [{
                    "assert": {
                        "with": { "target": { "the": "cmd.z/target", "as": "Entity" } }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "target": { "?": { "name": "target" } }
                    }
                }]
            }))
            .expect("stamp rule compiles")
        };

        // First rule installed; a dispatch warms the discovery cache
        // at this head.
        branch
            .transaction()
            .assert(Induct(stamp("result.first/target")))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        let target: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .dispatch(
                dialog_query::the!("cmd.z/target")
                    .of("cmd:z1".parse::<Entity>()?)
                    .is(target.clone()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        // Second rule on the same attribute — the head advances, so
        // the cached discovery for on:cmd.z/target is stale now.
        branch
            .transaction()
            .assert(Induct(stamp("result.second/target")))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let command: Entity = "cmd:z2".parse()?;
        branch
            .transaction()
            .dispatch(
                dialog_query::the!("cmd.z/target")
                    .of(command.clone())
                    .is(target.clone()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        assert_eq!(
            values(&branch, &operator, "result.first/target", &command).await?,
            vec![Value::Entity(target.clone())],
            "the first rule must still fire"
        );
        assert_eq!(
            values(&branch, &operator, "result.second/target", &command).await?,
            vec![Value::Entity(target)],
            "the rule installed after the cache warmed must fire too"
        );
        Ok(())
    }

    /// Retracting a rule's facts uninstalls it: commits after the
    /// retraction no longer fire it, while facts derived before the
    /// retraction stay (an inductive head is a transition, not a
    /// membership).
    #[dialog_common::test]
    async fn it_stops_firing_a_retracted_rule() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .transaction()
            .assert(Induct(tagger()))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let before: Entity = "doc:before".parse()?;
        branch
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(before.clone())
                    .is("before".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert_eq!(
            values(&branch, &operator, "derived/tag", &before).await?,
            vec![Value::String("before".to_string())],
            "the rule fires while installed"
        );

        branch
            .transaction()
            .retract(Induct(tagger()))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let after: Entity = "doc:after".parse()?;
        branch
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(after.clone())
                    .is("after".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert!(
            values(&branch, &operator, "derived/tag", &after)
                .await?
                .is_empty(),
            "a retracted rule must not fire"
        );
        assert_eq!(
            values(&branch, &operator, "derived/tag", &before).await?,
            vec![Value::String("before".to_string())],
            "facts derived before the retraction stay"
        );
        Ok(())
    }

    /// A rule retracted in the same commit that would have triggered
    /// it does not fire: the overlay's retraction wins over the
    /// committed slice within the very commit.
    #[dialog_common::test]
    async fn it_suppresses_a_rule_retracted_in_the_triggering_commit() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .transaction()
            .assert(Induct(tagger()))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let doc: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .retract(Induct(tagger()))
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("hello".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert!(
            values(&branch, &operator, "derived/tag", &doc)
                .await?
                .is_empty(),
            "the same-commit retraction must suppress the firing"
        );
        Ok(())
    }

    /// Forged facts in the carved-out `dialog.rule/*` namespace are
    /// inert: bytes stored under an entity that is not their content
    /// address fail the hydration check and never fire — the semantic
    /// integrity that makes the reserved-namespace carve-out safe.
    #[dialog_common::test]
    async fn it_ignores_forged_rule_facts() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // A real rule body stored under a *wrong* entity, with a
        // trigger-index entry pointing at it — the shape a buggy or
        // malicious writer could produce now that dialog.rule/* is
        // writable.
        let forged: Entity = "rule:forged".parse()?;
        let on: Entity = "on:doc/title".parse()?;
        branch
            .transaction()
            .assert(
                dialog_query::the!("dialog.rule/source")
                    .of(forged.clone())
                    .is(tagger().encode()),
            )
            .assert(dialog_query::the!("dialog.rule/on").of(forged).is(on))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let doc: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("hello".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert!(
            values(&branch, &operator, "derived/tag", &doc)
                .await?
                .is_empty(),
            "a forged rule fact must never fire"
        );
        Ok(())
    }

    /// Installation handles current state: a rule installed *after*
    /// its premises already hold fires at the install commit — the
    /// circumstance "rule exists ∧ premises hold" completes there.
    #[dialog_common::test]
    async fn it_applies_an_installed_rule_to_current_state() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // The matching state exists first.
        let doc: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("hello".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        // Installing the rule is the completing transition.
        branch
            .transaction()
            .assert(Induct(tagger()))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert_eq!(
            values(&branch, &operator, "derived/tag", &doc).await?,
            vec![Value::String("hello".to_string())],
            "an installed rule must fire over already-matching state"
        );
        Ok(())
    }

    /// Installing a consumption rule drains the backlog: existing
    /// facts matching the body are retracted at the install commit.
    #[dialog_common::test]
    async fn it_drains_a_backlog_when_a_consumption_rule_installs() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let drain: InductiveRule = serde_json::from_value(json!({
            "retract!": {
                "with": { "body": { "the": "queue.item/body", "as": "Text" } }
            },
            "when": [{
                "assert": {
                    "with": { "body": { "the": "queue.item/body", "as": "Text" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "body": { "?": { "name": "body" } }
                }
            }]
        }))?;

        let item: Entity = "item:1".parse()?;
        branch
            .transaction()
            .assert(
                dialog_query::the!("queue.item/body")
                    .of(item.clone())
                    .is("pending".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        branch
            .transaction()
            .assert(Induct(drain))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert!(
            values(&branch, &operator, "queue.item/body", &item)
                .await?
                .is_empty(),
            "installing a consumption rule must drain the existing backlog"
        );
        Ok(())
    }

    /// Installing a *deductive* rule makes concepts newly derivable
    /// over existing base facts; inductive rules premised on them must
    /// re-evaluate at that install commit.
    #[dialog_common::test]
    async fn it_reevaluates_when_a_deductive_rule_installs() -> Result<()> {
        use crate::rules::Deduce;
        use dialog_query::DeductiveRule;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alert: InductiveRule = serde_json::from_value(json!({
            "assert!": {
                "with": { "duty": { "the": "alert/duty", "as": "Text" } }
            },
            "when": [{
                "assert": {
                    "with": { "duty": { "the": "actor.status/duty", "as": "Text" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "duty": { "?": { "name": "duty" } }
                }
            }]
        }))?;

        // The inductive rule and the base fact exist; the status
        // concept is not derivable yet, so nothing fires.
        let actor: Entity = "actor:1".parse()?;
        branch
            .transaction()
            .assert(Induct(alert))
            .assert(
                dialog_query::the!("shift/duty")
                    .of(actor.clone())
                    .is("on-duty".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert!(
            values(&branch, &operator, "alert/duty", &actor)
                .await?
                .is_empty(),
            "nothing derives the status yet"
        );

        // Installing the projection is the completing transition: the
        // status becomes derivable over the existing shift fact.
        let status: DeductiveRule = serde_json::from_value(json!({
            "deduce": {
                "with": { "duty": { "the": "actor.status/duty", "as": "Text" } }
            },
            "when": [{
                "assert": {
                    "with": { "duty": { "the": "shift/duty", "as": "Text" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "duty": { "?": { "name": "duty" } }
                }
            }]
        }))?;
        branch
            .transaction()
            .assert(Deduce(status))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert_eq!(
            values(&branch, &operator, "alert/duty", &actor).await?,
            vec![Value::String("on-duty".to_string())],
            "installing the deductive rule must re-evaluate its dependents"
        );
        Ok(())
    }

    /// A rule watching a durable attribute; used by the watermark
    /// tests below.
    fn tagger() -> InductiveRule {
        serde_json::from_value(json!({
            "assert!": {
                "with": { "tag": { "the": "derived/tag", "as": "Text" } }
            },
            "when": [{
                "assert": {
                    "with": { "title": { "the": "doc/title", "as": "Text" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "title": { "?": { "name": "tag" } }
                }
            }]
        }))
        .expect("tagger rule compiles")
    }

    /// A raw [`Branch::commit`] bypasses induction — the model of a
    /// pull. The watermark records the lag, and the next inducing
    /// instant ([`Branch::induce`] here) catches up: the rule fires
    /// over facts that arrived through the raw path. A second induce
    /// is a no-op — the watermark is at the head.
    #[dialog_common::test]
    async fn it_catches_up_over_a_raw_commit() -> Result<()> {
        use dialog_artifacts::{Artifact, Instruction};
        use futures_util::stream;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .transaction()
            .assert(Induct(tagger()))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        // Head advances without induction — the pull surrogate.
        let doc: Entity = "doc:1".parse()?;
        branch
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "doc/title".parse()?,
                of: doc.clone(),
                is: Value::String("hello".into()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert!(
            values(&branch, &operator, "derived/tag", &doc)
                .await?
                .is_empty(),
            "a raw commit must not induce by itself"
        );

        // The next inducing instant catches up over the lag.
        let induced = branch.induce(&operator).await?;
        branch.refresh(&operator).await?;
        assert_eq!(
            values(&branch, &operator, "derived/tag", &doc).await?,
            vec![Value::String("hello".to_string())],
            "catch-up must fire the rule over the raw commit's facts"
        );

        // Watermark at head: a second induce is a no-op.
        let settled = branch.induce(&operator).await?;
        assert_eq!(settled, induced, "a settled branch must not re-induce");
        Ok(())
    }

    /// Completion across instants: neither fact alone satisfies the
    /// two-premise body — P arrives through a raw commit (nobody's
    /// transaction), Q through an ordinary one. The transaction's
    /// induction sees its own delta *plus* the watermark lag, so the
    /// conjunction completes at the instant it first exists.
    #[dialog_common::test]
    async fn it_completes_a_conjunction_across_instants() -> Result<()> {
        use dialog_artifacts::{Artifact, Instruction};
        use futures_util::stream;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let pair: InductiveRule = serde_json::from_value(json!({
            "assert!": {
                "with": { "both": { "the": "derived/both", "as": "Text" } }
            },
            "when": [
                {
                    "assert": {
                        "with": { "p": { "the": "fact.p/v", "as": "Text" } }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "p": { "?": { "name": "both" } }
                    }
                },
                {
                    "assert": {
                        "with": { "q": { "the": "fact.q/v", "as": "Text" } }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "q": { "?": { "name": "_q" } }
                    }
                }
            ]
        }))?;

        branch
            .transaction()
            .assert(Induct(pair))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        // P arrives outside any transaction.
        let subject: Entity = "pair:1".parse()?;
        branch
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "fact.p/v".parse()?,
                of: subject.clone(),
                is: Value::String("p".into()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        // Q arrives through a transaction: its induction sees Q (own
        // delta) and P (lag) and the conjunction completes.
        branch
            .transaction()
            .assert(
                dialog_query::the!("fact.q/v")
                    .of(subject.clone())
                    .is("q".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert_eq!(
            values(&branch, &operator, "derived/both", &subject).await?,
            vec![Value::String("p".to_string())],
            "the conjunction must complete at the instant both facts exist"
        );
        Ok(())
    }

    /// A replica that has never induced adopts the head fire-forward:
    /// pre-existing matching state does not fire retroactively; only
    /// facts arriving after adoption do.
    #[dialog_common::test]
    async fn it_adopts_a_branch_without_retroactive_firing() -> Result<()> {
        use dialog_artifacts::{Artifact, Instruction};
        use futures_util::stream;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // Rule and a matching fact both land through raw commits: the
        // branch has state and rules, but no induction ever ran and no
        // watermark exists.
        let mut install = dialog_artifacts::Changes::new();
        install.assert(Induct(tagger()));
        let old: Entity = "doc:old".parse()?;
        branch
            .commit(install.into_stream())
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        branch
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "doc/title".parse()?,
                of: old.clone(),
                is: Value::String("old".into()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        // First inducing instant: adopts the head without retroactive
        // firing over the pre-existing title.
        branch.induce(&operator).await?;
        branch.refresh(&operator).await?;
        assert!(
            values(&branch, &operator, "derived/tag", &old)
                .await?
                .is_empty(),
            "adoption must be fire-forward, not retroactive"
        );

        // From here on the watermark tracks: a new raw fact is caught
        // up at the next instant.
        let fresh: Entity = "doc:new".parse()?;
        branch
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "doc/title".parse()?,
                of: fresh.clone(),
                is: Value::String("new".into()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        branch.induce(&operator).await?;
        branch.refresh(&operator).await?;
        assert_eq!(
            values(&branch, &operator, "derived/tag", &fresh).await?,
            vec![Value::String("new".to_string())],
            "facts arriving after adoption must fire"
        );
        assert!(
            values(&branch, &operator, "derived/tag", &old)
                .await?
                .is_empty(),
            "the pre-adoption fact stays unfired"
        );
        Ok(())
    }

    /// A dispatched command no rule consumes derives nothing durable:
    /// the settled batch is empty, so the commit is a true no-op — the
    /// head keeps its revision and the command leaves no trace.
    #[dialog_common::test]
    async fn it_commits_nothing_for_an_unconsumed_command() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let base = branch
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of("doc:1".parse::<Entity>()?)
                    .is("hello".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let unchanged = branch
            .transaction()
            .dispatch(
                dialog_query::the!("cmd.noop/target")
                    .of("cmd:noop".parse::<Entity>()?)
                    .is("doc:1".parse::<Entity>()?),
            )
            .commit()
            .perform(&operator)
            .await?;
        assert_eq!(
            unchanged, base,
            "an unconsumed command must not mint a revision"
        );
        Ok(())
    }
}
