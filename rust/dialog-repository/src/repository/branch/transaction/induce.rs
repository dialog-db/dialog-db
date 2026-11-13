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

use dialog_corpus::tree::CorpusNode;
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::blob::Load as BlobLoad;
use dialog_effects::blob::Read as BlobRead;
use dialog_search_tree::nodes;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use dialog_capability::{Fork, Provider};
use dialog_common::{ConditionalSync, Holds};
use dialog_corpus::selector::Constrained;
use dialog_corpus::{
    Change, Changes, DialogCorpusError, Entity, Fact, Instruction, Pick, Relation, Select,
    Selector, Statement, Value,
};
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::memory::Resolve;
use dialog_query::attribute::The;
use dialog_query::rule::inductive::Polarity;
use dialog_query::rule::statement::Reach;
use dialog_query::{Any, Binding, Environment, InductiveRule, Match, Term};
use futures_util::{StreamExt as _, TryStreamExt};

use crate::repository::archive::erased;
use crate::repository::branch::session::{QueryEnv, shared_metadata};
use crate::repository::source::View;
use crate::rules::{
    TriggerFootprint, hydrate, hydrate_inductive, on_attr, reads_attr, source_attr, transient_attr,
};
use crate::{CommitError, RemoteSite, Revision, Staged};
use std::slice::from_ref;
use std::sync::Arc;

/// Round bound for the induction loop: a cascade still emitting
/// transients or novelty after this many rounds fails the commit
/// rather than diverging (the same posture SQL databases take with
/// recursive trigger depth).
pub(crate) const MAX_ROUNDS: u32 = 16;

/// Run commit-time induction over `changes` + `transients`, folding
/// durable novelty into `changes`. Transients never enter `changes`;
/// they are visible to rule bodies for exactly one round.
///
/// Returns every transient a rule head emitted, across all rounds. The
/// dispatched `transients` are not part of it.
pub(crate) async fn induce<Env>(
    view: &View<'_>,
    changes: &mut Changes,
    transients: Changes,
    env: &Env,
) -> Result<Changes, CommitError>
where
    Env: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Identify>
        + Provider<ArchiveLoad>
        + Provider<nodes::Write<CorpusNode>>
        + ConditionalSync
        + Provider<nodes::Read<CorpusNode>>
        + Provider<nodes::Preload<CorpusNode>>
        + ConditionalSync
        + Provider<BlobLoad>
        + Provider<dialog_corpus::Preload>
        + Provider<dialog_corpus::Speculation>
        + Provider<Fork<RemoteSite, Resolve>>
        + Holds
        + ConditionalSync
        + 'static,
{
    // Transients never commit, so an asset among them would be dropped:
    // refuse the commit before anything is written instead.
    if transients.has_assets() {
        return Err(DialogCorpusError::AssetsUnsupported("a dispatched transient".into()).into());
    }

    // Round 1 stimulus: everything the commit changes, plus the
    // watermark lag — facts that entered the branch since the last
    // inducing instant (a pull, a raw commit, a crash between publish
    // and induce). Every head advance is an instant; the lag is how a
    // missed one is caught up.
    let mut stimulus: Vec<Instruction> = changes.clone().into_instructions();
    stimulus.extend(transients.clone().into_instructions());
    stimulus.extend(lag_delta(view, env).await?);
    let mut induced = Changes::new();
    if stimulus.is_empty() {
        return Ok(induced);
    }

    // Committed trigger structures, resolved once per induction: the
    // footprint (which `on:` keys exist at all — the O(1) gate) and
    // the head it was scanned at, which keys every committed-slice
    // cache lookup below. The overlay slice is never head-cached; it
    // is re-scanned each round (cheap, in-memory) so rules installed
    // by this very commit — or by a rule during induction — fire.
    let dispatch = Dispatch::resolve(view.clone(), env).await?;

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
        let mut assert_rows: Vec<Fact> = Vec::new();
        let mut retract_attrs: BTreeSet<Relation> = BTreeSet::new();
        let mut replace_attrs: BTreeSet<Relation> = BTreeSet::new();
        for instruction in &stimulus {
            match instruction {
                Instruction::Assert(a, policy) => {
                    assert_rows.push(a.clone());
                    if policy.elects() {
                        replace_attrs.insert(a.the.clone());
                    }
                }
                Instruction::Retract(a) => {
                    retract_attrs.insert(a.the.clone());
                }
            }
        }
        // Reach is tracked per attribute, or per keyed half of a
        // domain for a collection: a stimulus row touches its own
        // attribute, and a rule reading or writing a collection
        // reaches every attribute of that half.
        let mut touched: BTreeSet<Reach> = stimulus
            .iter()
            .map(|instruction| match instruction {
                Instruction::Assert(a, _) | Instruction::Retract(a) => {
                    Reach::Relation(a.the.clone())
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
            if let Instruction::Assert(a, _) = instruction {
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
                touched.insert(Reach::of(field.descriptor().the()));
            }
        }

        // The frozen round view: branch ⊕ durable changes ⊕ this
        // round's transients, through the same layered QueryEnv a
        // transaction query uses, so rule bodies read exactly what a
        // mid-transaction query would: the writes as staged layers
        // beside the shared metadata.
        let view = QueryEnv::new(
            vec![view.to_source()],
            shared_metadata(from_ref(view), &operator),
            env,
        )
        .with_layers(vec![
            Staged::from(changes.clone()),
            Staged::from(transient_overlay.clone()),
        ]);

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
        for reach in &touched {
            for on in reach.probes() {
                candidates.extend(dispatch.triggers(&on, &overlay, env).await?);
            }
        }
        candidates.extend(installed.iter().cloned());

        // Attributes only reachable through the deductive closure: a
        // candidate premised on one changed *derivedly*, which a base
        // row cannot seed.
        let expanded: BTreeSet<Reach> = touched.difference(&direct).cloned().collect();

        let mut novelty = Changes::new();
        let mut emitted_transients = Changes::new();

        // Spike `share`: every candidate is loaded before any fires, so
        // the rules that state one body are known up front and their
        // body is evaluated once. Firing order is the candidates' own
        // either way.
        let mut loaded: Vec<(Entity, Arc<InductiveRule>, bool, bool)> = Vec::new();
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
            let (positive, unless) = premise_reach(&rule);
            let full = installed.contains(&entity)
                || expanded
                    .iter()
                    .any(|a| positive.iter().chain(unless.iter()).any(|p| p.overlaps(a)))
                || retract_attrs
                    .iter()
                    .any(|a| unless.iter().any(|u| u.covers(a)))
                || replace_attrs
                    .iter()
                    .any(|a| unless.iter().any(|u| u.covers(a)));
            loaded.push((entity, rule, transient_head, full));
        }
        let mut shared = spike::Shared::of(&loaded);
        for (at, (entity, rule, transient_head, full)) in loaded.iter().enumerate() {
            let transient_head = *transient_head;
            if *full {
                fire(
                    rule,
                    transient_head,
                    &view,
                    &mut novelty,
                    &mut emitted_transients,
                )
                .await?;
            } else if let Some((group, rename)) = shared.group(at) {
                // One evaluation of the body the group states, under a
                // rule whose head names every variable any of the
                // group's heads reads; each rule writes its own head
                // from the same matches.
                if group.matches.is_none() {
                    group.matches = Some(
                        seeded_matches(
                            &group.rule,
                            &spike::PlanKey::Shared(group.key.clone()),
                            &assert_rows,
                            &view,
                            Seeding::Grouped,
                        )
                        .await?,
                    );
                }
                let matches = group.matches.clone().unwrap_or_default();
                if !matches.is_empty() {
                    emit_renamed(
                        rule,
                        rename.as_deref(),
                        transient_head,
                        matches,
                        &view,
                        &mut novelty,
                        &mut emitted_transients,
                    )
                    .await?;
                }
            } else {
                fire_seeded(
                    entity,
                    rule,
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
        emitted_transients.clone().assert(&mut induced);
        transient_overlay = emitted_transients;
    }

    Ok(induced)
}

/// The committed side of trigger dispatch for one induction run: the
/// line (branch or snapshot), the head every cache entry is keyed by,
/// and the trigger footprint (the O(1) gate). All committed lookups
/// flow through the line's shared [`RuleCache`](crate::RuleCache) under
/// the established disciplines — discovery head-keyed, hydrated bodies
/// content-addressed, the overlay never head-cached.
struct Dispatch<'a> {
    view: View<'a>,
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
                if let Change::Assert(Value::Entity(key), _) = change {
                    slice
                        .on
                        .entry(key.clone())
                        .or_default()
                        .push(entity.clone());
                }
            } else if *attribute == reads {
                if let Change::Assert(Value::Entity(key), _) = change {
                    slice
                        .reads
                        .entry(key.clone())
                        .or_default()
                        .push(entity.clone());
                }
            } else if *attribute == source {
                match change {
                    Change::Assert(Value::Bytes(bytes), _) => {
                        slice.sources.insert(entity.clone(), bytes.clone());
                    }
                    Change::Retract(_) => {
                        slice.removed.insert(entity.clone());
                    }
                    _ => {}
                }
            } else if *attribute == transient {
                match change {
                    Change::Assert(_, _) => {
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
    async fn resolve<Env>(view: View<'a>, env: &Env) -> Result<Dispatch<'a>, CommitError>
    where
        Env: Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + dialog_common::Holds
            + ConditionalSync
            + 'static,
    {
        let Some(head) = view.revision().cloned() else {
            // A branch with no commits has no committed rules.
            return Ok(Dispatch {
                view,
                head: None,
                footprint: TriggerFootprint::default(),
            });
        };

        let cache = view.rule_cache();
        let footprint = match cache.footprint(&head) {
            Some(footprint) => footprint,
            None => {
                let mut footprint = TriggerFootprint::default();
                for claim in committed(&view, Selector::new().the(on_attr()), env).await? {
                    if let Value::Entity(key) = claim.is {
                        footprint.on.insert(key);
                    }
                }
                for claim in committed(&view, Selector::new().the(reads_attr()), env).await? {
                    if let Value::Entity(key) = claim.is {
                        footprint.reads.insert(key);
                    }
                }
                cache.record_footprint(head.clone(), footprint.clone());
                footprint
            }
        };
        Ok(Dispatch {
            view,
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
        Env: Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + dialog_common::Holds
            + ConditionalSync
            + 'static,
    {
        let mut entities: Vec<Entity> = Vec::new();
        if let Some(head) = &self.head
            && self.footprint.on.contains(on)
        {
            let cache = self.view.rule_cache();
            let committed_entities = match cache.triggers(on, head) {
                Some(entities) => entities,
                None => {
                    let selector = Selector::new().the(on_attr()).is(Value::Entity(on.clone()));
                    let entities: Vec<Entity> = committed(&self.view, selector, env)
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
        touched: &mut BTreeSet<Reach>,
        overlay: &OverlayTriggers,
        env: &Env,
    ) -> Result<(), CommitError>
    where
        Env: Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + dialog_common::Holds
            + ConditionalSync
            + 'static,
    {
        let cache = self.view.rule_cache();
        let mut frontier: Vec<Reach> = touched.iter().cloned().collect();
        while let Some(reach) = frontier.pop() {
            let mut readers: Vec<Entity> = Vec::new();
            for on in reach.probes() {
                if let Some(head) = &self.head
                    && self.footprint.reads.contains(&on)
                {
                    let committed_readers = match cache.reads(&on, head) {
                        Some(entities) => entities,
                        None => {
                            let selector = Selector::new()
                                .the(reads_attr())
                                .is(Value::Entity(on.clone()));
                            let entities: Vec<Entity> = committed(&self.view, selector, env)
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
            }
            readers.retain(|entity| !overlay.removed.contains(entity));

            for reader in readers {
                let Some(body) = self.deductive(&reader, overlay, env).await? else {
                    continue;
                };
                for (_, field) in body.conclusion().with().iter() {
                    let derived = Reach::of(field.descriptor().the());
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
        Env: Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + dialog_common::Holds
            + ConditionalSync
            + 'static,
    {
        let cache = self.view.rule_cache();
        let bytes = match overlay.sources.get(entity) {
            Some(bytes) => Some(bytes.clone()),
            None => self.source_bytes(entity, env).await?,
        };
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        if let Some(body) = cache.body(entity, &bytes) {
            return Ok(Some(body));
        }
        Ok(hydrate(&bytes)
            .ok()
            // Content-address check: forged bytes stored under a
            // mismatching entity are inert.
            .filter(|body| body.stored_as(entity))
            .inspect(|body| {
                cache.record_body(entity.clone(), &bytes, body.clone());
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
    ) -> Result<Option<Arc<InductiveRule>>, CommitError>
    where
        Env: Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + dialog_common::Holds
            + ConditionalSync
            + 'static,
    {
        let cache = self.view.rule_cache();
        // Spike `entity`: a rule this thread has loaded from the
        // committed tree is found by its entity alone, with no read of
        // its source and no hash of it.
        let committed = !overlay.sources.contains_key(entity);
        if committed && let Some(rule) = spike::rule_of(entity) {
            return Ok(Some(rule));
        }
        let bytes = match overlay.sources.get(entity) {
            Some(bytes) => Some(bytes.clone()),
            None => self.source_bytes(entity, env).await?,
        };
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        if let Some(rule) = cache.inductive(entity, &bytes) {
            let rule = Arc::new(rule);
            if committed {
                spike::keep_rule(entity, &rule);
            }
            return Ok(Some(rule));
        }
        // Undecodable bytes, or bytes whose content address is not the
        // entity they were stored under, are forged or corrupt entries
        // in the carved-out namespace — inert, like any dangling index
        // entry. This check is what makes the `dialog.rule/*`
        // reserved-namespace carve-out safe.
        let Ok(rule) = hydrate_inductive(&bytes) else {
            return Ok(None);
        };
        if !rule.stored_as(entity) {
            return Ok(None);
        }
        cache.record_inductive(entity.clone(), &bytes, rule.clone());
        let rule = Arc::new(rule);
        if committed {
            spike::keep_rule(entity, &rule);
        }
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
        Env: Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + dialog_common::Holds
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
        let cache = self.view.rule_cache();
        if let Some(verdict) = cache.transient(concept, head) {
            return Ok(verdict);
        }
        let selector = Selector::new().the(transient_attr()).of(concept.clone());
        let verdict = !committed(&self.view, selector, env).await?.is_empty();
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
        Env: Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + dialog_common::Holds
            + ConditionalSync
            + 'static,
    {
        if self.head.is_none() {
            return Ok(None);
        }
        let selector = Selector::new().the(source_attr()).of(entity.clone());
        Ok(committed(&self.view, selector, env)
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
/// inducing instant advanced it), and always empty for a snapshot: a
/// snapshot's head moves only through its own transactions, every one
/// of which induces, so nothing can slip past.
///
/// A `None` watermark (this replica has never induced) adopts the
/// current head *without* catch-up: induction is fire-forward — a
/// newly installed rule does not fire retroactively over existing
/// state, and neither does a newly adopted engine over an existing
/// branch. Reserved-namespace facts (`dialog.*` version-control
/// records, which every commit writes) are excluded from the lag, so
/// catching up over N commits stimulates rules with the *data* those
/// commits changed, not their bookkeeping.
async fn lag_delta<Env>(view: &View<'_>, env: &Env) -> Result<Vec<Instruction>, CommitError>
where
    Env: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<ArchiveLoad>
        + Provider<nodes::Write<CorpusNode>>
        + ConditionalSync
        + Provider<nodes::Read<CorpusNode>>
        + Provider<nodes::Preload<CorpusNode>>
        + ConditionalSync
        + Provider<BlobLoad>
        + Provider<dialog_corpus::Preload>
        + Provider<dialog_corpus::Speculation>
        + Provider<Fork<RemoteSite, Resolve>>
        + dialog_common::Holds
        + ConditionalSync
        + 'static,
{
    use dialog_common::Blake3Hash as NodeHash;
    use dialog_corpus::tree::fetch_spilled;
    use dialog_corpus::{EntityKey, Key, KeyViewConstruct, State};
    use dialog_search_tree::Change as TreeChange;

    let Some(branch) = view.branch() else {
        return Ok(Vec::new());
    };
    let Some(head) = view.revision() else {
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
    // surfaces once. Reads load through the env exactly as a select's do:
    // a pulled head's changed paths may reference remote-only blocks.
    let archive = branch.archive();
    let store = erased(env);
    let raw_store = store;
    let storage = store;
    let previous = crate::Index::at(archive.index(), NodeHash::from(*watermark.tree.hash()));
    let next = crate::Index::at(archive.index(), NodeHash::from(*head.tree.hash()));

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
        let spilled = fetch_spilled(&raw_store, &archive, &entry.key)
            .await
            .map_err(|error| CommitError::Induction(format!("watermark spilled: {error:?}")))?;
        let fact = Fact::from_key_datum_with_value(&entry.key, datum, spilled)
            .map_err(|error| CommitError::Induction(format!("watermark datum: {error:?}")))?;
        if induction_ignores(&fact.the.to_string()) {
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
            Instruction::Assert(fact, dialog_corpus::Pick::All)
        } else {
            Instruction::Retract(fact)
        });
    }
    Ok(lag)
}

/// Whether induction drops facts under `the`: version-control records
/// (which every commit writes) are excluded from the lag, so catching
/// up over N commits stimulates rules with the *data* those commits
/// changed, not their bookkeeping. The carved-out rule and marker
/// prefixes pass through, so a rule arriving by pull or raw commit
/// installs at this instant.
///
/// A commit whose every fact this drops is an inducing instant with an
/// empty delta; [`Branch::commit`](crate::Branch::commit) carries the
/// watermark over such a commit on that ground.
pub(crate) fn induction_ignores(the: &str) -> bool {
    the.starts_with("dialog.")
        && !the.starts_with("dialog.rule/")
        && !the.starts_with("dialog.concept/")
}

/// Collect the facts a selector matches on the line's committed
/// tree (no overlay — the cacheable slice).
async fn committed<Env>(
    view: &View<'_>,
    selector: Selector<Constrained>,
    env: &Env,
) -> Result<Vec<Fact>, CommitError>
where
    Env: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<ArchiveLoad>
        + Provider<nodes::Write<CorpusNode>>
        + ConditionalSync
        + Provider<nodes::Read<CorpusNode>>
        + Provider<nodes::Preload<CorpusNode>>
        + ConditionalSync
        + Provider<BlobLoad>
        + Provider<dialog_corpus::Preload>
        + Provider<dialog_corpus::Speculation>
        + Provider<Fork<RemoteSite, Resolve>>
        + dialog_common::Holds
        + ConditionalSync
        + 'static,
{
    let stream = crate::Select::from_view(view.clone(), selector)
        .perform(env)
        .await
        .map_err(|error| CommitError::Induction(format!("committed probe: {error}")))?;
    stream
        .map(|item| item.and_then(|view| view.to_owned()))
        .try_collect()
        .await
        .map_err(|error| CommitError::Induction(format!("committed probe: {error}")))
}

/// Collect the facts a selector matches in the layered view.
async fn select<'a>(
    view: &QueryEnv<'a>,
    selector: Selector<Constrained>,
) -> Result<Vec<Fact>, CommitError> {
    let stream = Provider::<Select<'_>>::execute(view, selector)
        .await
        .map_err(|error| CommitError::Induction(format!("dispatch probe: {error}")))?;
    stream
        .map(|item| item.and_then(|view| view.to_owned()))
        .try_collect()
        .await
        .map_err(|error| CommitError::Induction(format!("dispatch probe: {error}")))
}

/// Evaluate one rule's body against the frozen round view and emit its
/// head for every binding: transient heads into `transients`, durable
/// heads (novelty-checked against the view) into `novelty`.
async fn fire<'a>(
    rule: &InductiveRule,
    transient_head: bool,
    view: &QueryEnv<'a>,
    novelty: &mut Changes,
    transients: &mut Changes,
) -> Result<(), CommitError> {
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

/// The reach of a rule's concept premises, split by polarity:
/// positive premises (seedable by an assert/replace row) and `unless`
/// premises (only enabled by removal — never seedable).
fn premise_reach(rule: &InductiveRule) -> (BTreeSet<Reach>, BTreeSet<Reach>) {
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
            target.insert(Reach::of(field.descriptor().the()));
        }
    }
    (positive, unless)
}

/// Delta-restricted firing: bind stimulus rows into every positive
/// concept premise that names their attributes, then evaluate the body
/// with those bindings fixed — the remaining premises join against the
/// frozen view through the planner as usual. Every new match this
/// round must bind at least one new row into at least one positive
/// premise (removal-enabled and derived-premise firings take the
/// full-body path instead), so seeding is complete for this candidate
/// class while costing the delta's join fan-out, not relation size.
///
/// Several rows of one entity that [`seed_groups`] finds combinable are
/// first bound into one evaluation. It finds every match of the rows
/// seeded one by one when it finds any (see [`seed_groups`] for why);
/// when it finds none, the rows are seeded one by one.
async fn fire_seeded<'a>(
    entity: &Entity,
    rule: &InductiveRule,
    transient_head: bool,
    rows: &[Fact],
    view: &QueryEnv<'a>,
    novelty: &mut Changes,
    transients: &mut Changes,
) -> Result<(), CommitError> {
    let key = spike::PlanKey::Rule(entity.clone());
    let matches = seeded_matches(rule, &key, rows, view, Seeding::Grouped).await?;
    #[cfg(test)]
    let before = (novelty.clone(), transients.clone());
    if !matches.is_empty() {
        emit_matches(rule, transient_head, matches, view, novelty, transients).await?;
    }
    #[cfg(test)]
    oracle::agrees(
        &key,
        rule,
        transient_head,
        rows,
        view,
        before,
        novelty,
        transients,
    )
    .await?;
    Ok(())
}

/// How [`seeded_matches`] binds a premise's rows.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Seeding {
    /// Each row alone: the reference the grouped seeding must equal.
    #[cfg_attr(not(test), allow(dead_code))]
    PerRow,
    /// Combinable rows of one entity together, falling back to each
    /// alone when together they find nothing.
    Grouped,
}

/// The matches a seeded firing of `rule` finds for `rows`, in the order
/// seeding the rows one by one finds them: a group's matches stand at
/// its first row, and a row seeded alone after its group found nothing
/// stands at its own. The order is what [`emit_matches`] writes in, and
/// two matches writing one transient cell under `last` are decided by
/// it: the later replaces the earlier.
async fn seeded_matches<'a>(
    rule: &InductiveRule,
    key: &spike::PlanKey,
    rows: &[Fact],
    view: &QueryEnv<'a>,
    seeding: Seeding,
) -> Result<Vec<Match>, CommitError> {
    use dialog_query::{Premise, Proposition};

    // Spike `subject`: the body with every subject named, so a seed
    // binds the entity its rows are about. Spike `plan`: the plans this
    // rule was given before, by the names a seed binds.
    let body = spike::Body::of(rule);
    let mut plans = spike::Plans::of(key);
    let premises = match &body {
        Some(body) => &body.premises,
        None => &rule.analysis().premises,
    };

    let mut matches: Vec<Match> = Vec::new();
    for premise in premises {
        let Premise::Assert(Proposition::Concept(query)) = premise else {
            continue;
        };
        let groups = match seeding {
            Seeding::Grouped => seed_groups(query, rows),
            Seeding::PerRow => (0..rows.len()).map(|at| vec![at]).collect(),
        };
        // What each group or row found, keyed by the row it stands at.
        let mut found: Vec<(usize, Vec<Match>)> = Vec::with_capacity(groups.len());
        for group in groups {
            #[cfg(test)]
            if seeding == Seeding::Grouped {
                oracle::count(if group.len() > 1 { 1 } else { group.len() });
            }
            if group.len() > 1 {
                let together = evaluate_seed(
                    rule,
                    body.as_ref(),
                    &mut plans,
                    query,
                    group.iter().map(|&at| &rows[at]),
                    view,
                )
                .await?;
                if !together.is_empty() {
                    found.push((group[0], together));
                    continue;
                }
            }
            #[cfg(test)]
            if seeding == Seeding::Grouped && group.len() > 1 {
                oracle::count(group.len());
            }
            for at in group {
                found.push((
                    at,
                    evaluate_seed(rule, body.as_ref(), &mut plans, query, [&rows[at]], view)
                        .await?,
                ));
            }
        }
        // Groups come in the order of their first rows, so only rows
        // seeded alone after their group found nothing are out of
        // place; a stable sort puts them back between the others.
        found.sort_by_key(|(at, _)| *at);
        matches.extend(found.into_iter().flat_map(|(_, found)| found));
    }
    plans.keep(key);
    Ok(matches)
}

/// Evaluates `rule` with `rows` bound into `query` (see [`seed`]); no
/// matches when the rows do not bind it.
async fn evaluate_seed<'a, 'r>(
    rule: &InductiveRule,
    body: Option<&spike::Body>,
    plans: &mut spike::Plans,
    query: &dialog_query::ConceptQuery,
    rows: impl IntoIterator<Item = &'r Fact>,
    view: &QueryEnv<'a>,
) -> Result<Vec<Match>, CommitError> {
    let Some((matched, scope)) = seed(query, rows) else {
        return Ok(Vec::new());
    };
    plans
        .plan(rule, body, &scope)
        .evaluate(matched.seed(), view)
        .try_collect()
        .await
        .map_err(|error| CommitError::Induction(format!("seeded rule body: {error}")))
}

/// The rows of `rows` that may seed `query` together: the rows of one
/// entity that each bind exactly one field of the premise, a single
/// attribute (not a collection's half, whose key varies by row) read
/// under `last`, no two of them the same field. Every other row is a
/// group of its own; a row naming no field of the premise is in none.
///
/// Bound together, a group finds the matches where every field holds
/// its row's value. Each row alone binds its own field and reads the
/// others free, and a free field under `last` reads the one value its
/// cell elects in the round view. A seeded read yields a row's value
/// only when that value is the one elected. So when the group finds any
/// match, every field's elected value is its row's value, each row alone
/// reads exactly the frame the group bound, and the two find the same
/// matches. When the group finds none, some field elects another value
/// (an overlay row standing above this commit, a derived candidate, a
/// tie with another claim of the cell) or the rest of the body fails;
/// either way [`seeded_matches`] seeds the rows one by one, which is
/// what it did before. No assumption about which layer a read elects
/// from is made here: the engine's own seeded read checks it.
///
/// Groups come in the order of their first rows, and the rows of a
/// group in their own order, so the matches keep the order seeding the
/// rows one by one finds them in.
fn seed_groups(query: &dialog_query::ConceptQuery, rows: &[Fact]) -> Vec<Vec<usize>> {
    let mut groups: Vec<Vec<usize>> = Vec::new();
    let mut together: BTreeMap<&Entity, (Vec<usize>, BTreeSet<&str>, bool)> = BTreeMap::new();
    for (at, row) in rows.iter().enumerate() {
        let fields: Vec<_> = query
            .predicate
            .with()
            .iter()
            .filter(|(_, field)| Reach::of(field.descriptor().the()).covers(&row.the))
            .collect();
        let [(name, field)] = fields.as_slice() else {
            // Binds no field (nothing to seed) or several: alone.
            if !fields.is_empty() {
                groups.push(vec![at]);
            }
            continue;
        };
        let combinable = field.descriptor().the().attribute().is_some()
            && matches!(field.descriptor().pick(), Pick::Last);
        if !combinable {
            groups.push(vec![at]);
            continue;
        }
        let (members, names, repeated) = together.entry(&row.of).or_default();
        if !names.insert(name) {
            *repeated = true;
        }
        members.push(at);
    }
    for (_, (members, _, repeated)) in together {
        if repeated {
            groups.extend(members.into_iter().map(|at| vec![at]));
        } else {
            groups.push(members);
        }
    }
    groups.sort_by_key(|group| group[0]);
    groups
}

/// Binds `rows` into `query`: each row's value where its attribute
/// matches a field, the key too for a collection field, and its subject
/// as `this`. `None` when a row matches no field or contradicts a
/// binding (a constant it does not read as, or another row's value).
fn seed<'r>(
    query: &dialog_query::ConceptQuery,
    rows: impl IntoIterator<Item = &'r Fact>,
) -> Option<(Match, Environment)> {
    let mut matched = Match::new();
    let mut scope = Environment::new();
    for row in rows {
        let mut seeded = false;
        for (name, field) in query.predicate.with().iter() {
            let relation = field.descriptor().the();
            if !Reach::of(relation).covers(&row.the) {
                continue;
            }
            if !bind_seed(
                &mut matched,
                &mut scope,
                query.terms.get(name),
                row.is.clone(),
            ) {
                return None;
            }
            // A collection premise binds the entry's key too: the
            // name half of the row's attribute.
            if relation.attribute().is_none()
                && !bind_seed(
                    &mut matched,
                    &mut scope,
                    query.terms.get(&The::key_operand(name)),
                    Value::String(row.the.name().to_owned()),
                )
            {
                return None;
            }
            seeded = true;
        }
        if !seeded
            || !bind_seed(
                &mut matched,
                &mut scope,
                query.terms.get("this"),
                Value::Entity(row.of.clone()),
            )
        {
            return None;
        }
    }
    Some((matched, scope))
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
            scope.add(name.as_ref());
            true
        }
        // A constant agrees with the number its type reads the row as,
        // as a scan for that constant admits the row.
        Some(Term::Constant(expected)) => value.reads_as(expected),
        Some(Term::Variable { name: None, .. }) | None => true,
    }
}

/// Prototype changes to commit-time induction, for
/// `research/rule-heads.md`: each is off unless `DIALOG_SPIKE` names it
/// (comma-separated, or `all`), so one build measures every variant.
/// Nothing here is meant to land as it is.
///
/// - `plan`: a rule is planned once for each set of names a seed binds,
///   and the plan is kept for the life of the thread, where today it is
///   planned again for every seed of every commit.
/// - `share`: when every candidate of a round that fires seeded states
///   the same body, the same premises under a renaming of variables, it
///   is evaluated once, under a rule whose head names every variable any
///   of their heads reads. Incomplete: a round whose candidates state
///   two bodies is left as it is, a head that writes a collection is
///   left out, and the union's head types every variable it names for
///   every member.
/// - `subject`: a positive concept premise that leaves its subject
///   unnamed is given a variable for it, so a seed binds the entity its
///   rows are about and the premise is read at that entity.
/// - `entity`: a committed rule is found by the entity it is stored
///   under, which is a hash of it, where today each commit reads the
///   rule's source from the tree and hashes it to find the rule it
///   decoded before. Unsound as it stands: it no longer checks that the
///   source is stored on this line, so a trigger row with no rule
///   behind it here would fire a rule loaded elsewhere.
mod spike {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::env;
    use std::rc::Rc;
    use std::sync::{Arc, OnceLock};

    use dialog_corpus::Entity;
    use dialog_query::{
        Any, Conjunction, Environment, InductiveRule, Match, Planner, Premise, Proposition, Term,
        TypeEnv,
    };
    use serde_json::Value as Json;

    /// Which prototypes are on.
    #[derive(Clone, Copy, Default)]
    pub(super) struct Switches {
        pub plan: bool,
        pub share: bool,
        pub subject: bool,
        pub entity: bool,
    }

    /// The switches `DIALOG_SPIKE` sets, read once.
    pub(super) fn on() -> Switches {
        static ON: OnceLock<Switches> = OnceLock::new();
        *ON.get_or_init(|| {
            let set = env::var("DIALOG_SPIKE").unwrap_or_default();
            let has = |name: &str| set.split(',').any(|each| each == name || each == "all");
            Switches {
                plan: has("plan"),
                share: has("share"),
                subject: has("subject"),
                // Not part of `all`: it was added after the others were
                // measured, and is measured apart.
                entity: set.split(',').any(|each| each == "entity"),
            }
        })
    }

    /// A rule's body as a seeded firing evaluates it under `subject`:
    /// its premises with every subject named, and the types they plan
    /// under.
    pub(super) struct Body {
        pub premises: Vec<Premise>,
        pub types: Arc<TypeEnv>,
    }

    impl Body {
        /// The body of `rule` with its subjects named; `None` when
        /// `subject` is off, and the rule's own premises are used.
        pub(super) fn of(rule: &InductiveRule) -> Option<Self> {
            if !on().subject {
                return None;
            }
            let mut premises = rule.analysis().premises.clone();
            for (at, premise) in premises.iter_mut().enumerate() {
                let Premise::Assert(Proposition::Concept(query)) = premise else {
                    continue;
                };
                let named = matches!(
                    query.terms.get("this"),
                    Some(Term::Constant(_) | Term::Variable { name: Some(_), .. })
                );
                if !named {
                    query
                        .terms
                        .insert("this".into(), Term::<Any>::var(format!("subject.{at}")));
                }
            }
            Some(Body {
                premises,
                types: rule.analysis().types.clone(),
            })
        }
    }

    /// What a kept plan belongs to: a stored rule, or the body a group
    /// of rules shares.
    #[derive(Clone, PartialEq, Eq, Hash)]
    pub(super) enum PlanKey {
        Rule(Entity),
        Shared(Rc<str>),
    }

    thread_local! {
        /// Every plan kept under `plan`, by what it plans and the names
        /// its scope binds, sorted.
        static PLANS: RefCell<HashMap<PlanKey, HashMap<Vec<String>, Conjunction>>> =
            RefCell::new(HashMap::new());
        /// Under `entity`, every committed rule this thread has loaded,
        /// by the entity it is stored under.
        static RULES: RefCell<HashMap<Entity, Arc<InductiveRule>>> = RefCell::new(HashMap::new());
    }

    /// The rule stored under `entity`, if `entity` is on and this
    /// thread loaded it before.
    pub(super) fn rule_of(entity: &Entity) -> Option<Arc<InductiveRule>> {
        if !on().entity {
            return None;
        }
        RULES.with(|rules| rules.borrow().get(entity).cloned())
    }

    /// Keep `rule` as what `entity` stores, when `entity` is on.
    pub(super) fn keep_rule(entity: &Entity, rule: &Arc<InductiveRule>) {
        if on().entity {
            RULES.with(|rules| rules.borrow_mut().insert(entity.clone(), rule.clone()));
        }
    }

    /// The plans of one rule, taken out of the thread's for one seeded
    /// firing and put back by [`keep`](Self::keep).
    pub(super) struct Plans(HashMap<Vec<String>, Conjunction>);

    impl Plans {
        pub(super) fn of(key: &PlanKey) -> Self {
            if !on().plan {
                return Plans(HashMap::new());
            }
            Plans(PLANS.with(|plans| plans.borrow_mut().remove(key).unwrap_or_default()))
        }

        pub(super) fn keep(self, key: &PlanKey) {
            if on().plan {
                PLANS.with(|plans| plans.borrow_mut().insert(key.clone(), self.0));
            }
        }

        /// The plan of `rule`, or of `body` when its subjects are
        /// named, for `scope`.
        pub(super) fn plan(
            &mut self,
            rule: &InductiveRule,
            body: Option<&Body>,
            scope: &Environment,
        ) -> Conjunction {
            let plan = || match body {
                Some(body) => Planner::with_types(body.premises.clone(), body.types.clone())
                    .plan(scope)
                    .expect("an analyzed rule is plannable by construction"),
                None => rule.plan(scope),
            };
            if !on().plan {
                return plan();
            }
            let mut names: Vec<String> = scope.iter().map(str::to_string).collect();
            names.sort();
            self.0.entry(names).or_insert_with(plan).clone()
        }
    }

    /// For a head field of a rule, the variable that holds its value in
    /// the matches of the rule its group is evaluated under.
    pub(super) type Rename = HashMap<String, String>;

    /// One body several candidates of a round state.
    pub(super) struct Group {
        /// The entities of the rules that state it, joined.
        pub key: Rc<str>,
        /// The rule the body is evaluated under.
        pub rule: InductiveRule,
        /// What it matched this round, once it has been evaluated.
        pub matches: Option<Vec<Match>>,
    }

    /// The groups of a round's candidates, by candidate.
    pub(super) struct Shared {
        groups: Vec<Group>,
        of: HashMap<usize, (usize, Option<Rc<Rename>>)>,
    }

    /// A group as it is kept between rounds and commits: the rule its
    /// body is evaluated under, and each member's renaming.
    type Kept = Option<(InductiveRule, Vec<Option<Rc<Rename>>>)>;

    thread_local! {
        /// Each rule as its stored spelling encodes, by its entity.
        static SPELLINGS: RefCell<HashMap<Entity, Rc<Json>>> = RefCell::new(HashMap::new());
        /// Every group made, by its members' entities.
        static KEPT: RefCell<HashMap<Rc<str>, Kept>> = RefCell::new(HashMap::new());
    }

    impl Shared {
        /// Group the candidates that fire seeded and state one body:
        /// the same premises under a renaming of their variables that
        /// keeps `this`. A candidate alone in its group is in none, and
        /// neither is one whose head writes a collection.
        pub(super) fn of(loaded: &[(Entity, Arc<InductiveRule>, bool, bool)]) -> Self {
            let mut shared = Shared {
                groups: Vec::new(),
                of: HashMap::new(),
            };
            if !on().share {
                return shared;
            }
            let seeded: Vec<usize> = loaded
                .iter()
                .enumerate()
                .filter(|(_, (_, rule, _, full))| {
                    !full
                        && rule
                            .conclusion()
                            .with()
                            .iter()
                            .all(|(_, field)| field.descriptor().the().attribute().is_some())
                })
                .map(|(at, _)| at)
                .collect();
            if seeded.len() < 2 {
                return shared;
            }
            let key: Rc<str> = seeded
                .iter()
                .map(|&at| loaded[at].0.to_string())
                .collect::<Vec<_>>()
                .join(" ")
                .into();
            // The groups of this set of candidates, as made before.
            let kept = KEPT.with(|kept| kept.borrow().get(&key).cloned());
            let kept = kept.unwrap_or_else(|| {
                let made = group(&seeded, loaded);
                KEPT.with(|kept| kept.borrow_mut().insert(key.clone(), made.clone()));
                made
            });
            let Some((rule, renames)) = kept else {
                return shared;
            };
            for (&at, rename) in seeded.iter().zip(renames) {
                shared.of.insert(at, (0, rename));
            }
            shared.groups.push(Group {
                key,
                rule,
                matches: None,
            });
            shared
        }

        /// The group candidate `at` is in, if it is in one, and how its
        /// head reads the group's matches.
        pub(super) fn group(&mut self, at: usize) -> Option<(&mut Group, Option<Rc<Rename>>)> {
            let (group, rename) = self.of.get(&at)?.clone();
            Some((self.groups.get_mut(group)?, rename))
        }
    }

    /// One group of all of `seeded`, if every one of them states the
    /// body of the first: the rule that body is evaluated under, whose
    /// head names every variable any member's head reads, and each
    /// member's renaming. `None` when they do not all state one body. A
    /// round whose seeded candidates state two bodies or more is left
    /// ungrouped: the spike does not partition them.
    fn group(seeded: &[usize], loaded: &[(Entity, Arc<InductiveRule>, bool, bool)]) -> Kept {
        let spellings: Vec<Rc<Json>> = seeded
            .iter()
            .map(|&at| spelling(&loaded[at].0, &loaded[at].1))
            .collect::<Option<_>>()?;
        let first = &spellings[0];
        let mut union = (**first).clone();
        let mut renames: Vec<Option<Rc<Rename>>> = vec![None];
        for other in &spellings[1..] {
            let rename = same_body(first, other)?;
            // The fields of this member's head, under the names their
            // variables have in the first member's spelling.
            let mut reads = Rename::new();
            for (name, field) in head(other)? {
                let variable = rename.get(name).cloned().unwrap_or_else(|| name.clone());
                let fields = head_mut(&mut union)?;
                if !fields.contains_key(&variable) {
                    fields.insert(variable.clone(), field.clone());
                }
                reads.insert(name.clone(), variable);
            }
            renames.push(Some(Rc::new(reads)));
        }
        let rule: InductiveRule = serde_json::from_value(union).ok()?;
        Some((rule, renames))
    }

    /// `rule` as it encodes, kept by the entity it is stored under.
    fn spelling(entity: &Entity, rule: &InductiveRule) -> Option<Rc<Json>> {
        if let Some(spelling) = SPELLINGS.with(|all| all.borrow().get(entity).cloned()) {
            return Some(spelling);
        }
        let mut json = serde_json::to_value(rule).ok()?;
        // The union is compiled as an asserting rule: only its body is
        // evaluated, and each member writes with its own polarity.
        let object = json.as_object_mut()?;
        if let Some(head) = object.remove("retract!") {
            object.insert("assert!".into(), head);
        }
        object.remove("description");
        let spelling = Rc::new(json);
        SPELLINGS.with(|all| all.borrow_mut().insert(entity.clone(), spelling.clone()));
        Some(spelling)
    }

    /// The fields of an encoded rule's head.
    fn head(rule: &Json) -> Option<&serde_json::Map<String, Json>> {
        rule.get("assert!")?.get("with")?.as_object()
    }

    fn head_mut(rule: &mut Json) -> Option<&mut serde_json::Map<String, Json>> {
        rule.get_mut("assert!")?.get_mut("with")?.as_object_mut()
    }

    /// The renaming of `other`'s variables under which its `when` and
    /// `unless` are those of `first`, whatever the order of the
    /// premises: a variable of `other` to the variable of `first` in
    /// its place. `None` when there is none.
    fn same_body(first: &Json, other: &Json) -> Option<Rename> {
        let mut rename = Rename::new();
        rename.insert("this".into(), "this".into());
        for part in ["when", "unless"] {
            let empty = Vec::new();
            let of_first = first.get(part).and_then(Json::as_array).unwrap_or(&empty);
            let of_other = other.get(part).and_then(Json::as_array).unwrap_or(&empty);
            if of_first.len() != of_other.len() {
                return None;
            }
            let mut taken = vec![false; of_first.len()];
            rename = pair(of_first, of_other, &mut taken, rename)?;
        }
        Some(rename)
    }

    /// Pair each premise of `other` with a premise of `first` not yet
    /// `taken` that it equals under one renaming, trying every pairing.
    fn pair(first: &[Json], other: &[Json], taken: &mut [bool], rename: Rename) -> Option<Rename> {
        let Some((premise, rest)) = other.split_first() else {
            return Some(rename);
        };
        for at in 0..first.len() {
            if taken[at] {
                continue;
            }
            let mut attempt = rename.clone();
            if !same(&first[at], premise, &mut attempt) {
                continue;
            }
            taken[at] = true;
            if let Some(found) = pair(first, rest, taken, attempt) {
                return Some(found);
            }
            taken[at] = false;
        }
        None
    }

    /// The name of the variable `json` encodes, if it encodes a named
    /// one: `{ "?": { "name": … } }`.
    fn variable(json: &Json) -> Option<&str> {
        let object = json.as_object()?;
        if object.len() != 1 {
            return None;
        }
        object.get("?")?.get("name")?.as_str()
    }

    /// Whether `other` is `first` with its variables renamed by
    /// `rename`, which is extended by the variables met and kept one to
    /// one.
    fn same(first: &Json, other: &Json, rename: &mut Rename) -> bool {
        if let (Some(a), Some(b)) = (variable(first), variable(other)) {
            // The rest of the variable, its type, has to agree too.
            let rest = |json: &Json| {
                let mut rest = json["?"].clone();
                rest.as_object_mut().map(|object| object.remove("name"));
                rest
            };
            if rest(first) != rest(other) {
                return false;
            }
            return match rename.get(b) {
                Some(bound) => bound == a,
                None if rename.values().any(|bound| bound == a) => false,
                None => {
                    rename.insert(b.to_string(), a.to_string());
                    true
                }
            };
        }
        match (first, other) {
            (Json::Object(a), Json::Object(b)) => {
                a.len() == b.len()
                    && a.iter().all(|(key, value)| {
                        b.get(key).is_some_and(|other| same(value, other, rename))
                    })
            }
            (Json::Array(a), Json::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same(a, b, rename))
            }
            (a, b) => a == b,
        }
    }
}

/// What a head writes, as `emit_matches` tells two heads apart: its
/// subject, then each field's attribute, value and pick.
type WrittenHead = (Entity, Vec<(Relation, Value, Pick)>);

/// Emit a rule's head for every produced match: transient heads into
/// `transients`, durable heads (novelty-checked against the frozen
/// view) into `novelty`.
async fn emit_matches<'a>(
    rule: &InductiveRule,
    transient_head: bool,
    matches: Vec<Match>,
    view: &QueryEnv<'a>,
    novelty: &mut Changes,
    transients: &mut Changes,
) -> Result<(), CommitError> {
    emit_renamed(
        rule,
        None,
        transient_head,
        matches,
        view,
        novelty,
        transients,
    )
    .await
}

/// [`emit_matches`] for matches found under another spelling of the
/// rule's body (spike `share`): `rename` gives, for a head field of
/// `rule`, the variable that holds its value in `matches`.
async fn emit_renamed<'a>(
    rule: &InductiveRule,
    rename: Option<&spike::Rename>,
    transient_head: bool,
    matches: Vec<Match>,
    view: &QueryEnv<'a>,
    novelty: &mut Changes,
    transients: &mut Changes,
) -> Result<(), CommitError> {
    let heads = written_heads(rule, rename, &matches)?;

    // A seeded rule is evaluated once per stimulus row a premise names,
    // so an event that carries several of a premise's fields finds the
    // same binding once per field: each distinct head is written once,
    // the first time it is found. The heads are compared where they
    // stand, without a copy, and only when there are two to compare.
    let mut first = vec![true; heads.len()];
    if heads.len() > 1 {
        let mut seen: hashbrown::HashSet<&WrittenHead> =
            hashbrown::HashSet::with_capacity(heads.len());
        for (at, head) in heads.iter().enumerate() {
            first[at] = seen.insert(head);
        }
    }

    for ((this, cells), first) in heads.into_iter().zip(first) {
        if !first {
            continue;
        }
        let mut head = Changes::new();
        for (attribute, value, pick) in cells {
            match rule.polarity() {
                // A retracting head dissociates the exact bound
                // triple, cardinality-independent.
                Polarity::Retract => {
                    dialog_corpus::Update::dissociate(&mut head, attribute, this.clone(), value);
                }
                // An asserting head writes as the field's pick says: a
                // set appends, and a choosing pick succeeds the claim
                // it elects, resolved against the view at commit.
                Polarity::Assert => {
                    dialog_corpus::Update::associate(
                        &mut head,
                        attribute,
                        this.clone(),
                        value,
                        pick,
                    );
                }
            }
        }

        if transient_head {
            head.assert(transients);
            continue;
        }
        // A head written by succession elects the claim it succeeds
        // against the round view, as a transaction's own writes do
        // against the line at commit.
        Box::pin(super::succession::resolve_against(view, &mut head)).await?;
        for instruction in head.into_instructions() {
            if is_novel(view, &instruction).await? {
                match instruction {
                    Instruction::Assert(a, policy) => {
                        dialog_corpus::Update::associate(novelty, a.the, a.of, a.is, policy)
                    }
                    Instruction::Retract(a) => {
                        dialog_corpus::Update::dissociate(novelty, a.the, a.of, a.is)
                    }
                }
            }
        }
    }
    Ok(())
}

/// The head each of `matches` writes, in order: its subject and the
/// cells of the fields it binds. A match that leaves `this` unbound
/// writes nothing and is left out.
fn written_heads(
    rule: &InductiveRule,
    rename: Option<&spike::Rename>,
    matches: &[Match],
) -> Result<Vec<WrittenHead>, CommitError> {
    let conclusion = rule.conclusion();
    let mut heads: Vec<WrittenHead> = Vec::with_capacity(matches.len());
    for matched in matches {
        // The head subject. A rule whose premises leave `this` unbound
        // has no cell to write; skip the binding.
        let this = match matched.lookup(&Term::<Any>::var("this")) {
            Ok(Binding::Present(Value::Entity(entity))) => entity,
            _ => continue,
        };

        let mut cells = Vec::new();
        for (name, field) in conclusion.with().iter() {
            let variable = rename
                .and_then(|rename| rename.get(name))
                .map_or(name, String::as_str);
            let Ok(Binding::Present(value)) = matched.lookup(&Term::<Any>::var(variable)) else {
                // Optional head fields the frame didn't bind emit
                // nothing.
                continue;
            };
            // A collection head writes under `domain/key`, the key
            // being what the body bound to the field's key operand.
            let relation = field.descriptor().the();
            let attribute = match relation.attribute() {
                Some(attribute) => attribute,
                None => {
                    let key = Term::<Any>::var(The::key_operand(name));
                    let Ok(Binding::Present(Value::String(key))) = matched.lookup(&key) else {
                        continue;
                    };
                    relation.entry(&key).map_err(|error| {
                        CommitError::Induction(format!("head field {name}: {error}"))
                    })?
                }
            };
            cells.push((attribute, value, field.descriptor().pick().clone()));
        }
        heads.push((this, cells));
    }
    Ok(heads)
}

/// Whether applying `instruction` would change the view: asserting a
/// triple already present (or replacing with the value already held)
/// contributes nothing, and neither does retracting an absent triple.
/// This is the fixpoint's convergence test — a rule whose head
/// re-derives existing state terminates for free. The view is the
/// frozen round view, so siblings within a round judge novelty against
/// identical state.
async fn is_novel<'a>(view: &QueryEnv<'a>, instruction: &Instruction) -> Result<bool, CommitError> {
    let fact = match instruction {
        Instruction::Assert(a, _) | Instruction::Retract(a) => a,
    };
    let selector = Selector::new()
        .the(fact.the.clone())
        .of(fact.of.clone())
        .is(fact.is.clone());
    let present = !select(view, selector).await?.is_empty();
    Ok(match instruction {
        Instruction::Assert(_, _) => !present,
        Instruction::Retract(_) => present,
    })
}

/// A differential check, in tests only: every seeded firing also seeds
/// its rows one by one, writes those matches through the same emission
/// against the same frozen view, and what the two write must be the
/// same, novelty and transients alike. Comparing what is written, not
/// the heads found, sees the order of the matches too: two transient
/// writes to one cell under `last` are decided by which comes later.
/// It runs under every test that commits through a rule, so the
/// equivalence [`seed_groups`] argues for is checked wherever those
/// tests reach, not only where a test was written for it.
#[cfg(test)]
mod oracle {
    use super::{InductiveRule, QueryEnv, Seeding, emit_matches, seeded_matches};
    use crate::CommitError;
    use dialog_corpus::{Changes, Fact};
    use std::cell::Cell;

    // Per thread: the test runtime drives each test on one thread.
    thread_local! {
        static EVALUATIONS: Cell<usize> = const { Cell::new(0) };
    }

    /// Notes `n` evaluations of grouped seeding on this thread.
    pub(super) fn count(n: usize) {
        EVALUATIONS.with(|evaluations| evaluations.set(evaluations.get() + n));
    }

    /// The evaluations grouped seeding made on this thread since the
    /// last call, the oracle's own per-row ones not among them.
    pub(super) fn evaluations() -> usize {
        EVALUATIONS.with(|evaluations| evaluations.replace(0))
    }

    /// Seeding the rows of `rows` one by one, from the `before` the
    /// grouped firing started from, writes the `novelty` and
    /// `transients` the grouped firing wrote; panics with both when it
    /// does not.
    pub(super) async fn agrees(
        key: &super::spike::PlanKey,
        rule: &InductiveRule,
        transient_head: bool,
        rows: &[Fact],
        view: &QueryEnv<'_>,
        before: (Changes, Changes),
        novelty: &Changes,
        transients: &Changes,
    ) -> Result<(), CommitError> {
        let per_row = seeded_matches(rule, key, rows, view, Seeding::PerRow).await?;
        let (mut expected_novelty, mut expected_transients) = before;
        if !per_row.is_empty() {
            emit_matches(
                rule,
                transient_head,
                per_row,
                view,
                &mut expected_novelty,
                &mut expected_transients,
            )
            .await?;
        }
        assert_eq!(
            novelty, &expected_novelty,
            "grouped seeding must write the novelty seeding row by row writes"
        );
        assert_eq!(
            transients, &expected_transients,
            "grouped seeding must write the transients seeding row by row writes"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dialog_effects::archive::Load as ArchiveLoad;
    use dialog_effects::blob::Load as BlobLoad;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::test_repo;
    use crate::repository::source::Live;
    use crate::rules::Transient;
    use crate::{Branch, CommitError, RemoteSite, Revision};
    use anyhow::Result;
    use dialog_capability::{Fork, Provider};
    use dialog_common::ConditionalSync;
    use dialog_corpus::tree::CorpusNode;
    use dialog_corpus::{Changes, Entity, Fact, Instruction, Selector, Value};
    use dialog_effects::archive::{Get, Put};
    use dialog_effects::blob::Read as BlobRead;
    use dialog_effects::memory::Resolve;
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::rule::statement::on_entities;
    use dialog_query::{ConceptDescriptor, InductiveRule};
    use dialog_search_tree::nodes;
    use futures_util::StreamExt as _;
    use serde_json::json;

    /// Collect the values a `(the, of)` pair holds on the branch.
    async fn values<Env>(branch: &Branch, env: &Env, the: &str, of: &Entity) -> Result<Vec<Value>>
    where
        Env: Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + dialog_common::Holds
            + ConditionalSync
            + 'static,
    {
        let selector = Selector::new().the(the.parse()?).of(of.clone());
        let stream = branch.claims().select(selector).perform(env).await?;
        let facts: Vec<_> = stream.collect().await;
        Ok(facts
            .into_iter()
            .map(|item| item.and_then(|view| view.to_owned()))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|fact| fact.is)
            .collect())
    }

    /// The increment rule: `assert! counter{count: ?prev + 1} when
    /// increment{counter: ?this}, counter{this: ?this, count: ?prev}`.
    fn increment_rule() -> InductiveRule {
        serde_json::from_value(json!({
            "description": "Increment a counter on an increment command",
            "assert!": {
                "with": {
                    "count": { "the": "counter/count", "as": "natural:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "counter": { "the": "cmd.increment/counter", "as": "entity:" }
                        }
                    },
                    "where": {
                        "counter": { "?": { "name": "this" } }
                    }
                },
                {
                    "assert": {
                        "with": {
                            "count": { "the": "counter/count", "as": "natural:" }
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
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let counter: Entity = "ctr:1".parse()?;

        branch
            .transaction()
            .assert(increment_rule())
            .assert(
                dialog_query::the!("counter/count")
                    .of(counter.clone())
                    .is(1u64),
            )
            .commit()
            .publish()
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
            .publish()
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

    /// The increment rule over a counter declared `integer:`: the band
    /// stores a non-negative count as a natural whatever type wrote it,
    /// so the body's integer view must read the stored natural.
    fn integer_increment_rule() -> InductiveRule {
        serde_json::from_value(json!({
            "description": "Increment an integer counter on an increment command",
            "assert!": {
                "with": {
                    "count": { "the": "counter/count", "as": "integer:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "counter": { "the": "cmd.increment/counter", "as": "entity:" }
                        }
                    },
                    "where": {
                        "counter": { "?": { "name": "this" } }
                    }
                },
                {
                    "assert": {
                        "with": {
                            "count": { "the": "counter/count", "as": "integer:" }
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
        .expect("integer increment rule compiles")
    }

    /// A count asserted as a natural in the same commit as the increment
    /// seeds the rule's `integer:` premise. The seed is read through the
    /// variable's view, as a scan reads it: the natural 40 is the integer
    /// 40, and the commit increments it rather than failing. The staged
    /// 40 is not yet a claim the head can succeed, so 41 lands beside it.
    #[dialog_common::test]
    async fn it_seeds_an_integer_premise_from_a_natural_fact() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let counter: Entity = "ctr:1".parse()?;
        branch
            .transaction()
            .assert(integer_increment_rule())
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let command: Entity = "cmd:1".parse()?;
        branch
            .transaction()
            .assert(
                dialog_query::the!("counter/count")
                    .of(counter.clone())
                    .is(40u128),
            )
            .dispatch(
                dialog_query::the!("cmd.increment/counter")
                    .of(command)
                    .is(counter.clone()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        assert_eq!(
            values(&branch, &operator, "counter/count", &counter).await?,
            vec![Value::UnsignedInt(40), Value::UnsignedInt(41)],
            "the seeded natural must read as the integer and be incremented"
        );
        Ok(())
    }

    /// A guarded decrement over a natural counter: `count - 1` is an
    /// integer, which the natural head takes only once `>= 0` proves it
    /// a natural. At one the decrement commits zero; at zero the
    /// difference is `-1`, the guard leaves no row, and the commit
    /// changes nothing rather than failing or writing a negative.
    #[dialog_common::test]
    async fn it_decrements_a_natural_counter_down_to_zero() -> Result<()> {
        let rule: InductiveRule = serde_json::from_value(json!({
            "description": "Decrement a counter on a decrement command, never below zero",
            "assert!": {
                "with": {
                    "count": { "the": "counter/count", "as": "natural:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "counter": { "the": "cmd.decrement/counter", "as": "entity:" }
                        }
                    },
                    "where": {
                        "counter": { "?": { "name": "this" } }
                    }
                },
                {
                    "assert": {
                        "with": {
                            "count": { "the": "counter/count", "as": "natural:" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "count": { "?": { "name": "prev" } }
                    }
                },
                {
                    "assert": "math/difference",
                    "where": {
                        "of": { "?": { "name": "prev" } },
                        "subtract": 1,
                        "is": { "?": { "name": "count" } }
                    }
                },
                {
                    "assert": ">=",
                    "where": {
                        "of": { "?": { "name": "count" } },
                        "with": 0
                    }
                }
            ]
        }))
        .expect("the guarded decrement compiles");

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let counter: Entity = "ctr:1".parse()?;
        branch
            .transaction()
            .assert(rule)
            .assert(
                dialog_query::the!("counter/count")
                    .of(counter.clone())
                    .is(1u64),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        for (command, expected) in [("cmd:1", 0u128), ("cmd:2", 0u128)] {
            let command: Entity = command.parse()?;
            branch
                .transaction()
                .dispatch(
                    dialog_query::the!("cmd.decrement/counter")
                        .of(command)
                        .is(counter.clone()),
                )
                .commit()
                .publish()
                .perform(&operator)
                .await?;
            branch.refresh(&operator).await?;
            assert_eq!(
                values(&branch, &operator, "counter/count", &counter).await?,
                vec![Value::UnsignedInt(expected)],
                "the counter never goes below zero"
            );
        }
        Ok(())
    }

    /// The two-replica route: a count written by another replica's rule
    /// arrives by pull (a raw commit here), so the next local commit
    /// replays it through the watermark lag as a seed row. It reads back
    /// as a natural, and must seed the `integer:` premise like a scan
    /// would, so the increment after a merge commits.
    #[dialog_common::test]
    async fn it_increments_an_integer_counter_after_a_pulled_count() -> Result<()> {
        use dialog_corpus::{Fact, Instruction};
        use futures_util::stream;

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let counter: Entity = "ctr:1".parse()?;
        branch
            .transaction()
            .assert(integer_increment_rule())
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        // The other replica's count lands without local induction.
        branch
            .commit(stream::iter(vec![Instruction::Assert(
                Fact {
                    the: "counter/count".parse()?,
                    of: counter.clone(),
                    is: Value::SignedInt(1),
                    cause: None,
                },
                dialog_corpus::Pick::All,
            )]))
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
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        assert_eq!(
            values(&branch, &operator, "counter/count", &counter).await?,
            vec![Value::UnsignedInt(2)],
            "the increment after a pull must read the pulled count through the integer view"
        );
        Ok(())
    }

    /// A durable write triggers a rule with no command anywhere; the
    /// `unless` guard blocks it while its fact holds; *retracting* the
    /// guard's fact newly enables the rule — the retraction is the
    /// trigger, reached through the `unless` premise's index entry.
    #[dialog_common::test]
    async fn it_triggers_on_durable_change_and_on_retraction_via_unless() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let rule: InductiveRule = serde_json::from_value(json!({
            "description": "A described task that is not done is open",
            "assert!": {
                "with": {
                    "desc": { "the": "task.open/desc", "as": "text:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "desc": { "the": "task/desc", "as": "text:" }
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
                            "done": { "the": "task/done", "as": "boolean:" }
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
            .assert(rule)
            .commit()
            .publish()
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
            .publish()
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
            .publish()
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
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let intermediate: ConceptDescriptor = serde_json::from_value(json!({
            "with": {
                "target": { "the": "cmd.stage/target", "as": "entity:" }
            }
        }))?;

        let stage: InductiveRule = serde_json::from_value(json!({
            "assert!": {
                "with": {
                    "target": { "the": "cmd.stage/target", "as": "entity:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "target": { "the": "cmd.start/target", "as": "entity:" }
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
                    "target": { "the": "result/target", "as": "entity:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "target": { "the": "cmd.stage/target", "as": "entity:" }
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
            .assert(stage)
            .assert(finish)
            .commit()
            .publish()
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
            .publish()
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

    /// A rule that relays a transient into another transient concept:
    /// `assert! {to} when {from}`, both `this`-keyed on the command.
    fn relay(from: &str, to: &str) -> Result<InductiveRule> {
        Ok(serde_json::from_value(json!({
            "assert!": {
                "with": {
                    "target": { "the": to, "as": "entity:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "target": { "the": from, "as": "entity:" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "target": { "?": { "name": "target" } }
                    }
                }
            ]
        }))?)
    }

    /// Mark the `{target}` concept over `the` as transient.
    fn transient_target(the: &str) -> Result<Transient> {
        let concept: ConceptDescriptor = serde_json::from_value(json!({
            "with": {
                "target": { "the": the, "as": "entity:" }
            }
        }))?;
        Ok(Transient(concept.this()))
    }

    /// The `(attribute, entity, value)` triples a [`Changes`] asserts.
    fn asserted(changes: &Changes) -> Vec<(String, Entity, Value)> {
        let mut triples: Vec<_> = changes
            .clone()
            .into_instructions()
            .into_iter()
            .filter_map(|instruction| match instruction {
                Instruction::Assert(a, _) => Some((a.the.to_string(), a.of, a.is)),
                Instruction::Retract(_) => None,
            })
            .collect();
        triples.sort_by_key(|(the, of, _)| (the.clone(), of.to_string()));
        triples
    }

    /// Transients a rule concludes are reported on the staged batch, from
    /// every round of the cascade, so a host whose handlers live outside
    /// the rule system can run them. The transient the transaction
    /// dispatched itself is not reported back.
    #[dialog_common::test]
    async fn it_reports_the_transients_induction_emitted() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .transaction()
            .assert(transient_target("cmd.stage/target")?)
            .assert(transient_target("cmd.finish/target")?)
            .assert(relay("cmd.start/target", "cmd.stage/target")?)
            .assert(relay("cmd.stage/target", "cmd.finish/target")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let command: Entity = "cmd:start".parse()?;
        let target: Entity = "doc:1".parse()?;
        let batch = branch
            .transaction()
            .dispatch(
                dialog_query::the!("cmd.start/target")
                    .of(command.clone())
                    .is(target.clone()),
            )
            .commit()
            .perform(&operator)
            .await?;

        assert_eq!(
            asserted(batch.induced()),
            vec![
                (
                    "cmd.finish/target".to_string(),
                    command.clone(),
                    Value::Entity(target.clone())
                ),
                (
                    "cmd.stage/target".to_string(),
                    command.clone(),
                    Value::Entity(target.clone())
                ),
            ],
            "both rounds' transient heads are reported, the dispatched command is not"
        );

        batch.publish().perform(&operator).await?;
        assert!(
            values(&branch, &operator, "cmd.finish/target", &command)
                .await?
                .is_empty(),
            "a reported transient still never reaches the branch"
        );
        Ok(())
    }

    /// Every link of a staged chain adds the transients its own induction
    /// emitted: publishing the chain runs all of them, so none may drop
    /// out when the next link is committed.
    #[dialog_common::test]
    async fn it_accumulates_induced_transients_across_staged_links() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .transaction()
            .assert(transient_target("cmd.stage/target")?)
            .assert(relay("cmd.start/target", "cmd.stage/target")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let first: Entity = "cmd:first".parse()?;
        let second: Entity = "cmd:second".parse()?;
        let target: Entity = "doc:1".parse()?;
        let batch = branch
            .transaction()
            .dispatch(
                dialog_query::the!("cmd.start/target")
                    .of(first.clone())
                    .is(target.clone()),
            )
            .commit()
            .perform(&operator)
            .await?
            .dispatch(
                dialog_query::the!("cmd.start/target")
                    .of(second.clone())
                    .is(target.clone()),
            )
            .commit()
            .perform(&operator)
            .await?;

        assert_eq!(
            asserted(batch.induced()),
            vec![
                (
                    "cmd.stage/target".to_string(),
                    first,
                    Value::Entity(target.clone())
                ),
                (
                    "cmd.stage/target".to_string(),
                    second,
                    Value::Entity(target)
                ),
            ]
        );
        Ok(())
    }

    /// Two transient-headed rules feeding each other exhaust the round
    /// bound and fail the commit instead of diverging.
    #[dialog_common::test]
    async fn it_errors_on_a_runaway_cascade() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let ping: ConceptDescriptor = serde_json::from_value(json!({
            "with": { "n": { "the": "cmd.ping/n", "as": "natural:" } }
        }))?;
        let pong: ConceptDescriptor = serde_json::from_value(json!({
            "with": { "n": { "the": "cmd.pong/n", "as": "natural:" } }
        }))?;

        let ping_to_pong: InductiveRule = serde_json::from_value(json!({
            "assert!": {
                "with": { "n": { "the": "cmd.pong/n", "as": "natural:" } }
            },
            "when": [{
                "assert": {
                    "with": { "n": { "the": "cmd.ping/n", "as": "natural:" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "n": { "?": { "name": "n" } }
                }
            }]
        }))?;
        let pong_to_ping: InductiveRule = serde_json::from_value(json!({
            "assert!": {
                "with": { "n": { "the": "cmd.ping/n", "as": "natural:" } }
            },
            "when": [{
                "assert": {
                    "with": { "n": { "the": "cmd.pong/n", "as": "natural:" } }
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
            .assert(ping_to_pong)
            .assert(pong_to_ping)
            .commit()
            .publish()
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
            .publish()
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
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let stamp = |command: &str, result: &str| -> InductiveRule {
            serde_json::from_value(json!({
                "assert!": {
                    "with": { "target": { "the": result, "as": "entity:" } }
                },
                "when": [{
                    "assert": {
                        "with": { "target": { "the": command, "as": "entity:" } }
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
            .assert(stamp("cmd.x/target", "result.x/target"))
            .assert(stamp("cmd.y/target", "result.y/target"))
            .commit()
            .publish()
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
            .publish()
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
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let consume: InductiveRule = serde_json::from_value(json!({
            "retract!": {
                "with": {
                    "body": { "the": "mailbox.message/body", "as": "text:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "message": { "the": "cmd.ack/message", "as": "entity:" }
                        }
                    },
                    "where": {
                        "message": { "?": { "name": "this" } }
                    }
                },
                {
                    "assert": {
                        "with": {
                            "body": { "the": "mailbox.message/body", "as": "text:" }
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
            .assert(consume)
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
            .publish()
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
            .publish()
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
        use dialog_query::DeductiveRule;

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // actor.status/duty of ?a is ?d  when  shift/duty of ?a is ?d
        let status: DeductiveRule = serde_json::from_value(json!({
            "deduce": {
                "with": { "duty": { "the": "actor.status/duty", "as": "text:" } }
            },
            "when": [{
                "assert": {
                    "with": { "duty": { "the": "shift/duty", "as": "text:" } }
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
                "with": { "note": { "the": "task/note", "as": "text:" } }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "actor": { "the": "inbox.message/actor", "as": "entity:" },
                            "body": { "the": "inbox.message/body", "as": "text:" }
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
                        "with": { "duty": { "the": "actor.status/duty", "as": "text:" } }
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
            .assert(status)
            .assert(notify)
            .commit()
            .publish()
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
            .publish()
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
            .publish()
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
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let stamp = |result: &str| -> InductiveRule {
            serde_json::from_value(json!({
                "assert!": {
                    "with": { "target": { "the": result, "as": "entity:" } }
                },
                "when": [{
                    "assert": {
                        "with": { "target": { "the": "cmd.z/target", "as": "entity:" } }
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
            .assert(stamp("result.first/target"))
            .commit()
            .publish()
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
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        // Second rule on the same attribute — the head advances, so
        // the cached discovery for on:cmd.z/target is stale now.
        branch
            .transaction()
            .assert(stamp("result.second/target"))
            .commit()
            .publish()
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
            .publish()
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
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .transaction()
            .assert(tagger())
            .commit()
            .publish()
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
            .publish()
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
            .retract(tagger())
            .commit()
            .publish()
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
            .publish()
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
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .transaction()
            .assert(tagger())
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let doc: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .retract(tagger())
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("hello".to_string()),
            )
            .commit()
            .publish()
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
        let (operator, profile) = test_session_with_peer().await;
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
            .publish()
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
            .publish()
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
        let (operator, profile) = test_session_with_peer().await;
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
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        // Installing the rule is the completing transition.
        branch
            .transaction()
            .assert(tagger())
            .commit()
            .publish()
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
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let drain: InductiveRule = serde_json::from_value(json!({
            "retract!": {
                "with": { "body": { "the": "queue.item/body", "as": "text:" } }
            },
            "when": [{
                "assert": {
                    "with": { "body": { "the": "queue.item/body", "as": "text:" } }
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
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        branch
            .transaction()
            .assert(drain)
            .commit()
            .publish()
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
        use dialog_query::DeductiveRule;

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alert: InductiveRule = serde_json::from_value(json!({
            "assert!": {
                "with": { "duty": { "the": "alert/duty", "as": "text:" } }
            },
            "when": [{
                "assert": {
                    "with": { "duty": { "the": "actor.status/duty", "as": "text:" } }
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
            .assert(alert)
            .assert(
                dialog_query::the!("shift/duty")
                    .of(actor.clone())
                    .is("on-duty".to_string()),
            )
            .commit()
            .publish()
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
                "with": { "duty": { "the": "actor.status/duty", "as": "text:" } }
            },
            "when": [{
                "assert": {
                    "with": { "duty": { "the": "shift/duty", "as": "text:" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "duty": { "?": { "name": "duty" } }
                }
            }]
        }))?;
        branch
            .transaction()
            .assert(status)
            .commit()
            .publish()
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
                "with": { "tag": { "the": "derived/tag", "as": "text:" } }
            },
            "when": [{
                "assert": {
                    "with": { "title": { "the": "doc/title", "as": "text:" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "title": { "?": { "name": "tag" } }
                }
            }]
        }))
        .expect("tagger rule compiles")
    }

    /// The induction watermark of a branch, as stored.
    async fn watermark<Env>(branch: &Branch, env: &Env) -> Result<Option<Revision>>
    where
        Env: Provider<Resolve> + ConditionalSync,
    {
        let cell = branch.induction_cell();
        cell.resolve().perform(env).await?;
        Ok(cell.content())
    }

    /// A machinery commit writes the reserved records induction drops,
    /// so it is an inducing instant with an empty delta: the watermark
    /// follows it when it stood at the head the commit built on. A lag
    /// an application fact left through the raw path stays where it
    /// is, and the next inducing instant catches up over both.
    #[dialog_common::test]
    async fn it_carries_the_watermark_over_a_machinery_commit() -> Result<()> {
        use dialog_corpus::{Fact, Instruction};
        use futures_util::stream;

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .transaction()
            .assert(tagger())
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert_eq!(
            watermark(&branch, &operator).await?,
            branch.revision(),
            "a transaction leaves the watermark at the head"
        );

        let record = |audience: &str| -> Result<Instruction> {
            Ok(Instruction::Assert(
                Fact {
                    the: "dialog.ucan/audience".parse()?,
                    of: "ucan:1".parse()?,
                    is: Value::String(audience.into()),
                    cause: None,
                },
                dialog_corpus::Pick::All,
            ))
        };

        // Reserved records alone: the watermark follows the head.
        branch
            .commit(stream::iter(vec![record("did:key:zAlice")?]))
            .machinery()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert_eq!(
            watermark(&branch, &operator).await?,
            branch.revision(),
            "a machinery commit carries the watermark to its head"
        );
        let carried = branch.revision();

        // An application fact through the raw path lags.
        let doc: Entity = "doc:1".parse()?;
        branch
            .commit(stream::iter(vec![Instruction::Assert(
                Fact {
                    the: "doc/title".parse()?,
                    of: doc.clone(),
                    is: Value::String("hello".into()),
                    cause: None,
                },
                dialog_corpus::Pick::All,
            )]))
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert_eq!(
            watermark(&branch, &operator).await?,
            carried,
            "a raw commit of an application fact leaves the watermark behind"
        );

        // A machinery commit over a lag leaves the lag for the next
        // inducing instant.
        branch
            .commit(stream::iter(vec![record("did:key:zBob")?]))
            .machinery()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert_eq!(
            watermark(&branch, &operator).await?,
            carried,
            "a machinery commit does not skip a lag"
        );
        assert!(
            values(&branch, &operator, "derived/tag", &doc)
                .await?
                .is_empty(),
            "nothing induced yet"
        );

        // The next inducing instant catches up over both commits.
        branch.induce(&operator).await?;
        branch.refresh(&operator).await?;
        assert_eq!(
            values(&branch, &operator, "derived/tag", &doc).await?,
            vec![Value::String("hello".into())],
            "the lagged fact induces at the next instant"
        );
        assert_eq!(
            watermark(&branch, &operator).await?,
            branch.revision(),
            "the catch-up carries the watermark to the head"
        );
        Ok(())
    }

    /// A raw [`Branch::commit`] bypasses induction — the model of a
    /// pull. The watermark records the lag, and the next inducing
    /// instant ([`Branch::induce`] here) catches up: the rule fires
    /// over facts that arrived through the raw path. A second induce
    /// is a no-op — the watermark is at the head.
    #[dialog_common::test]
    async fn it_catches_up_over_a_raw_commit() -> Result<()> {
        use dialog_corpus::{Fact, Instruction};
        use futures_util::stream;

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .transaction()
            .assert(tagger())
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        // Head advances without induction — the pull surrogate.
        let doc: Entity = "doc:1".parse()?;
        branch
            .commit(stream::iter(vec![Instruction::Assert(
                Fact {
                    the: "doc/title".parse()?,
                    of: doc.clone(),
                    is: Value::String("hello".into()),
                    cause: None,
                },
                dialog_corpus::Pick::All,
            )]))
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
        use dialog_corpus::{Fact, Instruction};
        use futures_util::stream;

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let pair: InductiveRule = serde_json::from_value(json!({
            "assert!": {
                "with": { "both": { "the": "derived/both", "as": "text:" } }
            },
            "when": [
                {
                    "assert": {
                        "with": { "p": { "the": "fact.p/v", "as": "text:" } }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "p": { "?": { "name": "both" } }
                    }
                },
                {
                    "assert": {
                        "with": { "q": { "the": "fact.q/v", "as": "text:" } }
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
            .assert(pair)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        // P arrives outside any transaction.
        let subject: Entity = "pair:1".parse()?;
        branch
            .commit(stream::iter(vec![Instruction::Assert(
                Fact {
                    the: "fact.p/v".parse()?,
                    of: subject.clone(),
                    is: Value::String("p".into()),
                    cause: None,
                },
                dialog_corpus::Pick::All,
            )]))
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
            .publish()
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
        use dialog_corpus::{Fact, Instruction};
        use futures_util::stream;

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // Rule and a matching fact both land through raw commits: the
        // branch has state and rules, but no induction ever ran and no
        // watermark exists.
        let mut install = dialog_corpus::Changes::new();
        install.assert(tagger());
        let old: Entity = "doc:old".parse()?;
        branch
            .commit(install.into_stream())
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        branch
            .commit(stream::iter(vec![Instruction::Assert(
                Fact {
                    the: "doc/title".parse()?,
                    of: old.clone(),
                    is: Value::String("old".into()),
                    cause: None,
                },
                dialog_corpus::Pick::All,
            )]))
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
            .commit(stream::iter(vec![Instruction::Assert(
                Fact {
                    the: "doc/title".parse()?,
                    of: fresh.clone(),
                    is: Value::String("new".into()),
                    cause: None,
                },
                dialog_corpus::Pick::All,
            )]))
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
        let (operator, profile) = test_session_with_peer().await;
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
            .publish()
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
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            unchanged, base,
            "an unconsumed command must not mint a revision"
        );
        Ok(())
    }

    /// A rules-free transaction commit carries the trigger footprint
    /// forward to the head it publishes, so the steady-state no-rules
    /// commit never re-pays the footprint range scans; a commit that
    /// installs a rule must NOT carry it (the next dispatch rescans).
    #[dialog_common::test]
    async fn it_carries_the_footprint_across_rule_free_commits() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .transaction()
            .assert(
                dialog_query::the!("doc/note")
                    .of("doc:1".parse::<Entity>()?)
                    .is("n".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        let head = branch.revision().expect("committed");
        assert!(
            branch.rule_cache().footprint(&head).is_some(),
            "a rules-free commit records the footprint at its new head"
        );

        branch
            .transaction()
            .assert(
                dialog_query::the!("doc/note")
                    .of("doc:2".parse::<Entity>()?)
                    .is("n".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        let head = branch.revision().expect("committed");
        assert!(
            branch.rule_cache().footprint(&head).is_some(),
            "the carried footprint keeps riding rules-free commits"
        );

        let stamp: InductiveRule = serde_json::from_value(json!({
            "assert!": {
                "with": { "target": { "the": "result.carry/target", "as": "entity:" } }
            },
            "when": [{
                "assert": {
                    "with": { "target": { "the": "cmd.carry/target", "as": "entity:" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "target": { "?": { "name": "target" } }
                }
            }]
        }))
        .expect("rule compiles");
        branch
            .transaction()
            .assert(stamp)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        let head = branch.revision().expect("committed");
        assert!(
            branch.rule_cache().footprint(&head).is_none(),
            "a rule install invalidates the footprint; the next dispatch rescans"
        );
        Ok(())
    }

    /// The list concept: every position-named entry of `todo.list`,
    /// valued by a member entity.
    fn list_members() -> serde_json::Value {
        json!({
            "with": {
                "member": {
                    "the": { "domain": "todo.list", "keyed": "sequence" },
                    "cardinality": "many",
                    "as": "entity:"
                }
            }
        })
    }

    /// The entries of `list`, in key order.
    async fn members<Env>(branch: &Branch, env: &Env, list: &Entity) -> Result<Vec<(String, Value)>>
    where
        Env: Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + dialog_common::Holds
            + ConditionalSync
            + 'static,
    {
        let selector = Selector::new()
            .the_starting_with("todo.list/".to_string())
            .of(list.clone());
        let stream = branch.claims().select(selector).perform(env).await?;
        let facts: Vec<_> = stream.collect().await;
        let mut entries: Vec<(String, Value)> = facts
            .into_iter()
            .map(|item| item.and_then(|view| view.to_owned()))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|fact| (fact.the.name().to_owned(), fact.is))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(entries)
    }

    /// An inductive rule derives INTO a sequence: the head's entry is
    /// written under `todo.list/<key>`, the key being what the body
    /// bound — here `dialog/position` deriving a first position for
    /// the member.
    #[dialog_common::test]
    async fn it_induces_an_entry_into_a_sequence() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let rule: InductiveRule = serde_json::from_value(json!({
            "description": "An item joins its list",
            "assert!": list_members(),
            "when": [
                {
                    "assert": {
                        "with": {
                            "list": { "the": "todo.item/list", "as": "entity:" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "member" } },
                        "list": { "?": { "name": "this" } }
                    }
                },
                {
                    "assert": "dialog/position",
                    "where": {
                        "member": { "?": { "name": "member" } },
                        "after": "",
                        "before": "",
                        "is": { "?": { "name": "member/key" } }
                    }
                }
            ]
        }))?;
        let list: Entity = "list:1".parse()?;
        let item: Entity = "item:1".parse()?;
        branch
            .transaction()
            .assert(rule)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        branch
            .transaction()
            .assert(
                dialog_query::the!("todo.item/list")
                    .of(item.clone())
                    .is(list.clone()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let entries = members(&branch, &operator, &list).await?;
        assert_eq!(entries.len(), 1, "one entry for the one item");
        let (key, value) = &entries[0];
        assert!(
            key.starts_with(|c: char| c.is_ascii_uppercase()),
            "the key is a position: {key}"
        );
        assert_eq!(value, &Value::Entity(item));
        Ok(())
    }

    /// A command naming every field of a premise seeds the rule once per
    /// field, and each evaluation finds the same binding. Induction writes
    /// the head once, not once per seed: the head here is a set, which
    /// keeps every copy written to it.
    #[dialog_common::test]
    async fn it_writes_a_head_once_however_many_rows_seed_it() -> Result<()> {
        use dialog_corpus::Statement;

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let rule: InductiveRule = serde_json::from_value(json!({
            "description": "Record who an assign command names",
            "assert!": {
                "with": {
                    "assignee": { "the": "issue/assignee", "as": "text:", "pick": "all:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "issue": { "the": "cmd.assign/issue", "as": "entity:" },
                            "who": { "the": "cmd.assign/who", "as": "text:" },
                            "note": { "the": "cmd.assign/note", "as": "text:" }
                        }
                    },
                    "where": {
                        "issue": { "?": { "name": "this" } },
                        "who": { "?": { "name": "assignee" } },
                        "note": { "?": { "name": "note" } }
                    }
                }
            ]
        }))?;
        branch
            .transaction()
            .assert(rule)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let command: Entity = "cmd:1".parse()?;
        let issue: Entity = "issue:1".parse()?;
        let mut transients = Changes::new();
        dialog_query::the!("cmd.assign/issue")
            .of(command.clone())
            .is(issue.clone())
            .assert(&mut transients);
        dialog_query::the!("cmd.assign/who")
            .of(command.clone())
            .is("member-0".to_string())
            .assert(&mut transients);
        dialog_query::the!("cmd.assign/note")
            .of(command)
            .is("from the board".to_string())
            .assert(&mut transients);

        let mut changes = Changes::new();
        super::induce(
            &Live::from(&branch).pin(),
            &mut changes,
            transients,
            &operator,
        )
        .await?;

        assert_eq!(
            asserted(&changes),
            vec![(
                "issue/assignee".to_string(),
                issue,
                Value::String("member-0".to_string())
            )],
            "three seeds of one binding write its head once"
        );
        Ok(())
    }

    /// The first positive concept premise of `rule`.
    fn first_premise(rule: &InductiveRule) -> dialog_query::ConceptQuery {
        use dialog_query::{Premise, Proposition};
        rule.analysis()
            .premises
            .iter()
            .find_map(|premise| match premise {
                Premise::Assert(Proposition::Concept(query)) => Some(query.clone()),
                _ => None,
            })
            .expect("the rule has a concept premise")
    }

    /// Rows of one entity may seed together when each binds one distinct
    /// single attribute read under `last`, wherever the row came from. A
    /// set field and two rows to one field each seed alone.
    #[dialog_common::test]
    fn it_groups_rows_that_bind_distinct_last_fields() -> Result<()> {
        let rule: InductiveRule = serde_json::from_value(json!({
            "assert!": {
                "with": { "seen": { "the": "seen/who", "as": "text:", "pick": "all:" } }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "issue": { "the": "cmd.assign/issue", "as": "entity:" },
                            "who": { "the": "cmd.assign/who", "as": "text:" },
                            "tag": { "the": "cmd.assign/tag", "as": "text:", "pick": "all:" }
                        }
                    },
                    "where": {
                        "issue": { "?": { "name": "this" } },
                        "who": { "?": { "name": "seen" } },
                        "tag": { "?": { "name": "tag" } }
                    }
                }
            ]
        }))?;
        let query = first_premise(&rule);
        let row = |the: &str, of: &str, is: Value| -> Result<Fact> {
            Ok(Fact {
                the: the.parse()?,
                of: of.parse()?,
                is,
                cause: None,
            })
        };
        let text = |value: &str| Value::String(value.to_string());
        let rows = vec![
            row(
                "cmd.assign/issue",
                "cmd:1",
                Value::Entity("issue:1".parse()?),
            )?,
            row("cmd.assign/who", "cmd:1", text("member-0"))?,
            row("cmd.assign/tag", "cmd:1", text("urgent"))?,
            row("cmd.assign/who", "cmd:2", text("member-1"))?,
            row("cmd.assign/who", "cmd:3", text("member-2"))?,
            row("cmd.assign/who", "cmd:3", text("member-3"))?,
            row("unrelated/name", "cmd:1", text("ignored"))?,
        ];
        let mut groups = super::seed_groups(&query, &rows);
        groups.sort();
        assert_eq!(
            groups,
            vec![vec![0, 1], vec![2], vec![3], vec![4], vec![5]],
            "cmd:1's `last` rows may seed together; its set row and cmd:3's \
             two writes to one field seed alone; cmd:2's one row is a group \
             of one; a row naming no field seeds nothing"
        );
        Ok(())
    }

    /// A rule whose premise reads three `last` fields of an assign
    /// command and records the `who` it names as the issue's assignee.
    fn assign_rule() -> Result<InductiveRule> {
        Ok(serde_json::from_value(json!({
            "description": "Record who an assign command names",
            "assert!": {
                "with": {
                    "assignee": { "the": "issue/assignee", "as": "text:", "pick": "all:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "issue": { "the": "cmd.assign/issue", "as": "entity:" },
                            "who": { "the": "cmd.assign/who", "as": "text:" },
                            "note": { "the": "cmd.assign/note", "as": "text:" }
                        }
                    },
                    "where": {
                        "issue": { "?": { "name": "this" } },
                        "who": { "?": { "name": "assignee" } },
                        "note": { "?": { "name": "note" } }
                    }
                }
            ]
        }))?)
    }

    /// The happy path: the command's three rows bind one evaluation,
    /// which finds the one binding seeding them one by one finds.
    #[dialog_common::test]
    async fn it_evaluates_a_command_once_when_its_fields_bind_together() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch
            .transaction()
            .assert(assign_rule()?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let command: Entity = "cmd:1".parse()?;
        let issue: Entity = "issue:1".parse()?;
        super::oracle::evaluations();
        branch
            .transaction()
            .dispatch(
                dialog_query::the!("cmd.assign/issue")
                    .of(command.clone())
                    .is(issue.clone()),
            )
            .dispatch(
                dialog_query::the!("cmd.assign/who")
                    .of(command.clone())
                    .is("member-0".to_string()),
            )
            .dispatch(
                dialog_query::the!("cmd.assign/note")
                    .of(command)
                    .is("from the board".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            super::oracle::evaluations(),
            1,
            "three rows of one command, one evaluation"
        );
        branch.refresh(&operator).await?;
        assert_eq!(
            values(&branch, &operator, "issue/assignee", &issue).await?,
            vec![Value::String("member-0".to_string())]
        );
        Ok(())
    }

    /// A session overlay row stands above every staged write, so the
    /// round view elects the overlay's `who`, not the command's. Bound
    /// together the rows find nothing; seeded one by one, the rows that
    /// leave `who` free read the overlay's value and match. The firing
    /// falls back to them and derives what they derive.
    #[dialog_common::test]
    async fn it_falls_back_when_an_overlay_row_stands_above_a_grouped_field() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch
            .transaction()
            .assert(assign_rule()?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let command: Entity = "cmd:1".parse()?;
        let issue: Entity = "issue:1".parse()?;
        branch.overlay().assert(
            dialog_query::the!("cmd.assign/who")
                .of(command.clone())
                .is("from-the-session".to_string()),
        )?;
        super::oracle::evaluations();
        branch
            .transaction()
            .dispatch(
                dialog_query::the!("cmd.assign/issue")
                    .of(command.clone())
                    .is(issue.clone()),
            )
            .dispatch(
                dialog_query::the!("cmd.assign/who")
                    .of(command.clone())
                    .is("member-0".to_string()),
            )
            .dispatch(
                dialog_query::the!("cmd.assign/note")
                    .of(command)
                    .is("from the board".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            super::oracle::evaluations(),
            4,
            "the group found nothing, so each of its three rows was seeded alone"
        );
        branch.overlay().clear();
        branch.refresh(&operator).await?;
        assert_eq!(
            values(&branch, &operator, "issue/assignee", &issue).await?,
            vec![Value::String("from-the-session".to_string())],
            "the rows left free read the overlay's value, as seeding them alone does"
        );
        Ok(())
    }

    /// A deductive rule derives `who` from `alias`; its candidate stands
    /// past the commit's own `who` claim, so `last` elects the derived
    /// value. The commit writes `who` itself, so the rule is seeded, not
    /// fully evaluated, and the group binding the commit's `who` finds
    /// nothing. The firing falls back to the rows one by one.
    #[dialog_common::test]
    async fn it_falls_back_when_a_derived_candidate_displaces_a_grouped_field() -> Result<()> {
        use dialog_query::DeductiveRule;

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alias: DeductiveRule = serde_json::from_value(json!({
            "deduce": {
                "with": { "who": { "the": "cmd.assign/who", "as": "text:" } }
            },
            "when": [{
                "assert": {
                    "with": { "alias": { "the": "cmd.assign/alias", "as": "text:" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "alias": { "?": { "name": "who" } }
                }
            }]
        }))?;
        branch
            .transaction()
            .assert(alias)
            .assert(assign_rule()?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let command: Entity = "cmd:1".parse()?;
        let issue: Entity = "issue:1".parse()?;
        branch
            .transaction()
            .assert(
                dialog_query::the!("cmd.assign/issue")
                    .of(command.clone())
                    .is(issue.clone()),
            )
            .assert(
                dialog_query::the!("cmd.assign/who")
                    .of(command.clone())
                    .is("member-0".to_string()),
            )
            .assert(
                dialog_query::the!("cmd.assign/note")
                    .of(command.clone())
                    .is("from the board".to_string()),
            )
            .assert(
                dialog_query::the!("cmd.assign/alias")
                    .of(command)
                    .is("alias-0".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        let assignees = values(&branch, &operator, "issue/assignee", &issue).await?;
        assert!(
            assignees.contains(&Value::String("alias-0".to_string())),
            "the rows left free read the derived `who`, as seeding them alone does: {assignees:?}"
        );
        Ok(())
    }

    /// A durable `who` written in round 1 and a transient `who` emitted
    /// in round 2 for the same command stand equal in round 3's view:
    /// both at the pending edition, neither succeeding the other, so
    /// `last` elects by [`Value::tiebreak`]. The values are chosen so
    /// the durable one wins. The command's transient rows group in
    /// round 3 and bind the transient `who`, which `last` does not
    /// elect, so the group finds nothing; seeded one by one, the rows
    /// that leave `who` free read the durable value and match. The
    /// firing falls back to them and writes what they write.
    ///
    /// Checking a group against the stimulus alone (whether the round's
    /// rows hold another claim of a grouped cell) could not see this:
    /// round 3's stimulus holds one `who`, the transient, and the claim
    /// that displaces it was folded into the commit two rounds before.
    #[dialog_common::test]
    async fn it_falls_back_when_a_round_one_write_ties_a_round_two_transient() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // Round 1 stages the command's fields as transients; round 2
        // turns them into the command, also transient; round 3 fires
        // `assign_rule` on it.
        let staged = json!({
            "who": { "the": "stage/who", "as": "text:" },
            "issue": { "the": "stage/issue", "as": "entity:" },
            "note": { "the": "stage/note", "as": "text:" }
        });
        let command_fields = json!({
            "who": { "the": "cmd.assign/who", "as": "text:" },
            "issue": { "the": "cmd.assign/issue", "as": "entity:" },
            "note": { "the": "cmd.assign/note", "as": "text:" }
        });
        let bind = json!({
            "this": { "?": { "name": "this" } },
            "who": { "?": { "name": "who" } },
            "issue": { "?": { "name": "issue" } },
            "note": { "?": { "name": "note" } }
        });
        let stage: InductiveRule = serde_json::from_value(json!({
            "assert!": { "with": staged },
            "when": [{
                "assert": { "with": {
                    "who": { "the": "src/who", "as": "text:" },
                    "issue": { "the": "src/issue", "as": "entity:" },
                    "note": { "the": "src/note", "as": "text:" }
                } },
                "where": bind
            }]
        }))?;
        let issue_command: InductiveRule = serde_json::from_value(json!({
            "assert!": { "with": command_fields },
            "when": [{ "assert": { "with": staged }, "where": bind }]
        }))?;
        let transient = |with: &serde_json::Value| -> Result<Transient> {
            let concept: ConceptDescriptor = serde_json::from_value(json!({ "with": with }))?;
            Ok(Transient(concept.this()))
        };
        branch
            .transaction()
            .assert(assign_rule()?)
            .assert(stage)
            .assert(issue_command)
            .assert(transient(&staged)?)
            .assert(transient(&command_fields)?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let (a, b) = (
            Value::String("member-a".to_string()),
            Value::String("member-b".to_string()),
        );
        let (durable, transient) = if a.tiebreak() > b.tiebreak() {
            (a, b)
        } else {
            (b, a)
        };
        let (Value::String(durable_who), Value::String(transient_who)) =
            (durable.clone(), transient)
        else {
            unreachable!("both values are text");
        };

        let command: Entity = "cmd:1".parse()?;
        let issue: Entity = "issue:1".parse()?;
        super::oracle::evaluations();
        branch
            .transaction()
            .assert(
                dialog_query::the!("cmd.assign/who")
                    .of(command.clone())
                    .is(durable_who),
            )
            .assert(
                dialog_query::the!("src/who")
                    .of(command.clone())
                    .is(transient_who),
            )
            .assert(
                dialog_query::the!("src/issue")
                    .of(command.clone())
                    .is(issue.clone()),
            )
            .assert(
                dialog_query::the!("src/note")
                    .of(command)
                    .is("from the board".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            super::oracle::evaluations(),
            // Round 1: `assign_rule` seeded by the durable `who` alone,
            // and `stage` by its three rows together. Round 2:
            // `issue_command` by its three rows together. Round 3:
            // `assign_rule` by the command's three rows together, which
            // find nothing, then by each alone.
            1 + 1 + 1 + (1 + 3),
            "the round-3 group found nothing, so each of its three rows was seeded alone"
        );
        branch.refresh(&operator).await?;
        assert_eq!(
            values(&branch, &operator, "issue/assignee", &issue).await?,
            vec![durable],
            "the rows left free read the durable `who` the tie elects, as seeding them alone does"
        );
        Ok(())
    }

    /// A row the watermark lag carries in groups with this commit's rows
    /// of the same entity: the issue arrives by a raw commit, the `who`
    /// and `note` by an inducing one, and the firing derives the head
    /// as seeding them one by one does.
    #[dialog_common::test]
    async fn it_groups_a_lag_row_with_the_commit_rows_of_its_entity() -> Result<()> {
        use futures_util::stream;

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch
            .transaction()
            .assert(assign_rule()?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let command: Entity = "cmd:1".parse()?;
        let issue: Entity = "issue:1".parse()?;
        branch
            .commit(stream::iter(vec![Instruction::Assert(
                Fact {
                    the: "cmd.assign/issue".parse()?,
                    of: command.clone(),
                    is: Value::Entity(issue.clone()),
                    cause: None,
                },
                dialog_corpus::Pick::Last,
            )]))
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        super::oracle::evaluations();
        branch
            .transaction()
            .assert(
                dialog_query::the!("cmd.assign/who")
                    .of(command.clone())
                    .is("member-0".to_string()),
            )
            .assert(
                dialog_query::the!("cmd.assign/note")
                    .of(command)
                    .is("from the board".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            super::oracle::evaluations(),
            1,
            "the lag's row and the commit's two, one evaluation"
        );
        branch.refresh(&operator).await?;
        assert_eq!(
            values(&branch, &operator, "issue/assignee", &issue).await?,
            vec![Value::String("member-0".to_string())]
        );
        Ok(())
    }

    /// A set field shows every value it holds, not only the one this
    /// commit adds, so a row binding it never seeds with the others:
    /// the title row, seeded alone, still finds the tag the branch
    /// already held.
    #[dialog_common::test]
    async fn it_finds_older_values_of_a_set_field_a_row_leaves_free() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let rule: InductiveRule = serde_json::from_value(json!({
            "description": "Record each tag a titled document holds",
            "assert!": {
                "with": { "tag": { "the": "seen/tag", "as": "text:", "pick": "all:" } }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "tag": { "the": "doc/tag", "as": "text:", "pick": "all:" },
                            "title": { "the": "doc/title", "as": "text:" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "tag": { "?": { "name": "tag" } },
                        "title": { "?": { "name": "title" } }
                    }
                }
            ]
        }))?;
        let doc: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .assert(
                dialog_query::the!("doc/tag")
                    .of(doc.clone())
                    .is("old".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        branch
            .transaction()
            .assert(rule)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        branch
            .transaction()
            .assert(
                dialog_query::the!("doc/tag")
                    .of(doc.clone())
                    .is("new".to_string()),
            )
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Title".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let mut tags = values(&branch, &operator, "seen/tag", &doc).await?;
        tags.sort_by_key(|value| format!("{value:?}"));
        assert_eq!(
            tags,
            vec![
                Value::String("new".to_string()),
                Value::String("old".to_string())
            ],
            "the title's seed reaches the tag the branch already held"
        );
        Ok(())
    }

    /// An inductive rule READING a sequence wakes for a member write:
    /// the write's attribute probes the domain half's cover key, the
    /// rule seeds with both the value and the key bound, and its head
    /// records the key it saw.
    #[dialog_common::test]
    async fn it_fires_a_rule_reading_a_collection() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let rule: InductiveRule = serde_json::from_value(json!({
            "description": "Record where a member sits",
            "assert!": {
                "with": {
                    "key": { "the": "seen/key", "as": "text:" }
                }
            },
            "when": [
                {
                    "assert": list_members(),
                    "where": {
                        "this": { "?": { "name": "list" } },
                        "member": {
                            "the": { "?": { "name": "key" } },
                            "is": { "?": { "name": "this" } }
                        }
                    }
                }
            ]
        }))?;
        let list: Entity = "list:1".parse()?;
        let item: Entity = "item:1".parse()?;
        branch
            .transaction()
            .assert(rule)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let member: dialog_corpus::Relation = "todo.list/N5".parse()?;
        branch
            .transaction()
            .assert(
                dialog_query::Relation::from(member)
                    .of(list.clone())
                    .is(item.clone()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        assert_eq!(
            values(&branch, &operator, "seen/key", &item).await?,
            vec![Value::String("N5".to_string())],
            "the rule fired on the member write with the key bound"
        );
        Ok(())
    }

    /// A head entry whose key has the wrong shape for its collection
    /// fails the commit rather than landing in the other half of the
    /// domain: a dictionary keyed by an uppercase name would be a
    /// sequence member.
    #[dialog_common::test]
    async fn it_refuses_a_key_of_the_wrong_shape() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let rule: InductiveRule = serde_json::from_value(json!({
            "description": "File an item under its name",
            "assert!": {
                "with": {
                    "entry": {
                        "the": { "domain": "todo.named", "keyed": "dictionary" },
                        "cardinality": "many",
                        "as": "entity:"
                    }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "list": { "the": "todo.item/list", "as": "entity:" },
                            "name": { "the": "todo.item/name", "as": "text:" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "entry" } },
                        "list": { "?": { "name": "this" } },
                        "name": { "?": { "name": "entry/key" } }
                    }
                }
            ]
        }))?;
        let list: Entity = "list:1".parse()?;
        let item: Entity = "item:1".parse()?;
        branch
            .transaction()
            .assert(rule)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let result = branch
            .transaction()
            .assert(
                dialog_query::the!("todo.item/list")
                    .of(item.clone())
                    .is(list.clone()),
            )
            .assert(
                dialog_query::the!("todo.item/name")
                    .of(item.clone())
                    .is("Milk".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await;
        assert!(
            matches!(result, Err(CommitError::Induction(ref message)) if message.contains("Milk")),
            "an uppercase key is not a dictionary key: {result:?}"
        );
        Ok(())
    }
}
