//! Commit-time induction: trigger-indexed dispatch of inductive rules.
//!
//! Runs as step 0 of a transaction commit
//! ([`TransactionCommit::perform`](super::TransactionCommit::perform)),
//! before the durable batch is applied. The commit's delta — durable
//! asserts, durable retracts, and dispatched transients alike — seeds a
//! round loop:
//!
//! 1. The round's stimulus yields the set of *touched attributes*; each
//!    probes the `db.rule/on` trigger index (against the transaction
//!    view, so rules installed in the same commit fire). Rules that
//!    don't watch a touched attribute are never loaded, planned, or
//!    evaluated — dispatch cost follows the delta, not the rule
//!    population.
//! 2. Each candidate's body evaluates against the frozen round view
//!    (branch ⊕ durable changes ⊕ this round's transients). Sibling
//!    rules in a round read identical state.
//! 3. Bound heads emit facts by cardinality (`Replace` for one,
//!    `Assert` for many). A head concept carrying the
//!    `db.concept/transient` marker routes to the next round's
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

use std::collections::BTreeSet;

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, Attribute, Changes, Entity, Instruction, Select, Statement, Value,
};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::memory::Resolve;
use dialog_query::{Any, Binding, Cardinality, Environment, InductiveRule, Match, Term};
use futures_util::TryStreamExt;

use crate::layer::tombstones_from;
use crate::repository::branch::QueryLayer;
use crate::repository::branch::session::QueryEnv;
use crate::rules::{
    hydrate_inductive, on_attr, on_entity, reads_attr, source_attr, transient_attr,
};
use crate::{Branch, CommitError, RemoteSite};

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
    // Round 1 stimulus: everything the commit changes.
    let mut stimulus: Vec<Instruction> = changes.clone().into_instructions();
    stimulus.extend(transients.clone().into_instructions());
    if stimulus.is_empty() {
        return Ok(());
    }

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
        let mut touched: BTreeSet<Attribute> = stimulus
            .iter()
            .map(|instruction| match instruction {
                Instruction::Assert(a) | Instruction::Replace(a) | Instruction::Retract(a) => {
                    a.the.clone()
                }
            })
            .collect();

        // The frozen round view: branch ⊕ durable changes ⊕ this
        // round's transients, through the same layered QueryEnv a
        // transaction query uses, so rule bodies read exactly what a
        // mid-transaction query would.
        let mut view = changes.clone();
        transient_overlay.clone().assert(&mut view);
        let overlay = QueryLayer::from(branch).with(view).overlay(&operator);
        let tombstones = tombstones_from(&overlay);
        let view = QueryEnv::new(vec![branch.clone()], overlay, tombstones, env);

        // Close the touched set over derivation: a base-fact write
        // reaches inductive rules premised on the derived concepts it
        // (transitively) supports through deductive rules.
        expand_through_deduction(&view, &mut touched).await?;

        // Trigger-indexed discovery: one `db.rule/on` lookup per
        // touched attribute. Nothing ever enumerates all rules.
        let mut candidates: BTreeSet<Entity> = BTreeSet::new();
        for attribute in &touched {
            let Some(on) = on_entity(attribute) else {
                continue;
            };
            let selector = ArtifactSelector::new().the(on_attr()).is(Value::Entity(on));
            let claims = select(&view, selector).await?;
            candidates.extend(claims.into_iter().map(|artifact| artifact.of));
        }

        let mut novelty = Changes::new();
        let mut emitted_transients = Changes::new();
        for entity in candidates {
            let Some(rule) = load(&view, &entity).await? else {
                continue;
            };
            fire(&rule, &view, &mut novelty, &mut emitted_transients).await?;
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

/// Close `touched` over the deductive support graph: for each touched
/// attribute, `db.rule/reads` names the deductive rules whose bodies
/// read it; their conclusions' attributes are *derived-touched* — a
/// write to the base can flip them — and recurse until the frontier is
/// exhausted. An inductive premise on a derived concept then probes
/// exactly like one on a base concept.
///
/// The closure is composed fresh from per-rule facts at dispatch time,
/// never stored, so a deductive rule installed after an inductive one
/// is picked up automatically. Monotone over a finite attribute set,
/// so termination is structural. Polarity is deliberately ignored
/// across derived edges: through negation, an assertion of a base fact
/// can retract a derived one, so any change to a support attribute
/// counts.
async fn expand_through_deduction<'a, Env>(
    view: &QueryEnv<'a, Env>,
    touched: &mut BTreeSet<Attribute>,
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
    let mut frontier: Vec<Attribute> = touched.iter().cloned().collect();
    while let Some(attribute) = frontier.pop() {
        let Some(on) = on_entity(&attribute) else {
            continue;
        };
        let selector = ArtifactSelector::new()
            .the(reads_attr())
            .is(Value::Entity(on));
        for claim in select(view, selector).await? {
            let sources = select(
                view,
                ArtifactSelector::new().the(source_attr()).of(claim.of),
            )
            .await?;
            let Some(bytes) = sources.into_iter().find_map(|artifact| match artifact.is {
                Value::Bytes(bytes) => Some(bytes),
                _ => None,
            }) else {
                continue;
            };
            // A `reads` entry only ever hangs off a deductive rule; a
            // body that fails to decode is skipped like any dangling
            // index entry.
            let Ok(rule) = dialog_query::DeductiveRule::decode(&bytes) else {
                continue;
            };
            for (_, field) in rule.conclusion().with().iter() {
                let derived: Attribute = field.descriptor().the().clone().into();
                if touched.insert(derived.clone()) {
                    frontier.push(derived);
                }
            }
        }
    }
    Ok(())
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

/// Hydrate the inductive rule stored at `entity` from its
/// `db.rule/source` claim in the view. A dangling trigger-index entry
/// (source retracted, index entry surviving) is skipped rather than
/// failing the commit.
async fn load<'a, Env>(
    view: &QueryEnv<'a, Env>,
    entity: &Entity,
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
    let selector = ArtifactSelector::new()
        .the(source_attr())
        .of(entity.clone());
    let claims = select(view, selector).await?;
    let Some(bytes) = claims.into_iter().find_map(|artifact| match artifact.is {
        Value::Bytes(bytes) => Some(bytes),
        _ => None,
    }) else {
        return Ok(None);
    };
    hydrate_inductive(&bytes)
        .map(Some)
        .map_err(|error| CommitError::Induction(error.to_string()))
}

/// Evaluate one rule's body against the frozen round view and emit its
/// head for every binding: transient heads into `transients`, durable
/// heads (novelty-checked against the view) into `novelty`.
async fn fire<'a, Env>(
    rule: &InductiveRule,
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

    let conclusion = rule.conclusion();
    let transient_head = is_transient(view, &conclusion.this()).await?;

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
            match field.descriptor().cardinality() {
                Cardinality::One => {
                    dialog_artifacts::Update::associate_unique(
                        &mut head,
                        attribute,
                        this.clone(),
                        value,
                    );
                }
                Cardinality::Many => {
                    dialog_artifacts::Update::associate(&mut head, attribute, this.clone(), value);
                }
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

/// Whether the concept at `entity` carries the `db.concept/transient`
/// marker in the view (branch or overlay — a command declared in the
/// same commit as its first use counts).
async fn is_transient<'a, Env>(
    view: &QueryEnv<'a, Env>,
    concept: &Entity,
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
    let selector = ArtifactSelector::new()
        .the(transient_attr())
        .of(concept.clone());
    Ok(!select(view, selector).await?.is_empty())
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

    /// A rule's transient head (marked `db.concept/transient`) becomes
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

    /// The inbox/duty scenario with the duty status *derived*: the
    /// inductive rule's premise names `actor.status/duty`, which no
    /// commit ever writes — a deductive rule concludes it from
    /// `shift/duty`. A message arrives while the actor is off duty
    /// (rule probed via the inbox attributes, join fails); then a
    /// `shift/duty` write flips the derived status. The dispatch
    /// closure must carry that base write through `db.rule/reads` to
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
