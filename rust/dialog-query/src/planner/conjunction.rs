use super::Plan;
use crate::attribute::query::DynamicAttributeQuery;
use crate::concept::query::ConceptQuery;
use crate::merge_join::left_merge_join;
use crate::optional::OptionalAttributeQuery;
use crate::query::Application;
use crate::selection::{Match, Selection};
use crate::types::Any;
use crate::{Environment, EvaluationError, SortOrder, Term, multi_merge_join};
use core::pin::Pin;
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_corpus::selector::Constrained;
use dialog_corpus::{Estimate, Likelihood, Preload, PreloadRequest, Selector};
use futures_util::{StreamExt, stream};
use std::collections::HashSet;
use std::sync::Arc;

/// Largest ratio of the widest to narrowest scan range estimate that still
/// takes the merge path. Above it, one scan is selective enough that a nested
/// loop driving from it reads fewer blocks than a merge scanning every range
/// in full.
///
/// The estimates come from the tree (per-child
/// [`Scale`](dialog_search_tree::Scale) sums with exact edges, a couple of
/// blocks per level per scan), so an
/// all-broad concept join — every attribute a full entity range — has a ratio
/// near 1, while pinning one attribute to a value collapses that scan's range
/// to a narrow band and pushes the ratio well past this threshold.
const MERGE_COST_BALANCE: usize = 3;

/// An ordered sequence of [`Plan`] steps produced by the query planner.
///
/// A `Conjunction` is the main execution plan for a conjunction of premises.
/// The planner orders the steps so that each step's prerequisites are
/// satisfied by the bindings produced by earlier steps. At evaluation time,
/// the join feeds an initial [`Match`](crate::selection::Match) stream
/// through each step in order, progressively binding more variables.
///
/// The `cost` field is the sum of all step costs and is used when comparing
/// alternative plans (e.g. across different rule bodies in a [`Disjunction`](super::Disjunction)).
///
/// Create a `Conjunction` via [`Planner::plan`](super::Planner::plan). To
/// re-plan for a different scope, plan the rule's premises again; a
/// `Conjunction` is a finalized plan, not a re-planner.
#[derive(Debug, Clone, PartialEq)]
pub struct Conjunction {
    /// The ordered steps to execute
    pub steps: Vec<Plan>,
    /// Total execution cost
    pub cost: usize,
    /// Variables provided/bound by this join
    pub binds: Environment,
    /// Variables required in the environment to execute this join
    pub env: Environment,
}

impl Conjunction {
    /// Evaluate this conjunction by executing all steps in order.
    /// Each step feeds its output as input to the next, building up bindings.
    ///
    /// Returns `Pin<Box<...>>` because each step's output type depends on the
    /// previous step. Boxing erases the nesting from the type and keeps each
    /// step at pointer size on the stack.
    pub fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        selection: M,
        env: &'a Env,
    ) -> Pin<Box<dyn Selection + 'a>>
    where
        Env: crate::Scope<'a>,
    {
        // A conjunction whose every step is an attribute scan sorted on the
        // same free variable is an equi-join on that variable (the shape a
        // concept's implicit rule always has). Such a run *can* be evaluated
        // as an N-way merge: each scan runs once and the sorted outputs are
        // intersected, rather than probing the later scans once per row of
        // the earlier ones. Whether the merge actually wins depends on the
        // incoming row's bindings (a pinned selective attribute favors the
        // nested loop), so that choice is made per query inside
        // `evaluate_maybe_merge`. Any structurally ineligible conjunction
        // keeps the fold unchanged.
        if let Some(variable) = self.merge_variable() {
            return self.evaluate_maybe_merge(selection, env, variable);
        }

        self.into_fold(selection, env)
    }

    /// The nested-loop fold: each step feeds its output to the next.
    fn into_fold<'a, Env, M: Selection + 'a>(
        self,
        selection: M,
        env: &'a Env,
    ) -> Pin<Box<dyn Selection + 'a>>
    where
        Env: crate::Scope<'a>,
    {
        self.steps.into_iter().fold(
            Box::pin(selection) as Pin<Box<dyn Selection + 'a>>,
            |selection, plan| plan.evaluate_boxed(selection, env),
        )
    }

    /// The single variable every step is sorted on, if this conjunction is
    /// *structurally* a merge-eligible equi-join, else `None`.
    ///
    /// Eligible when there are at least two steps, every one is a positive
    /// attribute [`Scan`](Plan::Scan) (not optional, not a formula, concept,
    /// or constraint), and every scan reports the *same* [`SortOrder::On`]
    /// variable. Because the scans all sort on that shared variable and none
    /// is a chained probe, they are independent inputs to an intersection.
    ///
    /// This is only the structural test. Whether a merge actually reads fewer
    /// blocks than the nested loop depends on the incoming row's bindings (a
    /// pinned selective attribute favors the loop), which are not known until
    /// evaluation; that decision is made per query in
    /// [`evaluate_maybe_merge`](Self::evaluate_maybe_merge).
    fn merge_variable(&self) -> Option<String> {
        if self.steps.len() < 2 {
            return None;
        }

        let mut shared: Option<String> = None;
        let mut required = false;
        for step in &self.steps {
            let variable = match step {
                Plan::Scan(_, query) => match query.sort_order() {
                    SortOrder::On(name) => {
                        required = true;
                        name
                    }
                    SortOrder::None => return None,
                },
                // An optional field whose scan is sorted on the same
                // variable is left-merged: read once in order, an entity it
                // lacks bound Absent, instead of probed once per entity.
                Plan::OptionalScan(_, optional) => match optional.query().sort_order() {
                    SortOrder::On(name) => name,
                    SortOrder::None => return None,
                },
                // An attribute concept read with its entity free yields
                // its rows sorted on the entity, stored and derived alike
                // (see `ConceptQuery::evaluate`), so it merges on that
                // variable like a scan of the attribute would.
                Plan::Concept(_, query) => {
                    required = true;
                    MergeInput::relation_variable(query)?
                }
                _ => return None,
            };
            match &shared {
                None => shared = Some(variable),
                Some(existing) if *existing == variable => {}
                Some(_) => return None,
            }
        }
        // Something must say which entities there are: optional fields
        // alone only widen.
        if required { shared } else { None }
    }

    /// Whether the scans are balanced enough to merge, given a row's
    /// bindings, measured from the tree rather than the cost model's fixed
    /// constants.
    ///
    /// A merge reads every input range in full, in parallel; the nested loop
    /// reads one range and probes the others only at surviving entities. So
    /// when one scan is far more selective than the rest, the loop touches
    /// far fewer blocks. Each scan is resolved against `base` (binding any
    /// value the caller pinned) and its range size estimated via
    /// [`Estimate`] — the range's edge paths per scan, an advisory upper
    /// bound from the tree's per-child [`Scale`](dialog_search_tree::Scale)s
    /// with the edges counted exactly. The
    /// merge is taken only when the widest scan's estimate is within a small
    /// multiple of the narrowest; a scan a caller pinned to a value estimates
    /// far narrower and pushes the ratio past the threshold, so the query
    /// keeps the fold.
    ///
    /// A scan that cannot be turned into a selector (no bound field) or that
    /// the store cannot estimate is treated as maximally broad, which keeps
    /// the balanced (all-broad) case eligible and only ever errs toward the
    /// fold.
    async fn scans_balanced_for<Env>(&self, base: &Match, env: &Env) -> bool
    where
        Env: Provider<Estimate> + ConditionalSync,
    {
        let selectors: Vec<_> = self
            .steps
            .iter()
            .filter_map(|step| match step {
                Plan::Scan(_, query) => Some((query.as_ref(), query.resolved_selector(base))),
                _ => None,
            })
            .collect();
        // A left-merged optional field reads its whole range, where the
        // fold probes it once per entity the required scans admit. A
        // narrow optional range is cheap to read either way, so it is
        // weighed against the required scans' size, not as one of them.
        let optional: Vec<_> = self
            .steps
            .iter()
            .filter_map(|step| match step {
                Plan::OptionalScan(_, optional) => Some(optional.query().resolved_selector(base)),
                _ => None,
            })
            .collect();

        // A cardinality-one scan with its entity and attribute both bound
        // reads at most one live value. Scans that are all like that are
        // the same size by construction, so they are balanced, and walking
        // the tree to estimate each range would only confirm it -- a second
        // walk per scan, per row, on the lookups rules are made of.
        let point = selectors.iter().all(|(query, selector)| {
            matches!(query, DynamicAttributeQuery::Only(_))
                && selector.as_ref().is_ok_and(|selector| {
                    selector.entity().is_some() && selector.attribute().is_some()
                })
        });
        if !selectors.is_empty() && point {
            return true;
        }

        // A required scan pinned to a value or an entity admits a handful of
        // entities: a left merge would read every optional field's whole
        // range for them, and the fold probes each once. Decided without
        // estimating, as it was decided before optional fields could merge:
        // on a cold replica an estimate is a walk of the range's edges.
        let pinned = selectors.iter().any(|(_, selector)| {
            selector.as_ref().is_ok_and(|selector| {
                selector.entity().is_some()
                    || selector.value().is_some()
                    || selector.value_prefix().is_some()
                    || selector.value_lower().is_some()
                    || selector.value_upper().is_some()
            })
        });
        if !optional.is_empty() && pinned {
            return false;
        }

        let estimate =
            async |selector: Result<Selector<Constrained>, EvaluationError>| match selector {
                Ok(selector) => Provider::<Estimate>::execute(env, selector)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(u64::MAX),
                Err(_) => u64::MAX,
            };
        let mut min_size = u64::MAX;
        let mut max_size = 0u64;
        for (_, selector) in selectors {
            // A selector build failure or an unavailable estimate means
            // "range size unknown"; treat as maximally broad so an all-broad
            // join stays eligible and a genuinely selective one is never
            // wrongly merged on a missing estimate.
            let size = match selector {
                Ok(selector) => Provider::<Estimate>::execute(env, selector)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(u64::MAX),
                Err(_) => u64::MAX,
            };
            min_size = min_size.min(size);
            max_size = max_size.max(size);
        }
        if min_size == 0 || max_size > min_size.saturating_mul(MERGE_COST_BALANCE as u64) {
            return false;
        }
        for selector in optional {
            if estimate(selector).await > min_size.saturating_mul(MERGE_COST_BALANCE as u64) {
                return false;
            }
        }
        true
    }

    /// Evaluate a structurally merge-eligible conjunction, choosing per query
    /// between the N-way merge and the nested-loop fold by the incoming
    /// rows' selectivity.
    ///
    /// Every match in a selection shares one binding pattern, but the bound
    /// values differ, and a value decides how wide a scan's range is: so the
    /// choice is made per distinct set of resolved ranges via
    /// [`scans_balanced_for`](Self::scans_balanced_for), and consecutive rows
    /// resolving to the same ranges run under one decision (a fold run keeps
    /// its probe pipelining). An empty selection yields nothing either way.
    /// On the merge path, each scan is resolved against the incoming row
    /// (binding any variables the caller already supplied), evaluated
    /// independently, and
    /// the sorted outputs are intersected on `variable`'s encoded value via
    /// [`multi_merge_join`]; the incoming row's own bindings are folded back
    /// in, preserving the caller's context exactly as the fold would.
    fn evaluate_maybe_merge<'a, Env, M: Selection + 'a>(
        self,
        selection: M,
        env: &'a Env,
        variable: String,
    ) -> Pin<Box<dyn Selection + 'a>>
    where
        Env: crate::Scope<'a>,
    {
        // Erased at the entry; see `ConceptQuery::evaluate`.
        self.evaluate_maybe_merge_erased(Box::pin(selection), env, variable)
    }

    fn evaluate_maybe_merge_erased<'a, Env>(
        self,
        selection: Pin<Box<dyn Selection + 'a>>,
        env: &'a Env,
        variable: String,
    ) -> Pin<Box<dyn Selection + 'a>>
    where
        Env: crate::Scope<'a>,
    {
        Box::pin(crate::try_stream! {
            let mut selection = Box::pin(selection.peekable());

            // Rows share one binding pattern, so the first says whether the
            // caller already bound the join variable. If it did, every scan
            // is a lookup on that one entity: the merge would read exactly
            // what the fold reads, after resolving and estimating every scan
            // per row and running each as a stream of its own. Fold instead.
            let bound = matches!(
                selection.as_mut().peek().await,
                Some(Ok(first)) if first.value_of(&variable).is_some()
            );
            if bound {
                for await row in self.into_fold(selection, env) {
                    yield row?;
                }
                return;
            }

            let scans: Vec<MergeInput> = self
                .steps
                .iter()
                .filter_map(|step| match step {
                    Plan::Scan(_, query) => Some(MergeInput::Scan(query.clone())),
                    Plan::Concept(_, query) => Some(MergeInput::Relation(query.clone())),
                    Plan::OptionalScan(..) => None,
                    // merge_variable already proved every step merges.
                    _ => unreachable!("merge eligibility guarantees every step merges"),
                })
                .collect();
            let optional: Vec<OptionalAttributeQuery> = self
                .steps
                .iter()
                .filter_map(|step| match step {
                    Plan::OptionalScan(_, query) => Some((**query).clone()),
                    _ => None,
                })
                .collect();

            // What each scan will read given a row's bindings. Every row of
            // a selection shares one binding PATTERN, but the bound VALUES
            // can differ from row to row, and the consecutive rows that
            // resolve to one set of ranges are evaluated as a run (the
            // fold keeps its probe pipelining across the run's rows). The
            // merge-versus-fold choice is made once, on the first row:
            // how wide a scan's range is follows mostly from which
            // attribute it reads and which fields pin it, which every row
            // shares, and estimating every scan's range again for every
            // row walks the tree about as much as the scan itself would.
            let fingerprint = |row: &Match| -> Vec<Option<Selector<Constrained>>> {
                scans.iter().map(|scan| scan.resolved_selector(row)).collect()
            };
            let mut decided: Option<bool> = None;

            let mut listening = true;
            let mut hinted: HashSet<Selector<Constrained>> = HashSet::new();
            while let Some(first) = selection.next().await {
                let first = first?;
                let shape = fingerprint(&first);
                let balanced = match decided {
                    Some(balanced) => balanced,
                    None => {
                        let balanced = self.scans_balanced_for(&first, env).await;
                        decided = Some(balanced);
                        balanced
                    }
                };
                let mut run = vec![first];
                loop {
                    let same = match selection.as_mut().peek().await {
                        Some(Ok(next)) => fingerprint(next) == shape,
                        // The end, or an error the next pull surfaces.
                        _ => false,
                    };
                    if !same {
                        break;
                    }
                    let Some(next) = selection.next().await else {
                        break;
                    };
                    run.push(next?);
                }

                // An optional field merges only when the row leaves its
                // value, cause and layer free: a pinned or Absent-claimed
                // one is a check of one entity's fact, which the fold makes.
                let free = run.iter().all(|row| {
                    optional.iter().all(|optional| {
                        let query = optional.query();
                        !row.contains(query.is())
                            && !row.contains(&Term::<Any>::from(query.cause()))
                            && !row.contains(&Term::<Any>::from(query.layer()))
                    })
                });

                if !balanced || !free {
                    // A selective attribute is pinned: the nested-loop fold
                    // reads fewer blocks by driving from the narrow scan.
                    let run = stream::iter(run.into_iter().map(Ok));
                    for await row in self.clone().into_fold(run, env) {
                        yield row?;
                    }
                    continue;
                }

                for base in run {
                    // The merge will read every input range in full, so each
                    // range is committed work: hint it Likely, and a driven
                    // plan replicates it level-parallel while the merge's own
                    // streams consume — the demand reads join the in-flight
                    // hydrations or find the blocks local. Hinting stops on
                    // the env's first refusal, exactly as probe pipelining
                    // does, and a range this merge already hinted (rows that
                    // bind nothing the scans resolve against all name the
                    // same ranges) is not hinted again.
                    if listening {
                        for scan in &scans {
                            let Some(selector) = scan.resolved_selector(&base) else {
                                continue;
                            };
                            if !hinted.insert(selector.clone()) {
                                continue;
                            }
                            listening = Provider::<Preload>::execute(
                                env,
                                PreloadRequest {
                                    selector,
                                    likelihood: Likelihood::Likely,
                                },
                            )
                            .await;
                            if !listening {
                                break;
                            }
                        }
                    }

                    // Each scan seeded from the incoming row so any
                    // caller-supplied bindings resolve into the scan's
                    // constants; the shared join variable stays free and
                    // drives the merge.
                    let mut inputs: Vec<Pin<Box<dyn Selection + 'a>>> =
                        Vec::with_capacity(scans.len());
                    for scan in &scans {
                        let seeded = base.clone().seed();
                        inputs.push(match scan {
                            MergeInput::Scan(scan) => {
                                Box::pin((**scan).clone().evaluate(env, seeded))
                            }
                            MergeInput::Relation(query) => {
                                Box::pin(query.clone().evaluate(seeded, env))
                            }
                        });
                    }

                    let variable = variable.clone();
                    // The key reads a scanned entity in place: ordering rows
                    // by an encoding of a materialized entity built one, and
                    // hashed its URI, for every row of every input.
                    let key = Arc::new(move |m: &Match| m.join_key(&variable));
                    let key = {
                        let key = key.clone();
                        move |m: &Match| key(m)
                    };

                    let merged: Pin<Box<dyn Selection + 'a>> = if optional.is_empty() {
                        Box::pin(multi_merge_join(inputs, key))
                    } else {
                        let sides = optional
                            .iter()
                            .map(|optional| {
                                let query = optional.query();
                                // The terms the probe's fallback binds
                                // Absent, the value first (see
                                // `left_merge_join`).
                                let absent = vec![
                                    query.is().clone(),
                                    Term::<Any>::from(query.cause()),
                                    Term::<Any>::from(query.layer()),
                                ];
                                let side: Pin<Box<dyn Selection + 'a>> = Box::pin(
                                    Application::evaluate(query.clone(), base.clone().seed(), env),
                                );
                                (side, absent)
                            })
                            .collect();
                        let left_key = key.clone();
                        Box::pin(left_merge_join(
                            multi_merge_join(inputs, key),
                            sides,
                            left_key,
                        ))
                    };
                    for await row in merged {
                        let row = row?;
                        // Fold the joined bindings back onto the incoming row
                        // so the caller's context (and provenance) is
                        // preserved.
                        if let Some(merged) = base.clone().combine(&row) {
                            yield merged;
                        }
                    }
                }
            }
        })
    }
}

/// One input of an N-way merge: a stored attribute scan, or an attribute
/// concept read whose rows, stored and derived, arrive sorted on the
/// entity.
#[derive(Debug, Clone)]
enum MergeInput {
    Scan(Box<DynamicAttributeQuery>),
    Relation(ConceptQuery),
}

impl MergeInput {
    /// The variable an attribute concept read is sorted on: its entity
    /// term's name, when the read is over an attribute concept with a
    /// named entity variable. A set-widened read is not an input: it
    /// yields a row for an entity nothing matched, so it extends the
    /// other inputs' rows rather than intersecting with them.
    fn relation_variable(query: &ConceptQuery) -> Option<String> {
        query.predicate.attribute_field()?;
        if query.widens() {
            return None;
        }
        query
            .terms
            .get("this")
            .and_then(|term| term.name())
            .map(str::to_string)
    }

    /// What this input reads given a row's bindings, for a stored scan;
    /// a relation resolves nothing ahead of evaluation.
    fn resolved_selector(&self, row: &Match) -> Option<Selector<Constrained>> {
        match self {
            MergeInput::Scan(scan) => scan.resolved_selector(row).ok(),
            MergeInput::Relation(_) => None,
        }
    }
}

/// End-to-end regression tests for the optionality bug family: each
/// test asserts the *agreed* observable semantics of set-widening
/// through a planned conjunction. Tests that pin a known-broken
/// behavior are `#[ignore]`d with the Beads issue that fixes them;
/// remove the attribute when the fix lands.
///
/// The agreed semantics: body premises are *filters*,
/// a slot that demands a present value excludes rows where the
/// variable is Absent (occurrence-typing narrowing); `Coalesce` is
/// the explicit opt-in for treating a missing value as a default;
/// absence is only ever read relative to a known entity.
#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::attribute::Relation;
    use crate::attribute::query::AttributeQuery;
    use crate::formula::math::Sum;
    use crate::formula::string::Uppercase;
    use crate::optional::OptionalAttributeQuery;
    use crate::planner::{Plan, Planner};
    use crate::query::Application;
    use crate::selection::Match;
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::the;
    use crate::types::Any;
    use crate::{
        AttributeDescriptor, Binding, Cardinality, ConceptDescriptor, ConceptFieldDescriptor,
        ConceptQuery, Environment, EvaluationError, Formula, Negation, Parameters, Premise,
        Proposition, Term, Type, Value,
    };
    use dialog_corpus::Entity;
    use dialog_peer::helpers::{test_repo, test_session_with_peer};
    use dialog_peer::{Peer, Session};
    use dialog_repository::{Branch, Transaction};
    use dialog_storage::provider::storage::VolatileSpace;
    use futures_util::{TryStreamExt, stream};

    /// A two-attribute conjunction over a shared entity is structurally merge
    /// eligible, and the tree-derived balance check picks merge only when
    /// neither scan is selective. This pins the regression the merge
    /// introduced: with a value pinned on one scan, `scans_balanced_for` must
    /// report imbalance (from the real range estimates) so the query falls
    /// back to the nested-loop fold instead of scanning every range in full.
    #[dialog_common::test]
    async fn it_prefers_the_fold_when_one_scan_is_selective() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // Seed a spread of entities, each with a name and a role, so the two
        // attribute ranges are broad and comparably sized, while any single
        // value ("name-7") is selective.
        // Deterministic entities, so the tree's shape (and with it the
        // estimates the guard compares) is the same on every run.
        let mut tx = branch.transaction();
        for i in 0..2000 {
            let entity: Entity = format!("id:thing-{i}").parse()?;
            tx = tx
                .assert(
                    the!("thing/name")
                        .of(entity.clone())
                        .is(format!("name-{i}")),
                )
                .assert(the!("thing/role").of(entity).is(format!("role-{}", i % 4)));
        }
        tx.commit().publish().perform(&operator).await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let env = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let name_scan = AttributeQuery::new(
            Term::from(the!("thing/name")),
            Term::<Entity>::var("this"),
            Term::<Any>::var("name"),
            Term::var("c1"),
            Some(Cardinality::One),
        );
        let role_scan = AttributeQuery::new(
            Term::from(the!("thing/role")),
            Term::<Entity>::var("this"),
            Term::<Any>::var("role"),
            Term::var("c2"),
            Some(Cardinality::One),
        );

        let plan = Planner::from(vec![
            Premise::Assert(Proposition::Attribute(Box::new(name_scan))),
            Premise::Assert(Proposition::Attribute(Box::new(role_scan))),
        ])
        .plan(&Environment::new())?;

        // Both scans sort on the shared entity, so the conjunction is
        // structurally eligible.
        assert_eq!(plan.merge_variable().as_deref(), Some("this"));

        // Nothing pinned: both ranges are the full attribute range,
        // comparably sized, so the estimates are balanced and the merge is
        // taken.
        assert!(
            plan.scans_balanced_for(&Match::new(), &env).await,
            "with no value pinned the ranges are comparable and should merge"
        );

        // Pin the name value, which is unique per entity: the name scan
        // collapses to a single-entity band while the role scan stays a full
        // range, so the tree estimates are decisively imbalanced and the
        // fold is preferred. (A near-unique value makes the ratio
        // unambiguous, unlike a low-cardinality one near the threshold.)
        let mut pinned = Match::new();
        pinned.bind(
            &Term::<Any>::var("name"),
            Value::String("name-7".to_string()),
        )?;
        assert!(
            !plan.scans_balanced_for(&pinned, &env).await,
            "with the name value pinned the name scan is selective and should fold"
        );

        Ok(())
    }

    /// The merge-versus-fold choice follows the rows' bound VALUES, not
    /// just the first row: a run of rows pinning a broad value merges, a
    /// run pinning a rare one folds, and either way every row's rows come
    /// out exactly as the fold alone produces them.
    #[dialog_common::test]
    async fn it_decides_merge_or_fold_per_run_of_bindings() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // Names unique per entity; half the roles share one broad value,
        // the other half are unique to their entity (a unique value makes
        // the ratio unambiguous; a merely rare one sits near the guard once
        // the estimate's edge inflation is counted).
        let mut tx = branch.transaction();
        for i in 0..1200 {
            let entity: Entity = format!("id:thing-{i}").parse()?;
            let role = if i % 2 == 0 {
                "even".to_string()
            } else {
                format!("solo-{i}")
            };
            tx = tx
                .assert(
                    the!("thing/name")
                        .of(entity.clone())
                        .is(format!("name-{i}")),
                )
                .assert(the!("thing/role").of(entity).is(role));
        }
        tx.commit().publish().perform(&operator).await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let env = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let plan = Planner::from(vec![
            Premise::Assert(Proposition::Attribute(Box::new(AttributeQuery::new(
                Term::from(the!("thing/name")),
                Term::<Entity>::var("this"),
                Term::<Any>::var("name"),
                Term::var("c1"),
                Some(Cardinality::One),
            )))),
            Premise::Assert(Proposition::Attribute(Box::new(AttributeQuery::new(
                Term::from(the!("thing/role")),
                Term::<Entity>::var("this"),
                Term::<Any>::var("role"),
                Term::var("c2"),
                Some(Cardinality::One),
            )))),
        ])
        .plan(&Environment::new())?;

        let pinned = |role: &str| -> anyhow::Result<Match> {
            let mut row = Match::new();
            row.bind(&Term::<Any>::var("role"), Value::String(role.to_string()))?;
            Ok(row)
        };
        // A broad value merges, a unique one folds: the runs below are
        // decided differently.
        assert!(plan.scans_balanced_for(&pinned("even")?, &env).await);
        assert!(!plan.scans_balanced_for(&pinned("solo-7")?, &env).await);

        let rows = vec![
            pinned("even")?,
            pinned("even")?,
            pinned("solo-7")?,
            pinned("even")?,
        ];
        let normalize = |rows: Vec<Match>| {
            let mut keyed: Vec<(String, String, String)> = rows
                .into_iter()
                .map(|row| {
                    (
                        format!("{:?}", row.value_of("this")),
                        format!("{:?}", row.value_of("name")),
                        format!("{:?}", row.value_of("role")),
                    )
                })
                .collect();
            keyed.sort();
            keyed
        };
        let chosen = plan
            .clone()
            .evaluate(stream::iter(rows.clone().into_iter().map(Ok)), &env)
            .try_collect::<Vec<_>>()
            .await?;
        let oracle = plan
            .into_fold(stream::iter(rows.into_iter().map(Ok)), &env)
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(chosen.len(), 600 * 3 + 1, "every row's matches");
        assert_eq!(normalize(chosen), normalize(oracle));
        Ok(())
    }

    /// Rows that bind the join variable read one entity per scan, so the
    /// conjunction evaluates them as the fold does, row by row, and yields
    /// each bound entity's own values and nothing else.
    #[dialog_common::test]
    async fn it_folds_rows_that_bind_the_join_variable() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let mut tx = branch.transaction();
        for i in 0..50 {
            let entity: Entity = format!("id:thing-{i}").parse()?;
            tx = tx
                .assert(
                    the!("thing/name")
                        .of(entity.clone())
                        .is(format!("name-{i}")),
                )
                .assert(the!("thing/role").of(entity).is(format!("role-{i}")));
        }
        tx.commit().publish().perform(&operator).await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let env = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let plan = Planner::from(vec![
            Premise::Assert(Proposition::Attribute(Box::new(AttributeQuery::new(
                Term::from(the!("thing/name")),
                Term::<Entity>::var("this"),
                Term::<Any>::var("name"),
                Term::var("c1"),
                Some(Cardinality::One),
            )))),
            Premise::Assert(Proposition::Attribute(Box::new(AttributeQuery::new(
                Term::from(the!("thing/role")),
                Term::<Entity>::var("this"),
                Term::<Any>::var("role"),
                Term::var("c2"),
                Some(Cardinality::One),
            )))),
        ])
        .plan(&Environment::new())?;
        assert_eq!(plan.merge_variable().as_deref(), Some("this"));

        let bound = |i: usize| -> anyhow::Result<Match> {
            let mut row = Match::new();
            row.bind(
                &Term::<Any>::var("this"),
                Value::Entity(format!("id:thing-{i}").parse()?),
            )?;
            Ok(row)
        };
        let rows = vec![bound(3)?, bound(17)?, bound(3)?, bound(42)?];
        let chosen = plan
            .clone()
            .evaluate(stream::iter(rows.clone().into_iter().map(Ok)), &env)
            .try_collect::<Vec<_>>()
            .await?;
        let oracle = plan
            .into_fold(stream::iter(rows.into_iter().map(Ok)), &env)
            .try_collect::<Vec<_>>()
            .await?;

        let values = |rows: &[Match]| -> Vec<(String, String)> {
            rows.iter()
                .map(|row| {
                    (
                        format!("{:?}", row.value_of("name")),
                        format!("{:?}", row.value_of("role")),
                    )
                })
                .collect()
        };
        let expected: Vec<(String, String)> = [3, 17, 3, 42]
            .iter()
            .map(|i| {
                (
                    format!("{:?}", Some(&Value::String(format!("name-{i}")))),
                    format!("{:?}", Some(&Value::String(format!("role-{i}")))),
                )
            })
            .collect();
        assert_eq!(
            values(&chosen),
            expected,
            "one row per bound entity, in order"
        );
        assert_eq!(chosen, oracle);
        Ok(())
    }

    /// Coalesce must take the *source* when the lookup finds a value
    /// and the fallback only when it does not. The coalesce's source
    /// slot is a hard requirement, so the planner orders it after
    /// the left-join that binds `?nickname`.
    #[dialog_common::test]
    async fn it_takes_present_source_over_coalesce_fallback() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(
                the!("person/nickname")
                    .of(alice.clone())
                    .is("Ali".to_string()),
            )
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let name_scan = AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("person"),
            Term::<String>::var("name").into(),
            Term::var("c1"),
            Some(Cardinality::One),
        );
        let optional_nickname = OptionalAttributeQuery::new(
            Term::from(the!("person/nickname")),
            Term::<Entity>::var("person"),
            Term::<String>::var("nickname").into(),
            Term::blank(),
            Some(Cardinality::One),
        );
        let coalesce = Term::<Option<String>>::var("nickname")
            .unwrap_or("Anon".to_string())
            .is(Term::<String>::var("display"));

        let plan = Planner::from(vec![
            Premise::Assert(Proposition::Attribute(Box::new(name_scan))),
            optional_nickname.into(),
            coalesce,
        ])
        .plan(&Environment::new())?;
        let results: Vec<Match> = plan
            .evaluate(Match::new().seed(), &source)
            .try_collect()
            .await?;

        assert_eq!(results.len(), 2, "both people produce a row");
        let mut found_alice = false;
        let mut found_bob = false;
        for row in &results {
            let name = row.lookup(&Term::var("name"))?.content()?;
            let display = row.lookup(&Term::var("display"))?.content()?;
            match (&name, &display) {
                (Value::String(n), Value::String(d)) if n == "Alice" => {
                    assert!(
                        d == "Ali",
                        "Present nickname must win over the fallback, got {d:?}"
                    );
                    found_alice = true;
                }
                (Value::String(n), Value::String(d)) if n == "Bob" => {
                    assert!(
                        d == "Anon",
                        "missing nickname takes the fallback, got {d:?}"
                    );
                    found_bob = true;
                }
                other => panic!("unexpected (name, display): {other:?}"),
            }
        }
        assert!(found_alice && found_bob);
        Ok(())
    }

    /// An `Absent` binding flowing into a negated premise matches
    /// nothing: the negation's inner query is filtered for that row
    /// (a scalar slot demands a present value), so the row *passes*
    /// the negation. An entity with no nickname is not treated as if
    /// it had every banned one.
    #[dialog_common::test]
    async fn it_negates_absent_as_matching_nothing() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let club = Entity::new()?;
        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(
                the!("person/nickname")
                    .of(alice.clone())
                    .is("Ali".to_string()),
            )
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .assert(the!("club/banned").of(club.clone()).is("Ali".to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let name_scan = AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("person"),
            Term::<String>::var("name").into(),
            Term::var("c1"),
            Some(Cardinality::One),
        );
        let optional_nickname = OptionalAttributeQuery::new(
            Term::from(the!("person/nickname")),
            Term::<Entity>::var("person"),
            Term::<String>::var("nickname").into(),
            Term::blank(),
            Some(Cardinality::One),
        );
        let banned_scan = AttributeQuery::new(
            Term::from(the!("club/banned")),
            Term::blank(),
            Term::<String>::var("nickname").into(),
            Term::blank(),
            Some(Cardinality::One),
        );

        let plan = Planner::from(vec![
            Premise::Assert(Proposition::Attribute(Box::new(name_scan))),
            optional_nickname.into(),
            Premise::Unless(Negation::not(Proposition::Attribute(Box::new(banned_scan)))),
        ])
        .plan(&Environment::new())?;
        let results: Vec<Match> = plan
            .evaluate(Match::new().seed(), &source)
            .try_collect()
            .await?;

        assert_eq!(
            results.len(),
            1,
            "Alice (banned nickname) is filtered; Bob (no nickname) passes"
        );
        let name = results[0].lookup(&Term::var("name"))?.content()?;
        assert!(
            matches!(&name, Value::String(n) if n == "Bob"),
            "the surviving row is Bob, got {name:?}"
        );
        Ok(())
    }

    /// A concept's optional field crossing into a premise that
    /// demands a present value *filters* the rows where it is
    /// Absent: it must not abort the stream. The concept boundary
    /// delivers Bob's `Absent`, and the formula (a scalar context)
    /// excludes his row, the same filter-by-default semantics that
    /// the within-rule narrowing produces.
    #[dialog_common::test]
    async fn it_filters_concept_rows_with_absent_field_from_required_formula() -> anyhow::Result<()>
    {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(
                the!("person/nickname")
                    .of(alice.clone())
                    .is("Ali".to_string()),
            )
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let concept = ConceptDescriptor::try_from(vec![
            (
                "name".to_string(),
                ConceptFieldDescriptor::required(AttributeDescriptor::new(
                    the!("person/name"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                )),
            ),
            (
                "nickname".to_string(),
                ConceptFieldDescriptor::optional(AttributeDescriptor::new(
                    the!("person/nickname"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                )),
            ),
        ])?;
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("person"));
        terms.insert("name".to_string(), Term::var("name"));
        terms.insert("nickname".to_string(), Term::var("nickname"));
        let concept_premise = Premise::Assert(Proposition::Concept(ConceptQuery {
            terms,
            predicate: concept,
        }));

        let mut formula_terms = Parameters::new();
        formula_terms.insert("of".to_string(), Term::var("nickname"));
        formula_terms.insert("is".to_string(), Term::var("upper"));
        let uppercase = Premise::Assert(Proposition::Formula(
            Uppercase::apply(formula_terms)?.into(),
        ));

        let plan = Planner::from(vec![concept_premise, uppercase]).plan(&Environment::new())?;
        let results: Vec<Match> = plan
            .evaluate(Match::new().seed(), &source)
            .try_collect()
            .await?;

        assert_eq!(
            results.len(),
            1,
            "Bob (Absent nickname) is filtered by the formula's required input; the stream must not error"
        );
        let name = results[0].lookup(&Term::var("name"))?.content()?;
        let upper = results[0].lookup(&Term::var("upper"))?.content()?;
        assert!(
            matches!(&name, Value::String(n) if n == "Alice"),
            "the surviving row is Alice, got {name:?}"
        );
        assert!(
            matches!(&upper, Value::String(u) if u == "ALI"),
            "the formula computed over the Present value, got {upper:?}"
        );
        Ok(())
    }

    /// Formulas are polymorphic over the numeric types: one
    /// math/sum premise computes over signed-integer facts (its
    /// scheme instantiates per row), and a row whose inputs cannot
    /// share a single type is a non-match — no promotion, no error.
    #[dialog_common::test]
    async fn it_sums_signed_integers_and_mixed_rows_as_floats() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        branch
            .transaction()
            .assert(the!("game/score").of(alice.clone()).is(-5i64))
            .assert(the!("game/bonus").of(alice.clone()).is(-3i64))
            .assert(the!("game/score").of(bob.clone()).is(2i64))
            // Bob's bonus is a float: his row computes as floats.
            .assert(the!("game/bonus").of(bob.clone()).is(0.5f64))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let score_scan = AttributeQuery::new(
            Term::from(the!("game/score")),
            Term::<Entity>::var("player"),
            Term::var("score"),
            Term::var("c1"),
            Some(Cardinality::One),
        );
        let bonus_scan = AttributeQuery::new(
            Term::from(the!("game/bonus")),
            Term::<Entity>::var("player"),
            Term::var("bonus"),
            Term::var("c2"),
            Some(Cardinality::One),
        );
        let mut sum_terms = Parameters::new();
        sum_terms.insert("of".to_string(), Term::var("score"));
        sum_terms.insert("with".to_string(), Term::var("bonus"));
        sum_terms.insert("is".to_string(), Term::var("total"));
        let sum = Premise::Assert(Proposition::Formula(Sum::apply(sum_terms)?.into()));

        let plan = Planner::from(vec![
            Premise::Assert(Proposition::Attribute(Box::new(score_scan))),
            Premise::Assert(Proposition::Attribute(Box::new(bonus_scan))),
            sum,
        ])
        .plan(&Environment::new())?;
        let results: Vec<Match> = plan
            .evaluate(Match::new().seed(), &source)
            .try_collect()
            .await?;

        assert_eq!(
            results.len(),
            2,
            "Alice computes within one type; Bob's mixed row as floats"
        );
        let total = |player: &Entity| -> anyhow::Result<Value> {
            let row = results
                .iter()
                .find(|row| {
                    row.lookup(&Term::var("player"))
                        .ok()
                        .and_then(|binding| binding.content().ok())
                        == Some(Value::Entity(player.clone()))
                })
                .ok_or_else(|| anyhow::anyhow!("no row for {player:?}"))?;
            Ok(row.lookup(&Term::var("total"))?.content()?)
        };
        assert_eq!(total(&bob)?, Value::Float(2.5), "2 + 0.5 as floats");
        assert_eq!(
            total(&alice)?,
            Value::SignedInt(-8),
            "signed arithmetic, previously inexpressible, just works"
        );
        Ok(())
    }

    /// Polymorphic literals: an integer literal adapts losslessly to
    /// each row's instantiation, so one premise with a literal works
    /// over signed data — the literal follows the data, never the
    /// other way around.
    #[dialog_common::test]
    async fn it_adapts_integer_literals_to_the_rows_type() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(the!("game/score").of(alice.clone()).is(-5i64))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let score_scan = AttributeQuery::new(
            Term::from(the!("game/score")),
            Term::<Entity>::var("player"),
            Term::var("score"),
            Term::var("c1"),
            Some(Cardinality::One),
        );
        let mut sum_terms = Parameters::new();
        sum_terms.insert("of".to_string(), Term::var("score"));
        // An unsigned literal over signed data: adapts to -5 + 1.
        sum_terms.insert(
            "with".to_string(),
            Term::<Any>::Constant(Value::UnsignedInt(1)),
        );
        sum_terms.insert("is".to_string(), Term::var("total"));
        let sum = Premise::Assert(Proposition::Formula(Sum::apply(sum_terms)?.into()));

        let plan = Planner::from(vec![
            Premise::Assert(Proposition::Attribute(Box::new(score_scan))),
            sum,
        ])
        .plan(&Environment::new())?;
        let results: Vec<Match> = plan
            .evaluate(Match::new().seed(), &source)
            .try_collect()
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].lookup(&Term::var("total"))?.content()?,
            Value::SignedInt(-4),
            "the literal instantiated to the row's signed type"
        );
        Ok(())
    }

    /// A type predicate filters heterogeneous data and narrows the
    /// feeding scan: `?tag.number()` keeps the numeric facts and the
    /// narrowing stamps the scan so non-numeric facts never reach
    /// the predicate.
    #[dialog_common::test]
    async fn it_filters_rows_through_type_predicates() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(the!("misc/tag").of(alice.clone()).is("blue".to_string()))
            .assert(the!("misc/tag").of(alice.clone()).is(7u32))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let scan = AttributeQuery::new(
            Term::from(the!("misc/tag")),
            Term::<Entity>::var("this"),
            Term::var("tag"),
            Term::var("c1"),
            Some(Cardinality::Many),
        );
        let plan = Planner::from(vec![
            Premise::Assert(Proposition::Attribute(Box::new(scan))),
            Term::<Any>::var("tag").number(),
        ])
        .plan(&Environment::new())?;
        let results: Vec<Match> = plan
            .evaluate(Match::new().seed(), &source)
            .try_collect()
            .await?;

        assert_eq!(results.len(), 1, "only the numeric fact survives");
        assert_eq!(
            results[0].lookup(&Term::var("tag"))?.content()?,
            Value::UnsignedInt(7)
        );
        Ok(())
    }

    /// Characterization of the agreed filter semantics, passing
    /// today: a formula slot demanding a present value narrows a
    /// set-widened attribute variable rule-wide (occurrence typing),
    /// so entities lacking the optional fact are excluded: no
    /// Absent fallback row is emitted and no Absent reaches the
    /// formula. `Coalesce` remains the explicit opt-in for a
    /// default instead of exclusion.
    #[dialog_common::test]
    async fn it_narrows_optional_formula_input_to_a_filter() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(the!("person/age").of(alice.clone()).is(25u32))
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let name_scan = AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("person"),
            Term::<String>::var("name").into(),
            Term::var("c1"),
            Some(Cardinality::One),
        );
        let optional_age = OptionalAttributeQuery::new(
            Term::from(the!("person/age")),
            Term::<Entity>::var("person"),
            Term::<u32>::var("age").into(),
            Term::blank(),
            Some(Cardinality::One),
        );
        let mut sum_terms = Parameters::new();
        sum_terms.insert("of".to_string(), Term::var("age"));
        sum_terms.insert("with".to_string(), Term::<Any>::constant(1u32));
        sum_terms.insert("is".to_string(), Term::var("total"));
        let sum = Premise::Assert(Proposition::Formula(Sum::apply(sum_terms)?.into()));

        let plan = Planner::from(vec![
            Premise::Assert(Proposition::Attribute(Box::new(name_scan))),
            optional_age.into(),
            sum,
        ])
        .plan(&Environment::new())?;
        let results: Vec<Match> = plan
            .evaluate(Match::new().seed(), &source)
            .try_collect()
            .await?;

        assert_eq!(
            results.len(),
            1,
            "Bob lacks the optional age the formula requires; only Alice survives"
        );
        let name = results[0].lookup(&Term::var("name"))?.content()?;
        let total = results[0].lookup(&Term::var("total"))?.content()?;
        assert!(
            matches!(&name, Value::String(n) if n == "Alice"),
            "the surviving row is Alice, got {name:?}"
        );
        assert_eq!(
            u32::try_from(total.clone()).ok(),
            Some(26),
            "the formula computed over the Present value"
        );
        Ok(())
    }

    /// A required scan of `the` over `?e`, its value in `value` and its
    /// cause in `cause`.
    fn required(the: Term<Relation>, value: &str, cause: &str) -> Premise {
        Premise::Assert(Proposition::Attribute(Box::new(AttributeQuery::new(
            the,
            Term::<Entity>::var("e"),
            Term::<Any>::var(value),
            Term::var(cause),
            Some(Cardinality::Many),
        ))))
    }

    /// An optional field `the` of `?e`, its value in `value` and its cause
    /// in `cause`, of the given cardinality.
    fn optional(
        the: Term<Relation>,
        value: &str,
        cause: &str,
        cardinality: Cardinality,
    ) -> OptionalAttributeQuery {
        OptionalAttributeQuery::new(
            the,
            Term::<Entity>::var("e"),
            Term::<Any>::var(value),
            Term::var(cause),
            Some(cardinality),
        )
    }

    /// How many filler entities, each with a `person/name` and nothing
    /// else, the fixtures below add: enough that the name range's
    /// estimate is not zero, so the balance check admits the merge.
    const FILLER: usize = 400;

    type TestPeer = Peer<VolatileSpace, Session>;

    /// A branch holding `FILLER` named entities plus what `facts` adds.
    async fn fixture<F>(facts: F) -> anyhow::Result<(TestPeer, Branch)>
    where
        F: for<'b> FnOnce(Transaction<&'b Branch>) -> Transaction<&'b Branch>,
    {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let mut tx = branch.transaction();
        for i in 0..FILLER {
            let entity: Entity = format!("id:filler-{i:04}").parse()?;
            tx = tx.assert(the!("person/name").of(entity).is(format!("filler-{i}")));
        }
        facts(tx).commit().publish().perform(&operator).await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        Ok((operator, branch))
    }

    /// What evaluating one selection through the chosen path and through
    /// the fold gave, with the plan's optional steps.
    struct Outcomes {
        optional: Vec<OptionalAttributeQuery>,
        merge: Result<Vec<Match>, EvaluationError>,
        fold: Result<Vec<Match>, EvaluationError>,
    }

    /// Plan `premises`, check `optionals` optional steps survive planning
    /// and that `rows` take the left merge (merge eligible on `?e`,
    /// balanced, and the `free` check holds since `rows` bind none of the
    /// optional fields' terms), then evaluate `rows` through the chosen path
    /// and through the fold.
    async fn merge_and_fold<'a, Env>(
        premises: Vec<Premise>,
        optionals: usize,
        rows: Vec<Match>,
        env: &'a Env,
    ) -> anyhow::Result<Outcomes>
    where
        Env: crate::Scope<'a>,
    {
        let plan = Planner::from(premises).plan(&Environment::new())?;
        let optional: Vec<OptionalAttributeQuery> = plan
            .steps
            .iter()
            .filter_map(|step| match step {
                Plan::OptionalScan(_, query) => Some((**query).clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            optional.len(),
            optionals,
            "expected {optionals} optional steps to survive planning, got {}",
            optional.len()
        );
        assert_eq!(
            plan.merge_variable().as_deref(),
            Some("e"),
            "expected the conjunction to be left-merge eligible on ?e"
        );
        for row in &rows {
            assert!(
                plan.scans_balanced_for(row, env).await,
                "expected the scans to be balanced so the left merge is taken"
            );
        }
        let merge = plan
            .clone()
            .evaluate(stream::iter(rows.clone().into_iter().map(Ok)), env)
            .try_collect::<Vec<_>>()
            .await;
        let fold = plan
            .into_fold(stream::iter(rows.into_iter().map(Ok)), env)
            .try_collect::<Vec<_>>()
            .await;
        Ok(Outcomes {
            optional,
            merge,
            fold,
        })
    }

    /// Each row's `names`, `Absent` and unbound told apart, in row order.
    fn shown(rows: &[Match], names: &[&str]) -> Vec<Vec<String>> {
        rows.iter()
            .map(|row| {
                names
                    .iter()
                    .map(|name| match row.get(name) {
                        Some(Binding::Present(value)) => format!("{value:?}"),
                        Some(Binding::Absent) => "Absent".to_string(),
                        None => "unbound".to_string(),
                    })
                    .collect()
            })
            .collect()
    }

    /// [`shown`], then each name's standing and each optional field's
    /// claim, so a difference in citations shows too.
    fn detailed(
        rows: &[Match],
        names: &[&str],
        optional: &[OptionalAttributeQuery],
    ) -> Vec<Vec<String>> {
        shown(rows, names)
            .into_iter()
            .zip(rows)
            .map(|(mut shown, row)| {
                for name in names {
                    shown.push(format!("{:?}", row.standing_of(name)));
                }
                for query in optional {
                    shown.push(format!("{:?}", Application::realize(query, row.clone())));
                }
                shown
            })
            .collect()
    }

    /// The rows of `shown` whose first column is not a filler entity.
    fn named(rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
        rows.into_iter()
            .filter(|row| !row[0].contains("filler"))
            .collect()
    }

    /// Assert the merge path gave exactly the fold's rows, in the fold's
    /// order, with the same standings and claims.
    fn assert_same(outcomes: &Outcomes, names: &[&str]) {
        let fold = match &outcomes.fold {
            Ok(rows) => detailed(rows, names, &outcomes.optional),
            Err(error) => panic!("the fold failed: {error:?}"),
        };
        match &outcomes.merge {
            Ok(rows) => assert_eq!(
                detailed(rows, names, &outcomes.optional),
                fold,
                "expected the left merge to yield the fold's rows"
            ),
            Err(error) => panic!(
                "expected the left merge to yield the fold's {} rows, \
                 got an error instead: {error:?}",
                fold.len()
            ),
        }
    }

    /// Two optional fields share one value variable, `maybe (person/nick
    /// ?e ?x)` and `maybe (person/alias ?e ?x)`, beside a required
    /// `(person/name ?e ?n)`. Alice has a nick and no alias, Bob neither.
    ///
    /// The fold (what `main` evaluates) drops Alice, whose alias probe has
    /// `?x` pinned to her nick and finds no such alias (a mismatch, not
    /// absence), and yields Bob with `?x` Absent. The left merge binds
    /// Alice's `?x` Absent where her nick already bound it Present, and the
    /// whole query fails with `EvaluationError::Assignment`.
    #[dialog_common::test]
    async fn it_left_merges_optional_fields_sharing_a_value_as_the_fold_does() -> anyhow::Result<()>
    {
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        let (operator, branch) = fixture(|tx| {
            tx.assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(the!("person/nick").of(alice.clone()).is("Al".to_string()))
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
        })
        .await?;
        let env = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let outcomes = merge_and_fold(
            vec![
                required(Term::from(the!("person/name")), "n", "c1"),
                optional(
                    Term::from(the!("person/nick")),
                    "x",
                    "c2",
                    Cardinality::Many,
                )
                .into(),
                optional(
                    Term::from(the!("person/alias")),
                    "x",
                    "c3",
                    Cardinality::Many,
                )
                .into(),
            ],
            2,
            vec![Match::new()],
            &env,
        )
        .await?;

        let names = ["e", "n", "x"];
        let fold = match &outcomes.fold {
            Ok(rows) => named(shown(rows, &names)),
            Err(error) => panic!("the fold failed: {error:?}"),
        };
        assert_eq!(
            fold,
            vec![vec![
                format!("{:?}", Value::Entity(bob.clone())),
                format!("{:?}", Value::String("Bob".into())),
                "Absent".to_string(),
            ]],
            "the fold drops Alice (alias mismatch) and yields Bob with ?x Absent"
        );
        if let Err(error) = &outcomes.merge {
            assert!(
                !matches!(error, EvaluationError::Assignment { .. }),
                "expected the left merge to yield the fold's rows {fold:?}, \
                 got EvaluationError::Assignment instead: {error:?}"
            );
        }
        assert_same(&outcomes, &names);
        Ok(())
    }

    /// An optional field whose layer is a named variable, `maybe
    /// (person/nick ?e ?k in: ?where)`, beside a required `(person/name ?e
    /// ?n)`. Bob has no nick.
    ///
    /// The fold (what `main` evaluates) binds Bob's `?k`, its cause and
    /// `?where` Absent. The left merge binds only `?k` and the cause
    /// Absent and leaves `?where` unbound.
    #[dialog_common::test]
    async fn it_binds_an_absent_fields_layer_absent_under_the_left_merge() -> anyhow::Result<()> {
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        let (operator, branch) = fixture(|tx| {
            tx.assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(the!("person/nick").of(alice.clone()).is("Al".to_string()))
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
        })
        .await?;
        let env = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let outcomes = merge_and_fold(
            vec![
                required(Term::from(the!("person/name")), "n", "c1"),
                optional(
                    Term::from(the!("person/nick")),
                    "k",
                    "c2",
                    Cardinality::Many,
                )
                .with_layer(Term::var("where"))
                .into(),
            ],
            1,
            vec![Match::new()],
            &env,
        )
        .await?;

        let names = ["e", "n", "k", "where"];
        let bob_row = |rows: &[Match]| -> Vec<String> {
            named(shown(rows, &names))
                .into_iter()
                .find(|row| row[0].contains("id:bob"))
                .expect("a row for Bob")
        };
        let fold = bob_row(outcomes.fold.as_ref().expect("the fold succeeds"));
        assert_eq!(
            fold[3], "Absent",
            "the fold binds Bob's ?where Absent: {fold:?}"
        );
        let merge = bob_row(outcomes.merge.as_ref().expect("the merge succeeds"));
        assert_eq!(
            merge, fold,
            "expected Bob's left-merged row to bind ?where as the fold does (Absent)"
        );
        assert_same(&outcomes, &names);
        Ok(())
    }

    /// A cardinality-many required field gives several left rows per
    /// entity (Alice has two names), and a cardinality-many optional field
    /// several side rows (Alice two nicks, Bob one, Carol none). The left
    /// merge yields the fold's rows, in its order, with its standings and
    /// claims.
    #[dialog_common::test]
    async fn it_left_merges_a_many_valued_required_field_as_the_fold_does() -> anyhow::Result<()> {
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        let carol: Entity = "id:carol".parse()?;
        let (operator, branch) = fixture(|tx| {
            tx.assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alicia".to_string()),
            )
            .assert(the!("person/nick").of(alice.clone()).is("Al".to_string()))
            .assert(the!("person/nick").of(alice.clone()).is("Ally".to_string()))
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .assert(the!("person/nick").of(bob.clone()).is("Bobby".to_string()))
            .assert(
                the!("person/name")
                    .of(carol.clone())
                    .is("Carol".to_string()),
            )
        })
        .await?;
        let env = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let outcomes = merge_and_fold(
            vec![
                required(Term::from(the!("person/name")), "n", "c1"),
                optional(
                    Term::from(the!("person/nick")),
                    "k",
                    "c2",
                    Cardinality::Many,
                )
                .into(),
            ],
            1,
            vec![Match::new()],
            &env,
        )
        .await?;
        let names = ["e", "n", "k"];
        assert_eq!(
            named(shown(
                outcomes.fold.as_ref().expect("the fold succeeds"),
                &names
            ))
            .len(),
            2 * 2 + 1 + 1,
            "Alice's two names by two nicks, Bob's one, Carol's Absent"
        );
        assert_same(&outcomes, &names);
        Ok(())
    }

    /// A cardinality-one optional field elects one value per entity: Alice
    /// has two nicks, Bob one, Carol none. The left merge elects as the
    /// per-entity probe does.
    #[dialog_common::test]
    async fn it_left_merges_a_cardinality_one_optional_field_as_the_fold_does() -> anyhow::Result<()>
    {
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        let carol: Entity = "id:carol".parse()?;
        let (operator, branch) = fixture(|tx| {
            tx.assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(the!("person/nick").of(alice.clone()).is("Al".to_string()))
            .assert(the!("person/nick").of(alice.clone()).is("Ally".to_string()))
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .assert(the!("person/nick").of(bob.clone()).is("Bobby".to_string()))
            .assert(
                the!("person/name")
                    .of(carol.clone())
                    .is("Carol".to_string()),
            )
        })
        .await?;
        let env = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let outcomes = merge_and_fold(
            vec![
                required(Term::from(the!("person/name")), "n", "c1"),
                optional(Term::from(the!("person/nick")), "k", "c2", Cardinality::One).into(),
            ],
            1,
            vec![Match::new()],
            &env,
        )
        .await?;
        let names = ["e", "n", "k"];
        assert_eq!(
            named(shown(
                outcomes.fold.as_ref().expect("the fold succeeds"),
                &names
            ))
            .len(),
            3,
            "one row per entity"
        );
        assert_same(&outcomes, &names);
        Ok(())
    }

    /// An incoming selection of several rows, binding a variable no premise
    /// reads, runs the left merge once per row; every row yields what the
    /// fold yields for it, in order.
    #[dialog_common::test]
    async fn it_left_merges_each_incoming_row_as_the_fold_does() -> anyhow::Result<()> {
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        let (operator, branch) = fixture(|tx| {
            tx.assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(the!("person/nick").of(alice.clone()).is("Al".to_string()))
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
        })
        .await?;
        let env = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let tagged = |tag: i64| -> anyhow::Result<Match> {
            let mut row = Match::new();
            row.bind(&Term::<Any>::var("tag"), Value::SignedInt(tag.into()))?;
            Ok(row)
        };
        let outcomes = merge_and_fold(
            vec![
                required(Term::from(the!("person/name")), "n", "c1"),
                optional(
                    Term::from(the!("person/nick")),
                    "k",
                    "c2",
                    Cardinality::Many,
                )
                .into(),
            ],
            1,
            vec![tagged(1)?, tagged(2)?, tagged(1)?],
            &env,
        )
        .await?;
        let names = ["e", "tag", "n", "k"];
        assert_eq!(
            outcomes.fold.as_ref().expect("the fold succeeds").len(),
            3 * (FILLER + 2),
            "every entity once per incoming row"
        );
        assert_same(&outcomes, &names);
        Ok(())
    }
}
