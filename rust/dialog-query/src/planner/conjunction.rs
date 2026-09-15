use super::Plan;
use crate::attribute::query::DynamicAttributeQuery;
use crate::selection::{Match, Selection};
use crate::{Environment, SortOrder, multi_merge_join};
use core::pin::Pin;
use dialog_artifacts::{Estimate, Likelihood, Preload, PreloadRequest, encode_value_owned};
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use futures_util::StreamExt;

/// Largest ratio of the widest to narrowest scan range estimate that still
/// takes the merge path. Above it, one scan is selective enough that a nested
/// loop driving from it reads fewer blocks than a merge scanning every range
/// in full.
///
/// The estimates come from the tree (per-child
/// [`Scale`](dialog_search_tree::Scale) sums, one root read per scan), so an
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
            |selection, plan| Box::pin(plan.evaluate(selection, env)),
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
        for step in &self.steps {
            let query = match step {
                Plan::Scan(_, query) => query,
                _ => return None,
            };
            let variable = match query.sort_order() {
                SortOrder::On(name) => name,
                SortOrder::None => return None,
            };
            match &shared {
                None => shared = Some(variable),
                Some(existing) if *existing == variable => {}
                Some(_) => return None,
            }
        }
        shared
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
    /// [`Estimate`] — one root-node read per scan, an advisory upper bound
    /// from the tree's per-child [`Scale`](dialog_search_tree::Scale)s. The
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
        let mut min_size = u64::MAX;
        let mut max_size = 0u64;
        for step in &self.steps {
            let Plan::Scan(_, query) = step else { continue };
            // A selector build failure or an unavailable estimate means
            // "range size unknown"; treat as maximally broad so an all-broad
            // join stays eligible and a genuinely selective one is never
            // wrongly merged on a missing estimate.
            let size = match query.resolved_selector(base) {
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
        min_size != 0 && max_size <= min_size.saturating_mul(MERGE_COST_BALANCE as u64)
    }

    /// Evaluate a structurally merge-eligible conjunction, choosing per query
    /// between the N-way merge and the nested-loop fold by the incoming
    /// rows' selectivity.
    ///
    /// Every match in a selection shares one binding pattern (only the
    /// values differ), so the merge-versus-fold choice is uniform across the
    /// stream. This peeks the first row, decides once via
    /// [`scans_balanced_for`](Self::scans_balanced_for), and runs the chosen
    /// strategy over the whole selection (the peeked row put back at the
    /// head). An empty selection yields nothing either way. On the merge
    /// path, each scan is resolved against the incoming row (binding any
    /// variables the caller already supplied), evaluated independently, and
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
        Box::pin(crate::try_stream! {
            let mut selection = Box::pin(selection.peekable());

            // Decide from the first row's bindings; nothing to do if empty.
            let balanced = match selection.as_mut().peek().await {
                Some(Ok(first)) => {
                    let first = first.clone();
                    self.scans_balanced_for(&first, env).await
                }
                // Empty, or a pending error surfaced on the next poll below.
                _ => true,
            };

            if !balanced {
                // A selective attribute is pinned: the nested-loop fold reads
                // fewer blocks by driving from the narrow scan.
                for await row in self.into_fold(selection, env) {
                    yield row?;
                }
                return;
            }

            let scans: Vec<DynamicAttributeQuery> = self
                .steps
                .into_iter()
                .map(|step| match step {
                    Plan::Scan(_, query) => *query,
                    // merge_variable already proved every step is a Scan.
                    _ => unreachable!("merge eligibility guarantees every step is a Scan"),
                })
                .collect();

            let mut listening = true;
            for await incoming in selection {
                let base = incoming?;

                // The merge will read every input range in full, so each
                // range is committed work: hint it Likely, and a driven
                // plan replicates it level-parallel while the merge's own
                // streams consume — the demand reads join the in-flight
                // hydrations or find the blocks local. Hinting stops on
                // the env's first refusal, exactly as probe pipelining
                // does.
                if listening {
                    for scan in &scans {
                        let Ok(selector) = scan.resolved_selector(&base) else {
                            continue;
                        };
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
                // caller-supplied bindings resolve into the scan's constants;
                // the shared join variable stays free and drives the merge.
                let mut inputs: Vec<Pin<Box<dyn Selection + 'a>>> =
                    Vec::with_capacity(scans.len());
                for scan in &scans {
                    let seeded = base.clone().seed();
                    inputs.push(Box::pin(scan.clone().evaluate(env, seeded)));
                }

                let variable = variable.clone();
                let key = move |m: &Match| -> Option<Vec<u8>> {
                    m.value_of(&variable).map(encode_value_owned)
                };

                for await row in multi_merge_join(inputs, key) {
                    let row = row?;
                    // Fold the joined bindings back onto the incoming row so
                    // the caller's context (and provenance) is preserved.
                    if let Some(merged) = base.clone().combine(&row) {
                        yield merged;
                    }
                }
            }
        })
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

    use crate::attribute::query::AttributeQuery;
    use crate::formula::math::Sum;
    use crate::formula::string::Uppercase;
    use crate::optional::OptionalAttributeQuery;
    use crate::planner::Planner;
    use crate::selection::Match;
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::the;
    use crate::types::Any;
    use crate::{
        AttributeDescriptor, Cardinality, ConceptDescriptor, ConceptFieldDescriptor, ConceptQuery,
        Environment, Formula, Negation, Parameters, Premise, Proposition, Term, Type, Value,
    };
    use dialog_artifacts::Entity;
    use dialog_operator::helpers::{test_operator_with_profile, test_repo};
    use futures_util::TryStreamExt;

    /// A two-attribute conjunction over a shared entity is structurally merge
    /// eligible, and the tree-derived balance check picks merge only when
    /// neither scan is selective. This pins the regression the merge
    /// introduced: with a value pinned on one scan, `scans_balanced_for` must
    /// report imbalance (from the real range estimates) so the query falls
    /// back to the nested-loop fold instead of scanning every range in full.
    #[dialog_common::test]
    async fn it_prefers_the_fold_when_one_scan_is_selective() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // Seed a spread of entities, each with a name and a role, so the two
        // attribute ranges are broad and comparably sized, while any single
        // value ("name-7") is selective.
        let mut tx = branch.transaction();
        for i in 0..2000 {
            let entity = Entity::new()?;
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

    /// Coalesce must take the *source* when the lookup finds a value
    /// and the fallback only when it does not. The coalesce's source
    /// slot is a hard requirement, so the planner orders it after
    /// the left-join that binds `?nickname`.
    #[dialog_common::test]
    async fn it_takes_present_source_over_coalesce_fallback() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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
    async fn it_sums_signed_integers_and_filters_mixed_rows() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        branch
            .transaction()
            .assert(the!("game/score").of(alice.clone()).is(-5i64))
            .assert(the!("game/bonus").of(alice.clone()).is(-3i64))
            .assert(the!("game/score").of(bob.clone()).is(2i64))
            // Bob's bonus is a float: his row cannot instantiate the
            // scheme to one type.
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
            1,
            "Alice computes within one type; Bob's mixed row is a non-match"
        );
        assert_eq!(
            results[0].lookup(&Term::var("total"))?.content()?,
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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
}
