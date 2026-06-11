use super::Plan;
use crate::Environment;
use crate::selection::Selection;
use crate::source::SelectRules;
use core::pin::Pin;
use dialog_artifacts::Select;
use dialog_capability::Provider;
use dialog_common::ConditionalSync;

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
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        self.steps.into_iter().fold(
            Box::pin(selection) as Pin<Box<dyn Selection + 'a>>,
            |selection, plan| Box::pin(plan.evaluate(selection, env)),
        )
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
    use dialog_repository::helpers::{test_operator_with_profile, test_repo};
    use futures_util::TryStreamExt;

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
