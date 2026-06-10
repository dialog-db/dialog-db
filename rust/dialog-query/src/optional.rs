use crate::attribute::The;
use crate::attribute::query::DynamicAttributeQuery;
use crate::environment::Environment;
use crate::query::Application;
use crate::selection::{Binding, Match, Selection};
use crate::source::SelectRules;
use crate::type_system::{Primitive, Type as Kind};
use crate::types::Any;
use crate::{
    Cardinality, Claim, Entity, EvaluationError, Parameters, Premise, Proposition, Requirement,
    Schema, Term, try_stream,
};
use core::pin::Pin;
use dialog_artifacts::{Cause, Select};
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use std::fmt::Display;
use std::fmt::{Formatter, Result as FmtResult};

/// Left-join projection over a scalar attribute lookup: the
/// semantic-layer realization of an optional (`maybe`) concept field.
///
/// For each input row, whose entity is guaranteed bound, the wrapped
/// scalar lookup runs and:
///
/// - every matching fact extends the row as usual (`is` bound
///   Present);
/// - when *no* fact exists for the entity, one fallback row is
///   yielded with the `is` (and named `cause`) slot bound to
///   [`Binding::Absent`] — set-widening;
/// - when the row already pins `is` to a Present value and no fact
///   matches it, nothing is yielded: that is a value *mismatch*, not
///   absence;
/// - when the row already binds `is` to `Absent`, the row passes
///   through only if the entity truly has no fact — a fact would
///   contradict the claimed absence.
///
/// The associative layer stays scalar: the wrapped query's `is` term
/// carries a plain value kind, and the widening (`T ∪ Nothing`) is
/// declared here, in [`OptionalAttributeQuery::schema`]. The schema also marks
/// the entity slot hard-required — "absent" is only meaningful for a
/// known entity ("absent for *whom*?"), so the planner must bind the
/// entity through some other premise before scheduling this step.
/// That requirement is the structural fix for the bug family where
/// an optional lookup leading an unbound scan silently dropped
/// entities (#348).
#[derive(Debug, Clone, PartialEq)]
pub struct OptionalAttributeQuery {
    /// The scalar attribute lookup to left-join against.
    query: DynamicAttributeQuery,
}

impl OptionalAttributeQuery {
    /// Create a left-join over the given attribute lookup terms. The
    /// `is` term is scalar; set-widening is declared by this wrapper,
    /// not by the term's kind.
    pub fn new(
        the: Term<The>,
        of: Term<Entity>,
        is: Term<Any>,
        cause: Term<Cause>,
        cardinality: Option<Cardinality>,
    ) -> Self {
        Self {
            query: DynamicAttributeQuery::new(the, of, is, cause, cardinality),
        }
    }

    /// The wrapped scalar lookup.
    pub fn query(&self) -> &DynamicAttributeQuery {
        &self.query
    }

    /// Unwrap into the scalar lookup, dropping the left-join. The
    /// planner uses this to demote an optional lookup whose value variable was
    /// narrowed to non-optional by rule inference: when a sibling
    /// premise guarantees the value is Present, the fallback can
    /// never fire and the premise is an ordinary scan.
    pub fn into_query(self) -> DynamicAttributeQuery {
        self.query
    }

    /// Get the 'is' (value) term of the wrapped lookup.
    pub fn is(&self) -> &Term<Any> {
        self.query.is()
    }

    /// Get the 'of' (entity) term of the wrapped lookup.
    pub fn of(&self) -> &Term<Entity> {
        self.query.of()
    }

    /// Schema of the wrapped lookup, adjusted for the left-join
    /// contract: the entity slot is hard-required (outside the choice
    /// group — absence is read relative to a known entity), and the
    /// `is`/`cause` slots are set-widened with `Nothing` since the
    /// fallback row binds them to Absent.
    pub fn schema(&self) -> Schema {
        let mut schema = self.query.schema();

        if let Some(field) = schema.get_mut("of") {
            field.requirement = Requirement::required();
        }
        if let Some(field) = schema.get_mut("is") {
            field.content_type = Some(match field.content_type.take() {
                Some(kind) => kind.optional(),
                None => Kind::primitive_set(Primitive::ALL).optional(),
            });
        }
        if let Some(field) = schema.get_mut("cause") {
            field.content_type = field.content_type.take().map(|kind| kind.optional());
        }

        schema
    }

    /// Parameters of the wrapped lookup.
    pub fn parameters(&self) -> Parameters {
        self.query.parameters()
    }

    /// Cost estimate: the wrapped lookup's estimate. The fallback row
    /// adds no meaningful cost.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        self.query.estimate(env)
    }

    /// Evaluate the left-join row by row.
    pub fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        env: &'a Env,
        selection: M,
    ) -> impl Selection + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        let selector = self;
        try_stream! {
            for await candidate in selection {
                let base = candidate?;

                // Structural invariant: the schema hard-requires the
                // entity, so the planner only schedules this step once
                // the entity is determined. A violation is a planner
                // bug, surfaced as an error instead of a phantom row.
                let entity_known = selector.query.of().resolve(&base).is_constant();
                if !entity_known {
                    Err(EvaluationError::UnboundVariable {
                        variable_name: selector
                            .query
                            .of()
                            .name()
                            .unwrap_or("of")
                            .to_string(),
                    })?;
                }

                let is_term = selector.query.is().clone();
                let claimed_absent =
                    matches!(base.lookup(&is_term), Ok(Binding::Absent));

                if claimed_absent {
                    // The row claims the value is absent. A fact for
                    // this entity would contradict that claim; no fact
                    // confirms it and the row passes through unchanged.
                    let probe = DynamicAttributeQuery::new(
                        selector.query.the().clone(),
                        selector.query.of().clone(),
                        Term::blank(),
                        Term::blank(),
                        Some(Cardinality::Many),
                    );
                    let inner: Pin<Box<dyn Selection + 'a>> =
                        Box::pin(Application::evaluate(probe, base.clone().seed(), env));
                    let mut found = false;
                    for await extension in inner {
                        extension?;
                        found = true;
                        break;
                    }
                    if !found {
                        yield base;
                    }
                    continue;
                }

                let value_pinned = base.is_present(&is_term);

                let inner: Pin<Box<dyn Selection + 'a>> = Box::pin(
                    Application::evaluate(selector.query.clone(), base.clone().seed(), env),
                );
                let mut produced = false;
                for await extension in inner {
                    let extension = extension?;
                    produced = true;
                    yield extension;
                }

                // Set-widening: the entity has no such fact, and the
                // value was not pinned Present (a pinned value with no
                // matching fact is a mismatch, not absence).
                if !produced && !value_pinned {
                    let mut fallback = base;
                    fallback.bind_absent(&is_term)?;
                    let cause_term: Term<Any> = Term::<Any>::from(selector.query.cause());
                    fallback.bind_absent(&cause_term)?;
                    yield fallback;
                }
            }
        }
    }
}

impl Application for OptionalAttributeQuery {
    type Conclusion = Claim;

    fn evaluate<'a, Env, M: Selection + 'a>(self, selection: M, env: &'a Env) -> impl Selection + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        self.evaluate(env, selection)
    }

    fn realize(&self, input: Match) -> Result<Claim, EvaluationError> {
        self.query.realize(input)
    }
}

impl Display for OptionalAttributeQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "maybe {}", self.query)
    }
}

impl From<OptionalAttributeQuery> for Proposition {
    fn from(query: OptionalAttributeQuery) -> Self {
        Proposition::OptionalAttribute(Box::new(query))
    }
}

impl From<OptionalAttributeQuery> for Premise {
    fn from(query: OptionalAttributeQuery) -> Self {
        Premise::Assert(Proposition::OptionalAttribute(Box::new(query)))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::the;
    use crate::{Type, Value};
    use dialog_repository::helpers::{test_operator_with_profile, test_repo};

    fn optional_nickname() -> OptionalAttributeQuery {
        OptionalAttributeQuery::new(
            Term::from(the!("person/nickname")),
            Term::<Entity>::var("person"),
            Term::<String>::var("nickname").into(),
            Term::blank(),
            Some(Cardinality::One),
        )
    }

    /// The schema declares the left-join contract: hard-required
    /// entity, set-widened value and cause slots — while the wrapped
    /// lookup stays scalar.
    #[dialog_common::test]
    fn it_widens_schema_but_keeps_inner_scalar() {
        let maybe = optional_nickname();

        let schema = maybe.schema();
        let of = schema.get("of").expect("of field");
        assert_eq!(of.requirement, Requirement::required());
        let is = schema.get("is").expect("is field");
        assert!(is.content_type.as_ref().expect("is kind").is_optional());
        assert!(
            is.content_type
                .as_ref()
                .expect("is kind")
                .primitive_part()
                .contains(Type::String),
            "widening preserves the inner value type"
        );

        // The wrapped lookup is scalar: its own schema does not widen.
        let inner = maybe.query().schema();
        let inner_is = inner.get("is").expect("inner is field");
        assert!(
            !inner_is.content_type.as_ref().expect("kind").is_optional(),
            "the associative layer stays scalar"
        );
    }

    /// Present fact: the row extends with the Present value, no
    /// fallback.
    #[dialog_common::test]
    async fn it_passes_present_fact_through() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(
                the!("person/nickname")
                    .of(alice.clone())
                    .is("Ali".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let mut input = Match::new();
        input.bind(&Term::var("person"), Value::Entity(alice))?;

        let results: Vec<Match> =
            Selection::try_vec(optional_nickname().evaluate(&source, input.seed())).await?;
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].lookup(&Term::var("nickname"))?,
            Binding::Present(Value::String("Ali".into()))
        );
        Ok(())
    }

    /// Missing fact: one fallback row with the value bound Absent.
    #[dialog_common::test]
    async fn it_yields_absent_fallback_on_miss() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let bob = Entity::new()?;
        let mut input = Match::new();
        input.bind(&Term::var("person"), Value::Entity(bob))?;

        let results: Vec<Match> =
            Selection::try_vec(optional_nickname().evaluate(&source, input.seed())).await?;
        assert_eq!(results.len(), 1, "set-widening yields one fallback row");
        assert_eq!(results[0].lookup(&Term::var("nickname"))?, Binding::Absent);
        Ok(())
    }

    /// A pinned Present value with no matching fact is a mismatch,
    /// not absence: zero rows, no fallback.
    #[dialog_common::test]
    async fn it_treats_pinned_value_mismatch_as_filter() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(
                the!("person/nickname")
                    .of(alice.clone())
                    .is("Ali".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let mut input = Match::new();
        input.bind(&Term::var("person"), Value::Entity(alice))?;
        input.bind(&Term::var("nickname"), Value::String("Al".into()))?;

        let results: Vec<Match> =
            Selection::try_vec(optional_nickname().evaluate(&source, input.seed())).await?;
        assert_eq!(
            results.len(),
            0,
            "value mismatch filters; no phantom Absent"
        );
        Ok(())
    }

    /// A row claiming Absent passes only when the entity truly has
    /// no fact; an existing fact contradicts the claim.
    #[dialog_common::test]
    async fn it_checks_claimed_absence_against_facts() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        branch
            .transaction()
            .assert(
                the!("person/nickname")
                    .of(alice.clone())
                    .is("Ali".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let mut claims_absent_alice = Match::new();
        claims_absent_alice.bind(&Term::var("person"), Value::Entity(alice))?;
        claims_absent_alice.bind_absent(&Term::var("nickname"))?;
        let results: Vec<Match> =
            Selection::try_vec(optional_nickname().evaluate(&source, claims_absent_alice.seed()))
                .await?;
        assert_eq!(results.len(), 0, "a fact contradicts the claimed absence");

        let mut claims_absent_bob = Match::new();
        claims_absent_bob.bind(&Term::var("person"), Value::Entity(bob))?;
        claims_absent_bob.bind_absent(&Term::var("nickname"))?;
        let results: Vec<Match> =
            Selection::try_vec(optional_nickname().evaluate(&source, claims_absent_bob.seed()))
                .await?;
        assert_eq!(results.len(), 1, "no fact confirms the claimed absence");
        Ok(())
    }

    /// A `Cardinality::Many` left-join: every fact extends the row
    /// (one output row per fact), and a miss still yields exactly
    /// one `Absent` row — set-widening is about the *entity*, not
    /// the fact count.
    #[dialog_common::test]
    async fn it_left_joins_cardinality_many_fields() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        branch
            .transaction()
            .assert(the!("person/alias").of(alice.clone()).is("Ali".to_string()))
            .assert(
                the!("person/alias")
                    .of(alice.clone())
                    .is("Allie".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let aliases = OptionalAttributeQuery::new(
            Term::from(the!("person/alias")),
            Term::<Entity>::var("person"),
            Term::<String>::var("alias").into(),
            Term::blank(),
            Some(Cardinality::Many),
        );

        let mut alice_row = Match::new();
        alice_row.bind(&Term::var("person"), Value::Entity(alice))?;
        let results: Vec<Match> =
            Selection::try_vec(aliases.clone().evaluate(&source, alice_row.seed())).await?;
        assert_eq!(results.len(), 2, "every alias extends the row");

        let mut bob_row = Match::new();
        bob_row.bind(&Term::var("person"), Value::Entity(bob))?;
        let results: Vec<Match> =
            Selection::try_vec(aliases.evaluate(&source, bob_row.seed())).await?;
        assert_eq!(results.len(), 1, "a miss yields exactly one fallback row");
        assert_eq!(results[0].lookup(&Term::var("alias"))?, Binding::Absent);
        Ok(())
    }

    /// Positive-polarity counterpart of Absent-matches-nothing: a
    /// scalar scan whose value variable arrives bound `Absent` (from
    /// an upstream left-join) filters the row instead of scanning
    /// unconstrained or aborting the stream.
    #[dialog_common::test]
    async fn it_filters_scalar_scan_over_absent_binding() -> anyhow::Result<()> {
        use crate::query::Application;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let club = Entity::new()?;
        branch
            .transaction()
            .assert(the!("club/banned").of(club.clone()).is("Ali".to_string()))
            .commit()
            .perform(&operator)
            .await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let scan = DynamicAttributeQuery::new(
            Term::from(the!("club/banned")),
            Term::blank(),
            Term::<String>::var("nickname").into(),
            Term::blank(),
            Some(Cardinality::One),
        );

        let mut row = Match::new();
        row.bind(&Term::var("person"), Value::Entity(alice))?;
        row.bind_absent(&Term::var("nickname"))?;

        let results: Vec<Match> =
            Selection::try_vec(Application::evaluate(scan, row.seed(), &source)).await?;
        assert_eq!(
            results.len(),
            0,
            "an Absent binding matches nothing in a scalar slot"
        );
        Ok(())
    }

    /// An unbound entity is a planner-contract violation surfaced as
    /// an error, never a phantom row.
    #[dialog_common::test]
    async fn it_errors_on_unbound_entity() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let results =
            Selection::try_vec(optional_nickname().evaluate(&source, Match::new().seed())).await;
        assert!(results.is_err(), "unbound entity must error");
        Ok(())
    }
}
