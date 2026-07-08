use super::all::AttributeQueryAll;
use super::resolution::{Accumulator, Resolution};
use crate::Claim;
use crate::Value;
use crate::artifact::{ArtifactSelector, ArtifactsAttribute, Constrained};
use crate::attribute::The;
use crate::environment::Environment;
use crate::query::Application;
use crate::query::Output;
use crate::schema::Cardinality;
use crate::selection::{Match, Selection};
use crate::source::SelectRules;
use crate::type_system::Type as Kind;
use crate::types::{Any, Record};
use crate::{Entity, EvaluationError, Parameters, Schema, Term, try_stream};
use dialog_artifacts::{Cause, Select};
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use std::fmt::Display;
use std::fmt::{Formatter, Result as FmtResult};
use std::hash::{Hash, Hasher};

/// Winner verification.
///
/// When the entity is unknown, results from the base scan (VAE or AEV) are
/// not guaranteed to contain all competing values for the same
/// `(attribute, entity)` pair. Each candidate is verified by a secondary
/// `(attribute, entity)` lookup resolved with the query's [`Resolution`].
/// Yields the match only if the candidate matches the resolved product —
/// under a fold, a diverged group's product byte-matches neither stored
/// sibling, so value-bound reads of a diverged record yield zero rows until
/// a write converges storage (spec §6.10).
fn challenge<'a, Env>(
    env: &'a Env,
    selector: AttributeQueryAll,
    resolution: Resolution,
    candidate: Match,
) -> impl Selection + 'a
where
    Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
{
    try_stream! {
        // The candidate fact is cited on the row by `merge`; read it
        // from the claim rather than from the row's terms; a blank
        // term (e.g. an unconstrained entity) never stores a binding,
        // so term lookups cannot recover the fact.
        let claim = candidate.prove(selector.source())?;
        let attribute = ArtifactsAttribute::try_from(Value::from(claim.the().clone()))?;
        let entity = claim.of().clone();
        let value = claim.is().clone();
        let cause_term = selector.cause();
        let cause = if cause_term.is_blank() {
            None
        } else {
            Some(Cause::try_from(candidate.lookup(&Term::from(cause_term))?.content()?)?)
        };

        let challengers = Provider::<Select<'_>>::execute(env, ArtifactSelector::new()
            .the(attribute)
            .of(entity)).await?;

        let mut group: Option<Accumulator> = None;
        for await each in challengers {
            let challenger = each?;
            group = Some(match group {
                None => resolution.begin(challenger),
                Some(group) => group.absorb(challenger),
            });
        }

        let winner = group.map(|group| resolution.resolve(group));
        if let Some(winner) = winner
            && winner.is == value
        {
            let winner_cause = winner.cause.unwrap_or(Cause([0; 32]));
            if cause.is_none() || cause == Some(winner_cause) {
                yield candidate;
            }
        }
    }
}

/// Winner-selecting attribute query for `Cardinality::One`.
///
/// Wraps an [`AttributeQueryAll`] and resolves competing values per
/// `(attribute, entity)` pair to a single row, using the query's
/// [`Resolution`]: pick-one by highest cause (the default, and every scalar
/// attribute's behavior) or a format-aware fold for record attributes.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AttributeQueryOnly {
    query: AttributeQueryAll,
    /// How competing siblings resolve to one row. Never part of the query's
    /// identity (equality, hashing, serialization): the strategy is a pure
    /// function of the attribute and its format, which are already identity.
    #[serde(skip)]
    resolution: Resolution,
}

/// Identity ignores [`Resolution`]: see the field documentation.
impl PartialEq for AttributeQueryOnly {
    fn eq(&self, other: &Self) -> bool {
        self.query == other.query
    }
}

impl Eq for AttributeQueryOnly {}

impl Hash for AttributeQueryOnly {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.query.hash(state);
    }
}

impl AttributeQueryOnly {
    /// Create a new winner-selecting attribute query. Scalar like
    /// every associative-layer lookup: zero rows on miss.
    pub fn new(the: Term<The>, of: Term<Entity>, is: Term<Any>, cause: Term<Cause>) -> Self {
        Self {
            query: AttributeQueryAll::new(the, of, is, cause),
            resolution: Resolution::Choose,
        }
    }

    /// Replace this query's sibling-resolution strategy.
    pub fn with_resolution(self, resolution: Resolution) -> Self {
        Self { resolution, ..self }
    }

    /// Get the 'the' (attribute) term.
    pub fn the(&self) -> &Term<The> {
        self.query.the()
    }

    /// Get the 'of' (entity) term.
    pub fn of(&self) -> &Term<Entity> {
        self.query.of()
    }

    /// Get the 'is' (value) parameter.
    pub fn is(&self) -> &Term<Any> {
        self.query.is()
    }

    /// Return a copy with the `is` term's type narrowed to `kind`.
    /// See [`AttributeQueryAll::with_type`].
    pub(crate) fn with_type(self, kind: Kind) -> Self {
        Self {
            query: self.query.with_type(kind),
            resolution: self.resolution,
        }
    }

    /// See [`AttributeQueryAll::with_subject_kinds`].
    pub(crate) fn with_subject_kinds(self, the: Option<Kind>, of: Option<Kind>) -> Self {
        Self {
            query: self.query.with_subject_kinds(the, of),
            resolution: self.resolution,
        }
    }

    /// Get the 'cause' term.
    pub fn cause(&self) -> &Term<Cause> {
        self.query.cause()
    }

    /// Get the source term (internal claim handle).
    pub fn source(&self) -> &Term<Record> {
        self.query.source()
    }

    /// Map `Term<The>` to `Term<ArtifactsAttribute>`.
    pub fn attribute(&self) -> Term<ArtifactsAttribute> {
        self.query.attribute()
    }

    /// Returns the schema describing this application's parameters.
    pub fn schema(&self) -> Schema {
        self.query.schema()
    }

    /// Estimate cost for Cardinality::One semantics.
    ///
    /// The cost table in [`Cardinality::estimate`] already includes the
    /// VERIFY overhead for VAE-based lookups, so no additional adjustment
    /// is needed here.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        let the = self.the().is_bound(env);
        let of = self.of().is_bound(env);
        let is = self.is().is_bound(env);

        Cardinality::One.estimate(the, of, is)
    }

    /// Returns the parameters for this query.
    pub fn parameters(&self) -> Parameters {
        self.query.parameters()
    }

    /// Evaluate with winner selection based on scan strategy.
    ///
    /// The strategy is chosen **per match** after resolving variables from
    /// the incoming selection, so that bindings produced by earlier premises
    /// are taken into account:
    ///
    /// - **Sliding window**: entity known (EAV), or attribute known without
    ///   value (AEV). Results are grouped by `(attribute, entity)` so we
    ///   resolve each group in a single pass.
    /// - **Challenge**: value known without entity ({is}, {the, is}, {of, is}
    ///   without entity). Each candidate is verified by a secondary
    ///   `(attribute, entity)` lookup because the scan is not grouped by
    ///   entity or because blanking the value would widen the scan.
    ///
    /// Both paths combine a group's siblings through the same
    /// [`Resolution`], so a format-aware fold substitutes into pick-one
    /// winner selection at exactly one point.
    pub fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        env: &'a Env,
        selection: M,
    ) -> impl Selection + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        let Self {
            query: selector,
            resolution,
        } = self;
        try_stream! {
            for await each in selection {
                let base = each?;

                // An Absent-bound parameter matches nothing at the
                // scalar layer: filter the row without scanning.
                if selector.absent_blocked(&base) {
                    continue;
                }

                // Resolve variables from the incoming match so that bindings
                // from earlier premises are visible to the strategy decision.
                let resolved = selector.resolve(&base);

                let entity_known = resolved.of().is_constant();
                let attribute_known = resolved.the().is_constant();
                let value_known = resolved.is().is_constant();

                if entity_known || (attribute_known && !value_known) {
                    // Sliding window path.
                    let value_constraint = resolved.is().as_constant().cloned();

                    let scan = AttributeQueryAll::new(
                        resolved.the().clone(),
                        resolved.of().clone(),
                        Term::blank(),
                        resolved.cause().clone(),
                    );

                    let mut candidate: Option<Accumulator> = None;

                    let stream = Provider::<Select<'_>>::execute(env, (&scan).try_into()?).await?;
                    for await artifact in stream {
                        let artifact = artifact?;

                        candidate = Some(match candidate.take() {
                            Some(group) if group.groups_with(&artifact) => {
                                group.absorb(artifact)
                            }
                            Some(group) => {
                                let winner = resolution.resolve(group);
                                if (value_constraint.is_none() || value_constraint.as_ref() == Some(&winner.is))
                                    && selector.admits(&winner.is)
                                {
                                    let mut extension = base.clone();
                                    selector.merge(&mut extension, &winner)?;
                                    yield extension;
                                }
                                resolution.begin(artifact)
                            }
                            None => resolution.begin(artifact),
                        });
                    }

                    // Yield the final group's winner.
                    if let Some(group) = candidate.take() {
                        let winner = resolution.resolve(group);
                        if (value_constraint.is_none() || value_constraint.as_ref() == Some(&winner.is))
                            && selector.admits(&winner.is)
                        {
                            let mut extension = base.clone();
                            selector.merge(&mut extension, &winner)?;
                            yield extension;
                        }
                    }
                } else {
                    // Secondary lookup path (Box::pin to avoid stack overflow).
                    let candidates = Box::pin(resolved.evaluate(env, base.clone().seed()));
                    for await candidate in candidates {
                        let candidate = candidate?;
                        let verified = Box::pin(challenge(env, selector.clone(), resolution.clone(), candidate));
                        for await v in verified {
                            yield v?;
                        }
                    }
                }
            }
        }
    }

    /// Execute this query, returning a stream of claims.
    pub fn perform<'a, Env>(self, env: &'a Env) -> impl Output<Claim> + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
        Self: Sized,
    {
        Application::perform(self, env)
    }
}

impl Application for AttributeQueryOnly {
    type Conclusion = Claim;

    fn evaluate<'a, Env, M: Selection + 'a>(self, selection: M, env: &'a Env) -> impl Selection + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        self.evaluate(env, selection)
    }

    fn realize(&self, input: Match) -> Result<Claim, EvaluationError> {
        input.prove(self.query.source())
    }
}

impl TryFrom<&AttributeQueryOnly> for ArtifactSelector<Constrained> {
    type Error = EvaluationError;

    fn try_from(from: &AttributeQueryOnly) -> Result<Self, Self::Error> {
        ArtifactSelector::try_from(&from.query)
    }
}

impl Display for AttributeQueryOnly {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Display::fmt(&self.query, f)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::query::Output;
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::{Value, the};
    use dialog_repository::helpers::{test_operator_with_profile, test_repo};

    macro_rules! assert_relation {
        ($branch:expr, $operator:expr, $the:expr, $of:expr, $is:expr) => {{
            $branch
                .transaction()
                .assert($the.clone().of($of.clone()).is($is))
                .commit()
                .perform($operator)
                .await
                .unwrap();
        }};
    }

    #[dialog_common::test]
    async fn it_selects_winner_with_constant_entity() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        assert_relation!(branch, &operator, name_attr, alice, "Alice".to_string());
        assert_relation!(branch, &operator, name_attr, alice, "Alicia".to_string());

        let query = AttributeQueryOnly::new(
            Term::var("the"),
            Term::from(alice.clone()),
            Term::var("value"),
            Term::var("cause"),
        );

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(
            results.len(),
            1,
            "EAV path should yield one winner per (attribute, entity)"
        );
        assert_eq!(results[0].of(), &alice);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_selects_winner_with_constant_attribute() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let name_attr = the!("person/name");

        assert_relation!(branch, &operator, name_attr, alice, "Alice".to_string());
        assert_relation!(branch, &operator, name_attr, alice, "Alicia".to_string());
        assert_relation!(branch, &operator, name_attr, bob, "Bob".to_string());
        assert_relation!(branch, &operator, name_attr, bob, "Robert".to_string());

        let query = AttributeQueryOnly::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
        );

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(
            results.len(),
            2,
            "AEV path should yield one winner per entity"
        );

        let alice_results: Vec<_> = results.iter().filter(|f| f.of() == &alice).collect();
        let bob_results: Vec<_> = results.iter().filter(|f| f.of() == &bob).collect();

        assert_eq!(alice_results.len(), 1);
        assert_eq!(bob_results.len(), 1);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_selects_winner_via_vae_path() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        assert_relation!(branch, &operator, name_attr, alice, "Alice".to_string());
        assert_relation!(branch, &operator, name_attr, alice, "Alicia".to_string());

        // First find the winner via AEV to know which value wins.
        let aev_query = AttributeQueryOnly::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
        );

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let aev_results = aev_query.perform(&source).try_vec().await?;
        assert_eq!(aev_results.len(), 1);
        let winner_value = aev_results[0].is().clone();

        // VAE path: only value known, both the and of are variables.
        let vae_query = AttributeQueryOnly::new(
            Term::var("the"),
            Term::var("person"),
            Term::Constant(winner_value.clone()),
            Term::var("cause"),
        );

        let vae_results = vae_query.perform(&source).try_vec().await?;

        assert_eq!(
            vae_results.len(),
            1,
            "VAE path should verify and return the winner"
        );
        assert_eq!(vae_results[0].is(), &winner_value);

        Ok(())
    }

    /// When both attribute and value are known ({the, is}) but entity is
    /// unknown, the VAE scan only sees artifacts matching that exact value.
    /// If another value is the actual winner for an entity, the scan won't
    /// see it. The challenge/verification path must detect this and filter
    /// out non-winners.
    #[dialog_common::test]
    async fn it_verifies_winner_for_attribute_and_value_known() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let entity = Entity::new()?;

        // Assert two competing values for the same (attribute, entity) pair.
        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(entity.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .perform(&operator)
            .await
            .unwrap();
        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(entity.clone())
                    .is("Alicia".to_string()),
            )
            .commit()
            .perform(&operator)
            .await
            .unwrap();

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        // First, determine which value is the actual winner via an
        // unconstrained Cardinality::One query (entity known -> EAV path).
        let race = the!("person/name")
            .of(Term::from(entity.clone()))
            .is(Term::<String>::var("name"))
            .cardinality(Cardinality::One)
            .perform(&source)
            .try_vec()
            .await?;
        assert_eq!(race.len(), 1);
        let winner_value = race[0].is().clone();
        let (winner, looser) = if winner_value == Value::String("Alice".into()) {
            ("Alice".to_string(), "Alicia".to_string())
        } else {
            ("Alicia".to_string(), "Alice".to_string())
        };

        // Query with {the, is} for the LOSER value.
        // The VAE scan finds it, but verification must reject it.
        let results = the!("person/name")
            .of(Term::var("person"))
            .is(looser.clone())
            .cardinality(Cardinality::One)
            .perform(&source)
            .try_vec()
            .await?;

        assert_eq!(
            results.len(),
            0,
            "The loser value '{}' should be filtered out by winner verification",
            looser,
        );

        // Query with {the, is} for the WINNER value.
        // Verification confirms it is the winner.
        let results = the!("person/name")
            .of(Term::var("person"))
            .is(winner.clone())
            .cardinality(Cardinality::One)
            .perform(&source)
            .try_vec()
            .await?;

        assert_eq!(results.len(), 1, "The winner value should be returned");
        assert_eq!(results[0].of(), &entity);

        Ok(())
    }

    /// {of, is}: entity + value known, attribute unknown.
    /// The challenge path must reject the loser and accept the winner.
    #[dialog_common::test]
    async fn it_verifies_winner_for_entity_and_value_known() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let entity = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(entity.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .perform(&operator)
            .await
            .unwrap();
        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(entity.clone())
                    .is("Alicia".to_string()),
            )
            .commit()
            .perform(&operator)
            .await
            .unwrap();

        // Determine the winner via EAV (entity known, value unknown).
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let race = the!("person/name")
            .of(Term::from(entity.clone()))
            .is(Term::<String>::var("name"))
            .cardinality(Cardinality::One)
            .perform(&source)
            .try_vec()
            .await?;
        assert_eq!(race.len(), 1);
        let winner_value = race[0].is().clone();
        let (winner, looser) = if winner_value == Value::String("Alice".into()) {
            ("Alice".to_string(), "Alicia".to_string())
        } else {
            ("Alicia".to_string(), "Alice".to_string())
        };

        // {of, is} with the LOSER value -- should return nothing.
        let results = Term::<The>::var("relation")
            .of(entity.clone())
            .is(looser.clone())
            .cardinality(Cardinality::One)
            .perform(&source)
            .try_vec()
            .await?;

        assert_eq!(
            results.len(),
            0,
            "{{of, is}} with loser value '{}' should be filtered out",
            looser,
        );

        // {of, is} with the WINNER value -- should return the winner.
        let results = Term::<The>::var("relation")
            .of(entity.clone())
            .is(winner.clone())
            .cardinality(Cardinality::One)
            .perform(&source)
            .try_vec()
            .await?;

        assert_eq!(
            results.len(),
            1,
            "{{of, is}} with winner value '{}' should be returned",
            winner,
        );
        assert_eq!(results[0].of(), &entity);

        Ok(())
    }

    /// {is}: only value known.
    /// The challenge path must reject the loser and accept the winner.
    #[dialog_common::test]
    async fn it_verifies_winner_for_value_only_known() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let entity = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(entity.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .perform(&operator)
            .await
            .unwrap();
        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(entity.clone())
                    .is("Alicia".to_string()),
            )
            .commit()
            .perform(&operator)
            .await
            .unwrap();

        // Determine the winner via EAV.
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let race = the!("person/name")
            .of(Term::from(entity.clone()))
            .is(Term::<String>::var("name"))
            .cardinality(Cardinality::One)
            .perform(&source)
            .try_vec()
            .await?;
        assert_eq!(race.len(), 1);
        let winner_value = race[0].is().clone();
        let (winner, looser) = if winner_value == Value::String("Alice".into()) {
            ("Alice".to_string(), "Alicia".to_string())
        } else {
            ("Alicia".to_string(), "Alice".to_string())
        };

        // {is} with the LOSER value -- should return nothing.
        let results = Term::<The>::var("relation")
            .of(Term::var("person"))
            .is(looser.clone())
            .cardinality(Cardinality::One)
            .perform(&source)
            .try_vec()
            .await?;

        assert_eq!(
            results.len(),
            0,
            "{{is}} with loser value '{}' should be filtered out",
            looser,
        );

        // {is} with the WINNER value -- should return the winner.
        let results = Term::<The>::var("relation")
            .of(Term::var("person"))
            .is(winner.clone())
            .cardinality(Cardinality::One)
            .perform(&source)
            .try_vec()
            .await?;

        assert_eq!(
            results.len(),
            1,
            "{{is}} with winner value '{}' should be returned",
            winner,
        );
        assert_eq!(results[0].of(), &entity);

        Ok(())
    }

    /// An *optional* `is` whose variable an earlier premise bound to
    /// a value the entity does NOT have must NOT emit an Absent
    /// fallback. The fact exists (the entity has the attribute with a
    /// different value); the miss is a value mismatch, not absence.
    /// "Absent" means "no fact for this attribute," so a Required
    /// field in the same situation yields zero rows; the optional
    /// field must agree on the row count (zero), never assert the
    /// attribute is missing when it is merely different.
    ///
    /// Before the fix, the sliding-window winner failed the resolved
    /// value constraint, `produced` stayed false, and the fallback
    /// fired on `is_optional()` alone, binding the already-Present
    /// `is` variable to Absent, which errors and aborts the whole
    /// stream.
    #[dialog_common::test]
    async fn it_does_not_emit_absent_on_optional_value_mismatch() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let bob = Entity::new()?;
        let nickname_attr = the!("person/nickname");

        // Bob HAS a nickname, but it is "Bobby".
        assert_relation!(branch, &operator, nickname_attr, bob, "Bobby".to_string());

        // Optional `is` (set-widened): a missing fact would normally
        // yield one Absent fallback row.
        let optional_is: Term<Any> = Term::<Option<String>>::var("nickname").into();
        let query = AttributeQueryOnly::new(
            Term::from(the!("person/nickname")),
            Term::from(bob.clone()),
            optional_is.clone(),
            Term::var("cause"),
        );

        // An earlier premise constrained ?nickname to "Ali": a value
        // Bob does not have.
        let mut seed = Match::new();
        seed.bind(&optional_is, Value::from("Ali".to_string()))?;

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results =
            Selection::try_vec(Application::evaluate(query, seed.seed(), &source)).await?;

        assert_eq!(
            results.len(),
            0,
            "a value mismatch on an optional field is NOT absence: \
             the attribute exists with a different value, so no row \
             (and certainly no Absent fallback) should be produced"
        );

        Ok(())
    }

    mod fold {
        use super::*;
        use crate::attribute::AttributeStatement;
        use crate::attribute::query::resolution::Resolution;
        use crate::attribute::query::resolution::test::{ByteSet, Picky};

        /// An additive (raw) record assertion: two of these for one
        /// `(the, of)` coexist as sibling claims, simulating the storage
        /// state concurrent replicas produce after sync.
        fn record_fact(the: The, of: &Entity, bytes: Vec<u8>) -> AttributeStatement {
            AttributeStatement {
                the,
                of: of.clone(),
                is: Value::Record(bytes.into()),
                cause: None,
                cardinality: None,
            }
        }

        #[dialog_common::test]
        async fn it_folds_record_siblings_with_entity_known() -> anyhow::Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let note = Entity::new()?;
            branch
                .transaction()
                .assert(record_fact(the!("note/body"), &note, vec![1, 2]))
                .commit()
                .perform(&operator)
                .await?;
            branch
                .transaction()
                .assert(record_fact(the!("note/body"), &note, vec![2, 3]))
                .commit()
                .perform(&operator)
                .await?;

            let query = AttributeQueryOnly::new(
                Term::from(the!("note/body")),
                Term::from(note.clone()),
                Term::var("doc"),
                Term::var("cause"),
            )
            .with_resolution(Resolution::fold::<ByteSet>());

            let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
            let results = query.perform(&source).try_vec().await?;

            assert_eq!(results.len(), 1, "EAV path folds siblings into one row");
            assert_eq!(
                results[0].is(),
                &Value::Record(vec![1, 2, 3].into()),
                "the row carries the merged document, not either fork"
            );

            Ok(())
        }

        #[dialog_common::test]
        async fn it_folds_each_group_separately_via_aev() -> anyhow::Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let diverged = Entity::new()?;
            let settled = Entity::new()?;
            for fact in [
                record_fact(the!("note/body"), &diverged, vec![1, 2]),
                record_fact(the!("note/body"), &diverged, vec![2, 3]),
                record_fact(the!("note/body"), &settled, vec![7]),
            ] {
                branch
                    .transaction()
                    .assert(fact)
                    .commit()
                    .perform(&operator)
                    .await?;
            }

            let query = AttributeQueryOnly::new(
                Term::from(the!("note/body")),
                Term::var("note"),
                Term::var("doc"),
                Term::var("cause"),
            )
            .with_resolution(Resolution::fold::<ByteSet>());

            let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
            let results = query.perform(&source).try_vec().await?;

            assert_eq!(results.len(), 2, "one row per (attribute, entity) group");

            let diverged_row = results.iter().find(|r| r.of() == &diverged).unwrap();
            let settled_row = results.iter().find(|r| r.of() == &settled).unwrap();
            assert_eq!(diverged_row.is(), &Value::Record(vec![1, 2, 3].into()));
            assert_eq!(
                settled_row.is(),
                &Value::Record(vec![7].into()),
                "a singleton group passes through untouched"
            );

            Ok(())
        }

        /// A single sibling never decodes (spec §4.4: pass through, zero
        /// added cost): even bytes the format rejects flow through the
        /// fold-resolution query unchanged.
        #[dialog_common::test]
        async fn it_passes_single_sibling_through_without_decoding() -> anyhow::Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let note = Entity::new()?;
            branch
                .transaction()
                .assert(record_fact(the!("note/body"), &note, vec![0xFF, 1]))
                .commit()
                .perform(&operator)
                .await?;

            let query = AttributeQueryOnly::new(
                Term::from(the!("note/body")),
                Term::from(note.clone()),
                Term::var("doc"),
                Term::var("cause"),
            )
            .with_resolution(Resolution::fold::<Picky>());

            let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
            let results = query.perform(&source).try_vec().await?;

            assert_eq!(results.len(), 1);
            assert_eq!(results[0].is(), &Value::Record(vec![0xFF, 1].into()));

            Ok(())
        }

        /// Value-bound reads of a *diverged* record yield zero rows: the
        /// fold product byte-matches neither stored sibling, and the
        /// product itself is not a stored key (spec §6.10). A settled
        /// (single-sibling) record remains queryable by value.
        #[dialog_common::test]
        async fn it_verifies_fold_product_on_challenge_path() -> anyhow::Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let diverged = Entity::new()?;
            let settled = Entity::new()?;
            for fact in [
                record_fact(the!("note/body"), &diverged, vec![1, 2]),
                record_fact(the!("note/body"), &diverged, vec![2, 3]),
                record_fact(the!("note/body"), &settled, vec![7]),
            ] {
                branch
                    .transaction()
                    .assert(fact)
                    .commit()
                    .perform(&operator)
                    .await?;
            }

            let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
            let by_value = |value: Vec<u8>| {
                AttributeQueryOnly::new(
                    Term::from(the!("note/body")),
                    Term::var("note"),
                    Term::Constant(Value::Record(value.into())),
                    Term::var("cause"),
                )
                .with_resolution(Resolution::fold::<ByteSet>())
            };

            let sibling = by_value(vec![1, 2]).perform(&source).try_vec().await?;
            assert_eq!(
                sibling.len(),
                0,
                "a stored fork is not the fold product: filtered by verification"
            );

            let product = by_value(vec![1, 2, 3]).perform(&source).try_vec().await?;
            assert_eq!(
                product.len(),
                0,
                "the fold product is not a stored key until a write converges storage"
            );

            let settled_row = by_value(vec![7]).perform(&source).try_vec().await?;
            assert_eq!(
                settled_row.len(),
                1,
                "a settled record remains queryable by its value"
            );
            assert_eq!(settled_row[0].of(), &settled);

            Ok(())
        }

        /// The fold product is identical whichever order the forks were
        /// written in: every replica projects the same merged value.
        #[dialog_common::test]
        async fn it_folds_deterministically_across_write_orders() -> anyhow::Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let note = Entity::new()?;

            let mut products = Vec::new();
            for order in [[vec![1u8, 2], vec![2u8, 3]], [vec![2u8, 3], vec![1u8, 2]]] {
                let repo = test_repo(&operator, &profile).await;
                let branch = repo.branch("main").open().perform(&operator).await?;
                for bytes in order {
                    branch
                        .transaction()
                        .assert(record_fact(the!("note/body"), &note, bytes))
                        .commit()
                        .perform(&operator)
                        .await?;
                }

                let query = AttributeQueryOnly::new(
                    Term::from(the!("note/body")),
                    Term::from(note.clone()),
                    Term::var("doc"),
                    Term::var("cause"),
                )
                .with_resolution(Resolution::fold::<ByteSet>());

                let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
                let results = query.perform(&source).try_vec().await?;
                assert_eq!(results.len(), 1);
                products.push(results[0].is().clone());
            }

            assert_eq!(
                products[0], products[1],
                "write order must not change the projected value"
            );

            Ok(())
        }

        /// `Cardinality::Many` never folds: sibling sets are the `All`
        /// path's intended semantics, and `with_resolution` is a no-op on
        /// the `All` variant.
        #[dialog_common::test]
        async fn it_never_folds_cardinality_many() -> anyhow::Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let note = Entity::new()?;
            for bytes in [vec![1, 2], vec![2, 3]] {
                branch
                    .transaction()
                    .assert(record_fact(the!("note/body"), &note, bytes))
                    .commit()
                    .perform(&operator)
                    .await?;
            }

            let query = crate::DynamicAttributeQuery::new(
                Term::from(the!("note/body")),
                Term::from(note.clone()),
                Term::var("doc"),
                Term::var("cause"),
                Some(Cardinality::Many),
            )
            .with_resolution(Resolution::fold::<ByteSet>());

            let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
            let results = query.perform(&source).try_vec().await?;

            assert_eq!(results.len(), 2, "Many yields every sibling, unfolded");

            Ok(())
        }

        /// Physical convergence rides the existing `Replace` write path
        /// (spec §4.4): read the folded document, write it back as a
        /// `Cardinality::One` assertion, and storage collapses to a single
        /// sibling — the merged one.
        #[dialog_common::test]
        async fn it_converges_storage_via_replace_write() -> anyhow::Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let note = Entity::new()?;
            for bytes in [vec![1, 2], vec![2, 3]] {
                branch
                    .transaction()
                    .assert(record_fact(the!("note/body"), &note, bytes))
                    .commit()
                    .perform(&operator)
                    .await?;
            }

            // Read the folded document.
            let query = AttributeQueryOnly::new(
                Term::from(the!("note/body")),
                Term::from(note.clone()),
                Term::var("doc"),
                Term::var("cause"),
            )
            .with_resolution(Resolution::fold::<ByteSet>());
            let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
            let results = query.clone().perform(&source).try_vec().await?;
            assert_eq!(results.len(), 1);
            let merged = results[0].is().clone();

            // Write it back as an ordinary Cardinality::One edit: the
            // `Replace` supersedes every different-valued sibling.
            branch
                .transaction()
                .assert(AttributeStatement {
                    the: the!("note/body"),
                    of: note.clone(),
                    is: merged.clone(),
                    cause: None,
                    cardinality: Some(Cardinality::One),
                })
                .commit()
                .perform(&operator)
                .await?;

            let all = AttributeQueryAll::new(
                Term::from(the!("note/body")),
                Term::from(note.clone()),
                Term::var("doc"),
                Term::var("cause"),
            );
            let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
            let siblings = all.perform(&source).try_vec().await?;

            assert_eq!(siblings.len(), 1, "storage collapsed to one sibling");
            assert_eq!(siblings[0].is(), &merged);

            Ok(())
        }

        /// The resolution strategy never participates in query identity:
        /// it is a pure function of the attribute and its format, which
        /// are already part of attribute identity.
        #[dialog_common::test]
        fn it_keeps_resolution_out_of_identity() {
            let plain = AttributeQueryOnly::new(
                Term::from(the!("note/body")),
                Term::var("note"),
                Term::var("doc"),
                Term::var("cause"),
            );
            let folding = plain
                .clone()
                .with_resolution(Resolution::fold::<ByteSet>());

            assert_eq!(plain, folding, "equality ignores the strategy");
            assert_eq!(
                serde_json::to_string(&plain).unwrap(),
                serde_json::to_string(&folding).unwrap(),
                "serialization ignores the strategy"
            );
        }
    }

    /// When entity is a variable that gets bound by an earlier premise in
    /// the selection, the per-match dispatch should resolve it and use the
    /// sliding window path rather than the challenge path.
    #[dialog_common::test]
    async fn it_uses_sliding_window_when_entity_bound_at_eval_time() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        assert_relation!(branch, &operator, name_attr, alice, "Alice".to_string());
        assert_relation!(branch, &operator, name_attr, alice, "Alicia".to_string());

        // Query with entity as a variable.
        let query = AttributeQueryOnly::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
        );

        // Pre-bind the entity variable in the incoming selection,
        // simulating what would happen when a prior premise binds it.
        let mut seed = Match::new();
        seed.bind(&Term::<Any>::var("person"), Value::Entity(alice.clone()))
            .unwrap();

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = Application::evaluate(query, seed.seed(), &source);
        let results = Selection::try_vec(results).await?;

        assert_eq!(
            results.len(),
            1,
            "Should use sliding window and yield one winner when entity is pre-bound"
        );

        Ok(())
    }
}
