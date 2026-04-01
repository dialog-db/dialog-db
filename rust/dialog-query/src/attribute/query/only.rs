use super::all::AttributeQueryAll;
use crate::Claim;
use crate::artifact::{ArtifactSelector, ArtifactsAttribute, Constrained};
use crate::attribute::The;
use crate::environment::Environment;
use crate::query::Application;
use crate::query::Output;
use crate::schema::Cardinality;
use crate::selection::{Match, Selection};
use crate::source::SelectRules;
use crate::types::{Any, Record};
use crate::{Entity, EvaluationError, Parameters, Schema, Term, try_stream};
use dialog_artifacts::{Artifact, Cause, Select};
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use std::fmt::Display;
use std::fmt::{Formatter, Result as FmtResult};

/// Given two artifacts for the same `(attribute, entity)` pair, return the
/// winner. The winner is the artifact with the higher cause; when causes are
/// equal (including both `None`), the fact hash (`Cause::from`) breaks the tie.
fn choose(current: Artifact, challenger: Artifact) -> Artifact {
    match (&current.cause, &challenger.cause) {
        (Some(a), Some(b)) if a > b => current,
        (Some(a), Some(b)) if a < b => challenger,
        (Some(_), None) => current,
        (None, Some(_)) => challenger,
        _ => {
            // Causes are equal — use the fact hash as a deterministic tiebreaker.
            if Cause::from(&current) >= Cause::from(&challenger) {
                current
            } else {
                challenger
            }
        }
    }
}

/// Winner verification.
///
/// When the entity is unknown, results from the base scan (VAE or AEV) are
/// not guaranteed to contain all competing values for the same
/// `(attribute, entity)` pair. Each candidate is verified by a secondary
/// `(attribute, entity)` lookup to find the true winner. Yields the match
/// only if the candidate matches the winner.
fn challenge<'a, Env>(
    env: &'a Env,
    selector: AttributeQueryAll,
    candidate: Match,
) -> impl Selection + 'a
where
    Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
{
    try_stream! {
        let relation = selector.attribute();
        let attribute = ArtifactsAttribute::try_from(candidate.lookup(&Term::from(&relation))?)?;
        let entity = Entity::try_from(candidate.lookup(&Term::from(selector.of()))?)?;
        let value = candidate.lookup(selector.is())?;
        let cause_term = selector.cause();
        let cause = if cause_term.is_blank() {
            None
        } else {
            Some(Cause::try_from(candidate.lookup(&Term::from(cause_term))?)?)
        };

        let challengers = Provider::<Select<'_>>::execute(env, ArtifactSelector::new()
            .the(attribute)
            .of(entity)).await?;

        let mut winner: Option<Artifact> = None;
        for await each in challengers {
            let challenger = each?;
            winner = Some(match winner {
                None => challenger,
                Some(winner) => choose(winner, challenger),
            });
        }

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
/// Wraps an [`AttributeQueryAll`] and applies winner selection logic so that
/// only one value per `(attribute, entity)` pair is yielded — the one with
/// the highest cause.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub struct AttributeQueryOnly {
    query: AttributeQueryAll,
}

impl AttributeQueryOnly {
    /// Create a new winner-selecting attribute query.
    pub fn new(the: Term<The>, of: Term<Entity>, is: Term<Any>, cause: Term<Cause>) -> Self {
        Self {
            query: AttributeQueryAll::new(the, of, is, cause),
        }
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
    /// - **Sliding window** — entity known (EAV), or attribute known without
    ///   value (AEV). Results are grouped by `(attribute, entity)` so we
    ///   pick the winner in a single pass.
    /// - **Challenge** — value known without entity ({is}, {the, is}, {of, is}
    ///   without entity). Each candidate is verified by a secondary
    ///   `(attribute, entity)` lookup because the scan is not grouped by
    ///   entity or because blanking the value would widen the scan.
    pub fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        env: &'a Env,
        selection: M,
    ) -> impl Selection + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        let selector = self.query;
        try_stream! {
            for await each in selection {
                let base = each?;

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

                    let mut candidate: Option<Artifact> = None;

                    let stream = Provider::<Select<'_>>::execute(env, (&scan).try_into()?).await?;
                    for await artifact in stream {
                        let artifact = artifact?;

                        candidate = Some(match candidate.take() {
                            Some(current) if current.the == artifact.the && current.of == artifact.of => {
                                choose(current, artifact)
                            }
                            Some(winner) => {
                                if value_constraint.is_none() || value_constraint.as_ref() == Some(&winner.is) {
                                    let mut extension = base.clone();
                                    selector.merge(&mut extension, &winner)?;
                                    yield extension;
                                }
                                artifact
                            }
                            None => artifact,
                        });
                    }

                    // Yield the final group's winner.
                    if let Some(winner) = candidate.take()
                        && (value_constraint.is_none() || value_constraint.as_ref() == Some(&winner.is))
                    {
                        let mut extension = base.clone();
                        selector.merge(&mut extension, &winner)?;
                        yield extension;
                    }
                } else {
                    // Secondary lookup path (Box::pin to avoid stack overflow).
                    let candidates = Box::pin(resolved.evaluate(env, base.clone().seed()));
                    for await candidate in candidates {
                        let candidate = candidate?;
                        let verified = Box::pin(challenge(env, selector.clone(), candidate));
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
    use dialog_artifacts::{Artifact, Cause};
    use dialog_repository::helpers::{test_operator, test_repo};
    use std::str::FromStr;

    macro_rules! assert_relation {
        ($branch:expr, $operator:expr, $the:expr, $of:expr, $is:expr) => {{
            $branch
                .edit()
                .assert($the.clone().of($of.clone()).is($is))
                .commit()
                .perform($operator)
                .await
                .unwrap();
        }};
    }

    #[dialog_common::test]
    async fn it_selects_winner_with_constant_entity() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
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
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
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
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
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
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let entity = Entity::new()?;

        // Assert two competing values for the same (attribute, entity) pair.
        branch
            .edit()
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
            .edit()
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

    /// {of, is} — entity + value known, attribute unknown.
    /// The challenge path must reject the loser and accept the winner.
    #[dialog_common::test]
    async fn it_verifies_winner_for_entity_and_value_known() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let entity = Entity::new()?;

        branch
            .edit()
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
            .edit()
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
        let (winner, looser) = if winner_value == crate::Value::String("Alice".into()) {
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

    /// {is} — only value known.
    /// The challenge path must reject the loser and accept the winner.
    #[dialog_common::test]
    async fn it_verifies_winner_for_value_only_known() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let entity = Entity::new()?;

        branch
            .edit()
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
            .edit()
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
        let (winner, looser) = if winner_value == crate::Value::String("Alice".into()) {
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

    #[dialog_common::test]
    async fn choose_prefers_higher_cause() {
        let attr = dialog_artifacts::Attribute::from_str("person/name").unwrap();
        let entity = Entity::new().unwrap();

        let older = Artifact {
            the: attr.clone(),
            of: entity.clone(),
            is: crate::Value::String("Alice".into()),
            cause: Some(Cause([1u8; 32])),
        };

        let newer = Artifact {
            the: attr,
            of: entity,
            is: crate::Value::String("Alicia".into()),
            cause: Some(Cause([2u8; 32])),
        };

        let winner = choose(older.clone(), newer.clone());
        assert_eq!(winner.cause, newer.cause, "Higher cause should win");

        // Reversed argument order should produce the same winner.
        let winner2 = choose(newer.clone(), older.clone());
        assert_eq!(winner2.cause, newer.cause);
    }

    #[dialog_common::test]
    async fn choose_uses_fact_hash_for_equal_causes() {
        let attr = dialog_artifacts::Attribute::from_str("person/name").unwrap();
        let entity = Entity::new().unwrap();

        let a = Artifact {
            the: attr.clone(),
            of: entity.clone(),
            is: crate::Value::String("Alice".into()),
            cause: Some(Cause([1u8; 32])),
        };

        let b = Artifact {
            the: attr,
            of: entity,
            is: crate::Value::String("Alicia".into()),
            cause: Some(Cause([1u8; 32])),
        };

        let winner_ab = choose(a.clone(), b.clone());
        let winner_ba = choose(b.clone(), a.clone());

        // The winner should be deterministic regardless of argument order.
        assert_eq!(
            Cause::from(&winner_ab),
            Cause::from(&winner_ba),
            "Tiebreaker should be deterministic"
        );
    }

    /// When entity is a variable that gets bound by an earlier premise in
    /// the selection, the per-match dispatch should resolve it and use the
    /// sliding window path rather than the challenge path.
    #[dialog_common::test]
    async fn it_uses_sliding_window_when_entity_bound_at_eval_time() -> anyhow::Result<()> {
        use crate::selection::Match;

        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
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
        seed.bind(
            &Term::<Any>::var("person"),
            crate::Value::Entity(alice.clone()),
        )
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
