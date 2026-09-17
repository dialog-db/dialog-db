use super::all::AttributeQueryAll;
use super::pipelined;
use crate::Claim;
use crate::Value;
use crate::artifact::{ArtifactSelector, ArtifactsAttribute, Constrained};
use crate::attribute::The;
use crate::environment::Environment;
use crate::query::Application;
use crate::query::Output;
use crate::schema::Cardinality;
use crate::selection::{Match, Selection};
use crate::type_system::Type as Kind;
use crate::types::{Any, Record};
use crate::{Entity, EvaluationError, Parameters, Schema, Term, try_stream};
use dialog_artifacts::{Artifact, ArtifactView, Cause, DialogArtifactsError, Select};
use dialog_capability::Provider;
use std::fmt::Display;
use std::fmt::{Formatter, Result as FmtResult};

/// Materializes an election winner, treating a corrupt stored row
/// ([`DialogArtifactsError::CorruptEntry`]) as an ignorable non-result
/// (`Ok(None)`, with a warning) rather than a query failure: a corrupt or
/// foreign-written tree entry must not poison every query that ranges over
/// it. All other errors propagate.
fn materialize_winner(winner: ArtifactView) -> Result<Option<Artifact>, DialogArtifactsError> {
    match winner.to_owned() {
        Ok(artifact) => Ok(Some(artifact)),
        Err(DialogArtifactsError::CorruptEntry(reason)) => {
            tracing::warn!(%reason, "ignoring corrupt stored row in election");
            Ok(None)
        }
        Err(error) => Err(error),
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
    Env: crate::Scope<'a>,
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

        let mut winner: Option<ArtifactView> = None;
        for await each in challengers {
            let challenger = each?;
            // The election policy lives with the value layer
            // (`ArtifactView::elect`); this loop only folds rows
            // through it.
            winner = Some(match winner {
                None => challenger,
                Some(winner) => winner.elect(challenger)?,
            });
        }

        // Only the surviving winner decodes its value; the losing
        // challengers never materialized anything.
        if let Some(winner) = winner
            && winner.value()? == value
        {
            let winner_cause = winner.cause().cloned().unwrap_or(Cause([0; 32]));
            if cause.is_none() || cause == Some(winner_cause) {
                yield candidate;
            }
        }
    }
}

/// Winner-selecting attribute query for `Cardinality::One`.
///
/// Wraps an [`AttributeQueryAll`] and yields one value per
/// `(attribute, entity)` pair. Which row survives is not this layer's
/// decision: competing rows fold through [`ArtifactView::elect`], the
/// value layer's cardinality-one election, and the engine encodes no
/// policy of its own.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub struct AttributeQueryOnly {
    query: AttributeQueryAll,
}

impl AttributeQueryOnly {
    /// Create a new winner-selecting attribute query. Scalar like
    /// every associative-layer lookup: zero rows on miss.
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

    /// Return a copy with the `is` term's type narrowed to `kind`.
    /// See [`AttributeQueryAll::with_type`].
    pub(crate) fn with_type(self, kind: Kind) -> Self {
        Self {
            query: self.query.with_type(kind),
        }
    }

    /// See [`AttributeQueryAll::with_subject_kinds`].
    pub(crate) fn with_subject_kinds(self, the: Option<Kind>, of: Option<Kind>) -> Self {
        Self {
            query: self.query.with_subject_kinds(the, of),
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
        let the = self.the().is_bound(env) || self.the().is_ranged();
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
    ///   pick the winner in a single pass.
    /// - **Challenge**: value known without entity ({is}, {the, is}, {of, is}
    ///   without entity). Each candidate is verified by a secondary
    ///   `(attribute, entity)` lookup because the scan is not grouped by
    ///   entity or because blanking the value would widen the scan.
    pub fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        env: &'a Env,
        selection: M,
    ) -> impl Selection + 'a
    where
        Env: crate::Scope<'a>,
    {
        let selector = self.query;
        // Pipeline the probes: while this loop awaits one row's scan, the
        // scans the next rows will issue are offered as preload hints, so
        // a cold replica replicates them concurrently instead of paying
        // one round trip per row (see `super::pipelined`). The hint
        // mirrors the sliding-window path's blanked scan exactly; the
        // challenge path's secondary lookups are not hinted.
        let hinted = selector.clone();
        let selection = pipelined(selection, env, move |base| {
            if hinted.absent_blocked(base) {
                return None;
            }
            let resolved = hinted.resolve(base);
            let entity_known = resolved.of().is_constant();
            let attribute_known = resolved.the().is_constant();
            let value_known = resolved.is().is_constant();
            if entity_known || (attribute_known && !value_known) {
                let scan = AttributeQueryAll::new(
                    resolved.the().clone(),
                    resolved.of().clone(),
                    Term::blank(),
                    resolved.cause().clone(),
                );
                (&scan).try_into().ok()
            } else {
                None
            }
        });
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

                    let mut candidate: Option<ArtifactView> = None;

                    let stream = Provider::<Select<'_>>::execute(env, (&scan).try_into()?).await?;
                    for await artifact in stream {
                        let artifact = artifact?;

                        candidate = Some(match candidate.take() {
                            Some(current) => {
                                // Group membership compares the raw key
                                // bytes; nothing materializes while the
                                // window slides within a group.
                                if current.the_bytes()? == artifact.the_bytes()?
                                    && current.of_bytes()? == artifact.of_bytes()?
                                {
                                    current.elect(artifact)?
                                } else {
                                    // Group closed: only its winner pays
                                    // for materialization, and only if it
                                    // clears the value checks. A winner
                                    // whose stored bytes fail validation
                                    // (`CorruptEntry`) is a corrupt tree
                                    // entry: drop the group's yield rather
                                    // than failing the query.
                                    match materialize_winner(current)? {
                                        Some(winner)
                                            if (value_constraint.is_none()
                                                || value_constraint.as_ref() == Some(&winner.is))
                                                && selector.admits(&winner) =>
                                        {
                                            let mut extension = base.clone();
                                            selector.merge(&mut extension, &winner)?;
                                            yield extension;
                                        }
                                        _ => {}
                                    }
                                    artifact
                                }
                            }
                            None => artifact,
                        });
                    }

                    // Yield the final group's winner (unless its stored
                    // bytes are corrupt; see the group-close arm above).
                    if let Some(winner) = candidate.take() {
                        match materialize_winner(winner)? {
                            Some(winner)
                                if (value_constraint.is_none()
                                    || value_constraint.as_ref() == Some(&winner.is))
                                    && selector.admits(&winner) =>
                            {
                                let mut extension = base.clone();
                                selector.merge(&mut extension, &winner)?;
                                yield extension;
                            }
                            _ => {}
                        }
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
        Env: crate::Scope<'a>,
        Self: Sized,
    {
        Application::perform(self, env)
    }
}

impl Application for AttributeQueryOnly {
    type Conclusion = Claim;

    fn evaluate<'a, Env, M: Selection + 'a>(self, selection: M, env: &'a Env) -> impl Selection + 'a
    where
        Env: crate::Scope<'a>,
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
    use dialog_operator::helpers::{test_operator_with_profile, test_repo};
    use futures_util::TryStreamExt as _;

    macro_rules! assert_relation {
        ($branch:expr, $operator:expr, $the:expr, $of:expr, $is:expr) => {{
            $branch
                .transaction()
                .assert($the.clone().of($of.clone()).is($is))
                .commit()
                .publish()
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

    /// After a merge of two concurrent installs of a seed (one revision
    /// each, one value per name), every name follows the same revision:
    /// the election compares versions, not facts, so a batch resolves
    /// whole in both scan shapes, and the direction follows causal depth,
    /// not which branch pulled.
    #[dialog_common::test]
    async fn it_follows_one_revision_for_every_name_after_a_merge() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let main = repo.branch("main").open().perform(&operator).await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;
        let filler = the!("seed/filler");
        let definition = the!("seed/definition");

        macro_rules! install {
            ($branch:expr, $names:expr, $side:expr) => {{
                let mut transaction = $branch.transaction();
                for (index, name) in $names.iter().enumerate() {
                    transaction = transaction.assert(
                        definition
                            .clone()
                            .of(name.clone())
                            .is(format!("{}:{index}", $side)),
                    );
                }
                transaction.commit().publish().perform(&operator).await?;
            }};
        }

        // Which revision every name of a seed resolved to, per scan shape.
        macro_rules! winners {
            ($branch:expr, $names:expr) => {{
                let source = TestEnv::new(&$branch, &operator, RuleRegistry::new());
                let mut by_entity = Vec::new();
                for name in $names.iter() {
                    let query = AttributeQueryOnly::new(
                        Term::from(definition.clone()),
                        Term::from(name.clone()),
                        Term::var("value"),
                        Term::var("cause"),
                    );
                    let results = query.perform(&source).try_vec().await?;
                    assert_eq!(results.len(), 1, "one winner per name");
                    by_entity.push(side_of(results[0].is()));
                }
                let query = AttributeQueryOnly::new(
                    Term::from(definition.clone()),
                    Term::var("name"),
                    Term::var("value"),
                    Term::var("cause"),
                );
                let by_attribute: Vec<_> = query
                    .perform(&source)
                    .try_vec()
                    .await?
                    .iter()
                    .filter(|result| $names.contains(result.of()))
                    .map(|result| side_of(result.is()))
                    .collect();
                assert_eq!(by_attribute.len(), $names.len(), "one winner per name");
                (by_entity, by_attribute)
            }};
        }

        fn side_of(value: &Value) -> String {
            let Value::String(value) = value else {
                panic!("seed values are strings, got {value:?}");
            };
            value.split(':').next().expect("side:index").to_string()
        }

        // A shared base, so the pulls below are merges.
        assert_relation!(main, &operator, filler, Entity::new()?, "base".to_string());
        feature.set_upstream(&main).perform(&operator).await?;
        feature.pull().perform(&operator).await?;

        // main goes one revision deeper before installing its seed;
        // feature installs its own straight away.
        let first: Vec<Entity> = (0..6).map(|_| Entity::new()).collect::<Result<_, _>>()?;
        assert_relation!(
            main,
            &operator,
            filler,
            Entity::new()?,
            "deeper".to_string()
        );
        install!(main, first, "main");
        install!(feature, first, "feature");
        feature.pull().perform(&operator).await?;

        let (by_entity, by_attribute) = winners!(feature, first);
        assert_eq!(
            by_entity,
            vec!["main"; 6],
            "the deeper revision wins every name"
        );
        assert_eq!(by_attribute, by_entity, "both scan shapes agree");

        // Now feature is the deeper writer of a second seed.
        let second: Vec<Entity> = (0..6).map(|_| Entity::new()).collect::<Result<_, _>>()?;
        assert_relation!(
            feature,
            &operator,
            filler,
            Entity::new()?,
            "deeper".to_string()
        );
        install!(feature, second, "feature");
        install!(main, second, "main");
        feature.pull().perform(&operator).await?;

        let (by_entity, by_attribute) = winners!(feature, second);
        assert_eq!(
            by_entity,
            vec!["feature"; 6],
            "depth decides, not the branch"
        );
        assert_eq!(by_attribute, by_entity, "both scan shapes agree");

        // A winner stays won: main, which beat feature's install of the
        // first seed, revises half of that seed without ever seeing
        // feature's rows. Feature's rows for those names are still live
        // on feature, so they contest the revision as they contested the
        // install, and lose to it the same way. The untouched half keeps
        // the outcome it had, so no name of the seed changes sides.
        let mut revision = main.transaction();
        for (index, name) in first.iter().take(3).enumerate() {
            revision = revision
                .retract(
                    definition
                        .clone()
                        .of(name.clone())
                        .is(format!("main:{index}")),
                )
                .assert(
                    definition
                        .clone()
                        .of(name.clone())
                        .is(format!("main:{}", index + 10)),
                );
        }
        revision.commit().publish().perform(&operator).await?;
        feature.pull().perform(&operator).await?;

        let (by_entity, by_attribute) = winners!(feature, first);
        assert_eq!(
            by_entity,
            vec!["main"; 6],
            "a revision built on the winner beats what the winner beat"
        );
        assert_eq!(by_attribute, by_entity, "both scan shapes agree");
        let source = TestEnv::new(&feature, &operator, RuleRegistry::new());
        let revised = AttributeQueryOnly::new(
            Term::from(definition.clone()),
            Term::from(first[0].clone()),
            Term::var("value"),
            Term::var("cause"),
        )
        .perform(&source)
        .try_vec()
        .await?;
        assert_eq!(
            revised[0].is(),
            &Value::String("main:10".into()),
            "the revised names carry the revision's value"
        );

        Ok(())
    }

    /// The case the version election leaves open, shown rather than
    /// argued (dialog-db/dialog-db#513, second half).
    ///
    /// Two branches install the same six-name seed concurrently. Main's
    /// install is the deeper revision, so every name follows main once
    /// the two meet. But feature, still without having seen main's
    /// rows, revises half of its own seed in a later commit. That
    /// revision is deeper than main's install, so for the three names
    /// it touched it wins, while the three it left alone still belong
    /// to main's install: feature's lineage lost the contest for the
    /// batch and then overtook it for part of the batch. A reader gets a
    /// seed stitched from both installs, on every replica alike.
    ///
    /// A per-row election cannot prevent this. For an untouched name it
    /// sees only the two installs; for a revised name it does not know
    /// the siblings exist. It also takes a lineage that lost and then
    /// grew deeper than the winner before the two met, which needs a
    /// long-used branch installing concurrently with a deeper one: a
    /// fresh branch meeting an established one never gets there. The
    /// writer closes it from its side: a revision that re-asserts the
    /// whole batch, unchanged names included, lifts every name to its
    /// version, since a row stands at its deepest collapsed claim. This
    /// test pins the mixed outcome so a change in either direction is a
    /// visible decision, not an accident.
    #[dialog_common::test]
    async fn it_still_mixes_seeds_when_a_losing_lineage_revises_part_of_its_batch()
    -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let main = repo.branch("main").open().perform(&operator).await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;
        let filler = the!("seed/filler");
        let definition = the!("seed/definition");

        macro_rules! install {
            ($branch:expr, $names:expr, $side:expr) => {{
                let mut transaction = $branch.transaction();
                for (index, name) in $names.iter().enumerate() {
                    transaction = transaction.assert(
                        definition
                            .clone()
                            .of(name.clone())
                            .is(format!("{}:{index}", $side)),
                    );
                }
                transaction.commit().publish().perform(&operator).await?;
            }};
        }

        macro_rules! winners {
            ($branch:expr, $names:expr) => {{
                let source = TestEnv::new(&$branch, &operator, RuleRegistry::new());
                let mut by_entity = Vec::new();
                for name in $names.iter() {
                    let query = AttributeQueryOnly::new(
                        Term::from(definition.clone()),
                        Term::from(name.clone()),
                        Term::var("value"),
                        Term::var("cause"),
                    );
                    let results = query.perform(&source).try_vec().await?;
                    assert_eq!(results.len(), 1, "one winner per name");
                    by_entity.push(side_of(results[0].is()));
                }
                let query = AttributeQueryOnly::new(
                    Term::from(definition.clone()),
                    Term::var("name"),
                    Term::var("value"),
                    Term::var("cause"),
                );
                let mut by_attribute: Vec<(Entity, String)> = query
                    .perform(&source)
                    .try_vec()
                    .await?
                    .iter()
                    .filter(|result| $names.contains(result.of()))
                    .map(|result| (result.of().clone(), side_of(result.is())))
                    .collect();
                by_attribute.sort_by_key(|(entity, _)| {
                    $names
                        .iter()
                        .position(|name| name == entity)
                        .expect("a seed name")
                });
                let by_attribute: Vec<String> =
                    by_attribute.into_iter().map(|(_, side)| side).collect();
                (by_entity, by_attribute)
            }};
        }

        fn side_of(value: &Value) -> String {
            let Value::String(value) = value else {
                panic!("seed values are strings, got {value:?}");
            };
            value.split(':').next().expect("side:index").to_string()
        }

        // A shared base, so the pulls below are merges.
        assert_relation!(main, &operator, filler, Entity::new()?, "base".to_string());
        feature.set_upstream(&main).perform(&operator).await?;
        feature.pull().perform(&operator).await?;

        // Concurrent installs: main goes one revision deeper first, so
        // its install outranks feature's.
        let names: Vec<Entity> = (0..6).map(|_| Entity::new()).collect::<Result<_, _>>()?;
        assert_relation!(
            main,
            &operator,
            filler,
            Entity::new()?,
            "deeper".to_string()
        );
        install!(main, names, "main");
        install!(feature, names, "feature");

        // Feature, still unaware of main's install, goes deeper than it
        // and revises half of its own seed: it retracts the values it
        // wrote and asserts new ones for the first three names.
        assert_relation!(
            feature,
            &operator,
            filler,
            Entity::new()?,
            "deeper still".to_string()
        );
        let mut revision = feature.transaction();
        for (index, name) in names.iter().take(3).enumerate() {
            revision = revision
                .retract(
                    definition
                        .clone()
                        .of(name.clone())
                        .is(format!("feature:{index}")),
                )
                .assert(
                    definition
                        .clone()
                        .of(name.clone())
                        .is(format!("feature:{}", index + 10)),
                );
        }
        revision.commit().publish().perform(&operator).await?;

        // Now the two meet, in both directions.
        feature.pull().perform(&operator).await?;
        main.pull().from(&feature).perform(&operator).await?;

        let mixed = vec!["feature", "feature", "feature", "main", "main", "main"];
        for branch in [&feature, &main] {
            let (by_entity, by_attribute) = winners!(branch, names);
            assert_eq!(
                by_entity, mixed,
                "the revised names follow feature's deeper revision, the rest main's install"
            );
            assert_eq!(by_attribute, by_entity, "both scan shapes agree");
        }

        Ok(())
    }

    /// A revision that re-asserts a fact already live records its own
    /// version on the row beside the earlier claim's, so a batch that
    /// re-asserts every name, unchanged ones included, lifts every name
    /// to its version. That is the writer's tool for keeping a seed
    /// whole under the election, which stands a row at its deepest
    /// collapsed claim.
    #[dialog_common::test]
    async fn it_records_a_re_asserted_claim_under_the_new_revision() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let main = repo.branch("main").open().perform(&operator).await?;
        let definition = the!("seed/definition");
        let name = Entity::new()?;
        for _ in 0..2 {
            main.transaction()
                .assert(definition.clone().of(name.clone()).is("v".to_string()))
                .commit()
                .publish()
                .perform(&operator)
                .await?;
        }
        let source = TestEnv::new(&main, &operator, RuleRegistry::new());
        let rows: Vec<ArtifactView> =
            Provider::<Select<'_>>::execute(&source, ArtifactSelector::new().of(name.clone()))
                .await?
                .try_collect()
                .await?;
        let editions: Vec<u64> = rows
            .iter()
            .flat_map(|row| row.versions().map(|version| version.edition.into()))
            .collect();
        assert_eq!(rows.len(), 1, "one live row for the fact");
        assert_eq!(
            editions.len(),
            2,
            "both revisions stand on the row: {editions:?}"
        );
        assert_ne!(editions[0], editions[1], "two distinct editions");
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
            .publish()
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
            .publish()
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
            .publish()
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
            .publish()
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
            .publish()
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
            .publish()
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
