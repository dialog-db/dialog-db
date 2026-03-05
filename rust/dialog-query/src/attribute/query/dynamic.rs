use crate::Cardinality;
use crate::Claim;
use crate::artifact::{ArtifactSelector, ArtifactsAttribute, Constrained};
use crate::attribute::The;
use crate::environment::Environment;
use crate::negation::Negation;
use crate::proposition::Proposition;
use crate::query::Application;
use crate::query::Output;
use crate::selection::{Match, Selection};
use crate::types::{Any, Record};
use crate::{Entity, EvaluationError, Parameters, Premise, Schema, Source, Term};
use dialog_artifacts::Cause;
use futures_util::future::Either;
use serde::Serialize;
use std::fmt::Display;
use std::fmt::{Formatter, Result as FmtResult};
use std::ops::Not;

use super::all::AttributeQueryAll;
use super::only::AttributeQueryOnly;

/// Type-erased attribute query that dispatches between cardinality variants.
///
/// `All` yields every matching artifact (Cardinality::Many semantics).
/// `Only` yields one winner per `(attribute, entity)` pair (Cardinality::One).
///
/// When constructed with `cardinality: None` or `Some(Cardinality::Many)`,
/// the `All` variant is used. `Some(Cardinality::One)` selects `Only`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum DynamicAttributeQuery {
    /// Yield all matching artifacts.
    All(AttributeQueryAll),
    /// Yield only the winning artifact per `(attribute, entity)`.
    Only(AttributeQueryOnly),
}

impl DynamicAttributeQuery {
    /// Create a new attribute query with the given cardinality.
    ///
    /// `None` or `Some(Cardinality::Many)` → `All` variant.
    /// `Some(Cardinality::One)` → `Only` variant.
    pub fn new(
        the: Term<The>,
        of: Term<Entity>,
        is: Term<Any>,
        cause: Term<Cause>,
        cardinality: Option<Cardinality>,
    ) -> Self {
        match cardinality {
            Some(Cardinality::One) => {
                DynamicAttributeQuery::Only(AttributeQueryOnly::new(the, of, is, cause))
            }
            _ => DynamicAttributeQuery::All(AttributeQueryAll::new(the, of, is, cause)),
        }
    }

    /// Get the 'the' (attribute) term.
    pub fn the(&self) -> &Term<The> {
        match self {
            DynamicAttributeQuery::All(q) => q.the(),
            DynamicAttributeQuery::Only(q) => q.the(),
        }
    }

    /// Get the 'of' (entity) term.
    pub fn of(&self) -> &Term<Entity> {
        match self {
            DynamicAttributeQuery::All(q) => q.of(),
            DynamicAttributeQuery::Only(q) => q.of(),
        }
    }

    /// Get the 'is' (value) parameter.
    pub fn is(&self) -> &Term<Any> {
        match self {
            DynamicAttributeQuery::All(q) => q.is(),
            DynamicAttributeQuery::Only(q) => q.is(),
        }
    }

    /// Get the 'cause' term.
    pub fn cause(&self) -> &Term<Cause> {
        match self {
            DynamicAttributeQuery::All(q) => q.cause(),
            DynamicAttributeQuery::Only(q) => q.cause(),
        }
    }

    /// Get the source term (internal claim handle).
    pub fn source(&self) -> &Term<Record> {
        match self {
            DynamicAttributeQuery::All(q) => q.source(),
            DynamicAttributeQuery::Only(q) => q.source(),
        }
    }

    /// Map `Term<The>` to `Term<ArtifactsAttribute>`.
    pub fn attribute(&self) -> Term<ArtifactsAttribute> {
        match self {
            DynamicAttributeQuery::All(q) => q.attribute(),
            DynamicAttributeQuery::Only(q) => q.attribute(),
        }
    }

    /// Get the cardinality of this query.
    pub fn cardinality(&self) -> Cardinality {
        match self {
            DynamicAttributeQuery::All(_) => Cardinality::Many,
            DynamicAttributeQuery::Only(_) => Cardinality::One,
        }
    }

    /// Returns the schema describing this application's parameters.
    pub fn schema(&self) -> Schema {
        match self {
            DynamicAttributeQuery::All(q) => q.schema(),
            DynamicAttributeQuery::Only(q) => q.schema(),
        }
    }

    /// Estimate cost based on how many parameters are constrained and cardinality.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        match self {
            DynamicAttributeQuery::All(q) => q.estimate(env),
            DynamicAttributeQuery::Only(q) => q.estimate(env),
        }
    }

    /// Returns the parameters for this query.
    pub fn parameters(&self) -> Parameters {
        match self {
            DynamicAttributeQuery::All(q) => q.parameters(),
            DynamicAttributeQuery::Only(q) => q.parameters(),
        }
    }

    /// Evaluate, dispatching to the appropriate cardinality variant.
    pub fn evaluate<S: Source, M: Selection>(self, source: S, selection: M) -> impl Selection {
        match self {
            DynamicAttributeQuery::All(query) => Either::Left(query.evaluate(source, selection)),
            DynamicAttributeQuery::Only(query) => Either::Right(query.evaluate(source, selection)),
        }
    }

    /// Execute this query, returning a stream of claims.
    pub fn perform<S: Source>(self, source: &S) -> impl Output<Claim>
    where
        Self: Sized,
    {
        Application::perform(self, source)
    }
}

impl Application for DynamicAttributeQuery {
    type Conclusion = Claim;

    fn evaluate<S: Source, M: Selection>(self, selection: M, source: &S) -> impl Selection {
        self.evaluate(source.clone(), selection)
    }

    fn realize(&self, input: Match) -> Result<Claim, EvaluationError> {
        match self {
            DynamicAttributeQuery::All(q) => q.realize(input),
            DynamicAttributeQuery::Only(q) => q.realize(input),
        }
    }
}

impl TryFrom<&DynamicAttributeQuery> for ArtifactSelector<Constrained> {
    type Error = EvaluationError;

    fn try_from(from: &DynamicAttributeQuery) -> Result<Self, Self::Error> {
        match from {
            DynamicAttributeQuery::All(q) => ArtifactSelector::try_from(q),
            DynamicAttributeQuery::Only(q) => ArtifactSelector::try_from(q),
        }
    }
}

impl Display for DynamicAttributeQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            DynamicAttributeQuery::All(q) => Display::fmt(q, f),
            DynamicAttributeQuery::Only(q) => Display::fmt(q, f),
        }
    }
}

impl Not for DynamicAttributeQuery {
    type Output = Premise;

    fn not(self) -> Self::Output {
        Premise::Unless(Negation::not(Proposition::Attribute(Box::new(self))))
    }
}

impl From<DynamicAttributeQuery> for Proposition {
    fn from(query: DynamicAttributeQuery) -> Self {
        Proposition::Attribute(Box::new(query))
    }
}

impl From<DynamicAttributeQuery> for Premise {
    fn from(query: DynamicAttributeQuery) -> Self {
        Premise::Assert(Proposition::Attribute(Box::new(query)))
    }
}

impl From<&DynamicAttributeQuery> for Premise {
    fn from(query: &DynamicAttributeQuery) -> Self {
        Premise::Assert(Proposition::Attribute(Box::new(query.clone())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Output;
    use crate::selection::{Match, Selection};
    use crate::{Session, the};
    use dialog_storage::MemoryStorageBackend;

    #[dialog_common::test]
    async fn it_evaluates() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::the;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        let claims = vec![name_attr.clone().of(alice.clone()).is("Alice".to_string())];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let query = DynamicAttributeQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::Many),
        );

        let session = Session::open(artifacts);
        let selection = Application::evaluate(query, Match::new().seed(), &session);

        let results = Selection::try_vec(selection).await?;

        assert_eq!(results.len(), 1);

        let candidate = &results[0];

        assert!(candidate.contains(&Term::var("person")));
        assert!(candidate.contains(&Term::var("name")));

        let person_id: Entity = Entity::try_from(candidate.lookup(&Term::var("person"))?)?;
        let name_value: crate::Value = candidate.lookup(&Term::var("name"))?;

        assert_eq!(person_id, alice);
        assert_eq!(name_value, crate::Value::String("Alice".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_single_value_for_cardinality_one() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::the;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        let mut session = Session::open(artifacts.clone());
        session
            .transact(vec![
                name_attr.clone().of(alice.clone()).is("Alice".to_string()),
            ])
            .await?;

        let mut session = Session::open(artifacts.clone());
        session
            .transact(vec![
                name_attr.clone().of(alice.clone()).is("Alicia".to_string()),
            ])
            .await?;

        let query = DynamicAttributeQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let session = Session::open(artifacts.clone());
        let results = query.perform(&session).try_vec().await?;

        assert_eq!(
            results.len(),
            1,
            "Cardinality::One should return only one value per entity-attribute pair, got {}",
            results.len()
        );

        let query_many = DynamicAttributeQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::Many),
        );

        let session = Session::open(artifacts);
        let results_many = query_many.perform(&session).try_vec().await?;

        assert_eq!(
            results_many.len(),
            2,
            "Cardinality::Many should return all values, got {}",
            results_many.len()
        );

        Ok(())
    }

    macro_rules! assert_relation {
        ($artifacts:expr, $the:expr, $of:expr, $is:expr) => {{
            let mut session = Session::open($artifacts.clone());
            let mut tx = session.edit();
            tx.assert($the.clone().of($of.clone()).is($is));
            session.commit(tx).await.unwrap();
        }};
    }

    #[dialog_common::test]
    async fn it_selects_winner_via_eav_scan() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");
        let age_attr = the!("person/age");

        assert_relation!(artifacts, name_attr, alice, "Alice".to_string());
        assert_relation!(artifacts, name_attr, alice, "Alicia".to_string());
        assert_relation!(artifacts, age_attr, alice, 30i64);

        let query = DynamicAttributeQuery::new(
            Term::var("the"),
            Term::from(alice.clone()),
            Term::var("value"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let session = Session::open(artifacts);
        let results = query.perform(&session).try_vec().await?;

        assert_eq!(
            results.len(),
            2,
            "EAV scan with Cardinality::One should return one value per attribute, got {}",
            results.len()
        );

        let name_results: Vec<_> = results.iter().filter(|f| *f.the() == name_attr).collect();
        let age_results: Vec<_> = results.iter().filter(|f| *f.the() == age_attr).collect();

        assert_eq!(name_results.len(), 1, "Should have exactly one name result");
        assert_eq!(age_results.len(), 1, "Should have exactly one age result");
        assert_eq!(age_results[0].is(), &crate::Value::SignedInt(30));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_selects_winner_via_aev_scan() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let name_attr = the!("person/name");

        assert_relation!(artifacts, name_attr, alice, "Alice".to_string());
        assert_relation!(artifacts, name_attr, alice, "Alicia".to_string());
        assert_relation!(artifacts, name_attr, bob, "Bob".to_string());
        assert_relation!(artifacts, name_attr, bob, "Robert".to_string());

        let query = DynamicAttributeQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let session = Session::open(artifacts);
        let results = query.perform(&session).try_vec().await?;

        assert_eq!(
            results.len(),
            2,
            "AEV scan with Cardinality::One should return one value per entity, got {}",
            results.len()
        );

        let alice_results: Vec<_> = results.iter().filter(|f| f.of() == &alice).collect();
        let bob_results: Vec<_> = results.iter().filter(|f| f.of() == &bob).collect();

        assert_eq!(
            alice_results.len(),
            1,
            "Should have exactly one alice result"
        );
        assert_eq!(bob_results.len(), 1, "Should have exactly one bob result");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_selects_winner_via_vae_scan() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        assert_relation!(artifacts, name_attr, alice, "Alice".to_string());
        assert_relation!(artifacts, name_attr, alice, "Alicia".to_string());

        let aev_query = DynamicAttributeQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let session = Session::open(artifacts.clone());
        let aev_results = aev_query.perform(&session).try_vec().await?;
        assert_eq!(aev_results.len(), 1);
        let expected_winner_value = aev_results[0].is().clone();

        let vae_query = DynamicAttributeQuery::new(
            Term::var("the"),
            Term::var("person"),
            Term::Constant(expected_winner_value.clone()),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let session = Session::open(artifacts.clone());
        let vae_results = vae_query.perform(&session).try_vec().await?;

        assert_eq!(
            vae_results.len(),
            1,
            "VAE scan should return the winner after secondary lookup, got {}",
            vae_results.len()
        );
        assert_eq!(vae_results[0].is(), &expected_winner_value);

        let losing_value = if expected_winner_value == crate::Value::String("Alice".into()) {
            crate::Value::String("Alicia".into())
        } else {
            crate::Value::String("Alice".into())
        };

        let vae_loser_query = DynamicAttributeQuery::new(
            Term::var("the"),
            Term::var("person"),
            Term::Constant(losing_value),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let session = Session::open(artifacts);
        let vae_loser_results = vae_loser_query.perform(&session).try_vec().await?;

        assert_eq!(
            vae_loser_results.len(),
            0,
            "VAE scan for the losing value should return nothing, got {}",
            vae_loser_results.len()
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_picks_deterministic_winner() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        assert_relation!(artifacts, name_attr, alice, "Alice".to_string());
        assert_relation!(artifacts, name_attr, alice, "Alicia".to_string());

        let eav_query = DynamicAttributeQuery::new(
            Term::var("the"),
            Term::from(alice.clone()),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let session = Session::open(artifacts.clone());
        let eav_results = eav_query.perform(&session).try_vec().await?;
        let eav_name_results: Vec<_> = eav_results
            .iter()
            .filter(|f| *f.the() == name_attr)
            .collect();
        assert_eq!(eav_name_results.len(), 1);
        let eav_winner = eav_name_results[0].is().clone();

        let aev_query = DynamicAttributeQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let session = Session::open(artifacts.clone());
        let aev_results = aev_query.perform(&session).try_vec().await?;
        let aev_alice: Vec<_> = aev_results.iter().filter(|f| f.of() == &alice).collect();
        assert_eq!(aev_alice.len(), 1);
        let aev_winner = aev_alice[0].is().clone();

        assert_eq!(
            eav_winner, aev_winner,
            "EAV and AEV paths should pick the same winner"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_queries_from_the() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        let claims = vec![name_attr.of(alice.clone()).is("Alice".to_string())];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let query = DynamicAttributeQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            None,
        );

        assert_eq!(query.the(), &Term::from(the!("person/name")));

        let session = Session::open(artifacts);
        let results = query.perform(&session).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].of(), &alice);
        assert_eq!(results[0].is(), &crate::Value::String("Alice".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_executes_query() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;
        use crate::the;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("person/name");

        let claims = vec![name_attr.clone().of(alice.clone()).is("Alice".to_string())];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let query = DynamicAttributeQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::Many),
        );

        let session = Session::open(artifacts);
        let results = query.perform(&session).try_vec().await?;

        assert_eq!(results.len(), 1);
        let fact = &results[0];
        assert_eq!(fact.the(), &name_attr);
        assert_eq!(fact.of(), &alice);
        assert_eq!(fact.is(), &crate::Value::String("Alice".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_facts() -> anyhow::Result<()> {
        use crate::Statement;
        use crate::artifact::Artifacts;
        use crate::attribute::AttributeStatement;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;

        let alice_name: AttributeStatement = the!("user/name")
            .of(alice.clone())
            .is("Alice".to_string())
            .into();

        let mut session = Session::open(artifacts.clone());
        session.transact(vec![alice_name.clone()]).await?;

        let query = DynamicAttributeQuery::new(
            Term::from(the!("user/name")),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let results = query
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].of(), &alice);
        assert_eq!(results[0].is(), &crate::Value::String("Alice".to_string()));

        let mut session = Session::open(artifacts.clone());
        session.transact([alice_name.revert()]).await?;

        let query2 = DynamicAttributeQuery::new(
            Term::from(the!("user/name")),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let results2 = query2
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(results2.len(), 0, "Fact should be retracted");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_mixes_constants_and_variables() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let alice = Entity::new()?;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let claims = vec![the!("user/name").of(alice.clone()).is("Alice".to_string())];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let query = DynamicAttributeQuery::new(
            Term::from(the!("user/name")),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let results = query
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(results.len(), 1, "Should find Alice's name fact");
        assert_eq!(results[0].domain(), "user");
        assert_eq!(results[0].name(), "name");
        assert_eq!(results[0].of(), &alice);
        assert_eq!(results[0].is(), &crate::Value::String("Alice".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_queries_without_descriptor() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let claims = vec![
            the!("user/name").of(alice.clone()).is("Alice".to_string()),
            the!("user/name").of(bob.clone()).is("Bob".to_string()),
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let query = DynamicAttributeQuery::new(
            Term::from(the!("user/name")),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let results = query
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(results.len(), 2, "Should find both Alice and Bob");

        let has_alice = results
            .iter()
            .any(|f| f.of == alice && f.is == crate::Value::String("Alice".to_string()));
        let has_bob = results
            .iter()
            .any(|f| f.of == bob && f.is == crate::Value::String("Bob".to_string()));
        assert!(has_alice && has_bob);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_accepts_string_literal_as_value_term() -> anyhow::Result<()> {
        use crate::artifact::Artifacts;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;

        let claims = vec![the!("user/name").of(alice.clone()).is("Alice".to_string())];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let query = DynamicAttributeQuery::new(
            Term::from(the!("user/name")),
            alice.clone().into(),
            Term::constant("Alice".to_string()),
            Term::blank(),
            None,
        );

        let results = query
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;
        assert_eq!(results.len(), 1);

        Ok(())
    }
}
