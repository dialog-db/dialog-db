//! Succession of a claim under a choosing policy, resolved at commit.
//!
//! A write through an attribute read under `last`, `max`, `min` or
//! `top` succeeds the claim the attribute stands for: the candidate a
//! read under the policy returns. Which candidate that is depends on
//! the line and on the rules deriving the relation, so the statement
//! records a [`Succession`] and the commit resolves it here, before
//! induction sees the batch: the attribute is read for the entity
//! through the same view a mid-transaction query has, under the policy,
//! without the written value, and the stored claim holding the elected
//! value, if one does, is retracted beside the assertion. A candidate a
//! rule derives is not a claim: when the read elects a derived value
//! nothing is retracted, and the write stands beside it as one more
//! candidate. A write under `all` appends and succeeds nothing.

use crate::repository::branch::QueryLayer;
use crate::repository::branch::session::QueryEnv;
use crate::repository::source::SourceRef;
use crate::{CommitError, RemoteSite, Staged};
use dialog_artifacts::{ArtifactSelector, Changes, Select, Succession, Update, Value};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::memory::Resolve;
use dialog_query::attribute::{AttributeDescriptor, Relation, The};
use dialog_query::query::Output as _;
use dialog_query::types::Any;
use dialog_query::{
    Binding, Cardinality, ConceptDescriptor, ConceptFieldDescriptor, ConceptQuery, Match,
    Parameters, Term,
};
use futures_util::TryStreamExt;
use std::fmt::Display;

/// Resolve every succession `changes` holds against `source`, read
/// through the batch itself, into the retraction of the stored claim it
/// succeeds, if any, and the assertion of its value.
pub(crate) async fn resolve<Env>(
    source: SourceRef<'_>,
    changes: &mut Changes,
    env: &Env,
) -> Result<(), CommitError>
where
    Env: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Identify>
        + Provider<crate::Hydrate>
        + Provider<dialog_artifacts::Preload>
        + Provider<dialog_artifacts::Speculation>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let successions = changes.take_successions();
    if successions.is_empty() {
        return Ok(());
    }
    let operator = Identify.perform(env).await?;
    // The view reads the batch without its successions, so the election
    // is over what the write observed, never over itself.
    let view = QueryEnv::new(
        vec![source.to_source()],
        QueryLayer::from(source).overlay(&operator),
        env,
    )
    .with_layers(vec![Staged::from(changes.clone())]);
    resolve_in(&view, successions, changes).await
}

/// Resolve every succession `changes` holds against `view`, which must
/// not hold the successions' own values: what an inductive rule's head
/// writes resolves against the round view the rule fired on.
pub(crate) async fn resolve_against<Env>(
    view: &QueryEnv<'_, Env>,
    changes: &mut Changes,
) -> Result<(), CommitError>
where
    Env: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<crate::Hydrate>
        + Provider<dialog_artifacts::Preload>
        + Provider<dialog_artifacts::Speculation>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let successions = changes.take_successions();
    if successions.is_empty() {
        return Ok(());
    }
    resolve_in(view, successions, changes).await
}

/// The resolution itself: each succession becomes the retraction of
/// the stored claim holding the value `view` elects for its cell, if
/// one does, and the assertion of its own value, both written to
/// `changes`.
async fn resolve_in<Env>(
    view: &QueryEnv<'_, Env>,
    successions: Vec<(
        dialog_artifacts::Attribute,
        dialog_artifacts::Entity,
        Value,
        Succession,
    )>,
    changes: &mut Changes,
) -> Result<(), CommitError>
where
    Env: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<crate::Hydrate>
        + Provider<dialog_artifacts::Preload>
        + Provider<dialog_artifacts::Speculation>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let failed = |error: &dyn Display| CommitError::Succession(error.to_string());

    for (the, of, is, succession) in successions {
        // The attribute read under the policy, for this entity: what a
        // read returns, stored or derived.
        let (select, among) = match &succession {
            Succession::Last => (dialog_query::Select::Last, Vec::new()),
            Succession::Max => (dialog_query::Select::Max, Vec::new()),
            Succession::Min => (dialog_query::Select::Min, Vec::new()),
            Succession::Top(among) => (dialog_query::Select::Top, among.clone()),
        };
        let attribute = AttributeDescriptor::over(
            Relation::Attribute(The::from(the.clone())),
            "",
            Cardinality::One,
            None,
        )
        .with_select(select, among);
        let predicate =
            ConceptDescriptor::of_attribute(&ConceptFieldDescriptor::required(attribute));
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::constant(of.clone()));
        terms.insert(
            ConceptDescriptor::VALUE.to_string(),
            Term::<Any>::var(ConceptDescriptor::VALUE),
        );
        let rows: Vec<Match> = ConceptQuery { terms, predicate }
            .evaluate(Match::new().seed(), view)
            .try_vec()
            .await
            .map_err(|error| failed(&error))?;
        let elected: Option<Value> =
            rows.into_iter()
                .find_map(|row| match row.get(ConceptDescriptor::VALUE) {
                    Some(Binding::Present(value)) => Some(value.clone()),
                    _ => None,
                });

        // The elected value is succeeded where a stored claim holds it:
        // a derived candidate is not a claim and stays.
        if let Some(elected) = elected
            && elected != is
        {
            let selector = ArtifactSelector::new()
                .the(the.clone())
                .of(of.clone())
                .is(elected.clone());
            let held = Provider::<Select<'_>>::execute(view, selector)
                .await
                .map_err(|error| failed(&error))?
                .try_next()
                .await
                .map_err(|error| failed(&error))?
                .is_some();
            if held {
                changes.dissociate(the.clone(), of.clone(), elected);
            }
        }
        changes.associate(the, of, is);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::Branch;
    use crate::helpers::test_repo;
    use anyhow::Result;
    use dialog_artifacts::{ArtifactSelector, Attribute, Entity, Succession, Value};
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::attribute::The;
    use dialog_query::query::Output as _;
    use dialog_query::rule::DeductiveRuleDescriptor;
    use dialog_query::types::Any;
    use dialog_query::{
        AttributeStatement, Cardinality, ConceptConclusion, ConceptDescriptor, ConceptQuery,
        DeductiveRule, Parameters, Term,
    };
    use dialog_storage::provider::storage::VolatileSpace;
    use futures_util::StreamExt as _;

    /// A write of `org/salary` under `max`: the statement a field
    /// reading the relation under that policy writes.
    fn salary(of: &Entity, value: u32) -> AttributeStatement {
        AttributeStatement {
            the: The::from("org/salary".parse::<Attribute>().expect("an attribute")),
            of: of.clone(),
            is: Value::UnsignedInt(value.into()),
            cause: None,
            cardinality: Some(Cardinality::One),
            succession: Some(Succession::Max),
        }
    }

    /// The live claims of `of`'s `org/salary` cell, sorted.
    async fn stored(
        branch: &Branch,
        operator: &dialog_peer::Peer<VolatileSpace, dialog_peer::Session>,
        of: &Entity,
    ) -> Result<Vec<u128>> {
        let selector = ArtifactSelector::new()
            .the("org/salary".parse()?)
            .of(of.clone());
        let stream = branch.claims().select(selector).perform(operator).await?;
        let mut values: Vec<u128> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|item| item.and_then(|view| view.to_owned()))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter_map(|artifact| match artifact.is {
                Value::UnsignedInt(value) => Some(value),
                _ => None,
            })
            .collect();
        values.sort();
        Ok(values)
    }

    /// `org/salary` read under `max` for `of`.
    async fn read_max(
        branch: &Branch,
        operator: &dialog_peer::Peer<VolatileSpace, dialog_peer::Session>,
        of: &Entity,
    ) -> Result<Vec<u64>> {
        let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
            "salary": { "the": "org/salary", "as": "UnsignedInteger", "select": "max" }
        }}))?;
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::constant(of.clone()));
        terms.insert("salary".to_string(), Term::<Any>::var("salary"));
        let rows: Vec<ConceptConclusion> = branch
            .select(ConceptQuery { predicate, terms })
            .perform(operator)
            .try_vec()
            .await?;
        rows.iter()
            .map(|row| Ok(row.get::<u64>("salary")?))
            .collect()
    }

    /// A write under `max` succeeds the claim a `max` read returns: the
    /// greatest live claim is retracted beside the written value,
    /// whether the write is greater or smaller than it, so the cell
    /// holds one claim after each write and a read returns the latest.
    #[dialog_common::test]
    async fn it_succeeds_the_claim_a_max_read_returns() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;

        for (write, expected) in [(100u32, 100u128), (200, 200), (150, 150)] {
            branch
                .transaction()
                .assert(salary(&alice, write))
                .commit()
                .publish()
                .perform(&operator)
                .await?;
            assert_eq!(
                stored(&branch, &operator, &alice).await?,
                vec![expected],
                "writing {write} succeeds the elected claim"
            );
            assert_eq!(
                read_max(&branch, &operator, &alice).await?,
                vec![expected as u64]
            );
        }

        // Writing the value the cell already holds retracts nothing.
        branch
            .transaction()
            .assert(salary(&alice, 150))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(stored(&branch, &operator, &alice).await?, vec![150]);
        Ok(())
    }

    /// Two writers succeed the same claim concurrently: each retracts
    /// the claim it observed and asserts its own, and the merge keeps
    /// both of theirs, where the read under `max` elects. No write is
    /// lost and no merge is refused.
    #[dialog_common::test]
    async fn it_keeps_both_claims_of_concurrent_successions() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let a = repo.branch("a").open().perform(&operator).await?;
        let b = repo.branch("b").open().perform(&operator).await?;
        let alice = Entity::new()?;

        a.transaction()
            .assert(salary(&alice, 100))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        b.pull().from(&a).perform(&operator).await?;
        assert_eq!(stored(&b, &operator, &alice).await?, vec![100]);

        a.transaction()
            .assert(salary(&alice, 200))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        b.transaction()
            .assert(salary(&alice, 300))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(stored(&a, &operator, &alice).await?, vec![200]);
        assert_eq!(stored(&b, &operator, &alice).await?, vec![300]);

        let mut quiesced = false;
        for _ in 0..4 {
            let pulled_a = a.pull().from(&b).perform(&operator).await?;
            let pulled_b = b.pull().from(&a).perform(&operator).await?;
            if pulled_a.is_none() && pulled_b.is_none() {
                quiesced = true;
                break;
            }
        }
        assert!(quiesced, "mutual pulls reach a fixed point");
        assert_eq!(
            stored(&a, &operator, &alice).await?,
            vec![200, 300],
            "both successions stand; the claim both observed is gone"
        );
        assert_eq!(read_max(&a, &operator, &alice).await?, vec![300]);
        assert_eq!(read_max(&b, &operator, &alice).await?, vec![300]);
        Ok(())
    }

    /// A candidate a rule derives is not a claim: a write under `max`
    /// succeeds the greatest stored claim and leaves the derived
    /// candidate, which the read still elects while it is greater.
    #[dialog_common::test]
    async fn it_succeeds_stored_claims_only_beside_a_derived_candidate() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;

        // `org/salary(x) := bonus :- org/bonus(x) = bonus`.
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": {
                "salary": { "the": "org/salary", "as": "UnsignedInteger" }
            }},
            "when": [{
                "assert": { "with": {
                    "bonus": { "the": "org/bonus", "as": "UnsignedInteger" }
                }},
                "where": {
                    "this": { "?": { "name": "this" } },
                    "bonus": { "?": { "name": "salary" } }
                }
            }]
        }))?;
        let rule: DeductiveRule = descriptor.compile()?;
        branch
            .transaction()
            .assert(&rule)
            .assert(salary(&alice, 100))
            .assert(dialog_query::the!("org/bonus").of(alice.clone()).is(500u32))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(read_max(&branch, &operator, &alice).await?, vec![500]);

        branch
            .transaction()
            .assert(salary(&alice, 200))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            stored(&branch, &operator, &alice).await?,
            vec![100, 200],
            "the read elected the derived candidate, so no stored claim is succeeded"
        );
        assert_eq!(
            read_max(&branch, &operator, &alice).await?,
            vec![500],
            "the derived candidate stands and still wins the read"
        );

        // Once the derived candidate is gone, the greatest stored
        // claim is what a read returns, and a write succeeds it.
        branch
            .transaction()
            .retract(dialog_query::the!("org/bonus").of(alice.clone()).is(500u32))
            .assert(salary(&alice, 150))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            stored(&branch, &operator, &alice).await?,
            vec![100, 150],
            "the write succeeds the stored claim the read now elects"
        );
        Ok(())
    }

    /// A transaction reads its own `last` write as the commit will: the
    /// staged row stands at the edition the commit mints, so it is
    /// elected over the committed claim, and a second write in the same
    /// transaction succeeds the first, which was never committed and
    /// leaves no trace.
    #[dialog_common::test]
    async fn it_reads_its_own_last_write_before_the_commit() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;
        let last = |value: u32| AttributeStatement {
            succession: Some(Succession::Last),
            ..salary(&alice, value)
        };
        branch
            .transaction()
            .assert(last(100))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
            "salary": { "the": "org/salary", "as": "UnsignedInteger" }
        }}))?;
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::constant(alice.clone()));
        terms.insert("salary".to_string(), Term::<Any>::var("salary"));
        let query = ConceptQuery { predicate, terms };

        let transaction = branch.transaction().assert(last(200)).assert(last(300));
        let rows: Vec<ConceptConclusion> = transaction
            .query()
            .select(query.clone())
            .perform(&operator)
            .try_vec()
            .await?;
        let read: Vec<u64> = rows
            .iter()
            .map(|row| row.get::<u64>("salary"))
            .collect::<Result<_, _>>()?;
        assert_eq!(read, vec![300], "the transaction reads its latest write");

        transaction.commit().publish().perform(&operator).await?;
        assert_eq!(
            stored(&branch, &operator, &alice).await?,
            vec![300],
            "the committed claim is succeeded and the superseded staged write is gone"
        );
        Ok(())
    }

    /// A write under `last` succeeds the claim a `last` read returns:
    /// of two live claims, the newer. The older stays, as under any
    /// other policy; a `last` read returns the write afterwards, since
    /// it is newer than both.
    #[dialog_common::test]
    async fn it_succeeds_the_newest_claim_under_last() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;

        // Two claims written without a policy, one revision apart, so
        // the second is the newer.
        for value in [100u32, 200] {
            branch
                .transaction()
                .assert(dialog_query::the!("org/salary").of(alice.clone()).is(value))
                .commit()
                .publish()
                .perform(&operator)
                .await?;
        }
        assert_eq!(stored(&branch, &operator, &alice).await?, vec![100, 200]);

        let last = AttributeStatement {
            succession: Some(Succession::Last),
            ..salary(&alice, 150)
        };
        branch
            .transaction()
            .assert(last)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            stored(&branch, &operator, &alice).await?,
            vec![100, 150],
            "the newer claim is succeeded, the older stays"
        );

        let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
            "salary": { "the": "org/salary", "as": "UnsignedInteger" }
        }}))?;
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::constant(alice.clone()));
        terms.insert("salary".to_string(), Term::<Any>::var("salary"));
        let rows: Vec<ConceptConclusion> = branch
            .select(ConceptQuery { predicate, terms })
            .perform(&operator)
            .try_vec()
            .await?;
        let read: Vec<u64> = rows
            .iter()
            .map(|row| row.get::<u64>("salary"))
            .collect::<Result<_, _>>()?;
        assert_eq!(read, vec![150], "a last read returns the write");
        Ok(())
    }
}
