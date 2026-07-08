//! The doc-handle discipline end-to-end (`notes/automerge-integration-spec.md`
//! §4.3): fold at open, mid-session absorption without losing pending edits,
//! progressive open from the winner sibling, and physical convergence — one
//! stored sibling — on commit.
//!
//! Native-only for now: on wasm32 `Record`'s memo cache erases without
//! `Send`/`Sync` (the `ConditionalSend` convention), while the typed
//! attribute read path (`StaticAttributeQuery`'s `Application` impl) carries
//! a hard `Send` bound — so record-typed attributes do not yet compile for
//! wasm anywhere in the workspace (dialog-query's own record tests
//! included). Lift this gate when that convention mismatch is resolved.
#![cfg(not(target_arch = "wasm32"))]

use std::str::FromStr as _;

use anyhow::Result;
use dialog_artifacts::{
    Artifact, ArtifactSelector, Attribute as ArtifactsAttribute, Cause, Entity, Instruction,
    Record, RecordFormat as _, Value,
};
use dialog_automerge::{DocumentSession, SessionError, TextDocument};
use dialog_operator::Operator;
use dialog_repository::Branch;
use dialog_repository::helpers::{test_operator_with_profile, test_repo};
use dialog_storage::provider::storage::VolatileSpace;
use futures_util::{TryStreamExt as _, stream};

mod note {
    use dialog_automerge::TextDocument;
    use dialog_query::{Attribute, Recorded};

    /// Collaboratively edited note body
    #[derive(Attribute, Clone)]
    pub struct Body(pub Recorded<TextDocument>);

    /// Tags on the note
    #[derive(Attribute, Clone)]
    #[cardinality(many)]
    pub struct Tag(pub Recorded<TextDocument>);
}

type BodySession = DocumentSession<note::Body, TextDocument>;

async fn open_branch() -> Result<(Operator<VolatileSpace>, Branch)> {
    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    Ok((operator, branch))
}

/// Write `documents` as raw additive asserts — sibling claims on one
/// `(the, of)` pair, exactly the storage state concurrent replicas produce
/// after sync (raw asserts never supersede; spec §2).
async fn assert_siblings(
    branch: &Branch,
    operator: &Operator<VolatileSpace>,
    entity: &Entity,
    documents: &[&TextDocument],
) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    let mut instructions = Vec::new();
    for document in documents {
        let record = Record::from_format((*document).clone())?;
        records.push(record.clone());
        instructions.push(Instruction::Assert(Artifact {
            the: ArtifactsAttribute::from_str("note/body")?,
            of: entity.clone(),
            is: Value::Record(record),
            cause: None,
        }));
    }
    branch
        .commit(stream::iter(instructions))
        .perform(operator)
        .await?;
    Ok(records)
}

/// Every stored sibling record for `note/body` on `entity`.
async fn stored_records(
    branch: &Branch,
    operator: &Operator<VolatileSpace>,
    entity: &Entity,
) -> Result<Vec<Record>> {
    let artifacts: Vec<Artifact> = branch
        .claims()
        .select(
            ArtifactSelector::new()
                .the(ArtifactsAttribute::from_str("note/body")?)
                .of(entity.clone()),
        )
        .perform(operator)
        .await?
        .try_collect()
        .await?;
    Ok(artifacts
        .into_iter()
        .filter_map(|artifact| match artifact.is {
            Value::Record(record) => Some(record),
            _ => None,
        })
        .collect())
}

#[dialog_common::test]
async fn open_is_none_when_the_attribute_holds_nothing() -> Result<()> {
    let (operator, branch) = open_branch().await?;
    let session = BodySession::open(&branch, &operator, Entity::new()?).await?;
    assert!(session.is_none());
    Ok(())
}

/// Create → edit → commit → reopen: the round trip preserves the text, and
/// storage holds exactly one sibling carrying the document's canonical bytes.
#[dialog_common::test]
async fn create_commit_reopen_round_trips() -> Result<()> {
    let (operator, branch) = open_branch().await?;
    let entity = Entity::new()?;

    let mut session = BodySession::create(entity.clone(), TextDocument::new())?;
    session.document_mut().splice(0, 0, "hello world")?;
    session.commit(&branch, &operator).await?;

    let stored = stored_records(&branch, &operator, &entity).await?;
    assert_eq!(stored.len(), 1);
    assert_eq!(
        stored[0].as_bytes(),
        session.document().encode()?.as_slice()
    );

    let reopened = BodySession::open(&branch, &operator, entity)
        .await?
        .expect("committed document reopens");
    assert_eq!(reopened.document().text(), "hello world");
    Ok(())
}

/// The fold at open: two diverged sibling claims project as one document
/// containing both forks' edits, and the commit that follows collapses
/// storage to a single sibling holding the merged canonical bytes.
#[dialog_common::test]
async fn open_folds_diverged_siblings_and_commit_collapses_them() -> Result<()> {
    let (operator, branch) = open_branch().await?;
    let entity = Entity::new()?;

    let mut base = TextDocument::new();
    base.splice(0, 0, "draft")?;
    let mut left = base.fork();
    let mut right = base.fork();
    left.splice(0, 0, "my ")?;
    right.splice(5, 0, " notes")?;

    assert_siblings(&branch, &operator, &entity, &[&left, &right]).await?;
    assert_eq!(stored_records(&branch, &operator, &entity).await?.len(), 2);

    let mut session = BodySession::open(&branch, &operator, entity.clone())
        .await?
        .expect("siblings exist");
    assert_eq!(session.document().text(), "my draft notes");

    session.commit(&branch, &operator).await?;
    let stored = stored_records(&branch, &operator, &entity).await?;
    assert_eq!(stored.len(), 1, "Replace superseded both forks");
    assert_eq!(
        stored[0].as_bytes(),
        TextDocument::merge(&left, &right).encode()?.as_slice()
    );
    Ok(())
}

/// Mid-session absorption (spec §6.15): a sibling that lands while the
/// session holds pending local edits is merged into the live document — the
/// pending edits survive, and the next commit includes both.
#[dialog_common::test]
async fn refresh_absorbs_arrivals_without_losing_pending_edits() -> Result<()> {
    let (operator, branch) = open_branch().await?;
    let entity = Entity::new()?;

    let mut base = TextDocument::new();
    base.splice(0, 0, "hello")?;
    assert_siblings(&branch, &operator, &entity, &[&base]).await?;

    let mut session = BodySession::open(&branch, &operator, entity.clone())
        .await?
        .expect("document exists");

    // Pending local edit, not committed.
    session.document_mut().splice(0, 0, ">> ")?;
    // Nothing new stored yet.
    assert_eq!(session.refresh(&branch, &operator).await?, 0);

    // A concurrent writer's fork lands via sync while the session is open.
    let mut remote = base.fork();
    remote.splice(5, 0, " world")?;
    assert_siblings(&branch, &operator, &entity, &[&remote]).await?;

    assert_eq!(session.refresh(&branch, &operator).await?, 1);
    assert_eq!(session.document().text(), ">> hello world");
    // Idempotent: the absorbed sibling does not arrive twice.
    assert_eq!(session.refresh(&branch, &operator).await?, 0);

    session.commit(&branch, &operator).await?;
    let stored = stored_records(&branch, &operator, &entity).await?;
    assert_eq!(stored.len(), 1, "the inclusive Replace superseded the fork");

    let reopened = BodySession::open(&branch, &operator, entity)
        .await?
        .expect("document exists");
    assert_eq!(reopened.document().text(), ">> hello world");
    Ok(())
}

/// Progressive open (spec §4.3): the session seeds from the deterministic
/// pick-one winner — what a fold-less reader would project — and absorbing
/// the deferred siblings converges to byte-identity with the eager fold.
#[dialog_common::test]
async fn progressive_open_seeds_the_winner_then_converges() -> Result<()> {
    let (operator, branch) = open_branch().await?;
    let entity = Entity::new()?;

    let mut base = TextDocument::new();
    base.splice(0, 0, "draft")?;
    let mut left = base.fork();
    let mut right = base.fork();
    left.splice(0, 0, "my ")?;
    right.splice(5, 0, " notes")?;

    let records = assert_siblings(&branch, &operator, &entity, &[&left, &right]).await?;

    // The winner under the read path's rule (causes are None, so the fact
    // hash decides — `resolution::choose`).
    let fact = |record: &Record| {
        Cause::from(&Artifact {
            the: ArtifactsAttribute::from_str("note/body").unwrap(),
            of: entity.clone(),
            is: Value::Record(record.clone()),
            cause: None,
        })
    };
    let winner = if fact(&records[0]) >= fact(&records[1]) {
        &left
    } else {
        &right
    };

    let (mut session, deferred) = BodySession::open_progressive(&branch, &operator, entity.clone())
        .await?
        .expect("siblings exist");
    assert_eq!(session.document().text(), winner.text());
    assert_eq!(deferred.len(), 1);

    for sibling in &deferred {
        assert!(session.absorb(sibling)?);
    }
    assert_eq!(session.document().text(), "my draft notes");

    let eager = BodySession::open(&branch, &operator, entity)
        .await?
        .expect("siblings exist");
    assert_eq!(session.document().encode()?, eager.document().encode()?);
    Ok(())
}

/// Foreign bytes under the attribute (spec §6.12): undecodable siblings drop
/// out of the fold deterministically; when nothing decodes, open reports it
/// rather than pretending absence; a direct absorb surfaces the failure.
#[dialog_common::test]
async fn undecodable_siblings_are_dropped_or_reported() -> Result<()> {
    let (operator, branch) = open_branch().await?;

    // Garbage beside a real document: the fold drops the garbage.
    let entity = Entity::new()?;
    let mut document = TextDocument::new();
    document.splice(0, 0, "real")?;
    let garbage = Record::from(vec![0xde, 0xad, 0xbe, 0xef]);
    branch
        .commit(stream::iter([Instruction::Assert(Artifact {
            the: ArtifactsAttribute::from_str("note/body")?,
            of: entity.clone(),
            is: Value::Record(garbage.clone()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;
    assert_siblings(&branch, &operator, &entity, &[&document]).await?;

    let mut session = BodySession::open(&branch, &operator, entity)
        .await?
        .expect("a decodable sibling exists");
    assert_eq!(session.document().text(), "real");
    assert!(matches!(
        session.absorb(&garbage),
        Err(SessionError::Record(_))
    ));

    // Only garbage: open errors instead of returning None or a document.
    let foreign = Entity::new()?;
    branch
        .commit(stream::iter([Instruction::Assert(Artifact {
            the: ArtifactsAttribute::from_str("note/body")?,
            of: foreign.clone(),
            is: Value::Record(Record::from(vec![0xff, 0x00])),
            cause: None,
        })]))
        .perform(&operator)
        .await?;
    assert!(matches!(
        BodySession::open(&branch, &operator, foreign).await,
        Err(SessionError::Undecodable { siblings: 1 })
    ));
    Ok(())
}

/// Committing unchanged canonical bytes leaves storage exactly as it was —
/// the same-value `Replace` is a no-op at the tree.
#[dialog_common::test]
async fn recommitting_the_same_document_is_stable() -> Result<()> {
    let (operator, branch) = open_branch().await?;
    let entity = Entity::new()?;

    let mut session = BodySession::create(entity.clone(), TextDocument::new())?;
    session.document_mut().splice(0, 0, "stable")?;
    session.commit(&branch, &operator).await?;
    let before = stored_records(&branch, &operator, &entity).await?;

    let mut reopened = BodySession::open(&branch, &operator, entity.clone())
        .await?
        .expect("document exists");
    reopened.commit(&branch, &operator).await?;

    let after = stored_records(&branch, &operator, &entity).await?;
    assert_eq!(before, after);
    Ok(())
}

/// A `Cardinality::Many` attribute has no single document to hold open: the
/// session refuses it up front.
#[dialog_common::test]
async fn sessions_require_cardinality_one() -> Result<()> {
    let result =
        DocumentSession::<note::Tag, TextDocument>::create(Entity::new()?, TextDocument::new());
    assert!(matches!(result, Err(SessionError::NotSingular { .. })));
    // The descriptor really is cardinality-many (the guard, not the derive,
    // is under test).
    assert_eq!(
        note::Tag::descriptor().cardinality(),
        dialog_query::Cardinality::Many
    );
    Ok(())
}
