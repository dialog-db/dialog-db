//! A snapshot reads exactly like the branch it was taken from, and keeps
//! reading the same thing after the branch moves on.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::Result;
use dialog_artifacts::history::History as _;
use dialog_artifacts::{Artifact, ArtifactSelector, Entity, Value};
use dialog_effects::blob::BlobError;
use dialog_operator::helpers::test_operator_with_profile;
use dialog_query::query::Output;
use dialog_query::{Concept, Query, Term, the};
use futures_util::{StreamExt as _, stream};

use crate::helpers::test_repo;
use crate::repository::source::SourceRef;
use crate::schema::DidExt as _;
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::memory::Resolve;

use crate::{Blob, CommitError, QueryLayer, RemoteSite, Select, TreeReference, schema};

mod people {
    /// `test/name` attribute used by the Person concept below.
    #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("test")]
    pub struct Name(
        /// The person's name string.
        pub String,
    );
}

/// A simple concept to query through.
#[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Person {
    /// The person entity.
    pub this: Entity,
    /// Their `test/name` attribute.
    pub name: people::Name,
}

fn person(id: &str, name: &str) -> Person {
    Person {
        this: id.parse().expect("entity parses"),
        name: people::Name(name.into()),
    }
}

/// Every `test/name` value on a line, sorted, read through the raw
/// artifact index (the lowest read path there is).
async fn names<'a, Env>(source: impl Into<SourceRef<'a>>, env: &Env) -> Result<Vec<String>>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let rows: Vec<Artifact> = Select::from_source(
        source.into(),
        ArtifactSelector::new().the("test/name".parse()?),
    )
    .to_owned()
    .perform(env)
    .await?
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect::<Result<_, _>>()?;
    let mut names: Vec<String> = rows
        .into_iter()
        .filter_map(|row| match row.is {
            Value::String(name) => Some(name),
            _ => None,
        })
        .collect();
    names.sort();
    Ok(names)
}

/// The `SessionBranch` rows a query layer yields.
async fn session_branches<Env>(
    layer: QueryLayer<'_>,
    env: &Env,
) -> Result<Vec<schema::SessionBranch>>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Identify>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    Ok(layer
        .select(Query::<schema::SessionBranch> {
            this: schema::Session::entity().into(),
            branch: Term::var("branch"),
        })
        .perform(env)
        .try_vec()
        .await?)
}

/// Every `Person` a query layer yields, by name, sorted.
async fn people<Env>(layer: QueryLayer<'_>, env: &Env) -> Result<Vec<String>>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Identify>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let mut names: Vec<String> = layer
        .select(Query::<Person> {
            this: Term::var("this"),
            name: Term::var("name"),
        })
        .perform(env)
        .try_vec()
        .await?
        .into_iter()
        .map(|row| row.name.0)
        .collect();
    names.sort();
    Ok(names)
}

#[dialog_common::test]
async fn it_reads_what_the_branch_reads() -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .assert(person("id:alice", "Alice"))
        .assert(person("id:bob", "Bob"))
        .commit()
        .perform(&operator)
        .await?;

    let snapshot = branch.snapshot().expect("a committed branch snapshots");
    assert_eq!(snapshot.revision(), branch.revision().expect("head"));
    assert_eq!(snapshot.of(), branch.of());

    // The raw index, the typed shortcut, and the composable layer all
    // read the same two facts the branch does.
    let expected = vec!["Alice".to_string(), "Bob".to_string()];
    assert_eq!(names(&snapshot, &operator).await?, expected);
    assert_eq!(names(&branch, &operator).await?, expected);
    assert_eq!(people(snapshot.query(), &operator).await?, expected);
    let typed: Vec<Person> = snapshot
        .select(Query::<Person> {
            this: "id:alice".parse::<Entity>()?.into(),
            name: Term::var("name"),
        })
        .perform(&operator)
        .try_vec()
        .await?;
    assert_eq!(typed, vec![person("id:alice", "Alice")]);
    Ok(())
}

#[dialog_common::test]
async fn it_stays_pinned_while_the_branch_advances() -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .assert(person("id:alice", "Alice"))
        .commit()
        .perform(&operator)
        .await?;
    let snapshot = branch.snapshot().expect("snapshot");
    let pinned = snapshot.revision().clone();

    branch
        .transaction()
        .assert(person("id:bob", "Bob"))
        .commit()
        .perform(&operator)
        .await?;

    assert_eq!(
        names(&snapshot, &operator).await?,
        vec!["Alice".to_string()],
        "the snapshot keeps naming the revision it was taken at"
    );
    assert_eq!(snapshot.revision(), pinned);
    assert_eq!(
        names(&branch, &operator).await?,
        vec!["Alice".to_string(), "Bob".to_string()]
    );
    // A fresh snapshot follows the branch.
    let later = branch.snapshot().expect("snapshot");
    assert_eq!(
        names(&later, &operator).await?,
        vec!["Alice".to_string(), "Bob".to_string()]
    );
    Ok(())
}

#[dialog_common::test]
async fn it_has_no_snapshot_before_the_first_commit() -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    assert!(branch.snapshot().is_none());
    Ok(())
}

/// A snapshot minted cold from the repository (no branch handle, no
/// warm caches) reads the same revision the same way.
#[dialog_common::test]
async fn it_reads_through_a_cold_handle() -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let revision = branch
        .transaction()
        .assert(person("id:alice", "Alice"))
        .commit()
        .perform(&operator)
        .await?;

    let cold = repo.snapshot(revision);
    assert_eq!(names(&cold, &operator).await?, vec!["Alice".to_string()]);
    assert_eq!(
        people(cold.query(), &operator).await?,
        vec!["Alice".to_string()]
    );
    Ok(())
}

/// A revision whose root the store does not hold fails at the read,
/// naming the missing block, exactly as an unreachable branch does.
#[dialog_common::test]
async fn it_fails_when_the_root_is_absent() -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let mut revision = branch
        .transaction()
        .assert(person("id:alice", "Alice"))
        .commit()
        .perform(&operator)
        .await?;
    revision.tree = TreeReference::from([7u8; 32]);

    let snapshot = repo.snapshot(revision);
    let result = snapshot
        .claims()
        .select(ArtifactSelector::new().the("test/name".parse()?))
        .perform(&operator)
        .await;
    assert!(
        matches!(
            result,
            Err(dialog_search_tree::DialogSearchTreeError::Node(_))
        ),
        "an absent root must fail the read up front"
    );
    Ok(())
}

#[dialog_common::test]
async fn it_joins_a_snapshot_into_a_branch_query() -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let main = repo.branch("main").open().perform(&operator).await?;
    main.transaction()
        .assert(person("id:alice", "Alice"))
        .commit()
        .perform(&operator)
        .await?;
    let snapshot = main.snapshot().expect("snapshot");

    let feature = repo.branch("feature").open().perform(&operator).await?;
    feature
        .transaction()
        .assert(person("id:bob", "Bob"))
        .commit()
        .perform(&operator)
        .await?;

    assert_eq!(
        people(feature.query().join(&snapshot), &operator).await?,
        vec!["Alice".to_string(), "Bob".to_string()],
        "a joined snapshot is a peer source"
    );
    assert_eq!(
        people(snapshot.query().join(&feature), &operator).await?,
        vec!["Alice".to_string(), "Bob".to_string()],
        "in either order"
    );
    let layer = feature.query().join(&snapshot);
    assert_eq!(layer.branches().len(), 1);
    assert_eq!(layer.snapshots().len(), 1);
    Ok(())
}

/// A snapshot query carries the session and replica metadata a branch
/// query does; it is not a branch, so it mints no `SessionBranch` row —
/// and joining a branch in adds exactly that branch's row.
#[dialog_common::test]
async fn it_injects_session_metadata() -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .assert(person("id:alice", "Alice"))
        .commit()
        .perform(&operator)
        .await?;
    let snapshot = branch.snapshot().expect("snapshot");

    let sessions: Vec<schema::Session> = snapshot
        .query()
        .select(Query::<schema::Session> {
            this: schema::Session::entity().into(),
            profile: Term::var("profile"),
            operator: Term::var("operator"),
        })
        .perform(&operator)
        .try_vec()
        .await?;
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].profile.0, profile.did().this());

    let replica = schema::Replica::new(profile.did(), branch.of().clone());
    let replicas: Vec<schema::Replica> = snapshot
        .query()
        .select(Query::<schema::Replica> {
            this: replica.this.clone().into(),
            subject: Term::var("subject"),
            profile: Term::var("profile"),
        })
        .perform(&operator)
        .try_vec()
        .await?;
    assert_eq!(replicas, vec![replica.clone()]);

    assert!(
        session_branches(snapshot.query(), &operator)
            .await?
            .is_empty(),
        "a snapshot is not a branch in scope"
    );
    let joined = session_branches(snapshot.query().join(&branch), &operator).await?;
    assert_eq!(joined.len(), 1);
    assert_eq!(
        joined[0].branch.0,
        schema::Branch::new(&replica, "main").this
    );
    Ok(())
}

#[dialog_common::test]
async fn it_folds_the_overlay_into_reads() -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .assert(person("id:alice", "Alice"))
        .commit()
        .perform(&operator)
        .await?;
    let snapshot = branch.snapshot().expect("snapshot");

    snapshot.overlay().assert(person("id:bob", "Bob"));
    assert_eq!(
        people(snapshot.query(), &operator).await?,
        vec!["Alice".to_string(), "Bob".to_string()],
        "an overlay assert surfaces in reads"
    );
    snapshot.overlay().retract(
        the!("test/name")
            .of("id:alice".parse::<Entity>()?)
            .is("Alice".to_string()),
    );
    assert_eq!(
        people(snapshot.query(), &operator).await?,
        vec!["Bob".to_string()],
        "an overlay retract tombstones the stored fact"
    );
    // The branch's own overlay is separate.
    assert_eq!(
        people(branch.query(), &operator).await?,
        vec!["Alice".to_string()]
    );
    // A clone shares the overlay, like branch clones do.
    let clone = snapshot.clone();
    assert_eq!(
        people(clone.query(), &operator).await?,
        vec!["Bob".to_string()]
    );
    snapshot.overlay().clear();
    assert_eq!(
        people(clone.query(), &operator).await?,
        vec!["Alice".to_string()]
    );
    Ok(())
}

/// A deductive rule committed on the branch derives on the snapshot,
/// resolved through the same layered rule resolution.
#[dialog_common::test]
async fn it_resolves_committed_rules() -> Result<()> {
    use dialog_query::rule::DeductiveRuleDescriptor;
    use dialog_query::{ConceptQuery, Parameters};

    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;

    // employee(this, name) :- person-name(this, name)
    let rule = {
        let json = serde_json::json!({
            "deduce": { "with": { "name": { "the": "org/employee-name", "as": "Text" } } },
            "when": [{
                "assert": { "with": { "name": { "the": "org/person-name", "as": "Text" } } },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "name": { "?": { "name": "name" } }
                }
            }]
        });
        let descriptor: DeductiveRuleDescriptor =
            serde_json::from_value(json).expect("descriptor parses");
        descriptor.compile().expect("rule compiles")
    };
    let employee = rule.conclusion().clone();
    let alice: Entity = "id:alice".parse()?;
    branch
        .transaction()
        .assert(&rule)
        .assert(
            the!("org/person-name")
                .of(alice.clone())
                .is("Alice".to_string()),
        )
        .commit()
        .perform(&operator)
        .await?;

    let snapshot = branch.snapshot().expect("snapshot");
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("name".into(), Term::var("name"));
    let rows = snapshot
        .query()
        .select(ConceptQuery {
            predicate: employee,
            terms,
        })
        .perform(&operator)
        .try_vec()
        .await?;
    assert_eq!(rows.len(), 1, "the committed rule derives on the snapshot");
    assert_eq!(*rows[0].entity(), alice);
    Ok(())
}

/// The built-in derived version-control concepts — the DAG edge and
/// its recursive closure — resolve on a snapshot, concluded from the
/// signed records in its tree.
#[dialog_common::test]
async fn it_resolves_derived_revision_concepts() -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;

    let mut revisions = Vec::new();
    for name in ["Alice", "Bob", "Carol"] {
        revisions.push(
            branch
                .transaction()
                .assert(the!("user/name").of(Entity::new()?).is(name.to_string()))
                .commit()
                .perform(&operator)
                .await?,
        );
    }
    let [first, second, third] = &revisions[..] else {
        unreachable!("three commits were made");
    };
    let snapshot = branch.snapshot().expect("snapshot");

    let edges: Vec<schema::RevisionParent> = snapshot
        .query()
        .select(Query::<schema::RevisionParent> {
            this: third.entity().into(),
            parent: Term::var("parent"),
        })
        .perform(&operator)
        .try_vec()
        .await?;
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].parent.0, second.entity());

    let mut reachable: Vec<Entity> = snapshot
        .query()
        .select(Query::<schema::RevisionAncestor> {
            this: third.entity().into(),
            ancestor: Term::var("ancestor"),
        })
        .perform(&operator)
        .try_vec()
        .await?
        .into_iter()
        .map(|row| row.ancestor.0)
        .collect();
    reachable.sort();
    let mut expected = vec![first.entity(), second.entity()];
    expected.sort();
    assert_eq!(reachable, expected);
    Ok(())
}

#[dialog_common::test]
async fn it_logs_history() -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let mut revisions = Vec::new();
    for name in ["Alice", "Bob"] {
        revisions.push(
            branch
                .transaction()
                .assert(the!("user/name").of(Entity::new()?).is(name.to_string()))
                .commit()
                .perform(&operator)
                .await?,
        );
    }
    let snapshot = branch.snapshot().expect("snapshot");

    let log = snapshot.log(&operator, 10).await?;
    let versions: Vec<_> = log.iter().map(|(version, _)| *version).collect();
    assert_eq!(
        versions,
        vec![revisions[1].version(), revisions[0].version()],
        "newest first, through the whole ancestry"
    );
    assert_eq!(
        versions,
        branch
            .log(&operator, 10)
            .await?
            .iter()
            .map(|(version, _)| *version)
            .collect::<Vec<_>>()
    );
    let record = snapshot
        .history(&operator)
        .revision_record(&revisions[1].version())
        .await?
        .expect("the head's record is retrievable");
    assert_eq!(record.parents, vec![revisions[0].version()]);
    Ok(())
}

/// Blob reads bind to a snapshot; writes and retractions, which would
/// have to advance it through a reference, are refused and leave both
/// the snapshot and the branch as they were.
#[dialog_common::test]
async fn it_reads_blobs_and_refuses_to_write_them() -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;

    let bytes = b"snapshot blob".repeat(64);
    let entity = Blob::import(stream::iter(vec![Ok(bytes.clone())]))
        .write(branch.blobs())
        .perform(&operator)
        .await?;
    let snapshot = branch.snapshot().expect("snapshot");

    let size = Blob::from(entity.clone())
        .size(snapshot.blobs())
        .perform(&operator)
        .await?;
    assert_eq!(size, Some(bytes.len() as u64));

    let mut reader = Blob::from(entity.clone())
        .read(snapshot.blobs())
        .perform(&operator)
        .await?;
    let mut read = Vec::new();
    while let Some(chunk) = reader.next().await? {
        read.extend_from_slice(&chunk);
    }
    assert_eq!(read, bytes);

    let refused = Blob::import(stream::iter(vec![Ok::<_, BlobError>(b"more".to_vec())]))
        .write(snapshot.blobs())
        .perform(&operator)
        .await
        .err();
    assert!(
        matches!(refused, Some(CommitError::Detached)),
        "a blob write through a snapshot reference is refused: {refused:?}"
    );
    let refused = Blob::from(entity.clone())
        .retract(snapshot.blobs())
        .perform(&operator)
        .await;
    assert!(
        matches!(refused, Err(CommitError::Detached)),
        "a blob retraction through a snapshot reference is refused: {refused:?}"
    );

    // Nothing moved.
    assert_eq!(snapshot.revision(), branch.revision().expect("head"));
    let fresh = repo.branch("main").load().perform(&operator).await?;
    assert_eq!(fresh.revision(), branch.revision());
    assert_eq!(
        Blob::from(entity)
            .size(fresh.blobs())
            .perform(&operator)
            .await?,
        Some(bytes.len() as u64)
    );
    Ok(())
}

/// A blob the snapshot's revision does not reference is a miss, not an
/// error path into some upstream — a snapshot has none.
#[dialog_common::test]
async fn it_reports_an_unreferenced_blob_as_absent() -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .assert(person("id:alice", "Alice"))
        .commit()
        .perform(&operator)
        .await?;
    let snapshot = branch.snapshot().expect("snapshot");

    let never = Entity::from_blob(&[3u8; 32])?;
    assert_eq!(
        Blob::from(never.clone())
            .size(snapshot.blobs())
            .perform(&operator)
            .await?,
        None
    );
    let missing = Blob::from(never)
        .read(snapshot.blobs())
        .perform(&operator)
        .await
        .err();
    assert!(
        matches!(missing, Some(CommitError::Blob(BlobError::NotFound(_)))),
        "an unreferenced blob is not found: {missing:?}"
    );
    Ok(())
}
