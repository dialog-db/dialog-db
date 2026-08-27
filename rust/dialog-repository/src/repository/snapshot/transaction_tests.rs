//! A snapshot transacts with the branch's own command types, advances in
//! place, and never touches the branch it was taken from.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::Result;
use dialog_artifacts::history::{History as _, Version};
use dialog_artifacts::{Artifact, ArtifactSelector, Changes, Entity, Instruction, Value};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::memory::Resolve;
use dialog_operator::helpers::test_operator_with_profile;
use dialog_query::attribute::The;
use dialog_query::query::Output;
use dialog_query::{Query, Term, the};
use dialog_storage::provider::storage::VolatileSpace;
use futures_util::{StreamExt as _, stream};

use crate::helpers::test_repo;
use crate::repository::source::SourceRef;
use crate::{
    Branch, CommitError, Item, PublishError, RemoteSite, Repository, Select, Snapshot,
    TreeReference, schema,
};

/// Every `user/name` value on a line, sorted.
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
    let mut names: Vec<String> = values(source, env, "user/name", None)
        .await?
        .into_iter()
        .filter_map(|value| match value {
            Value::String(name) => Some(name),
            _ => None,
        })
        .collect();
    names.sort();
    Ok(names)
}

/// The values under `the` on a line, narrowed to `of` when given.
async fn values<'a, Env>(
    source: impl Into<SourceRef<'a>>,
    env: &Env,
    the: &str,
    of: Option<&Entity>,
) -> Result<Vec<Value>>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let mut selector = ArtifactSelector::new().the(the.parse()?);
    if let Some(of) = of {
        selector = selector.of(of.clone());
    }
    let rows: Vec<Artifact> = Select::from_source(source.into(), selector)
        .to_owned()
        .perform(env)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;
    Ok(rows.into_iter().map(|row| row.is).collect())
}

/// Every `user/name` value a query layer yields, sorted: the overlay
/// and metadata path, as opposed to the raw index `names` reads.
async fn queried_names<Env>(layer: crate::QueryLayer<'_>, env: &Env) -> Result<Vec<String>>
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
    let mut rows: Vec<String> = layer
        .select(dialog_query::AttributeQuery::from(
            Term::<The>::from(the!("user/name"))
                .of(Term::<Entity>::var("e"))
                .is(Term::<String>::var("v")),
        ))
        .perform(env)
        .try_vec()
        .await?
        .into_iter()
        .filter_map(|claim| match claim.is {
            Value::String(name) => Some(name),
            _ => None,
        })
        .collect();
    rows.sort();
    Ok(rows)
}

/// A `user/name` statement, for transactions.
fn name(of: &str, is: &str) -> impl dialog_artifacts::Statement {
    the!("user/name")
        .of(of.parse::<Entity>().expect("entity parses"))
        .is(is.to_string())
}

/// A `user/name` artifact, for raw commits.
fn fact(of: &str, is: &str) -> Artifact {
    Artifact {
        the: "user/name".parse().expect("attribute parses"),
        of: of.parse().expect("entity parses"),
        is: Value::String(is.to_string()),
        cause: None,
    }
}

/// A branch with one committed fact and a snapshot of it.
async fn staged() -> Result<(
    dialog_operator::Operator<VolatileSpace>,
    dialog_identity::Profile,
    Repository,
    Branch,
    Snapshot,
)> {
    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .assert(name("user:alice", "Alice"))
        .commit()
        .perform(&operator)
        .await?;
    let snapshot = branch.snapshot().expect("a committed branch snapshots");
    Ok((operator, profile, repo, branch, snapshot))
}

#[dialog_common::test]
async fn it_advances_the_snapshot_and_not_the_branch() -> Result<()> {
    let (operator, _, repo, branch, snapshot) = staged().await?;
    let base = snapshot.revision();

    let minted = snapshot
        .transaction()
        .assert(name("user:bob", "Bob"))
        .commit()
        .perform(&operator)
        .await?;

    assert_eq!(
        snapshot.revision(),
        minted,
        "the snapshot advanced to the revision the commit returned"
    );
    assert_eq!(
        names(&snapshot, &operator).await?,
        vec!["Alice".to_string(), "Bob".to_string()],
        "and reads the new fact on top of the base"
    );
    assert_eq!(minted.edition, base.edition.successor());
    assert_ne!(minted.tree, base.tree);

    // The branch head did not move: neither the handle nor storage.
    assert_eq!(branch.revision(), Some(base.clone()));
    let fresh = repo.branch("main").load().perform(&operator).await?;
    assert_eq!(fresh.revision(), Some(base.clone()));
    assert_eq!(names(&fresh, &operator).await?, vec!["Alice".to_string()]);

    // The minted record's parent is the base revision.
    let record = snapshot
        .history(&operator)
        .revision_record(&minted.version())
        .await?
        .expect("the minted record is in the tree");
    assert_eq!(record.parents, vec![base.version()]);
    Ok(())
}

#[dialog_common::test]
async fn it_keeps_the_view_you_cloned() -> Result<()> {
    let (operator, _, _, _, snapshot) = staged().await?;
    let kept = snapshot.clone();
    let base = kept.revision();

    snapshot
        .transaction()
        .assert(name("user:bob", "Bob"))
        .commit()
        .perform(&operator)
        .await?;

    assert_eq!(kept.revision(), base, "the clone still names the base");
    assert_eq!(names(&kept, &operator).await?, vec!["Alice".to_string()]);
    assert_eq!(
        names(&snapshot, &operator).await?,
        vec!["Alice".to_string(), "Bob".to_string()]
    );

    // And the other way round: advancing the clone leaves the original.
    let head = snapshot.revision();
    kept.transaction()
        .assert(name("user:carol", "Carol"))
        .commit()
        .perform(&operator)
        .await?;
    assert_eq!(snapshot.revision(), head);
    assert_eq!(
        names(&kept, &operator).await?,
        vec!["Alice".to_string(), "Carol".to_string()]
    );
    Ok(())
}

/// The snapshot and the branch both advance from the same base under
/// the same operator: they must mint distinct versions, or one origin
/// would name two revisions at one edition.
#[dialog_common::test]
async fn it_mints_on_its_own_line() -> Result<()> {
    let (operator, _, _, branch, snapshot) = staged().await?;
    let base = snapshot.revision();

    let detached = snapshot
        .transaction()
        .assert(name("user:bob", "Bob"))
        .commit()
        .perform(&operator)
        .await?;
    let on_branch = branch
        .transaction()
        .assert(name("user:carol", "Carol"))
        .commit()
        .perform(&operator)
        .await?;

    assert_eq!(detached.edition, on_branch.edition);
    assert_ne!(
        detached.version(),
        on_branch.version(),
        "two lines advancing from one base mint distinct versions"
    );
    assert_ne!(detached.origin(), base.origin());
    assert_eq!(on_branch.origin(), base.origin());
    assert_ne!(
        detached.branch, base.branch,
        "a snapshot's revision names its own lineage, not the branch"
    );
    assert_eq!(
        names(&snapshot, &operator).await?,
        vec!["Alice".to_string(), "Bob".to_string()]
    );
    assert_eq!(
        names(&branch, &operator).await?,
        vec!["Alice".to_string(), "Carol".to_string()]
    );
    Ok(())
}

#[dialog_common::test]
async fn it_keeps_one_origin_across_consecutive_transactions() -> Result<()> {
    let (operator, _, _, _, snapshot) = staged().await?;
    let base = snapshot.revision();

    let first = snapshot
        .transaction()
        .assert(name("user:bob", "Bob"))
        .commit()
        .perform(&operator)
        .await?;
    let second = snapshot
        .transaction()
        .assert(name("user:carol", "Carol"))
        .commit()
        .perform(&operator)
        .await?;

    assert_eq!(
        second.origin(),
        first.origin(),
        "a chain of transactions is one sequential actor"
    );
    assert_eq!(second.edition, base.edition.successor().successor());
    assert_eq!(snapshot.revision(), second);
    assert_eq!(
        names(&snapshot, &operator).await?,
        vec!["Alice".to_string(), "Bob".to_string(), "Carol".to_string()]
    );

    // The log walks the chain back into the branch's history.
    let versions: Vec<Version> = snapshot
        .log(&operator, 10)
        .await?
        .into_iter()
        .map(|(version, _)| version)
        .collect();
    assert_eq!(
        versions,
        vec![second.version(), first.version(), base.version()]
    );
    // And the head's context carries both origins' watermarks.
    let context = second
        .context
        .clone()
        .expect("a minted head carries its context");
    assert!(context.observes(&base.version()));
    assert!(context.observes(&second.version()));
    Ok(())
}

/// A clone of an advanced snapshot is a new line: it and the original
/// can both go on from the same revision without minting the same
/// version.
#[dialog_common::test]
async fn it_gives_a_clone_its_own_line() -> Result<()> {
    let (operator, _, _, _, snapshot) = staged().await?;
    let first = snapshot
        .transaction()
        .assert(name("user:bob", "Bob"))
        .commit()
        .perform(&operator)
        .await?;
    let fork = snapshot.clone();

    let left = snapshot
        .transaction()
        .assert(name("user:carol", "Carol"))
        .commit()
        .perform(&operator)
        .await?;
    let right = fork
        .transaction()
        .assert(name("user:dave", "Dave"))
        .commit()
        .perform(&operator)
        .await?;

    assert_eq!(left.edition, right.edition);
    assert_ne!(left.version(), right.version());
    assert_ne!(left.origin(), right.origin());
    assert_eq!(
        left.origin(),
        first.origin(),
        "the original keeps the line it started"
    );
    assert_eq!(
        names(&snapshot, &operator).await?,
        vec!["Alice".to_string(), "Bob".to_string(), "Carol".to_string()]
    );
    assert_eq!(
        names(&fork, &operator).await?,
        vec!["Alice".to_string(), "Bob".to_string(), "Dave".to_string()]
    );
    // Both records point back at the fork point.
    for (side, minted) in [(&snapshot, &left), (&fork, &right)] {
        let record = side
            .history(&operator)
            .revision_record(&minted.version())
            .await?
            .expect("record");
        assert_eq!(record.parents, vec![first.version()]);
    }
    Ok(())
}

#[dialog_common::test]
async fn it_keeps_the_revision_for_a_noop() -> Result<()> {
    let (operator, _, _, _, snapshot) = staged().await?;
    let base = snapshot.revision();

    let unchanged = snapshot.transaction().commit().perform(&operator).await?;
    assert_eq!(unchanged, base, "an empty batch mints nothing");
    assert_eq!(snapshot.revision(), base);

    let unchanged = snapshot
        .transaction()
        .retract(name("user:nobody", "Never"))
        .commit()
        .perform(&operator)
        .await?;
    assert_eq!(unchanged, base, "retracting an absent fact mints nothing");

    let unchanged = snapshot
        .commit(stream::iter(Vec::<Instruction>::new()))
        .perform(&operator)
        .await?;
    assert_eq!(unchanged, base);

    // `allow_empty` mints anyway: the lineage advances, and the
    // revision's own records still land in the tree.
    let empty = snapshot
        .transaction()
        .commit()
        .allow_empty()
        .perform(&operator)
        .await?;
    assert_eq!(empty.edition, base.edition.successor());
    assert_ne!(empty.tree, base.tree);
    assert_eq!(snapshot.revision(), empty);
    let record = snapshot
        .history(&operator)
        .revision_record(&empty.version())
        .await?
        .expect("record");
    assert_eq!(record.parents, vec![base.version()]);
    assert_eq!(
        names(&snapshot, &operator).await?,
        vec!["Alice".to_string()]
    );
    Ok(())
}

/// What a snapshot commit mints is persisted, not merely cached on the
/// handle: a cold handle at the new revision reads it, and a complete
/// export of it succeeds.
#[dialog_common::test]
async fn it_persists_what_it_mints() -> Result<()> {
    let (operator, _, repo, _, snapshot) = staged().await?;
    let minted = snapshot
        .transaction()
        .assert(name("user:bob", "Bob"))
        .commit()
        .perform(&operator)
        .await?;

    let cold = repo.snapshot(minted);
    assert_eq!(
        names(&cold, &operator).await?,
        vec!["Alice".to_string(), "Bob".to_string()]
    );

    let items = cold.export().perform(&operator);
    futures_util::pin_mut!(items);
    let mut blocks = 0;
    while let Some(item) = items.next().await {
        match item? {
            Item::Block(block) => {
                assert!(block.is_intact());
                blocks += 1;
            }
            Item::Blob { .. } => {}
        }
    }
    assert!(blocks > 0, "the complete export walks the persisted tree");
    Ok(())
}

#[dialog_common::test]
async fn it_surfaces_pending_changes_in_the_transaction_query() -> Result<()> {
    let (operator, _, _, _, snapshot) = staged().await?;
    let transaction = snapshot
        .transaction()
        .assert(name("user:bob", "Bob"))
        .retract(name("user:alice", "Alice"));

    let pending: Vec<String> = {
        let mut rows: Vec<String> = transaction
            .query()
            .select(dialog_query::AttributeQuery::from(
                Term::<The>::from(the!("user/name"))
                    .of(Term::<Entity>::var("e"))
                    .is(Term::<String>::var("v")),
            ))
            .perform(&operator)
            .try_vec()
            .await?
            .into_iter()
            .filter_map(|claim| match claim.is {
                Value::String(name) => Some(name),
                _ => None,
            })
            .collect();
        rows.sort();
        rows
    };
    assert_eq!(
        pending,
        vec!["Bob".to_string()],
        "the view shows the pending assert and tombstones the pending retract"
    );
    assert_eq!(
        names(&snapshot, &operator).await?,
        vec!["Alice".to_string()],
        "the snapshot itself is untouched until commit"
    );

    transaction.commit().perform(&operator).await?;
    assert_eq!(names(&snapshot, &operator).await?, vec!["Bob".to_string()]);
    Ok(())
}

#[dialog_common::test]
async fn it_signs_the_minted_revision() -> Result<()> {
    let (operator, profile, _, _, snapshot) = staged().await?;
    let base = snapshot.revision();
    let revision = snapshot
        .transaction()
        .assert(name("user:bob", "Bob"))
        .commit()
        .perform(&operator)
        .await?;

    assert_eq!(revision.issuer, operator.did());
    revision.verify()?;
    let mut tampered = revision.clone();
    tampered.tree = TreeReference::from([9u8; 32]);
    assert!(tampered.verify().is_err());

    let record = snapshot
        .history(&operator)
        .revision_record(&revision.version())
        .await?
        .expect("record");
    assert_eq!(record.issuer, operator.did().to_string());
    assert_eq!(record.authority, profile.did().to_string());
    assert_eq!(record.branch, revision.branch);
    assert_ne!(record.branch, base.branch);
    Ok(())
}

#[dialog_common::test]
async fn it_rejects_writes_to_the_reserved_namespace() -> Result<()> {
    let (operator, _, _, _, snapshot) = staged().await?;
    let base = snapshot.revision();
    let forged = Artifact {
        the: "dialog.db/revision".parse()?,
        of: "forged:revision".parse()?,
        is: Value::String("lies".to_string()),
        cause: None,
    };
    let result = snapshot
        .commit(stream::iter(vec![Instruction::Assert(forged)]))
        .perform(&operator)
        .await;
    assert!(
        matches!(
            result,
            Err(CommitError::Artifact(
                dialog_artifacts::DialogArtifactsError::ReservedAttribute(_)
            ))
        ),
        "writes to the reserved namespace must be refused: {result:?}"
    );
    assert_eq!(snapshot.revision(), base, "a refused commit moves nothing");
    Ok(())
}

/// Two commits through one handle that both built on the same head:
/// the second is refused rather than silently dropping the first, the
/// same way a stale branch write is.
#[dialog_common::test]
async fn it_refuses_a_commit_built_on_a_stale_head() -> Result<()> {
    let (operator, _, _, _, snapshot) = staged().await?;
    let base = snapshot.revision();

    // A commit built on `base` that has not adopted its result yet is
    // exactly what a concurrent transaction on the same handle sees.
    // Reproduce it by advancing the head out from under a prepared
    // adoption.
    let first = snapshot
        .transaction()
        .assert(name("user:bob", "Bob"))
        .commit()
        .perform(&operator)
        .await?;
    let stale = snapshot.advance(&base, first.clone(), Entity::new()?);
    assert!(
        matches!(stale, Err(PublishError::VersionMismatch { .. })),
        "an advance built on a superseded head is refused: {stale:?}"
    );
    assert_eq!(snapshot.revision(), first, "the winning commit stands");
    Ok(())
}

/// Commit-time induction runs on a snapshot transaction exactly as on
/// a branch: a dispatched command fires the committed rule, the rule's
/// durable head folds into the snapshot's commit, and the command
/// itself is never written. The branch is untouched.
#[dialog_common::test]
async fn it_induces_on_commit() -> Result<()> {
    use dialog_query::InductiveRule;

    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;

    let increment: InductiveRule = serde_json::from_value(serde_json::json!({
        "description": "Increment a counter on an increment command",
        "assert!": {
            "with": { "count": { "the": "counter/count", "as": "UnsignedInteger" } }
        },
        "when": [
            {
                "assert": {
                    "with": { "counter": { "the": "cmd.increment/counter", "as": "Entity" } }
                },
                "where": { "counter": { "?": { "name": "this" } } }
            },
            {
                "assert": {
                    "with": { "count": { "the": "counter/count", "as": "UnsignedInteger" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "count": { "?": { "name": "prev" } }
                }
            },
            {
                "assert": "math/sum",
                "where": {
                    "of": { "?": { "name": "prev" } },
                    "with": 1,
                    "is": { "?": { "name": "count" } }
                }
            }
        ]
    }))
    .expect("increment rule compiles");

    let counter: Entity = "ctr:1".parse()?;
    branch
        .transaction()
        .assert(increment)
        .assert(the!("counter/count").of(counter.clone()).is(1u64))
        .commit()
        .perform(&operator)
        .await?;
    let snapshot = branch.snapshot().expect("snapshot");

    let command: Entity = "cmd:1".parse()?;
    snapshot
        .transaction()
        .dispatch(
            the!("cmd.increment/counter")
                .of(command.clone())
                .is(counter.clone()),
        )
        .commit()
        .perform(&operator)
        .await?;

    assert_eq!(
        values(&snapshot, &operator, "counter/count", Some(&counter)).await?,
        vec![Value::UnsignedInt(2)],
        "the rule's durable head folded into the snapshot's commit"
    );
    assert!(
        values(
            &snapshot,
            &operator,
            "cmd.increment/counter",
            Some(&command)
        )
        .await?
        .is_empty(),
        "the dispatched command never reaches the tree"
    );
    assert_eq!(
        values(&branch, &operator, "counter/count", Some(&counter)).await?,
        vec![Value::UnsignedInt(1)],
        "the branch is untouched"
    );

    // The next transaction on the snapshot induces against the head it
    // just minted: the counter goes on from 2, not from the branch's 1.
    snapshot
        .transaction()
        .dispatch(
            the!("cmd.increment/counter")
                .of(command)
                .is(counter.clone()),
        )
        .commit()
        .perform(&operator)
        .await?;
    assert_eq!(
        values(&snapshot, &operator, "counter/count", Some(&counter)).await?,
        vec![Value::UnsignedInt(3)]
    );
    Ok(())
}

/// A deductive rule staged in the transaction resolves in its query,
/// uncommitted, as on a branch.
#[dialog_common::test]
async fn it_resolves_rules_pending_in_the_transaction() -> Result<()> {
    use dialog_query::rule::DeductiveRuleDescriptor;
    use dialog_query::{ConceptQuery, Parameters};

    let (operator, _, _, _, snapshot) = staged().await?;
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
    let transaction = snapshot.transaction().assert(&rule).assert(
        the!("org/person-name")
            .of(alice.clone())
            .is("Alice".to_string()),
    );

    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("name".into(), Term::var("name"));
    let rows = transaction
        .query()
        .select(ConceptQuery {
            predicate: employee,
            terms,
        })
        .perform(&operator)
        .try_vec()
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(*rows[0].entity(), alice);
    Ok(())
}

#[dialog_common::test]
async fn it_integrates_external_changes() -> Result<()> {
    let (operator, _, _, _, snapshot) = staged().await?;
    let mut external = Changes::new();
    external.assert(name("user:bob", "Bob"));
    external.retract(name("user:alice", "Alice"));

    snapshot
        .transaction()
        .integrate(external)
        .commit()
        .perform(&operator)
        .await?;
    assert_eq!(names(&snapshot, &operator).await?, vec!["Bob".to_string()]);
    Ok(())
}

/// The session overlay stays with the snapshot across commits and is
/// never written.
#[dialog_common::test]
async fn it_keeps_the_overlay_across_commits() -> Result<()> {
    let (operator, _, _, _, snapshot) = staged().await?;
    snapshot.overlay().assert(name("user:ghost", "Ghost"));
    snapshot
        .transaction()
        .assert(name("user:bob", "Bob"))
        .commit()
        .perform(&operator)
        .await?;
    assert_eq!(
        queried_names(snapshot.query(), &operator).await?,
        vec!["Alice".to_string(), "Bob".to_string(), "Ghost".to_string()]
    );
    assert_eq!(
        names(&snapshot, &operator).await?,
        vec!["Alice".to_string(), "Bob".to_string()],
        "the overlay fact was never committed"
    );
    Ok(())
}

/// The derived ancestry closure of a snapshot-minted revision runs
/// through the base into the branch's history.
#[dialog_common::test]
async fn it_derives_ancestry_through_the_base() -> Result<()> {
    let (operator, _, _, branch, snapshot) = staged().await?;
    let base = snapshot.revision();
    assert_eq!(Some(base.clone()), branch.revision());

    let first = snapshot
        .transaction()
        .assert(name("user:bob", "Bob"))
        .commit()
        .perform(&operator)
        .await?;
    let second = snapshot
        .transaction()
        .assert(name("user:carol", "Carol"))
        .commit()
        .perform(&operator)
        .await?;

    let mut ancestors: Vec<Entity> = snapshot
        .query()
        .select(Query::<schema::RevisionAncestor> {
            this: second.entity().into(),
            ancestor: Term::var("ancestor"),
        })
        .perform(&operator)
        .try_vec()
        .await?
        .into_iter()
        .map(|row| row.ancestor.0)
        .collect();
    ancestors.sort();
    let mut expected = vec![base.entity(), first.entity()];
    expected.sort();
    assert_eq!(ancestors, expected);
    Ok(())
}

/// A commit through a snapshot minted cold from the repository (never a
/// branch handle) works the same: the base's blocks are read from
/// storage, the new ones written to it.
#[dialog_common::test]
async fn it_commits_through_a_cold_handle() -> Result<()> {
    let (operator, _, repo, _, snapshot) = staged().await?;
    let cold = repo.snapshot(snapshot.revision());
    let minted = cold
        .commit(stream::iter(vec![Instruction::Assert(fact(
            "user:bob", "Bob",
        ))]))
        .canonicalize()
        .perform(&operator)
        .await?;
    assert_eq!(cold.revision(), minted);
    assert_eq!(
        names(&cold, &operator).await?,
        vec!["Alice".to_string(), "Bob".to_string()]
    );
    let again = repo.snapshot(minted);
    assert_eq!(
        names(&again, &operator).await?,
        vec!["Alice".to_string(), "Bob".to_string()]
    );
    Ok(())
}
