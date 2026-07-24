//! Integration tests using provisioned S3 and UCAN test servers.
//!
//! These tests require `--features integration-tests` and spin up real
//! local S3 (and UCAN access) servers via `#[dialog_common::test]`.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::helpers::{test_operator_with_profile, unique_name};
use crate::{Blob, Branch, Repository, RepositoryExt as _, SiteAddress};
use anyhow::Result;
use dialog_artifacts::{Artifact, ArtifactSelector, Instruction, Value};
use dialog_capability::Subject;
use dialog_credentials::SignerCredential;
use dialog_effects::blob::BlobError;
use dialog_network::Network;
use dialog_operator::{Operator, Profile};
use dialog_remote_s3::helpers::S3Address;
use dialog_remote_s3::{Address as S3SiteAddress, S3Credential};
use dialog_storage::provider::storage::{Storage, VolatileSpace};
use futures_util::{StreamExt, stream};

fn s3_site_address(s3: &S3Address) -> S3SiteAddress {
    S3SiteAddress::builder(&s3.endpoint)
        .region("us-east-1")
        .bucket(&s3.bucket)
        .build()
        .unwrap()
}

async fn setup_repo_with_s3_remote(
    operator: &Operator<VolatileSpace>,
    profile: &Profile,
    s3: &S3Address,
    name: &str,
) -> Result<(Repository<SignerCredential>, Branch)> {
    let repo = profile
        .repository(unique_name(name))
        .create()
        .perform(operator)
        .await?;

    let site_address = s3_site_address(s3);

    // Save S3 credentials so the Operator can authorize fork requests
    let authorization = S3Credential::new(&s3.access_key_id, &s3.secret_access_key);
    profile
        .credential()
        .site(&site_address)
        .save(authorization)
        .perform(operator)
        .await?;

    let origin = repo
        .remote("origin")
        .create(site_address)
        .perform(operator)
        .await?;

    let branch = repo.branch("main").open().perform(operator).await?;
    let remote_branch = origin.branch("main").open().perform(operator).await?;
    branch.set_upstream(remote_branch).perform(operator).await?;

    Ok((repo, branch))
}

#[dialog_common::test]
async fn it_pushes_to_s3_remote(s3: S3Address) -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let (_repo, branch) = setup_repo_with_s3_remote(&operator, &profile, &s3, "push").await?;

    let artifact = Artifact {
        the: "user/name".parse()?,
        of: "user:1".parse()?,
        is: Value::String("Alice".into()),
        cause: None,
    };
    branch
        .commit(stream::iter(vec![Instruction::Assert(artifact)]))
        .perform(&operator)
        .await?;

    let result = branch.push().perform(&operator).await?;
    assert!(result.is_some(), "push should succeed");

    Ok(())
}

#[dialog_common::test]
async fn it_fetches_from_s3_remote(s3: S3Address) -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let (_repo, branch) = setup_repo_with_s3_remote(&operator, &profile, &s3, "fetch").await?;

    let artifact = Artifact {
        the: "user/name".parse()?,
        of: "user:1".parse()?,
        is: Value::String("Alice".into()),
        cause: None,
    };
    branch
        .commit(stream::iter(vec![Instruction::Assert(artifact)]))
        .perform(&operator)
        .await?;

    branch.push().perform(&operator).await?;

    let fetched = branch.fetch().perform(&operator).await?;
    assert!(fetched.is_some(), "fetch should find remote state");

    Ok(())
}

#[dialog_common::test]
async fn it_push_and_pull_roundtrip(s3: S3Address) -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let (_repo, branch) = setup_repo_with_s3_remote(&operator, &profile, &s3, "roundtrip").await?;

    let artifact = Artifact {
        the: "user/name".parse()?,
        of: "user:1".parse()?,
        is: Value::String("Alice".into()),
        cause: None,
    };
    branch
        .commit(stream::iter(vec![Instruction::Assert(artifact)]))
        .perform(&operator)
        .await?;

    branch.push().perform(&operator).await?;

    assert!(
        branch.upstream().is_some(),
        "should have upstream after push"
    );

    Ok(())
}

/// Push ships newly-referenced blob bytes to the remote before publishing, so a
/// second site sharing the remote can pull the revision and read a blob it never
/// wrote — exercising the push blob-upload hook and Task 4's remote-hydration
/// path end to end.
///
/// Both sites run over their own temp-dir native space (`Storage::temp()`): the
/// volatile space used elsewhere has no blob provider, and the two sites need
/// independent local blob stores so site B's read is a genuine local miss.
#[dialog_common::test]
async fn it_ships_blobs_on_push_and_hydrates_on_read(s3: S3Address) -> Result<()> {
    // --- Site A: write a blob, reference it, push. ---
    let storage_a = Storage::temp();
    let profile_a = Profile::open(unique_name("blob-ship-a"))
        .perform(&storage_a)
        .await?;
    let operator_a = profile_a
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage_a)
        .await?;

    let repo_a = profile_a
        .repository(unique_name("blob-ship"))
        .create()
        .perform(&operator_a)
        .await?;

    let site_a = s3_site_address(&s3);
    profile_a
        .credential()
        .site(&site_a)
        .save(S3Credential::new(&s3.access_key_id, &s3.secret_access_key))
        .perform(&operator_a)
        .await?;

    let origin_a = repo_a
        .remote("origin")
        .create(site_a)
        .perform(&operator_a)
        .await?;
    let branch_a = repo_a.branch("main").open().perform(&operator_a).await?;
    let remote_branch_a = origin_a.branch("main").open().perform(&operator_a).await?;
    branch_a
        .set_upstream(remote_branch_a)
        .perform(&operator_a)
        .await?;

    let payload: Vec<u8> = (0..50_000u32).map(|i| (i % 199) as u8).collect();
    let chunks: Vec<Result<Vec<u8>, BlobError>> =
        payload.chunks(8192).map(|c| Ok(c.to_vec())).collect();
    let blob = Blob::import(stream::iter(chunks))
        .write((&branch_a).into())
        .perform(&operator_a)
        .await?;
    assert!(branch_a.push().perform(&operator_a).await?.is_some());

    // --- Site B: same remote subject, separate local store; pull then read. ---
    let storage_b = Storage::temp();
    let profile_b = Profile::open(unique_name("blob-ship-b"))
        .perform(&storage_b)
        .await?;
    let operator_b = profile_b
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage_b)
        .await?;

    let repo_b = profile_b
        .repository(unique_name("blob-ship-b-repo"))
        .open()
        .perform(&operator_b)
        .await?;

    let site_b = s3_site_address(&s3);
    profile_b
        .credential()
        .site(&site_b)
        .save(S3Credential::new(&s3.access_key_id, &s3.secret_access_key))
        .perform(&operator_b)
        .await?;

    let origin_b = repo_b
        .remote("origin")
        .create(site_b)
        .subject(repo_a.did())
        .perform(&operator_b)
        .await?;
    let branch_b = repo_b.branch("main").open().perform(&operator_b).await?;
    let remote_branch_b = origin_b.branch("main").open().perform(&operator_b).await?;
    branch_b
        .set_upstream(remote_branch_b)
        .perform(&operator_b)
        .await?;

    branch_b.pull().perform(&operator_b).await?;

    assert_eq!(
        Blob::from(blob.clone())
            .size((&branch_b).into())
            .perform(&operator_b)
            .await?,
        Some(payload.len() as u64)
    );

    let mut reader = Blob::from(blob)
        .read((&branch_b).into())
        .perform(&operator_b)
        .await?;
    let mut out = Vec::new();
    while let Some(chunk) = reader.next().await? {
        out.extend(chunk);
    }
    assert_eq!(out, payload);

    Ok(())
}

/// Push ships a spilling scalar value's block to the remote before publishing.
///
/// A value larger than the tree's inline threshold does not travel in the key
/// or the fact payload; its bytes are a content-addressed block in the archive,
/// keyed by the value's 32-byte reference. The push spilled-ref differential
/// must surface that block so it lands on the remote alongside the tree nodes.
///
/// Proven two ways: (1) the block is directly readable from the remote archive
/// under its value reference, byte-equal to the value's bytes; and (2) a second
/// site with an entirely separate local store pulls the revision and selects
/// the fact back, reconstructing the exact `Value` it never wrote locally —
/// only possible if the spilled block reached the remote. A same-store local
/// select on site A confirms the round-trip end too.
#[dialog_common::test]
async fn it_ships_spilled_values_on_push_and_hydrates_on_read(s3: S3Address) -> Result<()> {
    // A value comfortably larger than the inline threshold, so its key spills to
    // a 32-byte reference and its bytes become a separate archive block.
    let inline_n = dialog_search_tree::Manifest::default().inline_n as usize;
    let big = "x".repeat(inline_n + 1);
    let value = Value::String(big.clone());
    let reference = value.to_reference();

    // --- Site A: commit a spilling fact, push. ---
    let storage_a = Storage::temp();
    let profile_a = Profile::open(unique_name("spill-ship-a"))
        .perform(&storage_a)
        .await?;
    let operator_a = profile_a
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage_a)
        .await?;

    let repo_a = profile_a
        .repository(unique_name("spill-ship"))
        .create()
        .perform(&operator_a)
        .await?;

    let site_a = s3_site_address(&s3);
    profile_a
        .credential()
        .site(&site_a)
        .save(S3Credential::new(&s3.access_key_id, &s3.secret_access_key))
        .perform(&operator_a)
        .await?;

    let origin_a = repo_a
        .remote("origin")
        .create(site_a)
        .perform(&operator_a)
        .await?;
    let branch_a = repo_a.branch("main").open().perform(&operator_a).await?;
    let remote_branch_a = origin_a.branch("main").open().perform(&operator_a).await?;
    branch_a
        .set_upstream(remote_branch_a)
        .perform(&operator_a)
        .await?;

    let artifact = Artifact {
        the: "doc/body".parse()?,
        of: "doc:1".parse()?,
        is: value.clone(),
        cause: None,
    };
    branch_a
        .commit(stream::iter(vec![Instruction::Assert(artifact)]))
        .perform(&operator_a)
        .await?;

    assert!(branch_a.push().perform(&operator_a).await?.is_some());

    // A same-store local select reconstructs the spilled value.
    let local: Vec<_> = branch_a
        .claims()
        .select(ArtifactSelector::new().the("doc/body".parse()?))
        .perform(&operator_a)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(local.len(), 1, "site A should read its own spilled fact");
    assert_eq!(local[0].is, value, "local select reconstructs the value");

    // The spilled block itself is present on the REMOTE archive, byte-equal to
    // the value's bytes, under the value's 32-byte reference.
    let remote_block = origin_a
        .archive()
        .index()
        .get(reference)
        .perform(&operator_a)
        .await?;
    assert_eq!(
        remote_block,
        Some(value.to_bytes()),
        "the spilled value block must be on the remote after push"
    );

    // --- Site B: same remote subject, separate local store; pull then select. ---
    let storage_b = Storage::temp();
    let profile_b = Profile::open(unique_name("spill-ship-b"))
        .perform(&storage_b)
        .await?;
    let operator_b = profile_b
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage_b)
        .await?;

    let repo_b = profile_b
        .repository(unique_name("spill-ship-b-repo"))
        .open()
        .perform(&operator_b)
        .await?;

    let site_b = s3_site_address(&s3);
    profile_b
        .credential()
        .site(&site_b)
        .save(S3Credential::new(&s3.access_key_id, &s3.secret_access_key))
        .perform(&operator_b)
        .await?;

    let origin_b = repo_b
        .remote("origin")
        .create(site_b)
        .subject(repo_a.did())
        .perform(&operator_b)
        .await?;
    let branch_b = repo_b.branch("main").open().perform(&operator_b).await?;
    let remote_branch_b = origin_b.branch("main").open().perform(&operator_b).await?;
    branch_b
        .set_upstream(remote_branch_b)
        .perform(&operator_b)
        .await?;

    branch_b.pull().perform(&operator_b).await?;

    // Site B never wrote the value locally; reconstructing it from its own store
    // proves the spilled block was shipped to the remote and hydrated on pull.
    let remote_side: Vec<_> = branch_b
        .claims()
        .select(ArtifactSelector::new().the("doc/body".parse()?))
        .perform(&operator_b)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(remote_side.len(), 1, "site B should read the pulled fact");
    assert_eq!(
        remote_side[0].is, value,
        "site B reconstructs the spilled value from the remote-shipped block"
    );

    Ok(())
}

/// A replica that pulled a spilled fact (pull ships tree nodes, never value
/// blocks) can retract it and push WITHOUT ever having read the value: a
/// retraction writes tombstones at the spilled keys, and tombstones must not
/// demand the value block from the local archive — requiring it would wedge
/// this replica's push forever, since nothing ever writes the block locally.
#[dialog_common::test]
async fn it_pushes_a_retraction_of_a_pulled_spilled_fact(s3: S3Address) -> Result<()> {
    let inline_n = dialog_search_tree::Manifest::default().inline_n as usize;
    let artifact = Artifact {
        the: "doc/body".parse()?,
        of: "doc:1".parse()?,
        is: Value::String("x".repeat(inline_n + 1)),
        cause: None,
    };

    // --- Site A: commit the spilling fact, push. ---
    let storage_a = Storage::temp();
    let profile_a = Profile::open(unique_name("spill-retract-a"))
        .perform(&storage_a)
        .await?;
    let operator_a = profile_a
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage_a)
        .await?;
    let repo_a = profile_a
        .repository(unique_name("spill-retract"))
        .create()
        .perform(&operator_a)
        .await?;
    let site_a = s3_site_address(&s3);
    profile_a
        .credential()
        .site(&site_a)
        .save(S3Credential::new(&s3.access_key_id, &s3.secret_access_key))
        .perform(&operator_a)
        .await?;
    let origin_a = repo_a
        .remote("origin")
        .create(site_a)
        .perform(&operator_a)
        .await?;
    let branch_a = repo_a.branch("main").open().perform(&operator_a).await?;
    let remote_branch_a = origin_a.branch("main").open().perform(&operator_a).await?;
    branch_a
        .set_upstream(remote_branch_a)
        .perform(&operator_a)
        .await?;
    branch_a
        .commit(stream::iter(vec![Instruction::Assert(artifact.clone())]))
        .perform(&operator_a)
        .await?;
    assert!(branch_a.push().perform(&operator_a).await?.is_some());

    // --- Site B: separate local store; pull, retract WITHOUT selecting, push. ---
    let storage_b = Storage::temp();
    let profile_b = Profile::open(unique_name("spill-retract-b"))
        .perform(&storage_b)
        .await?;
    let operator_b = profile_b
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage_b)
        .await?;
    let repo_b = profile_b
        .repository(unique_name("spill-retract-b-repo"))
        .open()
        .perform(&operator_b)
        .await?;
    let site_b = s3_site_address(&s3);
    profile_b
        .credential()
        .site(&site_b)
        .save(S3Credential::new(&s3.access_key_id, &s3.secret_access_key))
        .perform(&operator_b)
        .await?;
    let origin_b = repo_b
        .remote("origin")
        .create(site_b)
        .subject(repo_a.did())
        .perform(&operator_b)
        .await?;
    let branch_b = repo_b.branch("main").open().perform(&operator_b).await?;
    let remote_branch_b = origin_b.branch("main").open().perform(&operator_b).await?;
    branch_b
        .set_upstream(remote_branch_b)
        .perform(&operator_b)
        .await?;
    branch_b.pull().perform(&operator_b).await?;

    // The retraction is constructed from application state; site B never
    // selected the fact, so its local archive has no spilled block.
    branch_b
        .commit(stream::iter(vec![Instruction::Retract(artifact.clone())]))
        .perform(&operator_b)
        .await?;
    assert!(
        branch_b.push().perform(&operator_b).await?.is_some(),
        "a tombstone push must not demand the spilled block locally"
    );

    // --- Site A observes the retraction. ---
    branch_a.pull().perform(&operator_a).await?;
    let remaining: Vec<_> = branch_a
        .claims()
        .select(ArtifactSelector::new().the("doc/body".parse()?))
        .perform(&operator_a)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert!(
        remaining.is_empty(),
        "the retraction round-trips: {remaining:?}"
    );

    Ok(())
}

/// A subscription's change poll can see a spilled fact that arrived via pull:
/// pull replicates tree nodes but never value blocks, so the poll's spilled
/// fetch must fall back to the branch's remote exactly as a select does.
#[dialog_common::test]
async fn it_polls_subscriptions_over_pulled_spilled_facts(s3: S3Address) -> Result<()> {
    use dialog_query::attribute::The;
    use dialog_query::{AttributeQuery, Term, the};

    let inline_n = dialog_search_tree::Manifest::default().inline_n as usize;
    let body = "b".repeat(inline_n + 1);

    // --- Site A: repo + remote. ---
    let storage_a = Storage::temp();
    let profile_a = Profile::open(unique_name("spill-sub-a"))
        .perform(&storage_a)
        .await?;
    let operator_a = profile_a
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage_a)
        .await?;
    let repo_a = profile_a
        .repository(unique_name("spill-sub"))
        .create()
        .perform(&operator_a)
        .await?;
    let site_a = s3_site_address(&s3);
    profile_a
        .credential()
        .site(&site_a)
        .save(S3Credential::new(&s3.access_key_id, &s3.secret_access_key))
        .perform(&operator_a)
        .await?;
    let origin_a = repo_a
        .remote("origin")
        .create(site_a)
        .perform(&operator_a)
        .await?;
    let branch_a = repo_a.branch("main").open().perform(&operator_a).await?;
    let remote_branch_a = origin_a.branch("main").open().perform(&operator_a).await?;
    branch_a
        .set_upstream(remote_branch_a)
        .perform(&operator_a)
        .await?;

    // --- Site B: separate store, subscribed to doc bodies. ---
    let storage_b = Storage::temp();
    let profile_b = Profile::open(unique_name("spill-sub-b"))
        .perform(&storage_b)
        .await?;
    let operator_b = profile_b
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage_b)
        .await?;
    let repo_b = profile_b
        .repository(unique_name("spill-sub-b-repo"))
        .open()
        .perform(&operator_b)
        .await?;
    let site_b = s3_site_address(&s3);
    profile_b
        .credential()
        .site(&site_b)
        .save(S3Credential::new(&s3.access_key_id, &s3.secret_access_key))
        .perform(&operator_b)
        .await?;
    let origin_b = repo_b
        .remote("origin")
        .create(site_b)
        .subject(repo_a.did())
        .perform(&operator_b)
        .await?;
    let branch_b = repo_b.branch("main").open().perform(&operator_b).await?;
    let remote_branch_b = origin_b.branch("main").open().perform(&operator_b).await?;
    branch_b
        .set_upstream(remote_branch_b)
        .perform(&operator_b)
        .await?;

    let query = AttributeQuery::from(
        Term::<The>::from(the!("doc/body"))
            .of(Term::<dialog_artifacts::Entity>::var("e"))
            .is(Term::<String>::var("v")),
    );
    let mut subscription = branch_b.subscribe(query);
    let initial = subscription
        .poll(&operator_b)
        .await?
        .expect("the initial poll evaluates");
    assert!(initial.asserted.is_empty(), "nothing published yet");

    // --- Site A publishes a spilled fact; B pulls and polls. ---
    branch_a
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "doc/body".parse()?,
            of: "doc:1".parse()?,
            is: Value::String(body.clone()),
            cause: None,
        })]))
        .perform(&operator_a)
        .await?;
    assert!(branch_a.push().perform(&operator_a).await?.is_some());

    branch_b.pull().perform(&operator_b).await?;

    let delta = subscription
        .poll(&operator_b)
        .await?
        .expect("the pulled spilled fact must surface as a delta");
    assert_eq!(delta.asserted.len(), 1, "one asserted row: {delta:?}");
    assert_eq!(
        delta.asserted[0].is,
        Value::String(body),
        "the spilled value reconstructs through the remote fallback"
    );

    Ok(())
}

#[dialog_common::test]
async fn it_pull_returns_none_when_no_changes(s3: S3Address) -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let (_repo, branch) = setup_repo_with_s3_remote(&operator, &profile, &s3, "no-change").await?;

    let artifact = Artifact {
        the: "user/name".parse()?,
        of: "user:1".parse()?,
        is: Value::String("Alice".into()),
        cause: None,
    };
    branch
        .commit(stream::iter(vec![Instruction::Assert(artifact)]))
        .perform(&operator)
        .await?;

    branch.push().perform(&operator).await?;

    // Pull immediately after push — no new changes
    let pull_result = branch.pull().perform(&operator).await?;
    assert!(
        pull_result.is_none(),
        "pull with no changes should return None"
    );

    Ok(())
}

#[dialog_common::test]
async fn it_pushes_and_pulls_data_between_repos(s3: S3Address) -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;

    // Alice creates repo, commits, and pushes
    let (alice_repo, alice_branch) =
        setup_repo_with_s3_remote(&operator, &profile, &s3, "alice").await?;

    let artifact = Artifact {
        the: "user/name".parse()?,
        of: "user:alice".parse()?,
        is: Value::String("Alice".into()),
        cause: None,
    };
    alice_branch
        .commit(stream::iter(vec![Instruction::Assert(artifact)]))
        .perform(&operator)
        .await?;

    alice_branch.push().perform(&operator).await?;

    // Bob opens a second repo sharing Alice's subject, pulls
    let bob_repo = profile
        .repository(unique_name("bob"))
        .open()
        .perform(&operator)
        .await?;

    let origin = bob_repo
        .remote("origin")
        .create(s3_site_address(&s3))
        .subject(alice_repo.did())
        .perform(&operator)
        .await?;

    let bob_branch = bob_repo.branch("main").open().perform(&operator).await?;
    let remote_branch = origin.branch("main").open().perform(&operator).await?;
    bob_branch
        .set_upstream(remote_branch)
        .perform(&operator)
        .await?;

    let pull_result = bob_branch.pull().perform(&operator).await?;
    assert!(pull_result.is_some(), "Bob's pull should find Alice's data");

    // Verify Bob can query Alice's artifact
    let results: Vec<_> = bob_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(results.len(), 1, "Bob should have Alice's artifact");
    assert_eq!(
        results[0].is,
        Value::String("Alice".into()),
        "artifact value should match"
    );

    Ok(())
}

#[dialog_common::test]
async fn it_two_party_convergence(s3: S3Address) -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;

    // Alice commits and pushes
    let (alice_repo, alice_branch) =
        setup_repo_with_s3_remote(&operator, &profile, &s3, "conv-alice").await?;

    alice_branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:alice".parse()?,
            is: Value::String("Alice".into()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

    alice_branch.push().perform(&operator).await?;

    // Bob sets up repo pointing at same remote subject
    let bob_repo = profile
        .repository(unique_name("conv-bob"))
        .open()
        .perform(&operator)
        .await?;

    let origin = bob_repo
        .remote("origin")
        .create(s3_site_address(&s3))
        .subject(alice_repo.did())
        .perform(&operator)
        .await?;

    let bob_branch = bob_repo.branch("main").open().perform(&operator).await?;
    let remote_branch = origin.branch("main").open().perform(&operator).await?;
    bob_branch
        .set_upstream(remote_branch)
        .perform(&operator)
        .await?;

    // Bob pulls Alice's changes
    bob_branch.pull().perform(&operator).await?;

    // Bob commits his own artifact
    bob_branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:bob".parse()?,
            is: Value::String("Bob".into()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

    // Bob pushes
    bob_branch.push().perform(&operator).await?;

    // Alice pulls Bob's changes
    alice_branch.pull().perform(&operator).await?;

    // Both should have both artifacts
    let alice_results: Vec<_> = alice_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    let bob_results: Vec<_> = bob_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(
        alice_results.len(),
        2,
        "Alice should have both artifacts after pull"
    );
    assert_eq!(
        bob_results.len(),
        2,
        "Bob should have both artifacts after push"
    );

    Ok(())
}

// UCAN integration tests

use dialog_remote_ucan_s3::UcanAddress;
use dialog_remote_ucan_s3::helpers::UcanS3Address;

/// Alice creates a repo, delegates to Bob, Bob pulls, commits, pushes,
/// then Alice pulls Bob's changes.
#[dialog_common::test]
async fn it_collaborates_via_ucan_delegation(ucan: UcanS3Address) -> Result<()> {
    // Alice: create profile, operator, repo
    let (alice_operator, alice_profile) = test_operator_with_profile().await;
    let alice_repo = alice_profile
        .repository(unique_name("collab-alice"))
        .create()
        .perform(&alice_operator)
        .await?;

    // Delegate repo ownership to Alice's profile
    let alice_access = alice_repo.access();
    let ownership_chain = alice_access
        .claim(&alice_repo)
        .delegate(alice_profile.did())
        .perform(&alice_operator)
        .await?;
    alice_profile
        .access()
        .save(ownership_chain)
        .perform(&alice_operator)
        .await?;

    // Set up UCAN remote on Alice's repo
    let ucan_site = SiteAddress::Ucan(UcanAddress::new(&ucan.access_service_url));
    let alice_origin = alice_repo
        .remote("origin")
        .create(ucan_site.clone())
        .perform(&alice_operator)
        .await?;

    let alice_branch = alice_repo
        .branch("main")
        .open()
        .perform(&alice_operator)
        .await?;
    let remote_branch = alice_origin
        .branch("main")
        .open()
        .perform(&alice_operator)
        .await?;
    alice_branch
        .set_upstream(remote_branch)
        .perform(&alice_operator)
        .await?;

    // Alice commits and pushes initial data
    alice_branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:alice".parse()?,
            is: Value::String("Alice".into()),
            cause: None,
        })]))
        .perform(&alice_operator)
        .await?;

    alice_branch.push().perform(&alice_operator).await?;

    // Bob: create profile, operator
    let (bob_operator, bob_profile) = test_operator_with_profile().await;

    // Alice delegates repo access to Bob's profile
    let delegation_to_bob = alice_profile
        .access()
        .claim(&alice_repo)
        .delegate(bob_profile.did())
        .perform(&alice_operator)
        .await?;

    // Bob saves the delegation chain under his profile
    bob_profile
        .access()
        .save(delegation_to_bob)
        .perform(&bob_operator)
        .await?;

    // Bob creates his own repo (different DID) and adds Alice's remote
    let bob_repo = bob_profile
        .repository(unique_name("collab-bob"))
        .open()
        .perform(&bob_operator)
        .await?;

    let bob_origin = bob_repo
        .remote("origin")
        .create(ucan_site)
        .subject(alice_repo.did())
        .perform(&bob_operator)
        .await?;

    let bob_branch = bob_repo
        .branch("main")
        .open()
        .perform(&bob_operator)
        .await?;
    let remote_branch = bob_origin
        .branch("main")
        .open()
        .perform(&bob_operator)
        .await?;
    bob_branch
        .set_upstream(remote_branch)
        .perform(&bob_operator)
        .await?;

    // Bob pulls Alice's data
    let pull_result = bob_branch.pull().perform(&bob_operator).await?;
    assert!(pull_result.is_some(), "Bob should pull Alice's data");

    // Verify Bob has Alice's artifact
    let bob_results: Vec<_> = bob_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&bob_operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(bob_results.len(), 1, "Bob should have Alice's artifact");

    // Bob commits his own change
    bob_branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:bob".parse()?,
            is: Value::String("Bob".into()),
            cause: None,
        })]))
        .perform(&bob_operator)
        .await?;

    // Bob pushes
    let push_result = bob_branch.push().perform(&bob_operator).await?;
    assert!(push_result.is_some(), "Bob should push successfully");

    // Alice pulls Bob's changes
    let alice_pull = alice_branch.pull().perform(&alice_operator).await?;
    assert!(alice_pull.is_some(), "Alice should pull Bob's changes");

    // Alice should have both artifacts
    let alice_results: Vec<_> = alice_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&alice_operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        alice_results.len(),
        2,
        "Alice should have both artifacts after pulling Bob's changes"
    );

    Ok(())
}

/// Push and pull via UCAN access service.
#[dialog_common::test]
async fn it_pushes_and_pulls_via_ucan(ucan: UcanS3Address) -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;

    // Create repo and delegate ownership to the profile
    let repo = profile
        .repository(unique_name("ucan-repo"))
        .create()
        .perform(&operator)
        .await?;

    let repo_access = repo.access();
    let chain = repo_access
        .claim(&repo)
        .delegate(profile.did())
        .perform(&operator)
        .await?;
    profile.access().save(chain).perform(&operator).await?;

    // Set up UCAN remote
    let origin = repo
        .remote("origin")
        .create(SiteAddress::Ucan(UcanAddress::new(
            &ucan.access_service_url,
        )))
        .perform(&operator)
        .await?;

    let branch = repo.branch("main").open().perform(&operator).await?;
    let remote_branch = origin.branch("main").open().perform(&operator).await?;
    branch
        .set_upstream(remote_branch)
        .perform(&operator)
        .await?;

    // Commit and push via UCAN
    branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:ucan-test".parse()?,
            is: Value::String("UCAN User".into()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

    let push_result = branch.push().perform(&operator).await?;
    assert!(push_result.is_some(), "UCAN push should succeed");

    // Pull should find no changes (just pushed)
    let pull_result = branch.pull().perform(&operator).await?;
    assert!(pull_result.is_none(), "pull after push should return None");

    // Verify data survives select
    let results: Vec<_> = branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(results.len(), 1, "should have the pushed artifact");
    assert_eq!(results[0].is, Value::String("UCAN User".into()));

    Ok(())
}

/// Query an empty local replica. Data replicates on demand from the
/// remote. After removing the upstream, data is still available locally.
#[dialog_common::test]
async fn it_replicates_on_demand_and_caches_locally(s3: S3Address) -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;

    // Alice: create repo, commit data, push to remote
    let (alice_repo, alice_branch) =
        setup_repo_with_s3_remote(&operator, &profile, &s3, "replicate-alice").await?;

    alice_branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:alice".parse()?,
            is: Value::String("Alice".into()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;
    alice_branch.push().perform(&operator).await?;
    let alice_revision = alice_branch.revision().expect("should have revision");

    // Bob: empty repo pointing at Alice's remote
    let bob_repo = profile
        .repository(unique_name("replicate-bob"))
        .open()
        .perform(&operator)
        .await?;

    let origin = bob_repo
        .remote("origin")
        .create(s3_site_address(&s3))
        .subject(alice_repo.did())
        .perform(&operator)
        .await?;

    let bob_branch = bob_repo.branch("main").open().perform(&operator).await?;

    // Set Bob's revision to Alice's without pulling blocks
    bob_branch.reset(alice_revision).perform(&operator).await?;

    // Without any remote upstream tracked there is nothing to fall back
    // to, so reads of the unreplicated tree fail. (Upstreams accumulate —
    // `set_upstream` re-points the default but keeps tracking the rest —
    // so this check must run before the remote is ever tracked.)
    let no_remote_result = bob_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&operator)
        .await;
    assert!(
        no_remote_result.is_err(),
        "select should fail without remote when blocks aren't local"
    );

    // Track the remote so fallback can reach it
    let remote_branch = origin.branch("main").open().perform(&operator).await?;
    bob_branch
        .set_upstream(remote_branch)
        .perform(&operator)
        .await?;

    // Now query replicates tree blocks on demand from the remote
    let results: Vec<_> = bob_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(results.len(), 1, "should replicate and find Alice's data");
    assert_eq!(results[0].is, Value::String("Alice".into()));

    // Remove upstream (simulates remote going away) by pointing
    // at a non-existent local branch instead
    let nowhere = bob_repo.branch("nowhere").open().perform(&operator).await?;
    bob_branch.set_upstream(&nowhere).perform(&operator).await?;

    // Query again with no remote. Data should be cached locally.
    let cached_results: Vec<_> = bob_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(
        cached_results.len(),
        1,
        "data should be available from local cache"
    );
    assert_eq!(cached_results[0].is, Value::String("Alice".into()));

    Ok(())
}

/// Delegate repo to profile, push data to S3, pull from a new operator.
#[dialog_common::test]
async fn it_delegates_and_pushes_to_s3(s3: S3Address) -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let repo = profile
        .repository(unique_name("deleg-push"))
        .create()
        .perform(&operator)
        .await?;

    // Delegate repo ownership to the profile
    let chain = repo
        .access()
        .claim(&repo)
        .delegate(profile.did())
        .perform(&operator)
        .await?;
    profile.access().save(chain).perform(&operator).await?;

    // Save S3 credentials and set up remote
    let site_address = s3_site_address(&s3);
    let authorization = S3Credential::new(&s3.access_key_id, &s3.secret_access_key);
    profile
        .credential()
        .site(&site_address)
        .save(authorization)
        .perform(&operator)
        .await?;

    let origin = repo
        .remote("origin")
        .create(site_address)
        .perform(&operator)
        .await?;

    let branch = repo.branch("main").open().perform(&operator).await?;
    let remote_branch = origin.branch("main").open().perform(&operator).await?;
    branch
        .set_upstream(remote_branch)
        .perform(&operator)
        .await?;

    // Commit and push
    branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:delegated".parse()?,
            is: Value::String("Delegated Push".into()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

    let result = branch.push().perform(&operator).await?;
    assert!(result.is_some(), "push with delegation should succeed");

    Ok(())
}

/// Alice delegates, pushes to S3; Bob pulls and verifies data arrived.
#[dialog_common::test]
async fn it_delegates_pushes_and_pulls_via_s3(s3: S3Address) -> Result<()> {
    let (alice_operator, alice_profile) = test_operator_with_profile().await;
    let alice_repo = alice_profile
        .repository(unique_name("deleg-pull-a"))
        .create()
        .perform(&alice_operator)
        .await?;

    // Delegate repo to Alice's profile
    let chain = alice_repo
        .access()
        .claim(&alice_repo)
        .delegate(alice_profile.did())
        .perform(&alice_operator)
        .await?;
    alice_profile
        .access()
        .save(chain)
        .perform(&alice_operator)
        .await?;

    // Save S3 credentials for Alice and set up remote
    let site_address = s3_site_address(&s3);
    let authorization = S3Credential::new(&s3.access_key_id, &s3.secret_access_key);
    alice_profile
        .credential()
        .site(&site_address)
        .save(authorization)
        .perform(&alice_operator)
        .await?;

    let alice_origin = alice_repo
        .remote("origin")
        .create(site_address)
        .perform(&alice_operator)
        .await?;

    let alice_branch = alice_repo
        .branch("main")
        .open()
        .perform(&alice_operator)
        .await?;
    let remote_branch = alice_origin
        .branch("main")
        .open()
        .perform(&alice_operator)
        .await?;
    alice_branch
        .set_upstream(remote_branch)
        .perform(&alice_operator)
        .await?;

    alice_branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:alice".parse()?,
            is: Value::String("Alice Delegated".into()),
            cause: None,
        })]))
        .perform(&alice_operator)
        .await?;

    let push_result = alice_branch.push().perform(&alice_operator).await?;
    assert!(push_result.is_some(), "push should succeed");

    // Bob: fresh operator pulls from the same S3 remote
    let (bob_operator, bob_profile) = test_operator_with_profile().await;
    let bob_repo = bob_profile
        .repository(unique_name("deleg-pull-b"))
        .open()
        .perform(&bob_operator)
        .await?;

    // Save S3 credentials for Bob
    let bob_site_address = s3_site_address(&s3);
    let bob_authorization = S3Credential::new(&s3.access_key_id, &s3.secret_access_key);
    bob_profile
        .credential()
        .site(&bob_site_address)
        .save(bob_authorization)
        .perform(&bob_operator)
        .await?;

    let bob_origin = bob_repo
        .remote("origin")
        .create(bob_site_address)
        .subject(alice_repo.did())
        .perform(&bob_operator)
        .await?;

    let bob_branch = bob_repo
        .branch("main")
        .open()
        .perform(&bob_operator)
        .await?;
    let remote_branch = bob_origin
        .branch("main")
        .open()
        .perform(&bob_operator)
        .await?;
    bob_branch
        .set_upstream(remote_branch)
        .perform(&bob_operator)
        .await?;

    let pull_result = bob_branch.pull().perform(&bob_operator).await?;
    assert!(pull_result.is_some(), "pull should find Alice's data");

    let results: Vec<_> = bob_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&bob_operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(results.len(), 1, "should have Alice's artifact");
    assert_eq!(results[0].is, Value::String("Alice Delegated".into()));

    Ok(())
}
