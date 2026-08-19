//! Integration tests using provisioned S3 and UCAN test servers.
//!
//! These tests require `--features integration-tests` and spin up real
//! local S3 (and UCAN access) servers via `#[dialog_common::test]`.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use dialog_operator::DeriveOperator as _;
use std::collections::HashSet;

use crate::{
    Blob, Branch, Index, Item, NetworkedIndex, Repository, RepositoryArchiveExt as _,
    RepositoryExt as _, Revision, SiteAddress, SnapshotError,
};
use anyhow::{Context as _, Result};
use dialog_artifacts::tree::TreeStorageBridge;
use dialog_artifacts::{
    Artifact, ArtifactSelector, Datum, ENTITY_KEY_TAG, HISTORY_KEY_TAG, Instruction, Key, State,
    Value,
};
use dialog_capability::Subject;
use dialog_common::Blake3Hash as NodeHash;
use dialog_credentials::SignerCredential;
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_operator::helpers::{test_operator_with_profile, unique_name};
// Only the native-only tests below construct one.
#[cfg(not(feature = "web-integration-tests"))]
use dialog_effects::blob::BlobError;
use dialog_network::Network;
use dialog_operator::{Operator, Profile};
use dialog_remote_s3::helpers::S3Address;
use dialog_remote_s3::{Address as S3SiteAddress, S3Credential};
use dialog_search_tree::{
    ArchivedNodeBody, ContentAddressedStorage as TreeStorage, Traversable as _, Visit, into_owned,
};
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
// Native only: this test builds its sites on `Storage::temp()`, which is
// `cfg(not(target_arch = "wasm32"))` because it needs a real temp
// directory. The wasm equivalent is OPFS-backed and not interchangeable.
//
// Gated on the feature rather than the target: under
// `web-integration-tests` the macro emits a *native* wrapper that shells
// out to a wasm subprocess, so a target gate keeps the wrapper while
// removing the test it launches, and the wrapper then fails finding
// nothing to run.
#[cfg(not(feature = "web-integration-tests"))]
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

/// A blob retraction replicates on pull: the tombstoned index entry travels
/// with the tree nodes, so a replica that pulls it stops referencing the
/// blob (`size` answers `None`) and a replica that never hydrated the bytes
/// can no longer fetch them from the remote. Bytes already held locally are
/// untouched: retraction removes the reference, not the content, so a
/// replica that hydrated before the retraction still reads its local copy.
// Native only, feature-gated: same reasoning as
// `it_ships_blobs_on_push_and_hydrates_on_read` above.
#[cfg(not(feature = "web-integration-tests"))]
#[dialog_common::test]
async fn it_replicates_a_blob_retraction_on_pull(s3: S3Address) -> Result<()> {
    // --- Site A: write a blob, push. ---
    let storage_a = Storage::temp();
    let profile_a = Profile::open(unique_name("blob-retract-a"))
        .perform(&storage_a)
        .await?;
    let operator_a = profile_a
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage_a)
        .await?;

    let repo_a = profile_a
        .repository(unique_name("blob-retract"))
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

    // --- Site B: pull and hydrate the bytes while still referenced. ---
    let storage_b = Storage::temp();
    let profile_b = Profile::open(unique_name("blob-retract-b"))
        .perform(&storage_b)
        .await?;
    let operator_b = profile_b
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage_b)
        .await?;
    let repo_b = profile_b
        .repository(unique_name("blob-retract-b-repo"))
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
    let mut reader = Blob::from(blob.clone())
        .read((&branch_b).into())
        .perform(&operator_b)
        .await?;
    let mut out = Vec::new();
    while let Some(chunk) = reader.next().await? {
        out.extend(chunk);
    }
    assert_eq!(out, payload, "site B hydrates the bytes before retraction");

    // --- Site A retracts the blob and pushes the retraction. ---
    Blob::from(blob.clone())
        .retract((&branch_a).into())
        .perform(&operator_a)
        .await?;
    assert!(branch_a.push().perform(&operator_a).await?.is_some());

    // --- Site B pulls the retraction: the reference is gone, the hydrated
    // bytes are not. ---
    branch_b.pull().perform(&operator_b).await?;
    assert_eq!(
        Blob::from(blob.clone())
            .size((&branch_b).into())
            .perform(&operator_b)
            .await?,
        None,
        "a pulled retraction removes the index reference"
    );
    let mut reader = Blob::from(blob.clone())
        .read((&branch_b).into())
        .perform(&operator_b)
        .await?;
    let mut out = Vec::new();
    while let Some(chunk) = reader.next().await? {
        out.extend(chunk);
    }
    assert_eq!(
        out, payload,
        "locally hydrated bytes survive the retraction"
    );

    // --- Site C: fresh replica, pulls after the retraction; it can neither
    // see the reference nor hydrate the bytes. ---
    let storage_c = Storage::temp();
    let profile_c = Profile::open(unique_name("blob-retract-c"))
        .perform(&storage_c)
        .await?;
    let operator_c = profile_c
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage_c)
        .await?;
    let repo_c = profile_c
        .repository(unique_name("blob-retract-c-repo"))
        .open()
        .perform(&operator_c)
        .await?;
    let site_c = s3_site_address(&s3);
    profile_c
        .credential()
        .site(&site_c)
        .save(S3Credential::new(&s3.access_key_id, &s3.secret_access_key))
        .perform(&operator_c)
        .await?;
    let origin_c = repo_c
        .remote("origin")
        .create(site_c)
        .subject(repo_a.did())
        .perform(&operator_c)
        .await?;
    let branch_c = repo_c.branch("main").open().perform(&operator_c).await?;
    let remote_branch_c = origin_c.branch("main").open().perform(&operator_c).await?;
    branch_c
        .set_upstream(remote_branch_c)
        .perform(&operator_c)
        .await?;

    branch_c.pull().perform(&operator_c).await?;
    assert_eq!(
        Blob::from(blob.clone())
            .size((&branch_c).into())
            .perform(&operator_c)
            .await?,
        None,
        "a fresh replica pulls no reference to the retracted blob"
    );
    let refused = Blob::from(blob)
        .read((&branch_c).into())
        .perform(&operator_c)
        .await;
    assert!(
        matches!(
            refused,
            Err(crate::CommitError::Blob(BlobError::NotFound(_)))
        ),
        "an unreferenced blob cannot hydrate: {:?}",
        refused.as_ref().err()
    );

    Ok(())
}

/// A retained delegation replicates like any data: site A retains a chain
/// (facts + envelope blob in one commit) and pushes; site B pulls, finds the
/// delegation by an ordinary value-bound query on `dialog.ucan/audience` (the
/// shape a prover uses), and reads the envelope back byte-identical through
/// blob hydration. A retraction then replicates the same way: after B pulls
/// it, the facts and the blob reference are gone.
// Native only, feature-gated: same reasoning as
// `it_ships_blobs_on_push_and_hydrates_on_read` above.
#[cfg(not(feature = "web-integration-tests"))]
#[dialog_common::test]
async fn it_replicates_retained_delegations(s3: S3Address) -> Result<()> {
    use crate::DELEGATION_AUDIENCE;
    use dialog_capability::access::{Certificate as _, Delegation as _};
    use dialog_credentials::Ed25519Signer;

    // --- Site A: retain a delegation, push. ---
    let storage_a = Storage::temp();
    let profile_a = Profile::open(unique_name("delegation-ship-a"))
        .perform(&storage_a)
        .await?;
    let operator_a = profile_a
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage_a)
        .await?;
    let repo_a = profile_a
        .repository(unique_name("delegation-ship"))
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

    let space = Ed25519Signer::generate().await?;
    let holder = Ed25519Signer::generate().await?;
    let delegation = dialog_ucan_core::DelegationBuilder::new()
        .issuer(dialog_credentials::Signer::from(space.clone()))
        .audience(&holder)
        .subject(dialog_ucan_core::subject::Subject::Specific(
            dialog_varsig::Principal::did(&space),
        ))
        .command(vec!["storage".to_string()])
        .try_build()
        .await?;
    let chain =
        dialog_ucan::UcanDelegation::new(dialog_ucan_core::DelegationChain::new(delegation));
    let certificate = chain.certificates().pop().unwrap();
    let envelope = certificate.encode().unwrap();

    let entities = branch_a
        .delegations()
        .retain(chain.clone())
        .perform(&operator_a)
        .await?;
    assert_eq!(entities.len(), 1);
    let entity = entities[0].clone();
    assert!(branch_a.push().perform(&operator_a).await?.is_some());

    // --- Site B: pull, query by audience, read the envelope. ---
    let storage_b = Storage::temp();
    let profile_b = Profile::open(unique_name("delegation-ship-b"))
        .perform(&storage_b)
        .await?;
    let operator_b = profile_b
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage_b)
        .await?;
    let repo_b = profile_b
        .repository(unique_name("delegation-ship-b-repo"))
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

    // Value-bound query on the audience: the shape a prover's candidate
    // lookup takes, over facts site B never wrote.
    let holder_did = dialog_varsig::Principal::did(&holder).to_string();
    let found: Vec<_> = branch_b
        .claims()
        .select(
            ArtifactSelector::new()
                .the(DELEGATION_AUDIENCE.parse()?)
                .is(Value::String(holder_did.clone())),
        )
        .to_owned()
        .perform(&operator_b)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(found.len(), 1, "site B finds the delegation by audience");
    assert_eq!(found[0].of, entity);

    // The envelope hydrates from the remote and reads back byte-identical.
    let mut reader = Blob::from(entity.clone())
        .read((&branch_b).into())
        .perform(&operator_b)
        .await?;
    let mut bytes = Vec::new();
    while let Some(chunk) = reader.next().await? {
        bytes.extend(chunk);
    }
    assert_eq!(bytes, envelope, "the envelope replicates byte-identical");

    // --- Site A retracts and pushes; B pulls the retraction. ---
    branch_a
        .delegations()
        .retract(chain)
        .perform(&operator_a)
        .await?;
    assert!(branch_a.push().perform(&operator_a).await?.is_some());
    branch_b.pull().perform(&operator_b).await?;

    let after: Vec<_> = branch_b
        .claims()
        .select(
            ArtifactSelector::new()
                .the(DELEGATION_AUDIENCE.parse()?)
                .is(Value::String(holder_did)),
        )
        .to_owned()
        .perform(&operator_b)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert!(after.is_empty(), "a pulled retraction removes the facts");
    assert_eq!(
        Blob::from(entity)
            .size((&branch_b).into())
            .perform(&operator_b)
            .await?,
        None,
        "a pulled retraction removes the blob reference"
    );

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
// Native only: this test builds its sites on `Storage::temp()`, which is
// `cfg(not(target_arch = "wasm32"))` because it needs a real temp
// directory. The wasm equivalent is OPFS-backed and not interchangeable.
//
// Gated on the feature rather than the target: under
// `web-integration-tests` the macro emits a *native* wrapper that shells
// out to a wasm subprocess, so a target gate keeps the wrapper while
// removing the test it launches, and the wrapper then fails finding
// nothing to run.
#[cfg(not(feature = "web-integration-tests"))]
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
        .to_owned()
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
        .to_owned()
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
// Native only: this test builds its sites on `Storage::temp()`, which is
// `cfg(not(target_arch = "wasm32"))` because it needs a real temp
// directory. The wasm equivalent is OPFS-backed and not interchangeable.
//
// Gated on the feature rather than the target: under
// `web-integration-tests` the macro emits a *native* wrapper that shells
// out to a wasm subprocess, so a target gate keeps the wrapper while
// removing the test it launches, and the wrapper then fails finding
// nothing to run.
#[cfg(not(feature = "web-integration-tests"))]
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
        .to_owned()
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
// Native only: this test builds its sites on `Storage::temp()`, which is
// `cfg(not(target_arch = "wasm32"))` because it needs a real temp
// directory. The wasm equivalent is OPFS-backed and not interchangeable.
//
// Gated on the feature rather than the target: under
// `web-integration-tests` the macro emits a *native* wrapper that shells
// out to a wasm subprocess, so a target gate keeps the wrapper while
// removing the test it launches, and the wrapper then fails finding
// nothing to run.
#[cfg(not(feature = "web-integration-tests"))]
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
        .to_owned()
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

/// A retraction must survive a concurrent three-way pull.
///
/// The resurrection scenario observed in the wild: Alice and Bob share
/// a branch. Bob has pulled fact F. Alice pushes something unrelated,
/// moving the upstream past Bob's sync base. Bob retracts F and
/// commits. Bob then pulls: the merge is a genuine three-way (base has
/// F, theirs has F plus Alice's novelty, ours has the retraction). If
/// the merge treats theirs' unchanged copy of F as novelty over ours,
/// the retraction silently loses and the deleted fact resurrects on
/// every such merge — user-visible as "I delete a space and it comes
/// right back on refresh".
#[dialog_common::test]
async fn it_keeps_a_retraction_through_a_concurrent_pull(s3: S3Address) -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;

    // Alice creates the shared branch with fact F and pushes.
    let (alice_repo, alice_branch) =
        setup_repo_with_s3_remote(&operator, &profile, &s3, "retract-alice").await?;
    let fact = Artifact {
        the: "user/name".parse()?,
        of: "user:alice".parse()?,
        is: Value::String("Alice".into()),
        cause: None,
    };
    alice_branch
        .commit(stream::iter(vec![Instruction::Assert(fact.clone())]))
        .perform(&operator)
        .await?;
    alice_branch.push().perform(&operator).await?;

    // Bob tracks the same subject and pulls F.
    let bob_repo = profile
        .repository(unique_name("retract-bob"))
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
    bob_branch.pull().perform(&operator).await?;

    // Alice moves the upstream past Bob's sync base with an unrelated fact.
    let unrelated = Artifact {
        the: "user/name".parse()?,
        of: "user:carol".parse()?,
        is: Value::String("Carol".into()),
        cause: None,
    };
    alice_branch
        .commit(stream::iter(vec![Instruction::Assert(unrelated)]))
        .perform(&operator)
        .await?;
    alice_branch.push().perform(&operator).await?;

    // Bob retracts F locally.
    bob_branch
        .commit(stream::iter(vec![Instruction::Retract(fact.clone())]))
        .perform(&operator)
        .await?;
    let after_retract: Vec<_> = bob_branch
        .claims()
        .select(ArtifactSelector::new().of("user:alice".parse()?))
        .to_owned()
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert!(
        after_retract.is_empty(),
        "the retraction must take locally before the pull"
    );

    // Bob pulls: a real three-way merge (ours moved, theirs moved).
    bob_branch.pull().perform(&operator).await?;

    let after_pull: Vec<_> = bob_branch
        .claims()
        .select(ArtifactSelector::new().of("user:alice".parse()?))
        .to_owned()
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert!(
        after_pull.is_empty(),
        "the retraction must survive the merge; got resurrected: {after_pull:?}"
    );

    // And the unrelated novelty must have arrived.
    let carol: Vec<_> = bob_branch
        .claims()
        .select(ArtifactSelector::new().of("user:carol".parse()?))
        .to_owned()
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(carol.len(), 1, "concurrent novelty still merges in");

    Ok(())
}

/// A device that adopted the upstream head by reference must be able to
/// push its own novelty back to the same remote.
///
/// The everyday device cycle: a quiet replica pulls (scenario-3
/// fast-forward adoption — the head lands by root, zero block reads),
/// commits something of its own, and pushes. The push's novelty diff
/// walks base against current through the LOCAL archive only
/// (`LocalIndex`, no remote fallback), and where the trees differ it
/// descends into base-side nodes the adoption never fetched — failing
/// `Tree operation failed during push: Problem accessing node: Blob not
/// found` even though the missing nodes live on the very remote being
/// pushed to. The push.rs doc calls this a known limit for a head
/// adopted from a *different* remote; this pins that the same-remote
/// case must work, since it is every device's steady state.
#[dialog_common::test]
async fn it_pushes_novelty_after_adopting_the_upstream_head_by_reference(
    s3: S3Address,
) -> Result<()> {
    use crate::helpers::Counting;

    let (operator, profile) = test_operator_with_profile().await;

    // Device A gives the subject enough history that the tree has real
    // depth — the adopted head must hold subtrees B never fetches.
    let (alice_repo, alice_branch) =
        setup_repo_with_s3_remote(&operator, &profile, &s3, "adopt-push-a").await?;
    for batch in 0..4 {
        let facts: Vec<_> = (0..75)
            .map(|i| {
                Instruction::Assert(Artifact {
                    the: "user/name".parse().expect("valid attribute"),
                    of: format!("user:{batch}-{i}").parse().expect("valid entity"),
                    is: Value::String(format!("resident-{batch}-{i}")),
                    cause: None,
                })
            })
            .collect();
        alice_branch
            .commit(stream::iter(facts))
            .perform(&operator)
            .await?;
    }
    alice_branch.push().perform(&operator).await?;

    // Device B, same subject, fresh archive: the pull adopts A's head.
    let env = Counting::new(operator.clone());
    let bob_repo = profile
        .repository(unique_name("adopt-push-b"))
        .open()
        .perform(&env)
        .await?;
    let origin = bob_repo
        .remote("origin")
        .create(s3_site_address(&s3))
        .subject(alice_repo.did())
        .perform(&env)
        .await?;
    let bob_branch = bob_repo.branch("main").open().perform(&env).await?;
    let remote_branch = origin.branch("main").open().perform(&env).await?;
    bob_branch.set_upstream(remote_branch).perform(&env).await?;

    env.reset();
    bob_branch
        .pull()
        .perform(&env)
        .await?
        .expect("head adopted");
    assert_eq!(
        env.block_reads(),
        0,
        "the fixture must route through scenario-3 adoption (zero-read), \
         or it no longer reproduces the by-reference base: {:?}",
        env.snapshot()
    );

    // B's own novelty, then the push every device's sync drain performs.
    bob_branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:bob".parse()?,
            is: Value::String("Bob".into()),
            cause: None,
        })]))
        .perform(&env)
        .await?;

    let pushed = bob_branch.push().perform(&env).await?;
    assert!(
        pushed.is_some(),
        "a device that adopted the upstream head by reference pushes its \
         own novelty back to that same upstream"
    );

    Ok(())
}

/// A head carrying bulk adopted by reference from one remote pushes to a
/// second remote, with the pusher acting as a bridge: content the target
/// lacks is fetched from the remote that holds it and streamed through,
/// never hydrated into the pusher's own archive.
///
/// The N-remote shape of the by-reference push: device pulls a rich
/// history from remote A (scenario-3 adoption, zero reads), then pushes
/// to a brand-new remote B. Every block B needs — tree nodes and the
/// spilled value block a large fact left — crosses via the forwarder.
/// The proof is a fresh replica that has only ever heard of B reading
/// the complete history, big value included. A second push (one local
/// commit) then rides the ordinary novelty path against the advanced
/// base.
#[dialog_common::test]
async fn it_bridges_foreign_bulk_to_a_second_remote(s3: S3Address) -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;

    // Remote A: rich history, including a spilled (larger than inline)
    // value, pushed by the authoring device.
    let (alice_repo, alice_branch) =
        setup_repo_with_s3_remote(&operator, &profile, &s3, "bridge-a").await?;
    let inline_n = dialog_search_tree::Manifest::default().inline_n as usize;
    let big = "b".repeat(inline_n + 1);
    for batch in 0..4 {
        let mut facts: Vec<_> = (0..75)
            .map(|i| {
                Instruction::Assert(Artifact {
                    the: "user/name".parse().expect("valid attribute"),
                    of: format!("user:{batch}-{i}").parse().expect("valid entity"),
                    is: Value::String(format!("resident-{batch}-{i}")),
                    cause: None,
                })
            })
            .collect();
        if batch == 0 {
            facts.push(Instruction::Assert(Artifact {
                the: "doc/body".parse()?,
                of: "doc:big".parse()?,
                is: Value::String(big.clone()),
                cause: None,
            }));
        }
        alice_branch
            .commit(stream::iter(facts))
            .perform(&operator)
            .await?;
    }
    alice_branch.push().perform(&operator).await?;

    // The bridge device: same subject, fresh archive, tracking BOTH
    // remotes. The pull from A adopts the head by root.
    let b_address = S3Address {
        bucket: format!("{}-second", s3.bucket),
        ..s3.clone()
    };
    profile
        .credential()
        .site(s3_site_address(&b_address))
        .save(S3Credential::new(&s3.access_key_id, &s3.secret_access_key))
        .perform(&operator)
        .await?;
    let bridge_repo = profile
        .repository(unique_name("bridge"))
        .open()
        .perform(&operator)
        .await?;
    let origin_a = bridge_repo
        .remote("origin")
        .create(s3_site_address(&s3))
        .subject(alice_repo.did())
        .perform(&operator)
        .await?;
    let origin_b = bridge_repo
        .remote("mirror")
        .create(s3_site_address(&b_address))
        .subject(alice_repo.did())
        .perform(&operator)
        .await?;
    let bridge_branch = bridge_repo.branch("main").open().perform(&operator).await?;
    let remote_a = origin_a.branch("main").open().perform(&operator).await?;
    bridge_branch
        .set_upstream(remote_a)
        .perform(&operator)
        .await?;
    bridge_branch
        .pull()
        .perform(&operator)
        .await?
        .expect("head adopted from A");

    // Push the adopted head to B, which has never seen any of it.
    let remote_b = origin_b.branch("main").open().perform(&operator).await?;
    let pushed = bridge_branch
        .push()
        .to(&remote_b)
        .perform(&operator)
        .await?;
    assert!(
        pushed.is_some(),
        "the bridge push to the second remote lands"
    );

    // A replica that has only ever heard of B reads the full history.
    let reader_repo = profile
        .repository(unique_name("reader"))
        .open()
        .perform(&operator)
        .await?;
    let reader_origin = reader_repo
        .remote("origin")
        .create(s3_site_address(&b_address))
        .subject(alice_repo.did())
        .perform(&operator)
        .await?;
    let reader_branch = reader_repo.branch("main").open().perform(&operator).await?;
    let reader_remote = reader_origin
        .branch("main")
        .open()
        .perform(&operator)
        .await?;
    reader_branch
        .set_upstream(reader_remote)
        .perform(&operator)
        .await?;
    reader_branch
        .pull()
        .perform(&operator)
        .await?
        .expect("reader adopts from B");
    let names: Vec<_> = reader_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .to_owned()
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(names.len(), 300, "every bridged fact reads from B");
    let bodies: Vec<_> = reader_branch
        .claims()
        .select(ArtifactSelector::new().the("doc/body".parse()?))
        .to_owned()
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        bodies.len(),
        1,
        "the spilled fact bridged with its value block"
    );
    assert_eq!(
        bodies[0].is,
        Value::String(big),
        "the spilled value block reconstructs from B"
    );

    // Steady state: one local commit, pushed against the advanced base —
    // the ordinary novelty path, no bridging left to do.
    bridge_branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:bridge".parse()?,
            is: Value::String("Bridge".into()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;
    let again = bridge_branch
        .push()
        .to(&remote_b)
        .perform(&operator)
        .await?;
    assert!(again.is_some(), "the follow-up push lands its novelty");

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
        .to_owned()
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    let bob_results: Vec<_> = bob_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .to_owned()
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

/// The login flow: the ACCOUNT repository is the durable home of
/// delegations, and a device regains access by pulling it. A space
/// delegates to the account; the account's access branch (holding that
/// grant) is pushed to the access service. A device profile "logs in":
/// it retains the account-to-profile powerline locally, adds the account
/// as the upstream of its own access branch, and pulls. The pull adopts
/// the account's delegation records by reference; the prove then reads
/// them like any other read, replicating record blocks and envelope
/// bytes on demand through the walk's reach (each fetch authorized by
/// the retained powerline plus the in-memory session — proofs that are
/// already local). The chain proved is the three-hop ladder: space to
/// account (pulled), account to profile (retained at login), profile to
/// operator (in-memory session). No explicit download: this test pins
/// that proving works on-demand right after a bare pull.
#[dialog_common::test]
async fn it_regains_access_by_pulling_the_account(ucan: UcanS3Address) -> Result<()> {
    use dialog_capability::access::{
        Access as AccessAttenuation, Proof as _, Prove, Retain, TimeRange,
    };
    use dialog_credentials::{Credential as RawCredential, Ed25519Signer, SignerCredential};
    use dialog_effects::storage::{LocationExt as _, Storage as StorageFx};
    use dialog_operator::DeriveOperator as _;
    use dialog_ucan::{Parameters, Scope, Ucan, UcanDelegation};
    use dialog_ucan_core::command::Command as UcanCommand;
    use dialog_ucan_core::subject::Subject as UcanSubject;
    use dialog_ucan_core::{DelegationBuilder, DelegationChain};
    use dialog_varsig::Principal as _;

    let ucan_site = SiteAddress::Ucan(UcanAddress::new(&ucan.access_service_url));

    // --- The account: its own identity, its own repository, the durable
    // home of delegations. ---
    let account_storage = Storage::volatile();
    let account_signer = Ed25519Signer::generate().await?;
    let account_name = unique_name("account");
    StorageFx::profile(account_name.clone())
        .create(RawCredential::Signer(SignerCredential::from(
            account_signer.clone(),
        )))
        .perform(&account_storage)
        .await?;
    let account_profile = Profile::load(account_name)
        .perform(&account_storage)
        .await?;
    let account_operator = account_profile
        .derive(b"account-device")
        .allow(Subject::any())
        .network(Network::default())
        .build(account_storage)
        .await?;

    // A space grants the ACCOUNT (not a profile): the durable direction,
    // so a compromised device profile cannot cost the access.
    let space = Ed25519Signer::generate().await?;
    let space_grant = DelegationBuilder::new()
        .issuer(dialog_credentials::Signer::from(space.clone()))
        .audience(&account_signer)
        .subject(UcanSubject::Specific(space.did()))
        .command(vec!["storage".to_string()])
        .try_build()
        .await?;
    Subject::from(account_profile.did())
        .attenuate(AccessAttenuation)
        .invoke(Retain::<Ucan>::new(UcanDelegation::new(
            DelegationChain::new(space_grant),
        )))
        .perform(&account_operator)
        .await?;

    // Publish the account's access branch to the access service.
    let account_repo = crate::Repository::from(&account_profile);
    let account_origin = account_repo
        .remote("origin")
        .create(ucan_site.clone())
        .perform(&account_operator)
        .await?;
    let account_branch = account_repo
        .branch(crate::ACCESS_BRANCH)
        .open()
        .perform(&account_operator)
        .await?;
    let account_remote_branch = account_origin
        .branch(crate::ACCESS_BRANCH)
        .open()
        .perform(&account_operator)
        .await?;
    account_branch
        .set_upstream(account_remote_branch)
        .perform(&account_operator)
        .await?;
    assert!(
        account_branch
            .push()
            .perform(&account_operator)
            .await?
            .is_some()
    );

    // --- The device: fresh profile and operator. "Login" retains the
    // account-to-profile powerline locally (handed over out of band) and
    // points the profile's access branch at the account. ---
    let device_storage = Storage::volatile();
    let device_profile = Profile::open(unique_name("device"))
        .perform(&device_storage)
        .await?;
    let device_operator = device_profile
        .derive(b"device")
        .allow(Subject::any())
        .network(Network::default())
        .build(device_storage)
        .await?;

    let login_grant = DelegationBuilder::new()
        .issuer(dialog_credentials::Signer::from(account_signer.clone()))
        .audience(&device_profile.did())
        .subject(UcanSubject::Any)
        .command(vec![])
        .try_build()
        .await?;
    Subject::from(device_profile.did())
        .attenuate(AccessAttenuation)
        .invoke(Retain::<Ucan>::new(UcanDelegation::new(
            DelegationChain::new(login_grant),
        )))
        .perform(&device_operator)
        .await?;

    let device_repo = crate::Repository::from(&device_profile);
    let device_origin = device_repo
        .remote("account")
        .create(ucan_site)
        .subject(account_profile.did())
        .perform(&device_operator)
        .await?;
    let device_branch = device_repo
        .branch(crate::ACCESS_BRANCH)
        .open()
        .perform(&device_operator)
        .await?;
    let device_remote_branch = device_origin
        .branch(crate::ACCESS_BRANCH)
        .open()
        .perform(&device_operator)
        .await?;
    device_branch
        .set_upstream(device_remote_branch)
        .perform(&device_operator)
        .await?;

    // The pull IS the login's data path: authorized by the retained
    // powerline plus the in-memory session, it adopts the account's
    // delegation records and hydrates their envelopes.
    assert!(
        device_branch
            .pull()
            .perform(&device_operator)
            .await?
            .is_some(),
        "the login pull adopts the account's delegations"
    );

    // The device now proves access to the space through the full ladder:
    // space -> account (pulled), account -> profile (login), profile ->
    // operator (session).
    let mut claim = Prove::<Ucan>::new(
        device_operator.did(),
        Scope {
            subject: UcanSubject::Specific(space.did()),
            command: UcanCommand(vec!["storage".to_string()]),
            parameters: Parameters::default(),
        },
    );
    claim.duration = TimeRange::unbounded();
    let proof = Subject::from(device_profile.did())
        .attenuate(AccessAttenuation)
        .invoke(claim)
        .perform(&device_operator)
        .await?;
    assert_eq!(
        proof.proofs().len(),
        3,
        "space -> account -> profile -> operator"
    );

    Ok(())
}

/// The embedder's eager form of the login pull: `pull().download()`
/// materializes the adopted head locally — every delegation record
/// block and envelope blob — before any prove runs. Pinned by reading
/// an envelope's bytes straight from the device's local blob provider,
/// with no remote reach in the environment: only a download could have
/// put them there, since a bare pull adopts by reference and envelope
/// bytes otherwise replicate on first read.
#[dialog_common::test]
async fn it_downloads_the_account_branch_on_login(ucan: UcanS3Address) -> Result<()> {
    use dialog_capability::access::{Access as AccessAttenuation, Retain};
    use dialog_credentials::{Credential as RawCredential, Ed25519Signer, SignerCredential};
    use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
    use dialog_effects::blob::prelude::{ArchiveBlobExt as _, BlobExt as _};
    use dialog_effects::storage::{LocationExt as _, Storage as StorageFx};
    use dialog_operator::DeriveOperator as _;
    use dialog_ucan::{Ucan, UcanDelegation};
    use dialog_ucan_core::subject::Subject as UcanSubject;
    use dialog_ucan_core::{DelegationBuilder, DelegationChain};
    use dialog_varsig::Principal as _;
    use futures_util::StreamExt as _;

    let ucan_site = SiteAddress::Ucan(UcanAddress::new(&ucan.access_service_url));

    // The account, holding a space's grant in its pushed access branch.
    let account_storage = Storage::volatile();
    let account_signer = Ed25519Signer::generate().await?;
    let account_name = unique_name("account");
    StorageFx::profile(account_name.clone())
        .create(RawCredential::Signer(SignerCredential::from(
            account_signer.clone(),
        )))
        .perform(&account_storage)
        .await?;
    let account_profile = Profile::load(account_name)
        .perform(&account_storage)
        .await?;
    let account_operator = account_profile
        .derive(b"account-device")
        .allow(Subject::any())
        .network(Network::default())
        .build(account_storage)
        .await?;
    let space = Ed25519Signer::generate().await?;
    let space_grant = DelegationBuilder::new()
        .issuer(dialog_credentials::Signer::from(space.clone()))
        .audience(&account_signer)
        .subject(UcanSubject::Specific(space.did()))
        .command(vec!["storage".to_string()])
        .try_build()
        .await?;
    Subject::from(account_profile.did())
        .attenuate(AccessAttenuation)
        .invoke(Retain::<Ucan>::new(UcanDelegation::new(
            DelegationChain::new(space_grant),
        )))
        .perform(&account_operator)
        .await?;
    let account_repo = crate::Repository::from(&account_profile);
    let account_origin = account_repo
        .remote("origin")
        .create(ucan_site.clone())
        .perform(&account_operator)
        .await?;
    let account_branch = account_repo
        .branch(crate::ACCESS_BRANCH)
        .open()
        .perform(&account_operator)
        .await?;
    let account_remote_branch = account_origin
        .branch(crate::ACCESS_BRANCH)
        .open()
        .perform(&account_operator)
        .await?;
    account_branch
        .set_upstream(account_remote_branch)
        .perform(&account_operator)
        .await?;
    assert!(
        account_branch
            .push()
            .perform(&account_operator)
            .await?
            .is_some()
    );

    // The device logs in: retain the powerline, point at the account,
    // pull WITH download.
    let device_storage = Storage::volatile();
    let device_profile = Profile::open(unique_name("device"))
        .perform(&device_storage)
        .await?;
    let device_operator = device_profile
        .derive(b"device")
        .allow(Subject::any())
        .network(Network::default())
        .build(device_storage.clone())
        .await?;
    let login_grant = DelegationBuilder::new()
        .issuer(dialog_credentials::Signer::from(account_signer.clone()))
        .audience(&device_profile.did())
        .subject(UcanSubject::Any)
        .command(vec![])
        .try_build()
        .await?;
    Subject::from(device_profile.did())
        .attenuate(AccessAttenuation)
        .invoke(Retain::<Ucan>::new(UcanDelegation::new(
            DelegationChain::new(login_grant),
        )))
        .perform(&device_operator)
        .await?;
    let device_repo = crate::Repository::from(&device_profile);
    let device_origin = device_repo
        .remote("account")
        .create(ucan_site)
        .subject(account_profile.did())
        .perform(&device_operator)
        .await?;
    let device_branch = device_repo
        .branch(crate::ACCESS_BRANCH)
        .open()
        .perform(&device_operator)
        .await?;
    let device_remote_branch = device_origin
        .branch(crate::ACCESS_BRANCH)
        .open()
        .perform(&device_operator)
        .await?;
    device_branch
        .set_upstream(device_remote_branch)
        .perform(&device_operator)
        .await?;
    assert!(
        device_branch
            .pull()
            .download()
            .perform(&device_operator)
            .await?
            .is_some(),
        "the login pull adopts the account's delegations"
    );

    // Every retained delegation's envelope must now be readable from the
    // device's LOCAL blob provider — no remote reach in this env.
    use dialog_artifacts::ArtifactSelector;
    let store = super::blob::index_store(&device_branch, &device_operator).await;
    let facts: Vec<_> = crate::Select::new(
        &device_branch,
        ArtifactSelector::new().the(crate::DELEGATION_AUDIENCE.parse().unwrap()),
    )
    .execute(store)
    .await?
    .collect()
    .await;
    assert_eq!(facts.len(), 2, "the powerline and the space grant");
    for fact in facts {
        let artifact = fact?.to_owned()?;
        let digest = artifact
            .of
            .blob_hash()
            .expect("delegation entities are blob entities");
        let mut reader = device_branch
            .subject()
            .archive()
            .blob()
            .read(digest)
            .perform(&device_storage)
            .await?;
        let mut bytes = 0;
        while let Some(chunk) = reader.next().await? {
            bytes += chunk.len();
        }
        assert!(bytes > 0, "the envelope's bytes are local after download");
    }

    Ok(())
}

/// The upgrade path, end to end over the access service and local S3:
/// a delegation sitting in the LEGACY certificate store (as an old
/// install left it) no longer authorizes anything — the operator serves
/// proofs from the access branch only — so resolving the remote branch
/// revision fails. `profile.access().migrate()` moves the delegation
/// into the branch and drains the legacy store; an operator built after
/// the migration (migrate before build: the access branch is opened at
/// build time) resolves the remote branch revision through the migrated
/// credentials.
#[dialog_common::test]
async fn it_authorizes_via_migrated_credentials(ucan: UcanS3Address) -> Result<()> {
    use crate::MigrateAccess as _;
    use dialog_capability::access::{Access as AccessAttenuation, Export, Retain};
    use dialog_operator::DeriveOperator as _;
    use dialog_ucan::Ucan;

    // --- Alice: repo, ownership, UCAN remote, initial push. ---
    let (alice_operator, alice_profile) = test_operator_with_profile().await;
    let alice_repo = alice_profile
        .repository(unique_name("migrate-alice"))
        .create()
        .perform(&alice_operator)
        .await?;
    let ownership = alice_repo
        .access()
        .claim(&alice_repo)
        .delegate(alice_profile.did())
        .perform(&alice_operator)
        .await?;
    alice_profile
        .access()
        .save(ownership)
        .perform(&alice_operator)
        .await?;

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

    // --- Bob: the delegation lands in his LEGACY certificate store, the
    // way an old install left it (storage-routed, not through the
    // operator). ---
    let bob_storage = Storage::volatile();
    let bob_profile = Profile::open(unique_name("migrate-bob"))
        .perform(&bob_storage)
        .await?;
    let delegation_to_bob = alice_profile
        .access()
        .claim(&alice_repo)
        .delegate(bob_profile.did())
        .perform(&alice_operator)
        .await?;
    Subject::from(bob_profile.did())
        .attenuate(AccessAttenuation)
        .invoke(Retain::<Ucan>::new(delegation_to_bob))
        .perform(&bob_storage)
        .await?;

    let bob_operator = bob_profile
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(bob_storage.clone())
        .await?;
    let bob_repo = bob_profile
        .repository(unique_name("migrate-bob-repo"))
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

    // A legacy-store delegation authorizes nothing: resolving the remote
    // branch revision refuses.
    let refused = bob_branch.fetch().perform(&bob_operator).await;
    assert!(
        refused.is_err(),
        "the legacy store must not authorize: {:?}",
        refused.is_ok()
    );

    // Migrate: the delegation moves into Bob's access branch and the
    // legacy store drains.
    let retained = bob_profile.access().migrate().perform(&bob_storage).await?;
    assert!(!retained.is_empty(), "the delegation chain migrated");
    let remaining = Subject::from(bob_profile.did())
        .attenuate(AccessAttenuation)
        .invoke(Export::<Ucan>::new())
        .perform(&bob_storage)
        .await?;
    assert!(
        remaining.is_empty(),
        "the legacy store drained: {} left",
        remaining.len()
    );

    // Migrate before build: the operator opens its access branch at
    // build time, so the post-migration operator sees the migrated
    // credentials. Resolving the remote branch revision now succeeds.
    let bob_operator = bob_profile
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(bob_storage)
        .await?;
    let fetched = bob_branch.fetch().perform(&bob_operator).await?;
    assert!(
        fetched.is_some(),
        "the migrated credentials authorize the resolve"
    );

    // And the full pull works: Bob reads Alice's data.
    bob_branch.pull().perform(&bob_operator).await?;
    let facts: Vec<_> = bob_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .to_owned()
        .perform(&bob_operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(facts.len(), 1, "Bob reads Alice's data after migration");

    Ok(())
}

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
        .to_owned()
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
        .to_owned()
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
        .to_owned()
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
        .to_owned()
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
        .to_owned()
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
        .to_owned()
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
        .to_owned()
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

// `Snapshot::export` reads what the local store holds. `download` is the
// difference between "whatever is here" and "all of it": it routes reads
// through the branch's upstream, so a replica that has only ever seen the
// head can still export the revision whole.
//
// Site A commits and pushes; site B learns the revision but never pulls
// its content, so B's store is empty of everything the revision reaches.
// Exporting from B without `download` reaches nothing; with it, the same
// export produces the same content A holds.
/// Drain an export, counting what it produced.
async fn drain(
    items: impl futures_util::Stream<Item = Result<Item, SnapshotError>>,
) -> Result<(usize, usize)> {
    let (mut blocks, mut blobs) = (0usize, 0usize);
    futures_util::pin_mut!(items);
    while let Some(item) = items.next().await {
        match item? {
            Item::Block(_) => blocks += 1,
            Item::Blob { mut chunks, .. } => {
                // Drain the reader so the bytes are really fetched, not
                // merely announced.
                while chunks.next().await?.is_some() {}
                blobs += 1;
            }
        }
    }
    Ok((blocks, blobs))
}

#[dialog_common::test]
async fn it_downloads_missing_content_when_the_reach_asks_for_it(s3: S3Address) -> Result<()> {
    // --- Site A: commit content and push it to the remote. ---
    let (operator_a, profile_a) = test_operator_with_profile().await;
    let (repo_a, branch_a) =
        setup_repo_with_s3_remote(&operator_a, &profile_a, &s3, "reach-a").await?;

    let facts = vec![Instruction::Assert(Artifact {
        the: "document/body".parse()?,
        of: "document:one".parse()?,
        is: Value::String("downloaded on demand".repeat(64)),
        cause: None,
    })];
    branch_a
        .commit(stream::iter(facts))
        .perform(&operator_a)
        .await?;

    // A blob rides along so the reach is exercised on BOTH channels:
    // blocks hydrate through the archive index, blob bytes through the
    // blob store, and each has its own read path to the remote.
    let blob_bytes = b"downloaded on demand".repeat(512);
    Blob::import(stream::iter(vec![Ok(blob_bytes.clone())]))
        .write(branch_a.blobs())
        .perform(&operator_a)
        .await?;

    assert!(branch_a.push().perform(&operator_a).await?.is_some());
    let revision = branch_a.revision().expect("site A has a revision");

    let (expected_blocks, expected_blobs) = drain(
        repo_a
            .snapshot(revision.clone())
            .export()
            .perform(&operator_a),
    )
    .await
    .context("site A must be able to export everything it committed")?;
    assert!(expected_blocks > 0, "site A holds the content locally");
    assert_eq!(expected_blobs, 1, "site A's export carries the blob");

    // --- Site B: same remote, empty local store, head only. ---
    let storage_b = Storage::<VolatileSpace>::volatile();
    let profile_b = Profile::open(unique_name("reach-b"))
        .perform(&storage_b)
        .await?;
    let operator_b = profile_b
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage_b)
        .await?;
    let repo_b = profile_b
        .repository(unique_name("reach-b-repo"))
        .open()
        .perform(&operator_b)
        .await?;
    profile_b
        .credential()
        .site(s3_site_address(&s3))
        .save(S3Credential::new(&s3.access_key_id, &s3.secret_access_key))
        .perform(&operator_b)
        .await?;
    let origin_b = repo_b
        .remote("origin")
        .create(s3_site_address(&s3))
        .subject(repo_a.did())
        .perform(&operator_b)
        .await?;
    let branch_b = repo_b.branch("main").open().perform(&operator_b).await?;
    let remote_branch_b = origin_b.branch("main").open().perform(&operator_b).await?;
    branch_b
        .set_upstream(remote_branch_b)
        .perform(&operator_b)
        .await?;

    // Without reaching for the remote, B has nothing to export: the root
    // itself is absent, and everything under it is unreachable.
    let (local_blocks, local_blobs) = drain(
        repo_b
            .snapshot(revision.clone())
            .export()
            .sparse()
            .perform(&operator_b),
    )
    .await?;
    assert_eq!(
        (local_blocks, local_blobs),
        (0, 0),
        "a replica that never fetched holds none of the revision"
    );

    // With `download`, the same export walks the whole revision, pulling
    // what is missing through the upstream as it goes -- the blob
    // included, which travels its own channel.
    let upstream = repo_b.remote("origin").load().perform(&operator_b).await?;
    let (downloaded_blocks, downloaded_blobs) = drain(
        repo_b
            .snapshot(revision.clone())
            .export()
            .download(upstream)
            .perform(&operator_b),
    )
    .await
    .context("the download reach must fetch what site B lacks instead of failing")?;
    assert_eq!(
        (downloaded_blocks, downloaded_blobs),
        (expected_blocks, expected_blobs),
        "downloading yields the same content the origin holds, blocks and blob alike"
    );

    // And the fetched content was cached locally on the way through, so a
    // plain export now succeeds where it found nothing before.
    let (cached_blocks, cached_blobs) =
        drain(repo_b.snapshot(revision).export().perform(&operator_b)).await?;
    assert_eq!(
        (cached_blocks, cached_blobs),
        (expected_blocks, expected_blobs),
        "a downloaded revision stays available locally, the blob included"
    );
    Ok(())
}

/// Spilled-value references a revision's tree carries, read from raw leaf
/// entries -- deliberately independent of the export's own classification,
/// so a test can state what the tree references without trusting the code
/// under test -- split by the region of the referencing key: current (EAV)
/// versus history.
async fn raw_spill_references<C: dialog_varsig::Principal>(
    env: &Operator<VolatileSpace>,
    repository: &Repository<C>,
    revision: &Revision,
) -> Result<(HashSet<NodeHash>, HashSet<NodeHash>)> {
    let catalog = repository.subject().archive().index();
    let index = NetworkedIndex::new(env, catalog, None);
    let storage = TreeStorage::new(TreeStorageBridge(index));
    let tree = Index::from_hash(NodeHash::from(*revision.tree.hash()));

    let mut current = HashSet::new();
    let mut history = HashSet::new();
    let visits = tree.traverse_available(&storage);
    futures_util::pin_mut!(visits);
    while let Some(visit) = visits.next().await {
        let Visit::Present(node) = visit? else {
            panic!("a pulled replica holds its whole tree");
        };
        let ArchivedNodeBody::Segment(segment) = node.body() else {
            continue;
        };
        segment.for_each_entry::<Key, _>(|key, value| {
            let key = Key::from(key.to_vec());
            let value: State<Datum> = into_owned(value)?;
            if !matches!(value, State::Added(_)) {
                return Ok(());
            }
            let region = match key.tag() {
                ENTITY_KEY_TAG => &mut current,
                HISTORY_KEY_TAG => &mut history,
                _ => return Ok(()),
            };
            if let Some(reference) = key.value_spill_hash() {
                let reference: [u8; 32] =
                    reference.try_into().expect("a spill reference is 32 bytes");
                region.insert(NodeHash::from(reference));
            }
            Ok(())
        })?;
    }
    Ok((current, history))
}

// A pulled replica references spilled blocks it was never given: pull
// ships tree nodes, not value blocks. Site B pulls a spilled fact,
// commits novelty of its own, then pulls the fact's retraction through a
// real merge -- the shape `Branch::install`'s history scan existed for.
// Wherever the merged tree keeps its reference to the block (today the
// covered claim is physically retained in the current region and screened
// at read time; the merge retires the covered history record instead), a
// complete export must refuse rather than silently omit the block, and a
// `download` export must fetch it: site A pushed it to the shared remote
// when the fact was live.
#[dialog_common::test]
async fn it_downloads_spilled_values_a_pull_never_shipped(s3: S3Address) -> Result<()> {
    // --- Site A: a fact whose value spills, pushed while live. ---
    let (operator_a, profile_a) = test_operator_with_profile().await;
    let (repo_a, branch_a) =
        setup_repo_with_s3_remote(&operator_a, &profile_a, &s3, "retire-a").await?;

    let retracted = Artifact {
        the: "document/body".parse()?,
        of: "document:retired".parse()?,
        is: Value::String(
            "retired".repeat(dialog_search_tree::Manifest::default().inline_n as usize + 1),
        ),
        cause: None,
    };
    branch_a
        .commit(stream::iter(vec![Instruction::Assert(retracted.clone())]))
        .perform(&operator_a)
        .await?;
    assert!(branch_a.push().perform(&operator_a).await?.is_some());

    // --- Site B: pull the fact, then advance on its own so the next pull
    // is a real merge rather than a fast-forward adoption. ---
    let storage_b = Storage::<VolatileSpace>::volatile();
    let profile_b = Profile::open(unique_name("retire-b"))
        .perform(&storage_b)
        .await?;
    let operator_b = profile_b
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage_b)
        .await?;
    let repo_b = profile_b
        .repository(unique_name("retire-b-repo"))
        .open()
        .perform(&operator_b)
        .await?;
    profile_b
        .credential()
        .site(s3_site_address(&s3))
        .save(S3Credential::new(&s3.access_key_id, &s3.secret_access_key))
        .perform(&operator_b)
        .await?;
    let origin_b = repo_b
        .remote("origin")
        .create(s3_site_address(&s3))
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
    branch_b
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "note/text".parse()?,
            of: "note:local".parse()?,
            is: Value::String("site B novelty".into()),
            cause: None,
        })]))
        .perform(&operator_b)
        .await?;

    // --- Site A retracts; site B pulls the retraction through a merge. ---
    branch_a
        .commit(stream::iter(vec![Instruction::Retract(retracted)]))
        .perform(&operator_a)
        .await?;
    assert!(branch_a.push().perform(&operator_a).await?.is_some());
    branch_b.pull().perform(&operator_b).await?;
    let revision = branch_b.revision().expect("site B has a merged revision");

    // The fixture holds: the merged tree still references the spilled
    // block from some region -- history reads depend on that -- while
    // B's store does not hold the block itself. The union keeps the
    // assertion true whichever region the merge leaves the reference in.
    let (current, history) = raw_spill_references(&operator_b, &repo_b, &revision).await?;
    let referenced: HashSet<&NodeHash> = current.union(&history).collect();
    assert!(
        !referenced.is_empty(),
        "the merged tree must still reference the spilled value"
    );

    // Without reaching for the remote, a complete export must refuse: it
    // cannot read a block it does not hold, and silently omitting it would
    // only surface at the destination, at read time.
    let refused = drain(
        repo_b
            .snapshot(revision.clone())
            .export()
            .perform(&operator_b),
    )
    .await;
    assert!(
        refused.is_err(),
        "a complete export must not omit the spilled block it cannot read"
    );

    // With `download`, the export must carry every referenced block,
    // fetching the spilled value through the upstream.
    let upstream = repo_b.remote("origin").load().perform(&operator_b).await?;
    let items = repo_b
        .snapshot(revision)
        .export()
        .download(upstream)
        .perform(&operator_b);
    let mut exported = HashSet::new();
    futures_util::pin_mut!(items);
    while let Some(item) = items.next().await {
        if let Item::Block(block) = item? {
            exported.insert(block.digest);
        }
    }
    for reference in referenced {
        assert!(
            exported.contains(reference),
            "the export must carry the spilled value the tree references: {reference}"
        );
    }
    Ok(())
}
