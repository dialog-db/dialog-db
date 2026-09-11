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
// The aborted-push rig and its closure audit are native-only, like the
// tests that use them.
#[cfg(not(feature = "web-integration-tests"))]
use crate::{RemoteRepository, RemoteSite};
#[cfg(not(feature = "web-integration-tests"))]
use dialog_artifacts::{ShipmentRef, shipment_ref};
#[cfg(not(feature = "web-integration-tests"))]
use dialog_capability::{Fork, Provider};
#[cfg(not(feature = "web-integration-tests"))]
use dialog_effects::archive::prelude::{ArchiveExt as _, CatalogExt as _};
#[cfg(not(feature = "web-integration-tests"))]
use dialog_effects::blob::prelude::{ArchiveBlobExt as _, BlobExt as _};
#[cfg(not(feature = "web-integration-tests"))]
use dialog_effects::{
    Rejection,
    blob::{BlobWriter, Import as BlobImportEffect},
};
use dialog_network::Network;
use dialog_operator::{Operator, Profile};
use dialog_remote_s3::helpers::S3Address;
use dialog_remote_s3::{Address as S3SiteAddress, S3Credential};
#[cfg(not(feature = "web-integration-tests"))]
use dialog_search_tree::NoveltyOp;
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

/// Delegating [`Provider`] impls for [`AbortOnRemoteBlobImport`]: every
/// effect a push needs passes through untouched except the one the rig
/// fails.
#[cfg(not(feature = "web-integration-tests"))]
macro_rules! delegate_provider {
    ($($command:ty),+ $(,)?) => {
        $(
            #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
            #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
            impl<P> Provider<$command> for AbortOnRemoteBlobImport<P>
            where
                P: Provider<$command> + dialog_common::ConditionalSync,
            {
                async fn execute(
                    &self,
                    input: <$command as dialog_capability::Command>::Input,
                ) -> <$command as dialog_capability::Command>::Output {
                    self.inner.execute(input).await
                }
            }
        )+
    };
}

/// A provider that delegates every effect a push needs except remote
/// blob imports, which it rejects — a deterministic stand-in for a push
/// dying mid-transfer (a dropped connection, a killed tab). The push
/// protocol's reference-order invariant says whatever such a push
/// managed to land must be closure-complete; residue that violates it
/// poisons a later pusher's existence probes into publishing a head the
/// store cannot serve.
#[cfg(not(feature = "web-integration-tests"))]
struct AbortOnRemoteBlobImport<P> {
    inner: P,
}

#[cfg(not(feature = "web-integration-tests"))]
delegate_provider!(
    dialog_effects::archive::Get,
    dialog_effects::archive::Put,
    dialog_effects::memory::Resolve,
    dialog_effects::memory::Publish,
    dialog_effects::blob::Read,
    crate::Hydrate,
    Fork<RemoteSite, dialog_effects::archive::Get>,
    Fork<RemoteSite, dialog_effects::archive::Put>,
    Fork<RemoteSite, dialog_effects::memory::Resolve>,
    Fork<RemoteSite, dialog_effects::memory::Publish>,
    Fork<RemoteSite, dialog_effects::blob::Read>,
);

#[cfg(not(feature = "web-integration-tests"))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<P> Provider<Fork<RemoteSite, BlobImportEffect>> for AbortOnRemoteBlobImport<P>
where
    P: dialog_common::ConditionalSync,
{
    async fn execute(
        &self,
        _input: Fork<RemoteSite, BlobImportEffect>,
    ) -> Result<BlobWriter, BlobError> {
        Err(BlobError::Rejected(Rejection::Unavailable {
            reason: "rigged abort: this push dies at its first remote blob import".into(),
        }))
    }
}

/// Asserts the push protocol's reference-order invariant on a remote:
/// every head-tree block the remote holds is closure-complete — its
/// children, the blob bytes its entries name, and the spilled value
/// blocks they reference are all present too. This must hold after ANY
/// prefix of a push, aborted pushes included: it is what entitles a
/// later pusher to prune a whole subtree on one positive existence
/// probe. The head is walked through `index` (the pusher's archive,
/// optionally hydrating through a source remote), never trusting the
/// remote under test for its own audit.
#[cfg(not(feature = "web-integration-tests"))]
async fn assert_remote_closure_complete(
    operator: &Operator<VolatileSpace>,
    index: NetworkedIndex<'_, Operator<VolatileSpace>>,
    head: NodeHash,
    remote: &RemoteRepository,
) -> Result<()> {
    // Walk the head tree, collecting per node: its hash, its children,
    // and the blob/spill references its entries carry (stored entries in
    // a segment, buffered ops in an index node — both are bytes of the
    // node that holds them).
    let storage = TreeStorage::new(TreeStorageBridge(index));
    let tree = Index::from_hash(head);
    let mut nodes: Vec<(NodeHash, Vec<NodeHash>, Vec<ShipmentRef>)> = Vec::new();
    let visits = tree.traverse_available(&storage);
    futures_util::pin_mut!(visits);
    while let Some(visit) = visits.next().await {
        let Visit::Present(node) = visit? else {
            panic!("the audit walk must reach every block of the head");
        };
        let children = match node.body() {
            ArchivedNodeBody::Index(body) => {
                body.links()?.into_iter().map(|link| link.node).collect()
            }
            ArchivedNodeBody::Segment(_) => Vec::new(),
        };
        let mut entries: Vec<(Key, State<Datum>)> = Vec::new();
        match node.body() {
            ArchivedNodeBody::Segment(segment) => {
                segment.for_each_entry::<Key, _>(|key, value| {
                    entries.push((Key::from(key.to_vec()), into_owned(value)?));
                    Ok(())
                })?;
            }
            ArchivedNodeBody::Index(body) => {
                for entry in body.all_novelty::<Key>()? {
                    if let NoveltyOp::Assert(value) = entry.op {
                        entries.push((Key::from(entry.key), value));
                    }
                }
            }
        }
        let mut references = Vec::new();
        for (key, value) in entries {
            if let Some(reference) = shipment_ref(&key, &value, false)? {
                references.push(reference);
            }
        }
        nodes.push((node.hash().clone(), children, references));
    }

    // Probe the remote once per block.
    let address = remote.address();
    let mut present: HashSet<NodeHash> = HashSet::new();
    for (hash, _, _) in &nodes {
        let found: Option<Vec<u8>> = address
            .subject
            .clone()
            .archive()
            .catalog("index")
            .get(hash.clone())
            .fork(&address.address)
            .perform(operator)
            .await?;
        if found.is_some() {
            present.insert(hash.clone());
        }
    }

    // The invariant: presence implies the presence of everything
    // referenced.
    for (hash, children, references) in &nodes {
        if !present.contains(hash) {
            continue;
        }
        for child in children {
            assert!(
                present.contains(child),
                "closure violated: node {hash} is on the remote but its \
                 child {child} is not — an aborted push left residue that \
                 poisons existence probes"
            );
        }
        for reference in references {
            match reference {
                ShipmentRef::BlobAdded {
                    hash: blob_hash, ..
                } => {
                    let digest = NodeHash::from(*blob_hash);
                    let probe = address
                        .subject
                        .clone()
                        .archive()
                        .blob()
                        .read(digest.clone())
                        .fork(address.site())
                        .perform(operator)
                        .await;
                    let on_remote = match probe {
                        Ok(_) => true,
                        Err(BlobError::NotFound(_)) => false,
                        Err(error) => return Err(error.into()),
                    };
                    assert!(
                        on_remote,
                        "closure violated: node {hash} is on the remote but \
                         blob {digest} its entries name is not — an aborted \
                         push left residue that poisons existence probes"
                    );
                }
                ShipmentRef::SpilledValue(reference) => {
                    let reference = NodeHash::from(*reference);
                    let found: Option<Vec<u8>> = address
                        .subject
                        .clone()
                        .archive()
                        .catalog("index")
                        .get(reference.clone())
                        .fork(&address.address)
                        .perform(operator)
                        .await?;
                    assert!(
                        found.is_some(),
                        "closure violated: node {hash} is on the remote but \
                         spilled value block {reference} is not"
                    );
                }
                ShipmentRef::BlobRemoved(_) => {}
            }
        }
    }
    Ok(())
}

/// An aborted push must leave the target closure-complete — the
/// steady-state (sole remote) shape.
///
/// The push protocol writes in reference order: blob bytes and spilled
/// values, then by-reference subtrees, then held novelty children before
/// parents, then the revision. A push that uploaded its tree nodes FIRST
/// and died at the blob transfer would leave the remote holding nodes
/// whose blobs it lacks; that residue is unreachable today (the revision
/// was never published), but the moment another device pushes
/// overlapping content it probes those nodes, prunes on the hit, skips
/// the blobs, and publishes — a head the remote cannot serve. Pinned
/// here at the source: no prefix of a push may leave a present block
/// with an absent referent.
#[cfg(not(feature = "web-integration-tests"))]
#[dialog_common::test]
async fn it_leaves_an_aborted_push_closure_complete(s3: S3Address) -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let (repo, branch) =
        setup_repo_with_s3_remote(&operator, &profile, &s3, "abort-closure").await?;

    let facts: Vec<_> = (0..60)
        .map(|i| {
            Instruction::Assert(Artifact {
                the: "user/name".parse().expect("valid attribute"),
                of: format!("user:{i}").parse().expect("valid entity"),
                is: Value::String(format!("resident-{i}")),
                cause: None,
            })
        })
        .collect();
    branch
        .commit(stream::iter(facts))
        .perform(&operator)
        .await?;
    let blob_bytes = b"closure-pinned blob".repeat(64).to_vec();
    Blob::import(stream::iter(vec![Ok(blob_bytes)]))
        .write(branch.blobs())
        .perform(&operator)
        .await?;

    let rigged = AbortOnRemoteBlobImport {
        inner: operator.clone(),
    };
    let aborted = branch.push().perform(&rigged).await;
    assert!(
        aborted.is_err(),
        "the rigged push must abort at the blob import"
    );

    let origin = repo.remote("origin").load().perform(&operator).await?;
    let head = NodeHash::from(*branch.revision().expect("committed").tree.hash());
    let index = NetworkedIndex::new(&operator, branch.archive().index(), None);
    assert_remote_closure_complete(&operator, index, head, &origin).await?;

    Ok(())
}

/// An aborted push must leave the target closure-complete — the
/// N-remote bridge shape, which additionally exercises the ordering
/// between held novelty and the by-reference frontier.
///
/// The bridge device holds only what its own commit minted; the adopted
/// bulk (including a blob) is by reference from remote A. Its held
/// nodes reference the adopted subtree roots as children, so uploading
/// held novelty before forwarding the frontier would leave parents on B
/// whose children never arrived when the transfer dies — exactly the
/// probe-poisoning residue. The rig kills the push at the first blob
/// import; whatever landed on B must still be closure-complete.
#[cfg(not(feature = "web-integration-tests"))]
#[dialog_common::test]
async fn it_leaves_an_aborted_bridge_push_closure_complete(s3: S3Address) -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;

    // Remote A: history plus a blob, from the authoring device.
    let (alice_repo, alice_branch) =
        setup_repo_with_s3_remote(&operator, &profile, &s3, "abort-bridge-a").await?;
    for batch in 0..3 {
        let facts: Vec<_> = (0..60)
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
    let blob_bytes = b"bridge blob".repeat(128).to_vec();
    Blob::import(stream::iter(vec![Ok(blob_bytes)]))
        .write(alice_branch.blobs())
        .perform(&operator)
        .await?;
    alice_branch.push().perform(&operator).await?;

    // The bridge device: adopts A's head by reference, commits its own
    // fact on top (held novelty whose children are by-reference roots).
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
        .repository(unique_name("abort-bridge"))
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
    bridge_branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:bridge".parse()?,
            is: Value::String("Bridge".into()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

    // The rigged push to B dies when the adopted blob would cross.
    let remote_b = origin_b.branch("main").open().perform(&operator).await?;
    let rigged = AbortOnRemoteBlobImport {
        inner: operator.clone(),
    };
    let aborted = bridge_branch.push().to(&remote_b).perform(&rigged).await;
    assert!(
        aborted.is_err(),
        "the rigged bridge push must abort at the blob import"
    );

    // Audit B against the full head, hydrating the walk through A (the
    // bridge holds the adopted region only by reference).
    let head = NodeHash::from(*bridge_branch.revision().expect("committed").tree.hash());
    let index = NetworkedIndex::new(
        &operator,
        bridge_branch.archive().index(),
        Some(origin_a.clone()),
    );
    assert_remote_closure_complete(&operator, index, head, &origin_b).await?;

    Ok(())
}

/// Content adopted through a LOCAL upstream must still be forwarded to
/// a remote that lacks it.
///
/// The laundering shape: branch `backup` tracks remote A and adopts its
/// head by root; branch `main` pulls from local `backup` — same archive,
/// zero reads — so `main`'s head holds content whose provenance no entry
/// of `main`'s own upstream set names. Attribution that stops at the
/// local entry concludes the push target already has everything held by
/// reference, skips forwarding, and publishes a head the target cannot
/// serve. Attribution must resolve local upstreams transitively: the
/// content's remotes are `backup`'s remotes.
#[dialog_common::test]
async fn it_forwards_content_adopted_through_a_local_upstream(s3: S3Address) -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;

    // Remote A: history from the authoring device.
    let (alice_repo, alice_branch) =
        setup_repo_with_s3_remote(&operator, &profile, &s3, "launder-a").await?;
    for batch in 0..3 {
        let facts: Vec<_> = (0..60)
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

    // The device: `backup` adopts from A by root; `main` adopts from
    // local `backup`. Neither pull reads a block, so `main`'s archive
    // holds the whole head by reference and `main`'s upstream set names
    // only the local branch.
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
    let device_repo = profile
        .repository(unique_name("launder-device"))
        .open()
        .perform(&operator)
        .await?;
    let origin_a = device_repo
        .remote("origin")
        .create(s3_site_address(&s3))
        .subject(alice_repo.did())
        .perform(&operator)
        .await?;
    let origin_b = device_repo
        .remote("mirror")
        .create(s3_site_address(&b_address))
        .subject(alice_repo.did())
        .perform(&operator)
        .await?;
    let backup = device_repo
        .branch("backup")
        .open()
        .perform(&operator)
        .await?;
    let remote_a = origin_a.branch("main").open().perform(&operator).await?;
    backup.set_upstream(remote_a).perform(&operator).await?;
    backup
        .pull()
        .perform(&operator)
        .await?
        .expect("backup adopts from A");
    let main = device_repo.branch("main").open().perform(&operator).await?;
    main.pull()
        .from(&backup)
        .perform(&operator)
        .await?
        .expect("main adopts from local backup");

    // Push main to B: everything is by reference and nothing in main's
    // own upstream set names A — the forward must happen anyway.
    let remote_b = origin_b.branch("main").open().perform(&operator).await?;
    let pushed = main.push().to(&remote_b).perform(&operator).await?;
    assert!(
        pushed.is_some(),
        "the push through the local-upstream provenance lands"
    );

    // A replica that has only ever heard of B reads the full history.
    let reader_repo = profile
        .repository(unique_name("launder-reader"))
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
    assert_eq!(
        names.len(),
        180,
        "every fact adopted through the local upstream reads from B"
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

/// A proof walk that has to fetch a node of the access branch must never
/// wait on that fetch's own proof.
///
/// The access branch is what authorization walks, and it is also a
/// synced branch: a head adopted by root can reference nodes the local
/// archive does not hold. The next proof then fetches such a node
/// through the walk's reach, and that fetch is itself proven by a walk
/// over the same branch that needs the same node. Read through one
/// shared node cache, the inner walk joined the outer fetch's
/// single-flight claim and waited on the proof it was itself producing:
/// no I/O, nothing dropped, forever.
///
/// Bounded completion is the property; the outcome is not. An inner
/// proof that resolves from what is local lets the fetch proceed, and
/// one that cannot fails the operation it was proving.
// Native only: the bound is tokio's timer, which has no wasm runtime. Kept
// out of the wasm integration run too, or its native half would provision
// for a wasm half that was never compiled.
#[cfg(all(not(target_arch = "wasm32"), not(feature = "web-integration-tests")))]
#[dialog_common::test]
async fn it_never_waits_on_its_own_fetch_when_the_access_head_ran_ahead_of_the_archive(
    ucan: UcanS3Address,
) -> Result<()> {
    use dialog_capability::access::{Access as AccessAttenuation, Retain};
    use dialog_credentials::{Credential as RawCredential, Ed25519Signer, SignerCredential};
    use dialog_effects::storage::{LocationExt as _, Storage as StorageFx};
    use dialog_operator::DeriveOperator as _;
    use dialog_ucan::{Ucan, UcanDelegation};
    use dialog_ucan_core::subject::Subject as UcanSubject;
    use dialog_ucan_core::{DelegationBuilder, DelegationChain};
    use dialog_varsig::Principal as _;
    use std::time::Duration;
    use tokio::time::timeout;

    let ucan_site = SiteAddress::Ucan(UcanAddress::new(&ucan.access_service_url));

    // The account publishes its access branch, holding one grant, to the
    // access service.
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
    account_branch.push().perform(&account_operator).await?;
    let head = account_branch
        .revision()
        .context("the push established a head")?;

    // A device of the account: its login grant retained locally, the
    // account tracked as its access upstream.
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

    // The head runs ahead of the archive: the account's revision, none
    // of its nodes. Every proof from here has to fetch to read.
    device_branch.reset(head).perform(&device_operator).await?;

    let outcome = timeout(
        Duration::from_secs(30),
        device_branch.pull().perform(&device_operator),
    )
    .await;
    assert!(
        outcome.is_ok(),
        "the proof over an access head that ran ahead of the archive waited on its own fetch"
    );
    Ok(())
}

/// `download().operational()` materializes what reads need and skips the
/// history region.
///
/// Device A commits enough revisions that history is a real share of the
/// tree, then pushes. A second device pulls by reference and downloads
/// only the operational regions. Three things must hold afterwards, and
/// they are what the whole feature rests on:
///
/// 1. Facts read from the local store — the download genuinely put the
///    data regions on disk, rather than leaving reads to hydrate lazily.
/// 2. The scoped download reads strictly fewer blocks than a full one of
///    the same revision.
/// 3. The revision DAG survives: `log` walks ancestry, because revision
///    records are ordinary facts in the data indexes rather than
///    history-region entries.
#[dialog_common::test]
async fn it_downloads_only_the_operational_regions(s3: S3Address) -> Result<()> {
    use crate::helpers::Counting;

    let (operator, profile) = test_operator_with_profile().await;
    let (alice_repo, alice) =
        setup_repo_with_s3_remote(&operator, &profile, &s3, "operational-a").await?;

    // Several commits, so the history region holds many records rather
    // than one: history grows per edit, the data regions per live fact.
    for round in 0..6 {
        let facts: Vec<_> = (0..120)
            .map(|i| {
                Instruction::Assert(Artifact {
                    the: "user/name".parse().expect("valid attribute"),
                    of: format!("user:{round}-{i}").parse().expect("valid entity"),
                    // Wide enough that the fixture fills real leaves: a
                    // tree of a few 64KiB segments has no structure to
                    // prune, and the whole point is the pruning.
                    is: Value::String(format!("resident-{round}-{i}").repeat(24)),
                    cause: None,
                })
            })
            .collect();
        alice.commit(stream::iter(facts)).perform(&operator).await?;
    }
    assert!(alice.push().perform(&operator).await?.is_some());

    // A device that adopts Alice's head by reference, then materializes
    // only the operational regions.
    let open_replica = async |name: &str| -> Result<Branch> {
        let repo = profile
            .repository(unique_name(name))
            .open()
            .perform(&operator)
            .await?;
        let origin = repo
            .remote("origin")
            .create(s3_site_address(&s3))
            .subject(alice_repo.did())
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let remote_branch = origin.branch("main").open().perform(&operator).await?;
        branch
            .set_upstream(remote_branch)
            .perform(&operator)
            .await?;
        Ok(branch)
    };

    let scoped = open_replica("operational-b").await?;
    let scoped_env = Counting::new(operator.clone());
    assert!(scoped.pull().perform(&scoped_env).await?.is_some());
    scoped_env.reset();
    scoped.download().operational().perform(&scoped_env).await?;
    let scoped_reads = scoped_env.block_reads();

    // The same revision, materialized in full, for the comparison.
    let full = open_replica("operational-c").await?;
    let full_env = Counting::new(operator.clone());
    assert!(full.pull().perform(&full_env).await?.is_some());
    full_env.reset();
    full.download().perform(&full_env).await?;
    let full_reads = full_env.block_reads();

    assert!(
        scoped_reads < full_reads,
        "an operational download must read fewer blocks than a full one \
         (scoped {scoped_reads}, full {full_reads})"
    );

    // Every live fact is readable, and the revision DAG walks: revision
    // records are data-region facts, so a scoped download keeps them.
    // Read through the counting env with the tally cleared: the download
    // must have put these blocks on disk, so the reads may not reach the
    // remote. Without this the assertion would pass on lazy hydration
    // alone, which is exactly what the download is supposed to make
    // unnecessary.
    scoped_env.reset();
    let facts: Vec<_> = scoped
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .to_owned()
        .perform(&scoped_env)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        facts.len(),
        720,
        "every live fact survives an operational download"
    );
    assert_eq!(
        scoped_env.count("fork::Fork"),
        0,
        "the facts must read from the local store, not hydrate from the remote"
    );

    let log = scoped.log(&scoped_env, 100).await?;
    assert!(
        log.len() >= 6,
        "the revision DAG survives an operational download (got {} entries)",
        log.len()
    );

    Ok(())
}

/// #492: a download fetches its blocks ONE AT A TIME.
///
/// The scenario is a space join: create a database, put facts in it, add
/// a remote upstream, and materialize it with
/// `pull().download().operational()`. Over a remote archive every block
/// that misses locally is a network round trip, so the number that
/// decides whether this takes a second or a minute is how many of those
/// round trips are in flight AT ONCE — not how many there are.
///
/// A read tally cannot see this: n serial reads and n overlapped reads
/// count identically, which is why the neighbouring download tests bound
/// the count and pass while the app crawls. This measures the overlap.
///
/// The download HAS a fan-out — `traverse` walks each tree level with
/// `buffer_unordered(FETCH_CONCURRENCY)` — but it is upstream of a
/// `try_stream!` generator that yields one node per poll, and
/// `Download::perform` drains that generator with a `while let` that
/// awaits each item before asking for the next. An `async_stream`
/// generator only advances while polled, so the buffered reads never get
/// to run ahead of the consumer: the fan-out is there and never fans out.
#[dialog_common::test]
async fn it_downloads_one_block_at_a_time(s3: S3Address) -> Result<()> {
    use crate::helpers::Counting;

    let (operator, profile) = test_operator_with_profile().await;

    // A database with facts in it, published to the remote. Wide values
    // over several commits so the tree has interior structure: a couple
    // of blocks could be fetched serially without anyone noticing.
    let (source_repo, source) =
        setup_repo_with_s3_remote(&operator, &profile, &s3, "serial-download-a").await?;
    for round in 0..6 {
        let facts: Vec<_> = (0..120)
            .map(|i| {
                Instruction::Assert(Artifact {
                    the: "user/name".parse().expect("valid attribute"),
                    of: format!("user:{round}-{i}").parse().expect("valid entity"),
                    is: Value::String(format!("resident-{round}-{i}").repeat(24)),
                    cause: None,
                })
            })
            .collect();
        source
            .commit(stream::iter(facts))
            .perform(&operator)
            .await?;
    }

    // Blobs too: the account database stores delegation envelopes as
    // blobs, and those travel their own channel rather than the archive
    // index, so a download that parallelizes blocks may still serialize
    // these.
    for i in 0..24 {
        let bytes = format!("delegation envelope {i} ").repeat(64).into_bytes();
        Blob::import(stream::iter(vec![Ok(bytes)]))
            .write(source.blobs())
            .perform(&operator)
            .await?;
    }
    assert!(source.push().perform(&operator).await?.is_some());

    // A second database that adds the first as its upstream.
    let replica_repo = profile
        .repository(unique_name("serial-download-b"))
        .open()
        .perform(&operator)
        .await?;
    let origin = replica_repo
        .remote("origin")
        .create(s3_site_address(&s3))
        .subject(source_repo.did())
        .perform(&operator)
        .await?;
    let replica = replica_repo
        .branch("main")
        .open()
        .perform(&operator)
        .await?;

    // The replica has facts OF ITS OWN before it ever syncs, so the pull
    // is a real merge rather than a bare adoption: this is the app's
    // shape, where a profile has local state before joining a space.
    let local: Vec<_> = (0..80)
        .map(|i| {
            Instruction::Assert(Artifact {
                the: "post/title".parse().expect("valid attribute"),
                of: format!("post:{i}").parse().expect("valid entity"),
                is: Value::String(format!("ours-{i}").repeat(24)),
                cause: None,
            })
        })
        .collect();
    replica
        .commit(stream::iter(local))
        .perform(&operator)
        .await?;

    let remote_branch = origin.branch("main").open().perform(&operator).await?;
    replica
        .set_upstream(remote_branch)
        .perform(&operator)
        .await?;

    // The call under test, measured from cold.
    let env = Counting::new(operator.clone());
    replica
        .pull()
        .download()
        .operational()
        .perform(&env)
        .await?
        .expect("the replica adopts the upstream head");

    let reads = env.block_reads();
    let peak = env.peak_block_reads_in_flight();
    println!("MEASURED reads={reads} peak_in_flight={peak}");

    assert!(
        reads > 8,
        "the download must fetch a real tree for this to measure anything \
         (got {reads} reads): {:?}",
        env.snapshot()
    );
    assert!(
        peak > 1,
        "pull().download().operational() fetched {reads} blocks but never \
         had more than {peak} in flight at once, so each cost its own \
         round trip. The fan-out in `traverse` is defeated by the \
         one-item-per-poll drain in `Download::perform`.",
    );

    Ok(())
}

/// The same download, over a UCAN remote instead of bare S3.
///
/// Its S3 sibling saturates the fan-out (peak 16 of FETCH_CONCURRENCY),
/// so the download parallelizes correctly in isolation. The app does not:
/// a HAR of a space join shows 21 block GETs that are NEVER more than one
/// in flight, each preceded by its own `/ucan/` invocation, strictly
/// alternating. The one structural difference between that run and the
/// passing test is the UCAN remote — every block read must first redeem a
/// permit at the access service.
///
/// Permits are keyed `(site, method, path)` and the path is the block
/// digest, so per-block redemption is correct by design; 16 concurrent
/// GETs simply need 16 concurrent redeems. If redemption serializes, the
/// GETs inherit it and the fan-out above is defeated no matter how wide
/// it is.
#[dialog_common::test]
async fn it_downloads_one_block_at_a_time_over_ucan(ucan: UcanS3Address) -> Result<()> {
    use crate::helpers::Counting;

    let (operator, profile) = test_operator_with_profile().await;
    let site = SiteAddress::Ucan(UcanAddress::new(&ucan.access_service_url));

    // A database with facts in it, published to the UCAN remote.
    let source_repo = profile
        .repository(unique_name("ucan-serial-a"))
        .create()
        .perform(&operator)
        .await?;
    let chain = source_repo
        .access()
        .claim(&source_repo)
        .delegate(profile.did())
        .perform(&operator)
        .await?;
    profile.access().save(chain).perform(&operator).await?;
    let source_origin = source_repo
        .remote("origin")
        .create(site.clone())
        .perform(&operator)
        .await?;
    let source = source_repo.branch("main").open().perform(&operator).await?;
    let source_remote = source_origin
        .branch("main")
        .open()
        .perform(&operator)
        .await?;
    source
        .set_upstream(source_remote)
        .perform(&operator)
        .await?;
    for round in 0..6 {
        let facts: Vec<_> = (0..120)
            .map(|i| {
                Instruction::Assert(Artifact {
                    the: "user/name".parse().expect("valid attribute"),
                    of: format!("user:{round}-{i}").parse().expect("valid entity"),
                    is: Value::String(format!("resident-{round}-{i}").repeat(24)),
                    cause: None,
                })
            })
            .collect();
        source
            .commit(stream::iter(facts))
            .perform(&operator)
            .await?;
    }
    assert!(source.push().perform(&operator).await?.is_some());

    // A replica that adds it as upstream and materializes it.
    let replica_repo = profile
        .repository(unique_name("ucan-serial-b"))
        .open()
        .perform(&operator)
        .await?;
    let origin = replica_repo
        .remote("origin")
        .create(site)
        .subject(source_repo.did())
        .perform(&operator)
        .await?;
    let replica = replica_repo
        .branch("main")
        .open()
        .perform(&operator)
        .await?;
    let remote_branch = origin.branch("main").open().perform(&operator).await?;
    replica
        .set_upstream(remote_branch)
        .perform(&operator)
        .await?;

    let env = Counting::new(operator.clone());
    replica
        .pull()
        .download()
        .operational()
        .perform(&env)
        .await?
        .expect("the replica adopts the upstream head");

    let reads = env.block_reads();
    let peak = env.peak_block_reads_in_flight();
    println!("UCAN MEASURED reads={reads} peak_in_flight={peak}");

    assert!(
        reads > 8,
        "the download must fetch a real tree (got {reads}): {:?}",
        env.snapshot()
    );
    assert!(
        peak > 1,
        "over a UCAN remote the download fetched {reads} blocks but never \
         had more than {peak} in flight at once, so each cost its own \
         round trip. Its bare-S3 sibling reaches 16, so the serialization \
         is in permit redemption, not the download walk.",
    );

    Ok(())
}

/// #492: a download's block GETs are serial, while a push's PUTs of the
/// same tree are concurrent.
///
/// THE REPRODUCTION. A throttled HAR of a real space join measured, in
/// one run, over one link, against one UCAN remote:
///
/// - PUT (push/upload): 29 requests, peak 15 in flight.
/// - GET (pull/download): 20 requests, peak 1 in flight, in exact
///   lockstep (`/ucan` 2.0s -> GET 2.0s -> `/ucan` 2.0s -> ...).
///
/// Because both paths share the app, the browser, the link, the remote,
/// the block `Flight`, and the single-threaded worker, none of those can
/// be the cause: the push has all of them and still fans out. The
/// difference is the shape of the consumer.
///
/// - `Upload::perform` feeds a FULLY MATERIALIZED wave into
///   `.buffer_unordered(16).try_collect()`. `try_collect` is a TERMINAL
///   consumer: it polls until everything finishes, so all 16 uploads stay
///   in flight.
/// - The download's fan-out is `buffered(16)` inside `try_stream!`
///   (`traversal.rs`), wrapped by a second `try_stream!` (the snapshot
///   export), drained by `Download::perform`'s
///   `while let Some(item) = items.next().await`. An `async_stream`
///   generator SUSPENDS at every `yield`, so the reads only advance while
///   the consumer polls; the fan-out is parked after each item and never
///   accumulates.
///
/// This asserts the contrast directly, in one test, so the fix is pinned
/// by the same comparison that found it: push the tree (measuring PUT
/// overlap), then download it into a cold replica (measuring GET
/// overlap). It must run on wasm as well as native -- the browser is
/// where the symptom was observed.
#[dialog_common::test]
async fn it_downloads_serially_while_pushing_concurrently(
    ucan: UcanS3Address,
) -> Result<()> {
    use crate::helpers::Counting;

    let (operator, profile) = test_operator_with_profile().await;
    let site = SiteAddress::Ucan(UcanAddress::new(&ucan.access_service_url));

    // A database with enough facts that its tree has interior structure:
    // a handful of blocks could be fetched serially unnoticed.
    let source_repo = profile
        .repository(unique_name("serial-get-a"))
        .create()
        .perform(&operator)
        .await?;
    let chain = source_repo
        .access()
        .claim(&source_repo)
        .delegate(profile.did())
        .perform(&operator)
        .await?;
    profile.access().save(chain).perform(&operator).await?;
    let source_origin = source_repo
        .remote("origin")
        .create(site.clone())
        .perform(&operator)
        .await?;
    let source = source_repo.branch("main").open().perform(&operator).await?;
    let source_remote = source_origin
        .branch("main")
        .open()
        .perform(&operator)
        .await?;
    source
        .set_upstream(source_remote)
        .perform(&operator)
        .await?;
    for round in 0..4 {
        let facts: Vec<_> = (0..120)
            .map(|i| {
                Instruction::Assert(Artifact {
                    the: "user/name".parse().expect("valid attribute"),
                    of: format!("user:{round}-{i}").parse().expect("valid entity"),
                    is: Value::String(format!("resident-{round}-{i}").repeat(24)),
                    cause: None,
                })
            })
            .collect();
        source
            .commit(stream::iter(facts))
            .perform(&operator)
            .await?;
    }

    // The push, measured. This is the control: the same tree, the same
    // remote, the same authorization, going the other way.
    let push_env = Counting::new(operator.clone());
    assert!(source.push().perform(&push_env).await?.is_some());
    let writes = push_env.count("fork::Fork");
    let push_peak = push_env.peak_forks_in_flight();

    // A cold replica on a SECOND operator, hence a second local archive.
    //
    // This is load-bearing and easy to get wrong: two repositories opened
    // on one operator share its storage, so a "replica" built that way
    // already holds every block the push just wrote. Its download then
    // reads locally, issues no remote hydration at all, and reports a
    // healthy peak while measuring nothing. The `fork::Fork` assertion
    // below is what keeps that mistake from passing silently.
    let (replica_operator, replica_profile) = test_operator_with_profile().await;
    replica_profile
        .access()
        .save(
            source_repo
                .access()
                .claim(&source_repo)
                .delegate(replica_profile.did())
                .perform(&operator)
                .await?,
        )
        .perform(&replica_operator)
        .await?;
    let replica_repo = replica_profile
        .repository(unique_name("serial-get-b"))
        .open()
        .perform(&replica_operator)
        .await?;
    let origin = replica_repo
        .remote("origin")
        .create(site)
        .subject(source_repo.did())
        .perform(&replica_operator)
        .await?;
    let replica = replica_repo
        .branch("main")
        .open()
        .perform(&replica_operator)
        .await?;
    // Facts of its own BEFORE it ever syncs, so the pull is a real merge
    // rather than a bare adoption -- the app's shape, where a profile has
    // local state before joining a space.
    let local: Vec<_> = (0..80)
        .map(|i| {
            Instruction::Assert(Artifact {
                the: "post/title".parse().expect("valid attribute"),
                of: format!("post:{i}").parse().expect("valid entity"),
                is: Value::String(format!("ours-{i}").repeat(24)),
                cause: None,
            })
        })
        .collect();
    replica
        .commit(stream::iter(local))
        .perform(&replica_operator)
        .await?;

    let remote_branch = origin
        .branch("main")
        .open()
        .perform(&replica_operator)
        .await?;
    replica
        .set_upstream(remote_branch)
        .perform(&replica_operator)
        .await?;

    // The download, measured as the app performs it: chained onto the
    // pull, which is what `hydrate_untrusted` calls at login.
    //
    // Splitting the two and measuring only the download was tried and is
    // wrong: the merge hydrates the blocks it walks, so by the time a
    // separate download ran, the replica was warm and the measurement
    // covered local reads. The merge's own differential does fan out, so
    // a healthy peak here is not by itself proof the download's walk
    // parallelized -- what this pins is that the whole login path does
    // not collapse to one request at a time, which is the symptom.
    let pull_env = Counting::new(replica_operator.clone());
    replica
        .pull()
        .download()
        .operational()
        .perform(&pull_env)
        .await?
        .expect("the replica adopts the upstream head");
    let reads = pull_env.block_reads();
    let pull_peak = pull_env.peak_block_reads_in_flight();
    // The number that corresponds to the HAR: concurrent ROUND TRIPS.
    // Block reads that hit the local store overlap for free and tell us
    // nothing about wall time.
    let remote_peak = pull_env.peak_forks_in_flight();

    let hydrations = pull_env.count("hydrate::Hydrate");

    // The tree's depth, read back from the replica: the descent is
    // root-to-leaf and each level's read names the next, so depth bounds
    // how many fetches CANNOT overlap however wide the fan-out is.
    let head = NodeHash::from(*replica.revision().expect("pulled").tree.hash());
    let depth_index = NetworkedIndex::new(&replica_operator, replica.archive().index(), None);
    let depth_storage = TreeStorage::new(TreeStorageBridge(depth_index));
    let mut depth = 0usize;
    let mut at = Some(head);
    while let Some(hash) = at.take() {
        let Some(bytes) = depth_storage.retrieve(&hash).await? else {
            break;
        };
        depth += 1;
        let node = dialog_search_tree::PersistentNode::<Key, State<Datum>>::try_from(
            dialog_search_tree::Buffer::from(bytes),
        )?;
        if let ArchivedNodeBody::Index(index) = node.body() {
            at = index.links()?.first().map(|link| link.node.clone());
        }
    }

    println!(
        "PUSH writes={writes} peak={push_peak} | \
         PULL reads={reads} peak={pull_peak} hydrations={hydrations} \
         remote_peak={remote_peak} depth={depth}"
    );

    assert!(
        hydrations > 0,
        "the replica read {reads} blocks without one remote fetch, so it was \
         not cold and this measured local reads. The download must hydrate \
         through the remote for its overlap to mean anything. Effects seen: \
         {:?}",
        pull_env.snapshot()
    );
    assert!(
        writes > 8 && reads > 8,
        "both directions must move a real tree to compare them \
         (writes={writes}, reads={reads})"
    );
    assert!(
        push_peak > 1,
        "the push is the control and must fan out: {writes} uploads reached \
         peak {push_peak}. If this fails the comparison proves nothing and \
         the harness is at fault, not the download."
    );
    assert!(
        hydrations > 8,
        "the download made only {hydrations} remote fetches, too few for its \
         overlap to mean anything: the replica must actually pull the tree \
         across the wire. Effects seen: {:?}",
        pull_env.snapshot()
    );
    assert!(
        remote_peak > 1,
        "the push fanned out to peak {push_peak} over {writes} uploads, but \
         the login path's remote fetches reached only peak {remote_peak} \
         over {hydrations} of them -- one round trip at a time, which is \
         the HAR's shape exactly. Local block overlap ({pull_peak}) does \
         not pay for wall time; concurrent round trips do."
    );
    assert!(
        pull_peak > 1,
        "the push fanned out to peak {push_peak} over {writes} uploads, but \
         the download of the SAME tree over the SAME remote reached only \
         peak {pull_peak} over {reads} reads. Same app, link, remote, \
         flight and executor -- so the cause is the consumer shape: \
         `Upload::perform` drains a materialized wave with a terminal \
         `try_collect`, while the download's `buffered(16)` sits inside \
         `try_stream!` generators that suspend at every `yield`."
    );

    Ok(())
}

/// #492, the login path: a profile branch carrying what tonk's does.
///
/// Modelled on `hydrate_untrusted` in tonk's
/// `router/account_state.rs`, which is what runs when a device signs in:
/// point the profile's main branch at the account remote, then
/// `pull().download().operational()` so the authorization walk afterwards
/// reads entirely locally.
///
/// The content matters as much as the call. A tonk profile branch is not
/// uniform facts -- it accumulates, on one branch:
///
/// - retained delegations, each decomposing into facts PLUS a signed
///   envelope blob (`branch/delegation.rs`),
/// - device-link rows and space/replica index rows, ordinary facts.
///
/// Blobs are the part the fact-only sibling cannot reach: they travel
/// their own channel in the snapshot export, a second `buffer_unordered`
/// loop downstream of the block walk, and each consults the tree's blob
/// index before its bytes can be fetched. A fan-out restored in the
/// traversal alone does not cover them.
///
/// The call is chained exactly as the app chains it, rather than split
/// into a pull and a separate download: the ordering is part of what is
/// under test. `PullDownload` materializes BEFORE advancing the head, so
/// the measurement covers the download the login actually performs.
///
/// Peak overlap is the assertion for the usual reason: it is
/// latency-independent, so a memory-backed local server cannot hide a
/// serial download behind fast responses.
#[dialog_common::test]
async fn it_downloads_delegation_blobs_concurrently(ucan: UcanS3Address) -> Result<()> {
    use crate::helpers::Counting;
    use dialog_credentials::Ed25519Signer;

    let (operator, profile) = test_operator_with_profile().await;
    let site = SiteAddress::Ucan(UcanAddress::new(&ucan.access_service_url));

    let source_repo = profile
        .repository(unique_name("blob-sync-a"))
        .create()
        .perform(&operator)
        .await?;
    let chain = source_repo
        .access()
        .claim(&source_repo)
        .delegate(profile.did())
        .perform(&operator)
        .await?;
    profile.access().save(chain).perform(&operator).await?;
    let source_origin = source_repo
        .remote("origin")
        .create(site.clone())
        .perform(&operator)
        .await?;
    let source = source_repo.branch("main").open().perform(&operator).await?;
    let source_remote = source_origin
        .branch("main")
        .open()
        .perform(&operator)
        .await?;
    source
        .set_upstream(source_remote)
        .perform(&operator)
        .await?;

    // Many retained delegations: each contributes facts AND one envelope
    // blob, which is what makes this the profile's shape rather than a
    // generic fact sync.
    let space = Ed25519Signer::generate().await?;
    for _ in 0..24 {
        let holder = Ed25519Signer::generate().await?;
        let delegation = dialog_ucan_core::DelegationBuilder::new()
            .issuer(dialog_credentials::Signer::from(space.clone()))
            .audience(&dialog_varsig::Principal::did(&holder))
            .subject(dialog_ucan_core::subject::Subject::Specific(
                dialog_varsig::Principal::did(&space),
            ))
            .command(vec!["storage".to_string()])
            .try_build()
            .await?;
        source
            .delegations()
            .retain(dialog_ucan::UcanDelegation::new(
                dialog_ucan_core::DelegationChain::new(delegation),
            ))
            .perform(&operator)
            .await?;
    }
    // Device links and space index rows: the ordinary facts that sit
    // beside the delegations on a real profile branch, so the download
    // walks a tree of mixed content rather than one uniform region.
    let rows: Vec<_> = (0..160)
        .map(|i| {
            Instruction::Assert(Artifact {
                the: "device/link".parse().expect("valid attribute"),
                of: format!("device:{i}").parse().expect("valid entity"),
                is: Value::String(format!("device-{i}").repeat(24)),
                cause: None,
            })
        })
        .collect();
    source
        .commit(stream::iter(rows))
        .perform(&operator)
        .await?;
    assert!(source.push().perform(&operator).await?.is_some());

    // A cold replica on its own profile, operator and space: two repos on
    // one operator would share an archive and make every read local. The
    // `fork::Fork` assertion below is what keeps that from passing quietly.
    let (replica_operator, replica_profile) = test_operator_with_profile().await;
    replica_profile
        .access()
        .save(
            source_repo
                .access()
                .claim(&source_repo)
                .delegate(replica_profile.did())
                .perform(&operator)
                .await?,
        )
        .perform(&replica_operator)
        .await?;
    let replica_repo = replica_profile
        .repository(unique_name("blob-sync-b"))
        .open()
        .perform(&replica_operator)
        .await?;
    let origin = replica_repo
        .remote("origin")
        .create(site)
        .subject(source_repo.did())
        .perform(&replica_operator)
        .await?;
    let replica = replica_repo
        .branch("main")
        .open()
        .perform(&replica_operator)
        .await?;
    let remote_branch = origin
        .branch("main")
        .open()
        .perform(&replica_operator)
        .await?;
    replica
        .set_upstream(remote_branch)
        .perform(&replica_operator)
        .await?;

    // The login call itself, chained as `hydrate_untrusted` chains it.
    //
    // This measures the merge's reads as well as the download's, and the
    // merge's differential has a fan-out of its own -- so unlike the
    // fact-only sibling (which splits the two to keep the download
    // isolated) a healthy peak here does not prove the download
    // parallelized. That is deliberate: this test's job is to reproduce
    // what the app does, and `blob::Read` below is what pins the blob
    // channel specifically, whose reads no differential issues.
    let env = Counting::new(replica_operator.clone());
    replica
        .pull()
        .download()
        .operational()
        .perform(&env)
        .await?
        .expect("the replica adopts the upstream head");

    let reads = env.block_reads();
    let peak = env.peak_block_reads_in_flight();
    let hydrations = env.count("hydrate::Hydrate");
    let blob_reads = env.count("blob::Read");
    let remote_peak = env.peak_forks_in_flight();
    println!(
        "PROFILE reads={reads} peak={peak} forks={hydrations} \
         blob_reads={blob_reads} remote_peak={remote_peak}"
    );

    assert!(
        hydrations > 0,
        "the replica materialized without one remote fetch, so it was not \
         cold and this measured local reads"
    );
    assert!(
        reads > 8,
        "the download must move a real tree to measure anything (got {reads})"
    );
    assert!(
        blob_reads > 0,
        "the 24 retained delegations must have shipped envelope blobs for \
         this to exercise the blob channel at all (blob::Read={blob_reads}). \
         Without them this is just another fact sync and the sibling test \
         already covers it."
    );
    assert!(
        remote_peak > 1,
        "a profile carrying 24 delegation envelope blobs made {hydrations} \
         remote fetches but never had more than {remote_peak} open at once \
         -- one round trip at a time, the HAR's shape. Blobs travel their \
         own channel downstream of the block walk, so a fan-out restored in \
         the traversal alone does not cover them."
    );
    assert!(
        peak > 1,
        "a profile carrying 24 delegation envelope blobs materialized \
         {reads} reads but never had more than {peak} in flight at once, so \
         each cost its own round trip. Blobs travel their own channel in \
         the snapshot export, downstream of the block walk, so a fan-out \
         restored in the traversal alone does not cover them."
    );

    Ok(())
}


/// #492, the scenario the HAR traces: a SECOND DEVICE joining an account.
///
/// Both sides are seeded, and that is the whole point. A device's first
/// load creates its own profile with the shipped defaults -- concept
/// definitions, rules, views, a starter space -- before it has ever seen
/// an account. Signing in then points that already-populated branch at
/// the account's remote and pulls, so the pull is a MERGE of two
/// independently seeded trees, not the adoption of an empty one.
///
/// Every earlier version of this test got that wrong in one of two ways:
/// a replica with nothing local (the merge has no base, so its
/// differential fetches nothing and a download finds the tree already
/// there), or a replica sharing the source's storage (every read local,
/// nothing measured). Both passed while the app crawled.
///
/// The measurement is concurrent ROUND TRIPS (`Hydrate` for a download's
/// block fetches, forked effects for a push's uploads) -- never block
/// reads, which hit the local store and overlap for free.
#[dialog_common::test]
async fn it_joins_an_account_from_a_seeded_device(ucan: UcanS3Address) -> Result<()> {
    use crate::helpers::Counting;
    use crate::repository::snapshot::codec;

    // The fixture is a real CAR captured from a running tonk profile,
    // stored zstd-compressed: 820 KiB of tree becomes 54. `ruzstd` is
    // pure Rust, so it decompresses in the browser as well as natively.
    let compressed = include_bytes!("../../../tests/fixtures/profile.car.zst");
    let mut snapshot = Vec::new();
    std::io::copy(
        &mut ruzstd::decoding::StreamingDecoder::new(&compressed[..])?,
        &mut snapshot,
    )?;
    let (items, roots) = codec::decode_with_roots(&snapshot)?;
    let blobs = items
        .iter()
        .filter(|item| matches!(item, Ok(crate::Item::Blob { .. })))
        .count();

    // The joining device's OWN content, captured from a real first load
    // before it had ever seen an account: definitions, rules, views, a
    // starter space. This is what makes the pull a merge -- a device with
    // nothing local gives the differential no base to diff, so it fetches
    // nothing and the download that follows finds the tree already there.
    let device_compressed = include_bytes!("../../../tests/fixtures/device.car.zst");
    let mut device_snapshot = Vec::new();
    std::io::copy(
        &mut ruzstd::decoding::StreamingDecoder::new(&device_compressed[..])?,
        &mut device_snapshot,
    )?;
    let device_items = codec::decode(&device_snapshot)?;
    println!(
        "FIXTURE account: blocks={} blobs={blobs} roots={roots:?} | device: {} items",
        items.len() - blobs,
        device_items.len()
    );
    assert!(
        !roots.is_empty(),
        "the CAR must name its tree root, or the imported blocks are \
         reachable from nothing"
    );

    let (operator, profile) = test_operator_with_profile().await;
    let site = SiteAddress::Ucan(UcanAddress::new(&ucan.access_service_url));

    // Device 1: the account. Seed it the way a first load seeds a profile,
    // then publish it.
    let account_repo = profile
        .repository(unique_name("join-account"))
        .create()
        .perform(&operator)
        .await?;
    let chain = account_repo
        .access()
        .claim(&account_repo)
        .delegate(profile.did())
        .perform(&operator)
        .await?;
    profile.access().save(chain).perform(&operator).await?;
    let account_origin = account_repo
        .remote("origin")
        .create(site.clone())
        .perform(&operator)
        .await?;
    let account = account_repo
        .branch(crate::ACCESS_BRANCH)
        .open()
        .perform(&operator)
        .await?;
    account
        .set_upstream(account_origin.branch(crate::ACCESS_BRANCH).open().perform(&operator).await?)
        .perform(&operator)
        .await?;
    // The account's content is the REAL captured tree, imported blocks and
    // blobs alike -- not a synthetic stand-in. Importing puts the content
    // in the archive; committing the facts is what gives the branch a head
    // that references it, which is what a push has to ship and a join has
    // to pull.
    let imported = account_repo
        .import(stream::iter(items))
        .perform(&operator)
        .await?;
    println!("IMPORTED {imported:?}");
    let delegations = crate::helpers::fill_account_branch(&account, 2, &operator).await?;

    // The push is the control: same tree, same remote, other direction.
    let push_env = Counting::new(operator.clone());
    let pushed = account.push().perform(&push_env).await?;
    let uploads = push_env.count("fork::Fork");
    let push_peak = push_env.peak_forks_in_flight();
    println!("PUSH pushed={} uploads={uploads} peak={push_peak}", pushed.is_some());

    // Device 2: its own profile, its own storage, SEEDED with defaults of
    // its own before it ever sees the account -- the state a first load
    // leaves behind.
    let (device_operator, device_profile) = test_operator_with_profile().await;
    device_profile
        .access()
        .save(
            account_repo
                .access()
                .claim(&account_repo)
                .delegate(device_profile.did())
                .perform(&operator)
                .await?,
        )
        .perform(&device_operator)
        .await?;
    let device_repo = device_profile
        .repository(unique_name("join-device"))
        .open()
        .perform(&device_operator)
        .await?;
    let device = device_repo
        .branch(crate::ACCESS_BRANCH)
        .open()
        .perform(&device_operator)
        .await?;
    let device_imported = device_repo
        .import(stream::iter(device_items))
        .perform(&device_operator)
        .await?;
    println!("DEVICE IMPORTED {device_imported:?}");
    crate::helpers::fill_account_branch(&device, 1, &device_operator).await?;

    // Sign in: point the seeded branch at the account and pull.
    let device_remote = device_repo
        .remote("account-access")
        .create(site)
        .subject(account_repo.did())
        .perform(&device_operator)
        .await?;
    device
        .set_upstream(
            device_remote
                .branch(crate::ACCESS_BRANCH)
                .open()
                .perform(&device_operator)
                .await?,
        )
        .perform(&device_operator)
        .await?;

    let env = Counting::new(device_operator.clone());
    device.pull().download().perform(&env).await?;

    let hydrations = env.count("hydrate::Hydrate");
    let remote_peak = env.peak_forks_in_flight();
    let reads = env.block_reads();
    let local_peak = env.peak_block_reads_in_flight();
    let serial_run = env.longest_serial_fetch_run();
    println!(
        "JOIN delegations={delegations} hydrations={hydrations} \
         remote_peak={remote_peak} serial_run={serial_run} \
         reads={reads} local_peak={local_peak}"
    );

    assert!(
        hydrations > 8,
        "the joining device must pull the account across the wire for its \
         overlap to mean anything (hydrations={hydrations}). Effects: {:?}",
        env.snapshot()
    );
    // The serial RUN, not the peak. A peak over the whole pull can read
    // healthy while a long prefix of it is strictly one-at-a-time, which
    // is what let this bug hide: natively the run is 0, in the browser it
    // is 30 -- same code, same fixtures, same test.
    assert!(
        serial_run < 4,
        "a device joining the account made {hydrations} remote fetches, and \
         {serial_run} of them ran back-to-back with nothing else in flight \
         (peak {remote_peak} over the whole pull). One round trip at a time \
         is the HAR's shape exactly. Natively this same test serializes \
         none, so a failure here is the wasm runtime, not the walk."
    );

    Ok(())
}
