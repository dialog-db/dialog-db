//! End-to-end Operator -> Network -> Fs tests.
//!
//! These drive the real env-bound authorization path: an [`Operator`] forks an
//! effect at an [`FsAddress`], `FsFork::authorize` proves the operator holds a
//! delegation for that exact effect (gating read vs write), verifies the
//! directory is the subject's space, then performs against it. They mirror
//! `dialog-repository`'s UCAN collaboration tests, with a local directory as the
//! remote instead of an access service.
//!
//! The remote vault is seeded manually: a repository is created with a volatile
//! operator, then its credential is written into a fresh [`Location`] directory
//! as `credential/key/self`, making that directory the repository's space. (The
//! milestone that lets a repository be created *directly on* a File System
//! Access directory is separate work.) Because the vault is a [`Location`], the
//! tests run on native and the web alike.

use anyhow::Result;
use dialog_artifacts::{Artifact, ArtifactSelector, Instruction, Value};
use dialog_capability::Subject;
use dialog_credentials::{Credential, SignerCredential};
use dialog_effects::archive::prelude::*;
use dialog_effects::credential::prelude::*;
use dialog_effects::storage::Location;
use dialog_operator::helpers::{test_operator_with_profile, unique_name};
use dialog_operator::{Operator, Profile};
use dialog_remote_fs::FsAddress;
use dialog_repository::{Branch, Repository, RepositoryExt as _, SiteAddress};
use dialog_storage::provider::FileSystem;
use dialog_storage::provider::storage::VolatileSpace;
use dialog_storage::resource::Resource;
use futures_util::{StreamExt, stream};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// Seed a fresh directory as the space for `repo` by writing its credential to
/// `credential/key/self`, returning the directory's [`Location`] and the
/// [`FsAddress`] naming it.
async fn seed_vault(repo: &Repository<SignerCredential>) -> Result<(Location, FsAddress)> {
    let location = Location::temp(unique_name("fs-vault"));
    let filesystem = FileSystem::open(&location).await?;
    let credential = Credential::Signer(repo.credential().clone());
    repo.did()
        .credential()
        .key("self")
        .save(credential)
        .perform(&filesystem)
        .await?;
    Ok((location.clone(), FsAddress::new(location)))
}

/// Create a repository, delegate full ownership to the profile, seed an FS
/// vault as the repo's space, and add it as the `origin` remote with an
/// upstream-tracking `main` branch.
async fn setup_repo_with_fs_remote(
    operator: &Operator<VolatileSpace>,
    profile: &Profile,
    name: &str,
) -> Result<(Repository<SignerCredential>, Location, Branch)> {
    let repo = profile
        .repository(unique_name(name))
        .create()
        .perform(operator)
        .await?;

    // Delegate repo ownership to the profile so prove can authorize forks.
    let chain = repo
        .access()
        .claim(&repo)
        .delegate(profile.did())
        .perform(operator)
        .await?;
    profile.access().save(chain).perform(operator).await?;

    let (location, address) = seed_vault(&repo).await?;

    let origin = repo
        .remote("origin")
        .create(SiteAddress::Fs(address))
        .perform(operator)
        .await?;

    let branch = repo.branch("main").open().perform(operator).await?;
    let remote_branch = origin.branch("main").open().perform(operator).await?;
    branch.set_upstream(remote_branch).perform(operator).await?;

    Ok((repo, location, branch))
}

fn artifact(of: &str, name: &str) -> Result<Artifact> {
    Ok(Artifact {
        the: "user/name".parse()?,
        of: of.parse()?,
        is: Value::String(name.into()),
        cause: None,
    })
}

#[dialog_common::test]
async fn it_pushes_and_pulls_via_fs_remote() -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let (_repo, _location, branch) =
        setup_repo_with_fs_remote(&operator, &profile, "fs-push").await?;

    branch
        .commit(stream::iter(vec![Instruction::Assert(artifact(
            "user:1", "Alice",
        )?)]))
        .perform(&operator)
        .await?;

    let push = branch.push().perform(&operator).await?;
    assert!(push.is_some(), "fs push should succeed");

    // Pull right after push finds no new changes.
    let pull = branch.pull().perform(&operator).await?;
    assert!(pull.is_none(), "pull after push should return None");

    let results: Vec<_> = branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].is, Value::String("Alice".into()));
    Ok(())
}

#[dialog_common::test]
async fn it_shares_an_fs_remote_between_two_repos() -> Result<()> {
    // Two repos point at the same vault directory: Alice pushes, Bob pulls.
    let (operator, profile) = test_operator_with_profile().await;
    let (alice_repo, location, alice_branch) =
        setup_repo_with_fs_remote(&operator, &profile, "fs-share-a").await?;
    let address = FsAddress::new(location);

    alice_branch
        .commit(stream::iter(vec![Instruction::Assert(artifact(
            "user:alice",
            "Alice",
        )?)]))
        .perform(&operator)
        .await?;
    alice_branch.push().perform(&operator).await?;

    // Bob opens a second repo and points its origin at Alice's vault, targeting
    // Alice's subject.
    let bob_repo = profile
        .repository(unique_name("fs-share-b"))
        .open()
        .perform(&operator)
        .await?;

    // Bob needs a delegation for Alice's repo to authorize forks against it.
    let chain = alice_repo
        .access()
        .claim(&alice_repo)
        .delegate(profile.did())
        .perform(&operator)
        .await?;
    profile.access().save(chain).perform(&operator).await?;

    let bob_origin = bob_repo
        .remote("origin")
        .create(SiteAddress::Fs(address))
        .subject(alice_repo.did())
        .perform(&operator)
        .await?;

    let bob_branch = bob_repo.branch("main").open().perform(&operator).await?;
    let remote_branch = bob_origin.branch("main").open().perform(&operator).await?;
    bob_branch
        .set_upstream(remote_branch)
        .perform(&operator)
        .await?;

    let pull = bob_branch.pull().perform(&operator).await?;
    assert!(pull.is_some(), "Bob's pull should find Alice's data");

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
    assert_eq!(results[0].is, Value::String("Alice".into()));
    Ok(())
}

#[dialog_common::test]
async fn it_rejects_a_stale_push_on_cas_conflict() -> Result<()> {
    // Two repos share a vault. The first push advances the remote head; a
    // second push that hasn't seen that advance must fail the memory CAS.
    let (operator, profile) = test_operator_with_profile().await;
    let (alice_repo, location, alice_branch) =
        setup_repo_with_fs_remote(&operator, &profile, "fs-cas-a").await?;
    let address = FsAddress::new(location);

    // Bob shares Alice's vault and subject, tracking the same remote branch.
    let bob_repo = profile
        .repository(unique_name("fs-cas-b"))
        .open()
        .perform(&operator)
        .await?;
    let chain = alice_repo
        .access()
        .claim(&alice_repo)
        .delegate(profile.did())
        .perform(&operator)
        .await?;
    profile.access().save(chain).perform(&operator).await?;
    let bob_origin = bob_repo
        .remote("origin")
        .create(SiteAddress::Fs(address))
        .subject(alice_repo.did())
        .perform(&operator)
        .await?;
    let bob_branch = bob_repo.branch("main").open().perform(&operator).await?;
    let bob_remote = bob_origin.branch("main").open().perform(&operator).await?;
    bob_branch
        .set_upstream(bob_remote)
        .perform(&operator)
        .await?;

    // Alice commits and pushes first, advancing the remote head.
    alice_branch
        .commit(stream::iter(vec![Instruction::Assert(artifact(
            "user:alice",
            "Alice",
        )?)]))
        .perform(&operator)
        .await?;
    alice_branch.push().perform(&operator).await?;

    // Bob commits independently (still sees the empty remote) and pushes. The
    // remote head moved under him, so his publish must fail the CAS.
    bob_branch
        .commit(stream::iter(vec![Instruction::Assert(artifact(
            "user:bob", "Bob",
        )?)]))
        .perform(&operator)
        .await?;
    let stale = bob_branch.push().perform(&operator).await;
    assert!(
        stale.is_err(),
        "a push from a stale remote head must fail CAS: {stale:?}"
    );
    Ok(())
}

#[dialog_common::test]
async fn it_denies_a_read_without_authorization() -> Result<()> {
    // An operator with no delegation for the vault's subject cannot read it.
    let (operator, _profile) = test_operator_with_profile().await;

    // A standalone vault for some other subject.
    let (other_operator, other_profile) = test_operator_with_profile().await;
    let other_repo = other_profile
        .repository(unique_name("fs-foreign"))
        .create()
        .perform(&other_operator)
        .await?;
    let (_location, address) = seed_vault(&other_repo).await?;

    let digest = dialog_common::Blake3Hash::hash(b"anything");
    let result = Subject::from(other_repo.did())
        .archive()
        .catalog("index")
        .get(digest)
        .fork(&SiteAddress::Fs(address))
        .perform(&operator)
        .await;
    assert!(
        result.is_err(),
        "reading without a delegation must be denied"
    );
    Ok(())
}

#[dialog_common::test]
async fn it_allows_read_but_denies_write_with_read_only_delegation() -> Result<()> {
    // A read-only delegation authorizes Get but not Put: the command-prefix
    // match in prove gates the write for free.
    let (operator, profile) = test_operator_with_profile().await;
    let repo = profile
        .repository(unique_name("fs-readonly"))
        .create()
        .perform(&operator)
        .await?;
    let (_location, address) = seed_vault(&repo).await?;

    let content = b"seed".to_vec();
    let digest = dialog_common::Blake3Hash::hash(&content);

    // Delegate ONLY archive read (/archive/get) for this repo's subject.
    let chain = repo
        .access()
        .claim(
            Subject::from(repo.did())
                .archive()
                .catalog("index")
                .get(digest.clone()),
        )
        .delegate(profile.did())
        .perform(&operator)
        .await?;
    profile.access().save(chain).perform(&operator).await?;

    // Read is authorized (returns None: nothing written yet).
    let read = Subject::from(repo.did())
        .archive()
        .catalog("index")
        .get(digest)
        .fork(&SiteAddress::Fs(address.clone()))
        .perform(&operator)
        .await;
    assert!(
        read.is_ok(),
        "read-only delegation should authorize Get: {read:?}"
    );

    // Write is denied: no /archive/put in the delegation.
    let write = Subject::from(repo.did())
        .archive()
        .catalog("index")
        .put(content)
        .fork(&SiteAddress::Fs(address))
        .perform(&operator)
        .await;
    assert!(
        write.is_err(),
        "read-only delegation must deny Put: {write:?}"
    );
    Ok(())
}

#[dialog_common::test]
async fn it_allows_resolve_but_denies_publish_with_resolve_only_delegation() -> Result<()> {
    // The memory analogue of the archive read/write test: a /memory/resolve
    // delegation authorizes resolve but not /memory/publish.
    use dialog_effects::memory::prelude::*;

    let (operator, profile) = test_operator_with_profile().await;
    let repo = profile
        .repository(unique_name("fs-mem-readonly"))
        .create()
        .perform(&operator)
        .await?;
    let (_location, address) = seed_vault(&repo).await?;

    // Delegate ONLY memory resolve (/memory/resolve) for this repo's subject.
    let chain = repo
        .access()
        .claim(
            Subject::from(repo.did())
                .memory()
                .space("local")
                .cell("head")
                .resolve(),
        )
        .delegate(profile.did())
        .perform(&operator)
        .await?;
    profile.access().save(chain).perform(&operator).await?;

    // Resolve is authorized (returns None: nothing published yet).
    let resolved = Subject::from(repo.did())
        .memory()
        .space("local")
        .cell("head")
        .resolve()
        .fork(&SiteAddress::Fs(address.clone()))
        .perform(&operator)
        .await;
    assert!(
        resolved.is_ok(),
        "resolve-only delegation should authorize Resolve: {resolved:?}"
    );

    // Publish is denied: no /memory/publish in the delegation.
    let published = Subject::from(repo.did())
        .memory()
        .space("local")
        .cell("head")
        .publish(b"first".to_vec(), None)
        .fork(&SiteAddress::Fs(address))
        .perform(&operator)
        .await;
    assert!(
        published.is_err(),
        "resolve-only delegation must deny Publish: {published:?}"
    );
    Ok(())
}

#[dialog_common::test]
async fn it_denies_when_subject_is_not_the_directory() -> Result<()> {
    // The operator is fully authorized for `repo`, but points the remote at a
    // vault that belongs to a DIFFERENT subject. verify_subject must deny.
    let (operator, profile) = test_operator_with_profile().await;
    let repo = profile
        .repository(unique_name("fs-mismatch"))
        .create()
        .perform(&operator)
        .await?;
    let chain = repo
        .access()
        .claim(&repo)
        .delegate(profile.did())
        .perform(&operator)
        .await?;
    profile.access().save(chain).perform(&operator).await?;

    // Vault belongs to a stranger, not `repo`.
    let (other_operator, other_profile) = test_operator_with_profile().await;
    let other_repo = other_profile
        .repository(unique_name("fs-stranger"))
        .create()
        .perform(&other_operator)
        .await?;
    let (_location, address) = seed_vault(&other_repo).await?;

    let digest = dialog_common::Blake3Hash::hash(b"anything");
    let result = Subject::from(repo.did())
        .archive()
        .catalog("index")
        .get(digest)
        .fork(&SiteAddress::Fs(address))
        .perform(&operator)
        .await;
    assert!(
        result.is_err(),
        "a vault for a different subject must be denied"
    );
    Ok(())
}
