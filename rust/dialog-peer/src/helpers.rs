use std::str::FromStr;

use crate::{
    Allowance, Mode, OpenCredential, Peer, PeerError, PeerSpace, Session, SpaceVaultExt as _,
};
use anyhow::Result;
use base58::ToBase58;
use dialog_artifacts::{Artifact, Attribute, Entity, Value};
use dialog_capability::Subject;
use dialog_credentials::{Ed25519Signer, SignerCredential};
use dialog_effects::credential::CredentialError;
use dialog_effects::storage::Location;
use dialog_repository::{ACCESS_BRANCH, BranchReference, Repository};
use dialog_storage::provider::storage::{CredentialStore, Storage, VolatileSpace};
use dialog_varsig::{Did, Principal as _};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::sync::OnceLock;

/// Generate a unique name with a prefix for test isolation.
///
/// The name carries the process id as well as a timestamp: the test
/// runner starts one process per test, so the per-process counter alone
/// cannot disambiguate two tests whose first call lands on the same
/// clock tick — which is exactly how two concurrently running e2e tests
/// intermittently collided on one temp vault directory and found each
/// other's credentials in it.
pub fn unique_name(prefix: &str) -> String {
    use dialog_common::time;
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ts = time::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    // `process::id()` panics on wasm32-unknown (unsupported os call);
    // a browser test runs one module per process anyway, so the
    // timestamp + counter already disambiguate there.
    #[cfg(not(target_arch = "wasm32"))]
    let pid = {
        use std::process;
        process::id()
    };
    #[cfg(target_arch = "wasm32")]
    let pid = 0u32;
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{ts}-{pid}-{seq}")
}

/// The system every test storage belongs to: one fixed key, so a
/// storage and the grant of it can be made anywhere in a test.
pub async fn test_system() -> SignerCredential {
    let signer = Ed25519Signer::import(&[0x5e; 32])
        .await
        .expect("test_system: a fixed seed imports");
    SignerCredential::from(signer)
}

/// A volatile storage owned by the [test system](test_system).
pub async fn test_storage() -> Storage<VolatileSpace> {
    test_owned(Storage::volatile()).await
}

/// `storage`, owned by the [test system](test_system).
pub async fn test_owned<S: Clone>(storage: Storage<S>) -> Storage<S> {
    storage.owned_by(test_system().await.did())
}

/// The grant of a storage owned by the [test system](test_system).
pub async fn test_grant() -> Allowance {
    Allowance::storage(&test_system().await)
}

/// The `main` branch of the repository `home`: the state branch a peer
/// whose home is `home` keeps by default.
pub fn test_state(home: &Did) -> BranchReference {
    Repository::from(home.clone()).branch(ACCESS_BRANCH)
}

/// The credential store every test peer's key is kept in: one for the
/// test process, so a peer opened twice at one location acts with one
/// key.
pub fn test_credential_store() -> CredentialStore<VolatileSpace> {
    static CREDENTIALS: OnceLock<CredentialStore<VolatileSpace>> = OnceLock::new();
    CREDENTIALS.get_or_init(CredentialStore::new).clone()
}

/// Open a root peer: its key from the [test credential store](test_credential_store)
/// under `location`'s name, its home space at `location` in `storage`,
/// acting for itself, its records in the home's main branch, granted the
/// storage by the [test system](test_system), and [onboarded](onboard).
pub async fn open_peer<S: PeerSpace>(
    storage: Storage<S>,
    location: Location,
) -> Result<Peer<S>, PeerError> {
    let credential = OpenCredential::open(location.name.clone())
        .at(location.directory.clone())
        .perform(&test_credential_store())
        .await
        .map_err(|error| PeerError::Open(error.to_string()))?;
    let peer = Peer::new(credential.clone())
        .at(location)
        .space(test_state(&credential.did()))
        .with(storage)
        .grant(test_grant().await)
        .build()
        .await?;
    onboard(&peer)
        .await
        .map_err(|error| PeerError::State(error.to_string()))?;
    Ok(peer)
}

/// Onboard `peer`, unless its space has an account: the space creates the
/// `account` vault with the peer as its member, the account delegates to
/// the peer, and the peer becomes a member of `account` → `peer`, as
/// onboarding and sign-in would.
pub async fn onboard<S: PeerSpace>(peer: &Peer<S>) -> Result<(), CredentialError> {
    if peer.authority().await.is_ok() {
        return Ok(());
    }
    let account = peer.state().vault("account").create().perform(peer).await?;
    account.add(peer.did()).perform(peer).await?;
    account.delegate(peer.did()).perform(peer).await?;
    let peers = account.vault("peer").open().perform(peer).await?;
    peers.add(peer.did()).perform(peer).await
}

/// A fresh volatile peer under a unique name.
pub async fn test_peer() -> Peer<VolatileSpace> {
    open_peer(test_storage().await, Location::profile(unique_name("test")))
        .await
        .expect("test_peer: failed to open peer")
}

/// A session with a powerline grant on a fresh volatile peer.
pub async fn test_session() -> Peer<VolatileSpace, Session> {
    test_session_with_peer().await.0
}

/// A session with a powerline grant, and the peer it is a session of.
pub async fn test_session_with_peer() -> (Peer<VolatileSpace, Session>, Peer<VolatileSpace>) {
    let peer = test_peer().await;
    let worker = peer
        .session(b"test")
        .space(peer.state())
        .allow(Subject::any())
        .await
        .expect("test_session: failed to build worker");
    (worker, peer)
}

/// Create a test repository under `peer`, through `session`.
pub async fn test_repo<M: Mode>(
    session: &Peer<VolatileSpace, M>,
    peer: &Peer<VolatileSpace>,
) -> dialog_repository::Repository<dialog_credentials::Credential> {
    use dialog_repository::RepositoryExt as _;
    peer.space(unique_name("repo"))
        .open()
        .perform(session)
        .await
        .expect("test_repo: failed to open repository")
}

/// Generate deterministic test data consisting of facts that reference a
/// specified number of [`Entity`]s.
pub fn generate_data(entity_count: usize) -> Result<Vec<Artifact>> {
    let item_id_attribute = Attribute::from_str("item/id")?;
    let item_name_attribute = Attribute::from_str("item/name")?;
    let item_pointer_attribute = Attribute::from_str("attribute/pointer")?;
    let back_reference_attribute = Attribute::from_str("back/reference")?;
    let parent_attribute = Attribute::from_str("relationship/parentOf")?;

    let mut rng = ChaCha8Rng::from_seed([0u8; 32]);
    let mut data = vec![];
    let mut make_entity = || {
        Entity::try_from(format!("entity:{}", rng.r#gen::<[u8; 32]>().to_base58()))
            .expect("Failed to generate random entity")
    };
    let mut last_entity: Option<Entity> = None;

    for i in 0..entity_count {
        let entity = make_entity();

        data.push(Artifact {
            the: item_pointer_attribute.clone(),
            of: entity.clone(),
            is: Value::Symbol(parent_attribute.clone()),
            cause: None,
        });

        data.push(Artifact {
            the: item_id_attribute.clone(),
            of: entity.clone(),
            is: Value::UnsignedInt(i as u128),
            cause: None,
        });

        data.push(Artifact {
            the: item_name_attribute.clone(),
            of: entity.clone(),
            is: Value::String(format!("name{i}")),
            cause: None,
        });

        if let Some(parent_entity) = last_entity {
            data.push(Artifact {
                the: parent_attribute.clone(),
                of: entity.clone(),
                is: Value::Entity(parent_entity.clone()),
                cause: None,
            });
        }

        data.push(Artifact {
            the: back_reference_attribute.clone(),
            of: make_entity(),
            is: Value::Entity(entity.clone()),
            cause: None,
        });

        last_entity = Some(entity);
    }

    Ok(data)
}
