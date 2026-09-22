use std::str::FromStr;

use crate::{Peer, Session};
use anyhow::Result;
use base58::ToBase58;
use dialog_artifacts::{Artifact, Attribute, Entity, Value};
use dialog_capability::Subject;
use dialog_effects::storage::Location;
use dialog_storage::provider::storage::{Storage, VolatileSpace};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

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

/// A fresh volatile peer under a unique name.
pub async fn test_peer() -> Peer<VolatileSpace> {
    Peer::new()
        .storage(Storage::volatile())
        .open(Location::profile(unique_name("test")))
        .await
        .expect("test_peer: failed to open peer")
}

/// A session with a powerline grant on a fresh volatile peer.
pub async fn test_session() -> Session<VolatileSpace> {
    test_session_with_peer().await.0
}

/// A session with a powerline grant, and the peer it narrows.
pub async fn test_session_with_peer() -> (Session<VolatileSpace>, Peer<VolatileSpace>) {
    let peer = test_peer().await;
    let session = peer
        .session(b"test")
        .allow(Subject::any())
        .build()
        .await
        .expect("test_session: failed to build session");
    (session, peer)
}

/// Create a test repository under `peer`, through `session`.
pub async fn test_repo(
    session: &Session<VolatileSpace>,
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
