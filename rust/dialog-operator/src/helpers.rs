use crate::DeriveOperator as _;
use std::str::FromStr;

use crate::{Operator, Profile};
use anyhow::Result;
use base58::ToBase58;
use dialog_artifacts::{Artifact, Attribute, Entity, Value};
use dialog_capability::Subject;
use dialog_network::Network;
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
    use std::process;
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ts = time::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = process::id();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{ts}-{pid}-{seq}")
}

/// Build a test operator with a fresh profile and powerline delegation.
pub async fn test_operator() -> Operator<VolatileSpace> {
    let storage = Storage::volatile();
    let profile = Profile::open(unique_name("test"))
        .perform(&storage)
        .await
        .unwrap();
    profile
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage)
        .await
        .unwrap()
}

/// Build a test operator and return both the operator and the profile.
pub async fn test_operator_with_profile() -> (Operator<VolatileSpace>, Profile) {
    let storage = Storage::volatile();
    let profile = Profile::open(unique_name("test"))
        .perform(&storage)
        .await
        .unwrap();
    let operator = profile
        .derive(b"test")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage)
        .await
        .unwrap();
    (operator, profile)
}

/// Create a test repository using the given operator and profile.
pub async fn test_repo(
    operator: &Operator<VolatileSpace>,
    profile: &Profile,
) -> dialog_repository::Repository<dialog_credentials::Credential> {
    use dialog_repository::RepositoryExt as _;
    profile
        .repository(unique_name("repo"))
        .open()
        .perform(operator)
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
