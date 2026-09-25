//! The spaces a peer keeps: repositories by name, recorded in its state
//! branch.
//!
//! A peer's space names resolve here first: a name recorded with the
//! repository it names and where that repository is stored. The peer's
//! environment provides `space::Load` and `space::Create` over these,
//! falling back to its base directory for a name it has no record of.

use dialog_artifacts::{Changes, Entity};
use dialog_effects::storage::Location;
use dialog_query::{EvaluationError, Output as _, Query, Statement as _, Term};
use dialog_varsig::Did;

use crate::registry::{RegistryEnv, apply};
use crate::schema::{DidExt as _, Space, SpaceKey, space};
use crate::{Branch, CommitError};

/// Why a space could not be recorded or read back.
#[derive(Debug, thiserror::Error)]
pub enum SpaceError {
    /// Reading the peer's state failed.
    #[error("Failed to read spaces: {0}")]
    Query(#[from] EvaluationError),

    /// Recording the space failed.
    #[error("Failed to record a space: {0}")]
    Commit(#[from] CommitError),

    /// A recorded address or repository could not be read back.
    #[error("A recorded space could not be read: {0}")]
    Encoding(String),
}

/// Record in `state` that the repository `subject` is known as `name` and
/// stored at `location`.
///
/// A name picks out one repository: one recorded under it before, other
/// than `subject`, stops being known by it.
pub async fn record<Env: RegistryEnv>(
    state: &Branch,
    subject: &Did,
    name: &str,
    location: &Location,
    env: &Env,
) -> Result<(), SpaceError> {
    let mut changes = Changes::new();
    for previous in named(state, name, env).await? {
        if previous.this != subject.this() {
            previous.retract(&mut changes);
        }
    }
    Space {
        this: subject.this(),
        name: space::Name(name.to_string()),
        address: space::Address(location.uri()),
    }
    .assert(&mut changes);
    Ok(apply(state, changes, env).await?)
}

/// Record in `state` the key of the repository `subject`, sealed to the
/// account it delegates to.
pub async fn seal<Env: RegistryEnv>(
    state: &Branch,
    subject: &Did,
    sealed: Vec<u8>,
    env: &Env,
) -> Result<(), SpaceError> {
    let mut changes = Changes::new();
    SpaceKey {
        this: subject.this(),
        key: space::Key(sealed),
    }
    .assert(&mut changes);
    Ok(apply(state, changes, env).await?)
}

/// The key of the repository `subject`, sealed, if `state` holds it.
pub async fn sealed<Env: RegistryEnv>(
    state: &Branch,
    subject: &Did,
    env: &Env,
) -> Result<Option<Vec<u8>>, SpaceError> {
    let rows: Vec<SpaceKey> = Box::pin(
        state
            .query()
            .select(Query::<SpaceKey> {
                this: subject.this().into(),
                key: Term::var("key"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    Ok(rows.into_iter().next().map(|row| row.key.0))
}

/// The records of the repositories `state` knows by `name`.
async fn named<Env: RegistryEnv>(
    state: &Branch,
    name: &str,
    env: &Env,
) -> Result<Vec<Space>, SpaceError> {
    Ok(Box::pin(
        state
            .query()
            .select(Query::<Space> {
                this: Term::var("this"),
                name: name.to_string().into(),
                address: Term::var("address"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?)
}

/// The repositories `state` knows by `name`, and where each is stored.
pub async fn find<Env: RegistryEnv>(
    state: &Branch,
    name: &str,
    env: &Env,
) -> Result<Vec<(Did, Location)>, SpaceError> {
    named(state, name, env)
        .await?
        .into_iter()
        .map(|row| {
            let subject = subject(&row.this)?;
            let location = Location::from_uri(&row.address.0).ok_or_else(|| {
                SpaceError::Encoding(format!("{} is not a storage location", row.address.0))
            })?;
            Ok((subject, location))
        })
        .collect()
}

fn subject(entity: &Entity) -> Result<Did, SpaceError> {
    entity
        .to_string()
        .parse()
        .map_err(|_| SpaceError::Encoding(format!("{entity} is not a repository DID")))
}

/// Where `state` records the repository `subject` is stored, under each
/// name it is known by.
pub async fn locate<Env: RegistryEnv>(
    state: &Branch,
    subject: &Did,
    env: &Env,
) -> Result<Vec<(String, Location)>, SpaceError> {
    let rows: Vec<Space> = Box::pin(
        state
            .query()
            .select(Query::<Space> {
                this: subject.this().into(),
                name: Term::var("name"),
                address: Term::var("address"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    rows.into_iter()
        .map(|row| {
            let location = Location::from_uri(&row.address.0).ok_or_else(|| {
                SpaceError::Encoding(format!("{} is not a storage location", row.address.0))
            })?;
            Ok((row.name.0, location))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::{find, record};
    use crate::helpers::test_repo;
    use dialog_effects::storage::Location;
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_varsig::did;

    /// A name picks out one space: recording it for another repository
    /// moves it there, rather than leaving the name naming two.
    #[dialog_common::test]
    async fn it_moves_a_name_to_the_repository_last_recorded_under_it() -> anyhow::Result<()> {
        let (session, peer) = test_session_with_peer().await;
        let repo = test_repo(&session, &peer).await;
        let state = repo.branch("state").open().perform(&session).await?;
        let first = did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK");
        let second = did!("key:z6MkkZfZmshVFcBYo9RS6ZyUstxYdjjStQaFaL2TSTVdsiJh");

        record(&state, &first, "notes", &Location::temp("first"), &session).await?;
        record(
            &state,
            &second,
            "notes",
            &Location::temp("second"),
            &session,
        )
        .await?;

        assert_eq!(
            find(&state, "notes", &session).await?,
            vec![(second, Location::temp("second"))]
        );
        Ok(())
    }
}
