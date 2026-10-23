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
use crate::schema::{DidExt as _, Space, space};
use crate::secrets;
use crate::{Branch, CommitError};

/// The kind a repository's key is held sealed as.
pub const SPACE: &str = "space";

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

/// The entity of `peer`'s record of the repository `subject`.
fn kept(peer: &Did, subject: &Did) -> Entity {
    secrets::derived("space", format!("{peer}\u{0}{subject}").as_bytes())
}

/// Record in `state` that `peer` knows the repository `subject` as `name`
/// and stores it at `location`.
///
/// A name picks out one repository for a peer: one the peer recorded
/// under it before, other than `subject`, stops being known by it. Other
/// peers' names are their own.
pub async fn record<Env: RegistryEnv>(
    state: &Branch,
    peer: &Did,
    subject: &Did,
    name: &str,
    location: &Location,
    env: &Env,
) -> Result<(), SpaceError> {
    let mut changes = Changes::new();
    for previous in named(state, peer, name, env).await? {
        if previous.repository.0 != subject.this() {
            previous.retract(&mut changes);
        }
    }
    Space {
        this: kept(peer, subject),
        peer: space::Peer(peer.this()),
        repository: space::Repository(subject.this()),
        name: space::Name(name.to_string()),
        address: space::Address(location.uri()),
    }
    .assert(&mut changes);
    Ok(apply(state, changes, env).await?)
}

/// Record in `state` the key of the repository `subject`, sealed to
/// `account`: a principal whose key is held sealed, the way tonk keeps
/// custody.
pub async fn seal<Env: RegistryEnv>(
    state: &Branch,
    subject: &Did,
    account: &Did,
    sealed: Vec<u8>,
    env: &Env,
) -> Result<(), SpaceError> {
    secrets::hold_principal(
        state,
        subject,
        SPACE,
        secrets::sealed_message(account, sealed),
        env,
    )
    .await
    .map_err(|error| SpaceError::Encoding(error.to_string()))
}

/// The key of the repository `subject`, sealed, if `state` holds it.
pub async fn sealed<Env: RegistryEnv>(
    state: &Branch,
    subject: &Did,
    env: &Env,
) -> Result<Option<Vec<u8>>, SpaceError> {
    Ok(secrets::held_principal(state, subject, env)
        .await
        .map_err(|error| SpaceError::Encoding(error.to_string()))?
        .filter(|held| held.kind == SPACE)
        .map(|held| held.sealed))
}

/// The records of the repositories `peer` knows by `name` in `state`.
async fn named<Env: RegistryEnv>(
    state: &Branch,
    peer: &Did,
    name: &str,
    env: &Env,
) -> Result<Vec<Space>, SpaceError> {
    Ok(Box::pin(
        state
            .query()
            .select(Query::<Space> {
                this: Term::var("this"),
                peer: peer.this().into(),
                repository: Term::var("repository"),
                name: name.to_string().into(),
                address: Term::var("address"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?)
}

/// The repositories `peer` knows by `name` in `state`, and where each is
/// stored.
pub async fn find<Env: RegistryEnv>(
    state: &Branch,
    peer: &Did,
    name: &str,
    env: &Env,
) -> Result<Vec<(Did, Location)>, SpaceError> {
    named(state, peer, name, env)
        .await?
        .into_iter()
        .map(|row| {
            let subject = subject(&row.repository.0)?;
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

/// Where `peer` records the repository `subject` is stored, under each
/// name it knows it by.
pub async fn locate<Env: RegistryEnv>(
    state: &Branch,
    peer: &Did,
    subject: &Did,
    env: &Env,
) -> Result<Vec<(String, Location)>, SpaceError> {
    let rows: Vec<Space> = Box::pin(
        state
            .query()
            .select(Query::<Space> {
                this: Term::var("this"),
                peer: peer.this().into(),
                repository: subject.this().into(),
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

/// Every repository `peer` keeps in `state`, under each name it knows it
/// by.
pub async fn kept_by<Env: RegistryEnv>(
    state: &Branch,
    peer: &Did,
    env: &Env,
) -> Result<Vec<(Did, String)>, SpaceError> {
    let rows: Vec<Space> = Box::pin(
        state
            .query()
            .select(Query::<Space> {
                this: Term::var("this"),
                peer: peer.this().into(),
                repository: Term::var("repository"),
                name: Term::var("name"),
                address: Term::var("address"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    rows.into_iter()
        .map(|row| Ok((subject(&row.repository.0)?, row.name.0)))
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

    /// A name picks out one space for a peer: recording it for another
    /// repository moves it there, rather than leaving the name naming two.
    #[dialog_common::test]
    async fn it_moves_a_name_to_the_repository_last_recorded_under_it() -> anyhow::Result<()> {
        let (session, peer) = test_session_with_peer().await;
        let repo = test_repo(&session, &peer).await;
        let state = repo.branch("state").open().perform(&session).await?;
        let first = did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK");
        let second = did!("key:z6MkkZfZmshVFcBYo9RS6ZyUstxYdjjStQaFaL2TSTVdsiJh");
        let keeper = peer.did();

        record(
            &state,
            &keeper,
            &first,
            "notes",
            &Location::temp("first"),
            &session,
        )
        .await?;
        record(
            &state,
            &keeper,
            &second,
            "notes",
            &Location::temp("second"),
            &session,
        )
        .await?;

        assert_eq!(
            find(&state, &keeper, "notes", &session).await?,
            vec![(second, Location::temp("second"))]
        );
        Ok(())
    }

    /// Peers whose records share one space keep their own names and
    /// locations: one peer naming another repository leaves the other's
    /// name where it was.
    #[dialog_common::test]
    async fn it_keeps_each_peers_names_apart() -> anyhow::Result<()> {
        let (session, peer) = test_session_with_peer().await;
        let repo = test_repo(&session, &peer).await;
        let state = repo.branch("state").open().perform(&session).await?;
        let first = did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK");
        let second = did!("key:z6MkkZfZmshVFcBYo9RS6ZyUstxYdjjStQaFaL2TSTVdsiJh");
        let laptop = did!("key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH");
        let phone = did!("key:z6MkjchhfUsD6mmvni8mCdXHw216Xrm9bQe2mBH1P5RDjVJG");

        record(
            &state,
            &laptop,
            &first,
            "notes",
            &Location::temp("a"),
            &session,
        )
        .await?;
        record(
            &state,
            &phone,
            &second,
            "notes",
            &Location::temp("b"),
            &session,
        )
        .await?;

        assert_eq!(
            find(&state, &laptop, "notes", &session).await?,
            vec![(first, Location::temp("a"))]
        );
        assert_eq!(
            find(&state, &phone, "notes", &session).await?,
            vec![(second, Location::temp("b"))]
        );
        Ok(())
    }
}
