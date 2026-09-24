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
pub async fn record<Env: RegistryEnv>(
    state: &Branch,
    subject: &Did,
    name: &str,
    location: &Location,
    env: &Env,
) -> Result<(), SpaceError> {
    let mut changes = Changes::new();
    Space {
        this: subject.this(),
        name: space::Name(name.to_string()),
        address: space::Address(location.uri()),
    }
    .assert(&mut changes);
    Ok(apply(state, changes, env).await?)
}

/// The repositories `state` knows by `name`, and where each is stored.
pub async fn find<Env: RegistryEnv>(
    state: &Branch,
    name: &str,
    env: &Env,
) -> Result<Vec<(Did, Location)>, SpaceError> {
    let rows: Vec<Space> = Box::pin(
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
    .await?;
    rows.into_iter()
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
