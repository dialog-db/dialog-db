//! A host's contacts, kept in its state branch.
//!
//! The host's environment provides the `peer` effects over these: it
//! decides where its state lives, and these say what is recorded there.
//! Contacts are written under the machinery scope, since `dialog.peer/*`
//! is reserved: only the host writes its own state, so what is recorded
//! was authorized when it was written.

use dialog_artifacts::{Changes, Entity};
use dialog_effects::peer::PeerAddress;
use dialog_query::{EvaluationError, Output as _, Query, Statement as _, Term};

use crate::registry::{RegistryEnv, apply};
use crate::schema::{self, peer};
use crate::{Branch, CommitError};

/// Record in `state` that `peer` is reached at `address`.
pub async fn add_address<Env: RegistryEnv>(
    state: &Branch,
    peer: &Entity,
    address: &PeerAddress,
    env: &Env,
) -> Result<(), CommitError> {
    let mut changes = Changes::new();
    schema::PeerAddress {
        this: peer.clone(),
        address: peer::Address(address.0.clone()),
    }
    .assert(&mut changes);
    apply(state, changes, env).await
}

/// Record in `state` that `peer` is no longer reached at `address`.
pub async fn remove_address<Env: RegistryEnv>(
    state: &Branch,
    peer: &Entity,
    address: &PeerAddress,
    env: &Env,
) -> Result<(), CommitError> {
    let mut changes = Changes::new();
    schema::PeerAddress {
        this: peer.clone(),
        address: peer::Address(address.0.clone()),
    }
    .retract(&mut changes);
    apply(state, changes, env).await
}

/// Take back from `state` the name `peer` is known by, if it has one.
pub async fn remove_name<Env: RegistryEnv>(
    state: &Branch,
    peer: &Entity,
    env: &Env,
) -> Result<(), CommitError> {
    let named: Vec<schema::Contact> = Box::pin(
        state
            .query()
            .select(Query::<schema::Contact> {
                this: peer.clone().into(),
                name: Term::var("name"),
            })
            .perform(env)
            .try_vec(),
    )
    .await
    .map_err(|error| CommitError::Registry(error.to_string()))?;
    let mut changes = Changes::new();
    for contact in named {
        contact.retract(&mut changes);
    }
    apply(state, changes, env).await
}

/// Record in `state` that `peer` is known by `name`, replacing any name
/// it had.
pub async fn set_name<Env: RegistryEnv>(
    state: &Branch,
    peer: &Entity,
    name: &str,
    env: &Env,
) -> Result<(), CommitError> {
    let mut changes = Changes::new();
    schema::Contact {
        this: peer.clone(),
        name: peer::Name(name.to_string()),
    }
    .assert(&mut changes);
    apply(state, changes, env).await
}

/// The peers `state` knows by `name`.
pub async fn find<Env: RegistryEnv>(
    state: &Branch,
    name: &str,
    env: &Env,
) -> Result<Vec<Entity>, EvaluationError> {
    let rows: Vec<schema::Contact> = Box::pin(
        state
            .query()
            .select(Query::<schema::Contact> {
                this: Term::var("this"),
                name: name.to_string().into(),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    Ok(rows.into_iter().map(|row| row.this).collect())
}

/// Every address `state` records for `peer`.
pub async fn addresses<Env: RegistryEnv>(
    state: &Branch,
    peer: &Entity,
    env: &Env,
) -> Result<Vec<PeerAddress>, EvaluationError> {
    let rows: Vec<schema::PeerAddress> = Box::pin(
        state
            .query()
            .select(Query::<schema::PeerAddress> {
                this: peer.clone().into(),
                address: Term::var("address"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| PeerAddress(row.address.0))
        .collect())
}
