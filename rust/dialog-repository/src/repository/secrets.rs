//! Sealed messages, the vaults they are sealed to, and the secrets vaults
//! keep by name, recorded in the space a peer keeps its records in.
//!
//! A top-level vault is recorded by name; a vault derived from another is
//! recorded by its parent and its name within it, with the parent's
//! signature. A vault's members hold a copy of its key, itself a sealed
//! message, so adding a member is one more copy and nothing sealed to the
//! vault is sealed again. Everything recorded here is ciphertext: the space, and
//! the storage it lives in, hold nothing readable without a recipient's
//! key.

use base58::ToBase58 as _;
use dialog_artifacts::{Changes, Entity};
use dialog_common::Blake3Hash;
use dialog_query::{EvaluationError, Output as _, Query, Statement as _, Term};
use dialog_varsig::Did;

use crate::registry::{RegistryEnv, apply};
use crate::schema::{
    ChildVault, DidExt as _, RootVault, SealedKey, SealedMessage, SecretPrincipal, VaultSecret,
    secret, vault, vault_secret,
};
use crate::{Branch, CommitError};

/// Why a sealed message, a vault or a kept secret could not be
/// recorded or read back.
#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    /// Reading the peer's records failed.
    #[error("Failed to read sealed messages: {0}")]
    Query(#[from] EvaluationError),

    /// Recording failed.
    #[error("Failed to record a sealed message: {0}")]
    Commit(#[from] CommitError),

    /// A recorded principal could not be read back as a DID.
    #[error("A recorded principal could not be read: {0}")]
    Encoding(String),
}

/// An entity derived from `bytes`, under `scheme`.
fn derived(scheme: &str, bytes: &[u8]) -> Entity {
    format!(
        "{scheme}:{}",
        Blake3Hash::hash(bytes).as_bytes().to_base58()
    )
    .parse()
    .expect("a base58 digest makes a valid entity URI")
}

/// The DID an entity recorded for a principal names.
fn principal(entity: &Entity) -> Result<Did, SecretError> {
    entity
        .to_string()
        .parse()
        .map_err(|_| SecretError::Encoding(format!("{entity} is not a DID")))
}

/// The entity of the message whose ciphertext is `message`.
pub fn message_entity(message: &[u8]) -> Entity {
    derived("secret", message)
}

/// A sealed message, as a statement to assert: `message` sealed to `to`.
pub fn sealed_message(to: &Did, message: Vec<u8>) -> SealedMessage {
    SealedMessage {
        this: message_entity(&message),
        to: secret::To(to.this()),
        message: secret::Message(message),
    }
}

/// Record the sealed `message` in `state`.
pub async fn record<Env: RegistryEnv>(
    state: &Branch,
    message: SealedMessage,
    env: &Env,
) -> Result<(), SecretError> {
    let mut changes = Changes::new();
    message.assert(&mut changes);
    Ok(apply(state, changes, env).await?)
}

/// Record in `state` a copy of `principal`'s key, sealed to `holder`.
pub async fn grant<Env: RegistryEnv>(
    state: &Branch,
    principal: &Did,
    holder: &Did,
    sealed: Vec<u8>,
    env: &Env,
) -> Result<(), SecretError> {
    let mut changes = Changes::new();
    let message = sealed_message(holder, sealed);
    SealedKey {
        this: message.this.clone(),
        key_of: secret::KeyOf(principal.this()),
    }
    .assert(&mut changes);
    message.assert(&mut changes);
    Ok(apply(state, changes, env).await?)
}

/// The copies of `principal`'s key sealed to `holder`.
pub async fn keys_of<Env: RegistryEnv>(
    state: &Branch,
    principal: &Did,
    holder: &Did,
    env: &Env,
) -> Result<Vec<Vec<u8>>, SecretError> {
    let copies: Vec<SealedKey> = Box::pin(
        state
            .query()
            .select(Query::<SealedKey> {
                this: Term::var("this"),
                key_of: principal.this().into(),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    let mut keys = Vec::new();
    for copy in copies {
        if let Some((to, sealed)) = message(state, &copy.this, env).await?
            && to == *holder
        {
            keys.push(sealed);
        }
    }
    Ok(keys)
}

/// Every copy of a principal's key sealed to `holder`, with the principal
/// whose key each holds.
pub async fn keys_for<Env: RegistryEnv>(
    state: &Branch,
    holder: &Did,
    env: &Env,
) -> Result<Vec<(Did, Vec<u8>)>, SecretError> {
    let messages: Vec<SealedMessage> = Box::pin(
        state
            .query()
            .select(Query::<SealedMessage> {
                this: Term::var("this"),
                to: holder.this().into(),
                message: Term::var("message"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    let mut keys = Vec::new();
    for message in messages {
        let holds: Vec<SealedKey> = Box::pin(
            state
                .query()
                .select(Query::<SealedKey> {
                    this: message.this.clone().into(),
                    key_of: Term::var("key_of"),
                })
                .perform(env)
                .try_vec(),
        )
        .await?;
        for held in holds {
            keys.push((principal(&held.key_of.0)?, message.message.0.clone()));
        }
    }
    Ok(keys)
}

/// The message `this`: who it is sealed to, and its ciphertext.
pub async fn message<Env: RegistryEnv>(
    state: &Branch,
    this: &Entity,
    env: &Env,
) -> Result<Option<(Did, Vec<u8>)>, SecretError> {
    let rows: Vec<SealedMessage> = Box::pin(
        state
            .query()
            .select(Query::<SealedMessage> {
                this: this.clone().into(),
                to: Term::var("to"),
                message: Term::var("message"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    match rows.into_iter().next() {
        Some(row) => Ok(Some((principal(&row.to.0)?, row.message.0))),
        None => Ok(None),
    }
}

/// A principal whose key is held sealed: what it is, who can open the
/// message holding its key, and the message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldPrincipal {
    /// What the principal is: `space` for a repository's key.
    pub kind: String,
    /// Who the message holding its key is sealed to.
    pub to: Did,
    /// The sealed key.
    pub sealed: Vec<u8>,
}

/// The principal `principal`, if `state` records its key held sealed.
pub async fn held_principal<Env: RegistryEnv>(
    state: &Branch,
    principal: &Did,
    env: &Env,
) -> Result<Option<HeldPrincipal>, SecretError> {
    let rows: Vec<SecretPrincipal> = Box::pin(
        state
            .query()
            .select(Query::<SecretPrincipal> {
                this: principal.this().into(),
                kind: Term::var("kind"),
                seed: Term::var("seed"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    let Some((to, sealed)) = message(state, &row.seed.0, env).await? else {
        return Ok(None);
    };
    Ok(Some(HeldPrincipal {
        kind: row.kind.0,
        to,
        sealed,
    }))
}

/// Record in `state` that `principal`'s key is held sealed in `message`,
/// recording the message with it, in place of what held it before.
pub async fn hold_principal<Env: RegistryEnv>(
    state: &Branch,
    principal: &Did,
    kind: &str,
    message: SealedMessage,
    env: &Env,
) -> Result<(), SecretError> {
    let rows: Vec<SecretPrincipal> = Box::pin(
        state
            .query()
            .select(Query::<SecretPrincipal> {
                this: principal.this().into(),
                kind: Term::var("kind"),
                seed: Term::var("seed"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    let mut changes = Changes::new();
    for row in rows {
        row.retract(&mut changes);
    }
    SecretPrincipal {
        this: principal.this(),
        kind: secret::Kind(kind.to_string()),
        seed: secret::Seed(message.this.clone()),
    }
    .assert(&mut changes);
    message.assert(&mut changes);
    Ok(apply(state, changes, env).await?)
}

/// The top-level vault `state` records as `name`.
pub async fn root<Env: RegistryEnv>(
    state: &Branch,
    name: &str,
    env: &Env,
) -> Result<Option<Did>, SecretError> {
    let rows: Vec<RootVault> = Box::pin(
        state
            .query()
            .select(Query::<RootVault> {
                this: Term::var("this"),
                name: name.to_string().into(),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    match rows.into_iter().next() {
        Some(row) => Ok(Some(principal(&row.this)?)),
        None => Ok(None),
    }
}

/// Record in `state` the top-level vault `vault` as `name`.
pub async fn record_root<Env: RegistryEnv>(
    state: &Branch,
    vault: &Did,
    name: &str,
    env: &Env,
) -> Result<(), SecretError> {
    let mut changes = Changes::new();
    RootVault {
        this: vault.this(),
        name: vault::Name(name.to_string()),
    }
    .assert(&mut changes);
    Ok(apply(state, changes, env).await?)
}

/// The vaults `state` records as `parent`'s child `name`, each with the
/// parent's signature over it.
pub async fn children<Env: RegistryEnv>(
    state: &Branch,
    parent: &Did,
    name: &str,
    env: &Env,
) -> Result<Vec<(Did, Vec<u8>)>, SecretError> {
    let rows: Vec<ChildVault> = Box::pin(
        state
            .query()
            .select(Query::<ChildVault> {
                this: Term::var("this"),
                parent: parent.this().into(),
                name: name.to_string().into(),
                signature: Term::var("signature"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    rows.into_iter()
        .map(|row| Ok((principal(&row.this)?, row.signature.0)))
        .collect()
}

/// Record in `state` the vault `vault` as `parent`'s child `name`, with
/// the parent's `signature` over it.
pub async fn record_child<Env: RegistryEnv>(
    state: &Branch,
    vault: &Did,
    parent: &Did,
    name: &str,
    signature: Vec<u8>,
    env: &Env,
) -> Result<(), SecretError> {
    let mut changes = Changes::new();
    ChildVault {
        this: vault.this(),
        parent: vault::Parent(parent.this()),
        name: vault::Name(name.to_string()),
        signature: vault::Signature(signature),
    }
    .assert(&mut changes);
    Ok(apply(state, changes, env).await?)
}

/// The entity of the secret `vault` keeps as `name`.
fn secret_entity(vault: &Did, name: &str) -> Entity {
    derived("secret", format!("{vault}\u{0}{name}").as_bytes())
}

/// Record in `state` that `vault` keeps the sealed `message` as `name`, in
/// place of what it kept under the name before.
pub async fn keep_secret<Env: RegistryEnv>(
    state: &Branch,
    vault: &Did,
    name: &str,
    message: SealedMessage,
    env: &Env,
) -> Result<(), SecretError> {
    let mut changes = Changes::new();
    VaultSecret {
        this: secret_entity(vault, name),
        vault: vault_secret::Vault(vault.this()),
        name: vault_secret::Name(name.to_string()),
        message: vault_secret::Message(message.this.clone()),
    }
    .assert(&mut changes);
    message.assert(&mut changes);
    Ok(apply(state, changes, env).await?)
}

/// The sealed message `vault` keeps as `name`, if `state` records one.
pub async fn secret<Env: RegistryEnv>(
    state: &Branch,
    vault: &Did,
    name: &str,
    env: &Env,
) -> Result<Option<SealedMessage>, SecretError> {
    let Some(row) = kept(state, vault, name, env).await?.into_iter().next() else {
        return Ok(None);
    };
    let Some((to, message)) = message(state, &row.message.0, env).await? else {
        return Ok(None);
    };
    Ok(Some(sealed_message(&to, message)))
}

/// Forget the secret `vault` keeps as `name`.
pub async fn forget_secret<Env: RegistryEnv>(
    state: &Branch,
    vault: &Did,
    name: &str,
    env: &Env,
) -> Result<(), SecretError> {
    let mut changes = Changes::new();
    for row in kept(state, vault, name, env).await? {
        row.retract(&mut changes);
    }
    Ok(apply(state, changes, env).await?)
}

async fn kept<Env: RegistryEnv>(
    state: &Branch,
    vault: &Did,
    name: &str,
    env: &Env,
) -> Result<Vec<VaultSecret>, SecretError> {
    Ok(Box::pin(
        state
            .query()
            .select(Query::<VaultSecret> {
                this: secret_entity(vault, name).into(),
                vault: vault.this().into(),
                name: name.to_string().into(),
                message: Term::var("message"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?)
}
