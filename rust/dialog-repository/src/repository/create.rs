use crate::repository::upgrade::stamp;
use crate::{CreateRepositoryError, Repository};
use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_credentials::credential::{Credential, SignerCredential};
use dialog_credentials::{Ed25519Signer, Extractable};
use dialog_effects::memory::Publish;
use dialog_effects::space::{self, SpaceExt};
use dialog_effects::storage::StorageError;

/// Command to create a new repository.
///
/// Returns `Repository<SignerCredential>` since a freshly generated
/// credential always has a private key.
pub struct CreateRepository(pub Capability<space::Space>);

impl CreateRepository {
    /// Create the repository under a key the environment generates.
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<Repository<SignerCredential>, CreateRepositoryError>
    where
        Env: Provider<space::Create> + Provider<Publish> + ConditionalSync,
    {
        let created = self.0.create().perform(env).await?;
        created_repository(created, env).await
    }

    /// Create the repository under a caller-supplied key instead of one
    /// the environment generates.
    ///
    /// Useful when the space name is derived from the key's DID: generate
    /// the key first, derive the name, then create the repository under
    /// it. The key must be extractable, since the environment seals it to
    /// the account the repository delegates to.
    ///
    /// ```no_run
    /// # async fn example<Env>(
    /// #     peer: &dialog_peer::Peer<dialog_storage::provider::storage::VolatileSpace>,
    /// #     operator: &Env,
    /// # ) -> Result<(), Box<dyn std::error::Error>>
    /// # where Env: dialog_capability::Provider<dialog_effects::space::Create>
    /// #     + dialog_capability::Provider<dialog_effects::memory::Publish>
    /// #     + dialog_common::ConditionalSync {
    /// use dialog_credentials::{Ed25519Signer, Extractable, ExtractableKey as _};
    /// use dialog_repository::RepositoryExt;
    /// use dialog_varsig::Principal;
    ///
    /// let key = <Ed25519Signer<Extractable> as dialog_credentials::ExtractableKey>::generate().await?;
    /// let did = key.did().to_string();
    /// let name = &did[did.len() - 8..];
    ///
    /// let repo = peer
    ///     .space(name)
    ///     .create()
    ///     .with_credential(key)
    ///     .perform(operator)
    ///     .await?;
    /// # let _ = repo;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_credential(self, key: Ed25519Signer<Extractable>) -> CreateRepositoryWith {
        CreateRepositoryWith { space: self.0, key }
    }
}

/// A [`CreateRepository`] command bound to a caller-supplied key.
pub struct CreateRepositoryWith {
    space: Capability<space::Space>,
    key: Ed25519Signer<Extractable>,
}

impl CreateRepositoryWith {
    /// Execute against an operator.
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<Repository<SignerCredential>, CreateRepositoryError>
    where
        Env: Provider<space::Create> + Provider<Publish> + ConditionalSync,
    {
        let created = self.space.create_with(self.key).perform(env).await?;
        created_repository(created, env).await
    }
}

/// The repository just created under `created`, the key the environment
/// handed back, with its layout version recorded.
async fn created_repository<Env>(
    created: Credential,
    env: &Env,
) -> Result<Repository<SignerCredential>, CreateRepositoryError>
where
    Env: Provider<Publish> + ConditionalSync,
{
    let Credential::Signer(signer) = created else {
        return Err(CreateRepositoryError::Storage(StorageError::Storage(
            "the environment created the repository without handing back its key".into(),
        )));
    };
    let repository = Repository::from(signer);
    stamp(&repository.subject(), env).await?;
    Ok(repository)
}
