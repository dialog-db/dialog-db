use crate::{CreateRepositoryError, Repository};
use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_credentials::Ed25519Signer;
use dialog_credentials::credential::{Credential, SignerCredential};
use dialog_effects::space::{self, SpaceExt};

/// Command to create a new repository.
///
/// Returns `Repository<SignerCredential>` since a freshly generated
/// credential always has a private key.
pub struct CreateRepository(pub Capability<space::Space>);

impl CreateRepository {
    /// Create the repository with a freshly generated keypair.
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<Repository<SignerCredential>, CreateRepositoryError>
    where
        Env: Provider<space::Create> + ConditionalSync,
    {
        self.with_credential(Ed25519Signer::generate().await?)
            .perform(env)
            .await
    }

    /// Create the repository with a caller-supplied credential instead
    /// of generating a fresh keypair.
    ///
    /// Useful when the space name is derived from the credential's DID:
    /// generate the signer first, derive the name, then create the
    /// repository with that same signer.
    ///
    /// ```no_run
    /// # async fn example<Env>(
    /// #     profile: &dialog_operator::Profile,
    /// #     operator: &Env,
    /// # ) -> Result<(), Box<dyn std::error::Error>>
    /// # where Env: dialog_capability::Provider<dialog_effects::space::Create> + dialog_common::ConditionalSync {
    /// use dialog_credentials::Ed25519Signer;
    /// use dialog_repository::RepositoryExt;
    /// use dialog_varsig::Principal;
    ///
    /// let signer = Ed25519Signer::generate().await?;
    /// let did = signer.did().to_string();
    /// let name = &did[did.len() - 8..];
    ///
    /// let repo = profile
    ///     .repository(name)
    ///     .create()
    ///     .with_credential(signer)
    ///     .perform(operator)
    ///     .await?;
    /// # let _ = repo;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_credential(self, credential: impl Into<SignerCredential>) -> CreateRepositoryWith {
        CreateRepositoryWith {
            space: self.0,
            credential: credential.into(),
        }
    }
}

/// A [`CreateRepository`] command bound to a caller-supplied credential.
///
/// Because the credential is already provided, `perform` cannot fail to
/// generate a keypair — the only failure is backend storage.
pub struct CreateRepositoryWith {
    space: Capability<space::Space>,
    credential: SignerCredential,
}

impl CreateRepositoryWith {
    /// Execute against an operator.
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<Repository<SignerCredential>, CreateRepositoryError>
    where
        Env: Provider<space::Create> + ConditionalSync,
    {
        self.space
            .create(Credential::Signer(self.credential.clone()))
            .perform(env)
            .await?;
        Ok(Repository::from(self.credential))
    }
}
