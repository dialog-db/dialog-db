//! Remote site configuration and management.

use dialog_capability::Did;

use super::{
    Connection, OperatingAuthority, Operator, PlatformBackend, PlatformStorage, RemoteRepository,
    RemoteState, Site,
};
use crate::TypedStoreResource;
use crate::replica::ReplicaError;

/// Represents a configured remote site with its credentials.
///
/// This is the persisted state for a remote, storing the site name
/// and the credentials needed to connect to it.
pub struct RemoteSite<Backend: PlatformBackend, A: OperatingAuthority = Operator> {
    /// The site name.
    name: Site,
    /// Memory cell storing the remote state.
    memory: TypedStoreResource<RemoteState, Backend>,
    /// Storage for persistence (cloned, cheap).
    storage: PlatformStorage<Backend>,
    /// Issuer for signing requests.
    issuer: A,
}

impl<Backend: PlatformBackend, A: OperatingAuthority> RemoteSite<Backend, A> {
    /// Returns the site name.
    pub fn name(&self) -> &Site {
        &self.name
    }
}

impl<Backend: PlatformBackend + 'static, A: OperatingAuthority + 'static> RemoteSite<Backend, A> {
    /// Adds a new remote site configuration and persists it. If site with
    /// conflicting name is already configured produces an error, unless
    /// persisted configuration is identical to passed one, in which case
    /// operation is a noop upholding idempotence.
    pub async fn add(
        state: RemoteState,
        issuer: A,
        mut storage: PlatformStorage<Backend>,
    ) -> Result<Self, ReplicaError> {
        let memory = Self::mount(&state.site, &mut storage).await?;

        // Check if remote already exists
        if let Some(existing_state) = memory.read() {
            if state != existing_state {
                return Err(ReplicaError::RemoteAlreadyExists {
                    remote: state.site.clone(),
                });
            }
            // Same state, just return the existing site
            return Ok(Self {
                name: state.site,
                memory,
                storage,
                issuer,
            });
        }

        // Persist the new state
        memory
            .replace(Some(state.clone()), &mut storage)
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        Ok(Self {
            name: state.site,
            memory,
            storage,
            issuer,
        })
    }

    /// Load remote site that has previously being added. If site with
    /// a given name does not exists produces an error.
    pub async fn load(
        site: &Site,
        issuer: A,
        mut storage: PlatformStorage<Backend>,
    ) -> Result<Self, ReplicaError> {
        let memory = Self::mount(site, &mut storage).await?;

        if memory.read().is_some() {
            Ok(Self {
                name: site.clone(),
                memory,
                storage,
                issuer,
            })
        } else {
            Err(ReplicaError::RemoteNotFound {
                remote: site.clone(),
            })
        }
    }

    /// Get the remote state.
    pub fn state(&self) -> Option<RemoteState> {
        self.memory.read()
    }

    /// Connect to the remote storage.
    ///
    /// Remote S3 operations require an Operator with secret key access.
    /// Construct one from the Authority's secret key bytes if available.
    pub fn connect(&self, subject: &Did) -> Result<Connection, ReplicaError> {
        if let Some(state) = self.memory.read() {
            // Remote S3 operations require an Operator with secret key access.
            let operator = match self.issuer.secret_key_bytes() {
                Some(bytes) => Operator::from_secret(&bytes),
                None => {
                    return Err(ReplicaError::StorageError(
                        "Remote operations require an authority with extractable key material"
                            .to_string(),
                    ));
                }
            };
            state.credentials.connect(operator, subject)
        } else {
            Err(ReplicaError::RemoteNotFound {
                remote: self.name.clone(),
            })
        }
    }

    /// Mount the transactional memory cell for a remote site.
    async fn mount(
        site: &Site,
        storage: &mut PlatformStorage<Backend>,
    ) -> Result<TypedStoreResource<RemoteState, Backend>, ReplicaError> {
        let address = format!("site/{}", site);
        storage
            .open::<RemoteState>(&address.into_bytes())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))
    }

    /// Start building a reference to a repository at this remote site.
    ///
    /// The `subject` is the DID identifying the repository owner.
    pub fn repository(&self, subject: impl Into<Did>) -> RemoteRepository<Backend, A> {
        RemoteRepository::new(
            self.name.clone(),
            subject.into(),
            self.storage.clone(),
            self.issuer.clone(),
            self.memory.read().map(|s| s.credentials),
        )
    }
}
