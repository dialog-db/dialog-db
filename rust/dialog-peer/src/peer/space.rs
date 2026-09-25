//! Space capability providers for [`Peer`].

use super::{Mode, Peer};
use dialog_capability::{Capability, Policy, Provider, Subject, did};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::Credential;
use dialog_effects::credential::{self as credential_fx, prelude::*};
use dialog_effects::space as space_fx;
use dialog_effects::storage::{self as storage_fx, LocationExt as _};
use dialog_repository::registry::RegistryEnv;
use dialog_repository::spaces;
use dialog_storage::provider::storage::Storage;
use dialog_varsig::{Did, Principal as _};

/// The credential of a loaded space as a handle in mode `M` is given it:
/// whole to one that holds keys, and without its signing key otherwise.
fn handed<M: Mode>(credential: Credential) -> Credential {
    match credential.signer() {
        Some(signer) if !M::HOLDS_KEYS => Credential::from(signer.verifier()),
        _ => credential,
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<space_fx::Load> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Storage<S>: Provider<storage_fx::Load> + Provider<credential_fx::Load<Credential>>,
    Self: RegistryEnv + ConditionalSend + ConditionalSync,
{
    /// A space name resolves to the repository the peer recorded under
    /// it, loaded from where it was recorded. A name the peer has no
    /// record of is looked for in its base directory, and recorded when
    /// found there.
    async fn execute(
        &self,
        input: Capability<space_fx::Load>,
    ) -> Result<Credential, storage_fx::StorageError> {
        let subject = input.subject();
        if *subject != *self.home() {
            return Err(storage_fx::StorageError::Storage(format!(
                "space load denied: subject {subject} does not match the home {}",
                self.home()
            )));
        }

        let name = &space_fx::Space::of(&input).name;
        if let Some((repository, location)) = self.recorded_space(name).await? {
            let credential = self.load_at(location).await?;
            if credential.did() != repository {
                return Err(storage_fx::StorageError::Storage(format!(
                    "space {name} is recorded as {repository}, but its location holds {}",
                    credential.did()
                )));
            }
            return Ok(handed::<M>(credential));
        }

        // A repository named by its DID is the one already mounted, if it
        // is; else it may be recorded under another name, and is found by
        // the repository it is, from where it was recorded.
        if let Ok(repository) = name.parse::<Did>()
            && let Some(credential) = self.mounted(&repository).await
        {
            return Ok(handed::<M>(credential));
        }
        if let Ok(repository) = name.parse::<Did>()
            && let Some(location) = self.located(&repository).await?
        {
            let credential = self.load_at(location).await?;
            if credential.did() != repository {
                return Err(storage_fx::StorageError::Storage(format!(
                    "{repository} is recorded at a location that holds {}",
                    credential.did()
                )));
            }
            return Ok(handed::<M>(credential));
        }

        let location = storage_fx::Location::new(self.directory().clone(), name);
        let credential = self.load_at(location.clone()).await?;
        self.record_space(&credential.did(), name, &location).await;
        Ok(handed::<M>(credential))
    }
}

impl<S, M: Mode> Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: RegistryEnv,
{
    /// The repository this peer recorded under `name`, and where it is
    /// stored, if it recorded one. An ephemeral peer records nothing.
    async fn recorded_space(
        &self,
        name: &str,
    ) -> Result<Option<(Did, storage_fx::Location)>, storage_fx::StorageError> {
        let Some(state) = self.state_opt() else {
            return Ok(None);
        };
        let failed = |error: String| storage_fx::StorageError::Storage(error);
        state
            .refresh(self)
            .await
            .map_err(|error| failed(error.to_string()))?;
        let mut found = spaces::find(state, name, self)
            .await
            .map_err(|error| failed(error.to_string()))?;
        match found.len() {
            0 => Ok(None),
            1 => Ok(found.pop()),
            _ => Err(failed(format!(
                "more than one repository is recorded as {name}"
            ))),
        }
    }

    /// Where this peer recorded the repository `repository` is stored,
    /// under any name. An ephemeral peer records nothing.
    async fn located(
        &self,
        repository: &Did,
    ) -> Result<Option<storage_fx::Location>, storage_fx::StorageError> {
        let Some(state) = self.state_opt() else {
            return Ok(None);
        };
        let found = spaces::locate(state, repository, self)
            .await
            .map_err(|error| storage_fx::StorageError::Storage(error.to_string()))?;
        Ok(found.into_iter().next().map(|(_, location)| location))
    }

    /// Record that the repository `repository` is known as `name` and
    /// stored at `location`.
    ///
    /// Best-effort: the repository is where it is either way, and a name
    /// left unrecorded is found in the base directory and recorded the
    /// next time it is loaded.
    async fn record_space(&self, repository: &Did, name: &str, location: &storage_fx::Location) {
        if let Some(state) = self.state_opt() {
            let _ = spaces::record(state, repository, name, location, self).await;
        }
    }
}

impl<S: Clone, M: Mode> Peer<S, M>
where
    Storage<S>: Provider<storage_fx::Load> + Provider<credential_fx::Load<Credential>>,
{
    /// The credential of the repository `repository`, if its space is
    /// mounted in this peer's storage.
    async fn mounted(&self, repository: &Did) -> Option<Credential> {
        Subject::from(repository.clone())
            .credential()
            .key(credential_fx::SELF)
            .load()
            .perform(&self.storage)
            .await
            .ok()
    }

    /// The credential of the space stored at `location`.
    async fn load_at(
        &self,
        location: storage_fx::Location,
    ) -> Result<Credential, storage_fx::StorageError> {
        Subject::from(did!("local:storage"))
            .attenuate(storage_fx::Storage)
            .attenuate(location)
            .load()
            .perform(&self.storage)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<space_fx::Create> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Storage<S>: Provider<storage_fx::Create>,
    Self: RegistryEnv + ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<space_fx::Create>,
    ) -> Result<Credential, storage_fx::StorageError> {
        let subject = input.subject();
        if *subject != *self.home() {
            return Err(storage_fx::StorageError::Storage(format!(
                "space create denied: subject {subject} does not match the home {}",
                self.home()
            )));
        }

        let name = &space_fx::Space::of(&input).name;
        let credential = space_fx::Create::of(&input).credential.clone();
        let location = storage_fx::Location::new(self.directory().clone(), name);
        let created = Subject::from(did!("local:storage"))
            .attenuate(storage_fx::Storage)
            .attenuate(location.clone())
            .create(credential)
            .perform(&self.storage)
            .await?;
        self.record_space(&created.did(), name, &location).await;
        Ok(created)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::unique_name;
    use crate::{ClaimExt as _, Peer};
    use dialog_capability::{Subject, did};
    use dialog_credentials::{Credential, Ed25519Signer, SignerCredential};
    use dialog_effects::storage::{self as storage_fx, Directory, Location, LocationExt as _};
    use dialog_identity::OpenCredential;
    use dialog_repository::{RepositoryExt as _, spaces};
    use dialog_storage::provider::storage::{Storage, VolatileSpace};
    use dialog_varsig::Principal as _;

    /// A peer over `storage` acting as `credential`, looking for names it
    /// has no record of under `base`.
    async fn peer_at(
        storage: &Storage<VolatileSpace>,
        credential: &SignerCredential,
        base: &str,
    ) -> anyhow::Result<Peer<VolatileSpace>> {
        Ok(Peer::new(credential.clone())
            .storage(storage.clone())
            .base(Directory::At(base.into()))
            .await?)
    }

    /// A name resolves to the repository recorded under it, loaded from
    /// where it was recorded, though the peer now looks for names it has
    /// no record of somewhere else.
    #[dialog_common::test]
    async fn it_resolves_a_name_from_where_it_was_recorded() -> anyhow::Result<()> {
        let storage = Storage::volatile();
        let credential = OpenCredential::open(unique_name("alice"))
            .perform(&storage)
            .await?;
        let name = unique_name("notes");

        let first = peer_at(&storage, &credential, "/first").await?;
        let created = first.space(name.clone()).create().perform(&first).await?;

        let second = peer_at(&storage, &credential, "/second").await?;
        let loaded = second.space(name).load().perform(&second).await?;
        assert_eq!(loaded.did(), created.did());
        Ok(())
    }

    /// Opening a space mounts it in storage, which is the storage's to
    /// allow: a session its peer granted nothing over storage is refused,
    /// while one granted everything the peer holds opens it.
    #[dialog_common::test]
    async fn it_opens_a_space_for_a_session_only_with_the_storages_authority() -> anyhow::Result<()>
    {
        let storage = Storage::volatile();
        let credential = OpenCredential::open(unique_name("alice"))
            .perform(&storage)
            .await?;
        let peer = peer_at(&storage, &credential, "/gate").await?;
        let name = unique_name("notes");
        peer.space(name.clone()).create().perform(&peer).await?;

        let elsewhere = Ed25519Signer::generate().await?;
        let scoped = peer
            .session(b"scoped")
            .allow(Subject::from(elsewhere.did()).claim(peer.credential()))
            .await?;
        let refused = peer.space(name.clone()).load().perform(&scoped).await;
        assert!(
            refused.is_err(),
            "a session with no storage authority mounted a space"
        );

        let trusted = peer
            .session(b"trusted")
            .allow(Subject::any().claim(peer.credential()))
            .await?;
        let loaded = peer.space(name).load().perform(&trusted).await?;
        assert!(!loaded.did().to_string().is_empty());
        Ok(())
    }

    /// A repository found in the base directory, with no record of its
    /// name, is recorded the first time it is loaded.
    #[dialog_common::test]
    async fn it_records_a_name_found_in_the_base_directory() -> anyhow::Result<()> {
        let storage = Storage::volatile();
        let credential = OpenCredential::open(unique_name("alice"))
            .perform(&storage)
            .await?;
        let peer = peer_at(&storage, &credential, "/base").await?;
        let name = unique_name("notes");

        // Stored where the peer looks, but never recorded.
        let location = Location::new(Directory::At("/base".into()), name.as_str());
        let repository =
            Credential::Signer(SignerCredential::from(Ed25519Signer::generate().await?));
        Subject::from(did!("local:storage"))
            .attenuate(storage_fx::Storage)
            .attenuate(location.clone())
            .create(repository.clone())
            .perform(&storage)
            .await?;

        let state = peer.state()?;
        assert!(spaces::find(state, &name, &peer).await?.is_empty());
        let loaded = peer.space(name.clone()).load().perform(&peer).await?;
        assert_eq!(loaded.did(), repository.did());
        state.refresh(&peer).await?;
        assert_eq!(
            spaces::find(state, &name, &peer).await?,
            vec![(repository.did(), location)]
        );
        Ok(())
    }

    /// A repository created under a name is opened by its DID from a new
    /// process over the same storage: nothing is mounted yet, and nothing
    /// is at the location its DID names, so it is found from where the
    /// peer recorded it.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_opens_a_recorded_repository_by_did_after_a_restart() -> anyhow::Result<()> {
        use dialog_repository::RepositoryAtExt as _;
        use dialog_storage::provider::storage::NativeSpace;

        let root = tempfile::tempdir()?;
        let base = Directory::At(root.path().to_string_lossy().into_owned());
        let name = unique_name("alice");

        let first = Storage::<NativeSpace>::default();
        let credential = OpenCredential::open(name.clone())
            .at(base.clone())
            .perform(&first)
            .await?;
        let peer = Peer::new(credential.clone())
            .storage(first)
            .base(base.clone())
            .await?;
        let notes = peer.space("notes").create().perform(&peer).await?;

        let second = Storage::<NativeSpace>::default();
        let credential = OpenCredential::load(name)
            .at(base.clone())
            .perform(&second)
            .await?;
        let restarted = Peer::new(credential).storage(second).base(base).await?;
        let branch = restarted
            .did()
            .repository(notes.did())
            .branch("main")
            .open()
            .perform(&restarted)
            .await?;
        assert_eq!(branch.subject().did(), &notes.did());
        Ok(())
    }
}
