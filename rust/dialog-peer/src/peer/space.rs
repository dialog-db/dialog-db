//! Space capability providers for [`Peer`].

use core::fmt::Display;

use super::{Mode, Peer};
use dialog_capability::access::{Access, FromCapability as _, Prove, Retain};
use dialog_capability::{Ability, Capability, Constraint, Effect, Policy, Provider, Subject};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::key::{ExtractableKey, KeyExport};
use dialog_credentials::secret::{Context, SealedSecret};
use dialog_credentials::{
    Credential, Ed25519Signer, Ed25519Verifier, Extractable, Signer, SignerCredential,
};
use dialog_effects::credential::{self as credential_fx, prelude::*};
use dialog_effects::space as space_fx;
use dialog_effects::storage::{self as storage_fx, LocationExt as _};
use dialog_repository::registry::RegistryEnv;
use dialog_repository::spaces;
use dialog_storage::provider::storage::Storage;
use dialog_ucan::{Scope, Ucan, UcanDelegation};
use dialog_ucan_core::subject::Subject as UcanSubject;
use dialog_ucan_core::{DelegationBuilder, DelegationChain};
use dialog_varsig::{Did, Principal as _};

/// The context a space's key is sealed in, so a sealed key opens only as
/// one.
const SPACE_KEY: Context = Context::new("dialog.space/key");

/// A failure creating a space, as a storage error.
fn failed(error: impl Display) -> storage_fx::StorageError {
    storage_fx::StorageError::Storage(error.to_string())
}

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
    Storage<S>: Provider<storage_fx::Load>
        + Provider<credential_fx::Load<Credential>>
        + Provider<credential_fx::Save<Credential>>,
    Self: RegistryEnv
        + Provider<Prove<Ucan>>
        + Provider<Retain<Ucan>>
        + ConditionalSend
        + ConditionalSync,
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
            return self.adopted(credential).await;
        }

        // A repository named by its DID is the one already mounted, if it
        // is; else it may be recorded under another name, and is found by
        // the repository it is, from where it was recorded.
        if let Ok(repository) = name.parse::<Did>()
            && let Some(credential) = self.mounted(&repository).await
        {
            return self.adopted(credential).await;
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
            return self.adopted(credential).await;
        }

        let location = storage_fx::Location::new(self.directory().clone(), name);
        let credential = self.load_at(location.clone()).await?;
        self.record_space(&credential.did(), name, &location).await;
        self.adopted(credential).await
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

    /// Keep the sealed key of the repository `repository` in this peer's
    /// state, as it keeps the space's name and location: a peer keeping
    /// its state only in memory keeps the sealed key as long as it does.
    async fn seal_space(
        &self,
        repository: &Did,
        sealed: Vec<u8>,
    ) -> Result<(), storage_fx::StorageError> {
        match self.state_opt() {
            Some(state) => spaces::seal(state, repository, sealed, self)
                .await
                .map_err(failed),
            None => Ok(()),
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
    ) -> Result<Credential, storage_fx::StorageError>
    where
        Self: Provider<Prove<Ucan>>,
    {
        let load = Subject::from(self.system().clone())
            .attenuate(storage_fx::Storage)
            .attenuate(location)
            .load();
        self.may_mount(&load).await?;
        load.perform(&self.storage).await
    }

    /// `credential` as this peer hands a loaded space over, after
    /// bringing a space from before keys were sealed up to date.
    ///
    /// Such a space still holds its signing key. Its key is sealed to the
    /// account and the space delegates to it, and only then is the stored
    /// key replaced by its verifier: a load that stops part-way finds the
    /// key still there and does it again. The home is never touched, since
    /// its key is this peer's own identity.
    async fn adopted(&self, credential: Credential) -> Result<Credential, storage_fx::StorageError>
    where
        S: ConditionalSend + ConditionalSync + 'static,
        Self: RegistryEnv + Provider<Retain<Ucan>>,
        Storage<S>: Provider<credential_fx::Save<Credential>>,
    {
        let Credential::Signer(signer) = &credential else {
            return Ok(handed::<M>(credential));
        };
        if credential.did() == *self.home() {
            return Ok(handed::<M>(credential));
        }
        let signer = signer.signer().clone();
        // Other algorithms are features; without them this always holds.
        #[allow(irrefutable_let_patterns)]
        let Signer::Ed25519(ed25519) = &signer else {
            return Err(failed("only an Ed25519 space key can be sealed"));
        };
        // Natively every export is the seed; in the browser a key that
        // was stored whole was stored extractable.
        #[allow(irrefutable_let_patterns)]
        let KeyExport::Extractable(seed) = ed25519.export().await.map_err(failed)? else {
            return Err(failed("the space's stored key is not extractable"));
        };
        let sealed = self.seal_to_account(&seed).await?;
        self.delegate_to_account(&signer).await?;
        self.seal_space(&credential.did(), sealed.to_bytes())
            .await?;
        Subject::from(credential.did())
            .credential()
            .key(credential_fx::SELF)
            .save(Credential::from(signer.verifier()))
            .perform(&self.storage)
            .await
            .map_err(failed)?;
        Ok(handed::<M>(credential))
    }

    /// `seed`, sealed to the account this peer acts for.
    async fn seal_to_account(&self, seed: &[u8]) -> Result<SealedSecret, storage_fx::StorageError> {
        let account: Ed25519Verifier = self.account().to_string().parse().map_err(|_| {
            failed(format!(
                "the account {} has no key to seal the space's key to",
                self.account()
            ))
        })?;
        account
            .secret(SPACE_KEY)
            .conceal(seed)
            .await
            .map_err(failed)
    }

    /// Have the space `space` is the key of delegate its whole authority
    /// to the account this peer acts for, and retain the delegation where
    /// the peer proves from.
    async fn delegate_to_account(&self, space: &Signer) -> Result<(), storage_fx::StorageError>
    where
        Self: Provider<Retain<Ucan>>,
    {
        let delegation = DelegationBuilder::new()
            .issuer(space.clone())
            .audience(self.account())
            .subject(UcanSubject::Specific(space.did()))
            .command(Vec::new())
            .try_build()
            .await
            .map_err(|error| storage_fx::StorageError::Storage(format!("{error:?}")))?;
        Subject::from(self.home().clone())
            .attenuate(Access)
            .invoke(Retain::<Ucan>::new(UcanDelegation::new(
                DelegationChain::new(delegation),
            )))
            .perform(self)
            .await
            .map_err(|error| {
                storage_fx::StorageError::Storage(format!(
                    "the space's delegation to its account was not kept: {error}"
                ))
            })
    }

    /// Refuse `mount` unless this peer can prove the storage's system
    /// granted it: directly, for the peer acting as itself, or through
    /// the peer it is a session of.
    pub(crate) async fn may_mount<Fx>(
        &self,
        mount: &Capability<Fx>,
    ) -> Result<(), storage_fx::StorageError>
    where
        Self: Provider<Prove<Ucan>>,
        Fx: Effect + Clone,
        Fx::Of: Constraint,
        Capability<Fx>: Ability,
    {
        Subject::from(self.did())
            .attenuate(Access)
            .invoke(Prove::<Ucan>::new(
                self.did(),
                Scope::from_capability(mount),
            ))
            .perform(self)
            .await
            .map(|_| ())
            .map_err(|error| {
                storage_fx::StorageError::Storage(format!(
                    "mounting a space is not authorized: {error}"
                ))
            })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<space_fx::Create> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Storage<S>: Provider<storage_fx::Create>
        + Provider<storage_fx::Load>
        + Provider<credential_fx::Load<Credential>>,
    Self: RegistryEnv
        + Provider<Prove<Ucan>>
        + Provider<Retain<Ucan>>
        + ConditionalSend
        + ConditionalSync,
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
        let key = match &space_fx::Create::of(&input).key {
            Some(key) => key.0.clone(),
            None => <Ed25519Signer<Extractable> as ExtractableKey>::generate()
                .await
                .map_err(failed)?,
        };

        // The space's key is sealed to the account it delegates to, and
        // kept only as that: the account can reach it, to name co-owners
        // or rotate, and nothing else can.
        // Only the browser has a second, opaque kind of export; natively
        // every export is the seed.
        #[allow(irrefutable_let_patterns)]
        let KeyExport::Extractable(seed) = key.export().await.map_err(failed)? else {
            return Err(failed("the space's key is not extractable"));
        };
        let sealed = self.seal_to_account(&seed).await?;
        let signer = Signer::from(
            Ed25519Signer::import(KeyExport::Extractable(seed))
                .await
                .map_err(failed)?,
        );
        let location = storage_fx::Location::new(self.directory().clone(), name);

        // The space keeps its identity, not its key: whoever holds the
        // storage finds nothing to sign as the space with.
        let create = Subject::from(self.system().clone())
            .attenuate(storage_fx::Storage)
            .attenuate(location.clone())
            .create(Credential::from(signer.verifier()));
        self.may_mount(&create).await?;
        let created = create.perform(&self.storage).await?;

        // Its authority goes to the account it is created for, where the
        // peer acting for that account proves it from.
        self.delegate_to_account(&signer).await?;
        self.seal_space(&created.did(), sealed.to_bytes()).await?;
        self.record_space(&created.did(), name, &location).await;
        Ok(Credential::Signer(SignerCredential::from(signer)))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_grant, test_storage, test_system, unique_name};
    use crate::{ClaimExt as _, Peer};
    use dialog_capability::access::{Access, Prove};
    use dialog_capability::{Subject, did};
    use dialog_credentials::key::KeyExport;
    use dialog_credentials::secret::{Context, SealedSecret};
    use dialog_credentials::{Credential, Ed25519Signer, Signer, SignerCredential};
    use dialog_effects::credential::{self as credential_fx, prelude::*};
    use dialog_effects::storage::{self as storage_fx, Directory, Location, LocationExt as _};
    use dialog_identity::OpenCredential;
    use dialog_repository::{RepositoryExt as _, spaces};
    use dialog_storage::provider::storage::{Storage, VolatileSpace};
    use dialog_ucan::{Parameters, Scope, Ucan};
    use dialog_ucan_core::command::Command as UcanCommand;
    use dialog_ucan_core::subject::Subject as UcanSubject;
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
            .grant(test_grant().await)
            .base(Directory::At(base.into()))
            .await?)
    }

    /// A name resolves to the repository recorded under it, loaded from
    /// where it was recorded, though the peer now looks for names it has
    /// no record of somewhere else.
    #[dialog_common::test]
    async fn it_resolves_a_name_from_where_it_was_recorded() -> anyhow::Result<()> {
        let storage = test_storage().await;
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

    /// Only the system a storage belongs to grants mounting spaces in
    /// it: a peer built over a storage it was granted nothing over is
    /// refused rather than granted it.
    #[dialog_common::test]
    async fn it_refuses_to_build_a_peer_over_a_storage_it_was_not_granted() -> anyhow::Result<()> {
        let system = SignerCredential::from(Ed25519Signer::generate().await?);
        let storage = Storage::volatile().owned_by(system.did());
        let credential = OpenCredential::open(unique_name("alice"))
            .perform(&storage)
            .await?;
        let built = Peer::new(credential).storage(storage).await;
        assert!(
            built.is_err(),
            "a peer was built over a storage nobody granted it"
        );
        Ok(())
    }

    /// Opening a space mounts it in storage, which is the storage's to
    /// allow: a session its peer granted nothing over storage is refused,
    /// while one granted everything the peer holds opens it.
    #[dialog_common::test]
    async fn it_opens_a_space_for_a_session_only_with_the_storages_authority() -> anyhow::Result<()>
    {
        let storage = test_storage().await;
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

    /// A space keeps no signing key: whoever holds the storage finds the
    /// space's identity there, but nothing to sign as it with.
    #[dialog_common::test]
    async fn it_keeps_no_signing_key_in_a_created_space() -> anyhow::Result<()> {
        let storage = test_storage().await;
        let credential = OpenCredential::open(unique_name("alice"))
            .perform(&storage)
            .await?;
        let peer = peer_at(&storage, &credential, "/keys").await?;
        let created = peer
            .space(unique_name("notes"))
            .create()
            .perform(&peer)
            .await?;

        let stored = Subject::from(created.did())
            .credential()
            .key(credential_fx::SELF)
            .load()
            .perform(&storage)
            .await?;
        assert!(
            matches!(stored, Credential::Verifier(_)),
            "the space holds its signing key"
        );
        Ok(())
    }

    /// A space delegates to the account it is created for, so the peer
    /// acting for that account proves its authority over the space with
    /// no delegation minted by hand.
    #[dialog_common::test]
    async fn it_proves_authority_over_a_space_it_created() -> anyhow::Result<()> {
        let storage = test_storage().await;
        let credential = OpenCredential::open(unique_name("alice"))
            .perform(&storage)
            .await?;
        let peer = peer_at(&storage, &credential, "/authority").await?;
        let created = peer
            .space(unique_name("notes"))
            .create()
            .perform(&peer)
            .await?;

        let scope = Scope {
            subject: UcanSubject::Specific(created.did()),
            command: UcanCommand(vec!["archive".to_string()]),
            parameters: Parameters::default(),
        };
        Subject::from(peer.did())
            .attenuate(Access)
            .invoke(Prove::<Ucan>::new(peer.did(), scope))
            .perform(&peer)
            .await?;
        Ok(())
    }

    /// A space's key is kept only sealed to its account, and the account
    /// opens it: the key it reveals is the one the space is named by.
    #[dialog_common::test]
    async fn it_seals_a_created_spaces_key_to_its_account() -> anyhow::Result<()> {
        let storage = test_storage().await;
        let credential = OpenCredential::open(unique_name("alice"))
            .perform(&storage)
            .await?;
        let peer = peer_at(&storage, &credential, "/sealed").await?;
        let created = peer
            .space(unique_name("notes"))
            .create()
            .perform(&peer)
            .await?;

        let sealed = spaces::sealed(peer.state()?, &created.did(), &peer)
            .await?
            .expect("the space's key is kept sealed");
        // Other algorithms are features; without them this always holds.
        #[allow(irrefutable_let_patterns)]
        let Signer::Ed25519(account) = credential.signer() else {
            panic!("the account is an Ed25519 key");
        };
        let seed = account
            .secret(Context::new("dialog.space/key"))
            .reveal(&SealedSecret::from_bytes(&sealed)?)
            .await?;
        let key = Ed25519Signer::import(KeyExport::Extractable(seed)).await?;
        assert_eq!(key.did(), created.did());
        Ok(())
    }

    /// A space from before keys were sealed still holds its signing key.
    /// Loading it through a peer seals the key to the account, has the
    /// space delegate to it, and leaves only the verifier in the space.
    #[dialog_common::test]
    async fn it_seals_the_key_of_a_space_from_before_keys_were_sealed() -> anyhow::Result<()> {
        let storage = test_storage().await;
        let credential = OpenCredential::open(unique_name("alice"))
            .perform(&storage)
            .await?;
        let peer = peer_at(&storage, &credential, "/legacy").await?;
        let name = unique_name("notes");

        // Created the way every space was before: its key stored in it.
        let location = Location::new(Directory::At("/legacy".into()), name.as_str());
        let repository =
            Credential::Signer(SignerCredential::from(Ed25519Signer::generate().await?));
        Subject::from(did!("local:storage"))
            .attenuate(storage_fx::Storage)
            .attenuate(location)
            .create(repository.clone())
            .perform(&storage)
            .await?;

        let loaded = peer.space(name).load().perform(&peer).await?;
        assert_eq!(loaded.did(), repository.did());

        let stored = Subject::from(repository.did())
            .credential()
            .key(credential_fx::SELF)
            .load()
            .perform(&storage)
            .await?;
        assert!(
            matches!(stored, Credential::Verifier(_)),
            "the space still holds its signing key"
        );
        assert!(
            spaces::sealed(peer.state()?, &repository.did(), &peer)
                .await?
                .is_some(),
            "the space's key was not sealed"
        );
        let scope = Scope {
            subject: UcanSubject::Specific(repository.did()),
            command: UcanCommand(vec!["archive".to_string()]),
            parameters: Parameters::default(),
        };
        Subject::from(peer.did())
            .attenuate(Access)
            .invoke(Prove::<Ucan>::new(peer.did(), scope))
            .perform(&peer)
            .await?;
        Ok(())
    }

    /// A repository found in the base directory, with no record of its
    /// name, is recorded the first time it is loaded.
    #[dialog_common::test]
    async fn it_records_a_name_found_in_the_base_directory() -> anyhow::Result<()> {
        let storage = test_storage().await;
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

        let first = Storage::<NativeSpace>::default().owned_by(test_system().await.did());
        let credential = OpenCredential::open(name.clone())
            .at(base.clone())
            .perform(&first)
            .await?;
        let peer = Peer::new(credential.clone())
            .storage(first)
            .grant(test_grant().await)
            .base(base.clone())
            .await?;
        let notes = peer.space("notes").create().perform(&peer).await?;

        let second = Storage::<NativeSpace>::default().owned_by(test_system().await.did());
        let credential = OpenCredential::load(name)
            .at(base.clone())
            .perform(&second)
            .await?;
        let restarted = Peer::new(credential)
            .storage(second)
            .grant(test_grant().await)
            .base(base)
            .await?;
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
