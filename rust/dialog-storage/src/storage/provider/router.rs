//! Storage router: maps DIDs to Spaces.
//!
//! `Router` holds factory configuration and a runtime table mapping
//! subject DIDs to mounted [`Space`] instances.

use std::collections::HashMap;

use dialog_capability::Did;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::credential::Credential;
use dialog_effects::credential as cred_fx;
use dialog_varsig::Principal;

use super::location::Location;
use super::space::{Factory, Space};

/// The Space type produced by a set of factories.
type MountedSpace<AF, MF, CF, PF> = Space<
    <AF as Factory>::Provider,
    <MF as Factory>::Provider,
    <CF as Factory>::Provider,
    <PF as Factory>::Provider,
>;

/// Storage router that maps DIDs to mounted Spaces.
///
/// Generic over factory types. Each factory creates a provider for
/// its concern (archive, memory, credential, permit) from a Location.
/// Platform defaults are provided via type aliases.
pub struct Router<AF, MF, CF, PF>
where
    AF: Factory,
    MF: Factory,
    CF: Factory,
    PF: Factory,
{
    archive_factory: AF,
    memory_factory: MF,
    credential_factory: CF,
    permit_factory: PF,
    spaces: HashMap<Did, MountedSpace<AF, MF, CF, PF>>,
}

impl<AF, MF, CF, PF> Router<AF, MF, CF, PF>
where
    AF: Factory,
    MF: Factory,
    CF: Factory,
    PF: Factory,
    CF::Provider: dialog_capability::Provider<cred_fx::Load> + ConditionalSend + ConditionalSync,
{
    /// Create a new router with the given factories.
    pub fn new(
        archive_factory: AF,
        memory_factory: MF,
        credential_factory: CF,
        permit_factory: PF,
    ) -> Self {
        Self {
            archive_factory,
            memory_factory,
            credential_factory,
            permit_factory,
            spaces: HashMap::new(),
        }
    }

    /// Mount a location: create providers, read credential, register by DID.
    ///
    /// Returns the credential found at the location.
    pub async fn mount(&mut self, location: &Location) -> Result<Credential, MountError> {
        let archive = self.archive_factory.create(location);
        let memory = self.memory_factory.create(location);
        let credential_provider = self.credential_factory.create(location);
        let permit = self.permit_factory.create(location);

        // Read credential directly from the provider (no DID routing needed)
        let credential = load_credential(&credential_provider).await?;
        let did = credential.did();

        let space = Space {
            archive,
            memory,
            credential: credential_provider,
            permit,
        };

        self.spaces.insert(did, space);
        Ok(credential)
    }

    /// Look up the space for a DID.
    pub fn get(&self, did: &Did) -> Option<&MountedSpace<AF, MF, CF, PF>> {
        self.spaces.get(did)
    }

    /// Check if a DID is mounted.
    pub fn contains(&self, did: &Did) -> bool {
        self.spaces.contains_key(did)
    }
}

/// Load credential directly from a provider without capability routing.
async fn load_credential<P>(provider: &P) -> Result<Credential, MountError>
where
    P: dialog_capability::Provider<cred_fx::Load> + ConditionalSync,
{
    use dialog_capability::Subject;

    // Use a placeholder subject since we don't know the DID yet.
    // The provider is already scoped to the right location.
    let placeholder = dialog_capability::did!("local:storage");
    let capability = Subject::from(placeholder)
        .attenuate(cred_fx::Credential)
        .attenuate(cred_fx::Address::new("credential"))
        .invoke(cred_fx::Load);

    provider
        .execute(capability)
        .await
        .map_err(|e| MountError::Credential(e.to_string()))
}

/// Blanket Provider impl: routes effects by subject DID to the matching Space.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<AF, MF, CF, PF, Fx> dialog_capability::Provider<Fx> for Router<AF, MF, CF, PF>
where
    AF: Factory,
    MF: Factory,
    CF: Factory,
    PF: Factory,
    Fx: dialog_capability::Effect + ConditionalSend + 'static,
    Fx::Output: FromUnmounted,
    dialog_capability::Capability<Fx>: ConditionalSend,
    Space<AF::Provider, MF::Provider, CF::Provider, PF::Provider>:
        dialog_capability::Provider<Fx> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: dialog_capability::Capability<Fx>) -> Fx::Output {
        let did = input.subject().clone();
        match self.spaces.get(&did) {
            Some(space) => space.execute(input).await,
            None => Fx::Output::unmounted(&did),
        }
    }
}

trait FromUnmounted {
    fn unmounted(did: &Did) -> Self;
}

impl<T, E: From<dialog_capability::storage::StorageError>> FromUnmounted for Result<T, E> {
    fn unmounted(did: &Did) -> Self {
        Err(dialog_capability::storage::StorageError::Storage(format!("no mount for {did}")).into())
    }
}

/// Errors during mount.
#[derive(Debug, thiserror::Error)]
pub enum MountError {
    /// Credential not found or failed to load.
    #[error("Failed to load credential: {0}")]
    Credential(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::Volatile;
    use crate::provider::volatile::VolatileFactory;
    use dialog_capability::{Subject, did};
    use dialog_credentials::Ed25519Signer;
    use dialog_effects::archive::{Archive, Catalog, Get, Put};
    use dialog_effects::credential as cred_fx;

    type TestRouter = Router<VolatileFactory, VolatileFactory, VolatileFactory, VolatileFactory>;

    fn volatile_router() -> TestRouter {
        Router::new(
            VolatileFactory,
            VolatileFactory,
            VolatileFactory,
            VolatileFactory,
        )
    }

    async fn mount_with_credential(router: &mut TestRouter) -> Did {
        let provider = Volatile::new();

        let signer = Ed25519Signer::generate().await.unwrap();
        let did = dialog_varsig::Principal::did(&signer);
        let credential = dialog_credentials::credential::Credential::Signer(
            dialog_credentials::SignerCredential::from(signer),
        );

        let save_cap = Subject::from(did!("local:storage"))
            .attenuate(cred_fx::Credential)
            .attenuate(cred_fx::Address::new("credential"))
            .invoke(cred_fx::Save { credential });

        save_cap.perform(&provider).await.unwrap();

        let space = Space {
            archive: provider.clone(),
            memory: provider.clone(),
            credential: provider.clone(),
            permit: provider,
        };

        router.spaces.insert(did.clone(), space);
        did
    }

    #[dialog_common::test]
    async fn it_routes_archive_effects_by_did() {
        let mut router = volatile_router();
        let did = mount_with_credential(&mut router).await;

        let content = b"hello world".to_vec();
        let digest = dialog_common::Blake3Hash::hash(&content);

        let put_cap = Subject::from(did.clone())
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(digest.clone(), content.clone()));

        put_cap.perform(&router).await.unwrap();

        let get_cap = Subject::from(did)
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest));

        let result = get_cap.perform(&router).await;
        assert_eq!(result.unwrap(), Some(content));
    }

    #[dialog_common::test]
    async fn it_errors_for_unmounted_did() {
        let router = volatile_router();

        let get_cap = Subject::from(did!("key:zUnknown"))
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new([0u8; 32]));

        let result = get_cap.perform(&router).await;
        assert!(result.is_err(), "should error for unmounted DID");
    }

    #[dialog_common::test]
    async fn it_isolates_spaces_by_did() {
        let mut router = volatile_router();
        let did1 = mount_with_credential(&mut router).await;
        let did2 = mount_with_credential(&mut router).await;

        let content = b"did1 data".to_vec();
        let digest = dialog_common::Blake3Hash::hash(&content);

        let put_cap = Subject::from(did1.clone())
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(digest.clone(), content));

        put_cap.perform(&router).await.unwrap();

        let get_cap = Subject::from(did2)
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest));

        let result = get_cap.perform(&router).await;
        assert_eq!(result.unwrap(), None, "did2 should not see did1's data");
    }
}
