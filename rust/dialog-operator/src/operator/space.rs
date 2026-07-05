//! Space capability providers for Operator.

use super::Operator;
use dialog_capability::{Capability, Policy, Provider, Subject, did};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::Credential;
use dialog_effects::space as space_fx;
use dialog_effects::storage::{self as storage_fx, LocationExt as _};
use dialog_storage::provider::storage::Storage;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<space_fx::Load> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Storage<S>: Provider<storage_fx::Load>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<space_fx::Load>,
    ) -> Result<Credential, storage_fx::StorageError> {
        let subject = input.subject();
        if *subject != self.profile_did() {
            return Err(storage_fx::StorageError::Storage(format!(
                "space load denied: subject {subject} does not match profile {}",
                self.profile_did()
            )));
        }

        let name = &space_fx::Space::of(&input).name;
        let location = storage_fx::Location::new(self.directory.clone(), name);
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
impl<S> Provider<space_fx::Create> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Storage<S>: Provider<storage_fx::Create>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<space_fx::Create>,
    ) -> Result<Credential, storage_fx::StorageError> {
        let subject = input.subject();
        if *subject != self.profile_did() {
            return Err(storage_fx::StorageError::Storage(format!(
                "space create denied: subject {subject} does not match profile {}",
                self.profile_did()
            )));
        }

        let name = &space_fx::Space::of(&input).name;
        let credential = space_fx::Create::of(&input).credential.clone();
        let location = storage_fx::Location::new(self.directory.clone(), name);
        Subject::from(did!("local:storage"))
            .attenuate(storage_fx::Storage)
            .attenuate(location)
            .create(credential)
            .perform(&self.storage)
            .await
    }
}
