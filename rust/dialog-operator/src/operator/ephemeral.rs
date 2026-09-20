//! The operator's ephemeral-layer providers: a layer is a process
//! resource opened through the environment, never constructed. The
//! registry is weak, so a layer lives exactly as long as some handle
//! to it does, and opening its address afterwards fails honestly.

use dialog_artifacts::Entity;
use dialog_capability::Provider;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_repository::{CreateEphemeral, Ephemeral, EphemeralError, OpenEphemeral};

use crate::Operator;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<CreateEphemeral> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
{
    async fn execute(&self, _: ()) -> Ephemeral {
        self.ephemerals.create()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<OpenEphemeral> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
{
    async fn execute(&self, address: Entity) -> Result<Ephemeral, EphemeralError> {
        self.ephemerals
            .open(&address)
            .ok_or(EphemeralError::NotOpen(address))
    }
}
