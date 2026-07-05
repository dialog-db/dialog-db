//! DID-based effect routing.
//!
//! Routes capability effects to the correct space provider based on the
//! subject DID extracted from the capability.

use std::sync::Arc;

use async_trait::async_trait;
use dialog_capability::{Capability, Did, Effect, Provider, StorageError};
use dialog_common::{ConditionalSend, ConditionalSync};

use crate::resource::Pool;

/// Routes effects by subject DID to the matching store.
#[derive(Clone)]
pub struct Router<S> {
    pub spaces: Arc<Pool<Did, S>>,
}

impl<S> Router<S> {
    pub fn new(spaces: Arc<Pool<Did, S>>) -> Self {
        Self { spaces }
    }
}

trait FromUnmounted {
    fn unmounted(did: &Did) -> Self;
}

impl<T, E: From<StorageError>> FromUnmounted for Result<T, E> {
    fn unmounted(did: &Did) -> Self {
        Err(StorageError::Storage(format!("no mount for {did}")).into())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S, Fx> Provider<Fx> for Router<S>
where
    S: Provider<Fx> + ConditionalSync + Clone,
    Fx: Effect + ConditionalSend + 'static,
    Fx::Output: FromUnmounted,
    Capability<Fx>: ConditionalSend,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Fx>) -> Fx::Output {
        let did = input.subject().clone();
        let store = self.spaces.get(&did);
        match store {
            Some(store) => input.perform(&store).await,
            None => Fx::Output::unmounted(&did),
        }
    }
}
