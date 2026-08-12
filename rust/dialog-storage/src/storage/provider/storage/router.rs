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

trait FromSubjectNotFound {
    fn subject_not_found(did: &Did) -> Self;
}

impl<T, E: From<StorageError>> FromSubjectNotFound for Result<T, E> {
    fn subject_not_found(did: &Did) -> Self {
        Err(StorageError::SubjectNotFound {
            subject: did.clone(),
        }
        .into())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S, Fx> Provider<Fx> for Router<S>
where
    S: Provider<Fx> + ConditionalSync + Clone,
    Fx: Effect + ConditionalSend + 'static,
    Fx::Output: FromSubjectNotFound,
    Capability<Fx>: ConditionalSend,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Fx>) -> Fx::Output {
        let did = input.subject().clone();
        let store = self.spaces.get(&did);
        match store {
            Some(store) => input.perform(&store).await,
            None => Fx::Output::subject_not_found(&did),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_capability::did;

    // A subject this environment cannot serve is not a backend failure --
    // nothing is broken, and loading the space it belongs to makes the same
    // request succeed. It reports as its own condition, naming the subject,
    // so callers can tell that apart from "the store is down".
    #[dialog_common::test]
    async fn it_reports_a_subject_it_cannot_serve_as_its_own_condition() {
        let subject = did!("key:zUnknown");
        let result: Result<(), StorageError> = Result::subject_not_found(&subject);

        match result {
            Err(StorageError::SubjectNotFound { subject: reported }) => {
                assert_eq!(reported, subject)
            }
            other => panic!("expected SubjectNotFound naming the subject, got {other:?}"),
        }
    }
}
