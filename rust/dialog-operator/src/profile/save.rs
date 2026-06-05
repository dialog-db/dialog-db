use dialog_capability::access::{Access, Retain as AccessRetain};
use dialog_capability::{Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_ucan::{Ucan, UcanDelegation};
use dialog_varsig::Did;

use super::ProfileError;

type RetainUcan = AccessRetain<Ucan>;

/// Command to store a delegation chain under a profile's DID.
pub struct SaveDelegation {
    pub(super) did: Did,
    pub(super) chain: UcanDelegation,
}

impl SaveDelegation {
    /// Execute against the environment.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), ProfileError>
    where
        Env: Provider<RetainUcan> + ConditionalSync,
    {
        Subject::from(self.did)
            .attenuate(Access)
            .invoke(AccessRetain::<Ucan>::new(self.chain))
            .perform(env)
            .await
            .map_err(|e| ProfileError::Storage(e.to_string()))
    }
}
