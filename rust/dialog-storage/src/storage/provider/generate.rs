//! Generating a signing key in a store that keeps it in process: the key is
//! made here, and saved where the store saves any other key. A store that
//! keeps its keys elsewhere answers `Generate` its own way.

use dialog_capability::{Capability, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_credentials::{Credential, Ed25519Signer, SignerCredential};
use dialog_effects::credential::prelude::{
    CredentialCapabilityExt as _, CredentialKeyExt as _, CredentialSubjectExt as _,
    GenerateCredentialExt as _,
};
use dialog_effects::credential::{CredentialError, Generate, Save};

/// Make a signing key and save it at the address `input` names in `store`.
pub(crate) async fn generate<P>(
    store: &P,
    input: &Capability<Generate>,
) -> Result<Credential, CredentialError>
where
    P: Provider<Save<Credential>> + ConditionalSync,
{
    let signer = Ed25519Signer::generate()
        .await
        .map_err(|error| CredentialError::Storage(error.to_string()))?;
    let credential = Credential::Signer(SignerCredential::from(signer));
    Subject::from(input.subject().clone())
        .credential()
        .key(input.address())
        .save(credential.clone())
        .perform(store)
        .await?;
    Ok(credential)
}
