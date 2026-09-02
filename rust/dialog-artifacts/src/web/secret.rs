//! Web bindings for sealing secrets to a `did:key`.
//!
//! A holder of someone's DID can conceal bytes so that only that identity can
//! reveal them, with nothing published or exchanged first.
//!
//! Example usage in JavaScript:
//!
//! ```ignore
//! // This is JavaScript code that uses the WASM bindings, not a Rust doctest
//! import { conceal, Signer } from "dialog-artifacts";
//!
//! const VAULT = "dialog/vault/v1";
//!
//! const signer = await Signer.generate();
//!
//! // Anyone holding the DID can seal to it.
//! const sealed = await conceal(signer.did, VAULT, vaultKey);
//!
//! // Only the identity the DID names can open it.
//! const vaultKey = await signer.reveal(VAULT, sealed);
//! ```

use std::{cell::RefCell, collections::HashSet};

use dialog_credentials::{
    Ed25519Signer, Ed25519Verifier, KeyExport,
    secret::{Context, SealedSecret},
};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(typescript_custom_section)]
const SECRET_INTERFACE: &'static str = r#"
/**
 * The key material of a `Signer`, as `Signer.export` produces it and
 * `Signer.import` accepts it. Either a 32-byte seed, or - in the browser - an
 * object of opaque `CryptoKey`s that can be stored but not read.
 */
type KeyExport = Uint8Array|object;
"#;

#[wasm_bindgen]
extern "C" {
    /// The JavaScript form of a `Signer`'s key material.
    #[wasm_bindgen(typescript_type = "KeyExport")]
    pub type KeyExportDuckType;
}

/// Conceal `plain` so that only the identity named by `recipient` can reveal
/// it, in `context`.
///
/// The `recipient` is an Ed25519 `did:key` string and `context` is a label
/// scoping the secret to one purpose; revealing needs the same label, so a
/// secret sealed for one purpose cannot be opened as another. Sealing the same
/// bytes twice produces different output.
#[wasm_bindgen]
pub async fn conceal(
    recipient: String,
    context: String,
    plain: Vec<u8>,
) -> Result<Vec<u8>, JsError> {
    let recipient = recipient.parse::<Ed25519Verifier>()?;
    let sealed = recipient.secret(intern(&context)).conceal(&plain).await?;

    Ok(sealed.to_bytes())
}

/// An identity that can reveal secrets concealed to its `did:key`.
#[wasm_bindgen(js_name = "Signer")]
pub struct SignerBinding {
    signer: Ed25519Signer,
}

#[wasm_bindgen(js_class = "Signer")]
impl SignerBinding {
    /// Generate a new identity. In the browser the signing key is a
    /// non-extractable `WebCrypto` key, so `Signer.export` is the only way to
    /// carry it to another session.
    #[wasm_bindgen]
    pub async fn generate() -> Result<SignerBinding, JsError> {
        Ok(Self {
            signer: Ed25519Signer::generate().await?,
        })
    }

    /// Restore an identity from a value that `Signer.export` produced, or from
    /// a 32-byte seed.
    #[wasm_bindgen]
    pub async fn import(key: KeyExportDuckType) -> Result<SignerBinding, JsError> {
        let key = KeyExport::try_from(JsValue::from(key))?;

        Ok(Self {
            signer: Ed25519Signer::import(key).await?,
        })
    }

    /// The key material of this identity, in the form `Signer.import` accepts.
    ///
    /// In the browser this is a structured-cloneable object of opaque
    /// `CryptoKey`s rather than bytes, so it can be stored without the key
    /// material ever being readable.
    #[wasm_bindgen(unchecked_return_type = "KeyExport")]
    pub async fn export(&self) -> Result<JsValue, JsError> {
        Ok(self.signer.export().await?.into())
    }

    /// The `did:key` naming this identity; hand it out so others can conceal
    /// to it.
    #[wasm_bindgen(getter)]
    pub fn did(&self) -> String {
        self.signer.ed25519_did().to_string()
    }

    /// Reveal a secret concealed to this identity in `context`.
    ///
    /// Fails if the secret was sealed to a different identity or context, or
    /// if it has been tampered with; the cases are not distinguished.
    #[wasm_bindgen]
    pub async fn reveal(&self, context: String, sealed: Vec<u8>) -> Result<Vec<u8>, JsError> {
        let sealed = SealedSecret::from_bytes(&sealed)?;

        Ok(self.signer.secret(intern(&context)).reveal(&sealed).await?)
    }
}

/// Turn a context label from JavaScript into a [`Context`].
///
/// A `Context` is built from a static label, while JavaScript hands one over
/// as an owned string. Labels are few and live as long as the module, so each
/// distinct one is leaked once and reused for every later call.
fn intern(label: &str) -> Context {
    thread_local! {
        static LABELS: RefCell<HashSet<&'static str>> = RefCell::new(HashSet::new());
    }

    LABELS.with_borrow_mut(|labels| {
        if let Some(label) = labels.get(label).copied() {
            return Context::new(label);
        }

        let label: &'static str = String::from(label).leak();
        labels.insert(label);

        Context::new(label)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    const VAULT: &str = "dialog/vault/v1";

    #[dialog_common::test]
    async fn it_reveals_what_was_concealed_to_a_did() {
        let signer = SignerBinding::generate().await.unwrap();
        let plain = b"vault key".to_vec();

        // The sealing side holds only the DID string.
        let sealed = conceal(signer.did(), VAULT.to_owned(), plain.clone())
            .await
            .unwrap();

        let revealed = signer.reveal(VAULT.to_owned(), sealed).await.unwrap();

        assert_eq!(revealed, plain, "the same bytes should come back out");
    }
}
