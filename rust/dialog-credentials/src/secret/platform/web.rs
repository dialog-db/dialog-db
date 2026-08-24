//! Browser AES-256-GCM and HKDF-SHA256 via `WebCrypto`.

use super::super::SecretError;
use js_sys::{Object, Reflect, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{CryptoKey, SubtleCrypto};

/// Get the `SubtleCrypto` interface.
fn subtle() -> Result<SubtleCrypto, SecretError> {
    let global = js_sys::global();
    let crypto = Reflect::get(&global, &"crypto".into())
        .map_err(|_| SecretError::Crypto("crypto not found".into()))?;
    let subtle = Reflect::get(&crypto, &"subtle".into())
        .map_err(|_| SecretError::Crypto("crypto.subtle not found".into()))?;
    if subtle.is_undefined() {
        return Err(SecretError::Crypto("crypto.subtle is undefined".into()));
    }
    Ok(subtle.unchecked_into())
}

/// Derive an AES-256 key from a shared secret using HKDF-SHA256.
pub(super) async fn derive_key(shared: &[u8; 32], info: &[u8]) -> Result<[u8; 32], SecretError> {
    let subtle = subtle()?;

    // Import the raw shared secret as HKDF key material.
    let usages = js_sys::Array::new();
    usages.push(&"deriveBits".into());
    let data = Uint8Array::from(shared.as_slice());

    let promise = subtle
        .import_key_with_str("raw", &data.buffer(), "HKDF", false, &usages)
        .map_err(|e| SecretError::Crypto(format!("HKDF import: {e:?}")))?;
    let base_key: CryptoKey = JsFuture::from(promise)
        .await
        .map_err(|e| SecretError::Crypto(format!("HKDF import: {e:?}")))?
        .unchecked_into();

    let algorithm = Object::new();
    set(&algorithm, "name", &"HKDF".into())?;
    set(&algorithm, "hash", &"SHA-256".into())?;
    // An empty salt matches `Hkdf::new(None, ..)` on the native side.
    set(&algorithm, "salt", &Uint8Array::new_with_length(0).into())?;
    set(&algorithm, "info", &Uint8Array::from(info).into())?;

    let promise = subtle
        .derive_bits_with_object(&algorithm, &base_key, 256)
        .map_err(|e| SecretError::Crypto(format!("HKDF derive: {e:?}")))?;
    let derived = JsFuture::from(promise)
        .await
        .map_err(|e| SecretError::Crypto(format!("HKDF derive: {e:?}")))?;

    let array = Uint8Array::new(&derived);
    if array.length() != 32 {
        return Err(SecretError::Crypto(format!(
            "expected 32 derived bytes, got {}",
            array.length()
        )));
    }
    let mut key = [0u8; 32];
    array.copy_to(&mut key);
    Ok(key)
}

/// Import an AES-256-GCM key.
async fn import_aes_key(
    key: &[u8; 32],
    usage: &str,
) -> Result<(SubtleCrypto, CryptoKey), SecretError> {
    let subtle = subtle()?;
    let usages = js_sys::Array::new();
    usages.push(&usage.into());
    let data = Uint8Array::from(key.as_slice());

    let promise = subtle
        .import_key_with_str("raw", &data.buffer(), "AES-GCM", false, &usages)
        .map_err(|e| SecretError::Crypto(format!("AES import: {e:?}")))?;
    let key: CryptoKey = JsFuture::from(promise)
        .await
        .map_err(|e| SecretError::Crypto(format!("AES import: {e:?}")))?
        .unchecked_into();

    Ok((subtle, key))
}

/// Build the AES-GCM algorithm parameters.
fn aes_params(nonce: &[u8; 12], aad: &[u8]) -> Result<Object, SecretError> {
    let algorithm = Object::new();
    set(&algorithm, "name", &"AES-GCM".into())?;
    set(&algorithm, "iv", &Uint8Array::from(nonce.as_slice()).into())?;
    set(&algorithm, "additionalData", &Uint8Array::from(aad).into())?;
    // 128-bit tag, matching the RustCrypto default on native.
    set(&algorithm, "tagLength", &128.into())?;
    Ok(algorithm)
}

/// Encrypt with AES-256-GCM.
pub(super) async fn encrypt(
    key: &[u8; 32],
    nonce: &[u8; 12],
    plain: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, SecretError> {
    let (subtle, key) = import_aes_key(key, "encrypt").await?;
    let algorithm = aes_params(nonce, aad)?;
    let data = Uint8Array::from(plain);

    let promise = subtle
        .encrypt_with_object_and_buffer_source(&algorithm, &key, &data)
        .map_err(|e| SecretError::Crypto(format!("AES encrypt: {e:?}")))?;
    let result = JsFuture::from(promise)
        .await
        .map_err(|e| SecretError::Crypto(format!("AES encrypt: {e:?}")))?;

    Ok(Uint8Array::new(&result).to_vec())
}

/// Decrypt with AES-256-GCM.
pub(super) async fn decrypt(
    key: &[u8; 32],
    nonce: &[u8; 12],
    ciphertext: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, SecretError> {
    let (subtle, key) = import_aes_key(key, "decrypt").await?;
    let algorithm = aes_params(nonce, aad)?;
    let data = Uint8Array::from(ciphertext);

    let promise = subtle
        .decrypt_with_object_and_buffer_source(&algorithm, &key, &data)
        .map_err(|_| SecretError::Failed)?;
    // Authentication failures land here and collapse to `Failed`, matching
    // native: nothing distinguishes a wrong key from a tampered message.
    let result = JsFuture::from(promise)
        .await
        .map_err(|_| SecretError::Failed)?;

    Ok(Uint8Array::new(&result).to_vec())
}

/// Set a property on a JS object.
fn set(target: &Object, key: &str, value: &JsValue) -> Result<(), SecretError> {
    Reflect::set(target, &key.into(), value)
        .map(|_| ())
        .map_err(|e| SecretError::Crypto(format!("failed to set {key}: {e:?}")))
}
