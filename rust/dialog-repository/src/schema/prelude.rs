//! Extension traits for glob import.
//!
//! ```no_run
//! # use dialog_artifacts::Entity;
//! use dialog_repository::schema::prelude::*;
//!
//! # #[derive(serde::Serialize)] struct Origin;
//! # let origin = Origin;
//! let entity = Entity::of(&origin);
//! ```

use base58::ToBase58;
use dialog_artifacts::Entity;
use dialog_common::Blake3Hash;
use dialog_varsig::Did;
use serde::Serialize;

/// Derive an [`Entity`] from a serializable value.
///
/// `Entity` itself has no awareness of the content-derivation scheme
/// the schema uses. [`EntityExt::of`] hashes the dag-cbor encoding of
/// `value` and formats the result as a `did:key:z6Mk<base58>` URI.
///
/// # Canonical encoding
///
/// The hash is taken over `serde_ipld_dagcbor` bytes, so the resulting
/// entity depends only on the value's semantic content. Field
/// ordering, integer width, and map key sorting are fixed by the
/// dag-cbor specification, so independent implementations that
/// serialize the same logical value converge on the same entity.
///
/// # DID-key shape
///
/// The `did:key:z6Mk` prefix reuses the multibase/multicodec shape
/// dialog-db already uses for randomly generated entity URIs. The
/// `6Mk` prefix nominally indicates ed25519 key material, but nothing
/// in dialog-db enforces that the bytes actually *are* an ed25519
/// public key, so the same shape works for arbitrary 32-byte hashes.
/// If a future version of dialog-db begins validating the multicodec
/// prefix, this is the one place that would need to change.
pub trait EntityExt {
    /// Derive an `Entity` from the dag-cbor encoding of `value`.
    fn of<T: Serialize>(value: &T) -> Entity;
}

impl EntityExt for Entity {
    fn of<T: Serialize>(value: &T) -> Entity {
        let bytes = serde_ipld_dagcbor::to_vec(value)
            .expect("dag-cbor encoding should not fail for schema types");
        let hash = Blake3Hash::hash(&bytes);
        let encoded = hash.as_bytes().as_ref().to_base58();
        format!("did:key:z6Mk{encoded}")
            .parse()
            .expect("did:key URI formed from a 32-byte hash is always valid")
    }
}

/// View a [`Did`] as the entity it identifies.
///
/// DIDs and entities share the `did:method:identifier` URI shape, so
/// a DID string always parses as a valid [`Entity`]. Dialog treats
/// the two as distinct concerns — "a cryptographic identifier"
/// vs. "the subject of artifacts" — but when a schema concept's
/// identity *is* a DID (a profile, a repository subject), the DID is
/// also the concept's `this` entity. [`DidExt::this`] bridges the two.
pub trait DidExt {
    /// Produce the `Entity` this DID identifies.
    fn this(&self) -> Entity;
}

impl DidExt for Did {
    fn this(&self) -> Entity {
        self.as_str()
            .parse()
            .expect("DID string is always a valid Entity URI")
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_varsig::did;

    #[dialog_common::test]
    async fn same_value_same_entity() {
        assert_eq!(Entity::of(&"hello"), Entity::of(&"hello"));
    }

    #[dialog_common::test]
    async fn different_values_different_entity() {
        assert_ne!(Entity::of(&"alice"), Entity::of(&"bob"));
    }

    #[dialog_common::test]
    async fn entity_of_has_did_key_prefix() {
        let e = Entity::of(&"anything");
        assert!(e.to_string().starts_with("did:key:z6Mk"));
    }

    #[dialog_common::test]
    async fn did_this_preserves_uri() {
        let d = did!("key:z6MkTestEntity");
        assert_eq!(d.this().to_string(), d.as_str());
    }
}
