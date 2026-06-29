//! Blob request mapping for S3.
//!
//! Blobs live at `{subject}/blob/{base58(digest)}`, parallel to the block
//! archive at `{subject}/{catalog}/{base58(digest)}`. A `Read` is a GET (the
//! provider adds a `Range` header from the effect's range); an `Import` is a
//! PUT.

use super::S3Request;
use base58::ToBase58;
use dialog_capability::Capability;
use dialog_effects::blob::prelude::{BlobImportExt as _, BlobReadExt as _};
use dialog_effects::blob::{Import, Read};

impl From<&Capability<Read>> for S3Request {
    fn from(capability: &Capability<Read>) -> Self {
        S3Request {
            method: "GET".to_string(),
            path: format!(
                "{}/blob/{}",
                capability.subject(),
                capability.digest().as_bytes().to_base58()
            ),
            ..S3Request::default()
        }
    }
}

impl From<&Capability<Import>> for S3Request {
    fn from(capability: &Capability<Import>) -> Self {
        S3Request {
            method: "PUT".to_string(),
            path: format!(
                "{}/blob/{}",
                capability.subject(),
                capability.digest().as_bytes().to_base58()
            ),
            ..S3Request::default()
        }
    }
}
